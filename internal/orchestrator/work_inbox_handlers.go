package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	coordination "aion-coordination"
)

var contractResourceSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func (s *Server) handleWorkInbox(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	if s.workInbox == nil {
		return Response{ID: req.ID, Error: "work inbox is unavailable"}
	}
	inbox, err := s.workInbox.Inbox(context.Background(), principal)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var params struct {
		Current bool `json:"current,omitempty"`
	}
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
	}
	if !params.Current {
		return Response{ID: req.ID, Result: inbox}
	}
	if inbox.Active != nil {
		return Response{ID: req.ID, Result: map[string]interface{}{"current": inbox.Active}}
	}
	if len(inbox.Ready) > 0 {
		return Response{ID: req.ID, Result: map[string]interface{}{"current": inbox.Ready[0]}}
	}
	return Response{ID: req.ID, Result: map[string]interface{}{"current": nil, "waiting": inbox.Waiting}}
}

func (s *Server) handleWorkCheckpoint(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	if s.workInbox == nil {
		return Response{ID: req.ID, Error: "work inbox is unavailable"}
	}
	var params struct {
		WorkItemID string `json:"work_item_id"`
		Summary    string `json:"summary"`
		Next       string `json:"next,omitempty"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	checkpoint, err := s.workInbox.Checkpoint(context.Background(), principal, params.WorkItemID, params.Summary, params.Next)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: checkpoint}
}

func (s *Server) handleContractEnsure(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	if s.coordination == nil || s.dagManager == nil {
		return Response{ID: req.ID, Error: "coordination manager is unavailable"}
	}
	var params struct {
		ProducerNodeID string                       `json:"producer_node_id"`
		ConsumerNodeID string                       `json:"consumer_node_id"`
		Resource       string                       `json:"resource"`
		Details        coordination.ContractDetails `json:"details"`
		HardWait       bool                         `json:"hard_wait,omitempty"`
		CommandID      string                       `json:"command_id,omitempty"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	params.ProducerNodeID = strings.TrimSpace(params.ProducerNodeID)
	params.ConsumerNodeID = strings.TrimSpace(params.ConsumerNodeID)
	params.Resource = strings.TrimSpace(params.Resource)
	if params.ProducerNodeID == "" || params.ConsumerNodeID == "" || params.Resource == "" {
		return Response{ID: req.ID, Error: "contract-ensure requires producer node, consumer node, and resource"}
	}
	producer, err := s.dagManager.GetNode(params.ProducerNodeID)
	if err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("coordination: producer node: %v", err)}
	}
	consumer, err := s.dagManager.GetNode(params.ConsumerNodeID)
	if err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("coordination: consumer node: %v", err)}
	}
	consumerAgentID := expectedAgentForNode(consumer.DomainID, consumer.AssignedAgent)
	if principal.AgentID != consumerAgentID || !principalOwnsNode(principal, consumer.ID) {
		return Response{ID: req.ID, Error: fmt.Sprintf("coordination: agent %s does not own consumer node %s", principal.AgentID, consumer.ID)}
	}
	producerAgentID := expectedAgentForNode(producer.DomainID, producer.AssignedAgent)
	if producerAgentID == "" {
		return Response{ID: req.ID, Error: fmt.Sprintf("coordination: producer node %s has no domain owner", producer.ID)}
	}
	if params.Details.Name == "" {
		params.Details.Name = params.Resource
	}
	if params.Details.Kind == "" {
		params.Details.Kind = "interface"
	}
	contractID := deterministicContractID(producer.ID, consumer.ID, params.Resource)
	commandID := strings.TrimSpace(params.CommandID)
	if commandID == "" {
		commandID = "ensure:" + contractID
	}
	contract, err := s.coordination.Request(context.Background(), principal, coordination.RequestCommand{
		CommandID: commandID,
		Contract: coordination.Contract{
			ID:              contractID,
			ProducerAgentID: producerAgentID,
			ProducerNodeID:  producer.ID,
			ConsumerAgentID: consumerAgentID,
			ConsumerNodeID:  consumer.ID,
			Details:         params.Details,
			HardWait:        params.HardWait,
		},
	})
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.afterCoordinationMutation("contract_ensured")

	result := map[string]interface{}{"contract": contract}
	if s.workInbox != nil {
		producerPrincipal := s.coordinationPrincipal(producerAgentID)
		position, positionErr := s.workInbox.QueuePosition(context.Background(), producerPrincipal, contractWorkItemID(contract.ID, WorkItemContractPublish))
		if positionErr == nil {
			result["producer_work"] = position
		}
	}
	return Response{ID: req.ID, Result: result}
}

func deterministicContractID(producerNodeID, consumerNodeID, resource string) string {
	identity := strings.Join([]string{strings.TrimSpace(producerNodeID), strings.TrimSpace(consumerNodeID), strings.TrimSpace(resource)}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	name := strings.Trim(contractResourceSanitizer.ReplaceAllString(strings.ToLower(strings.TrimSpace(resource)), "-"), "-")
	if name == "" {
		name = "artifact"
	}
	if len(name) > 32 {
		name = name[:32]
	}
	return "contract-" + name + "-" + hex.EncodeToString(digest[:8])
}

func principalOwnsNode(principal coordination.Principal, nodeID string) bool {
	for _, candidate := range principal.NodeIDs {
		if candidate == nodeID {
			return true
		}
	}
	return false
}
