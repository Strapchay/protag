package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	coordination "aion-coordination"
)

func (s *Server) coordinationPrincipal(agentID string) coordination.Principal {
	principal := coordination.Principal{Role: coordination.RoleAgent, AgentID: strings.TrimSpace(agentID)}
	if s.dagManager == nil || principal.AgentID == "" {
		return principal
	}
	for _, node := range s.dagManager.Snapshot().Nodes {
		if strings.TrimSpace(node.AssignedAgent) == principal.AgentID {
			principal.NodeIDs = append(principal.NodeIDs, node.ID)
		}
	}
	return principal
}

func (s *Server) authenticatedCoordinationPrincipal(req Request) (coordination.Principal, error) {
	if s.coordination == nil {
		return coordination.Principal{}, fmt.Errorf("coordination manager is unavailable")
	}
	policy, ok := s.capabilities.resolvePolicy(req.Capability)
	if !ok {
		return coordination.Principal{}, fmt.Errorf("contract command requires a valid agent capability")
	}
	return s.coordinationPrincipal(policy.AgentID), nil
}

func (s *Server) validateContractAssignments(contract coordination.Contract) error {
	if s.dagManager == nil {
		return fmt.Errorf("coordination: DAG is unavailable")
	}
	producer, err := s.dagManager.GetNode(contract.ProducerNodeID)
	if err != nil {
		return fmt.Errorf("coordination: producer node: %w", err)
	}
	consumer, err := s.dagManager.GetNode(contract.ConsumerNodeID)
	if err != nil {
		return fmt.Errorf("coordination: consumer node: %w", err)
	}
	if expectedAgentForNode(producer.DomainID, producer.AssignedAgent) != contract.ProducerAgentID {
		return fmt.Errorf("coordination: producer agent %s does not own node %s", contract.ProducerAgentID, contract.ProducerNodeID)
	}
	if expectedAgentForNode(consumer.DomainID, consumer.AssignedAgent) != contract.ConsumerAgentID {
		return fmt.Errorf("coordination: consumer agent %s does not own node %s", contract.ConsumerAgentID, contract.ConsumerNodeID)
	}
	return nil
}

func expectedAgentForNode(domainID, assignedAgent string) string {
	if assignedAgent = strings.TrimSpace(assignedAgent); assignedAgent != "" {
		return assignedAgent
	}
	if domainID = strings.TrimSpace(domainID); domainID != "" {
		return "agent-" + domainID
	}
	return ""
}

func (s *Server) handleContractRequest(req Request) Response {
	if s.coordination == nil {
		return Response{ID: req.ID, Error: "coordination manager is unavailable"}
	}
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var command coordination.RequestCommand
	if err := json.Unmarshal(req.Params, &command); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	if err := s.validateContractAssignments(command.Contract); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	contract, err := s.coordination.Request(context.Background(), principal, command)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.afterCoordinationMutation("contract_requested")
	return Response{ID: req.ID, Result: contract}
}

func (s *Server) handleContractPublish(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var command coordination.PublishCommand
	if err := json.Unmarshal(req.Params, &command); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	contract, err := s.coordination.Publish(context.Background(), principal, command)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.afterCoordinationMutation("contract_published")
	return Response{ID: req.ID, Result: contract}
}

func (s *Server) handleContractFetch(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var params struct {
		ContractID string `json:"contract_id"`
		Revision   int64  `json:"revision,omitempty"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	contract, content, err := s.coordination.Fetch(context.Background(), principal, params.ContractID, params.Revision)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: map[string]interface{}{"contract": contract, "content": content}}
}

func (s *Server) handleContractValidate(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var command coordination.ValidateCommand
	if err := json.Unmarshal(req.Params, &command); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	contract, err := s.coordination.Validate(context.Background(), principal, command)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.afterCoordinationMutation("contract_fulfilled")
	return Response{ID: req.ID, Result: contract}
}

func (s *Server) handleContractCorrect(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var command coordination.CorrectCommand
	if err := json.Unmarshal(req.Params, &command); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	contract, err := s.coordination.Correct(context.Background(), principal, command)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.afterCoordinationMutation("contract_correction")
	return Response{ID: req.ID, Result: contract}
}

func (s *Server) handleContractWait(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var command coordination.WaitCommand
	if err := json.Unmarshal(req.Params, &command); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	contract, err := s.coordination.SetWait(context.Background(), principal, command)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.afterCoordinationMutation("contract_wait")
	return Response{ID: req.ID, Result: contract}
}

func (s *Server) handleContractCancel(req Request) Response {
	return s.handleContractTerminal(req, false)
}

func (s *Server) handleContractFail(req Request) Response {
	return s.handleContractTerminal(req, true)
}

func (s *Server) handleContractTerminal(req Request, failed bool) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var command coordination.TerminalCommand
	if err := json.Unmarshal(req.Params, &command); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	var contract *coordination.Contract
	if failed {
		contract, err = s.coordination.Fail(context.Background(), principal, command)
	} else {
		contract, err = s.coordination.Cancel(context.Background(), principal, command)
	}
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.afterCoordinationMutation("contract_terminal")
	return Response{ID: req.ID, Result: contract}
}

func (s *Server) handleContractStatus(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	var params struct {
		ContractID string `json:"contract_id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}
	contract, err := s.coordination.Get(context.Background(), principal, params.ContractID)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: contract}
}

func (s *Server) handleContractList(req Request) Response {
	principal, err := s.authenticatedCoordinationPrincipal(req)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	contracts, err := s.coordination.List(context.Background(), principal)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: map[string]interface{}{"contracts": contracts}}
}

func (s *Server) afterCoordinationMutation(reason string) {
	if s.coordinationEffectsCb != nil {
		s.coordinationEffectsCb()
	}
	if s.progressChangedCb != nil {
		go s.progressChangedCb(reason)
	}
}
