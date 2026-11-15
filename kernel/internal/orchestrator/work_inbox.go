package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	coordination "aion-coordination"
	"aion-kernel/internal/dag"
)

type WorkItemKind string

const (
	WorkItemNode               WorkItemKind = "dag_node"
	WorkItemContractPublish    WorkItemKind = "contract_publish"
	WorkItemContractValidate   WorkItemKind = "contract_validate"
	WorkItemContractCorrection WorkItemKind = "contract_correction"
)

type WorkItemState string

const (
	WorkItemActive    WorkItemState = "active"
	WorkItemReady     WorkItemState = "ready"
	WorkItemWaiting   WorkItemState = "waiting"
	WorkItemCompleted WorkItemState = "completed"
	WorkItemFailed    WorkItemState = "failed"
)

type WorkCheckpoint struct {
	WorkItemID string    `json:"work_item_id"`
	AgentID    string    `json:"agent_id"`
	Summary    string    `json:"summary"`
	Next       string    `json:"next,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// WorkContractDependency gives an isolated consumer the durable contract
// identity and public surface it needs without exposing producer source.
type WorkContractDependency struct {
	ID                string                       `json:"id"`
	Status            coordination.Status          `json:"status"`
	Revision          int64                        `json:"revision"`
	ProducerNodeID    string                       `json:"producer_node_id"`
	ConsumerNodeID    string                       `json:"consumer_node_id"`
	Details           coordination.ContractDetails `json:"details"`
	ArtifactAvailable bool                         `json:"artifact_available"`
}

type WorkItem struct {
	ID                  string                        `json:"id"`
	Kind                WorkItemKind                  `json:"kind"`
	State               WorkItemState                 `json:"state"`
	AgentID             string                        `json:"agent_id"`
	DomainID            string                        `json:"domain_id,omitempty"`
	NodeID              string                        `json:"node_id,omitempty"`
	ContractID          string                        `json:"contract_id,omitempty"`
	Revision            int64                         `json:"revision,omitempty"`
	Title               string                        `json:"title"`
	Objective           string                        `json:"objective,omitempty"`
	Priority            int32                         `json:"priority"`
	ReadyRank           int                           `json:"ready_rank,omitempty"`
	Dependencies        []string                      `json:"dependencies,omitempty"`
	WaitingReason       string                        `json:"waiting_reason,omitempty"`
	TargetFiles         []string                      `json:"target_files,omitempty"`
	RequiredContracts   []WorkContractDependency      `json:"required_contracts,omitempty"`
	RuntimeRequirements []string                      `json:"runtime_requirements,omitempty"`
	Checkpoint          *WorkCheckpoint               `json:"checkpoint,omitempty"`
	Contract            *coordination.ContractDetails `json:"contract,omitempty"`
	NodeTaskSpec        string                        `json:"node_task_spec,omitempty"`
	CreatedAt           time.Time                     `json:"created_at,omitempty"`
	UpdatedAt           time.Time                     `json:"updated_at,omitempty"`
}

type WorkInbox struct {
	AgentID           string     `json:"agent_id"`
	Active            *WorkItem  `json:"active,omitempty"`
	Ready             []WorkItem `json:"ready"`
	Waiting           []WorkItem `json:"waiting"`
	RecentlyCompleted []WorkItem `json:"recently_completed,omitempty"`
	GeneratedAt       time.Time  `json:"generated_at"`
}

type WorkQueuePosition struct {
	State      WorkItemState `json:"state"`
	ReadyAhead int           `json:"ready_ahead,omitempty"`
	Reason     string        `json:"reason,omitempty"`
}

// WorkInboxService projects authoritative DAG and coordination state into an
// agent-oriented work view. Only agent-authored checkpoints are persisted.
type WorkInboxService struct {
	mu              sync.Mutex
	dagManager      *dag.Manager
	coordination    *coordination.Manager
	checkpointPath  string
	checkpoints     map[string]WorkCheckpoint
	checkpointsRead bool
}

func NewWorkInboxService(dagManager *dag.Manager, manager *coordination.Manager, checkpointPath string) *WorkInboxService {
	return &WorkInboxService{
		dagManager:     dagManager,
		coordination:   manager,
		checkpointPath: strings.TrimSpace(checkpointPath),
		checkpoints:    make(map[string]WorkCheckpoint),
	}
}

func (s *WorkInboxService) SetRuntime(dagManager *dag.Manager, manager *coordination.Manager, checkpointPath string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dagManager = dagManager
	s.coordination = manager
	s.checkpointPath = strings.TrimSpace(checkpointPath)
	s.checkpoints = make(map[string]WorkCheckpoint)
	s.checkpointsRead = false
}

func (s *WorkInboxService) Inbox(ctx context.Context, principal coordination.Principal) (*WorkInbox, error) {
	if s == nil || s.dagManager == nil || s.coordination == nil {
		return nil, fmt.Errorf("work inbox is unavailable")
	}
	if principal.Role != coordination.RoleAgent || strings.TrimSpace(principal.AgentID) == "" {
		return nil, fmt.Errorf("work inbox requires an authenticated agent")
	}

	snapshot := s.dagManager.Snapshot()
	contracts, err := s.coordination.List(ctx, principal)
	if err != nil {
		return nil, err
	}
	checkpoints, err := s.loadCheckpoints()
	if err != nil {
		return nil, err
	}

	inbox := &WorkInbox{AgentID: principal.AgentID, GeneratedAt: time.Now().UTC()}
	nodes := make(map[string]dag.DagNode, len(snapshot.Nodes))
	dependencies := make(map[string][]string)
	for _, node := range snapshot.Nodes {
		nodes[node.ID] = node
	}
	for _, edge := range snapshot.Edges {
		dependencies[edge.ToNode] = append(dependencies[edge.ToNode], edge.FromNode)
	}

	for _, node := range snapshot.Nodes {
		agentID := expectedAgentForNode(node.DomainID, node.AssignedAgent)
		if agentID != principal.AgentID {
			continue
		}
		item := workItemForNode(node, dependencies[node.ID], nodes)
		if node.Status == dag.StatusPending && strings.TrimSpace(node.AssignedAgent) == "" && item.State == WorkItemReady {
			item.State = WorkItemWaiting
			item.WaitingReason = "awaiting allocator dispatch"
		}
		attachNodeRuntimeContext(&item, contracts)
		attachCheckpoint(&item, checkpoints)
		switch item.State {
		case WorkItemActive:
			if inbox.Active == nil || node.Priority < inbox.Active.Priority {
				if inbox.Active != nil {
					prior := *inbox.Active
					prior.State = WorkItemReady
					inbox.Ready = append(inbox.Ready, prior)
				}
				copy := item
				inbox.Active = &copy
			} else {
				item.State = WorkItemReady
				inbox.Ready = append(inbox.Ready, item)
			}
		case WorkItemReady:
			inbox.Ready = append(inbox.Ready, item)
		case WorkItemWaiting:
			inbox.Waiting = append(inbox.Waiting, item)
		default:
			inbox.RecentlyCompleted = append(inbox.RecentlyCompleted, item)
		}
	}

	for _, contract := range contracts {
		item, visible := workItemForContract(contract, principal.AgentID, nodes)
		if !visible {
			continue
		}
		attachCheckpoint(&item, checkpoints)
		switch item.State {
		case WorkItemReady:
			inbox.Ready = append(inbox.Ready, item)
		case WorkItemWaiting:
			inbox.Waiting = append(inbox.Waiting, item)
		default:
			inbox.RecentlyCompleted = append(inbox.RecentlyCompleted, item)
		}
	}

	sort.SliceStable(inbox.Ready, func(i, j int) bool {
		left, right := workItemOrder(inbox.Ready[i]), workItemOrder(inbox.Ready[j])
		if left != right {
			return left < right
		}
		if inbox.Ready[i].Priority != inbox.Ready[j].Priority {
			return inbox.Ready[i].Priority < inbox.Ready[j].Priority
		}
		if !inbox.Ready[i].CreatedAt.Equal(inbox.Ready[j].CreatedAt) {
			return inbox.Ready[i].CreatedAt.Before(inbox.Ready[j].CreatedAt)
		}
		return inbox.Ready[i].ID < inbox.Ready[j].ID
	})
	for index := range inbox.Ready {
		inbox.Ready[index].ReadyRank = index + 1
	}
	sort.SliceStable(inbox.Waiting, func(i, j int) bool { return inbox.Waiting[i].ID < inbox.Waiting[j].ID })
	sort.SliceStable(inbox.RecentlyCompleted, func(i, j int) bool {
		return inbox.RecentlyCompleted[i].UpdatedAt.After(inbox.RecentlyCompleted[j].UpdatedAt)
	})
	if len(inbox.RecentlyCompleted) > 10 {
		inbox.RecentlyCompleted = inbox.RecentlyCompleted[:10]
	}
	return inbox, nil
}

func (s *WorkInboxService) Checkpoint(ctx context.Context, principal coordination.Principal, workItemID, summary, next string) (*WorkCheckpoint, error) {
	workItemID = strings.TrimSpace(workItemID)
	summary = strings.TrimSpace(summary)
	next = strings.TrimSpace(next)
	if workItemID == "" || summary == "" {
		return nil, fmt.Errorf("work checkpoint requires work item and summary")
	}
	inbox, err := s.Inbox(ctx, principal)
	if err != nil {
		return nil, err
	}
	if !inboxContains(inbox, workItemID) {
		return nil, fmt.Errorf("work item %s is not visible to agent %s", workItemID, principal.AgentID)
	}
	checkpoint := WorkCheckpoint{
		WorkItemID: workItemID,
		AgentID:    principal.AgentID,
		Summary:    summary,
		Next:       next,
		UpdatedAt:  time.Now().UTC(),
	}
	if err := s.saveCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func (s *WorkInboxService) QueuePosition(ctx context.Context, principal coordination.Principal, workItemID string) (WorkQueuePosition, error) {
	inbox, err := s.Inbox(ctx, principal)
	if err != nil {
		return WorkQueuePosition{}, err
	}
	if inbox.Active != nil && inbox.Active.ID == workItemID {
		return WorkQueuePosition{State: WorkItemActive}, nil
	}
	for index, item := range inbox.Ready {
		if item.ID == workItemID {
			return WorkQueuePosition{State: WorkItemReady, ReadyAhead: index}, nil
		}
	}
	for _, item := range inbox.Waiting {
		if item.ID == workItemID {
			return WorkQueuePosition{State: WorkItemWaiting, Reason: item.WaitingReason}, nil
		}
	}
	return WorkQueuePosition{State: WorkItemCompleted}, nil
}

func (s *WorkInboxService) HasOpenCoordination(ctx context.Context) (bool, error) {
	if s == nil || s.coordination == nil {
		return false, nil
	}
	contracts, err := s.coordination.List(ctx, coordination.Principal{Role: coordination.RoleAdmin})
	if err != nil {
		return false, err
	}
	for _, contract := range contracts {
		if !contract.Status.Terminal() {
			return true, nil
		}
	}
	return false, nil
}

func workItemForNode(node dag.DagNode, dependencyIDs []string, nodes map[string]dag.DagNode) WorkItem {
	item := WorkItem{
		ID:           node.ID,
		Kind:         WorkItemNode,
		AgentID:      expectedAgentForNode(node.DomainID, node.AssignedAgent),
		DomainID:     node.DomainID,
		NodeID:       node.ID,
		Title:        "Implement DAG node " + node.ID,
		Objective:    node.TaskSpec,
		Priority:     node.Priority,
		Dependencies: append([]string(nil), dependencyIDs...),
		TargetFiles:  append([]string(nil), node.TargetFiles...),
		CreatedAt:    time.UnixMilli(node.CreatedAt).UTC(),
		UpdatedAt:    time.UnixMilli(node.UpdatedAt).UTC(),
	}
	switch node.Status {
	case dag.StatusInProgress:
		item.State = WorkItemActive
	case dag.StatusPending:
		var blocked []string
		for _, dependencyID := range dependencyIDs {
			dependency, ok := nodes[dependencyID]
			if !ok || !dependency.Status.IsTerminal() {
				blocked = append(blocked, dependencyID)
			}
		}
		if len(blocked) == 0 {
			item.State = WorkItemReady
		} else {
			item.State = WorkItemWaiting
			item.WaitingReason = "waiting on DAG dependencies: " + strings.Join(blocked, ", ")
		}
	case dag.StatusBlocked:
		item.State = WorkItemWaiting
		item.WaitingReason = "node is blocked"
	case dag.StatusDone:
		item.State = WorkItemCompleted
	case dag.StatusFailed:
		item.State = WorkItemFailed
	}
	return item
}

func attachNodeRuntimeContext(item *WorkItem, contracts []coordination.Contract) {
	if item == nil || item.Kind != WorkItemNode {
		return
	}
	for _, contract := range contracts {
		if contract.ConsumerNodeID != item.NodeID {
			continue
		}
		item.RequiredContracts = append(item.RequiredContracts, WorkContractDependency{
			ID:                contract.ID,
			Status:            contract.Status,
			Revision:          contract.Revision,
			ProducerNodeID:    contract.ProducerNodeID,
			ConsumerNodeID:    contract.ConsumerNodeID,
			Details:           contract.Details,
			ArtifactAvailable: contract.Artifact != nil,
		})
	}
	sort.SliceStable(item.RequiredContracts, func(i, j int) bool {
		return item.RequiredContracts[i].ID < item.RequiredContracts[j].ID
	})

	contractIDs := make([]string, 0, len(item.RequiredContracts))
	for _, dependency := range item.RequiredContracts {
		contractIDs = append(contractIDs, dependency.ID)
		entrypoint := strings.TrimSpace(dependency.Details.PublicEntrypoint)
		if dependency.Status == coordination.StatusFulfilled {
			item.RuntimeRequirements = append(item.RuntimeRequirements, fmt.Sprintf(
				"Contract %s is Fulfilled at revision %d; use public entrypoint %q. Producer source is intentionally absent from this isolated workspace. Do not call contract-ensure again or search for peer files; use a test-local import mock for unit tests and the assembled integration run for the real artifact.",
				dependency.ID, dependency.Revision, entrypoint,
			))
		} else {
			item.RuntimeRequirements = append(item.RuntimeRequirements, fmt.Sprintf(
				"Contract %s is currently %s; follow its lifecycle through the work inbox and do not invent or inspect the producer implementation.",
				dependency.ID, dependency.Status,
			))
		}
	}

	for _, target := range item.TargetFiles {
		if !isIntegrationDirectiveTarget(target) {
			continue
		}
		required := "[]"
		if data, err := json.Marshal(contractIDs); err == nil {
			required = string(data)
		}
		item.RuntimeRequirements = append(item.RuntimeRequirements, fmt.Sprintf(
			"%s is a reserved schema-v1 kernel execution manifest, not application input or configuration. Write exactly one JSON object with schema_version, id, command (absolute executable array), required_contracts=%s, owner_node_ids=[%q], and timeout_sec. Omit working_directory for the assembled workspace root. The command must verify this node against the real assembled contract artifacts.",
			target, required, item.NodeID,
		))
	}
}

func isIntegrationDirectiveTarget(path string) bool {
	path = strings.ReplaceAll(strings.TrimSpace(path), "\\", "/")
	return strings.HasSuffix(path, ".json") && strings.Contains("/"+strings.TrimPrefix(path, "/"), "/integration/directives/")
}

func workItemForContract(contract coordination.Contract, agentID string, nodes map[string]dag.DagNode) (WorkItem, bool) {
	details := contract.Details
	item := WorkItem{
		ContractID: contract.ID,
		Revision:   contract.Revision,
		Title:      contract.Details.Name,
		Objective:  strings.Join(contract.Details.Acceptance, "; "),
		Contract:   &details,
		CreatedAt:  contract.CreatedAt,
		UpdatedAt:  contract.UpdatedAt,
	}
	if node, ok := nodes[contract.ProducerNodeID]; ok {
		item.Priority = node.Priority
		item.DomainID = node.DomainID
	}

	switch agentID {
	case contract.ProducerAgentID:
		item.AgentID = agentID
		item.NodeID = contract.ProducerNodeID
		switch contract.Status {
		case coordination.StatusRequested:
			item.ID = contractWorkItemID(contract.ID, WorkItemContractPublish)
			item.Kind = WorkItemContractPublish
			item.State = WorkItemReady
			item.Title = "Publish contract " + contract.Details.Name
		case coordination.StatusCorrectionRequested:
			item.ID = contractWorkItemID(contract.ID, WorkItemContractCorrection)
			item.Kind = WorkItemContractCorrection
			item.State = WorkItemReady
			item.Title = "Correct contract " + contract.Details.Name
			item.Objective = contract.CorrectionReason
		case coordination.StatusProduced:
			item.ID = contractWorkItemID(contract.ID, WorkItemContractPublish)
			item.Kind = WorkItemContractPublish
			item.State = WorkItemWaiting
			item.WaitingReason = "waiting for consumer validation"
		default:
			item.ID = contractWorkItemID(contract.ID, WorkItemContractPublish)
			item.Kind = WorkItemContractPublish
			if contract.Status == coordination.StatusFulfilled {
				item.State = WorkItemCompleted
			} else {
				item.State = WorkItemFailed
				item.WaitingReason = contract.TerminalReason
			}
		}
		return item, true
	case contract.ConsumerAgentID:
		item.AgentID = agentID
		item.NodeID = contract.ConsumerNodeID
		if node, ok := nodes[contract.ConsumerNodeID]; ok {
			item.Priority = node.Priority
			item.DomainID = node.DomainID
			item.NodeTaskSpec = node.TaskSpec
		}
		switch contract.Status {
		case coordination.StatusProduced:
			item.ID = contractWorkItemID(contract.ID, WorkItemContractValidate)
			item.Kind = WorkItemContractValidate
			item.State = WorkItemReady
			item.Title = "Validate contract " + contract.Details.Name
		case coordination.StatusRequested, coordination.StatusCorrectionRequested:
			item.ID = contractWorkItemID(contract.ID, WorkItemContractValidate)
			item.Kind = WorkItemContractValidate
			item.State = WorkItemWaiting
			item.WaitingReason = "waiting for producer artifact"
		default:
			item.ID = contractWorkItemID(contract.ID, WorkItemContractValidate)
			item.Kind = WorkItemContractValidate
			if contract.Status == coordination.StatusFulfilled {
				item.State = WorkItemCompleted
			} else {
				item.State = WorkItemFailed
				item.WaitingReason = contract.TerminalReason
			}
		}
		return item, true
	default:
		return WorkItem{}, false
	}
}

func contractWorkItemID(contractID string, kind WorkItemKind) string {
	return "contract:" + contractID + ":" + string(kind)
}

func workItemOrder(item WorkItem) int {
	switch item.Kind {
	case WorkItemContractCorrection:
		return 0
	case WorkItemContractValidate:
		return 1
	case WorkItemContractPublish:
		return 2
	default:
		return 3
	}
}

func attachCheckpoint(item *WorkItem, checkpoints map[string]WorkCheckpoint) {
	if checkpoint, ok := checkpoints[item.ID]; ok {
		copy := checkpoint
		item.Checkpoint = &copy
	}
}

func inboxContains(inbox *WorkInbox, id string) bool {
	if inbox == nil {
		return false
	}
	if inbox.Active != nil && inbox.Active.ID == id {
		return true
	}
	for _, items := range [][]WorkItem{inbox.Ready, inbox.Waiting} {
		for _, item := range items {
			if item.ID == id {
				return true
			}
		}
	}
	return false
}

func (s *WorkInboxService) loadCheckpoints() (map[string]WorkCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.checkpointsRead {
		s.checkpointsRead = true
		if s.checkpointPath != "" {
			data, err := os.ReadFile(s.checkpointPath)
			if err != nil && !os.IsNotExist(err) {
				return nil, err
			}
			if len(data) > 0 {
				if err := json.Unmarshal(data, &s.checkpoints); err != nil {
					return nil, fmt.Errorf("decode work checkpoints: %w", err)
				}
			}
		}
	}
	result := make(map[string]WorkCheckpoint, len(s.checkpoints))
	for id, checkpoint := range s.checkpoints {
		result[id] = checkpoint
	}
	return result, nil
}

func (s *WorkInboxService) saveCheckpoint(checkpoint WorkCheckpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.checkpointsRead {
		if s.checkpointPath != "" {
			data, err := os.ReadFile(s.checkpointPath)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			if len(data) > 0 {
				if err := json.Unmarshal(data, &s.checkpoints); err != nil {
					return fmt.Errorf("decode work checkpoints: %w", err)
				}
			}
		}
		s.checkpointsRead = true
	}
	s.checkpoints[checkpoint.WorkItemID] = checkpoint
	if s.checkpointPath == "" {
		return nil
	}
	data, err := json.MarshalIndent(s.checkpoints, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.checkpointPath, append(data, '\n'))
}
