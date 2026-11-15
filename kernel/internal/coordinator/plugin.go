package coordinator

import (
	"context"
)

// Coordinator plans and replans work distribution across domain agents.
type Coordinator interface {
	// Plan takes a user goal + project context and produces a DAG of tasks
	// with domain assignments.
	Plan(ctx context.Context, req PlanRequest) (*PlanResponse, error)

	// Replan re-evaluates a subgraph when too many dependency injections
	// indicate the original plan was incomplete.
	Replan(ctx context.Context, req ReplanRequest) (*ReplanResponse, error)

	// Refine delivers a user message to the persistent Architect session.
	Refine(ctx context.Context, text string) error

	// GetArchitectAgent returns the underlying agent supervisor for the architect phase.
	GetArchitectAgent() interface{}
}

// ResolutionCoordinator is deliberately separate from Coordinator. Planning
// and architect implementations do not gain access to reconciliation unless
// they explicitly implement this sealed-artifact capability.
type ResolutionCoordinator interface {
	ResolveContract(ctx context.Context, req ResolutionRequest) (*ResolutionResult, error)
}

// BuildReviewer is the final acceptance phase of the same coordinator role. It
// reviews a converged build against the accepted spec and execution evidence.
// It is optional so simple coordinators and tests do not need to implement it.
type BuildReviewer interface {
	ReviewBuild(ctx context.Context, req BuildReviewRequest) (*BuildReviewResult, error)
}

type BuildReviewRequest struct {
	AttemptID       string `json:"attempt_id"`
	ArtifactDir     string `json:"artifact_dir"`
	SandboxDir      string `json:"sandbox_dir"`
	SessionDir      string `json:"session_dir"`
	Repair          bool   `json:"repair,omitempty"`
	ValidationError string `json:"validation_error,omitempty"`
	Progress        func() `json:"-"`
}

type BuildReviewResult struct {
	OutputPath string `json:"output_path"`
}

type ResolutionRequest struct {
	ResolutionID    string `json:"resolution_id"`
	ArtifactDir     string `json:"artifact_dir"`
	SessionDir      string `json:"session_dir"`
	Repair          bool   `json:"repair,omitempty"`
	ValidationError string `json:"validation_error,omitempty"`
	Progress        func() `json:"-"`
}

type ResolutionResult struct {
	OutputPath string `json:"output_path"`
}

// PlanRequest contains everything the Coordinator needs to produce a plan.
type PlanRequest struct {
	// AttemptID scopes planner session and artifacts to one build-spec attempt.
	AttemptID string `json:"attempt_id,omitempty"`
	// UserPrompt is the high-level user goal.
	UserPrompt string `json:"user_prompt"`
	// ProjectRoot is the absolute path to the project.
	ProjectRoot string `json:"project_root"`
	// ProjectScan contains the scanned project structure.
	ProjectScan *ProjectScan `json:"project_scan"`
	// Constraints are optional planning constraints.
	Constraints []string `json:"constraints,omitempty"`
}

// PlanResponse is the output of the Coordinator's planning phase.
type PlanResponse struct {
	// Domains are the identified domain areas.
	Domains []Domain `json:"domains"`
	// Nodes are the task nodes for the DAG.
	Nodes []TaskNode `json:"nodes"`
	// Edges are the dependency edges.
	Edges []TaskEdge `json:"edges"`
	// Contracts are durable cross-domain artifacts required by planned nodes.
	Contracts []PlannedContract `json:"contracts,omitempty"`
	// Milestones are optional client-facing capability groupings for progress.
	Milestones []Milestone `json:"milestones,omitempty"`
}

// PlannedContract declares one producer/consumer artifact before agents run.
type PlannedContract struct {
	Resource             string   `json:"resource"`
	ProducerNode         string   `json:"producer_node"`
	ConsumerNode         string   `json:"consumer_node"`
	Kind                 string   `json:"kind,omitempty"`
	Language             string   `json:"language,omitempty"`
	PublicEntrypoint     string   `json:"public_entrypoint,omitempty"`
	Acceptance           []string `json:"acceptance,omitempty"`
	HardWait             bool     `json:"hard_wait,omitempty"`
	HandshakeRequired    bool     `json:"handshake_required,omitempty"`
	SupersedesContractID string   `json:"supersedes_contract_id,omitempty"`
}

// Domain describes an area of responsibility.
type Domain struct {
	DomainID      string   `json:"domain_id"`
	Description   string   `json:"description"`
	AssignedPaths []string `json:"assigned_paths"`
	AgentType     string   `json:"agent_type"` // "domain" or "utility"
}

// TaskNode is a planned task for the DAG.
type TaskNode struct {
	ID                   string   `json:"id"`
	DomainID             string   `json:"domain_id"`
	TaskSpec             string   `json:"task_spec"`
	TargetFiles          []string `json:"target_files"`
	Priority             int32    `json:"priority"`
	OriginNodeID         string   `json:"origin_node_id,omitempty"`
	SupersedesContractID string   `json:"supersedes_contract_id,omitempty"`
}

// TaskEdge is a planned dependency edge.
type TaskEdge struct {
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
	Reason   string `json:"reason,omitempty"`
}

// Milestone groups planned nodes into a client-facing capability area.
type Milestone struct {
	ID              string   `json:"id"`
	Title           string   `json:"title"`
	Summary         string   `json:"summary,omitempty"`
	NodeIDs         []string `json:"node_ids"`
	Weight          int      `json:"weight,omitempty"`
	SuccessCriteria []string `json:"success_criteria,omitempty"`
}

// ReplanRequest contains the context needed for replanning.
type ReplanRequest struct {
	// CurrentDAG is the current state of the DAG.
	CurrentNodes []TaskNode `json:"current_nodes"`
	CurrentEdges []TaskEdge `json:"current_edges"`
	// InjectHistory records the dependency injections that triggered replan.
	InjectHistory []TaskEdge `json:"inject_history"`
	// AffectedNodes are the node IDs in the subgraph being replanned.
	AffectedNodes []string `json:"affected_nodes"`
}

// ReplanResponse contains the modifications to apply.
type ReplanResponse struct {
	// ModifiedNodes are nodes with updated task specs or priorities.
	ModifiedNodes []TaskNode `json:"modified_nodes"`
	// NewEdges are additional edges discovered during replanning.
	NewEdges []TaskEdge `json:"new_edges"`
	// DomainReassignments move tasks between domains.
	DomainReassignments map[string]string `json:"domain_reassignments,omitempty"`
}
