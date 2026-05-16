package dag

// NodeStatus represents the execution status of a DAG task node.
type NodeStatus byte

const (
	StatusPending    NodeStatus = 0
	StatusInProgress NodeStatus = 1
	StatusDone       NodeStatus = 2
	StatusFailed     NodeStatus = 3
	StatusBlocked    NodeStatus = 4
)

func (s NodeStatus) String() string {
	switch s {
	case StatusPending:
		return "Pending"
	case StatusInProgress:
		return "InProgress"
	case StatusDone:
		return "Done"
	case StatusFailed:
		return "Failed"
	case StatusBlocked:
		return "Blocked"
	default:
		return "Unknown"
	}
}

// IsTerminal returns true if the node is in a final state (Done or Failed).
func (s NodeStatus) IsTerminal() bool {
	return s == StatusDone || s == StatusFailed
}

// EdgeType represents the type of edge between DAG nodes.
type EdgeType byte

const (
	EdgeDependency   EdgeType = 0
	EdgeStubContract EdgeType = 1
)

func (e EdgeType) String() string {
	switch e {
	case EdgeDependency:
		return "Dependency"
	case EdgeStubContract:
		return "StubContract"
	default:
		return "Unknown"
	}
}

// DagNode represents a single task node in the DAG.
type DagNode struct {
	// ID is the immutable UUID for this node.
	ID string `json:"id"`
	// DomainID is the domain this node is assigned to (e.g., "auth", "data").
	DomainID string `json:"domain_id"`
	// TaskSpec is the JSON task specification. Cleared when status reaches Done.
	TaskSpec string `json:"task_spec,omitempty"`
	// Status is the current execution status.
	Status NodeStatus `json:"status"`
	// AssignedAgent is the UUID of the agent working on this node.
	AssignedAgent string `json:"assigned_agent,omitempty"`
	// TargetFiles lists files this task will create or modify.
	TargetFiles []string `json:"target_files,omitempty"`
	// Priority determines execution order (lower = higher priority).
	Priority int32 `json:"priority"`
	// CreatedAt is the Unix timestamp (ms) when this node was created.
	CreatedAt int64 `json:"created_at"`
	// UpdatedAt is the Unix timestamp (ms) of the last status update.
	UpdatedAt int64 `json:"updated_at"`
	// StartedAt is the Unix timestamp (ms) when the node execution began.
	StartedAt int64 `json:"started_at,omitempty"`
	// CompletedAt is the Unix timestamp (ms) when the node execution finished.
	CompletedAt int64 `json:"completed_at,omitempty"`
	// PromptTokens is the amount of tokens evaluated for the prompt.
	PromptTokens int32 `json:"prompt_tokens,omitempty"`
	// CompletionTokens is the amount of metric tokens generated.
	CompletionTokens int32 `json:"completion_tokens,omitempty"`
}

// DagEdge represents a directed edge between two DAG nodes.
type DagEdge struct {
	// FromNode is the UUID of the source node (dependency / producer).
	FromNode string `json:"from_node"`
	// ToNode is the UUID of the target node (dependent / consumer).
	ToNode string `json:"to_node"`
	// Type is the edge type.
	Type EdgeType `json:"edge_type"`
	// StubID is the stub contract UUID, for StubContract edges only.
	StubID string `json:"stub_id,omitempty"`
}

// DagHeader contains metadata about the DAG.
type DagHeader struct {
	// SchemaVersion for mmap compatibility checking.
	SchemaVersion uint32 `json:"schema_version"`
	// ActiveNodeCount is the number of nodes in non-terminal status.
	ActiveNodeCount uint32 `json:"active_node_count"`
	// MaxNodes is the ceiling for active nodes (SPLIT/INJECT rejected above this).
	MaxNodes uint32 `json:"max_nodes"`
	// LastModified is the Unix timestamp (ms) of last modification.
	LastModified int64 `json:"last_modified"`
}

// DagData is the root container for the entire DAG.
type DagData struct {
	Header DagHeader `json:"header"`
	Nodes  []DagNode `json:"nodes"`
	Edges  []DagEdge `json:"edges"`
}

// CurrentSchemaVersion is the schema version for compatibility checking.
const CurrentSchemaVersion uint32 = 1

// NewDagData creates a new empty DAG with the given max nodes ceiling.
func NewDagData(maxNodes uint32) *DagData {
	return &DagData{
		Header: DagHeader{
			SchemaVersion: CurrentSchemaVersion,
			MaxNodes:      maxNodes,
		},
		Nodes: make([]DagNode, 0),
		Edges: make([]DagEdge, 0),
	}
}
