package dag

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ManagerConfig configures the DAG Manager.
type ManagerConfig struct {
	// StoreFilePath is the path to the mmap-backed DAG file.
	StoreFilePath string
	// WalFilePath is the path to the WAL file.
	WalFilePath string
	// StoreSize is the mmap region size in bytes.
	StoreSize int
	// MaxNodes is the ceiling for active nodes.
	MaxNodes uint32
	// FlushDeadline is the maximum time between WAL flushes.
	FlushDeadline time.Duration
}

// Manager provides the high-level API for DAG operations.
// It combines the Store (mmap), WAL, and Validator into a single interface
// that ensures every mutation is validated, logged, and persisted.
type Manager struct {
	mu     sync.Mutex
	store  *Store
	wal    *WAL
	config ManagerConfig
	dag    *DagData // cached in-memory copy for fast reads
}

// NewManager creates a DAG Manager. If a WAL file exists with entries,
// it replays them to rebuild state.
func NewManager(config ManagerConfig) (*Manager, error) {
	if config.StoreSize <= 0 {
		config.StoreSize = DefaultStoreSize
	}
	if config.MaxNodes <= 0 {
		config.MaxNodes = 200
	}
	if config.FlushDeadline <= 0 {
		config.FlushDeadline = 50 * time.Millisecond
	}

	store, err := NewStore(config.StoreFilePath, config.StoreSize, true)
	if err != nil {
		return nil, fmt.Errorf("manager: create store: %w", err)
	}

	wal, err := NewWAL(config.WalFilePath, config.FlushDeadline)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("manager: create wal: %w", err)
	}

	m := &Manager{
		store:  store,
		wal:    wal,
		config: config,
		dag:    NewDagData(config.MaxNodes),
	}

	// Try to read existing store data
	existing, _, err := store.Read()
	if err == nil && len(existing.Nodes) > 0 {
		m.dag = existing
	}

	// Replay WAL to apply any mutations that weren't in the store
	if err := m.replayWAL(); err != nil {
		store.Close()
		wal.Close()
		return nil, fmt.Errorf("manager: replay wal: %w", err)
	}

	return m, nil
}

func (m *Manager) replayWAL() error {
	entries, err := m.wal.Replay()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := m.applyMutation(entry); err != nil {
			return fmt.Errorf("replay entry %s: %w", entry.Type, err)
		}
	}

	// Persist replayed state to store
	if len(entries) > 0 {
		if err := m.store.Write(m.dag); err != nil {
			return err
		}
		return m.wal.Truncate()
	}
	return nil
}

func (m *Manager) applyMutation(entry WalEntry) error {
	switch entry.Type {
	case MutAddNode:
		var node DagNode
		if err := json.Unmarshal(entry.Payload, &node); err != nil {
			return err
		}
		m.dag.Nodes = append(m.dag.Nodes, node)
		m.updateActiveCount()

	case MutUpdateNode:
		var update struct {
			ID      string       `json:"id"`
			Status  NodeStatus   `json:"status"`
			Metrics *NodeMetrics `json:"metrics,omitempty"`
		}
		if err := json.Unmarshal(entry.Payload, &update); err != nil {
			return err
		}
		for i := range m.dag.Nodes {
			if m.dag.Nodes[i].ID == update.ID {
				m.dag.Nodes[i].Status = update.Status
				m.dag.Nodes[i].UpdatedAt = entry.Timestamp
				if update.Status == StatusDone {
					m.dag.Nodes[i].TaskSpec = "" // growth control
				}
				if update.Metrics != nil {
					m.dag.Nodes[i].StartedAt = update.Metrics.StartedAt
					m.dag.Nodes[i].CompletedAt = update.Metrics.CompletedAt
					m.dag.Nodes[i].PromptTokens = update.Metrics.PromptTokens
					m.dag.Nodes[i].CompletionTokens = update.Metrics.CompletionTokens
				}
				break
			}
		}
		m.updateActiveCount()

	case MutAddEdge:
		var edge DagEdge
		if err := json.Unmarshal(entry.Payload, &edge); err != nil {
			return err
		}
		m.dag.Edges = append(m.dag.Edges, edge)

	case MutRemoveEdge:
		var remove struct {
			FromNode string `json:"from_node"`
			ToNode   string `json:"to_node"`
		}
		if err := json.Unmarshal(entry.Payload, &remove); err != nil {
			return err
		}
		filtered := m.dag.Edges[:0]
		for _, e := range m.dag.Edges {
			if e.FromNode != remove.FromNode || e.ToNode != remove.ToNode {
				filtered = append(filtered, e)
			}
		}
		m.dag.Edges = filtered

	case MutSplitNode:
		var split struct {
			ParentID string    `json:"parent_id"`
			SubNodes []DagNode `json:"sub_nodes"`
		}
		if err := json.Unmarshal(entry.Payload, &split); err != nil {
			return err
		}
		// Mark parent as Done and add sub-nodes
		for i := range m.dag.Nodes {
			if m.dag.Nodes[i].ID == split.ParentID {
				m.dag.Nodes[i].Status = StatusDone
				m.dag.Nodes[i].TaskSpec = ""
				break
			}
		}
		m.dag.Nodes = append(m.dag.Nodes, split.SubNodes...)
		m.updateActiveCount()

	case MutBulkLoad:
		var dag DagData
		if err := json.Unmarshal(entry.Payload, &dag); err != nil {
			return err
		}
		m.dag = &dag

	case MutAssignNode:
		var assign struct {
			ID      string `json:"id"`
			AgentID string `json:"agent_id"`
		}
		if err := json.Unmarshal(entry.Payload, &assign); err != nil {
			return err
		}
		for i := range m.dag.Nodes {
			if m.dag.Nodes[i].ID == assign.ID {
				m.dag.Nodes[i].AssignedAgent = assign.AgentID
				break
			}
		}
	}

	m.dag.Header.LastModified = entry.Timestamp
	return nil
}

func (m *Manager) updateActiveCount() {
	var count uint32
	for _, n := range m.dag.Nodes {
		if !n.Status.IsTerminal() {
			count++
		}
	}
	m.dag.Header.ActiveNodeCount = count
}

// AddNode adds a node to the DAG. Validates assignments and persists via WAL.
func (m *Manager) AddNode(node DagNode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node.CreatedAt == 0 {
		node.CreatedAt = time.Now().UnixMilli()
	}
	node.UpdatedAt = node.CreatedAt

	// Validate ceiling
	testNodes := append(m.dag.Nodes, node)
	if err := ValidateNodeCeiling(testNodes, m.dag.Header.MaxNodes); err != nil {
		return err
	}

	// Validate assignments
	if err := ValidateAssignments(testNodes); err != nil {
		return err
	}

	payload, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("manager: marshal node: %w", err)
	}

	if err := m.wal.AppendSync(WalEntry{Type: MutAddNode, Payload: payload}); err != nil {
		return fmt.Errorf("manager: wal append: %w", err)
	}

	m.dag.Nodes = append(m.dag.Nodes, node)
	m.updateActiveCount()
	m.dag.Header.LastModified = time.Now().UnixMilli()

	// Defer store write; rely on WAL for durability
	return nil
}

// NodeMetrics holds execution telemetry.
type NodeMetrics struct {
	StartedAt        int64 `json:"started_at"`
	CompletedAt      int64 `json:"completed_at"`
	PromptTokens     int32 `json:"prompt_tokens"`
	CompletionTokens int32 `json:"completion_tokens"`
}

// UpdateNode updates a node's status. Nullifies TaskSpec on Done (growth control).
func (m *Manager) UpdateNode(nodeID string, status NodeStatus) error {
	return m.UpdateNodeWithMetrics(nodeID, status, nil)
}

// UpdateNodeWithMetrics updates a node's status and records execution metrics.
func (m *Manager) UpdateNodeWithMetrics(nodeID string, status NodeStatus, metrics *NodeMetrics) error {
	return m.updateNodeWithMetrics(nodeID, "", status, metrics, false)
}

// UpdateNodeForAgent updates a node only when it is assigned to agentID.
func (m *Manager) UpdateNodeForAgent(nodeID, agentID string, status NodeStatus, metrics *NodeMetrics) error {
	return m.updateNodeWithMetrics(nodeID, agentID, status, metrics, true)
}

func (m *Manager) updateNodeWithMetrics(nodeID, agentID string, status NodeStatus, metrics *NodeMetrics, enforceOwner bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the node
	idx := -1
	for i := range m.dag.Nodes {
		if m.dag.Nodes[i].ID == nodeID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("manager: node '%s' not found", nodeID)
	}
	if enforceOwner {
		agentID = strings.TrimSpace(agentID)
		assignedAgent := strings.TrimSpace(m.dag.Nodes[idx].AssignedAgent)
		if agentID == "" {
			return fmt.Errorf("manager: update node '%s' requires an agent ID", nodeID)
		}
		if assignedAgent == "" {
			return fmt.Errorf("manager: node '%s' has no assigned agent", nodeID)
		}
		if assignedAgent != agentID {
			return fmt.Errorf("manager: agent '%s' cannot update node '%s'; assigned to '%s'", agentID, nodeID, assignedAgent)
		}
	}

	now := time.Now().UnixMilli()

	update := struct {
		ID      string       `json:"id"`
		Status  NodeStatus   `json:"status"`
		Metrics *NodeMetrics `json:"metrics,omitempty"`
	}{ID: nodeID, Status: status, Metrics: metrics}

	payload, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("manager: marshal update: %w", err)
	}

	if err := m.wal.AppendSync(WalEntry{Type: MutUpdateNode, Payload: payload}); err != nil {
		return fmt.Errorf("manager: wal append: %w", err)
	}

	m.dag.Nodes[idx].Status = status
	m.dag.Nodes[idx].UpdatedAt = now
	if status == StatusDone {
		m.dag.Nodes[idx].TaskSpec = ""
	}
	if metrics != nil {
		m.dag.Nodes[idx].StartedAt = metrics.StartedAt
		m.dag.Nodes[idx].CompletedAt = metrics.CompletedAt
		m.dag.Nodes[idx].PromptTokens = metrics.PromptTokens
		m.dag.Nodes[idx].CompletionTokens = metrics.CompletionTokens
	}
	m.updateActiveCount()
	m.dag.Header.LastModified = now

	return nil
}

// AssignNode binds an agent UUID to a specific Node.
func (m *Manager) AssignNode(nodeID string, agentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i := range m.dag.Nodes {
		if m.dag.Nodes[i].ID == nodeID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("manager: node '%s' not found", nodeID)
	}

	now := time.Now().UnixMilli()
	assign := struct {
		ID      string `json:"id"`
		AgentID string `json:"agent_id"`
	}{ID: nodeID, AgentID: agentID}

	payload, err := json.Marshal(assign)
	if err != nil {
		return fmt.Errorf("manager: marshal assign: %w", err)
	}

	if err := m.wal.AppendSync(WalEntry{Type: MutAssignNode, Payload: payload}); err != nil {
		return fmt.Errorf("manager: wal append: %w", err)
	}

	m.dag.Nodes[idx].AssignedAgent = agentID
	m.dag.Nodes[idx].UpdatedAt = now
	m.dag.Header.LastModified = now

	return nil
}

// AddEdge adds a directed edge. Validates that no cycle is created.
func (m *Manager) AddEdge(edge DagEdge) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate no cycles
	testEdges := append(m.dag.Edges, edge)
	if err := ValidateCycles(m.dag.Nodes, testEdges); err != nil {
		return err
	}

	payload, err := json.Marshal(edge)
	if err != nil {
		return fmt.Errorf("manager: marshal edge: %w", err)
	}

	if err := m.wal.AppendSync(WalEntry{Type: MutAddEdge, Payload: payload}); err != nil {
		return fmt.Errorf("manager: wal append: %w", err)
	}

	m.dag.Edges = append(m.dag.Edges, edge)
	m.dag.Header.LastModified = time.Now().UnixMilli()

	return nil
}

// SplitNode replaces a node with sub-nodes. Validates ceiling.
func (m *Manager) SplitNode(parentID string, subNodes []DagNode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find parent
	found := false
	for _, n := range m.dag.Nodes {
		if n.ID == parentID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("manager: parent node '%s' not found", parentID)
	}

	// Validate ceiling: parent becomes Done (terminal) but sub-nodes are active
	testNodes := make([]DagNode, len(m.dag.Nodes))
	copy(testNodes, m.dag.Nodes)
	for i := range testNodes {
		if testNodes[i].ID == parentID {
			testNodes[i].Status = StatusDone
		}
	}
	testNodes = append(testNodes, subNodes...)
	if err := ValidateNodeCeiling(testNodes, m.dag.Header.MaxNodes); err != nil {
		return err
	}

	now := time.Now().UnixMilli()
	for i := range subNodes {
		if subNodes[i].CreatedAt == 0 {
			subNodes[i].CreatedAt = now
		}
		subNodes[i].UpdatedAt = now
	}

	split := struct {
		ParentID string    `json:"parent_id"`
		SubNodes []DagNode `json:"sub_nodes"`
	}{ParentID: parentID, SubNodes: subNodes}

	payload, err := json.Marshal(split)
	if err != nil {
		return fmt.Errorf("manager: marshal split: %w", err)
	}

	if err := m.wal.AppendSync(WalEntry{Type: MutSplitNode, Payload: payload}); err != nil {
		return fmt.Errorf("manager: wal append: %w", err)
	}

	// Apply: mark parent Done, add sub-nodes
	for i := range m.dag.Nodes {
		if m.dag.Nodes[i].ID == parentID {
			m.dag.Nodes[i].Status = StatusDone
			m.dag.Nodes[i].TaskSpec = ""
			break
		}
	}
	m.dag.Nodes = append(m.dag.Nodes, subNodes...)
	m.updateActiveCount()
	m.dag.Header.LastModified = now

	return nil
}

// BulkLoad replaces the entire DAG state. Used for initial Coordinator output.
func (m *Manager) BulkLoad(dag *DagData) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ValidateAll(dag); err != nil {
		return fmt.Errorf("manager: validate bulk load: %w", err)
	}

	dag.Header.MaxNodes = m.config.MaxNodes
	dag.Header.SchemaVersion = CurrentSchemaVersion
	dag.Header.LastModified = time.Now().UnixMilli()

	payload, err := json.Marshal(dag)
	if err != nil {
		return fmt.Errorf("manager: marshal bulk load: %w", err)
	}

	if err := m.wal.AppendSync(WalEntry{Type: MutBulkLoad, Payload: payload}); err != nil {
		return fmt.Errorf("manager: wal append: %w", err)
	}

	m.dag = dag
	m.updateActiveCount()

	return nil
}

// GetNode returns a node by ID.
func (m *Manager) GetNode(nodeID string) (*DagNode, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.dag.Nodes {
		if m.dag.Nodes[i].ID == nodeID {
			node := m.dag.Nodes[i]
			return &node, nil
		}
	}
	return nil, fmt.Errorf("manager: node '%s' not found", nodeID)
}

// GetReadyNodes returns nodes with all dependencies satisfied (all parents Done).
func (m *Manager) GetReadyNodes() []DagNode {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Build set of dependencies for each node
	deps := make(map[string][]string) // node → list of from_nodes it depends on
	for _, e := range m.dag.Edges {
		deps[e.ToNode] = append(deps[e.ToNode], e.FromNode)
	}

	// Build status map
	status := make(map[string]NodeStatus)
	for _, n := range m.dag.Nodes {
		status[n.ID] = n.Status
	}

	var ready []DagNode
	for _, n := range m.dag.Nodes {
		if n.Status != StatusPending {
			continue
		}
		allDone := true
		for _, depID := range deps[n.ID] {
			if s, ok := status[depID]; !ok || !s.IsTerminal() {
				allDone = false
				break
			}
		}
		if allDone {
			ready = append(ready, n)
		}
	}

	return ready
}

// GetNodesByDomain returns all nodes assigned to a domain.
func (m *Manager) GetNodesByDomain(domainID string) []DagNode {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []DagNode
	for _, n := range m.dag.Nodes {
		if n.DomainID == domainID {
			result = append(result, n)
		}
	}
	return result
}

// Snapshot returns a copy of the full DAG data.
func (m *Manager) Snapshot() *DagData {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy
	snapshot := &DagData{
		Header: m.dag.Header,
		Nodes:  make([]DagNode, len(m.dag.Nodes)),
		Edges:  make([]DagEdge, len(m.dag.Edges)),
	}
	copy(snapshot.Nodes, m.dag.Nodes)
	copy(snapshot.Edges, m.dag.Edges)
	return snapshot
}

// Close flushes the WAL and closes both the WAL and store.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Persist final state to store
	storeErr := m.store.Write(m.dag)

	// Truncate WAL since store reflects latest state
	m.wal.Truncate()

	walErr := m.wal.Close()
	closeErr := m.store.Close()

	if storeErr != nil {
		return storeErr
	}
	if walErr != nil {
		return walErr
	}
	return closeErr
}
