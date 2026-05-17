package dashboard

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type tickMsg time.Time

type dagDataMsg struct {
	Nodes []NodeData
	Edges []EdgeData
	Err   error
}

type NodeData struct {
	ID            string `json:"id"`
	DomainID      string `json:"domain_id"`
	Type          string `json:"type,omitempty"`
	Status        string `json:"status"`
	AssignedAgent string `json:"assigned_agent"`
}

type EdgeData struct {
	From string `json:"from_node"`
	To   string `json:"to_node"`
}

type DagModel struct {
	width            int
	height           int
	nodes            []NodeData
	edges            []EdgeData
	selectedIdx      int
	selectedLine     int
	nodeLines        map[string]int
	lastError        error
	viewport         viewport.Model
	dirty            bool
	orchestratorAddr string
	Focused          bool
}

func NewDagModel(addr string) *DagModel {
	vp := viewport.New(80, 10)
	vp.SetContent("DAG is empty or initializing...")
	return &DagModel{
		orchestratorAddr: addr,
		viewport:         vp,
		nodeLines:        make(map[string]int),
		dirty:            true,
	}
}

func (m *DagModel) Init() tea.Cmd {
	return tea.Batch(
		m.fetchDagCmd(),
		m.tickCmd(),
	)
}

func (m *DagModel) tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *DagModel) fetchDagCmd() tea.Cmd {
	return func() tea.Msg {
		req := map[string]interface{}{
			"method": "read-dag",
			"id":     fmt.Sprintf("dag-tick-%d", time.Now().UnixNano()),
			"params": map[string]interface{}{},
		}
		reqBytes, _ := json.Marshal(req)

		conn, err := net.Dial("tcp", m.orchestratorAddr)
		if err != nil {
			return dagDataMsg{Err: err}
		}
		defer conn.Close()

		if _, err := conn.Write(reqBytes); err != nil {
			return dagDataMsg{Err: err}
		}

		var resp dagRPCResponse
		decoder := json.NewDecoder(conn)
		if err := decoder.Decode(&resp); err != nil {
			return dagDataMsg{Err: err}
		}
		if resp.Error != "" {
			return dagDataMsg{Err: fmt.Errorf("%s", resp.Error)}
		}

		nodes, edges, err := decodeDAGSnapshot(resp.Result)
		if err != nil {
			return dagDataMsg{Err: err}
		}
		return dagDataMsg{Nodes: nodes, Edges: edges}
	}
}

func (m *DagModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.MouseMsg:
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			m.viewport.LineUp(3)
			return m, nil
		case tea.MouseButtonWheelDown:
			m.viewport.LineDown(3)
			return m, nil
		}
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "left":
			if m.selectedIdx > 0 {
				m.selectedIdx--
				m.dirty = true
			}
		case "down", "right":
			if m.selectedIdx < len(m.nodes)-1 {
				m.selectedIdx++
				m.dirty = true
			}
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewport.Width = contentWidth(msg.Width, 4)
		m.viewport.Height = contentHeight(msg.Height, 2)
		m.dirty = true
	case tickMsg:
		return m, tea.Batch(m.tickCmd(), m.fetchDagCmd())
	case dagDataMsg:
		m.lastError = msg.Err
		if msg.Err == nil {
			m.nodes = msg.Nodes
			m.edges = msg.Edges
		}
		m.dirty = true
	}
	return m, nil
}

func (m *DagModel) View() string {
	if m.dirty {
		m.renderDAG()
		m.dirty = false
	}
	borderColor := lipgloss.Color("240")
	if m.Focused {
		borderColor = lipgloss.Color("205")
	}
	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Width(contentWidth(m.width, 2))
	return border.Render(m.viewport.View())
}

func (m *DagModel) renderDAG() {
	if m.lastError != nil {
		m.viewport.SetContent(wrapPlain(fmt.Sprintf("Error polling DAG: %v", m.lastError), contentWidth(m.width, 4)))
		return
	}
	if len(m.nodes) == 0 {
		m.selectedLine = 0
		m.viewport.SetContent("DAG is empty or initializing...")
		return
	}

	// Flat deterministic sort for navigation
	var orderedNodes []NodeData
	nodesMap := make(map[string]NodeData)
	for _, n := range m.nodes {
		nodesMap[n.ID] = n
	}

	// Calculate layers
	layers := m.computeLayers()
	for _, layer := range layers {
		orderedNodes = append(orderedNodes, layer...)
	}

	// Selected node logic
	if m.selectedIdx >= len(orderedNodes) {
		m.selectedIdx = len(orderedNodes) - 1
	}
	if m.selectedIdx < 0 {
		m.selectedIdx = 0
	}
	var selectedNode *NodeData
	if len(orderedNodes) > 0 {
		selectedNode = &orderedNodes[m.selectedIdx]
	}

	lines := []string{"DAG Visualization", ""}
	m.nodeLines = make(map[string]int, len(m.nodes))

	for _, layer := range layers {
		var row []string
		for _, node := range layer {
			style := m.styleForStatus(node.Status)
			prefix := " "
			if selectedNode != nil && node.ID == selectedNode.ID {
				style = style.Reverse(true)
				prefix = "▶"
			}
			nodeText := truncateToWidth(node.ID, 24)
			row = append(row, style.Render(fmt.Sprintf("%s[%s]", prefix, nodeText)))
		}
		rowLine := wrapPlain("    "+strings.Join(row, " -> "), contentWidth(m.width, 6))
		lines = append(lines, rowLine, "")
		for _, node := range layer {
			if selectedNode != nil && node.ID == selectedNode.ID {
				m.nodeLines[node.ID] = len(lines) - 2
			}
		}
	}

	// Selected Node Details Pane
	if selectedNode != nil {
		detailsW := contentWidth(m.width, 6)
		nodeType := selectedNode.Type
		if nodeType == "" {
			nodeType = selectedNode.DomainID
		}
		assignedAgent := selectedNode.AssignedAgent
		if strings.TrimSpace(assignedAgent) == "" && strings.TrimSpace(selectedNode.DomainID) != "" {
			assignedAgent = "agent-" + selectedNode.DomainID
		}
		if strings.TrimSpace(assignedAgent) == "" {
			assignedAgent = "unassigned"
		}
		lines = append(lines, "Selected Node", "")
		lines = append(lines,
			fmt.Sprintf("ID: %s", wrapPlain(selectedNode.ID, detailsW)),
			fmt.Sprintf("Domain: %s", wrapPlain(nodeType, detailsW)),
			fmt.Sprintf("Status: %s", wrapPlain(selectedNode.Status, detailsW)),
			fmt.Sprintf("Assigned Agent: %s", wrapPlain(assignedAgent, detailsW)),
		)
		m.selectedLine = m.nodeLines[selectedNode.ID]
		if m.selectedLine < 0 {
			m.selectedLine = 0
		}
	}

	m.viewport.SetContent(strings.Join(lines, "\n"))
	if selectedNode != nil && m.height > 0 {
		target := m.selectedLine - m.viewport.Height/2
		if target < 0 {
			target = 0
		}
		m.viewport.SetYOffset(target)
	}
}

func (m *DagModel) computeLayers() [][]NodeData {
	// Build adjacency list and indegree map
	indegree := make(map[string]int)
	adj := make(map[string][]string)
	nodesMap := make(map[string]NodeData)

	for _, n := range m.nodes {
		nodesMap[n.ID] = n
		indegree[n.ID] = 0
	}
	for _, e := range m.edges {
		adj[e.From] = append(adj[e.From], e.To)
		indegree[e.To]++
	}

	// Find roots
	var roots []string
	for id, in := range indegree {
		if in == 0 {
			roots = append(roots, id)
		}
	}
	sort.Strings(roots)

	var layers [][]NodeData
	currentLayer := roots

	for len(currentLayer) > 0 {
		var layerNodes []NodeData
		var nextLayer []string

		for _, id := range currentLayer {
			layerNodes = append(layerNodes, nodesMap[id])
			for _, neighbor := range adj[id] {
				indegree[neighbor]--
				if indegree[neighbor] == 0 {
					nextLayer = append(nextLayer, neighbor)
				}
			}
		}
		sort.Strings(nextLayer)
		layers = append(layers, layerNodes)
		currentLayer = nextLayer
	}

	return layers
}

type dagRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error"`
}

type dagSnapshotWire struct {
	Dag struct {
		Nodes []dagNodeWire `json:"nodes"`
		Edges []dagEdgeWire `json:"edges"`
	} `json:"dag"`
	Nodes []dagNodeWire `json:"nodes"`
	Edges []dagEdgeWire `json:"edges"`
}

type dagNodeWire struct {
	ID            string      `json:"id"`
	DomainID      string      `json:"domain_id"`
	Type          string      `json:"type"`
	Status        interface{} `json:"status"`
	AssignedAgent string      `json:"assigned_agent"`
}

type dagEdgeWire struct {
	From     string `json:"from"`
	To       string `json:"to"`
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
}

func decodeDAGSnapshot(raw json.RawMessage) ([]NodeData, []EdgeData, error) {
	if len(raw) == 0 {
		return nil, nil, nil
	}
	var snap dagSnapshotWire
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, nil, err
	}
	wireNodes := snap.Nodes
	wireEdges := snap.Edges
	if len(wireNodes) == 0 && len(snap.Dag.Nodes) > 0 {
		wireNodes = snap.Dag.Nodes
		wireEdges = snap.Dag.Edges
	}

	nodes := make([]NodeData, 0, len(wireNodes))
	for _, node := range wireNodes {
		nodes = append(nodes, NodeData{
			ID:            node.ID,
			DomainID:      node.DomainID,
			Type:          node.Type,
			Status:        normalizeNodeStatus(node.Status),
			AssignedAgent: node.AssignedAgent,
		})
	}

	edges := make([]EdgeData, 0, len(wireEdges))
	for _, edge := range wireEdges {
		from := edge.FromNode
		if from == "" {
			from = edge.From
		}
		to := edge.ToNode
		if to == "" {
			to = edge.To
		}
		edges = append(edges, EdgeData{From: from, To: to})
	}
	return nodes, edges, nil
}

func normalizeNodeStatus(status interface{}) string {
	switch v := status.(type) {
	case string:
		if v != "" {
			return v
		}
	case float64:
		switch int(v) {
		case 0:
			return "Pending"
		case 1:
			return "InProgress"
		case 2:
			return "Done"
		case 3:
			return "Failed"
		case 4:
			return "Blocked"
		}
	}
	return "Unknown"
}

func (m *DagModel) styleForStatus(status string) lipgloss.Style {
	base := lipgloss.NewStyle().Padding(0, 1).Bold(true)
	switch status {
	case "Pending":
		return base.Foreground(lipgloss.Color("240")) // Grey
	case "Active":
		return base.Foreground(lipgloss.Color("39")) // Blue
	case "Done":
		return base.Foreground(lipgloss.Color("40")) // Green
	case "Failed":
		return base.Foreground(lipgloss.Color("196")) // Red
	default:
		return base.Foreground(lipgloss.Color("205")) // Pink
	}
}
