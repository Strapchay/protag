package dashboard

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

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
	Type          string `json:"type"`
	Status        string `json:"status"`
	AssignedAgent string `json:"assigned_agent"`
}

type EdgeData struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type DagModel struct {
	width            int
	height           int
	nodes            []NodeData
	edges            []EdgeData
	selectedIdx      int
	lastError        error
	orchestratorAddr string
	Focused          bool
}

func NewDagModel(addr string) *DagModel {
	return &DagModel{
		orchestratorAddr: addr,
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

		var resp struct {
			Result struct {
				Dag struct {
					Nodes []NodeData `json:"nodes"`
					Edges []EdgeData `json:"edges"`
				} `json:"dag"`
			} `json:"result"`
			Error string `json:"error"`
		}
		decoder := json.NewDecoder(conn)
		if err := decoder.Decode(&resp); err != nil {
			return dagDataMsg{Err: err}
		}
		if resp.Error != "" {
			return dagDataMsg{Err: fmt.Errorf("%s", resp.Error)}
		}

		return dagDataMsg{Nodes: resp.Result.Dag.Nodes, Edges: resp.Result.Dag.Edges}
	}
}

func (m *DagModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "left":
			if m.selectedIdx > 0 {
				m.selectedIdx--
			}
		case "down", "right":
			if m.selectedIdx < len(m.nodes)-1 {
				m.selectedIdx++
			}
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tickMsg:
		return m, tea.Batch(m.tickCmd(), m.fetchDagCmd())
	case dagDataMsg:
		m.lastError = msg.Err
		if msg.Err == nil {
			m.nodes = msg.Nodes
			m.edges = msg.Edges
		}
	}
	return m, nil
}

func (m *DagModel) View() string {
	if m.lastError != nil {
		return wrapPlain(fmt.Sprintf("Error polling DAG: %v", m.lastError), contentWidth(m.width, 2))
	}
	if len(m.nodes) == 0 {
		return "DAG is empty or initializing..."
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

	var sb strings.Builder
	sb.WriteString("DAG Visualization\n\n")

	for _, layer := range layers {
		var row []string
		for _, node := range layer {
			style := m.styleForStatus(node.Status)
			if selectedNode != nil && node.ID == selectedNode.ID {
				style = style.Reverse(true)
			}
			nodeText := truncateToWidth(node.ID, 24)
			row = append(row, style.Render(fmt.Sprintf("[%s]", nodeText)))
		}
		sb.WriteString(wrapPlain("    "+strings.Join(row, " -> "), contentWidth(m.width, 6)) + "\n\n")
	}

	graphOutput := sb.String()

	// Selected Node Details Pane
	var detailsOutput string
	if selectedNode != nil {
		detailsW := contentWidth(m.width, 6)
		detailsOutput = fmt.Sprintf("ID: %s\nType: %s\nStatus: %s\nAssigned Agent: %s",
			wrapPlain(selectedNode.ID, detailsW),
			wrapPlain(selectedNode.Type, detailsW),
			wrapPlain(selectedNode.Status, detailsW),
			wrapPlain(selectedNode.AssignedAgent, detailsW))
	}

	borderColor := lipgloss.Color("240")
	if m.Focused {
		borderColor = lipgloss.Color("205")
	}

	mainBox := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(0, 1).
		Width(contentWidth(m.width, 2)).
		Render(graphOutput)

	detailsBox := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(0, 1).
		Width(contentWidth(m.width, 2)).
		Render(detailsOutput)

	return lipgloss.JoinVertical(lipgloss.Left, mainBox, detailsBox)
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
