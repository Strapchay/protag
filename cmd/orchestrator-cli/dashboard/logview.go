package dashboard

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"

	"aion-kernel/internal/hub"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type MultiLogModel struct {
	addr        string
	viewports   map[string]viewport.Model
	agentOutput map[string][]string
	agentMeta   map[string]agentTraceMeta
	agents      []string
	activeIdx   int
	eventCh     chan tea.Msg
	width       int
	height      int
	Focused     bool
	input       textarea.Model
}

type agentTraceMeta struct {
	LastKind   string
	LastStatus string
	LastLevel  string
}

func NewMultiLogModel(addr string) *MultiLogModel {
	ti := textarea.New()
	ti.Placeholder = "Type a message for the selected agent... (Esc to leave input)"
	ti.Focus()
	ti.CharLimit = 0
	ti.ShowLineNumbers = false
	ti.Prompt = ""
	ti.SetWidth(80)
	ti.SetHeight(1)

	return &MultiLogModel{
		addr:        addr,
		viewports:   make(map[string]viewport.Model),
		agentOutput: make(map[string][]string),
		agentMeta:   make(map[string]agentTraceMeta),
		eventCh:     make(chan tea.Msg, 100),
		input:       ti,
	}
}

func (m *MultiLogModel) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink,
		m.startStreamCmd(),
		m.readNextCmd(),
	)
}

func (m *MultiLogModel) startStreamCmd() tea.Cmd {
	return func() tea.Msg {
		go func() {
			req := map[string]interface{}{
				"method": "tail-hub-events",
				"id":     "multilog-1",
				"params": map[string]interface{}{},
			}
			reqBytes, _ := json.Marshal(req)

			conn, err := net.Dial("tcp", m.addr)
			if err != nil {
				m.eventCh <- StatusMsg{Text: "Orchestrator connection failed: " + err.Error(), Level: "error"}
				return
			}
			defer conn.Close()

			if _, err := conn.Write(reqBytes); err != nil {
				m.eventCh <- StatusMsg{Text: "Orchestrator stream failed: " + err.Error(), Level: "error"}
				return
			}
			m.eventCh <- StatusMsg{Text: "Connected to orchestrator", Level: "ok"}

			decoder := json.NewDecoder(conn)
			normalizer := &tuiEventNormalizer{}
			for {
				var msg hub.Message
				if err := decoder.Decode(&msg); err != nil {
					m.eventCh <- StatusMsg{Text: "Orchestrator stream closed: " + err.Error(), Level: "warn"}
					break
				}

				for _, event := range normalizer.Normalize(msg) {
					if event.Audience == tuiAudienceStatus {
						m.eventCh <- StatusMsg{Text: event.Content, Level: event.Level}
					} else {
						m.eventCh <- event
					}
				}
			}
		}()
		return nil
	}
}

func (m *MultiLogModel) readNextCmd() tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-m.eventCh
		if !ok {
			return nil
		}
		return msg
	}
}

func (m *MultiLogModel) NextAgent() {
	if len(m.agents) > 0 {
		m.activeIdx = (m.activeIdx + 1) % len(m.agents)
	}
}

func (m *MultiLogModel) PrevAgent() {
	if len(m.agents) > 0 {
		m.activeIdx--
		if m.activeIdx < 0 {
			m.activeIdx = len(m.agents) - 1
		}
	}
}

func (m *MultiLogModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.syncInputSize()
		for id, vp := range m.viewports {
			vp.Width = contentWidth(m.width, 4)
			vp.Height = m.transcriptHeight()
			vp.SetContent(m.renderAgentOutput(id, vp.Width))
			m.viewports[id] = vp
		}

	case tea.KeyMsg:
		if msg.String() == "esc" {
			if m.input.Focused() {
				m.input.Blur()
				return m, nil
			}
			m.input.Focus()
			return m, nil
		}

		if m.input.Focused() {
			switch msg.Type {
			case tea.KeyEnter:
				if err := m.sendActiveAgentMessage(); err != nil {
					m.appendSystemLine("Failed to send message: " + err.Error())
				}
				m.input.SetValue("")
				m.syncInputSize()
				return m, nil
			}
		} else {
			switch msg.String() {
			case "left", "h":
				m.PrevAgent()
				return m, nil
			case "right", "l":
				m.NextAgent()
				return m, nil
			case "up":
				if len(m.agents) > 0 {
					activeID := m.agents[m.activeIdx]
					vp := m.viewports[activeID]
					vp.LineUp(1)
					m.viewports[activeID] = vp
				}
				return m, nil
			case "down":
				if len(m.agents) > 0 {
					activeID := m.agents[m.activeIdx]
					vp := m.viewports[activeID]
					vp.LineDown(1)
					m.viewports[activeID] = vp
				}
				return m, nil
			}
		}

		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		cmds = append(cmds, cmd)
		m.syncInputSize()

	case StatusMsg:
		cmds = append(cmds, m.readNextCmd())

	case tuiEventMsg:
		if msg.Audience != tuiAudienceLogs {
			cmds = append(cmds, m.readNextCmd())
			return m, tea.Batch(cmds...)
		}
		id := msg.AgentID
		if _, exists := m.viewports[id]; !exists {
			previousActive := m.activeAgentID()
			m.agents = append(m.agents, id)
			sort.Strings(m.agents)
			if previousActive != "" {
				m.activeIdx = indexOfString(m.agents, previousActive)
				if m.activeIdx < 0 {
					m.activeIdx = 0
				}
			}
			vp := viewport.New(contentWidth(m.width, 4), m.transcriptHeight())
			m.viewports[id] = vp
		}

		m.agentOutput[id] = append(m.agentOutput[id], formatLogEvent(msg))
		if len(m.agentOutput[id]) > 500 {
			m.agentOutput[id] = m.agentOutput[id][len(m.agentOutput[id])-500:]
		}

		meta := m.agentMeta[id]
		meta.LastKind = msg.Kind
		meta.LastStatus = msg.Content
		meta.LastLevel = msg.Level
		m.agentMeta[id] = meta

		vp := m.viewports[id]
		wasAtBottom := vp.AtBottom()
		vp.SetContent(m.renderAgentOutput(id, vp.Width))
		if wasAtBottom {
			vp.GotoBottom()
		}
		m.viewports[id] = vp

		cmds = append(cmds, m.readNextCmd())
	}

	return m, tea.Batch(cmds...)
}

func (m *MultiLogModel) transcriptHeight() int {
	h := m.height - 8
	if h < 4 {
		h = 4
	}
	return h
}

func (m *MultiLogModel) syncInputSize() {
	wrapW := contentWidth(m.width, 6)
	lines := wrappedLineCount(m.input.Value(), wrapW)
	if lines < 1 {
		lines = 1
	}
	if lines > 4 {
		lines = 4
	}
	m.input.SetWidth(contentWidth(m.width, 4))
	m.input.SetHeight(lines)
}

func (m *MultiLogModel) activeAgentID() string {
	if len(m.agents) == 0 {
		return ""
	}
	return m.agents[m.activeIdx]
}

func (m *MultiLogModel) sendActiveAgentMessage() error {
	activeID := m.activeAgentID()
	if activeID == "" {
		return fmt.Errorf("no agent available")
	}
	text := strings.TrimSpace(m.input.Value())
	if text == "" {
		return nil
	}

	reqBytes, _ := json.Marshal(map[string]interface{}{
		"method": "send-message",
		"id":     "agents-send-1",
		"params": map[string]interface{}{
			"agent_id": activeID,
			"text":     text,
		},
	})

	conn, err := net.Dial("tcp", m.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.Write(reqBytes); err != nil {
		return err
	}

	var resp struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf(resp.Error)
	}

	m.appendSystemLine("sent to " + activeID + ": " + text)
	return nil
}

func (m *MultiLogModel) appendSystemLine(text string) {
	id := "system"
	if _, exists := m.viewports[id]; !exists {
		previousActive := m.activeAgentID()
		m.agents = append(m.agents, id)
		sort.Strings(m.agents)
		if previousActive != "" {
			m.activeIdx = indexOfString(m.agents, previousActive)
			if m.activeIdx < 0 {
				m.activeIdx = 0
			}
		}
		m.viewports[id] = viewport.New(contentWidth(m.width, 4), m.transcriptHeight())
	}
	m.agentOutput[id] = append(m.agentOutput[id], text)
	vp := m.viewports[id]
	vp.SetContent(m.renderAgentOutput(id, vp.Width))
	m.viewports[id] = vp
}

func (m *MultiLogModel) renderAgentOutput(id string, width int) string {
	wrapWidth := contentWidth(width, 2)
	var wrappedLines []string
	for _, rawLine := range m.agentOutput[id] {
		wrappedLines = append(wrappedLines, wrapPlain(rawLine, wrapWidth))
	}
	return strings.Join(wrappedLines, "\n")
}

func formatLogEvent(msg tuiEventMsg) string {
	switch msg.Kind {
	case tuiKindThinking:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Italic(true).Render("Thinking: " + msg.Content)
	case tuiKindToolStart:
		summary := msg.Summary
		if summary == "" {
			summary = "Tool: " + msg.Tool + "(" + msg.Input + ")"
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color("208")).Bold(true).Render(summary)
	default:
		return msg.Content
	}
}

func indexOfString(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}

func (m *MultiLogModel) View() string {
	if len(m.agents) == 0 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("Waiting for agent output...")
	}

	activeID := m.agents[m.activeIdx]
	vp := m.viewports[activeID]
	meta := m.agentMeta[activeID]

	var tabs []string
	for i, id := range m.agents {
		label := truncateToWidth(id, 18)
		style := lipgloss.NewStyle().Padding(0, 1)
		if i == m.activeIdx {
			style = style.Background(lipgloss.Color("39")).Foreground(lipgloss.Color("255")).Bold(true)
		} else {
			style = style.Background(lipgloss.Color("236")).Foreground(lipgloss.Color("248"))
		}
		tabs = append(tabs, style.Render(label))
	}

	header := lipgloss.NewStyle().
		Foreground(lipgloss.Color("255")).
		Background(lipgloss.Color("62")).
		Padding(0, 1).
		Render(fmt.Sprintf(" Agent: %s | Kind: %s | Last: %s ", activeID, meta.LastKind, truncateToWidth(meta.LastStatus, 60)))

	borderColor := lipgloss.Color("240")
	if m.Focused {
		borderColor = lipgloss.Color("205")
	}

	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Width(contentWidth(m.width, 2))

	vpView := vp.View()
	scrollbar := ""
	if vp.TotalLineCount() > vp.Height {
		percent := vp.ScrollPercent()
		thumbPos := int(percent * float64(vp.Height-1))
		var sb strings.Builder
		for i := 0; i < vp.Height; i++ {
			if i == thumbPos {
				sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("█"))
			} else {
				sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("238")).Render("│"))
			}
			if i < vp.Height-1 {
				sb.WriteString("\n")
			}
		}
		scrollbar = "\n" + sb.String()
	}

	inputBorder := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("236")).
		Width(contentWidth(m.width, 2)).
		Render(m.input.View())

	content := border.Render(vpView + scrollbar)
	return strings.Join([]string{strings.Join(tabs, " "), header, content, inputBorder}, "\n")
}
