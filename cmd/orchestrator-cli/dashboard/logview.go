package dashboard

import (
	"encoding/json"
	"net"
	"sort"
	"strings"

	"aion-kernel/internal/hub"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type MultiLogModel struct {
	addr        string
	viewports   map[string]viewport.Model
	agentOutput map[string][]string
	agents      []string // sorted keys for stable ordering
	activeIdx   int
	eventCh     chan tea.Msg
	width       int
	height      int
	Focused     bool
}

func NewMultiLogModel(addr string) *MultiLogModel {
	return &MultiLogModel{
		addr:        addr,
		viewports:   make(map[string]viewport.Model),
		agentOutput: make(map[string][]string),
		eventCh:     make(chan tea.Msg, 100),
	}
}

func (m *MultiLogModel) Init() tea.Cmd {
	return tea.Batch(
		m.startStreamCmd(),
		m.readNextCmd(),
	)
}

func (m *MultiLogModel) startStreamCmd() tea.Cmd {
	return func() tea.Msg {
		go func() {
			// We can just listen to the Hub stream and segregate by FromAgent.
			req := map[string]interface{}{
				"method": "tail-hub-events",
				"id":     "multilog-1",
				"params": map[string]interface{}{},
			}
			reqBytes, _ := json.Marshal(req)

			conn, err := net.Dial("tcp", m.addr)
			if err != nil {
				m.eventCh <- StatusMsg{
					Text:  "Orchestrator connection failed: " + err.Error(),
					Level: "error",
				}
				return
			}
			defer conn.Close()

			if _, err := conn.Write(reqBytes); err != nil {
				m.eventCh <- StatusMsg{
					Text:  "Orchestrator stream failed: " + err.Error(),
					Level: "error",
				}
				return
			}
			m.eventCh <- StatusMsg{
				Text:  "Connected to orchestrator",
				Level: "ok",
			}

			decoder := json.NewDecoder(conn)
			normalizer := &tuiEventNormalizer{}
			for {
				var msg hub.Message
				if err := decoder.Decode(&msg); err != nil {
					m.eventCh <- StatusMsg{
						Text:  "Orchestrator stream closed: " + err.Error(),
						Level: "warn",
					}
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

// NextAgent cycles the active viewport
func (m *MultiLogModel) NextAgent() {
	if len(m.agents) > 0 {
		m.activeIdx = (m.activeIdx + 1) % len(m.agents)
	}
}

// PrevAgent cycles backwards
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
		for id, vp := range m.viewports {
			vp.Width = contentWidth(m.width, 4)
			vp.Height = m.height
			vp.SetContent(m.renderAgentOutput(id, vp.Width))
			m.viewports[id] = vp
		}

	case tea.KeyMsg:
		if len(m.agents) > 0 {
			activeID := m.agents[m.activeIdx]
			vp := m.viewports[activeID]
			newVp, cmd := vp.Update(msg)
			m.viewports[activeID] = newVp
			cmds = append(cmds, cmd)
		}

	case StatusMsg:
		cmds = append(cmds, m.readNextCmd())

	case tuiEventMsg:
		if msg.Audience != tuiAudienceLogs {
			cmds = append(cmds, m.readNextCmd())
			return m, tea.Batch(cmds...)
		}
		id := msg.AgentID
		if _, exists := m.viewports[id]; !exists {
			m.agents = append(m.agents, id)
			sort.Strings(m.agents)
			vp := viewport.New(contentWidth(m.width, 4), m.height)
			m.viewports[id] = vp
		}

		m.agentOutput[id] = append(m.agentOutput[id], formatLogEvent(msg))
		if len(m.agentOutput[id]) > 500 {
			m.agentOutput[id] = m.agentOutput[id][len(m.agentOutput[id])-500:]
		}

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

func (m *MultiLogModel) View() string {
	if len(m.agents) == 0 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("Waiting for agent output...")
	}

	activeID := m.agents[m.activeIdx]
	vp := m.viewports[activeID]

	// Draw tabs
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

	header := wrapPlain(strings.Join(tabs, " "), contentWidth(m.width, 2))

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
		scrollbar = sb.String()
	}

	content := vpView
	if scrollbar != "" {
		// Join at the top to keep lines aligned
		content = lipgloss.JoinHorizontal(lipgloss.Top, vpView, scrollbar)
	}

	return header + "\n" + border.Render(content)
}
