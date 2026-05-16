package dashboard

import (
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// StatusLevel maps a level string to a lipgloss color.
var levelColor = map[string]lipgloss.Color{
	"info":  lipgloss.Color("39"),  // cyan
	"ok":    lipgloss.Color("82"),  // green
	"warn":  lipgloss.Color("214"), // orange
	"error": lipgloss.Color("196"), // red
}

// StatusModel is a persistent one-line status bar with an animated spinner.
type StatusModel struct {
	spinner spinner.Model
	text    string
	level   string
	width   int
	active  bool // true while spinner ticks
}

// StatusMsg is dispatched when a SystemStatus hub message arrives.
type StatusMsg struct {
	Text  string
	Level string
}

func NewStatusModel() *StatusModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
	return &StatusModel{
		spinner: s,
		text:    "Connecting to orchestrator...",
		level:   "info",
		active:  true,
	}
}

func (m *StatusModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m *StatusModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case StatusMsg:
		m.setStatus(msg.Text, msg.Level, msg.Level != "ok" && msg.Level != "error")
		if m.active {
			cmds = append(cmds, m.spinner.Tick)
		}
	case tuiEventMsg:
		if m.updateFromTUIEvent(msg) {
			if m.active {
				cmds = append(cmds, m.spinner.Tick)
			}
		}
	case spinner.TickMsg:
		if m.active {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			cmds = append(cmds, cmd)
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
	}

	return m, tea.Batch(cmds...)
}

func (m *StatusModel) setStatus(text, level string, active bool) {
	if strings.TrimSpace(text) == "" {
		return
	}
	if level == "" {
		level = "info"
	}
	m.text = text
	m.level = level
	m.active = active
	m.spinner.Style = lipgloss.NewStyle().Foreground(statusColor(level))
}

func (m *StatusModel) updateFromTUIEvent(msg tuiEventMsg) bool {
	if msg.Audience == tuiAudienceStatus {
		m.setStatus(msg.Content, msg.Level, msg.Level != "ok" && msg.Level != "error")
		return true
	}

	agent := msg.Author
	if agent == "" {
		agent = msg.AgentID
	}
	if agent == "" {
		agent = "Agent"
	}

	switch msg.Kind {
	case tuiKindThinking:
		m.setStatus(agent+" is thinking", "info", true)
		return true
	case tuiKindToolStart:
		summary := msg.Summary
		if summary == "" {
			summary = msg.Tool
		}
		if summary == "" {
			summary = "tool"
		}
		m.setStatus(agent+" is running: "+summary, "info", true)
		return true
	case tuiKindText:
		if msg.Role == "assistant" || msg.AgentID == "architect" || msg.AgentID == "orchestrator" {
			m.setStatus(agent+" is responding", "info", true)
			return true
		}
	}

	return false
}

func (m *StatusModel) View() string {
	color := statusColor(m.level)

	prefix := m.spinner.View() + " "
	if !m.active {
		if m.level == "ok" {
			prefix = "✅ "
		} else if m.level == "error" {
			prefix = "❌ "
		}
	}

	text := prefix + m.text

	// Right-pad to full width so the status bar always spans the terminal.
	textLen := len([]rune(prefix)) + len(m.text)
	if m.width > textLen {
		text += strings.Repeat(" ", m.width-textLen)
	}

	return lipgloss.NewStyle().
		Foreground(color).
		Bold(m.level == "error" || m.level == "warn").
		Render(text)
}

func statusColor(level string) lipgloss.Color {
	color, ok := levelColor[level]
	if !ok {
		return levelColor["info"]
	}
	return color
}
