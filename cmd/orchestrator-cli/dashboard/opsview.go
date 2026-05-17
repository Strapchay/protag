package dashboard

import (
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type OpsModel struct {
	viewport viewport.Model
	lines    []string
	width    int
	height   int
	Focused  bool
}

func NewOpsModel() *OpsModel {
	vp := viewport.New(80, 10)
	vp.SetContent("Waiting for model operations...")
	return &OpsModel{viewport: vp}
}

func (m *OpsModel) Init() tea.Cmd {
	return nil
}

func (m *OpsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewport.Width = contentWidth(msg.Width, 4)
		m.viewport.Height = contentHeight(msg.Height, 3)
		m.renderLines()
	case tea.MouseMsg:
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			m.viewport.LineUp(3)
			return m, nil
		case tea.MouseButtonWheelDown:
			m.viewport.LineDown(3)
			return m, nil
		}
	case tuiEventMsg:
		if msg.Kind != tuiKindToolStart || msg.Audience != tuiAudienceLogs {
			return m, nil
		}
		line := msg.Summary
		if line == "" {
			line = msg.Tool + "(" + msg.Input + ")"
		}
		if msg.AgentID != "" {
			line = "[" + msg.AgentID + "] " + line
		}
		m.lines = append(m.lines, line)
		if len(m.lines) > 500 {
			m.lines = m.lines[len(m.lines)-500:]
		}
		m.renderLines()
		m.viewport.GotoBottom()
	}

	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m *OpsModel) renderLines() {
	wrapW := contentWidth(m.viewport.Width, 2)
	if len(m.lines) == 0 {
		m.viewport.SetContent("Waiting for model operations...")
		return
	}
	var wrapped []string
	for _, line := range m.lines {
		wrapped = append(wrapped, wrapPlain(line, wrapW))
	}
	m.viewport.SetContent(strings.Join(wrapped, "\n"))
}

func (m *OpsModel) View() string {
	borderColor := lipgloss.Color("240")
	if m.Focused {
		borderColor = lipgloss.Color("205")
	}

	title := lipgloss.NewStyle().
		Foreground(lipgloss.Color("255")).
		Background(lipgloss.Color("62")).
		Padding(0, 1).
		Render(" Model Operations ")

	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(0, 1).
		Width(contentWidth(m.width, 2))

	return title + "\n" + border.Render(m.viewport.View())
}
