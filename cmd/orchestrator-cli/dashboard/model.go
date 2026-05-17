package dashboard

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Model is the root element of our BubbleTea interface.
type Model struct {
	Width        int
	Height       int
	SelectedPane string // "chat", "ops", "dag", "hub", "agents"
	Zoomed       bool

	// Child states
	DagView   *DagModel
	HubView   *HubModel
	LogView   *MultiLogModel
	OpsView   *OpsModel
	ChatInput *ChatModel
	StatusBar *StatusModel
}

// NewModel returns an initialized root dashboard model.
func NewModel(addr string) *Model {
	return &Model{
		SelectedPane: "chat",
		DagView:      NewDagModel(addr),
		HubView:      NewHubModel(addr),
		LogView:      NewMultiLogModel(addr),
		OpsView:      NewOpsModel(),
		ChatInput:    NewChatModel(addr),
		StatusBar:    NewStatusModel(),
	}
}

// Init handles initial generic I/O triggers on startup.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		tea.EnterAltScreen,
		m.DagView.Init(),
		m.HubView.Init(),
		m.LogView.Init(),
		m.OpsView.Init(),
		m.ChatInput.Init(),
		m.StatusBar.Init(),
	)
}

// Update processes BubbleTea events like keystrokes or resize payloads.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	handledGlobalKey := false
	handledResize := false

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}

		switch msg.String() {
		case "tab", "ctrl+j":
			m.nextPane()
			handledGlobalKey = true
		case "shift+tab", "ctrl+k":
			m.prevPane()
			handledGlobalKey = true
		case "q":
			if !m.isEditablePaneFocused() {
				return m, tea.Quit
			}
		case "enter":
			// If not already editing chat or agents, enter chat.
			if !m.isEditablePaneFocused() && m.ChatInput != nil {
				m.SelectedPane = "chat"
				m.ChatInput.input.Focus()
				handledGlobalKey = true
			}
		}
	case tea.WindowSizeMsg:
		m.Width = msg.Width
		m.Height = msg.Height
		handledResize = true

		m.resizePanes()
	}

	if m.DagView != nil {
		m.DagView.Focused = (m.SelectedPane == "dag")
	}
	if m.HubView != nil {
		m.HubView.Focused = (m.SelectedPane == "hub")
	}
	if m.LogView != nil {
		m.LogView.Focused = (m.SelectedPane == "agents")
	}
	if m.OpsView != nil {
		m.OpsView.Focused = (m.SelectedPane == "ops")
	}

	if m.ChatInput != nil {
		if m.SelectedPane == "chat" {
			if !m.ChatInput.input.Focused() {
				m.ChatInput.input.Focus()
			}
		} else {
			if m.ChatInput.input.Focused() {
				m.ChatInput.input.Blur()
			}
		}
	}
	if m.LogView != nil {
		if m.SelectedPane == "agents" {
			if !m.LogView.input.Focused() {
				m.LogView.input.Focus()
			}
		} else if m.LogView.input.Focused() {
			m.LogView.input.Blur()
		}
	}

	if handledResize {
		if m.StatusBar != nil {
			newSB, cmd := m.StatusBar.Update(tea.WindowSizeMsg{Width: m.Width})
			m.StatusBar = newSB.(*StatusModel)
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		}
		return m, tea.Batch(cmds...)
	}

	if handledGlobalKey {
		return m, nil
	}

	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch m.SelectedPane {
		case "dag":
			if m.DagView != nil {
				newDag, cmd := m.DagView.Update(keyMsg)
				m.DagView = newDag.(*DagModel)
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
		case "hub":
			if m.HubView != nil {
				newHub, cmd := m.HubView.Update(keyMsg)
				m.HubView = newHub.(*HubModel)
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
		case "agents":
			if m.LogView != nil {
				newLog, cmd := m.LogView.Update(keyMsg)
				m.LogView = newLog.(*MultiLogModel)
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
		case "ops":
			if m.OpsView != nil {
				newOps, cmd := m.OpsView.Update(keyMsg)
				m.OpsView = newOps.(*OpsModel)
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
		case "chat":
			if m.ChatInput != nil {
				newChat, cmd := m.ChatInput.Update(keyMsg)
				m.ChatInput = newChat.(*ChatModel)
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
		}
		return m, tea.Batch(cmds...)
	}

	// Delegate non-key messages to child panes
	if m.DagView != nil {
		newDag, cmd := m.DagView.Update(msg)
		m.DagView = newDag.(*DagModel)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	if m.HubView != nil {
		newHub, cmd := m.HubView.Update(msg)
		m.HubView = newHub.(*HubModel)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	if m.LogView != nil {
		newLog, cmd := m.LogView.Update(msg)
		m.LogView = newLog.(*MultiLogModel)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	if m.OpsView != nil {
		newOps, cmd := m.OpsView.Update(msg)
		m.OpsView = newOps.(*OpsModel)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	if m.StatusBar != nil {
		newSB, cmd := m.StatusBar.Update(msg)
		m.StatusBar = newSB.(*StatusModel)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	if m.ChatInput != nil {
		newChat, cmd := m.ChatInput.Update(msg)
		m.ChatInput = newChat.(*ChatModel)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *Model) isEditablePaneFocused() bool {
	if m.SelectedPane == "chat" && m.ChatInput != nil && m.ChatInput.input.Focused() {
		return true
	}
	if m.SelectedPane == "agents" && m.LogView != nil && m.LogView.input.Focused() {
		return true
	}
	return false
}

// View concatenates the application screen boundaries.
func (m *Model) View() string {
	if m.Width == 0 || m.Height == 0 {
		return "Starting Dashboard..."
	}

	var sb strings.Builder
	sb.WriteString(lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39")).
		Width(contentWidth(m.Width, 0)).
		Render("AION KERNEL DASHBOARD"))
	sb.WriteString("\n")
	sb.WriteString(m.renderTabs())
	sb.WriteString("\n")

	if m.DagView != nil && m.SelectedPane == "dag" {
		sb.WriteString(m.DagView.View())
		sb.WriteString("\n")
	}
	if m.HubView != nil && m.SelectedPane == "hub" {
		sb.WriteString(m.HubView.View())
		sb.WriteString("\n")
	}
	if m.LogView != nil && m.SelectedPane == "agents" {
		sb.WriteString(m.LogView.View())
		sb.WriteString("\n")
	}
	if m.OpsView != nil && m.SelectedPane == "ops" {
		sb.WriteString(m.OpsView.View())
		sb.WriteString("\n")
	}
	if m.ChatInput != nil && m.SelectedPane == "chat" {
		sb.WriteString(m.ChatInput.View())
		sb.WriteString("\n")
	}

	if m.StatusBar != nil {
		m.StatusBar.Update(tea.WindowSizeMsg{Width: m.Width})
		sb.WriteString(m.StatusBar.View())
		sb.WriteString("\n")
	}

	return sb.String()
}

func (m *Model) paneOrder() []string {
	return []string{"chat", "ops", "dag", "hub", "agents"}
}

func (m *Model) nextPane() {
	order := m.paneOrder()
	for i, pane := range order {
		if pane == m.SelectedPane {
			m.SelectedPane = order[(i+1)%len(order)]
			m.resizePanes()
			return
		}
	}
	m.SelectedPane = "chat"
	m.resizePanes()
}

func (m *Model) prevPane() {
	order := m.paneOrder()
	for i, pane := range order {
		if pane == m.SelectedPane {
			m.SelectedPane = order[(i+len(order)-1)%len(order)]
			m.resizePanes()
			return
		}
	}
	m.SelectedPane = "chat"
	m.resizePanes()
}

func (m *Model) resizePanes() {
	if m.Width == 0 || m.Height == 0 {
		return
	}

	paneHeight := m.Height - 4 // title, tabs, status, and spacing
	if paneHeight < 1 {
		paneHeight = 1
	}
	size := tea.WindowSizeMsg{Width: m.Width, Height: paneHeight}

	if m.DagView != nil {
		newM, _ := m.DagView.Update(size)
		m.DagView = newM.(*DagModel)
	}
	if m.HubView != nil {
		newM, _ := m.HubView.Update(size)
		m.HubView = newM.(*HubModel)
	}
	if m.LogView != nil {
		newM, _ := m.LogView.Update(size)
		m.LogView = newM.(*MultiLogModel)
	}
	if m.OpsView != nil {
		newM, _ := m.OpsView.Update(size)
		m.OpsView = newM.(*OpsModel)
	}
	if m.ChatInput != nil {
		newM, _ := m.ChatInput.Update(size)
		m.ChatInput = newM.(*ChatModel)
	}
}

func (m *Model) renderTabs() string {
	labels := map[string]string{
		"chat":   "Architect Chat",
		"ops":    "Operations",
		"dag":    "DAG",
		"hub":    "Context Hub",
		"agents": "Agents",
	}

	var tabs []string
	for _, pane := range m.paneOrder() {
		label := labels[pane]
		if m.Width < 48 {
			label = map[string]string{
				"chat":   "Chat",
				"ops":    "Ops",
				"dag":    "DAG",
				"hub":    "Hub",
				"agents": "Agents",
			}[pane]
		}
		style := lipgloss.NewStyle().Padding(0, 1)
		if pane == m.SelectedPane {
			style = style.Background(lipgloss.Color("39")).Foreground(lipgloss.Color("255")).Bold(true)
		} else {
			style = style.Background(lipgloss.Color("236")).Foreground(lipgloss.Color("248"))
		}
		tabs = append(tabs, style.Render(label))
	}
	return wrapPlain(strings.Join(tabs, " "), m.Width)
}
