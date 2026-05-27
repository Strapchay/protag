package dashboard

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

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
	renderCache map[string]string
	renderDirty map[string]bool
	agents      []string
	activeIdx   int
	tabStartIdx int
	eventCh     chan tea.Msg
	width       int
	height      int
	Focused     bool
	input       textarea.Model
	verbose     bool
}

type agentListTickMsg struct{}

type agentHistorySnapshotMsg struct {
	Messages []hub.Message
	AsOf     time.Time
	Err      error
}

type agentListMsg struct {
	Agents []agentListItem
	Err    error
}

type agentListItem struct {
	AgentID  string `json:"agent_id"`
	DomainID string `json:"domain_id"`
	State    string `json:"state"`
}

type agentTraceMeta struct {
	LastKind   string
	LastStatus string
	LastLevel  string
}

func NewMultiLogModel(addr string) *MultiLogModel {
	return NewMultiLogModelWithOptions(addr, Options{})
}

func NewMultiLogModelWithOptions(addr string, options Options) *MultiLogModel {
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
		renderCache: make(map[string]string),
		renderDirty: make(map[string]bool),
		eventCh:     make(chan tea.Msg, 100),
		input:       ti,
		verbose:     options.Verbose,
	}
}

func (m *MultiLogModel) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink,
		m.fetchSnapshotCmd(),
		m.fetchAgentsCmd(),
		m.agentListTickCmd(),
	)
}

func (m *MultiLogModel) agentListTickCmd() tea.Cmd {
	return tea.Tick(3*time.Second, func(time.Time) tea.Msg {
		return agentListTickMsg{}
	})
}

func (m *MultiLogModel) fetchAgentsCmd() tea.Cmd {
	return func() tea.Msg {
		reqBytes, _ := json.Marshal(map[string]interface{}{
			"method": "list-agents",
			"id":     "agents-list-1",
			"params": map[string]interface{}{},
		})
		conn, err := net.Dial("tcp", m.addr)
		if err != nil {
			return agentListMsg{Err: err}
		}
		defer conn.Close()
		if _, err := conn.Write(reqBytes); err != nil {
			return agentListMsg{Err: err}
		}
		var resp struct {
			Result struct {
				Agents []agentListItem `json:"agents"`
			} `json:"result"`
			Error string `json:"error"`
		}
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			return agentListMsg{Err: err}
		}
		if resp.Error != "" {
			return agentListMsg{Err: fmt.Errorf("%s", resp.Error)}
		}
		return agentListMsg{Agents: resp.Result.Agents}
	}
}

func (m *MultiLogModel) fetchSnapshotCmd() tea.Cmd {
	return func() tea.Msg {
		req := map[string]interface{}{
			"method": "hub-snapshot",
			"id":     "multilog-snapshot-1",
			"params": map[string]interface{}{},
		}
		reqBytes, _ := json.Marshal(req)

		conn, err := net.Dial("tcp", m.addr)
		if err != nil {
			return agentHistorySnapshotMsg{Err: err}
		}
		defer conn.Close()

		if _, err := conn.Write(reqBytes); err != nil {
			return agentHistorySnapshotMsg{Err: err}
		}

		var resp struct {
			Result struct {
				Messages []hub.Message `json:"messages"`
				AsOf     time.Time     `json:"as_of"`
			} `json:"result"`
			Error string `json:"error"`
		}
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			return agentHistorySnapshotMsg{Err: err}
		}
		if resp.Error != "" {
			return agentHistorySnapshotMsg{Err: fmt.Errorf("%s", resp.Error)}
		}
		return agentHistorySnapshotMsg{Messages: resp.Result.Messages, AsOf: resp.Result.AsOf}
	}
}

func (m *MultiLogModel) startStreamCmd(since time.Time) tea.Cmd {
	return func() tea.Msg {
		go func() {
			if m.verbose {
				m.eventCh <- StatusMsg{Text: "Opening live agent/hub stream at " + m.addr, Level: "info"}
			}
			req := map[string]interface{}{
				"method": "tail-hub-events",
				"id":     "multilog-1",
				"params": map[string]interface{}{"since": since},
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
			if m.verbose {
				m.eventCh <- StatusMsg{Text: "Live agent/hub stream connected", Level: "ok"}
			} else {
				m.eventCh <- StatusMsg{Text: "Connected to orchestrator", Level: "ok"}
			}

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
		for id := range m.viewports {
			m.renderDirty[id] = true
		}
		for id, vp := range m.viewports {
			vp.Width = contentWidth(m.width, 4)
			vp.Height = m.transcriptHeight()
			vp.SetContent(m.renderAgentOutput(id, vp.Width))
			m.viewports[id] = vp
		}

	case tea.MouseMsg:
		if len(m.agents) > 0 {
			switch msg.Button {
			case tea.MouseButtonWheelUp:
				m.scrollActive(-3)
				return m, nil
			case tea.MouseButtonWheelDown:
				m.scrollActive(3)
				return m, nil
			}
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

		switch msg.String() {
		case "left":
			m.PrevAgent()
			return m, nil
		case "right":
			m.NextAgent()
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
			switch msg.String() {
			case "pgup", "alt+up", "ctrl+up":
				m.scrollActive(-5)
				return m, nil
			case "pgdown", "pgdn", "alt+down", "ctrl+down":
				m.scrollActive(5)
				return m, nil
			}
		} else {
			switch msg.String() {
			case "h":
				m.PrevAgent()
				return m, nil
			case "l":
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
				m.scrollActive(1)
				return m, nil
			case "pgup":
				m.scrollActive(-5)
				return m, nil
			case "pgdown", "pgdn":
				m.scrollActive(5)
				return m, nil
			}
		}

		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		cmds = append(cmds, cmd)
		m.syncInputSize()

	case StatusMsg:
		cmds = append(cmds, m.readNextCmd())

	case agentListTickMsg:
		cmds = append(cmds, m.fetchAgentsCmd(), m.agentListTickCmd())

	case agentListMsg:
		if msg.Err == nil {
			for _, agent := range msg.Agents {
				m.ensureAgent(agent.AgentID)
				meta := m.agentMeta[agent.AgentID]
				meta.LastKind = "status"
				meta.LastStatus = strings.TrimSpace(agent.State)
				meta.LastLevel = "info"
				m.agentMeta[agent.AgentID] = meta
			}
		}

	case agentHistorySnapshotMsg:
		if msg.Err != nil {
			m.appendSystemLine("History snapshot failed: " + msg.Err.Error())
			cmds = append(cmds, func() tea.Msg {
				return StatusMsg{Text: "Agent history snapshot failed: " + msg.Err.Error(), Level: "error"}
			})
			return m, tea.Batch(cmds...)
		}
		if m.verbose {
			cmds = append(cmds, func() tea.Msg {
				return StatusMsg{Text: fmt.Sprintf("Agent history snapshot loaded: %d event(s)", len(msg.Messages)), Level: "info"}
			})
		}
		normalizer := &tuiEventNormalizer{}
		for _, raw := range msg.Messages {
			for _, event := range normalizer.Normalize(raw) {
				if event.Audience != tuiAudienceLogs {
					continue
				}
				id := event.AgentID
				if !isDisplayAgent(id) {
					continue
				}
				m.ensureAgent(id)
				m.agentOutput[id] = append(m.agentOutput[id], formatLogEvent(event))
				if len(m.agentOutput[id]) > 500 {
					m.agentOutput[id] = m.agentOutput[id][len(m.agentOutput[id])-500:]
				}
				m.renderDirty[id] = true
				meta := m.agentMeta[id]
				meta.LastKind = event.Kind
				meta.LastStatus = event.Content
				meta.LastLevel = event.Level
				m.agentMeta[id] = meta
			}
		}
		for id, vp := range m.viewports {
			vp.SetContent(m.renderAgentOutput(id, vp.Width))
			vp.GotoBottom()
			m.viewports[id] = vp
		}
		cmds = append(cmds, m.startStreamCmd(msg.AsOf), m.readNextCmd())

	case tuiEventMsg:
		if msg.Audience != tuiAudienceLogs {
			cmds = append(cmds, m.readNextCmd())
			return m, tea.Batch(cmds...)
		}
		id := msg.AgentID
		if !isDisplayAgent(id) {
			cmds = append(cmds, m.readNextCmd())
			return m, tea.Batch(cmds...)
		}
		m.ensureAgent(id)

		m.agentOutput[id] = append(m.agentOutput[id], formatLogEvent(msg))
		if len(m.agentOutput[id]) > 500 {
			m.agentOutput[id] = m.agentOutput[id][len(m.agentOutput[id])-500:]
		}
		m.renderDirty[id] = true

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

func isDisplayAgent(agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	if agentID == "user" || agentID == "orchestrator" || agentID == "context_prompt" || agentID == "system" {
		return false
	}
	return agentID == "coordinator" || agentID == "architect" || strings.HasPrefix(agentID, "agent-")
}

func (m *MultiLogModel) ensureAgent(id string) {
	if !isDisplayAgent(id) {
		return
	}
	if _, exists := m.viewports[id]; exists {
		return
	}
	previousActive := m.activeAgentID()
	m.agents = append(m.agents, id)
	sort.SliceStable(m.agents, func(i, j int) bool {
		return agentSortRank(m.agents[i]) < agentSortRank(m.agents[j]) ||
			(agentSortRank(m.agents[i]) == agentSortRank(m.agents[j]) && m.agents[i] < m.agents[j])
	})
	if previousActive != "" {
		m.activeIdx = indexOfString(m.agents, previousActive)
		if m.activeIdx < 0 {
			m.activeIdx = 0
		}
	}
	vp := viewport.New(contentWidth(m.width, 4), m.transcriptHeight())
	m.viewports[id] = vp
	m.ensureActiveTabVisible()
}

func agentSortRank(agentID string) int {
	switch agentID {
	case "coordinator":
		return 0
	case "architect":
		return 1
	}
	if strings.HasPrefix(agentID, "agent-") {
		return 2
	}
	return 3
}

func (m *MultiLogModel) transcriptHeight() int {
	h := m.height - m.input.Height() - 7
	if h < 1 {
		h = 1
	}
	return h
}

func (m *MultiLogModel) scrollActive(delta int) {
	if len(m.agents) == 0 {
		return
	}
	activeID := m.agents[m.activeIdx]
	vp := m.viewports[activeID]
	if delta < 0 {
		vp.LineUp(-delta)
	} else if delta > 0 {
		vp.LineDown(delta)
	}
	m.viewports[activeID] = vp
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
		return fmt.Errorf("%s", resp.Error)
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
	m.renderDirty[id] = true
	vp := m.viewports[id]
	vp.SetContent(m.renderAgentOutput(id, vp.Width))
	m.viewports[id] = vp
}

func (m *MultiLogModel) renderAgentOutput(id string, width int) string {
	if cached, ok := m.renderCache[id]; ok && !m.renderDirty[id] {
		return cached
	}
	wrapWidth := contentWidth(width, 2)
	var wrappedLines []string
	for _, rawLine := range m.agentOutput[id] {
		wrappedLines = append(wrappedLines, wrapPlain(rawLine, wrapWidth))
	}
	rendered := strings.Join(wrappedLines, "\n")
	m.renderCache[id] = rendered
	m.renderDirty[id] = false
	return rendered
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
	tabs := m.renderAgentTabs(contentWidth(m.width, 2))

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
		scrollbar = sb.String()
	}

	inputBorder := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("236")).
		Width(contentWidth(m.width, 2)).
		Render(m.input.View())

	content := vpView
	if scrollbar != "" {
		content = lipgloss.JoinHorizontal(lipgloss.Top, vpView, scrollbar)
	}
	content = border.Render(content)
	return strings.Join([]string{tabs, header, content, inputBorder}, "\n")
}

func (m *MultiLogModel) renderAgentTabs(maxWidth int) string {
	if len(m.agents) == 0 {
		return ""
	}

	tabFor := func(id string, selected bool) string {
		label := truncateToWidth(id, 18)
		style := lipgloss.NewStyle().Padding(0, 1)
		if selected {
			style = style.Background(lipgloss.Color("39")).Foreground(lipgloss.Color("255")).Bold(true)
		} else {
			style = style.Background(lipgloss.Color("236")).Foreground(lipgloss.Color("248"))
		}
		return style.Render(label)
	}

	tabs := make([]string, len(m.agents))
	widths := make([]int, len(m.agents))
	for i, id := range m.agents {
		tabs[i] = tabFor(id, i == m.activeIdx)
		widths[i] = lipgloss.Width(tabs[i])
	}

	ellipsis := lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render("…")
	ellipsisW := lipgloss.Width(ellipsis)
	spacerW := 1

	start := m.tabStartIdx
	if start < 0 {
		start = 0
	}
	if start > m.activeIdx {
		start = m.activeIdx
	}

	end := start
	total := 0
	for end < len(tabs) {
		nextW := widths[end]
		if end > start {
			nextW += spacerW
		}
		if total+nextW > maxWidth {
			break
		}
		total += nextW
		end++
	}

	if m.activeIdx >= end {
		start = m.activeIdx
		end = start
		total = 0
		for end < len(tabs) {
			nextW := widths[end]
			if end > start {
				nextW += spacerW
			}
			if total+nextW > maxWidth {
				break
			}
			total += nextW
			end++
		}
	}

	if start > 0 {
		total += ellipsisW + spacerW
	}
	if end < len(tabs) {
		total += ellipsisW + spacerW
	}

	for total > maxWidth && start < end {
		if start < m.activeIdx {
			total -= widths[start]
			if start < end-1 {
				total -= spacerW
			}
			start++
			if start > 0 {
				total += spacerW
			}
			continue
		}
		total -= widths[end-1]
		if end-start > 1 {
			total -= spacerW
		}
		end--
	}

	m.tabStartIdx = start
	m.ensureActiveTabVisible()

	var parts []string
	if start > 0 {
		parts = append(parts, ellipsis)
	}
	for i := start; i < end; i++ {
		parts = append(parts, tabs[i])
	}
	if end < len(tabs) {
		parts = append(parts, ellipsis)
	}
	return strings.Join(parts, " ")
}

func (m *MultiLogModel) ensureActiveTabVisible() {
	if m.activeIdx < 0 || m.activeIdx >= len(m.agents) {
		return
	}
	if m.tabStartIdx > m.activeIdx {
		m.tabStartIdx = m.activeIdx
	}
}
