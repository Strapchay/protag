package dashboard

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type chatLine struct {
	AgentID     string
	Author      string
	Text        string // Raw full text
	Style       lipgloss.Style
	Type        string // thinking, text, tool_start
	IsCollapsed bool
	CanCollapse bool

	// Cached state for the virtualizer
	CachedRender string
	LineCount    int
}

type ChatModel struct {
	addr            string
	viewport        viewport.Model
	input           textarea.Model
	history         []chatLine
	width           int
	height          int
	Focused         bool
	FocusHistory    bool
	autoScroll      bool
	logger          *log.Logger
	buildSpecActive bool
}

type architectCommand struct {
	Name        string
	Description string
}

var architectCommands = []architectCommand{
	{Name: "/build-spec", Description: "hand off finalized docs/build_spec.md to the Coordinator"},
	{Name: "/status", Description: "show current Solution Architect session state"},
	{Name: "/resume", Description: "reconcile live Architect context from persisted session state"},
	{Name: "/retry", Description: "retry the last safe failed/timed-out Architect request"},
	{Name: "/continue", Description: "ask the Architect to continue from current restored context"},
	{Name: "/show-spec", Description: "show docs/build_spec.md"},
	{Name: "/show-plan", Description: "show the current build-spec plan"},
	{Name: "/show-build-spec-trace", Description: "show the current build-spec planning trace"},
	{Name: "/coordinator-status", Description: "show the current build-spec attempt state"},
	{Name: "/clear", Description: "clear visible chat history only"},
	{Name: "/reset-session", Description: "delete current run state and start a fresh Architect session"},
	{Name: "/replan", Description: "trigger DAG replan"},
	{Name: "/revive", Description: "revive a stopped/crashed agent"},
	{Name: "/msg", Description: "send a message to a specific agent"},
	{Name: "/help", Description: "show this command list"},
}

func NewChatModel(addr string) *ChatModel {
	ti := textarea.New()
	ti.Placeholder = "Type a message for the Solution Architect... (Press 'ESC' to unfocus)"
	ti.Focus()
	ti.CharLimit = 0
	ti.ShowLineNumbers = false
	ti.Prompt = ""
	ti.SetWidth(80)
	ti.SetHeight(1)

	vp := viewport.New(80, 5)
	vp.SetContent("Awaiting interaction with Solution Architect...")

	f, err := os.OpenFile("tui_debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	var logger *log.Logger
	if err == nil {
		logger = log.New(f, "[TUI] ", log.Ltime|log.Lmicroseconds)
		logger.Println("--- TUI Session Started ---")
	}

	return &ChatModel{
		addr:       addr,
		viewport:   vp,
		input:      ti,
		history:    []chatLine{},
		autoScroll: true,
		logger:     logger,
	}
}

func (m *ChatModel) Init() tea.Cmd {
	return textarea.Blink
}

func (m *ChatModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.MouseMsg:
		if msg.Button == tea.MouseButtonWheelUp {
			m.autoScroll = false
		} else if msg.Button == tea.MouseButtonWheelDown && m.viewport.AtBottom() {
			m.autoScroll = true
		}
	case tea.KeyMsg:
		if m.logger != nil {
			m.logger.Printf("Key: %q (Type: %v), Focused: %v, FocusHistory: %v, AutoScroll: %v",
				msg.String(), msg.Type, m.input.Focused(), m.FocusHistory, m.autoScroll)
		}

		if msg.String() == "tab" {
			m.FocusHistory = !m.FocusHistory
			if m.FocusHistory {
				m.input.Blur()
			} else {
				m.input.Focus()
			}
			return m, nil
		}

		inputFocused := m.input.Focused()

		// Intercept history navigation only when history is focused or the key is
		// explicitly a history-scroll command. Plain Up/Down remain available to
		// the textarea while composing.
		switch msg.String() {
		case "up":
			if inputFocused && !m.FocusHistory {
				break
			}
			m.autoScroll = false
			m.viewport.LineUp(1)
			if m.FocusHistory {
				return m, nil
			}
		case "down":
			if inputFocused && !m.FocusHistory {
				break
			}
			m.viewport.LineDown(1)
			if m.viewport.AtBottom() {
				m.autoScroll = true
			}
			if m.FocusHistory {
				return m, nil
			}
		case "alt+up", "ctrl+up":
			m.autoScroll = false
			m.viewport.LineUp(1)
			return m, nil
		case "alt+down", "ctrl+down":
			m.viewport.LineDown(1)
			if m.viewport.AtBottom() {
				m.autoScroll = true
			}
			return m, nil
		case "pgup":
			if inputFocused && !m.FocusHistory {
				break
			}
			m.autoScroll = false
			m.viewport.ViewUp()
			if m.FocusHistory {
				return m, nil
			}
		case "pgdn":
			if inputFocused && !m.FocusHistory {
				break
			}
			m.viewport.ViewDown()
			if m.viewport.AtBottom() {
				m.autoScroll = true
			}
			if m.FocusHistory {
				return m, nil
			}
		case "ctrl+x":
			m.ToggleAllCollapse()
			return m, nil
		}

		if m.input.Focused() {
			switch msg.Type {
			case tea.KeyEnter:
				v := m.input.Value()
				v = strings.TrimSpace(v)
				m.input.SetValue("")
				m.syncInputHeight()
				m.syncViewportSize()
				if v != "" {
					m.AddMessage("user", "User", v, lipgloss.NewStyle().Foreground(lipgloss.Color("82")), "text")
					go m.executeCommand(v)
					if strings.HasPrefix(v, "/build-spec") {
						m.buildSpecActive = true
					}
				}
				// Force autoscroll on new message
				m.autoScroll = true
				return m, nil
			case tea.KeyEsc:
				if m.buildSpecActive {
					go m.executeCommand("/build-spec-cancel")
					m.buildSpecActive = false
					return m, nil
				}
				m.input.Blur()
				return m, nil
			}
		}
	case tuiEventMsg:
		if msg.Audience != tuiAudienceChat {
			return m, nil
		}

		// Skip echoes
		if msg.AgentID != "user" && msg.Role == "user" && len(m.history) > 0 {
			last := m.history[len(m.history)-1]
			if last.Author == "User" && last.Text == msg.Content {
				return m, nil
			}
		}

		// Determine author and style
		author := msg.Author
		if author == "" {
			author = "Architect"
		}
		authorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("39"))

		if author == "User" {
			authorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("82"))
		} else if author == "Architect" || msg.Role == "assistant" {
			authorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
		} else {
			authorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
		}

		displayText := msg.Content
		msgType := msg.Kind
		if msgType == "" {
			msgType = tuiKindText
		}

		if msgType == tuiKindThinking {
			author = "Architect Thinking"
			authorStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("240")).
				Italic(true)
		}

		// Format tool_start for chat specifically
		if msgType == tuiKindToolStart {
			summary := msg.Summary
			if summary == "" {
				summary = fmt.Sprintf("%s(%s)", msg.Tool, msg.Input)
			}
			displayText = lipgloss.NewStyle().
				Foreground(lipgloss.Color("208")).
				Bold(true).
				Render(summary)
		}

		m.AddMessage(msg.AgentID, author, displayText, authorStyle, msgType)
		if msg.AgentID == "orchestrator" {
			lower := strings.ToLower(msg.Content)
			switch {
			case strings.Contains(lower, "build-spec planning and allocation complete"),
				strings.Contains(lower, "build-spec planning canceled"),
				strings.Contains(lower, "planning failed"),
				strings.Contains(lower, "allocation failed"),
				strings.Contains(lower, "build-spec failed"):
				m.buildSpecActive = false
			}
		}
		return m, nil

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.input.SetWidth(contentWidth(msg.Width, 4))
		m.RebuildAllCache()
	}

	m.syncInputHeight()
	m.syncViewportSize()

	// Update children
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	cmds = append(cmds, cmd)
	m.syncInputHeight()
	m.syncViewportSize()

	// Update viewport.
	switch msg.(type) {
	case tea.KeyMsg:
		// While composing, keyboard input belongs only to the textarea. History
		// scrolling is handled explicitly above for PageUp/PageDown and modified
		// arrow keys, so ordinary editing keys cannot move the scrollbar.
		if m.FocusHistory || !m.input.Focused() {
			m.viewport, cmd = m.viewport.Update(msg)
			cmds = append(cmds, cmd)
		}
	default:
		// Non-key messages (WindowSizeMsg, MouseMsg, etc.) always go to viewport
		m.viewport, cmd = m.viewport.Update(msg)
		cmds = append(cmds, cmd)
	}

	if mouseMsg, ok := msg.(tea.MouseMsg); ok && mouseMsg.Button == tea.MouseButtonWheelDown && m.viewport.AtBottom() {
		m.autoScroll = true
	}

	// Update placeholder based on scroll state
	if !m.autoScroll {
		m.input.Placeholder = "History Mode (Scroll down to resume auto-scroll)"
	} else {
		m.input.Placeholder = "Type a message for the Solution Architect... (Press 'ESC' to unfocus)"
	}

	return m, tea.Batch(cmds...)
}

func (m *ChatModel) AddMessage(agentID, author, text string, style lipgloss.Style, msgType string) {
	// Update existing message if it's a stream continuation from the same agent/author.
	if len(m.history) > 0 {
		lastIdx := len(m.history) - 1
		last := &m.history[lastIdx]

		// Allow streaming for architects, users, the orchestrator, and the initial context prompt chunks
		isStreamable := agentID == "architect" || agentID == "user" || agentID == "orchestrator" || agentID == "context_prompt"
		if isStreamable && last.AgentID == agentID && last.Author == author && last.Type == msgType && (msgType == "text" || msgType == "thinking") {
			if len(text) > len(last.Text) && strings.HasPrefix(text, last.Text[:len(last.Text)/2]) {
				last.Text = text
				last.CachedRender = "" // Invalidate cache
				m.MeasureLine(last)
				m.renderHistory()
				return
			}
		}
	}

	if m.logger != nil {
		m.logger.Printf("Append Message: %s (%s), Type: %s, Len: %d", author, agentID, msgType, len(text))
	}
	newLine := chatLine{
		AgentID: agentID,
		Author:  author,
		Text:    text,
		Style:   style,
		Type:    msgType,
	}

	// Smarter collapsing:
	// 1. NEVER collapse actual content (text blocks) by default.
	// 2. ALWAYS collapse thinking blocks by default if they are long.
	lines := strings.Split(text, "\n")
	newLine.CanCollapse = len(lines) > 25

	if msgType == "thinking" {
		newLine.IsCollapsed = newLine.CanCollapse
	} else if msgType == tuiKindToolStart {
		newLine.CanCollapse = true
		newLine.IsCollapsed = true
	} else {
		newLine.IsCollapsed = false
	}

	m.MeasureLine(&newLine)
	m.history = append(m.history, newLine)

	if len(m.history) > 500 {
		m.history = m.history[len(m.history)-500:]
	}
	m.renderHistory()
}

func (m *ChatModel) MeasureLine(line *chatLine) {
	wrapW := contentWidth(m.width, 6)

	authorStr := line.Style.Bold(true).Render(line.Author + ": ")
	prefixW := lipgloss.Width(line.Author + ": ")
	contentW := contentWidth(wrapW, prefixW)
	content := wrapPlain(line.Text, contentW)
	if line.Type == tuiKindThinking {
		content = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245")).
			Italic(true).
			Render(content)
	}

	// Handle collapsing
	if line.IsCollapsed {
		lines := strings.Split(content, "\n")
		previewLines := 25
		if line.Type == tuiKindToolStart {
			previewLines = 1
		}
		if len(lines) > previewLines {
			content = strings.Join(lines[:previewLines], "\n") + fmt.Sprintf("\n\n%s",
				lipgloss.NewStyle().Foreground(lipgloss.Color("242")).Italic(true).Render(
					fmt.Sprintf("[... %d more lines. Press Ctrl+X to expand.]", len(lines)-previewLines)))
		} else if line.Type == tuiKindToolStart {
			content = strings.Join(lines[:minInt(len(lines), previewLines)], "\n") + "\n" +
				lipgloss.NewStyle().Foreground(lipgloss.Color("242")).Italic(true).Render("[operation collapsed. Press Ctrl+X to expand.]")
		}
	} else {
		// Update CanCollapse flag even if expanded
		lines := strings.Split(content, "\n")
		if line.Type == tuiKindToolStart {
			line.CanCollapse = true
		} else {
			line.CanCollapse = len(lines) > 25
		}
	}

	// Add separator
	separator := lipgloss.NewStyle().Foreground(lipgloss.Color("236")).Render(strings.Repeat("─", wrapW))

	line.CachedRender = authorStr + indentContinuation(content, prefixW) + "\n" + separator + "\n\n"
	line.LineCount = strings.Count(line.CachedRender, "\n")
}

func (m *ChatModel) syncInputHeight() {
	wrapW := contentWidth(m.width, 6)

	lines := wrappedLineCount(m.input.Value(), wrapW)
	maxHeight := m.height / 3
	if maxHeight < 3 {
		maxHeight = 3
	}
	if maxHeight > 12 {
		maxHeight = 12
	}
	if lines > maxHeight {
		lines = maxHeight
	}
	if lines < 1 {
		lines = 1
	}
	m.input.SetHeight(lines)
	m.input.SetWidth(contentWidth(m.width, 4))
}

func (m *ChatModel) syncViewportSize() {
	inputHeight := m.input.Height()
	vpHeight := m.height - inputHeight - 4 - m.commandSuggestionsHeight()
	if vpHeight < 1 {
		vpHeight = 1
	}
	m.viewport.Width = contentWidth(m.width, 4)
	m.viewport.Height = vpHeight
}

func wrappedLineCount(value string, wrapW int) int {
	if value == "" {
		return 1
	}
	lines := 0
	for _, rawLine := range strings.Split(value, "\n") {
		w := lipgloss.Width(rawLine)
		if w == 0 {
			lines++
		} else {
			lines += (w + wrapW - 1) / wrapW
		}
	}
	if lines < 1 {
		return 1
	}
	return lines
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func indentContinuation(text string, prefixWidth int) string {
	lines := strings.Split(text, "\n")
	if len(lines) <= 1 {
		return text
	}
	pad := strings.Repeat(" ", prefixWidth)
	for i := 1; i < len(lines); i++ {
		lines[i] = pad + lines[i]
	}
	return strings.Join(lines, "\n")
}

func (m *ChatModel) RebuildAllCache() {
	for i := range m.history {
		m.MeasureLine(&m.history[i])
	}
	m.renderHistory()
}

func (m *ChatModel) ToggleAllCollapse() {
	for i := range m.history {
		if m.history[i].CanCollapse {
			m.history[i].IsCollapsed = !m.history[i].IsCollapsed
			m.MeasureLine(&m.history[i])
		}
	}
	m.renderHistory()
}

func (m *ChatModel) renderHistory() {
	var sb strings.Builder
	for _, line := range m.history {
		sb.WriteString(line.CachedRender)
	}

	contentStr := sb.String()
	totalLines := strings.Count(contentStr, "\n")

	if m.logger != nil {
		m.logger.Printf("Render: Total Lines: %d, VP Height: %d, YOffset: %d, AtBottom: %v, AutoScroll: %v",
			totalLines, m.viewport.Height, m.viewport.YOffset, m.viewport.AtBottom(), m.autoScroll)
	}

	m.viewport.SetContent(contentStr)
	if m.autoScroll {
		m.viewport.GotoBottom()
	}
}

func (m *ChatModel) executeCommand(cmdStr string) {
	parts := strings.SplitN(cmdStr, " ", 3)
	if len(parts) == 0 {
		return
	}

	command := parts[0]
	var methodName string
	var params map[string]interface{}
	showResponse := false
	clearAfterResponse := false

	switch command {
	case "/help", "/commands":
		m.AddMessage("system", "TUI", architectCommandHelp(), lipgloss.NewStyle().Foreground(lipgloss.Color("39")), tuiKindText)
		return
	case "/clear":
		m.history = nil
		m.viewport.SetContent("")
		return
	case "/msg":
		if len(parts) < 3 {
			return // needs agent_id and text
		}
		methodName = "send-message"
		params = map[string]interface{}{
			"agent_id": parts[1],
			"text":     parts[2],
		}
	case "/build-spec":
		methodName = "send-message"
		params = map[string]interface{}{
			"agent_id": "orchestrator",
			"text":     cmdStr,
		}
	case "/retry":
		methodName = "architect-retry"
		params = map[string]interface{}{}
	case "/continue":
		methodName = "architect-continue"
		params = map[string]interface{}{}
	case "/resume":
		methodName = "architect-resume"
		params = map[string]interface{}{}
		showResponse = true
	case "/status":
		methodName = "architect-status"
		params = map[string]interface{}{}
		showResponse = true
	case "/show-spec":
		methodName = "architect-show-spec"
		params = map[string]interface{}{}
		showResponse = true
	case "/show-plan":
		methodName = "build-spec-show-plan"
		params = map[string]interface{}{}
		showResponse = true
	case "/show-build-spec-trace":
		methodName = "build-spec-show-trace"
		params = map[string]interface{}{}
		showResponse = true
	case "/coordinator-status":
		methodName = "build-spec-status"
		params = map[string]interface{}{}
		showResponse = true
	case "/reset-session":
		methodName = "architect-reset"
		params = map[string]interface{}{}
		showResponse = true
		clearAfterResponse = true
	case "/build-spec-cancel":
		methodName = "build-spec-cancel"
		params = map[string]interface{}{}
		showResponse = true
	case "/revive":
		if len(parts) < 2 {
			return
		}
		methodName = "revive-agent"
		params = map[string]interface{}{
			"agent_id": parts[1],
		}
	case "/replan":
		methodName = "trigger-replan"
		params = map[string]interface{}{}
	default:
		if strings.HasPrefix(command, "/") {
			m.AddMessage("system", "TUI", fmt.Sprintf("Unknown command %q.\n\n%s", command, architectCommandHelp()), lipgloss.NewStyle().Foreground(lipgloss.Color("214")), tuiKindText)
			return
		}
		// Default: send raw text to orchestrator (Solution Architect)
		methodName = "send-message"
		params = map[string]interface{}{
			"agent_id": "orchestrator",
			"text":     cmdStr,
		}
	}

	reqBytes, _ := json.Marshal(map[string]interface{}{
		"method": methodName,
		"id":     "chat-1",
		"params": params,
	})

	conn, err := net.Dial("tcp", m.addr)
	if err != nil {
		m.AddMessage("system", "TUI", "Command failed: "+err.Error(), lipgloss.NewStyle().Foreground(lipgloss.Color("196")), tuiKindText)
		return
	}
	defer conn.Close()
	if _, err := conn.Write(reqBytes); err != nil {
		m.AddMessage("system", "TUI", "Command failed: "+err.Error(), lipgloss.NewStyle().Foreground(lipgloss.Color("196")), tuiKindText)
		return
	}
	if showResponse {
		var resp struct {
			Result map[string]string `json:"result"`
			Error  string            `json:"error"`
		}
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			m.AddMessage("system", "TUI", "Command response failed: "+err.Error(), lipgloss.NewStyle().Foreground(lipgloss.Color("196")), tuiKindText)
			return
		}
		if resp.Error != "" {
			m.AddMessage("system", "TUI", resp.Error, lipgloss.NewStyle().Foreground(lipgloss.Color("196")), tuiKindText)
			return
		}
		text := resp.Result["status"]
		if text == "" {
			text = resp.Result["spec"]
		}
		if text == "" {
			text = resp.Result["plan"]
		}
		if text == "" {
			text = resp.Result["trace"]
		}
		if text == "" {
			text = "No status returned"
		}
		if clearAfterResponse {
			m.history = nil
			m.viewport.SetContent("")
		}
		m.AddMessage("system", "TUI", text, lipgloss.NewStyle().Foreground(lipgloss.Color("39")), tuiKindText)
	}
}

func architectCommandHelp() string {
	lines := []string{"Available commands:"}
	for _, command := range architectCommands {
		lines = append(lines, command.Name+" - "+command.Description)
	}
	return strings.Join(lines, "\n")
}

func (m *ChatModel) View() string {
	inputHeight := m.input.Height()
	vpHeight := m.height - inputHeight - 4 - m.commandSuggestionsHeight()
	if vpHeight < 1 {
		vpHeight = 1
	}

	// Ensure viewport knows its size
	m.viewport.Width = contentWidth(m.width, 4)
	m.viewport.Height = vpHeight

	vpView := m.viewport.View()
	scrollbar := ""
	if m.viewport.TotalLineCount() > m.viewport.Height {
		percent := m.viewport.ScrollPercent()
		if math.IsNaN(percent) {
			percent = 0
		}
		thumbPos := int(math.Round(percent * float64(vpHeight-1)))
		if thumbPos < 0 {
			thumbPos = 0
		}
		if thumbPos >= vpHeight {
			thumbPos = vpHeight - 1
		}

		var ssb strings.Builder
		for i := 0; i < vpHeight; i++ {
			if i == thumbPos {
				ssb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("█"))
			} else {
				ssb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("238")).Render("│"))
			}
			if i < vpHeight-1 {
				ssb.WriteString("\n")
			}
		}
		scrollbar = ssb.String()
	}

	content := vpView
	if scrollbar != "" {
		content = lipgloss.JoinHorizontal(lipgloss.Top, vpView, scrollbar)
	}

	// Border for the history
	historyBorderColor := lipgloss.Color("236")
	if m.FocusHistory {
		historyBorderColor = lipgloss.Color("205") // Hot pink if focused
	}
	historyStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(historyBorderColor).
		Width(contentWidth(m.width, 2)).
		Height(vpHeight)

	// Border for input
	inputBorderColor := lipgloss.Color("236")
	if m.input.Focused() {
		inputBorderColor = lipgloss.Color("39") // Blue if focused
	}
	inputStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(inputBorderColor).
		Width(contentWidth(m.width, 2))

	parts := []string{
		historyStyle.Render(content),
	}
	if suggestions := m.commandSuggestionsView(); suggestions != "" {
		parts = append(parts, suggestions)
	}
	parts = append(parts, inputStyle.Render(m.input.View()))
	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

func (m *ChatModel) commandSuggestionsView() string {
	matches := m.commandSuggestionMatches()
	if len(matches) == 0 {
		return ""
	}
	var lines []string
	for _, command := range matches {
		prefix := lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Render(command.Name) + " "
		wrapped := wrapPlain(command.Description, contentWidth(m.width, lipgloss.Width(command.Name)+5))
		lines = append(lines, prefix+indentContinuation(lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(wrapped), lipgloss.Width(command.Name)+1))
	}
	return lipgloss.NewStyle().
		Border(lipgloss.NormalBorder(), true, false, false, false).
		BorderForeground(lipgloss.Color("236")).
		Width(contentWidth(m.width, 2)).
		Render(strings.Join(lines, "\n"))
}

func (m *ChatModel) commandSuggestionsHeight() int {
	matches := m.commandSuggestionMatches()
	if len(matches) == 0 {
		return 0
	}
	return len(matches) + 1
}

func (m *ChatModel) commandSuggestionMatches() []architectCommand {
	value := strings.TrimSpace(m.input.Value())
	if !strings.HasPrefix(value, "/") || strings.Contains(value, " ") {
		return nil
	}
	var matches []architectCommand
	for _, command := range architectCommands {
		if strings.HasPrefix(command.Name, value) {
			matches = append(matches, command)
		}
	}
	if len(matches) == 0 {
		return nil
	}
	if len(matches) > 6 {
		matches = matches[:6]
	}
	return matches
}
