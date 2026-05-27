package dashboard

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"aion-kernel/internal/hub"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type hubEventMsg struct {
	Text       string
	Level      string
	Diagnostic bool
}
type hubSnapshotMsg struct {
	Messages []hub.Message
	AsOf     time.Time
	Err      error
}
type streamHubClosedMsg struct{}

type HubModel struct {
	addr      string
	viewport  viewport.Model
	eventCh   chan hubEventMsg
	lines     []string
	err       error
	connected bool
	Focused   bool
	verbose   bool
}

func NewHubModel(addr string) *HubModel {
	return NewHubModelWithOptions(addr, Options{})
}

func NewHubModelWithOptions(addr string, options Options) *HubModel {
	vp := viewport.New(80, 10)
	vp.SetContent("Waiting for Hub Events...")
	return &HubModel{
		addr:      addr,
		viewport:  vp,
		eventCh:   make(chan hubEventMsg, 100),
		connected: false,
		verbose:   options.Verbose,
	}
}

func (m *HubModel) Init() tea.Cmd {
	return m.fetchSnapshotCmd()
}

func (m *HubModel) fetchSnapshotCmd() tea.Cmd {
	return func() tea.Msg {
		reqBytes, _ := json.Marshal(map[string]interface{}{
			"method": "hub-snapshot",
			"id":     "hub-snapshot-1",
			"params": map[string]interface{}{},
		})

		conn, err := net.Dial("tcp", m.addr)
		if err != nil {
			return hubSnapshotMsg{Err: err}
		}
		defer conn.Close()

		if _, err := conn.Write(reqBytes); err != nil {
			return hubSnapshotMsg{Err: err}
		}

		var resp struct {
			Result struct {
				Messages []hub.Message `json:"messages"`
				AsOf     time.Time     `json:"as_of"`
			} `json:"result"`
			Error string `json:"error"`
		}
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			return hubSnapshotMsg{Err: err}
		}
		if resp.Error != "" {
			return hubSnapshotMsg{Err: fmt.Errorf("%s", resp.Error)}
		}
		return hubSnapshotMsg{Messages: resp.Result.Messages, AsOf: resp.Result.AsOf}
	}
}

func (m *HubModel) startLiveStreamCmd(since time.Time) tea.Cmd {
	return func() tea.Msg {
		go func() {
			req := map[string]interface{}{
				"method": "tail-hub-events",
				"id":     "hub-1",
				"params": map[string]interface{}{"since": since},
			}
			reqBytes, _ := json.Marshal(req)

			conn, err := net.Dial("tcp", m.addr)
			if err != nil {
				m.eventCh <- hubEventMsg{Text: fmt.Sprintf("Hub stream dial failed: %v", err), Level: "error", Diagnostic: true}
				close(m.eventCh)
				return
			}
			defer conn.Close()

			if _, err := conn.Write(reqBytes); err != nil {
				m.eventCh <- hubEventMsg{Text: fmt.Sprintf("Hub stream request failed: %v", err), Level: "error", Diagnostic: true}
				close(m.eventCh)
				return
			}
			if m.verbose {
				m.eventCh <- hubEventMsg{Text: "Hub live stream connected", Level: "ok", Diagnostic: true}
			}

			decoder := json.NewDecoder(conn)
			for {
				var raw map[string]interface{}
				if err := decoder.Decode(&raw); err != nil {
					close(m.eventCh)
					break
				}
				b, _ := json.Marshal(raw)
				m.eventCh <- hubEventMsg{Text: string(b)}
			}
		}()
		return nil
	}
}

func (m *HubModel) readNextCmd() tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-m.eventCh
		if !ok {
			return streamHubClosedMsg{}
		}
		return msg
	}
}

func (m *HubModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
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
	case hubSnapshotMsg:
		if msg.Err != nil {
			m.lines = append(m.lines, "Error loading hub snapshot: "+msg.Err.Error())
			m.renderLines()
			return m, func() tea.Msg { return StatusMsg{Text: "Hub snapshot failed: " + msg.Err.Error(), Level: "error"} }
		}
		if m.verbose {
			m.lines = append(m.lines, fmt.Sprintf("[diagnostic] Hub snapshot loaded: %d event(s)", len(msg.Messages)))
			cmds = append(cmds, func() tea.Msg {
				return StatusMsg{Text: fmt.Sprintf("Hub snapshot loaded: %d event(s); opening live stream", len(msg.Messages)), Level: "info"}
			})
		}
		for _, raw := range msg.Messages {
			b, _ := json.Marshal(raw)
			m.lines = append(m.lines, string(b))
		}
		if len(m.lines) > 1000 {
			m.lines = m.lines[len(m.lines)-1000:]
		}
		m.renderLines()
		m.viewport.GotoBottom()
		cmds = append(cmds, m.startLiveStreamCmd(msg.AsOf), m.readNextCmd())
	case hubEventMsg:
		if msg.Diagnostic {
			if m.verbose {
				m.lines = append(m.lines, "[diagnostic] "+msg.Text)
				m.renderLines()
				m.viewport.GotoBottom()
			}
			cmds = append(cmds, func() tea.Msg {
				return StatusMsg{Text: msg.Text, Level: msg.Level}
			}, m.readNextCmd())
			return m, tea.Batch(cmds...)
		}
		m.connected = true
		// Parse the raw hub message
		var hMsg hub.Message
		if err := json.Unmarshal([]byte(msg.Text), &hMsg); err != nil {
			// Fallback: check if it's wrapped in an RPC result envelope (unlikely but safe)
			var envelope struct {
				Result hub.Message `json:"result"`
			}
			if err := json.Unmarshal([]byte(msg.Text), &envelope); err == nil {
				hMsg = envelope.Result
			} else {
				return m, nil
			}
		}

		if hMsg.Type == hub.MsgSystemStatus {
			var p hub.SystemStatusPayload
			if err := json.Unmarshal(hMsg.Payload, &p); err == nil {
				cmds = append(cmds, func() tea.Msg {
					return StatusMsg{Text: p.Text, Level: p.Level}
				})
			}
		}

		// Format and show in hub log regardless of type.
		// Try to extract only the message part if it's a full RPC result
		cleanText := msg.Text
		var rpcResp struct {
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal([]byte(msg.Text), &rpcResp); err == nil && len(rpcResp.Result) > 0 {
			cleanText = string(rpcResp.Result)
		}

		m.lines = append(m.lines, cleanText)
		if len(m.lines) > 1000 {
			m.lines = m.lines[len(m.lines)-1000:]
		}

		m.renderLines()
		m.viewport.GotoBottom()
		cmds = append(cmds, m.readNextCmd())
	case streamHubClosedMsg:
		m.lines = append(m.lines, "[HUB STREAM DISCONNECTED]")
		m.viewport.SetContent(strings.Join(m.lines, "\n"))
		m.viewport.GotoBottom()
	}

	vp, cmd := m.viewport.Update(msg)
	m.viewport = vp
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m *HubModel) renderLines() {
	if len(m.lines) == 0 {
		m.viewport.SetContent("Waiting for Hub Events...")
		return
	}
	wrapW := contentWidth(m.viewport.Width, 2)
	var wrapped []string
	for _, l := range m.lines {
		wrapped = append(wrapped, wrapPlain(l, wrapW))
	}
	m.viewport.SetContent(strings.Join(wrapped, "\n"))
}

func (m *HubModel) View() string {
	borderColor := lipgloss.Color("240")
	if m.Focused {
		borderColor = lipgloss.Color("205")
	}

	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(0, 1).
		Width(contentWidth(m.viewport.Width, 0))

	title := lipgloss.NewStyle().
		Foreground(lipgloss.Color("255")).
		Background(lipgloss.Color("62")).
		Padding(0, 1).
		Render(" Aion Context Hub ")

	content := border.Render(m.viewport.View())
	return title + "\n" + content
}
