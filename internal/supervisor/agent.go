package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"aion-kernel/internal/hub"
)

// AgentState represents the lifecycle state of a supervised agent.
type AgentState int

const (
	StateStarting AgentState = iota
	StateRunning
	StateCrashed
	StateStopped
)

func (s AgentState) String() string {
	switch s {
	case StateStarting:
		return "Starting"
	case StateRunning:
		return "Running"
	case StateCrashed:
		return "Crashed"
	case StateStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

type AgentLifecycleEvent struct {
	Kind        string
	Content     string
	ToolName    string
	ToolInput   string
	ToolSummary string
	IsError     bool
	Error       string
}

// AgentConfig configures a domain agent supervisor.
type AgentConfig struct {
	// AgentID is the unique identifier for this agent.
	AgentID string
	// DomainID is the domain this agent is responsible for.
	DomainID string
	// AssignedPaths are the filesystem paths this agent owns.
	AssignedPaths []string
	// InitialPrompt is the starting prompt for the Pi Agent.
	InitialPrompt string
	// PiAgentConfig configures the Pi Agent subprocess.
	PiAgent PiAgentConfig
	// CgroupConfig configures resource limits.
	Cgroup CgroupConfig
	// HealthConfig
	HeartbeatTimeout time.Duration
	ProgressTimeout  time.Duration
	// MaxCrashRestarts is the maximum number of crash recoveries before giving up.
	MaxCrashRestarts int
}

// AgentSupervisor manages the lifecycle of a Pi Agent subprocess.
type AgentSupervisor struct {
	mu               sync.Mutex
	config           AgentConfig
	state            AgentState
	piAgent          *PiAgentProcess
	healthMonitor    *HealthMonitor
	contextCh        chan hub.Message
	cancel           context.CancelFunc
	crashCount       int
	recentTools      []string // sliding window for loop detection
	statusFn         func(text, level string)
	broadcastFn      func(msg hub.Message)
	lifecycleFn      func(event AgentLifecycleEvent)
	pendingBroadcast []hub.Message
	lastThinking     string
	lastText         string
	isThinking       bool
}

// NewAgentSupervisor creates a new agent supervisor.
func NewAgentSupervisor(config AgentConfig) *AgentSupervisor {
	if config.MaxCrashRestarts <= 0 {
		config.MaxCrashRestarts = 3
	}
	if config.HeartbeatTimeout <= 0 {
		config.HeartbeatTimeout = 5 * time.Minute
	}
	if config.ProgressTimeout <= 0 {
		config.ProgressTimeout = 15 * time.Minute
	}
	return &AgentSupervisor{
		config:      config,
		state:       StateStarting,
		contextCh:   make(chan hub.Message, 64),
		recentTools: make([]string, 0, 10),
	}
}

// SetStatusFunc registers a callback for live status broadcasts to the TUI.
func (s *AgentSupervisor) SetStatusFunc(fn func(text, level string)) {
	s.statusFn = fn
}

// SetBroadcastFunc registers a callback for broadcasting hub messages.
func (s *AgentSupervisor) SetBroadcastFunc(fn func(msg hub.Message)) {
	s.mu.Lock()
	s.broadcastFn = fn
	pending := append([]hub.Message(nil), s.pendingBroadcast...)
	s.pendingBroadcast = nil
	s.mu.Unlock()

	go func() {
		for _, msg := range pending {
			fn(msg)
		}
	}()
}

func (s *AgentSupervisor) SetLifecycleFunc(fn func(event AgentLifecycleEvent)) {
	s.mu.Lock()
	s.lifecycleFn = fn
	s.mu.Unlock()
}

func (s *AgentSupervisor) emitStatus(text, level string) {
	if s.statusFn != nil {
		s.statusFn(text, level)
	}
}

func (s *AgentSupervisor) displayLabel() string {
	switch strings.ToLower(strings.TrimSpace(s.config.DomainID)) {
	case "coordinator":
		return "Coordinator"
	case "architect":
		return "Architect"
	case "utility":
		return "Utility Agent"
	case "":
	}
	agentID := strings.ToLower(strings.TrimSpace(s.config.AgentID))
	if strings.HasPrefix(agentID, "agent-") {
		return "Domain Agent"
	}
	if s.config.DomainID != "" {
		return strings.Title(strings.ReplaceAll(s.config.DomainID, "_", " "))
	}
	if s.config.AgentID != "" {
		return strings.Title(strings.ReplaceAll(s.config.AgentID, "_", " "))
	}
	return "Agent"
}

func (s *AgentSupervisor) emitLifecycle(event AgentLifecycleEvent) {
	s.mu.Lock()
	fn := s.lifecycleFn
	s.mu.Unlock()
	if fn != nil {
		fn(event)
	}
}

func (s *AgentSupervisor) broadcast(msg hub.Message) {
	s.mu.Lock()
	fn := s.broadcastFn
	if fn == nil {
		if len(s.pendingBroadcast) >= 128 {
			s.pendingBroadcast = s.pendingBroadcast[1:]
		}
		s.pendingBroadcast = append(s.pendingBroadcast, msg)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	fn(msg)
}

// Start launches the Pi Agent and begins supervision.
func (s *AgentSupervisor) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	if err := s.spawnAgent(ctx); err != nil {
		cancel()
		return fmt.Errorf("supervisor: start agent %s: %w", s.config.AgentID, err)
	}

	return nil
}

func (s *AgentSupervisor) spawnAgent(ctx context.Context) error {
	s.mu.Lock()
	s.state = StateStarting
	s.mu.Unlock()

	// Create cgroup
	if err := CreateCgroup(s.config.Cgroup); err != nil {
		log.Printf("supervisor: cgroup creation failed for %s: %v", s.config.AgentID, err)
	}

	// Spawn Pi Agent
	piAgent, err := SpawnPiAgent(s.config.PiAgent)
	if err != nil {
		return fmt.Errorf("spawn pi agent: %w", err)
	}

	// Assign to cgroup
	if s.config.Cgroup.Enabled {
		if err := AssignProcess(s.config.Cgroup.BasePath, s.config.AgentID, piAgent.PID()); err != nil {
			log.Printf("supervisor: cgroup assign failed for %s: %v", s.config.AgentID, err)
		}
	}

	// Create health monitor
	hm := NewHealthMonitor(
		s.config.AgentID,
		s.config.HeartbeatTimeout,
		s.config.ProgressTimeout,
	)
	hm.OnTimeout(func(agentID string, status HealthStatus) {
		log.Printf("supervisor: agent %s health timeout: %d", agentID, status)
		s.handleCrash("health timeout")
	})
	hm.Start(ctx)

	s.mu.Lock()
	s.piAgent = piAgent
	s.healthMonitor = hm
	s.state = StateRunning
	s.mu.Unlock()

	// Start event listener
	go s.eventListener(ctx)

	// Start context routing
	go s.contextRouter(ctx)

	// Start crash detection
	go s.crashDetector(ctx)

	// Send initial prompt
	if s.config.InitialPrompt != "" {
		if err := s.SendPrompt(s.config.InitialPrompt); err != nil {
			return fmt.Errorf("send initial prompt: %w", err)
		}
	}

	log.Printf("supervisor: agent %s started (pid=%d, domain=%s)", s.config.AgentID, piAgent.PID(), s.config.DomainID)
	return nil
}

// DeliverContext delivers a hub message to the Pi Agent as a follow_up.
func (s *AgentSupervisor) DeliverContext(msg hub.Message) error {
	select {
	case s.contextCh <- msg:
		return nil
	default:
		return fmt.Errorf("supervisor: context channel full for agent %s", s.config.AgentID)
	}
}

// Stop gracefully stops the agent.
func (s *AgentSupervisor) Stop() error {
	s.mu.Lock()
	if s.state == StateStopped {
		s.mu.Unlock()
		return nil
	}
	s.state = StateStopped
	piAgent := s.piAgent
	hm := s.healthMonitor
	s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
	}

	if hm != nil {
		hm.Stop()
	}

	if piAgent != nil {
		// Try graceful abort
		piAgent.SendAbort()

		// Wait briefly for graceful exit
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-piAgent.Done():
			timer.Stop()
		case <-timer.C:
			// Force kill
			piAgent.Kill()
		}
	}

	// Clean up cgroup
	DestroyCgroup(s.config.Cgroup.BasePath, s.config.AgentID)

	log.Printf("supervisor: agent %s stopped", s.config.AgentID)
	return nil
}

// SendPrompt sends an initial prompt to the Pi Agent.
func (s *AgentSupervisor) SendPrompt(message string) error {
	s.mu.Lock()
	s.lastThinking = ""
	s.lastText = ""
	pa := s.piAgent
	s.mu.Unlock()
	if pa == nil {
		return fmt.Errorf("agent not started")
	}
	return pa.SendPrompt(message)
}

// SendFollowUp sends a follow-up message to the Pi Agent.
func (s *AgentSupervisor) SendFollowUp(message string) error {
	s.mu.Lock()
	s.lastThinking = ""
	s.lastText = ""
	pa := s.piAgent
	thinking := s.isThinking
	s.mu.Unlock()
	if pa == nil {
		return fmt.Errorf("agent not started")
	}

	if thinking {
		return pa.SendFollowUp(message)
	}
	// If idle, use prompt to trigger a new turn immediately
	return pa.SendPrompt(message)
}

// SendSteer sends a steer command to the Pi Agent.
func (s *AgentSupervisor) SendSteer(message string) error {
	s.mu.Lock()
	s.lastThinking = ""
	s.lastText = ""
	pa := s.piAgent
	thinking := s.isThinking
	s.mu.Unlock()
	if pa == nil {
		return fmt.Errorf("agent not started")
	}

	if thinking {
		return pa.SendSteer(message)
	}
	// If idle, use prompt to force processing of the steer
	return pa.SendPrompt("STEER: " + message)
}

// State returns the current agent state.
func (s *AgentSupervisor) State() AgentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// AgentID returns the agent's unique identifier.
func (s *AgentSupervisor) AgentID() string {
	return s.config.AgentID
}

// DomainID returns the agent's domain.
func (s *AgentSupervisor) DomainID() string {
	return s.config.DomainID
}

func (s *AgentSupervisor) eventListener(ctx context.Context) {
	s.mu.Lock()
	piAgent := s.piAgent
	s.mu.Unlock()

	if piAgent == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-piAgent.Events():
			if !ok {
				s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_stream_closed", IsError: true, Error: "Pi agent event stream closed"})
				return
			}
			s.handleEvent(event)
		}
	}
}

func (s *AgentSupervisor) handleEvent(event PiAgentEvent) {
	switch event.Type {
	case "turn_start":
		s.mu.Lock()
		s.isThinking = true
		s.mu.Unlock()
		s.emitStatus(s.displayLabel()+" is thinking...", "info")
		if s.healthMonitor != nil {
			s.healthMonitor.RecordHeartbeat()
		}
		s.lastThinking = ""
		s.lastText = ""
	case "turn_end":
		s.mu.Lock()
		s.isThinking = false
		s.mu.Unlock()
		s.emitLifecycle(AgentLifecycleEvent{Kind: "turn_end"})
		s.emitStatus(s.displayLabel()+" is idle", "ok")
		if s.healthMonitor != nil {
			s.healthMonitor.RecordHeartbeat()
		}
	case "text", "thinking", "message_start", "message_update", "message_end":
		pMsg, err := event.ParseMessage()
		if err != nil {
			// Some updates might not contain a full message yet, that's fine
			return
		}

		// Extract both thinking and text
		thinking := pMsg.FullThinking()
		text := pMsg.FullText()

		log.Printf("supervisor: agent %s broadcasting: thinkingLen=%d, textLen=%d", s.config.AgentID, len(thinking), len(text))

		// Broadcast thinking if present and relevant to this event type
		if thinking != "" && thinking != s.lastThinking && (event.Type == "thinking" || event.Type == "message_update" || event.Type == "message_start") {
			s.lastThinking = thinking
			s.emitLifecycle(AgentLifecycleEvent{Kind: "thinking", Content: thinking})
			payload := map[string]string{
				"type":    "thinking",
				"content": thinking,
				"role":    pMsg.Role,
			}
			msg, _ := hub.NewMessage(hub.MsgContextShare, s.config.AgentID, "tui", payload)
			s.broadcast(*msg)
		}

		// Broadcast text if present
		if text != "" && text != s.lastText {
			s.lastText = text
			s.emitLifecycle(AgentLifecycleEvent{Kind: "text", Content: text})
			payload := map[string]string{
				"type":    "text",
				"content": text,
				"role":    pMsg.Role,
			}
			msg, _ := hub.NewMessage(hub.MsgContextShare, s.config.AgentID, "tui", payload)
			s.broadcast(*msg)
		}
	case "tool_execution_start":
		if s.healthMonitor != nil {
			s.healthMonitor.RecordProgress()
		}
		// Extract tool info for status and chat visibility
		tool := extractToolInvocation(event)
		if tool.Name != "" {
			s.emitStatus(fmt.Sprintf("%s is running tool: %s", s.displayLabel(), tool.Name), "info")
			s.emitLifecycle(AgentLifecycleEvent{
				Kind:        "tool_start",
				ToolName:    tool.Name,
				ToolInput:   tool.Input,
				ToolSummary: summarizeToolInvocation(tool),
			})

			payload := map[string]string{
				"type":    "tool_start",
				"tool":    tool.Name,
				"input":   tool.Input,
				"summary": summarizeToolInvocation(tool),
			}
			msg, _ := hub.NewMessage(hub.MsgContextShare, s.config.AgentID, "tui", payload)
			s.broadcast(*msg)
		}
		s.detectLooping(event)
	case "tool_execution_end":
		s.emitLifecycle(AgentLifecycleEvent{Kind: "tool_end"})
		s.emitStatus(s.displayLabel()+" is thinking...", "info")
		if s.healthMonitor != nil {
			s.healthMonitor.RecordProgress()
		}
	case "file_modified":
		// Any successful file modification breaks the loop heuristic
		s.mu.Lock()
		s.recentTools = s.recentTools[:0]
		s.mu.Unlock()
	case "tool_error":
		s.emitLifecycle(AgentLifecycleEvent{Kind: "tool_error", IsError: true, Error: event.ErrorMessage})
		s.handleNetworkFault(event)
	case "auto_retry_start":
		s.emitStatus(fmt.Sprintf("Inference Retry %d/%d: %s", event.Attempt, event.MaxAttempts, event.ErrorMessage), "warn")
	case "agent_end":
		s.mu.Lock()
		s.isThinking = false
		s.mu.Unlock()
		if event.ErrorMessage != "" {
			s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_error", IsError: true, Error: event.ErrorMessage})
			s.emitStatus("Fatal Agent Error: "+event.ErrorMessage, "error")
			s.mu.Lock()
			s.state = StateStopped // Prevents crash detector from restarting
			s.mu.Unlock()
		} else {
			log.Printf("supervisor: agent %s finished normally", s.config.AgentID)
			s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_end"})
		}
	}
}

func (s *AgentSupervisor) detectLooping(event PiAgentEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var tool struct {
		Name  string `json:"name"`
		Input string `json:"input,omitempty"`
	}
	if err := json.Unmarshal(event.Data, &tool); err != nil {
		return
	}

	fingerprint := fmt.Sprintf("%s:%s", tool.Name, tool.Input)
	s.recentTools = append(s.recentTools, fingerprint)

	if len(s.recentTools) > 7 {
		s.recentTools = s.recentTools[1:]
	}

	if len(s.recentTools) == 7 {
		allSame := true
		for i := 1; i < 7; i++ {
			if s.recentTools[i] != s.recentTools[0] {
				allSame = false
				break
			}
		}
		if allSame {
			log.Printf("supervisor: LOOP DETECTED for agent %s on tool %s. Injecting steer.", s.config.AgentID, tool.Name)
			s.emitStatus(fmt.Sprintf("[%s] Loop detected on tool '%s' — injecting steer", s.config.AgentID, tool.Name), "warn")
			s.SendSteer(fmt.Sprintf("You have failed this action 7 times in a row. Stop looping. Summarize your hypothesis and try a completely different approach. Do not repeat: %s", tool.Name))
			s.recentTools = s.recentTools[:0] // reset sliding window
		}
	}
}

func (s *AgentSupervisor) handleNetworkFault(event PiAgentEvent) {
	msg := strings.ToLower(string(event.Message))
	if strings.Contains(msg, "401") || strings.Contains(msg, "403") || strings.Contains(msg, "auth") || strings.Contains(msg, "unauthorized") || strings.Contains(msg, "forbidden") {
		s.emitLifecycle(AgentLifecycleEvent{Kind: "provider_auth_error", IsError: true, Error: string(event.Message)})
		s.emitStatus(fmt.Sprintf("[%s] Provider authentication/configuration error: %s", s.config.AgentID, string(event.Message)), "error")
		return
	}
	if strings.Contains(msg, "econnreset") || strings.Contains(msg, "timeout") || strings.Contains(msg, "429") || strings.Contains(msg, "unavailable") {
		kind := "provider_unavailable"
		if strings.Contains(msg, "timeout") {
			kind = "network_timeout"
		}
		s.emitLifecycle(AgentLifecycleEvent{Kind: kind, IsError: true, Error: string(event.Message)})
		log.Printf("supervisor: TRANSIENT NETWORK FAULT detected for agent %s: %s. Applying backoff.", s.config.AgentID, string(event.Message))
		s.emitStatus(fmt.Sprintf("[%s] Network fault: %s — backing off 10s...", s.config.AgentID, string(event.Message)), "warn")
		s.mu.Lock()
		pa := s.piAgent
		s.mu.Unlock()

		if pa != nil {
			go func() {
				time.Sleep(10 * time.Second)
				log.Printf("supervisor: backoff complete. Nudging agent %s to continue.", s.config.AgentID)
				s.emitStatus(fmt.Sprintf("[%s] Backoff complete — resuming", s.config.AgentID), "info")
				s.SendSteer("Network fault occurred. Please re-evaluate and continue your previous action.")
			}()
		}
	}
}

type toolInvocation struct {
	Name  string
	Input string
	Args  map[string]interface{}
}

func extractToolInvocation(event PiAgentEvent) toolInvocation {
	inv := toolInvocation{Name: event.ToolName}

	if len(event.Args) > 0 {
		inv.Input = string(event.Args)
		_ = json.Unmarshal(event.Args, &inv.Args)
	}

	if len(event.Data) > 0 {
		var data struct {
			Name  string          `json:"name"`
			Input string          `json:"input,omitempty"`
			Args  json.RawMessage `json:"args,omitempty"`
		}
		if err := json.Unmarshal(event.Data, &data); err == nil {
			if inv.Name == "" {
				inv.Name = data.Name
			}
			if inv.Input == "" {
				inv.Input = data.Input
			}
			if len(data.Args) > 0 {
				inv.Input = string(data.Args)
				_ = json.Unmarshal(data.Args, &inv.Args)
			}
		}
	}

	if inv.Input == "" && len(inv.Args) > 0 {
		b, _ := json.Marshal(inv.Args)
		inv.Input = string(b)
	}
	return inv
}

func summarizeToolInvocation(inv toolInvocation) string {
	arg := func(keys ...string) string {
		for _, key := range keys {
			if value, ok := inv.Args[key]; ok {
				switch v := value.(type) {
				case string:
					return v
				default:
					return fmt.Sprintf("%v", v)
				}
			}
		}
		return ""
	}

	switch inv.Name {
	case "read", "read_file":
		if path := arg("path", "file", "file_path"); path != "" {
			return "Read file: " + path
		}
	case "bash", "run_command", "shell":
		if command := arg("command", "cmd"); command != "" {
			return "Run shell: " + command
		}
	case "write", "write_file":
		if path := arg("path", "file", "file_path"); path != "" {
			return "Write file: " + path
		}
	case "edit", "apply_patch":
		if path := arg("path", "file", "file_path"); path != "" {
			return "Edit file: " + path
		}
	case "glob", "list", "ls":
		if pattern := arg("pattern", "path", "glob"); pattern != "" {
			return "List files: " + pattern
		}
	case "grep", "search", "rg":
		if pattern := arg("pattern", "query"); pattern != "" {
			return "Search: " + pattern
		}
	}

	if inv.Input != "" {
		return fmt.Sprintf("%s: %s", inv.Name, inv.Input)
	}
	return inv.Name
}

func (s *AgentSupervisor) contextRouter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.contextCh:
			// Filter out our own messages to prevent echo loops
			if msg.FromAgent == s.config.AgentID {
				continue
			}

			s.mu.Lock()
			piAgent := s.piAgent
			s.mu.Unlock()

			if piAgent == nil || !piAgent.IsAlive() {
				continue
			}

			var followUpText string
			if msg.Type == hub.MsgContextShare && msg.FromAgent == "user" && msg.ToAgent == "orchestrator" {
				// Deliver user messages to the Architect as plain-text prompts
				var text string
				if err := json.Unmarshal(msg.Payload, &text); err == nil {
					followUpText = text
				} else {
					followUpText = string(msg.Payload)
				}
				log.Printf("supervisor: delivering user message to architect: %s", followUpText)
			} else {
				// Format other context messages as follow_up JSON
				payload, _ := json.Marshal(msg)
				followUpText = fmt.Sprintf("[Context Hub - %s] %s", msg.Type, string(payload))
			}

			if err := s.SendFollowUp(followUpText); err != nil {
				log.Printf("supervisor: failed to deliver context to %s: %v", s.config.AgentID, err)
			}
		}
	}
}

func (s *AgentSupervisor) crashDetector(ctx context.Context) {
	s.mu.Lock()
	piAgent := s.piAgent
	s.mu.Unlock()

	if piAgent == nil {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-piAgent.Done():
		s.mu.Lock()
		state := s.state
		s.mu.Unlock()

		if state == StateStopped {
			return // expected shutdown
		}

		s.handleCrash("process exited unexpectedly")
	}
}

func (s *AgentSupervisor) handleCrash(reason string) {
	s.mu.Lock()
	if s.state == StateStopped {
		s.mu.Unlock()
		return
	}
	s.state = StateCrashed
	s.crashCount++
	count := s.crashCount
	maxRestarts := s.config.MaxCrashRestarts
	piAgent := s.piAgent
	s.mu.Unlock()

	log.Printf("supervisor: agent %s crashed (reason: %s, crashes: %d/%d)", s.config.AgentID, reason, count, maxRestarts)
	s.emitStatus(fmt.Sprintf("[%s] Crashed: %s (%d/%d)", s.config.AgentID, reason, count, maxRestarts), "error")

	// Kill process group if still running
	if piAgent != nil {
		piAgent.Kill()
	}

	// Cancel old context to stop old goroutines
	if s.cancel != nil {
		s.cancel()
	}

	// Destroy and recreate cgroup
	DestroyCgroup(s.config.Cgroup.BasePath, s.config.AgentID)

	if count >= maxRestarts {
		log.Printf("supervisor: agent %s exceeded max restarts, giving up", s.config.AgentID)
		s.mu.Lock()
		s.state = StateStopped
		s.mu.Unlock()
		return
	}

	// Respawn with a new context
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()

	log.Printf("supervisor: respawning agent %s", s.config.AgentID)
	if err := s.spawnAgent(ctx); err != nil {
		log.Printf("supervisor: failed to respawn %s: %v", s.config.AgentID, err)
		s.mu.Lock()
		s.state = StateStopped
		s.mu.Unlock()
	}
}
