package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	isolation "aion-isolation"
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
	// IsolationEngine and IsolationPolicy are set for domain agents. The
	// supervisor owns the prepared workspace and recreates it on every spawn.
	IsolationEngine            isolation.Engine
	IsolationPolicy            isolation.Policy
	IsolationGeneration        uint64
	PersistIsolationGeneration func(uint64) error
	// PrepareGenerationEnv issues credentials tied to the just-prepared
	// workspace generation. It runs before the child process is spawned.
	PrepareGenerationEnv func(uint64) ([]string, error)
	// CgroupConfig configures resource limits.
	Cgroup CgroupConfig
	// HealthConfig
	HeartbeatTimeout      time.Duration
	ProgressTimeout       time.Duration
	ExternalActivityStale time.Duration
	ExternalActivityMax   time.Duration
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
	contextCh        chan contextDelivery
	cancel           context.CancelFunc
	crashCount       int
	recentTools      []string // sliding window for loop detection
	statusFn         func(text, level string)
	broadcastFn      func(msg hub.Message)
	lifecycleFn      func(event AgentLifecycleEvent)
	pendingBroadcast []hub.Message
	streamThinking   agentStreamState
	streamText       agentStreamState
	streamBlock      uint64
	isThinking       bool
	toolActive       bool
	toolPulseStop    chan struct{}
	autoRetryPending bool
	recoveryPrompt   string
	workspace        isolation.Workspace
	workspaceGen     uint64
}

type contextDelivery struct {
	message hub.Message
	result  chan error
}

type AgentRuntimeSnapshot struct {
	AgentID             string              `json:"agent_id"`
	DomainID            string              `json:"domain_id"`
	State               string              `json:"state"`
	PID                 int                 `json:"pid,omitempty"`
	WorkspaceGeneration uint64              `json:"workspace_generation,omitempty"`
	Workspace           *isolation.Snapshot `json:"workspace,omitempty"`
	InTurn              bool                `json:"in_turn"`
	ToolActive          bool                `json:"tool_active"`
	LastHeartbeat       time.Time           `json:"last_heartbeat,omitempty"`
	LastProgress        time.Time           `json:"last_progress,omitempty"`
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
	if config.ExternalActivityStale <= 0 {
		config.ExternalActivityStale = 45 * time.Second
	}
	if config.ExternalActivityMax <= 0 {
		config.ExternalActivityMax = 15 * time.Minute
	}
	return &AgentSupervisor{
		config:       config,
		state:        StateStarting,
		contextCh:    make(chan contextDelivery, 64),
		recentTools:  make([]string, 0, 10),
		workspaceGen: config.IsolationGeneration,
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

	s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_started"})
	return nil
}

func (s *AgentSupervisor) spawnAgent(ctx context.Context) error {
	s.mu.Lock()
	s.state = StateStarting
	s.mu.Unlock()

	piConfig := s.config.PiAgent
	s.mu.Lock()
	launchPrompt := s.config.InitialPrompt
	if strings.TrimSpace(s.recoveryPrompt) != "" {
		launchPrompt = s.recoveryPrompt
	}
	s.mu.Unlock()
	var workspace isolation.Workspace
	if s.config.IsolationEngine != nil {
		s.mu.Lock()
		s.workspaceGen++
		generation := s.workspaceGen
		s.mu.Unlock()
		policy := s.config.IsolationPolicy
		policy.Generation = generation
		prepared, err := s.config.IsolationEngine.Prepare(ctx, policy)
		if err != nil {
			return fmt.Errorf("prepare isolated workspace generation %d: %w", generation, err)
		}
		if s.config.PersistIsolationGeneration != nil {
			if err := s.config.PersistIsolationGeneration(generation); err != nil {
				_ = prepared.Close()
				return fmt.Errorf("persist isolated workspace generation %d: %w", generation, err)
			}
		}
		workspace = prepared
		piConfig.Workspace = prepared
		if s.config.PrepareGenerationEnv != nil {
			generationEnv, err := s.config.PrepareGenerationEnv(generation)
			if err != nil {
				_ = prepared.Close()
				return fmt.Errorf("prepare isolated workspace generation %d environment: %w", generation, err)
			}
			piConfig.Env = append(piConfig.Env, generationEnv...)
		}
	}
	cleanupWorkspace := func() {
		if workspace != nil {
			_ = workspace.Close()
		}
	}

	// Create cgroup
	if err := CreateCgroup(s.config.Cgroup); err != nil {
		if s.config.Cgroup.Strict() {
			cleanupWorkspace()
			return err
		}
		log.Printf("supervisor: cgroup creation failed for %s: %v", s.config.AgentID, err)
	}

	// Spawn Pi Agent
	piAgent, err := SpawnPiAgent(ctx, piConfig)
	if err != nil {
		cleanupWorkspace()
		return fmt.Errorf("spawn pi agent: %w", err)
	}
	// Any later crash respawn should reopen the session created by this process.
	s.config.PiAgent.ResumeSession = true

	// Assign to cgroup
	if s.config.Cgroup.Enabled {
		if err := AssignProcessWithConfig(s.config.Cgroup, piAgent.PID()); err != nil {
			if s.config.Cgroup.Strict() {
				piAgent.Kill()
				cleanupWorkspace()
				return err
			}
			log.Printf("supervisor: cgroup assign failed for %s: %v", s.config.AgentID, err)
		}
	}

	// Create health monitor
	hm := NewHealthMonitor(
		s.config.AgentID,
		s.config.HeartbeatTimeout,
		s.config.ProgressTimeout,
	)
	hm.SetExternalActivityTimeouts(s.config.ExternalActivityStale, s.config.ExternalActivityMax)
	hm.OnTimeout(func(agentID string, status HealthStatus) {
		s.mu.Lock()
		turnActive := s.isThinking
		state := s.state
		s.mu.Unlock()
		if !turnActive && state == StateRunning {
			// An RPC-mode Pi process is expected to be silent between turns.
			// Process exit is covered independently by crashDetector.
			hm.RecordHeartbeat()
			return
		}
		log.Printf("supervisor: agent %s health timeout: %d", agentID, status)
		s.handleCrash("health timeout")
	})
	if strings.TrimSpace(launchPrompt) == "" {
		hm.SetMonitoring(false)
	}
	hm.Start(ctx)

	s.mu.Lock()
	s.piAgent = piAgent
	s.healthMonitor = hm
	s.workspace = workspace
	s.state = StateRunning
	s.mu.Unlock()

	// Start event listener
	go s.eventListener(ctx)

	// Start context routing
	go s.contextRouter(ctx)

	// Start crash detection
	go s.crashDetector(ctx)

	// Send initial prompt
	if strings.TrimSpace(launchPrompt) != "" {
		if err := s.SendPrompt(launchPrompt); err != nil {
			hm.Stop()
			_ = piAgent.Kill()
			cleanupWorkspace()
			s.mu.Lock()
			s.workspace = nil
			s.mu.Unlock()
			return fmt.Errorf("send initial prompt: %w", err)
		}
	}

	log.Printf("supervisor: agent %s started (pid=%d, domain=%s)", s.config.AgentID, piAgent.PID(), s.config.DomainID)
	return nil
}

// DeliverContext delivers a Hub message through an active follow-up or an idle
// prompt turn and reports only after Pi accepts the RPC command.
func (s *AgentSupervisor) DeliverContext(msg hub.Message) error {
	delivery := contextDelivery{message: msg, result: make(chan error, 1)}
	select {
	case s.contextCh <- delivery:
	default:
		return fmt.Errorf("supervisor: context channel full for agent %s", s.config.AgentID)
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-delivery.result:
		return err
	case <-timer.C:
		return fmt.Errorf("supervisor: context delivery timed out for agent %s", s.config.AgentID)
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
	workspace := s.workspace
	s.workspace = nil
	s.mu.Unlock()

	s.stopToolPulse()
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
	if s.cancel != nil {
		s.cancel()
	}
	if workspace != nil {
		_ = workspace.Close()
	}

	// Clean up cgroup
	DestroyCgroup(s.config.Cgroup.BasePath, s.config.AgentID)
	s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_stopped"})

	log.Printf("supervisor: agent %s stopped", s.config.AgentID)
	return nil
}

// RestartWithPrompt reopens the persisted Pi session and starts one direct
// recovery turn. The prompt becomes the crash-recovery prompt for this agent
// until a later accepted direct prompt replaces it.
func (s *AgentSupervisor) RestartWithPrompt(ctx context.Context, prompt string) error {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return fmt.Errorf("supervisor: restart prompt is required")
	}
	s.mu.Lock()
	s.recoveryPrompt = prompt
	s.mu.Unlock()
	if err := s.Stop(); err != nil {
		return err
	}
	return s.Start(ctx)
}

// RestartFreshWithPrompt starts a new Pi conversation in the existing session
// directory. Older session files remain available for audit and UI history,
// while the supplied prompt becomes the first turn of the new conversation.
func (s *AgentSupervisor) RestartFreshWithPrompt(ctx context.Context, prompt string) error {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return fmt.Errorf("supervisor: fresh restart prompt is required")
	}
	s.mu.Lock()
	s.recoveryPrompt = prompt
	s.config.PiAgent.ResumeSession = false
	s.mu.Unlock()
	if err := s.Stop(); err != nil {
		return err
	}
	return s.Start(ctx)
}

// SendPrompt sends an initial prompt to the Pi Agent.
func (s *AgentSupervisor) SendPrompt(message string) error {
	s.mu.Lock()
	s.resetStreamStateLocked()
	s.autoRetryPending = false
	pa := s.piAgent
	// Treat an accepted prompt command as an active turn immediately. Pi emits
	// turn_start asynchronously, and leaving this false creates a dispatch race
	// where a second command can be mistaken for an idle prompt.
	s.isThinking = true
	s.mu.Unlock()
	if pa == nil {
		s.mu.Lock()
		s.isThinking = false
		s.mu.Unlock()
		return fmt.Errorf("agent not started")
	}
	if err := pa.SendPrompt(message); err != nil {
		s.mu.Lock()
		s.isThinking = false
		s.mu.Unlock()
		return err
	}
	s.mu.Lock()
	s.recoveryPrompt = message
	s.mu.Unlock()
	if s.healthMonitor != nil {
		s.healthMonitor.SetMonitoring(true)
	}
	return nil
}

// DeliverTurn queues a follow-up during an active turn and starts a prompt
// immediately when Pi is idle.
func (s *AgentSupervisor) DeliverTurn(message string) error {
	s.mu.Lock()
	s.resetStreamStateLocked()
	pa := s.piAgent
	thinking := s.isThinking || s.autoRetryPending
	s.mu.Unlock()
	if pa == nil {
		return fmt.Errorf("agent not started")
	}

	if thinking {
		if err := pa.SendFollowUp(message); err != nil {
			return err
		}
	} else {
		// If idle, use the supervisor prompt path so the pending turn is visible
		// before Pi emits its asynchronous turn_start event.
		return s.SendPrompt(message)
	}
	if s.healthMonitor != nil {
		s.healthMonitor.SetMonitoring(true)
	}
	return nil
}

// WaitForTurnIdle waits until Pi has emitted agent_end. A turn_end only closes
// one model/tool turn; Pi may immediately start another turn in the same agent
// loop, so treating it as idle can race a direct prompt against that next turn.
func (s *AgentSupervisor) WaitForTurnIdle(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		active := s.isThinking || s.autoRetryPending
		s.mu.Unlock()
		if !active {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// TurnIdle reports whether Pi has completed its current agent loop. It lets a
// scheduler defer this agent without blocking dispatch for unrelated domains.
func (s *AgentSupervisor) TurnIdle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.isThinking && !s.autoRetryPending && !s.toolActive
}

// TurnRetryPending reports whether Pi is retrying a failed inference turn
// internally. The allocator must not inject its own incomplete-work retry while
// this is true.
func (s *AgentSupervisor) TurnRetryPending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.autoRetryPending
}

// SendSteer sends a steer command to the Pi Agent.
func (s *AgentSupervisor) SendSteer(message string) error {
	s.mu.Lock()
	s.resetStreamStateLocked()
	pa := s.piAgent
	thinking := s.isThinking
	s.mu.Unlock()
	if pa == nil {
		return fmt.Errorf("agent not started")
	}

	if thinking {
		if err := pa.SendSteer(message); err != nil {
			return err
		}
	} else {
		// If idle, use prompt to force processing of the steer
		if err := pa.SendPrompt("STEER: " + message); err != nil {
			return err
		}
	}
	if s.healthMonitor != nil {
		s.healthMonitor.SetMonitoring(true)
	}
	return nil
}

// State returns the current agent state.
func (s *AgentSupervisor) State() AgentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// RecordActivity marks the supervised agent as live based on external runtime
// activity such as an in-flight gateway request.
func (s *AgentSupervisor) RecordActivity(phase ...string) {
	s.mu.Lock()
	hm := s.healthMonitor
	s.mu.Unlock()
	if hm == nil {
		return
	}
	if len(phase) > 0 && strings.TrimSpace(phase[0]) != "" {
		hm.RecordExternalActivity(strings.TrimSpace(phase[0]))
		return
	}
	hm.RecordHeartbeat()
}

// AgentID returns the agent's unique identifier.
func (s *AgentSupervisor) AgentID() string {
	return s.config.AgentID
}

// DomainID returns the agent's domain.
func (s *AgentSupervisor) DomainID() string {
	return s.config.DomainID
}

func (s *AgentSupervisor) RuntimeSnapshot() AgentRuntimeSnapshot {
	s.mu.Lock()
	snapshot := AgentRuntimeSnapshot{
		AgentID:             s.config.AgentID,
		DomainID:            s.config.DomainID,
		State:               s.state.String(),
		WorkspaceGeneration: s.workspaceGen,
		InTurn:              s.isThinking,
		ToolActive:          s.toolActive,
	}
	if s.piAgent != nil && s.piAgent.IsAlive() {
		snapshot.PID = s.piAgent.PID()
	}
	if s.workspace != nil {
		workspace := s.workspace.Snapshot()
		snapshot.Workspace = &workspace
	}
	hm := s.healthMonitor
	s.mu.Unlock()
	if hm != nil {
		snapshot.LastHeartbeat, snapshot.LastProgress = hm.Snapshot()
	}
	return snapshot
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
		s.toolActive = false
		s.resetStreamStateLocked()
		s.mu.Unlock()
		s.stopToolPulse()
		s.emitStatus(s.displayLabel()+" is thinking...", "info")
		if s.healthMonitor != nil {
			s.healthMonitor.SetMonitoring(true)
			s.healthMonitor.RecordHeartbeat()
		}
	case "turn_end":
		s.emitLifecycle(AgentLifecycleEvent{Kind: "turn_end"})
		if s.healthMonitor != nil {
			s.healthMonitor.RecordHeartbeat()
		}
	case "text", "thinking", "message_start", "message_update", "message_end":
		pMsg, mode, err := event.ParseStreamMessage()
		if err != nil {
			// Some updates might not contain a full message yet, that's fine
			return
		}

		if event.Type == "message_end" && strings.TrimSpace(pMsg.ErrorMessage) != "" {
			s.emitLifecycle(AgentLifecycleEvent{Kind: "model_error", IsError: true, Error: pMsg.ErrorMessage})
			s.emitStatus(fmt.Sprintf("%s inference turn failed: %s", s.displayLabel(), pMsg.ErrorMessage), "error")
			if s.healthMonitor != nil {
				s.healthMonitor.RecordHeartbeat()
			}
		}

		// Extract both thinking and text
		thinking := pMsg.FullThinking()
		text := pMsg.FullText()

		if event.Type != "message_update" {
			log.Printf("supervisor: agent %s broadcasting: thinkingLen=%d, textLen=%d", s.config.AgentID, len(thinking), len(text))
		}

		now := time.Now()
		force := event.Type == "message_end"
		s.mu.Lock()
		if event.Type == "message_start" {
			s.resetStreamStateLocked()
		}
		if s.streamBlock == 0 {
			s.resetStreamStateLocked()
		}
		block := s.streamBlockIDLocked()
		thinkingOut, emitThinking := s.streamThinking.observe(mode, thinking, now, force)
		textOut, emitText := s.streamText.observe(mode, text, now, force)
		role := pMsg.Role
		s.mu.Unlock()

		// Broadcast thinking if present. The dashboard receives bounded
		// snapshots so it can replace the current block instead of appending a
		// row for every token.
		if emitThinking {
			s.emitLifecycle(AgentLifecycleEvent{Kind: "thinking", Content: thinkingOut})
			payload := streamPayload("thinking", thinkingOut, role, block)
			msg, _ := hub.NewMessage(hub.MsgContextShare, s.config.AgentID, "tui", payload)
			s.broadcast(*msg)
		}

		// Broadcast text if present.
		if emitText {
			s.emitLifecycle(AgentLifecycleEvent{Kind: "text", Content: textOut})
			payload := streamPayload("text", textOut, role, block)
			msg, _ := hub.NewMessage(hub.MsgContextShare, s.config.AgentID, "tui", payload)
			s.broadcast(*msg)
		}
	case "tool_execution_start":
		s.mu.Lock()
		s.toolActive = true
		s.mu.Unlock()
		s.startToolPulse()
		if s.healthMonitor != nil {
			s.healthMonitor.RecordProgress()
			s.healthMonitor.RecordExternalActivity("running_tool")
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
		s.stopToolPulse()
		s.mu.Lock()
		s.toolActive = false
		s.mu.Unlock()
		s.emitLifecycle(AgentLifecycleEvent{Kind: "tool_end"})
		s.emitStatus(s.displayLabel()+" is thinking...", "info")
		if s.healthMonitor != nil {
			s.healthMonitor.RecordProgress()
			s.healthMonitor.RecordExternalActivity("tool_end")
		}
	case "file_modified":
		// Any successful file modification breaks the loop heuristic
		s.mu.Lock()
		s.recentTools = s.recentTools[:0]
		s.mu.Unlock()
		s.emitLifecycle(AgentLifecycleEvent{Kind: "file_modified", Content: string(event.Message)})
	case "tool_error":
		s.stopToolPulse()
		s.mu.Lock()
		s.toolActive = false
		s.mu.Unlock()
		if s.healthMonitor != nil {
			s.healthMonitor.RecordExternalActivity("tool_end")
		}
		s.emitLifecycle(AgentLifecycleEvent{Kind: "tool_error", IsError: true, Error: event.ErrorMessage})
		s.handleNetworkFault(event)
	case "auto_retry_start":
		s.mu.Lock()
		s.autoRetryPending = true
		s.isThinking = true
		s.mu.Unlock()
		if s.healthMonitor != nil {
			s.healthMonitor.SetMonitoring(true)
			s.healthMonitor.RecordHeartbeat()
		}
		s.emitStatus(fmt.Sprintf("Inference Retry %d/%d: %s", event.Attempt, event.MaxAttempts, event.ErrorMessage), "warn")
	case "agent_end":
		s.mu.Lock()
		s.isThinking = false
		s.autoRetryPending = false
		s.mu.Unlock()
		if s.healthMonitor != nil {
			s.healthMonitor.SetMonitoring(false)
		}
		if event.ErrorMessage != "" {
			s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_error", IsError: true, Error: event.ErrorMessage})
			s.emitStatus("Fatal Agent Error: "+event.ErrorMessage, "error")
			s.mu.Lock()
			s.state = StateStopped // Prevents crash detector from restarting
			s.mu.Unlock()
		} else {
			log.Printf("supervisor: agent %s finished normally", s.config.AgentID)
			s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_end"})
			s.emitStatus(s.displayLabel()+" is idle", "ok")
		}
	}
}

func (s *AgentSupervisor) detectLooping(event PiAgentEvent) {
	s.mu.Lock()

	var tool struct {
		Name  string `json:"name"`
		Input string `json:"input,omitempty"`
	}
	if err := json.Unmarshal(event.Data, &tool); err != nil {
		s.mu.Unlock()
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
			s.recentTools = s.recentTools[:0]
			s.mu.Unlock()
			log.Printf("supervisor: LOOP DETECTED for agent %s on tool %s. Injecting steer.", s.config.AgentID, tool.Name)
			s.emitStatus(fmt.Sprintf("[%s] Loop detected on tool '%s' — injecting steer", s.config.AgentID, tool.Name), "warn")
			s.SendSteer(fmt.Sprintf("You have failed this action 7 times in a row. Stop looping. Summarize your hypothesis and try a completely different approach. Do not repeat: %s", tool.Name))
			return
		}
	}
	s.mu.Unlock()
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
		case delivery := <-s.contextCh:
			msg := delivery.message
			// Filter out our own messages to prevent echo loops
			if msg.FromAgent == s.config.AgentID {
				delivery.result <- nil
				continue
			}

			s.mu.Lock()
			piAgent := s.piAgent
			s.mu.Unlock()

			if piAgent == nil || !piAgent.IsAlive() {
				delivery.result <- fmt.Errorf("supervisor: agent %s is not available for context delivery", s.config.AgentID)
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

			if err := s.DeliverTurn(followUpText); err != nil {
				log.Printf("supervisor: failed to deliver context to %s: %v", s.config.AgentID, err)
				delivery.result <- err
				continue
			}
			delivery.result <- nil
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
		current := s.piAgent
		s.mu.Unlock()

		if state == StateStopped || current != piAgent {
			return // expected shutdown
		}

		s.handleCrash("process exited unexpectedly")
	}
}

func (s *AgentSupervisor) handleCrash(reason string) {
	s.mu.Lock()
	if s.state == StateStopped || s.state == StateCrashed {
		s.mu.Unlock()
		return
	}
	s.state = StateCrashed
	s.crashCount++
	count := s.crashCount
	maxRestarts := s.config.MaxCrashRestarts
	piAgent := s.piAgent
	workspace := s.workspace
	s.workspace = nil
	s.mu.Unlock()

	log.Printf("supervisor: agent %s crashed (reason: %s, crashes: %d/%d)", s.config.AgentID, reason, count, maxRestarts)
	s.emitStatus(fmt.Sprintf("[%s] Crashed: %s (%d/%d)", s.config.AgentID, reason, count, maxRestarts), "error")
	s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_crashed", Error: reason})

	// Kill process group if still running
	if piAgent != nil {
		piAgent.Kill()
	}
	if workspace != nil {
		_ = workspace.Close()
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
		s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_stopped", Error: reason})
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
		s.emitLifecycle(AgentLifecycleEvent{Kind: "agent_stopped", Error: err.Error()})
	}
}

const (
	streamEmitMinChars    = 512
	streamEmitMinInterval = 500 * time.Millisecond
)

// agentStreamState accumulates one content stream (thinking or text) for the
// current assistant message. Deltas append; snapshots replace. Emission is
// throttled so a token stream becomes bounded dashboard updates rather than
// one hub message per token.
type agentStreamState struct {
	value    string
	emitted  string
	lastEmit time.Time
}

func (s *agentStreamState) reset() {
	*s = agentStreamState{}
}

func (s *agentStreamState) observe(mode, content string, now time.Time, force bool) (string, bool) {
	if content == "" {
		return "", false
	}
	if mode == PiStreamDelta {
		s.value += content
	} else {
		// Ignore a stale snapshot that is a strict prefix of what we already
		// accumulated. Any other snapshot is authoritative and replaces.
		if s.value != "" && content != s.value && strings.HasPrefix(s.value, content) {
			return "", false
		}
		s.value = content
	}
	if s.value == "" || s.value == s.emitted {
		return "", false
	}
	if !force &&
		len(s.value)-len(s.emitted) < streamEmitMinChars &&
		now.Sub(s.lastEmit) < streamEmitMinInterval {
		return "", false
	}
	s.emitted = s.value
	s.lastEmit = now
	return s.value, true
}

// resetStreamStateLocked begins a fresh dashboard stream block. Callers must
// hold s.mu.
func (s *AgentSupervisor) resetStreamStateLocked() {
	s.streamBlock++
	s.streamThinking.reset()
	s.streamText.reset()
}

func (s *AgentSupervisor) streamBlockIDLocked() string {
	return fmt.Sprintf("%s-%d", s.config.AgentID, s.streamBlock)
}

func streamPayload(kind, content, role, block string) map[string]string {
	return map[string]string{
		"type":    kind,
		"content": content,
		"role":    role,
		"stream":  PiStreamSnapshot,
		"block":   block,
	}
}

// startToolPulse keeps the agent healthy while a local tool runs. Pi emits no
// heartbeat between tool start and end, so a long command would otherwise be
// mistaken for an unresponsive agent and respawned mid-tool.
func (s *AgentSupervisor) startToolPulse() {
	s.mu.Lock()
	if s.toolPulseStop != nil {
		close(s.toolPulseStop)
	}
	stop := make(chan struct{})
	s.toolPulseStop = stop
	hm := s.healthMonitor
	s.mu.Unlock()
	if hm == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				hm.RecordExternalActivity("running_tool")
			}
		}
	}()
}

func (s *AgentSupervisor) stopToolPulse() {
	s.mu.Lock()
	if s.toolPulseStop != nil {
		close(s.toolPulseStop)
		s.toolPulseStop = nil
	}
	s.mu.Unlock()
}
