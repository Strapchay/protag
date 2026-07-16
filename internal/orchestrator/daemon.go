package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/locking"
	"aion-kernel/internal/memory"
	"aion-kernel/internal/stub"
	"aion-kernel/internal/supervisor"
)

// Daemon is the main Orchestrator process. It initializes all subsystems
// and manages the full lifecycle from startup through shutdown.
type Daemon struct {
	config                 *Config
	dagManager             *dag.Manager
	lockManager            *locking.Manager
	stubRegistry           *stub.Registry
	hubRouter              *hub.Router
	allocator              *Allocator
	server                 *Server
	coordinator            coordinator.Coordinator
	memoryStore            memory.Store
	inferenceGateway       *InferenceGateway
	ipcPaths               daemonIPCPaths
	projectRoot            string
	auditor                *Auditor
	runState               *RunState
	ctx                    context.Context
	cancel                 context.CancelFunc
	resetMu                sync.Mutex
	buildSpecMu            sync.Mutex
	buildSpecContinueArmed bool
	buildSpecCancel        context.CancelFunc
	buildSpecAttempt       *BuildSpecAttempt
	executionMonitorMu     sync.Mutex
	executionMonitorCancel context.CancelFunc
	executionMonitorRun    uint64
	executionMonitorActive bool
	progressMu             sync.Mutex
	lastProgressSignature  string
	staleMu                sync.Mutex
	staleReported          map[string]bool
}

// NewDaemon creates a new Orchestrator daemon with the given config.
func NewDaemon(config *Config, projectRoot string) (*Daemon, error) {
	// Ensure runtime directories exist
	aionDir := filepath.Join(projectRoot, ".aion")
	if err := os.MkdirAll(aionDir, 0755); err != nil {
		return nil, fmt.Errorf("daemon: create .aion dir: %w", err)
	}

	runState, err := LoadOrCreateCurrentRun(projectRoot, config)
	if err != nil {
		return nil, fmt.Errorf("daemon: load run state: %w", err)
	}
	config.Agents.SessionDir = runState.PiSessionsDir
	ipcPaths, err := prepareDaemonIPC(projectRoot)
	if err != nil {
		return nil, fmt.Errorf("daemon: prepare IPC: %w", err)
	}

	// Initialize DAG Manager
	dagMgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: runState.DagFile,
		WalFilePath:   runState.WalFile,
		MaxNodes:      config.Orchestrator.MaxActiveNodes,
		FlushDeadline: config.FlushDeadline(),
	})
	if err != nil {
		return nil, fmt.Errorf("daemon: init dag manager: %w", err)
	}

	// Initialize Lock Manager
	lockMgr := locking.NewManager(config.Agents.SharedFiles)

	// Initialize Stub Registry
	stubReg := stub.NewRegistry()

	// Initialize Hub Router
	hubRouter := hub.NewRouter(runState.LogsDir)

	// Initialize Allocator
	alloc := NewAllocator(config, projectRoot, hubRouter, dagMgr)
	alloc.SetIPCDir(ipcPaths.Dir)

	// Initialize Coordinator
	piCoord := newPiCoordinatorForRun(projectRoot, config, runState)
	coord := coordinator.Coordinator(piCoord)

	// Initialize Semantic Memory (if enabled)
	var memStore memory.Store
	if config.Memory.Enabled {
		embCfg := memory.EmbedderConfig{
			Type:    config.Memory.EmbedderType,
			Model:   config.Memory.EmbedderModel,
			BaseURL: config.Memory.EmbedderBaseURL,
		}
		embedder, err := memory.NewEmbedder(embCfg)
		if err != nil {
			return nil, fmt.Errorf("daemon: init embedder: %w", err)
		}

		memStore, err = memory.NewStore(config.Memory.StorePath, embedder)
		if err != nil {
			return nil, fmt.Errorf("daemon: init memory store: %w", err)
		}
	}

	var inferenceGateway *InferenceGateway
	if config.GatewayEnabled() {
		inferenceGateway = NewInferenceGateway(config, runState.LogsDir)
		inferenceGateway.SetActivityFunc(func(agentID, domainID, phase string) {
			if alloc.RecordAgentActivity(agentID, phase) && config.Orchestrator.LogLevel == "debug" {
				log.Printf("inference-gateway: liveness pulse agent=%s domain=%s phase=%s", agentID, domainID, phase)
			}
			if recorder, ok := coord.(interface {
				RecordPlannerGatewayActivity(agentID, domainID, phase string)
			}); ok {
				recorder.RecordPlannerGatewayActivity(agentID, domainID, phase)
			}
		})
	}

	// Initialize Server
	server := NewServer(dagMgr, lockMgr, stubReg, memStore)
	server.SetLogsDir(runState.LogsDir)
	server.SetLogLevel(config.Orchestrator.LogLevel)
	if inferenceGateway != nil {
		inferenceGateway.SetUnixSocket(ipcPaths.InferenceSocket)
		inferenceGateway.SetCapabilityResolver(server.ResolveAgentCapability)
	}
	if broadcaster, ok := coord.(interface{ SetBroadcastFunc(func(hub.Message)) }); ok {
		broadcaster.SetBroadcastFunc(func(msg hub.Message) {
			server.BroadcastHubEvent(msg)
		})
	}
	if tracer, ok := coord.(interface{ SetTraceFunc(func(string)) }); ok {
		tracer.SetTraceFunc(func(text string) {
			_ = appendBuildSpecTrace(runState.Root, text)
			if trace := truncateTraceForStatus(text); trace != "" {
				server.BroadcastAgentStatus("coordinator", "Build-spec trace: "+trace, "info")
			}
		})
	}

	// Initialize Auditor
	progressTimeout := time.Duration(config.Health.ProgressTimeoutSec) * time.Second
	scanInterval := 10 * time.Second // configurable later if needed
	aud := NewAuditor(dagMgr, lockMgr, hubRouter, progressTimeout, scanInterval)

	ctx, cancel := context.WithCancel(context.Background())

	daemon := &Daemon{
		config:           config,
		dagManager:       dagMgr,
		lockManager:      lockMgr,
		stubRegistry:     stubReg,
		hubRouter:        hubRouter,
		allocator:        alloc,
		server:           server,
		coordinator:      coord,
		memoryStore:      memStore,
		inferenceGateway: inferenceGateway,
		ipcPaths:         ipcPaths,
		auditor:          aud,
		runState:         runState,
		projectRoot:      projectRoot,
		ctx:              ctx,
		cancel:           cancel,
		staleReported:    make(map[string]bool),
	}

	server.SetHubCallback(func(msg hub.Message) {
		if msg.Type == hub.MsgDependencyDiscovered {
			go daemon.handleReplan(msg)
		}
		if err := daemon.hubRouter.Route(msg); err != nil {
			log.Printf("daemon: hub route error: %v", err)
		}
		server.BroadcastHubEvent(msg)
	})

	server.SetReplanCallback(func() {
		go daemon.handleReplan(hub.Message{Type: hub.MsgDependencyDiscovered})
	})

	server.SetReviveCallback(func(agentID string) error {
		return daemon.allocator.ReviveAgent(daemon.ctx, agentID)
	})

	server.SetRefineCallback(func(text string) {
		daemon.server.BroadcastStatus("Architect is thinking...", "info")
		if err := daemon.coordinator.Refine(daemon.ctx, text); err != nil {
			log.Printf("daemon: refinement routing failed: %v", err)
			daemon.server.BroadcastStatus("Refinement routing failed.", "error")
		}
	})

	if statusAware, ok := coord.(interface{ SetStatusFunc(func(string, string)) }); ok {
		statusAware.SetStatusFunc(func(text, level string) {
			server.BroadcastStatus(text, level)
		})
	}

	server.SetArchitectRetryCallback(func() error {
		retryable, ok := daemon.coordinator.(interface {
			RetryLast(context.Context) error
		})
		if !ok {
			return fmt.Errorf("architect retry is not supported")
		}
		return retryable.RetryLast(daemon.ctx)
	})

	server.SetArchitectContinueCallback(func() error {
		continuable, ok := daemon.coordinator.(interface {
			Continue(context.Context) error
		})
		if !ok {
			return fmt.Errorf("architect continue is not supported")
		}
		return continuable.Continue(daemon.ctx)
	})

	server.SetArchitectResumeCallback(func() error {
		resumable, ok := daemon.coordinator.(interface {
			Resume(context.Context) error
		})
		if !ok {
			return fmt.Errorf("architect resume is not supported")
		}
		return resumable.Resume(daemon.ctx)
	})

	server.SetBuildSpecContinueCallback(func() error {
		daemon.buildSpecMu.Lock()
		daemon.buildSpecContinueArmed = true
		daemon.buildSpecMu.Unlock()
		defer func() {
			daemon.buildSpecMu.Lock()
			daemon.buildSpecContinueArmed = false
			daemon.buildSpecMu.Unlock()
		}()
		if err := daemon.continueBuildSpecAgents(); err != nil {
			return err
		}
		daemon.startBuildSpecExecutionMonitor()
		return nil
	})
	server.SetBuildSpecStopAgentsCallback(daemon.stopBuildSpecAgents)

	server.SetArchitectStatusCallback(func() string {
		statusable, ok := daemon.coordinator.(interface{ SessionStatus() string })
		if !ok {
			return "Solution Architect session status is not supported"
		}
		return statusable.SessionStatus()
	})

	server.SetArchitectShowSpecCallback(func() (string, error) {
		data, err := os.ReadFile(filepath.Join(daemon.projectRoot, "docs", "build_spec.md"))
		if err != nil {
			return "", fmt.Errorf("read docs/build_spec.md: %w", err)
		}
		return string(data), nil
	})

	server.SetArchitectResetCallback(func() error {
		return daemon.ResetCurrentRun()
	})

	server.SetBuildSpecCallback(func(spec string) {
		_ = appendBuildSpecTrace(daemon.runState.Root, fmt.Sprintf("command received: %s", strings.TrimSpace(spec)))
		specPath := filepath.Join(daemon.projectRoot, "docs", "build_spec.md")
		if info, err := os.Stat(specPath); err != nil {
			if os.IsNotExist(err) {
				_ = appendBuildSpecTrace(daemon.runState.Root, fmt.Sprintf("preflight failed: missing build spec at %s", specPath))
				daemon.server.BroadcastStatus("Build spec missing: ask the Solution Architect to create docs/build_spec.md, including the docs directory if needed.", "error")
				log.Printf("daemon: build-spec missing at %s", specPath)
				return
			}
			_ = appendBuildSpecTrace(daemon.runState.Root, fmt.Sprintf("preflight failed: stat build spec: %v", err))
			daemon.server.BroadcastStatus(fmt.Sprintf("Build spec check failed: %v", err), "error")
			log.Printf("daemon: build-spec stat failed: %v", err)
			return
		} else if info.IsDir() {
			_ = appendBuildSpecTrace(daemon.runState.Root, fmt.Sprintf("preflight failed: build spec path is directory: %s", specPath))
			daemon.server.BroadcastStatus("Build spec path is a directory; expected docs/build_spec.md file.", "error")
			log.Printf("daemon: build-spec path is directory: %s", specPath)
			return
		} else if info.Size() == 0 {
			_ = appendBuildSpecTrace(daemon.runState.Root, fmt.Sprintf("preflight failed: build spec is empty at %s", specPath))
			daemon.server.BroadcastStatus("Build spec is empty: ask the Solution Architect to write the finalized spec before /build-spec.", "error")
			log.Printf("daemon: build-spec empty at %s", specPath)
			return
		}
		specData, err := os.ReadFile(specPath)
		if err != nil {
			_ = appendBuildSpecTrace(daemon.runState.Root, fmt.Sprintf("preflight failed: read build spec: %v", err))
			daemon.server.BroadcastStatus(fmt.Sprintf("Build spec read failed: %v", err), "error")
			log.Printf("daemon: build-spec read failed: %v", err)
			return
		}
		if err := daemon.TriggerBuildSpec(specPath, specData); err != nil {
			daemon.server.BroadcastStatus(fmt.Sprintf("Build spec failed to start: %v", err), "error")
			log.Printf("daemon: build-spec failed to start: %v", err)
		}
	})
	server.SetBuildSpecStatusCallback(func() string {
		return daemon.buildSpecStatus()
	})
	server.SetBuildSpecPlanCallback(func() (string, error) {
		return daemon.buildSpecPlan()
	})
	server.SetBuildSpecTraceCallback(func() (string, error) {
		return daemon.buildSpecTrace()
	})
	server.SetBuildSpecCancelCallback(func() error {
		return daemon.cancelBuildSpecAttempt()
	})
	server.SetBuildProgressCallback(func() (*BuildProgressSnapshot, error) {
		return daemon.buildProgressSnapshot()
	})
	server.SetProgressChangedCallback(func(reason string) {
		daemon.refreshBuildProgress(reason)
	})
	server.SetExecutionEventCallback(func(event ExecutionJournalEvent) {
		daemon.recordExecutionEvent(event)
	})
	server.SetRecoveryCallback(func(record RecoveryRecord) {
		daemon.recordRecovery(record)
	})
	server.SetRecoveryResolvedCallback(func(kind, nodeID, agentID string) {
		daemon.resolveRecovery(kind, nodeID, agentID)
	})
	server.SetBehaviorCallback(func(agentID, domainID, kind, evidence string) {
		daemon.observeAgentCoordinationBehavior(agentID, domainID, kind, evidence)
	})
	server.SetInferenceGatewayStatusCallback(func() InferenceGatewayStatus {
		if daemon.inferenceGateway == nil {
			return InferenceGatewayStatus{Enabled: false}
		}
		return daemon.inferenceGateway.Status()
	})
	server.SetInferenceGatewayCapacityCallback(func(capacity int) (InferenceGatewayStatus, error) {
		if daemon.inferenceGateway == nil || !daemon.config.GatewayEnabled() {
			return InferenceGatewayStatus{Enabled: false}, fmt.Errorf("inference gateway is disabled")
		}
		return daemon.inferenceGateway.SetCapacity(capacity)
	})
	server.SetAgentListCallback(func() []AgentInfo {
		infos := []AgentInfo{{
			AgentID:  "coordinator",
			DomainID: "coordinator",
			State:    "Available",
		}}
		infos = append(infos, daemon.allocator.AgentInfos()...)
		return infos
	})

	daemon.configureAllocatorCallbacks()

	return daemon, nil
}

func (d *Daemon) configureAllocatorCallbacks() {
	if d == nil || d.allocator == nil || d.server == nil {
		return
	}
	d.allocator.SetStatusFunc(func(text, level string) {
		d.server.BroadcastStatus(text, level)
	})
	d.allocator.SetAgentStatusFunc(func(agentID, text, level string) {
		d.server.BroadcastAgentStatus(agentID, text, level)
	})
	d.allocator.SetAgentStateFunc(func(agentID, domainID, state, reason string) {
		d.recordBuildSpecAgentState(agentID, domainID, state, reason)
		d.recordAgentLifecycleProgress(agentID, domainID, state, reason)
		level := "info"
		if state == "Crashed" {
			level = "error"
		}
		text := fmt.Sprintf("%s is %s", agentID, state)
		if strings.TrimSpace(reason) != "" {
			text += ": " + strings.TrimSpace(reason)
		}
		d.server.BroadcastAgentStatus(agentID, text, level)
	})
	d.allocator.SetLifecycleFunc(func(agentID, domainID string, event supervisor.AgentLifecycleEvent) {
		d.observeAgentLifecycleBehavior(agentID, domainID, event)
		d.recordAgentLifecycleRecovery(agentID, domainID, event)
	})
	d.allocator.SetBroadcastFunc(func(msg hub.Message) {
		d.server.BroadcastHubEvent(msg)
	})
	d.allocator.SetAgentRuntimeCapabilityFunc(d.server.IssueAgentRuntimeCapability)
	if d.auditor != nil {
		d.auditor.SetSuppressStaleNodeFunc(func(node dag.DagNode) bool {
			return d.shouldSuppressStaleNodeAudit(node)
		})
		d.auditor.SetStaleNodeFunc(func(node dag.DagNode, elapsed time.Duration) {
			d.recordStaleNode(node, elapsed)
		})
	}
}

func (d *Daemon) shouldSuppressStaleNodeAudit(node dag.DagNode) bool {
	if d == nil || d.runState == nil || strings.TrimSpace(node.ID) == "" {
		return false
	}
	d.buildSpecMu.Lock()
	var attempt *BuildSpecAttempt
	if d.buildSpecAttempt != nil {
		copyAttempt := *d.buildSpecAttempt
		attempt = &copyAttempt
	}
	d.buildSpecMu.Unlock()
	if attempt == nil {
		loaded, err := loadBuildSpecAttempt(d.runState.Root)
		if err != nil {
			return false
		}
		attempt = loaded
	}
	if attempt == nil || !attemptOwnsNode(attempt, node.ID) {
		return false
	}
	switch attempt.Status {
	case BuildSpecAttemptActive, BuildSpecAttemptPaused, BuildSpecAttemptAllocating:
	default:
		return false
	}
	expectedAgent := strings.TrimSpace(node.AssignedAgent)
	if expectedAgent == "" && strings.TrimSpace(node.DomainID) != "" {
		expectedAgent = "agent-" + strings.TrimSpace(node.DomainID)
	}
	if expectedAgent == "" || d.allocator == nil {
		return true
	}
	for _, info := range d.allocator.AgentInfos() {
		if info.AgentID == expectedAgent && info.State == "Running" {
			return false
		}
	}
	return true
}

func attemptOwnsNode(attempt *BuildSpecAttempt, nodeID string) bool {
	if attempt == nil || strings.TrimSpace(nodeID) == "" {
		return false
	}
	for _, id := range attempt.CreatedNodeIDs {
		if id == nodeID {
			return true
		}
	}
	if attempt.Plan != nil {
		for _, node := range attempt.Plan.Nodes {
			if node.ID == nodeID {
				return true
			}
		}
	}
	return false
}

func (d *Daemon) recordBuildSpecAgentState(agentID, domainID, state, reason string) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(state) == "" {
		return
	}

	d.buildSpecMu.Lock()
	defer d.buildSpecMu.Unlock()

	attempt := d.buildSpecAttempt
	if attempt == nil {
		loaded, err := loadBuildSpecAttempt(d.runState.Root)
		if err != nil || loaded == nil {
			return
		}
		switch loaded.Status {
		case BuildSpecAttemptPlanning, BuildSpecAttemptCommitting, BuildSpecAttemptAllocating, BuildSpecAttemptActive, BuildSpecAttemptPaused:
			attempt = loaded
			d.buildSpecAttempt = attempt
		default:
			return
		}
	}

	attempt.RecordAgentState(agentID, domainID, state, reason)
	_ = saveBuildSpecAttempt(d.runState.Root, attempt)
}

func (d *Daemon) recordExecutionEvent(event ExecutionJournalEvent) {
	if d == nil || d.runState == nil {
		return
	}
	attempt, _ := loadBuildSpecAttempt(d.runState.Root)
	event.RunID = d.runState.RunID
	if attempt != nil {
		event.AttemptID = attempt.AttemptID
	}
	if event.Severity == "" {
		event.Severity = "info"
	}
	_ = appendExecutionJournalEvent(d.runState.Root, event)
}

func (d *Daemon) recordRecovery(record RecoveryRecord) {
	if d == nil || d.runState == nil {
		return
	}
	attempt, _ := loadBuildSpecAttempt(d.runState.Root)
	record.RunID = d.runState.RunID
	if attempt != nil {
		record.AttemptID = attempt.AttemptID
	}
	_ = upsertRecoveryRecord(d.runState.Root, record)
}

func (d *Daemon) resolveRecovery(kind, nodeID, agentID string) {
	if d == nil || d.runState == nil {
		return
	}
	_ = markRecoveryResolved(d.runState.Root, kind, nodeID, agentID)
}

func (d *Daemon) recordAgentLifecycleProgress(agentID, domainID, state, reason string) {
	state = strings.TrimSpace(state)
	if state == "" {
		return
	}
	severity := "info"
	kind := "agent_state"
	summary := fmt.Sprintf("%s is %s", agentID, state)
	if strings.TrimSpace(reason) != "" {
		summary += ": " + strings.TrimSpace(reason)
	}
	switch state {
	case "Crashed":
		kind = "agent_failed"
		severity = "error"
		d.recordRecovery(RecoveryRecord{
			FailureID:        recoveryID("agent_failed", "", agentID),
			Kind:             "agent_failed",
			Severity:         "error",
			Status:           "open",
			AgentID:          agentID,
			DomainID:         domainID,
			Summary:          summary,
			LastError:        reason,
			SuggestedCommand: "/continue-agents",
		})
	case "Stopped":
		kind = "agent_stopped"
		severity = "warn"
	case "Running":
		d.resolveRecovery("agent_failed", "", agentID)
	}
	d.recordExecutionEvent(ExecutionJournalEvent{
		Kind:     kind,
		Severity: severity,
		AgentID:  agentID,
		DomainID: domainID,
		Summary:  summary,
	})
	d.refreshBuildProgress(kind)
}

func (d *Daemon) recordAgentLifecycleRecovery(agentID, domainID string, event supervisor.AgentLifecycleEvent) {
	if !event.IsError {
		return
	}
	kind, severity, summary, command := classifyLifecycleFailure(agentID, event)
	if kind == "" {
		return
	}
	d.recordRecovery(RecoveryRecord{
		FailureID:        recoveryID(kind, "", agentID),
		Kind:             kind,
		Severity:         severity,
		Status:           "open",
		AgentID:          agentID,
		DomainID:         domainID,
		Summary:          summary,
		LastError:        event.Error,
		SuggestedCommand: command,
	})
	d.recordExecutionEvent(ExecutionJournalEvent{
		Kind:     kind,
		Severity: severity,
		AgentID:  agentID,
		DomainID: domainID,
		Summary:  summary,
	})
	d.refreshBuildProgress(kind)
}

func classifyLifecycleFailure(agentID string, event supervisor.AgentLifecycleEvent) (kind, severity, summary, command string) {
	text := strings.ToLower(strings.TrimSpace(event.Kind + " " + event.Error + " " + event.Content))
	severity = "warn"
	command = "/continue-agents"
	switch {
	case strings.Contains(text, "401") || strings.Contains(text, "403") || strings.Contains(text, "auth") || strings.Contains(text, "unauthorized") || strings.Contains(text, "forbidden"):
		return "provider_auth_error", "error", fmt.Sprintf("%s has provider authentication/configuration failure.", agentID), "/progress"
	case strings.Contains(text, "429") || strings.Contains(text, "rate limit"):
		return "provider_rate_limited", "warn", fmt.Sprintf("%s is rate limited by its inference provider.", agentID), "/continue-agents"
	case strings.Contains(text, "timeout") || strings.Contains(text, "econnreset") || strings.Contains(text, "network"):
		return "network_timeout", "warn", fmt.Sprintf("%s hit an inference/network timeout.", agentID), "/continue-agents"
	case strings.Contains(text, "unavailable") || strings.Contains(text, "overloaded") || strings.Contains(text, "503"):
		return "provider_unavailable", "warn", fmt.Sprintf("%s inference provider is unavailable.", agentID), "/continue-agents"
	case strings.Contains(text, "context") && (strings.Contains(text, "length") || strings.Contains(text, "window") || strings.Contains(text, "too long")):
		return "context_window_error", "error", fmt.Sprintf("%s hit a context-window failure.", agentID), "/progress"
	case strings.Contains(text, "stream closed"):
		return "agent_stream_closed", "warn", fmt.Sprintf("%s event stream closed unexpectedly.", agentID), "/continue-agents"
	case event.Kind == "agent_error" || event.Kind == "tool_error":
		return "agent_error", "error", fmt.Sprintf("%s reported an execution error.", agentID), "/continue-agents"
	default:
		return "", "", "", ""
	}
}

func (d *Daemon) recordStaleNode(node dag.DagNode, elapsed time.Duration) {
	if strings.TrimSpace(node.ID) == "" {
		return
	}
	d.staleMu.Lock()
	if d.staleReported == nil {
		d.staleReported = make(map[string]bool)
	}
	if d.staleReported[node.ID] {
		d.staleMu.Unlock()
		return
	}
	d.staleReported[node.ID] = true
	d.staleMu.Unlock()

	summary := fmt.Sprintf("Task %s has been active without progress for %s.", node.ID, elapsed.Round(time.Second))
	d.recordRecovery(RecoveryRecord{
		FailureID:        recoveryID("stale_active_work", node.ID, node.AssignedAgent),
		Kind:             "stale_active_work",
		Severity:         "warn",
		Status:           "open",
		AgentID:          node.AssignedAgent,
		DomainID:         node.DomainID,
		NodeID:           node.ID,
		Summary:          summary,
		SuggestedCommand: "/continue-agents",
	})
	d.recordExecutionEvent(ExecutionJournalEvent{
		Kind:     "stale_active_work",
		Severity: "warn",
		AgentID:  node.AssignedAgent,
		DomainID: node.DomainID,
		NodeID:   node.ID,
		Summary:  summary,
	})
	d.refreshBuildProgress("stale_active_work")
}

func newPiCoordinatorForRun(projectRoot string, config *Config, runState *RunState) *coordinator.PiCoordinator {
	infConfig := config.Inference.Coordinator
	provider := infConfig.Provider
	model := infConfig.Model
	endpoint := infConfig.Endpoint
	var envVars []string
	if infConfig.UseProfile != "" {
		if profile, ok := config.Inference.Models[infConfig.UseProfile]; ok {
			provider = profile.Provider
			model = profile.Model
			if profile.Endpoint != "" {
				endpoint = profile.Endpoint
			}
			for k, v := range profile.Env {
				envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
			}
		}
	}
	if config.GatewayEnabled() {
		profileName := infConfig.UseProfile
		if profileName == "" {
			profileName = config.InferenceGateway.TargetProfile
		}
		targetProvider := provider
		targetModel := model
		gatewayURL := config.InferenceGateway.PublicBaseURL
		if gatewayURL == "" {
			gatewayURL = "http://" + config.InferenceGateway.ListenAddr
		}
		endpoint = gatewayURL
		provider = "aion-gateway"
		envVars = append(envVars,
			"AION_INFERENCE_GATEWAY_ENABLED=true",
			fmt.Sprintf("AION_INFERENCE_GATEWAY_URL=%s", gatewayURL),
			fmt.Sprintf("AION_INFERENCE_GATEWAY_KEY=%s", config.InferenceGateway.GatewayKey),
			fmt.Sprintf("AION_TARGET_PROVIDER=%s", targetProvider),
			fmt.Sprintf("AION_TARGET_PROFILE=%s", profileName),
			fmt.Sprintf("AION_TARGET_MODEL=%s", targetModel),
			fmt.Sprintf("AION_TARGET_API=%s", gatewayAPIForProvider(targetProvider)),
		)
	}

	return coordinator.NewPiCoordinator(projectRoot, coordinator.PiCoordinatorConfig{
		Binary:                     config.Agents.CommandPath,
		SessionDir:                 runState.PiSessionsDir,
		SessionStoreDir:            runState.AgentSessionsDir,
		Provider:                   provider,
		Model:                      model,
		Endpoint:                   endpoint,
		SkillPaths:                 config.Agents.SkillPaths,
		ExtensionPaths:             config.Agents.ExtensionPaths,
		Env:                        envVars,
		PlannerStartTimeout:        time.Duration(config.Health.CoordinatorPlannerStartTimeoutSec) * time.Second,
		PlannerFirstRequestTimeout: time.Duration(config.Health.CoordinatorPlannerFirstRequestTimeoutSec) * time.Second,
		PlannerArtifactTimeout:     time.Duration(config.Health.CoordinatorPlannerArtifactTimeoutSec) * time.Second,
	})
}

// Start initializes and starts the Orchestrator daemon.
func (d *Daemon) Start() error {
	log.Printf("daemon: starting aion-kernel on %s", d.config.Orchestrator.ListenAddr)
	log.Printf("daemon: project root: %s", d.projectRoot)

	if d.inferenceGateway != nil {
		if err := d.inferenceGateway.Start(); err != nil {
			return err
		}
	}
	if err := d.server.StartUnix(d.ipcPaths.ControlSocket); err != nil {
		if d.inferenceGateway != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = d.inferenceGateway.Shutdown(shutdownCtx)
			cancel()
		}
		return err
	}

	// Start server in background
	go func() {
		if err := d.server.ListenAndServe(d.config.Orchestrator.ListenAddr); err != nil {
			log.Printf("daemon: server error: %v", err)
		}
	}()

	// Start Auditor
	d.auditor.Start(d.ctx)

	if err := d.startArchitectPhase(); err != nil {
		log.Printf("daemon: failed to start architect coordinator: %v", err)
	}

	// Give server a moment to bind
	time.Sleep(100 * time.Millisecond)
	if addr := d.server.Addr(); addr != "" {
		if err := writeServerInfo(d.projectRoot, d.runState, addr); err != nil {
			log.Printf("daemon: failed to write server info: %v", err)
		}
	}

	if attempt, err := loadBuildSpecAttempt(d.runState.Root); err == nil && attempt != nil {
		switch attempt.Status {
		case BuildSpecAttemptActive, BuildSpecAttemptPaused, BuildSpecAttemptAllocating, BuildSpecAttemptPlanning, BuildSpecAttemptCommitting:
			d.seedBuildSpecAttemptContext(attempt)
			d.refreshBuildProgress("startup")
			d.server.BroadcastTransientStatus("Persisted build-spec attempt loaded. Issue /continue-agents to resume domain work.", "info")
		}
	}

	log.Printf("daemon: aion-kernel ready")
	return nil
}

func (d *Daemon) startArchitectPhase() error {
	if piCoord, ok := d.coordinator.(interface{ StartArchitect(context.Context) error }); ok {
		if err := piCoord.StartArchitect(d.ctx); err != nil {
			return err
		} else {
			// Register architect agent with hub router for message delivery
			if agent := d.coordinator.GetArchitectAgent(); agent != nil {
				if sup, ok := agent.(*supervisor.AgentSupervisor); ok {
					d.hubRouter.RegisterAgent("orchestrator", sup)
					sup.SetStatusFunc(func(text, level string) {
						if d.buildSpecAttemptIsActive() {
							return
						}
						d.server.BroadcastStatus(text, level)
					})
					sup.SetBroadcastFunc(func(msg hub.Message) {
						// Only process through hub router if it's NOT a TUI-bound message
						if msg.ToAgent != "tui" {
							_ = d.hubRouter.Route(msg)
						}
						// Always broadcast to TUI for observability
						d.server.BroadcastHubEvent(msg)
					})
					if replayable, ok := d.coordinator.(interface{ ReplayArchitectHistory() []hub.Message }); ok {
						for _, msg := range replayable.ReplayArchitectHistory() {
							d.server.BroadcastHubEvent(msg)
						}
					}
					d.server.BroadcastStatus("Solution Architect session started. Send messages to 'orchestrator' to refine the spec.", "ok")
				}
			}
		}
	}
	return nil
}

func (d *Daemon) ResetCurrentRun() error {
	d.resetMu.Lock()
	defer d.resetMu.Unlock()

	d.server.BroadcastStatus("Resetting current Aion run...", "warn")

	if stoppable, ok := d.coordinator.(interface{ StopArchitect() error }); ok {
		if err := stoppable.StopArchitect(); err != nil {
			return fmt.Errorf("stop architect: %w", err)
		}
	}
	d.stopBuildSpecExecutionMonitor()
	d.allocator.StopAll()

	oldDAG := d.dagManager
	oldRun := d.runState

	newRun, err := CreateNewCurrentRun(d.projectRoot, d.config)
	if err != nil {
		return fmt.Errorf("create new run: %w", err)
	}
	d.config.Agents.SessionDir = newRun.PiSessionsDir

	dagMgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: newRun.DagFile,
		WalFilePath:   newRun.WalFile,
		MaxNodes:      d.config.Orchestrator.MaxActiveNodes,
		FlushDeadline: d.config.FlushDeadline(),
	})
	if err != nil {
		return fmt.Errorf("init reset dag manager: %w", err)
	}

	lockMgr := locking.NewManager(d.config.Agents.SharedFiles)
	stubReg := stub.NewRegistry()
	hubRouter := hub.NewRouter(newRun.LogsDir)
	alloc := NewAllocator(d.config, d.projectRoot, hubRouter, dagMgr)
	alloc.SetIPCDir(d.ipcPaths.Dir)
	alloc.SetStatusFunc(func(text, level string) {
		d.server.BroadcastStatus(text, level)
	})

	coord := coordinator.Coordinator(newPiCoordinatorForRun(d.projectRoot, d.config, newRun))
	if statusAware, ok := coord.(interface{ SetStatusFunc(func(string, string)) }); ok {
		statusAware.SetStatusFunc(func(text, level string) {
			d.server.BroadcastStatus(text, level)
		})
	}

	d.dagManager = dagMgr
	d.lockManager = lockMgr
	d.stubRegistry = stubReg
	d.hubRouter = hubRouter
	d.allocator = alloc
	d.coordinator = coord
	d.runState = newRun
	d.auditor.dagManager = dagMgr
	d.auditor.lockManager = lockMgr
	d.auditor.hubRouter = hubRouter
	d.server.SetRuntimeSubsystems(dagMgr, lockMgr, stubReg, d.memoryStore)
	d.server.SetLogsDir(newRun.LogsDir)
	d.server.ClearAgentCapabilities()
	d.server.ClearHubHistory()
	d.configureAllocatorCallbacks()

	if oldDAG != nil {
		if err := oldDAG.Close(); err != nil {
			log.Printf("daemon: close old DAG manager during reset: %v", err)
		}
	}
	if oldRun != nil {
		if err := oldRun.Delete(); err != nil {
			return fmt.Errorf("delete old run: %w", err)
		}
	}
	if err := os.Remove(filepath.Join(d.projectRoot, "docs", "build_spec.md")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete build spec: %w", err)
	}

	if err := d.startArchitectPhase(); err != nil {
		return fmt.Errorf("start reset architect: %w", err)
	}
	d.server.BroadcastStatus("Current run reset; fresh Architect session is ready.", "ok")
	return nil
}

// WaitForShutdown blocks until a termination signal is received.
func (d *Daemon) WaitForShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Printf("daemon: received %s, shutting down...", sig)

	d.Shutdown()
}

// Shutdown performs graceful shutdown of all subsystems.
func (d *Daemon) Shutdown() {
	if checkpointable, ok := d.coordinator.(interface{ ShutdownCheckpoint() }); ok {
		checkpointable.ShutdownCheckpoint()
	}

	log.Println("daemon: stopping background tasks (auditor)...")
	d.stopBuildSpecExecutionMonitor()
	if d.cancel != nil {
		d.cancel()
	}

	log.Println("daemon: stopping agents...")
	d.allocator.StopAll()

	log.Println("daemon: stopping server...")
	if d.inferenceGateway != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := d.inferenceGateway.Shutdown(shutdownCtx); err != nil {
			log.Printf("daemon: inference gateway shutdown failed: %v", err)
		}
		cancel()
	}
	d.server.Stop()
	if err := deleteServerInfo(d.projectRoot); err != nil && !os.IsNotExist(err) {
		log.Printf("daemon: failed to remove server info: %v", err)
	}

	log.Println("daemon: flushing DAG...")
	d.dagManager.Close()

	log.Println("daemon: shutdown complete")
}

// DagManager returns the DAG manager.
func (d *Daemon) DagManager() *dag.Manager {
	return d.dagManager
}

// LockManager returns the lock manager.
func (d *Daemon) LockManager() *locking.Manager {
	return d.lockManager
}

// StubRegistry returns the stub registry.
func (d *Daemon) StubRegistry() *stub.Registry {
	return d.stubRegistry
}

// HubRouter returns the hub router.
func (d *Daemon) HubRouter() *hub.Router {
	return d.hubRouter
}

// Server returns the orchestrator server.
func (d *Daemon) Server() *Server {
	return d.server
}

// Allocator returns the agent allocator.
func (d *Daemon) Allocator() *Allocator {
	return d.allocator
}

// SubmitWork runs a project scan, generates a plan via the coordinator,
// commits it to the DAG, and generates initial domain prompts.
func (d *Daemon) SubmitWork(ctx context.Context, userPrompt string) error {
	log.Printf("daemon: scanning project at %s", d.projectRoot)
	d.server.BroadcastStatus("Scanning project files...", "info")
	scan, err := coordinator.ScanProject(d.projectRoot)
	if err != nil {
		d.server.BroadcastStatus(fmt.Sprintf("Project scan failed: %v", err), "error")
		return fmt.Errorf("daemon: project scan failed: %w", err)
	}

	log.Printf("daemon: requesting plan from coordinator")
	d.server.BroadcastStatus("Requesting plan from coordinator LLM...", "info")
	req := coordinator.PlanRequest{
		UserPrompt:  userPrompt,
		ProjectRoot: d.projectRoot,
		ProjectScan: scan,
	}

	plan, err := d.coordinator.Plan(ctx, req)
	if err != nil {
		d.server.BroadcastStatus(fmt.Sprintf("Planning failed: %v", err), "error")
		return fmt.Errorf("daemon: planning failed: %w", err)
	}
	if err := coordinator.ValidatePlanResponse(plan); err != nil {
		d.server.BroadcastStatus(fmt.Sprintf("Planning validation failed: %v", err), "error")
		return fmt.Errorf("daemon: planning validation failed: %w", err)
	}

	d.server.BroadcastStatus(fmt.Sprintf("Plan received: %d nodes, %d edges. Committing DAG...", len(plan.Nodes), len(plan.Edges)), "info")
	log.Printf("daemon: committing planned DAG (nodes: %d, edges: %d)", len(plan.Nodes), len(plan.Edges))

	// Commit DAG Nodes
	for _, rawNode := range plan.Nodes {
		n := dag.DagNode{
			ID:          rawNode.ID,
			DomainID:    rawNode.DomainID,
			TaskSpec:    rawNode.TaskSpec,
			Status:      dag.StatusPending,
			TargetFiles: rawNode.TargetFiles,
			Priority:    rawNode.Priority,
		}
		if err := d.dagManager.AddNode(n); err != nil {
			return fmt.Errorf("daemon: commit node %s: %w", n.ID, err)
		}
	}

	// Commit DAG Edges
	for _, rawEdge := range plan.Edges {
		e := dag.DagEdge{
			FromNode: rawEdge.FromNode,
			ToNode:   rawEdge.ToNode,
			Type:     dag.EdgeDependency,
		}
		if err := d.dagManager.AddEdge(e); err != nil {
			return fmt.Errorf("daemon: commit edge %s->%s: %w", e.FromNode, e.ToNode, err)
		}
	}

	log.Printf("daemon: generating initial system instructions")
	d.server.BroadcastStatus("Generating agent system instructions...", "info")
	prompts, err := coordinator.GenerateSystemInstructions(plan, "", userPrompt)
	if err != nil {
		return fmt.Errorf("daemon: generate prompts failed: %w", err)
	}

	log.Printf("daemon: allocating agents...")
	d.server.BroadcastStatus("Allocating domain agents...", "info")
	if err := d.allocator.Allocate(ctx, plan.Domains, prompts); err != nil {
		d.server.BroadcastStatus(fmt.Sprintf("Allocation failed: %v", err), "error")
		return fmt.Errorf("daemon: allocation failed: %w", err)
	}

	return nil
}

// CloseProject gracefully closes the orchestrator run after convergence, writes final summaries,
// and shuts down internal modules.
func (d *Daemon) CloseProject() error {
	log.Println("daemon: closing project, halting agents...")
	d.allocator.StopAll()

	if d.memoryStore != nil {
		snap := d.dagManager.Snapshot()
		doneCount := 0
		failedCount := 0
		for _, n := range snap.Nodes {
			if n.Status == dag.StatusDone {
				doneCount++
			} else if n.Status == dag.StatusFailed {
				failedCount++
			}
		}

		entry := memory.MemoryEntry{
			ID:        fmt.Sprintf("project-close-%d", time.Now().Unix()),
			Text:      fmt.Sprintf("Project %s execution finished. %d tasks completed successfully, %d tasks failed.", d.projectRoot, doneCount, failedCount),
			AgentID:   "orchestrator",
			TaskID:    "closure",
			ProjectID: "default",
			Timestamp: time.Now().Unix(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := d.memoryStore.Write(ctx, entry); err != nil {
			log.Printf("daemon: failed to write project closure memory: %v", err)
		} else {
			log.Println("daemon: successfully wrote project closure summary to semantic memory")
		}
	}

	log.Println("daemon: flushing state...")
	d.dagManager.Close()
	d.server.Stop()
	if err := deleteServerInfo(d.projectRoot); err != nil && !os.IsNotExist(err) {
		log.Printf("daemon: failed to remove server info: %v", err)
	}
	if d.cancel != nil {
		d.cancel()
	}

	log.Println("daemon: project successfully closed")
	return nil
}

// MonitorExecution blocks until the full DAG reaches a terminal state.
func (d *Daemon) MonitorExecution(ctx context.Context) error {
	return d.allocator.MonitorExecution(ctx, 2*time.Second)
}

func truncateTraceForStatus(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	const limit = 240
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "…"
}

func (d *Daemon) handleReplan(msg hub.Message) {
	log.Printf("daemon: handling REPLAN due to %s", msg.Type)
	d.server.BroadcastStatus("Replan triggered — re-evaluating DAG...", "warn")

	var edge dag.DagEdge
	if err := json.Unmarshal(msg.Payload, &edge); err != nil {
		log.Printf("daemon: replan parse payload error: %v", err)
		return
	}

	snap := d.dagManager.Snapshot()
	var coordNodes []coordinator.TaskNode
	for _, n := range snap.Nodes {
		coordNodes = append(coordNodes, coordinator.TaskNode{
			ID:          n.ID,
			DomainID:    n.DomainID,
			TaskSpec:    n.TaskSpec,
			TargetFiles: n.TargetFiles,
			Priority:    n.Priority,
		})
	}
	var coordEdges []coordinator.TaskEdge
	for _, e := range snap.Edges {
		coordEdges = append(coordEdges, coordinator.TaskEdge{
			FromNode: e.FromNode,
			ToNode:   e.ToNode,
			Reason:   e.Type.String(),
		})
	}

	req := coordinator.ReplanRequest{
		CurrentNodes: coordNodes,
		CurrentEdges: coordEdges,
		InjectHistory: []coordinator.TaskEdge{{
			FromNode: edge.FromNode,
			ToNode:   edge.ToNode,
		}},
		AffectedNodes: []string{edge.FromNode, edge.ToNode},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := d.coordinator.Replan(ctx, req)
	if err != nil {
		log.Printf("daemon: replan failed: %v", err)
		d.server.BroadcastStatus(fmt.Sprintf("Replan failed: %v", err), "error")
		return
	}

	log.Printf("daemon: replan returned %d modified nodes, %d new edges", len(resp.ModifiedNodes), len(resp.NewEdges))
	d.server.BroadcastStatus(fmt.Sprintf("Replan complete: %d nodes updated, %d new edges", len(resp.ModifiedNodes), len(resp.NewEdges)), "ok")

	// Apply replan response into DAG (best effort)
	for _, mn := range resp.ModifiedNodes {
		// Example: Just update active status if needed, but usually Replan affects TaskSpec which DAG Manager doesn't expose an API for directly over CLI right now.
		// For now we just log it since the default coordinator returns unmodified current nodes.
		_ = mn
	}

	for _, ne := range resp.NewEdges {
		newEdge := dag.DagEdge{
			FromNode: ne.FromNode,
			ToNode:   ne.ToNode,
			Type:     dag.EdgeDependency,
		}
		if err := d.dagManager.AddEdge(newEdge); err != nil {
			log.Printf("daemon: replan apply edge error: %v", err)
		}
	}
}
