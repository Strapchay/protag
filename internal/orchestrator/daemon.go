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
	config           *Config
	dagManager       *dag.Manager
	lockManager      *locking.Manager
	stubRegistry     *stub.Registry
	hubRouter        *hub.Router
	allocator        *Allocator
	server           *Server
	coordinator      coordinator.Coordinator
	memoryStore      memory.Store
	projectRoot      string
	auditor          *Auditor
	runState         *RunState
	ctx              context.Context
	cancel           context.CancelFunc
	resetMu          sync.Mutex
	buildSpecMu      sync.Mutex
	buildSpecCancel  context.CancelFunc
	buildSpecAttempt *BuildSpecAttempt
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

	// Initialize Server
	server := NewServer(dagMgr, lockMgr, stubReg, memStore)
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
		config:       config,
		dagManager:   dagMgr,
		lockManager:  lockMgr,
		stubRegistry: stubReg,
		hubRouter:    hubRouter,
		allocator:    alloc,
		server:       server,
		coordinator:  coord,
		memoryStore:  memStore,
		auditor:      aud,
		runState:     runState,
		projectRoot:  projectRoot,
		ctx:          ctx,
		cancel:       cancel,
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

	// Wire the allocator status callback to the server's broadcast mechanism.
	alloc.SetStatusFunc(func(text, level string) {
		server.BroadcastStatus(text, level)
	})

	return daemon, nil
}

func newPiCoordinatorForRun(projectRoot string, config *Config, runState *RunState) *coordinator.PiCoordinator {
	infConfig := config.Inference.Coordinator
	provider := infConfig.Provider
	model := infConfig.Model
	var envVars []string
	if infConfig.UseProfile != "" {
		if profile, ok := config.Inference.Models[infConfig.UseProfile]; ok {
			provider = profile.Provider
			model = profile.Model
			for k, v := range profile.Env {
				envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
			}
		}
	}

	return coordinator.NewPiCoordinator(projectRoot, coordinator.PiCoordinatorConfig{
		Binary:          config.Agents.CommandPath,
		SessionDir:      runState.PiSessionsDir,
		SessionStoreDir: runState.AgentSessionsDir,
		Provider:        provider,
		Model:           model,
		SkillPaths:      config.Agents.SkillPaths,
		Env:             envVars,
	})
}

// Start initializes and starts the Orchestrator daemon.
func (d *Daemon) Start() error {
	log.Printf("daemon: starting aion-kernel on %s", d.config.Orchestrator.ListenAddr)
	log.Printf("daemon: project root: %s", d.projectRoot)

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
	d.server.ClearHubHistory()

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
	if d.cancel != nil {
		d.cancel()
	}

	log.Println("daemon: stopping agents...")
	d.allocator.StopAll()

	log.Println("daemon: stopping server...")
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
	prompts, err := coordinator.GenerateSystemInstructions(plan, "")
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
