package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	coordination "aion-coordination"
	isolation "aion-isolation"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/supervisor"
)

// Allocator manages the spinning up and termination of domain agents.
type Allocator struct {
	config              *Config
	projectRoot         string
	hubRouter           *hub.Router
	dagManager          *dag.Manager
	activeAgents        map[string]*supervisor.AgentSupervisor
	pendingIncomplete   map[string]string
	incompleteRetries   map[string]int
	mu                  sync.Mutex
	statusFn            func(text, level string) // optional: broadcasts to TUI status bar
	agentStatusFn       func(agentID, text, level string)
	agentStateFn        func(agentID, domainID, state, reason string)
	lifecycleFn         func(agentID, domainID string, event supervisor.AgentLifecycleEvent)
	broadcastFn         func(hub.Message)
	capabilityFn        func(agentID string) (string, error)
	runtimeCapabilityFn func(AgentCapabilityPolicy) (string, error)
	ipcDir              string
	isolationEngine     isolation.Engine
	isolationErr        error
	workInbox           *WorkInboxService
	dispatchedWork      map[string]string
	nodeRecoveryMu      sync.Mutex
	nodeRecoveryContext map[string]string
	integrationGateFn   func(context.Context, *dag.DagData) (bool, error)
}

type AllocationMode string

const (
	AllocationModeInitial AllocationMode = "initial"
	AllocationModeResume  AllocationMode = "resume"
)

type AllocationOptions struct {
	Mode          AllocationMode
	ResumeMessage string
}

// NewAllocator creates a new allocator.
func NewAllocator(config *Config, projectRoot string, hubRouter *hub.Router, dagManager *dag.Manager) *Allocator {
	engine, isolationErr := newDomainIsolationEngine(config)
	ipcPaths, ipcErr := prepareDaemonIPC(projectRoot)
	if isolationErr == nil && ipcErr != nil {
		isolationErr = fmt.Errorf("allocator: prepare IPC: %w", ipcErr)
	}
	return &Allocator{
		config:              config,
		projectRoot:         projectRoot,
		hubRouter:           hubRouter,
		dagManager:          dagManager,
		activeAgents:        make(map[string]*supervisor.AgentSupervisor),
		pendingIncomplete:   make(map[string]string),
		incompleteRetries:   make(map[string]int),
		isolationEngine:     engine,
		isolationErr:        isolationErr,
		ipcDir:              ipcPaths.Dir,
		dispatchedWork:      make(map[string]string),
		nodeRecoveryContext: make(map[string]string),
	}
}

func (a *Allocator) SetWorkInboxService(service *WorkInboxService) {
	a.mu.Lock()
	a.workInbox = service
	if a.dispatchedWork == nil {
		a.dispatchedWork = make(map[string]string)
	}
	a.mu.Unlock()
}

func (a *Allocator) SetIntegrationGateFunc(fn func(context.Context, *dag.DagData) (bool, error)) {
	a.mu.Lock()
	a.integrationGateFn = fn
	a.mu.Unlock()
}

// SetStatusFunc registers a callback for live status broadcasts.
func (a *Allocator) SetStatusFunc(fn func(text, level string)) {
	a.statusFn = fn
}

func (a *Allocator) SetAgentStatusFunc(fn func(agentID, text, level string)) {
	a.agentStatusFn = fn
}

func (a *Allocator) SetAgentStateFunc(fn func(agentID, domainID, state, reason string)) {
	a.agentStateFn = fn
}

func (a *Allocator) SetLifecycleFunc(fn func(agentID, domainID string, event supervisor.AgentLifecycleEvent)) {
	a.lifecycleFn = fn
}

func (a *Allocator) SetBroadcastFunc(fn func(hub.Message)) {
	a.broadcastFn = fn
}

func (a *Allocator) SetAgentCapabilityFunc(fn func(agentID string) (string, error)) {
	a.capabilityFn = fn
}

func (a *Allocator) SetAgentRuntimeCapabilityFunc(fn func(AgentCapabilityPolicy) (string, error)) {
	a.runtimeCapabilityFn = fn
}

func (a *Allocator) SetIPCDir(path string) {
	a.ipcDir = strings.TrimSpace(path)
}

func (a *Allocator) emitStatus(text, level string) {
	if a.statusFn != nil {
		a.statusFn(text, level)
	}
}

func (a *Allocator) RecordAgentActivity(agentID string, phase ...string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	a.mu.Lock()
	agent := a.activeAgents[agentID]
	a.mu.Unlock()
	if agent == nil {
		return false
	}
	agent.RecordActivity(phase...)
	return true
}

// Allocate writes the context files and spins up the agents for the planned domains.
func (a *Allocator) Allocate(ctx context.Context, domains []coordinator.Domain, prompts map[string]string) error {
	return a.AllocateWithOptions(ctx, domains, prompts, AllocationOptions{Mode: AllocationModeInitial})
}

// AllocateWithOptions writes the context files and spins up agents, optionally
// resuming existing Pi sessions with a concise resume prompt instead of sending
// the full initial domain prompt again.
func (a *Allocator) AllocateWithOptions(ctx context.Context, domains []coordinator.Domain, prompts map[string]string, options AllocationOptions) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if options.Mode == "" {
		options.Mode = AllocationModeInitial
	}

	agentBaseDir := resolveProjectPath(a.projectRoot, a.config.Agents.SessionDir)
	if err := os.MkdirAll(agentBaseDir, 0755); err != nil {
		return fmt.Errorf("allocator: create session dir: %w", err)
	}

	allocatedCount := 0
	for _, domain := range domains {
		agentID := fmt.Sprintf("agent-%s", domain.DomainID)
		if options.Mode == AllocationModeResume && !a.domainNeedsResume(domain.DomainID, agentID) {
			log.Printf("allocator: skipping completed domain agent %s during resume", agentID)
			continue
		}
		prompt, ok := prompts[domain.DomainID]
		if !ok {
			log.Printf("allocator: warning: no prompt generated for domain %s", domain.DomainID)
			continue
		}

		agentDir := filepath.Join(agentBaseDir, agentID)
		if err := os.MkdirAll(agentDir, 0755); err != nil {
			return fmt.Errorf("allocator: create agent dir %s: %w", agentID, err)
		}

		agentsMDPath := filepath.Join(agentDir, "config", "AGENTS.md")
		if err := os.MkdirAll(filepath.Dir(agentsMDPath), 0755); err != nil {
			return fmt.Errorf("allocator: create Pi config dir for %s: %w", agentID, err)
		}
		if err := os.WriteFile(agentsMDPath, []byte(prompt), 0644); err != nil {
			return fmt.Errorf("allocator: write AGENTS.md for %s: %w", agentID, err)
		}
		isolatedRuntime, err := a.prepareIsolatedAgentRuntime(agentID, domain, agentDir)
		if err != nil {
			return err
		}
		generationPath, err := isolationGenerationStorePath(agentBaseDir, agentID)
		if err != nil {
			return err
		}
		isolationGeneration, err := loadIsolationGeneration(generationPath)
		if err != nil {
			return fmt.Errorf("allocator: load isolation generation for %s: %w", agentID, err)
		}

		// Resolve model profile
		infConfig := a.config.Inference.DomainAgents
		provider := infConfig.Provider
		model := infConfig.Model
		endpoint := infConfig.Endpoint
		envVars := []string{
			fmt.Sprintf("AION_AGENT_ID=%s", agentID),
			fmt.Sprintf("AION_DOMAIN_ID=%s", domain.DomainID),
			fmt.Sprintf("AION_AGENT_SESSION_DIR=%s", agentSandboxSessionDir),
			fmt.Sprintf("AION_ORCHESTRATOR_SOCKET=%s", agentOrchestratorSocket),
		}

		if infConfig.UseProfile != "" {
			if profile, ok := a.config.Inference.Models[infConfig.UseProfile]; ok {
				provider = profile.Provider
				model = profile.Model
				if profile.Endpoint != "" {
					endpoint = profile.Endpoint
				}
				if !a.config.GatewayEnabled() {
					for k, v := range profile.Env {
						envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
					}
				}
			} else {
				log.Printf("allocator: warning: model profile %s not found", infConfig.UseProfile)
			}
		} else {
			// Fallback to legacy inline config if no profile used
			if provider == "" {
				provider = a.config.Inference.Fallback.Provider
			}
			if model == "" {
				model = a.config.Inference.Fallback.Model
			}
			if endpoint == "" {
				endpoint = a.config.Inference.Fallback.Endpoint
			}
		}
		targetProfile := infConfig.UseProfile
		if targetProfile == "" {
			targetProfile = a.config.InferenceGateway.TargetProfile
		}
		targetProvider := provider
		targetModel := model
		if a.config.GatewayEnabled() {
			endpoint = "http://unix"
			provider = "aion-gateway"
			envVars = append(envVars,
				"AION_INFERENCE_GATEWAY_ENABLED=true",
				fmt.Sprintf("AION_INFERENCE_SOCKET=%s", agentInferenceSocket),
				fmt.Sprintf("AION_TARGET_PROFILE=%s", targetProfile),
				fmt.Sprintf("AION_TARGET_MODEL=%s", targetModel),
				fmt.Sprintf("AION_TARGET_API=%s", gatewayAPIForProvider(targetProvider)),
			)
			envVars = append(envVars, targetInferenceEnv()...)
		}

		// Domain instructions live in Pi's configured AGENTS.md context. Starting
		// idle avoids spending an inference turn on orientation before a node is
		// actually ready for this domain.
		initialPrompt := ""
		resumedWorkItemID := ""
		resumeAgent := options.Mode == AllocationModeResume && hasReusablePiSession(agentDir)
		if resumeAgent {
			initialPrompt, resumedWorkItemID = a.resumeLaunchDispatch(agentID, domain.DomainID, options.ResumeMessage)
			if resumedWorkItemID != "" {
				a.dispatchedWork[agentID] = resumedWorkItemID
			}
		}

		agentConfig := supervisor.AgentConfig{
			AgentID:             agentID,
			DomainID:            domain.DomainID,
			AssignedPaths:       domain.AssignedPaths,
			InitialPrompt:       initialPrompt,
			IsolationEngine:     a.isolationEngine,
			IsolationPolicy:     isolatedRuntime.policy,
			IsolationGeneration: isolationGeneration,
			PersistIsolationGeneration: func(generation uint64) error {
				return saveIsolationGeneration(generationPath, generation)
			},
			PrepareGenerationEnv: func(generation uint64) ([]string, error) {
				policy := AgentCapabilityPolicy{
					AgentID:    agentID,
					DomainID:   domain.DomainID,
					Profile:    targetProfile,
					Provider:   targetProvider,
					Model:      targetModel,
					Generation: generation,
				}
				var capability string
				var err error
				switch {
				case a.runtimeCapabilityFn != nil:
					capability, err = a.runtimeCapabilityFn(policy)
				case a.capabilityFn != nil:
					capability, err = a.capabilityFn(agentID)
				default:
					return nil, nil
				}
				if err != nil {
					return nil, fmt.Errorf("allocator: issue generation %d capability for %s: %w", generation, agentID, err)
				}
				return []string{fmt.Sprintf("AION_AGENT_CAPABILITY=%s", capability)}, nil
			},
			PiAgent: supervisor.PiAgentConfig{
				Binary:         isolatedRuntime.binary,
				SessionDir:     agentSandboxSessionDir,
				HostSessionDir: agentDir,
				ResumeSession:  resumeAgent,
				WorkingDir:     agentSandboxRoot,
				Provider:       provider,
				Model:          model,
				Endpoint:       endpoint,
				SkillPaths:     isolatedRuntime.skillPaths,
				ExtensionPaths: isolatedRuntime.extensionPaths,
				Env:            envVars,
			},
			Cgroup: supervisor.CgroupConfig{
				Enabled:        a.config.Cgroups.Enabled && strings.ToLower(strings.TrimSpace(a.config.Cgroups.Mode)) != "disabled",
				Mode:           a.config.Cgroups.Mode,
				BasePath:       a.config.Cgroups.BasePath,
				AgentID:        agentID,
				MemoryMaxBytes: a.config.Cgroups.MemoryMaxMB * 1024 * 1024,
				PidsMax:        a.config.Cgroups.PidsMax,
			},
			HeartbeatTimeout:      time.Duration(a.config.Health.HeartbeatTimeoutSec) * time.Second,
			ProgressTimeout:       time.Duration(a.config.Health.ProgressTimeoutSec) * time.Second,
			ExternalActivityStale: time.Duration(a.config.Health.ExternalActivityStaleTimeoutSec) * time.Second,
			ExternalActivityMax:   time.Duration(a.config.Health.ExternalActivityMaxDurationSec) * time.Second,
			MaxCrashRestarts:      3,
		}

		agent := supervisor.NewAgentSupervisor(agentConfig)
		if a.agentStatusFn != nil {
			agent.SetStatusFunc(func(text, level string) {
				a.agentStatusFn(agentID, text, level)
			})
		} else if a.statusFn != nil {
			agent.SetStatusFunc(a.statusFn)
		}
		if a.broadcastFn != nil {
			agent.SetBroadcastFunc(a.broadcastFn)
		}
		if a.agentStateFn != nil || a.lifecycleFn != nil {
			domainID := domain.DomainID
			agent.SetLifecycleFunc(func(event supervisor.AgentLifecycleEvent) {
				a.handleAgentLifecycle(agentID, domainID, event)
			})
		}
		a.activeAgents[agentID] = agent

		// Register with Hub Router
		a.hubRouter.RegisterAgent(agentID, agent)

		if err := agent.Start(ctx); err != nil {
			return fmt.Errorf("allocator: spawn agent %s: %w", agentID, err)
		}
		if resumeAgent {
			a.emitStatus(fmt.Sprintf("Agent %s resumed from existing Pi session (domain: %s)", agentID, domain.DomainID), "info")
		}

		a.emitStatus(fmt.Sprintf("Agent %s spawned (domain: %s)", agentID, domain.DomainID), "info")
		log.Printf("allocator: spawned agent %s (domain: %s)", agentID, domain.DomainID)
		allocatedCount++
	}

	a.emitStatus(fmt.Sprintf("%d agent(s) allocated — awaiting task dispatch", allocatedCount), "ok")
	return nil
}

func (a *Allocator) domainNeedsResume(domainID, agentID string) bool {
	if _, ok := a.actionableContractWorkForAgent(context.Background(), agentID); ok {
		return true
	}
	if a.dagManager == nil {
		return true
	}
	for _, node := range a.dagManager.Snapshot().Nodes {
		if node.DomainID == domainID && !node.Status.IsTerminal() {
			return true
		}
	}
	return false
}

func defaultDomainAgentResumeMessage(agentID, domainID string) string {
	return fmt.Sprintf(`Resume your existing Aion domain-agent session.

Agent: %s
Domain: %s

	Your current working directory is the isolated writable view for your domain. Paths outside your owned mounts are not present.
	Continue from your persisted Pi session context. Do not restart from the original system prompt.
	Use the loaded orchestrator-cli skill for coordination commands and workflow.
	Continue only the pending work assigned to your domain.`, agentID, domainID)
}

func (a *Allocator) handleAgentLifecycle(agentID, domainID string, event supervisor.AgentLifecycleEvent) {
	if a.lifecycleFn != nil {
		a.lifecycleFn(agentID, domainID, event)
	}
	if a.agentStateFn != nil {
		state, reason := mapAgentLifecycleState(event)
		if state != "" {
			a.agentStateFn(agentID, domainID, state, reason)
		}
	}
	if event.Kind == "agent_end" {
		node, ok := a.inProgressNodeForAgent(agentID)
		if ok {
			a.mu.Lock()
			if a.pendingIncomplete[agentID] == node.ID {
				a.mu.Unlock()
				return
			}
			a.pendingIncomplete[agentID] = node.ID
			a.mu.Unlock()
			go a.stopAgentWithIncompleteTask(agentID, domainID, node.ID)
			return
		}

		a.mu.Lock()
		workItemID := a.dispatchedWork[agentID]
		if workItemID == "" || a.pendingIncomplete[agentID] == workItemID {
			a.mu.Unlock()
			return
		}
		a.pendingIncomplete[agentID] = workItemID
		a.mu.Unlock()
		go a.stopAgentWithIncompleteWork(agentID, domainID, workItemID)
	}
}

func (a *Allocator) stopAgentWithIncompleteTask(agentID, domainID, nodeID string) {
	defer func() {
		a.mu.Lock()
		if a.pendingIncomplete[agentID] == nodeID {
			delete(a.pendingIncomplete, agentID)
		}
		a.mu.Unlock()
	}()
	// Allow an update-node RPC emitted at the end of the turn to commit before
	// deciding whether the task was abandoned.
	time.Sleep(250 * time.Millisecond)
	node, err := a.dagManager.GetNode(nodeID)
	if err != nil || node.Status != dag.StatusInProgress || node.AssignedAgent != agentID {
		return
	}
	a.mu.Lock()
	agent, live := a.activeAgents[agentID]
	a.mu.Unlock()
	if !live || agent.State() != supervisor.StateRunning {
		return
	}
	if agent.TurnRetryPending() {
		log.Printf("allocator: deferring incomplete work handling for %s while Pi retries inference", agentID)
		return
	}
	reason := fmt.Sprintf("agent loop ended before node %s reported a terminal status", nodeID)
	var checkpoint *WorkCheckpoint
	if a.workInbox != nil {
		if inbox, inboxErr := a.workInbox.Inbox(context.Background(), a.workPrincipal(agentID)); inboxErr == nil {
			if item, ok := findInboxItem(inbox, nodeID); ok {
				checkpoint = item.Checkpoint
			}
		}
	}
	if a.retryIncompleteTurn(agent, agentID, domainID, nodeID, checkpoint, reason) {
		return
	}
	log.Printf("allocator: stopping incomplete agent %s: %s", agentID, reason)
	a.emitStatus(fmt.Sprintf("Agent %s stopped with incomplete node %s; use /continue-agents to resume", agentID, nodeID), "warn")
	if a.lifecycleFn != nil {
		a.lifecycleFn(agentID, domainID, supervisor.AgentLifecycleEvent{
			Kind:    "task_incomplete",
			IsError: true,
			Error:   reason,
		})
	}
	if err := agent.Stop(); err != nil {
		log.Printf("allocator: stop incomplete agent %s: %v", agentID, err)
	}
}

func (a *Allocator) stopAgentWithIncompleteWork(agentID, domainID, workItemID string) {
	defer func() {
		a.mu.Lock()
		if a.pendingIncomplete[agentID] == workItemID {
			delete(a.pendingIncomplete, agentID)
		}
		a.mu.Unlock()
	}()
	// Coordination effects and terminal commands are persisted asynchronously
	// relative to Pi's agent_end event. Let those commits settle first.
	time.Sleep(250 * time.Millisecond)

	a.mu.Lock()
	service := a.workInbox
	agent, live := a.activeAgents[agentID]
	a.mu.Unlock()
	if service == nil || !live || agent.State() != supervisor.StateRunning {
		return
	}
	if agent.TurnRetryPending() {
		log.Printf("allocator: deferring incomplete work handling for %s while Pi retries inference", agentID)
		return
	}
	inbox, err := service.Inbox(context.Background(), a.workPrincipal(agentID))
	if err != nil {
		log.Printf("allocator: inspect unfinished work %s for %s: %v", workItemID, agentID, err)
		return
	}
	item, incomplete := actionableInboxItem(inbox, workItemID)
	if !incomplete {
		a.clearDispatchedWork(agentID, workItemID)
		return
	}

	reason := fmt.Sprintf("agent loop ended before work item %s reached a terminal or waiting state", workItemID)
	if a.retryIncompleteTurn(agent, agentID, domainID, workItemID, item.Checkpoint, reason) {
		return
	}
	log.Printf("allocator: stopping incomplete agent %s: %s", agentID, reason)
	a.emitStatus(fmt.Sprintf("Agent %s stopped with incomplete %s; use /continue-agents to resume", agentID, item.Kind), "warn")
	if a.lifecycleFn != nil {
		a.lifecycleFn(agentID, domainID, supervisor.AgentLifecycleEvent{
			Kind:    "task_incomplete",
			IsError: true,
			Error:   reason,
		})
	}
	if err := agent.Stop(); err != nil {
		log.Printf("allocator: stop incomplete agent %s: %v", agentID, err)
	}
}

func (a *Allocator) retryIncompleteTurn(agent *supervisor.AgentSupervisor, agentID, domainID, workItemID string, checkpoint *WorkCheckpoint, reason string) bool {
	if agent == nil || checkpoint != nil || a.config == nil || a.config.Agents.IncompleteTurnRetries <= 0 {
		return false
	}
	key := agentID + "\x00" + workItemID
	a.mu.Lock()
	if a.incompleteRetries == nil {
		a.incompleteRetries = make(map[string]int)
	}
	attempt := a.incompleteRetries[key] + 1
	if attempt > a.config.Agents.IncompleteTurnRetries {
		a.mu.Unlock()
		return false
	}
	a.incompleteRetries[key] = attempt
	a.mu.Unlock()

	prompt := fmt.Sprintf("AUTOMATIC INCOMPLETE-TURN RETRY %d/%d\nThe prior Pi loop ended without a terminal update or durable checkpoint. Do not restart or re-plan the task. Inspect the authoritative work inbox, validate work already written, correct failures, then checkpoint or report a terminal node/contract state.\n\n%s",
		attempt, a.config.Agents.IncompleteTurnRetries, a.resumePromptForAgent(agentID, domainID, reason))
	if err := agent.DeliverTurn(prompt); err != nil {
		log.Printf("allocator: incomplete-turn retry delivery failed for %s: %v", agentID, err)
		return false
	}
	log.Printf("allocator: retrying incomplete work item %s for %s (%d/%d)", workItemID, agentID, attempt, a.config.Agents.IncompleteTurnRetries)
	a.emitStatus(fmt.Sprintf("Retrying incomplete work for %s (%d/%d)", agentID, attempt, a.config.Agents.IncompleteTurnRetries), "warn")
	if a.lifecycleFn != nil {
		a.lifecycleFn(agentID, domainID, supervisor.AgentLifecycleEvent{Kind: "task_retry", Error: reason})
	}
	return true
}

func actionableInboxItem(inbox *WorkInbox, workItemID string) (WorkItem, bool) {
	item, ok := findInboxItem(inbox, workItemID)
	if !ok {
		return WorkItem{}, false
	}
	return item, item.State == WorkItemReady || item.State == WorkItemActive
}

func (a *Allocator) inProgressNodeForAgent(agentID string) (dag.DagNode, bool) {
	if a.dagManager == nil {
		return dag.DagNode{}, false
	}
	snapshot := a.dagManager.Snapshot()
	for _, node := range snapshot.Nodes {
		if node.Status == dag.StatusInProgress && node.AssignedAgent == agentID {
			return node, true
		}
	}
	return dag.DagNode{}, false
}

func hasReusablePiSession(agentDir string) bool {
	entries, err := os.ReadDir(agentDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.EqualFold(filepath.Ext(entry.Name()), ".jsonl") {
			return true
		}
	}
	return false
}

func mapAgentLifecycleState(event supervisor.AgentLifecycleEvent) (string, string) {
	switch event.Kind {
	case "agent_started":
		return "Running", ""
	case "agent_stopped":
		return "Stopped", event.Error
	case "agent_crashed", "agent_stream_closed", "agent_error", "provider_auth_error":
		return "Crashed", event.Error
	default:
		return "", ""
	}
}

type AgentInfo struct {
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

func (a *Allocator) AgentInfos() []AgentInfo {
	a.mu.Lock()
	defer a.mu.Unlock()

	infos := make([]AgentInfo, 0, len(a.activeAgents))
	for agentID, agent := range a.activeAgents {
		runtime := agent.RuntimeSnapshot()
		infos = append(infos, AgentInfo{
			AgentID:             agentID,
			DomainID:            agent.DomainID(),
			State:               runtime.State,
			PID:                 runtime.PID,
			WorkspaceGeneration: runtime.WorkspaceGeneration,
			Workspace:           runtime.Workspace,
			InTurn:              runtime.InTurn,
			ToolActive:          runtime.ToolActive,
			LastHeartbeat:       runtime.LastHeartbeat,
			LastProgress:        runtime.LastProgress,
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].AgentID < infos[j].AgentID
	})
	return infos
}

// StopAll terminates all managed agents.
func (a *Allocator) StopAll() {
	a.mu.Lock()
	agents := a.activeAgents
	a.activeAgents = make(map[string]*supervisor.AgentSupervisor)
	a.pendingIncomplete = make(map[string]string)
	a.incompleteRetries = make(map[string]int)
	a.mu.Unlock()

	log.Printf("allocator: stopping all agents...")
	var wg sync.WaitGroup
	for agentID, agent := range agents {
		wg.Add(1)
		go func(agentID string, agent *supervisor.AgentSupervisor) {
			defer wg.Done()
			agent.Stop()
			a.hubRouter.UnregisterAgent(agentID)
		}(agentID, agent)
	}
	wg.Wait()
}

// ReviveAgent manually restarts a crashed or stalled agent.
func (a *Allocator) ReviveAgent(ctx context.Context, agentID string) error {
	a.mu.Lock()
	agent, ok := a.activeAgents[agentID]
	a.mu.Unlock()

	if !ok {
		return fmt.Errorf("allocator: agent %s not found", agentID)
	}

	log.Printf("allocator: admin reviving agent %s", agentID)
	prompt := a.recoveryPromptForAgent(agentID, agent.DomainID())
	a.mu.Lock()
	delete(a.pendingIncomplete, agentID)
	a.mu.Unlock()
	if err := agent.RestartWithPrompt(ctx, prompt); err != nil {
		return fmt.Errorf("allocator: start revived agent: %w", err)
	}
	return nil
}

// ResetAgentSession preserves prior Pi transcripts but starts a new
// conversation with the current authoritative work item. Use this when a
// persisted model context is looping or otherwise no longer recoverable by a
// normal resumed prompt.
func (a *Allocator) ResetAgentSession(ctx context.Context, agentID string) error {
	return a.ResetAgentSessionWithPrompt(ctx, agentID, "")
}

// ResetAgentSessionWithPrompt starts a clean conversation with an optional
// operator correction followed by the authoritative current work item.
func (a *Allocator) ResetAgentSessionWithPrompt(ctx context.Context, agentID, operatorPrompt string) error {
	a.mu.Lock()
	agent, ok := a.activeAgents[agentID]
	a.mu.Unlock()
	if !ok {
		return fmt.Errorf("allocator: agent %s not found; run /continue-agents first", agentID)
	}
	prompt := a.resumePromptForAgent(agentID, agent.DomainID(), "")
	if operatorPrompt = strings.TrimSpace(operatorPrompt); operatorPrompt != "" {
		prompt += "\n\nAUTHORITATIVE OPERATOR CORRECTION:\n" + operatorPrompt +
			"\nApply this correction before any other action. It overrides conflicting assumptions in prior session history, while node ownership and isolation policy remain unchanged."
	}
	a.mu.Lock()
	delete(a.pendingIncomplete, agentID)
	delete(a.incompleteRetries, agentID)
	a.mu.Unlock()
	log.Printf("allocator: starting fresh Pi session for agent %s", agentID)
	if err := agent.RestartFreshWithPrompt(ctx, prompt); err != nil {
		return fmt.Errorf("allocator: reset session for %s: %w", agentID, err)
	}
	a.emitStatus(fmt.Sprintf("Agent %s started a fresh recovery session; prior transcripts were preserved", agentID), "info")
	return nil
}

func (a *Allocator) recoveryPromptForAgent(agentID, domainID string) string {
	return a.resumePromptForAgent(agentID, domainID, "")
}

func (a *Allocator) resumeLaunchPrompt(agentID, domainID, base string) string {
	prompt, _ := a.resumeLaunchDispatch(agentID, domainID, base)
	return prompt
}

func (a *Allocator) resumeLaunchDispatch(agentID, domainID, base string) (string, string) {
	if item, ok := a.actionableContractWorkForAgent(context.Background(), agentID); ok {
		return a.resumePromptForWorkItem(agentID, domainID, base, item), item.ID
	}
	if _, ok := a.inProgressNodeForAgent(agentID); !ok {
		return "", ""
	}
	return a.resumePromptForAgent(agentID, domainID, base), ""
}

func (a *Allocator) resumePromptForAgent(agentID, domainID, base string) string {
	if item, ok := a.actionableContractWorkForAgent(context.Background(), agentID); ok {
		return a.resumePromptForWorkItem(agentID, domainID, base, item)
	}
	var b strings.Builder
	base = strings.TrimSpace(base)
	if base == "" {
		base = defaultDomainAgentResumeMessage(agentID, domainID)
	}
	b.WriteString(base)
	if node, ok := a.inProgressNodeForAgent(agentID); ok {
		b.WriteString("\n\nThe previous agent loop ended before reporting completion. Resume this assigned node now:\n")
		if item, found := a.currentNodeWorkForAgent(context.Background(), agentID); found {
			b.WriteString(formatWorkItemPrompt(item))
			a.appendNodeRecoveryContext(&b, node.ID)
			return b.String()
		}
		b.WriteString(fmt.Sprintf("Node: %s\nSpec: %s\n", node.ID, node.TaskSpec))
		if len(node.TargetFiles) > 0 {
			b.WriteString("Target files: ")
			b.WriteString(strings.Join(node.TargetFiles, ", "))
			b.WriteString("\n")
		}
		b.WriteString("Workspace root is /workspace. Resolve every target from /workspace and do not duplicate its first path component after changing directories.\n")
		b.WriteString("Persist every new implementation, fixture, and test beneath an owned target path. Do not create /workspace/tests or another shared root unless it is explicitly listed in your assigned targets; place focused tests under the owned domain path instead.\n")
		b.WriteString("Run `orchestrator-cli work-inbox --current` before acting and confirm this node is the authoritative active item. Work only this assigned node. Do not claim or update another pending or unassigned node. If you need a cross-domain artifact, use `contract-ensure --wait` and end the turn instead of polling. Record a `work-checkpoint` before yielding incomplete work. Finish or explicitly fail/block this node through the loaded orchestrator-cli workflow, then end the agent loop so the allocator can dispatch later work.")
		a.appendNodeRecoveryContext(&b, node.ID)
	} else {
		b.WriteString("\n\nYou do not currently have an assigned InProgress node. Do not claim, start, or update a pending or unassigned node. End this agent loop now and wait for the allocator to send a direct task dispatch.")
	}
	return b.String()
}

func (a *Allocator) appendNodeRecoveryContext(b *strings.Builder, nodeID string) {
	recovery := a.nodeRecoveryContextFor(nodeID)
	if recovery == "" {
		return
	}
	b.WriteString("\n\nAUTHORITATIVE RECOVERY CONTEXT:\n")
	b.WriteString(recovery)
	b.WriteString("\nTreat this diagnostic as the acceptance failure for the current node. Repair only files in your owned workspace, rerun the relevant checks, and report the node terminal only after the failure is resolved.")
}

func (a *Allocator) SetNodeRecoveryContext(nodeID, recovery string) {
	nodeID = strings.TrimSpace(nodeID)
	recovery = strings.TrimSpace(recovery)
	if nodeID == "" {
		return
	}
	a.nodeRecoveryMu.Lock()
	defer a.nodeRecoveryMu.Unlock()
	if recovery == "" {
		delete(a.nodeRecoveryContext, nodeID)
		return
	}
	if a.nodeRecoveryContext == nil {
		a.nodeRecoveryContext = make(map[string]string)
	}
	a.nodeRecoveryContext[nodeID] = recovery
}

func (a *Allocator) ClearNodeRecoveryContext(nodeID string) {
	a.SetNodeRecoveryContext(nodeID, "")
}

func (a *Allocator) nodeRecoveryContextFor(nodeID string) string {
	a.nodeRecoveryMu.Lock()
	defer a.nodeRecoveryMu.Unlock()
	return a.nodeRecoveryContext[strings.TrimSpace(nodeID)]
}

// DeliverNodeRecovery records a node-scoped diagnostic and delivers it to the
// assigned agent when that runtime is already available. A missing runtime is
// not an error: AllocateWithOptions reconstructs the prompt from this context
// after an explicit /continue-agents.
func (a *Allocator) DeliverNodeRecovery(ctx context.Context, nodeID, recovery string) (bool, error) {
	if a == nil || a.dagManager == nil {
		return false, fmt.Errorf("allocator: DAG manager is unavailable")
	}
	node, err := a.dagManager.GetNode(strings.TrimSpace(nodeID))
	if err != nil {
		return false, err
	}
	agentID := expectedAgentForNode(node.DomainID, node.AssignedAgent)
	if strings.TrimSpace(agentID) == "" {
		return false, fmt.Errorf("allocator: node %s has no recoverable agent", node.ID)
	}
	a.SetNodeRecoveryContext(node.ID, recovery)
	prompt := a.resumePromptForAgent(agentID, node.DomainID, "")

	a.mu.Lock()
	agent := a.activeAgents[agentID]
	a.mu.Unlock()
	if agent == nil {
		return false, nil
	}
	if agent.State() == supervisor.StateRunning {
		if err := agent.DeliverTurn(prompt); err != nil {
			return false, fmt.Errorf("allocator: deliver node recovery to %s: %w", agentID, err)
		}
		return true, nil
	}
	if err := agent.RestartWithPrompt(ctx, prompt); err != nil {
		return false, fmt.Errorf("allocator: restart %s for node recovery: %w", agentID, err)
	}
	return true, nil
}

func (a *Allocator) resumePromptForWorkItem(agentID, domainID, base string, item WorkItem) string {
	base = strings.TrimSpace(base)
	if base == "" {
		base = defaultDomainAgentResumeMessage(agentID, domainID)
	}
	return base +
		"\n\nThe previous agent loop ended before completing durable coordination work. Resume the authoritative contract item below now:\n" +
		formatWorkItemPrompt(item)
}

func (a *Allocator) currentNodeWorkForAgent(ctx context.Context, agentID string) (WorkItem, bool) {
	service := a.workInbox
	if service == nil {
		return WorkItem{}, false
	}
	inbox, err := service.Inbox(ctx, a.workPrincipal(agentID))
	if err != nil {
		log.Printf("allocator: build node recovery inbox for %s: %v", agentID, err)
		return WorkItem{}, false
	}
	if inbox.Active != nil && inbox.Active.Kind == WorkItemNode {
		return *inbox.Active, true
	}
	for _, item := range inbox.Ready {
		if item.Kind == WorkItemNode {
			return item, true
		}
	}
	return WorkItem{}, false
}

func (a *Allocator) actionableContractWorkForAgent(ctx context.Context, agentID string) (WorkItem, bool) {
	// The inbox service is installed before allocation starts and remains stable
	// for the run. This helper is also called while AllocateWithOptions holds the
	// allocator mutex, so it must not attempt to reacquire that lock.
	service := a.workInbox
	if service == nil {
		return WorkItem{}, false
	}
	inbox, err := service.Inbox(ctx, a.workPrincipal(agentID))
	if err != nil {
		log.Printf("allocator: build recovery inbox for %s: %v", agentID, err)
		return WorkItem{}, false
	}
	if inbox.Active != nil && inbox.Active.Kind != WorkItemNode {
		return *inbox.Active, true
	}
	for _, item := range inbox.Ready {
		if item.Kind != WorkItemNode {
			return item, true
		}
	}
	return WorkItem{}, false
}

// MonitorExecution blocks until the DAG is fully converged or a timeout occurs.
// Automatically shuts down the daemon upon convergence.
func (a *Allocator) MonitorExecution(ctx context.Context, checkInterval time.Duration) error {
	log.Printf("allocator: starting execution monitor...")
	a.emitStatus("Execution monitor started — polling DAG for ready tasks", "info")
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.dispatchReadyTasks(ctx)
			converged, err := a.checkConvergenceContext(ctx)
			if err != nil {
				log.Printf("allocator: monitor error: %v", err)
				a.emitStatus(fmt.Sprintf("Monitor error: %v", err), "error")
				continue
			}
			if converged {
				log.Printf("allocator: execution converged (DAG, coordination, and integration terminal)")
				a.emitStatus("System converged — implementation and integration validation complete", "ok")
				return nil
			}
		}
	}
}

func (a *Allocator) checkConvergence() (bool, error) {
	return a.checkConvergenceContext(context.Background())
}

func (a *Allocator) checkConvergenceContext(ctx context.Context) (bool, error) {
	snap := a.dagManager.Snapshot()

	if len(snap.Nodes) == 0 {
		return false, nil // Empty DAG is not considered converged if we just started
	}

	for _, node := range snap.Nodes {
		if !node.Status.IsTerminal() {
			return false, nil // Found a non-terminal node
		}
	}
	if a.workInbox != nil {
		open, err := a.workInbox.HasOpenCoordination(context.Background())
		if err != nil {
			return false, err
		}
		if open {
			return false, nil
		}
	}
	a.mu.Lock()
	integrationGate := a.integrationGateFn
	a.mu.Unlock()
	if integrationGate != nil {
		return integrationGate(ctx, snap)
	}

	return true, nil
}

// dispatchReadyTasks queries the DAG for ready nodes and dispatches them to idle agents.
func (a *Allocator) dispatchReadyTasks(ctx context.Context) {
	snap := a.dagManager.Snapshot()
	readyNodes := a.dagManager.GetReadyNodes()
	if len(readyNodes) > 0 {
		ids := make([]string, 0, len(readyNodes))
		for _, node := range readyNodes {
			ids = append(ids, node.ID)
		}
		log.Printf("allocator: ready task scan found %d node(s): %s", len(ids), strings.Join(ids, ","))
	}
	a.dispatchReadyContractWork(ctx, snap)

	// Track which agents are currently busy
	agentBusy := make(map[string]bool)
	for _, n := range snap.Nodes {
		if n.Status == dag.StatusInProgress {
			if n.AssignedAgent != "" {
				agentBusy[n.AssignedAgent] = true
			} else {
				// Fallback if not assigned
				agentBusy[fmt.Sprintf("agent-%s", n.DomainID)] = true
			}
		}
	}

	// Sort ready nodes by priority ascending (lower value = higher priority)
	sort.SliceStable(readyNodes, func(i, j int) bool {
		return readyNodes[i].Priority < readyNodes[j].Priority
	})

	for _, node := range readyNodes {
		agentID := fmt.Sprintf("agent-%s", node.DomainID)
		if a.hasReadyContractWork(ctx, agentID) {
			continue
		}

		a.mu.Lock()
		agent, exists := a.activeAgents[agentID]
		a.mu.Unlock()
		if !exists || agent.State() != supervisor.StateRunning || agentBusy[agentID] {
			continue // agent offline or busy
		}

		// A busy agent must not block this global scan. Retry its work on the
		// next monitor tick while allowing unrelated domains to dispatch now.
		if !agent.TurnIdle() {
			log.Printf("allocator: deferring node %s because agent %s is still active", node.ID, agentID)
			continue
		}

		log.Printf("allocator: dispatching node %s to idle agent %s", node.ID, agentID)

		if err := a.dagManager.AssignNode(node.ID, agentID); err != nil {
			log.Printf("allocator: failed to assign node %s to %s: %v", node.ID, agentID, err)
			continue
		}
		if err := a.dagManager.UpdateNode(node.ID, dag.StatusInProgress); err != nil {
			log.Printf("allocator: failed to update node %s to InProgress: %v", node.ID, err)
			continue
		}

		agentBusy[agentID] = true

		var b strings.Builder
		b.WriteString(fmt.Sprintf("TASK DISPATCH (Node %s):\n", node.ID))
		b.WriteString(fmt.Sprintf("Spec: %s\n", node.TaskSpec))
		if len(node.TargetFiles) > 0 {
			b.WriteString("Target Files: ")
			for _, f := range node.TargetFiles {
				b.WriteString(fmt.Sprintf("%s ", f))
			}
			b.WriteString("\n")
		}
		b.WriteString("\nExecute this task, then mark it Done using:\n")
		b.WriteString("the update flow defined in your loaded skills.\n")
		b.WriteString("Run `orchestrator-cli work-inbox --current` first and confirm this node is the authoritative active item. Work only this dispatched node. Do not claim or update another pending or unassigned node. If you discover a missing cross-domain resource, use `contract-ensure --wait` and end the turn instead of polling or guessing. Record a concise `work-checkpoint` before yielding incomplete work. After the terminal update, end the agent loop so the allocator can dispatch later work.\n")
		b.WriteString("\nWorkspace root is /workspace. Resolve every target file from /workspace and write each exact target path once; do not duplicate its first path component after changing directories.\n")
		b.WriteString("Persist every new implementation, fixture, and test beneath an owned target path. Do not create /workspace/tests or another shared root unless it is explicitly listed in this node's targets; place focused tests under the owned domain path instead.\n")
		for _, target := range node.TargetFiles {
			if isIntegrationDirectiveTarget(target) {
				b.WriteString(fmt.Sprintf("Create the planned schema-v1 integration directive at %s. This exact consumer-owned target will be executed after implementation and contract work is terminal.\n", target))
			}
		}
		b.WriteString("Your current working directory contains only your isolated writable domain mounts. Use contracts and orchestrator messages for cross-domain requirements.\n")

		if err := agent.SendPrompt(b.String()); err != nil {
			log.Printf("allocator: failed to deliver task to agent %s: %v", agentID, err)
			a.emitStatus(fmt.Sprintf("⚠ Dispatch failed for agent %s: %v", agentID, err), "warn")
		} else {
			a.emitStatus(fmt.Sprintf("Dispatched node %s → %s", node.ID, agentID), "info")
		}
	}
}

func (a *Allocator) dispatchReadyContractWork(ctx context.Context, snapshot *dag.DagData) {
	if a.workInbox == nil || snapshot == nil {
		return
	}
	busy := make(map[string]bool)
	for _, node := range snapshot.Nodes {
		if node.Status == dag.StatusInProgress {
			busy[expectedAgentForNode(node.DomainID, node.AssignedAgent)] = true
		}
	}
	a.mu.Lock()
	agents := make(map[string]*supervisor.AgentSupervisor, len(a.activeAgents))
	for agentID, agent := range a.activeAgents {
		agents[agentID] = agent
	}
	a.mu.Unlock()
	agentIDs := make([]string, 0, len(agents))
	for agentID := range agents {
		agentIDs = append(agentIDs, agentID)
	}
	sort.Strings(agentIDs)
	for _, agentID := range agentIDs {
		if busy[agentID] {
			continue
		}
		item, ok := a.readyContractWork(ctx, agentID)
		if !ok {
			a.clearDispatchedWork(agentID, "")
			continue
		}
		a.mu.Lock()
		alreadyDispatched := a.dispatchedWork[agentID] == item.ID
		a.mu.Unlock()
		if alreadyDispatched {
			continue
		}
		agent := agents[agentID]
		prompt := formatWorkItemPrompt(item)
		var err error
		if agent.State() == supervisor.StateRunning {
			if !agent.TurnIdle() {
				log.Printf("allocator: deferring work item %s because agent %s is still active", item.ID, agentID)
				continue
			}
			err = agent.SendPrompt(prompt)
		} else {
			err = agent.RestartWithPrompt(ctx, prompt)
		}
		if err != nil {
			log.Printf("allocator: failed to dispatch work item %s to %s: %v", item.ID, agentID, err)
			a.emitStatus(fmt.Sprintf("Work dispatch failed for %s: %v", agentID, err), "warn")
			continue
		}
		a.MarkWorkDispatched(agentID, item.ID)
		a.emitStatus(fmt.Sprintf("Dispatched %s to %s", item.ID, agentID), "info")
	}
}

func (a *Allocator) hasReadyContractWork(ctx context.Context, agentID string) bool {
	_, ok := a.readyContractWork(ctx, agentID)
	return ok
}

func (a *Allocator) readyContractWork(ctx context.Context, agentID string) (WorkItem, bool) {
	if a.workInbox == nil {
		return WorkItem{}, false
	}
	inbox, err := a.workInbox.Inbox(ctx, a.workPrincipal(agentID))
	if err != nil {
		log.Printf("allocator: build work inbox for %s: %v", agentID, err)
		return WorkItem{}, false
	}
	for _, item := range inbox.Ready {
		if item.Kind != WorkItemNode {
			return item, true
		}
	}
	return WorkItem{}, false
}

func (a *Allocator) workPrincipal(agentID string) coordination.Principal {
	principal := coordination.Principal{Role: coordination.RoleAgent, AgentID: agentID}
	if a.dagManager == nil {
		return principal
	}
	for _, node := range a.dagManager.Snapshot().Nodes {
		if expectedAgentForNode(node.DomainID, node.AssignedAgent) == agentID {
			principal.NodeIDs = append(principal.NodeIDs, node.ID)
		}
	}
	return principal
}

func (a *Allocator) MarkWorkDispatched(agentID, workItemID string) {
	a.mu.Lock()
	if a.dispatchedWork == nil {
		a.dispatchedWork = make(map[string]string)
	}
	a.dispatchedWork[agentID] = workItemID
	a.mu.Unlock()
}

// ManagesAgent reports whether the allocator owns the lifecycle of agentID.
// Coordinator and architect processes are supervised through separate paths.
func (a *Allocator) ManagesAgent(agentID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.activeAgents[strings.TrimSpace(agentID)]
	return ok
}

// DeliverOrWakeContext routes context to a live agent or reopens its persisted
// Pi session when durable coordination work arrives while it is stopped.
func (a *Allocator) DeliverOrWakeContext(ctx context.Context, message hub.Message, workItemID string) error {
	a.mu.Lock()
	agent := a.activeAgents[message.ToAgent]
	a.mu.Unlock()
	if agent == nil {
		return fmt.Errorf("allocator: agent %s not found", message.ToAgent)
	}
	prompt := formatContextWakePrompt(message)
	if workItemID != "" && a.workInbox != nil {
		inbox, err := a.workInbox.Inbox(ctx, a.workPrincipal(message.ToAgent))
		if err == nil {
			if item, ok := findInboxItem(inbox, workItemID); ok {
				prompt = formatWorkItemPrompt(item) + "\n\n" + prompt
			}
		}
	}
	if agent.State() == supervisor.StateRunning {
		if err := agent.DeliverTurn(prompt); err != nil {
			return err
		}
	} else {
		if err := agent.RestartWithPrompt(ctx, prompt); err != nil {
			return err
		}
	}
	if workItemID != "" {
		a.MarkWorkDispatched(message.ToAgent, workItemID)
	}
	return nil
}

// SteerOrWakeContext gives operator interventions precedence over an active
// agent loop while still using a direct prompt for an idle Pi session.
func (a *Allocator) SteerOrWakeContext(ctx context.Context, message hub.Message) error {
	a.mu.Lock()
	agent := a.activeAgents[message.ToAgent]
	a.mu.Unlock()
	if agent == nil {
		return fmt.Errorf("allocator: agent %s not found", message.ToAgent)
	}
	prompt := formatContextWakePrompt(message)
	if agent.State() == supervisor.StateRunning {
		return agent.SendSteer(prompt)
	}
	return agent.RestartWithPrompt(ctx, prompt)
}

func (a *Allocator) clearDispatchedWork(agentID, completedWorkItemID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if completedWorkItemID == "" || a.dispatchedWork[agentID] == completedWorkItemID {
		delete(a.dispatchedWork, agentID)
	}
}

func formatWorkItemPrompt(item WorkItem) string {
	var b strings.Builder
	b.WriteString("WORK ITEM DISPATCH\n")
	b.WriteString(fmt.Sprintf("Work item: %s\nKind: %s\n", item.ID, item.Kind))
	if item.NodeID != "" {
		b.WriteString("Related node: " + item.NodeID + "\n")
	}
	if item.ContractID != "" {
		b.WriteString("Contract: " + item.ContractID + "\n")
		b.WriteString(fmt.Sprintf("Current revision: %d\n", item.Revision))
	}
	if item.Objective != "" {
		b.WriteString("Objective: " + item.Objective + "\n")
	}
	if item.Contract != nil {
		if language := strings.TrimSpace(item.Contract.Language); language != "" {
			b.WriteString("Contract language: " + language + "\n")
		}
		if entrypoint := strings.TrimSpace(item.Contract.PublicEntrypoint); entrypoint != "" {
			b.WriteString("Required public entrypoint: " + entrypoint + "\n")
		}
	}
	if item.Checkpoint != nil {
		b.WriteString("Last checkpoint: " + item.Checkpoint.Summary + "\n")
		if item.Checkpoint.Next != "" {
			b.WriteString("Checkpoint next action: " + item.Checkpoint.Next + "\n")
		}
	}
	for _, dependency := range item.RequiredContracts {
		b.WriteString(fmt.Sprintf("Required contract: %s status=%s revision=%d resource=%s public_entrypoint=%s\n",
			dependency.ID,
			dependency.Status,
			dependency.Revision,
			dependency.Details.Name,
			dependency.Details.PublicEntrypoint,
		))
	}
	for _, requirement := range item.RuntimeRequirements {
		b.WriteString("Authoritative runtime requirement: " + requirement + "\n")
	}
	b.WriteString("\nRun `orchestrator-cli work-inbox --current` first and confirm this is the current dispatched item. Process only this item. Do not poll or inspect another domain's workspace.\n")
	if item.Kind == WorkItemNode {
		b.WriteString("For DAG work, pass the exact `node_id` field to `update-node`; the inbox item ID is also that raw DAG node ID.\n")
	} else {
		b.WriteString("For contract work, pass the exact `contract_id` field to contract lifecycle commands and the inbox `id` field to `work-checkpoint`.\n")
	}
	switch item.Kind {
	case WorkItemContractPublish, WorkItemContractCorrection:
		b.WriteString(fmt.Sprintf("Create the requested public contract artifact as a concise JSON file under /tmp or an owned domain path. It must repeat the declared language and exact public entrypoint, define the callable/schema surface needed by the consumer, and include enough usage detail for an isolated consumer to integrate without inspecting your workspace. Publish it with `orchestrator-cli contract-publish --contract-id %s --artifact-file <path> --expected-revision %d`. Do not expose private implementation source.\n", item.ContractID, item.Revision))
	case WorkItemContractValidate:
		artifactPath := contractValidationArtifactPath(item.ContractID, item.Revision)
		b.WriteString(fmt.Sprintf("Fetch the immutable public descriptor with `orchestrator-cli contract-fetch --contract-id %s --revision %d --output %s`. The `--output` value is a regular file path, not a directory. Read that descriptor and confirm that its declared language, exact public entrypoint, signatures/schema, and usage details are sufficient for isolated implementation. Do not expect producer source or execute producer behavior in this workspace; behavioral acceptance runs later in the assembled integration workspace. Never guess a missing integration locator. If the descriptor is actionable and consistent with the stated acceptance criteria, run `orchestrator-cli contract-validate --contract-id %s --revision %d --evidence '<json>'`; otherwise request a concrete correction.\n", item.ContractID, item.Revision, artifactPath, item.ContractID, item.Revision))
	}
	b.WriteString("If you must yield before completing it, record a concise `work-checkpoint`. End the turn when the item is completed or waiting so the kernel can schedule the next ready item.")
	return b.String()
}

func contractValidationArtifactPath(contractID string, revision int64) string {
	contractID = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, strings.TrimSpace(contractID))
	contractID = strings.Trim(contractID, "-")
	if contractID == "" {
		contractID = "artifact"
	}
	return fmt.Sprintf("/tmp/aion-contract-%s-r%d.json", contractID, revision)
}

func formatContextWakePrompt(message hub.Message) string {
	data, _ := json.Marshal(message)
	return fmt.Sprintf("[Context Hub - %s] %s\n\nRun `orchestrator-cli work-inbox --current` to confirm the authoritative ready work before acting.", message.Type, data)
}

func findInboxItem(inbox *WorkInbox, workItemID string) (WorkItem, bool) {
	if inbox == nil {
		return WorkItem{}, false
	}
	if inbox.Active != nil && inbox.Active.ID == workItemID {
		return *inbox.Active, true
	}
	for _, items := range [][]WorkItem{inbox.Ready, inbox.Waiting, inbox.RecentlyCompleted} {
		for _, item := range items {
			if item.ID == workItemID {
				return item, true
			}
		}
	}
	return WorkItem{}, false
}

// targetInferenceEnv forwards optional Pi target tuning values to sandboxed
// agents. Non-isolated agents inherit the daemon environment; isolated agents
// receive only the environment explicitly attached to their command.
func targetInferenceEnv() []string {
	names := []string{
		"AION_TARGET_MAX_TOKENS",
		"AION_TARGET_CONTEXT_WINDOW",
		"AION_TARGET_REASONING_EFFORT",
	}
	values := make([]string, 0, len(names))
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			values = append(values, name+"="+value)
		}
	}
	return values
}
