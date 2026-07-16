package orchestrator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/supervisor"
)

// Allocator manages the spinning up and termination of domain agents.
type Allocator struct {
	config        *Config
	projectRoot   string
	hubRouter     *hub.Router
	dagManager    *dag.Manager
	activeAgents  map[string]*supervisor.AgentSupervisor
	mu            sync.Mutex
	statusFn      func(text, level string) // optional: broadcasts to TUI status bar
	agentStatusFn func(agentID, text, level string)
	agentStateFn  func(agentID, domainID, state, reason string)
	lifecycleFn   func(agentID, domainID string, event supervisor.AgentLifecycleEvent)
	broadcastFn   func(hub.Message)
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
	return &Allocator{
		config:       config,
		projectRoot:  projectRoot,
		hubRouter:    hubRouter,
		dagManager:   dagManager,
		activeAgents: make(map[string]*supervisor.AgentSupervisor),
	}
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

	for _, domain := range domains {
		agentID := fmt.Sprintf("agent-%s", domain.DomainID)
		prompt, ok := prompts[domain.DomainID]
		if !ok {
			log.Printf("allocator: warning: no prompt generated for domain %s", domain.DomainID)
			continue
		}

		agentDir := filepath.Join(agentBaseDir, agentID)
		if err := os.MkdirAll(agentDir, 0755); err != nil {
			return fmt.Errorf("allocator: create agent dir %s: %w", agentID, err)
		}

		agentsMDPath := filepath.Join(agentDir, "AGENTS.md")
		if err := os.WriteFile(agentsMDPath, []byte(prompt), 0644); err != nil {
			return fmt.Errorf("allocator: write AGENTS.md for %s: %w", agentID, err)
		}
		agentWorkDir, err := a.prepareAgentWorkDir(agentID, domain, prompt)
		if err != nil {
			return err
		}

		// Resolve model profile
		infConfig := a.config.Inference.DomainAgents
		provider := infConfig.Provider
		model := infConfig.Model
		endpoint := infConfig.Endpoint
		envVars := []string{
			fmt.Sprintf("AION_ORCHESTRATOR_ADDR=%s", a.config.Orchestrator.ListenAddr),
			fmt.Sprintf("AION_AGENT_ID=%s", agentID),
			fmt.Sprintf("AION_DOMAIN_ID=%s", domain.DomainID),
			fmt.Sprintf("AION_AGENT_SESSION_DIR=%s", agentDir),
		}

		if infConfig.UseProfile != "" {
			if profile, ok := a.config.Inference.Models[infConfig.UseProfile]; ok {
				provider = profile.Provider
				model = profile.Model
				if profile.Endpoint != "" {
					endpoint = profile.Endpoint
				}
				for k, v := range profile.Env {
					envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
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
		if a.config.GatewayEnabled() {
			profileName := infConfig.UseProfile
			if profileName == "" {
				profileName = a.config.InferenceGateway.TargetProfile
			}
			targetProvider := provider
			targetModel := model
			gatewayURL := a.config.InferenceGateway.PublicBaseURL
			if gatewayURL == "" {
				gatewayURL = "http://" + a.config.InferenceGateway.ListenAddr
			}
			endpoint = gatewayURL
			provider = "aion-gateway"
			envVars = append(envVars,
				"AION_INFERENCE_GATEWAY_ENABLED=true",
				fmt.Sprintf("AION_INFERENCE_GATEWAY_URL=%s", gatewayURL),
				fmt.Sprintf("AION_INFERENCE_GATEWAY_KEY=%s", a.config.InferenceGateway.GatewayKey),
				fmt.Sprintf("AION_TARGET_PROVIDER=%s", targetProvider),
				fmt.Sprintf("AION_TARGET_PROFILE=%s", profileName),
				fmt.Sprintf("AION_TARGET_MODEL=%s", targetModel),
				fmt.Sprintf("AION_TARGET_API=%s", gatewayAPIForProvider(targetProvider)),
			)
		}

		initialPrompt := prompt
		resumeAgent := options.Mode == AllocationModeResume && hasReusablePiSession(agentDir)
		if resumeAgent {
			initialPrompt = ""
		}

		agentConfig := supervisor.AgentConfig{
			AgentID:       agentID,
			DomainID:      domain.DomainID,
			AssignedPaths: domain.AssignedPaths,
			InitialPrompt: initialPrompt,
			PiAgent: supervisor.PiAgentConfig{
				Binary:         a.config.Agents.CommandPath,
				SessionDir:     agentDir,
				ResumeSession:  resumeAgent,
				WorkingDir:     agentWorkDir,
				Provider:       provider,
				Model:          model,
				Endpoint:       endpoint,
				SkillPaths:     a.config.Agents.SkillPaths,
				ExtensionPaths: a.config.Agents.ExtensionPaths,
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
				if a.lifecycleFn != nil {
					a.lifecycleFn(agentID, domainID, event)
				}
				if a.agentStateFn == nil {
					return
				}
				state, reason := mapAgentLifecycleState(event)
				if state == "" {
					return
				}
				a.agentStateFn(agentID, domainID, state, reason)
			})
		}
		a.activeAgents[agentID] = agent

		// Register with Hub Router
		a.hubRouter.RegisterAgent(agentID, agent)

		if err := agent.Start(ctx); err != nil {
			return fmt.Errorf("allocator: spawn agent %s: %w", agentID, err)
		}
		if resumeAgent {
			resumeMessage := options.ResumeMessage
			if strings.TrimSpace(resumeMessage) == "" {
				resumeMessage = defaultDomainAgentResumeMessage(agentID, domain.DomainID)
			}
			if err := agent.SendPrompt(resumeMessage); err != nil {
				return fmt.Errorf("allocator: resume agent %s: %w", agentID, err)
			}
			a.emitStatus(fmt.Sprintf("Agent %s resumed from existing Pi session (domain: %s)", agentID, domain.DomainID), "info")
		}

		a.emitStatus(fmt.Sprintf("Agent %s spawned (domain: %s)", agentID, domain.DomainID), "info")
		log.Printf("allocator: spawned agent %s (domain: %s)", agentID, domain.DomainID)
	}

	a.emitStatus(fmt.Sprintf("%d agent(s) allocated — awaiting task dispatch", len(domains)), "ok")
	return nil
}

func defaultDomainAgentResumeMessage(agentID, domainID string) string {
	return fmt.Sprintf(`Resume your existing Aion domain-agent session.

Agent: %s
Domain: %s

Your current working directory is a filtered source workspace for your domain. Treat it as the project source view; do not cd outside it or inspect parent/runtime directories.
Continue from your persisted Pi session context. Do not restart from the original system prompt.
Use orchestrator-cli to inspect the DAG, acquire locks, update node status, create stubs, and coordinate with other agents.
Continue only the pending work assigned to your domain.`, agentID, domainID)
}

func hasReusablePiSession(agentDir string) bool {
	entries, err := os.ReadDir(agentDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "AGENTS.md" || name == "pi_raw.log" {
			continue
		}
		return true
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

func (a *Allocator) prepareAgentWorkDir(agentID string, domain coordinator.Domain, prompt string) (string, error) {
	rootHash := fmt.Sprintf("%x", sha256.Sum256([]byte(a.projectRoot)))[:12]
	workDir := filepath.Join(os.TempDir(), "aion-kernel-agent-workspaces", rootHash, agentID)
	workDirAbs, err := filepath.Abs(workDir)
	if err != nil {
		return "", fmt.Errorf("allocator: resolve filtered workdir for %s: %w", agentID, err)
	}
	if err := os.RemoveAll(workDir); err != nil {
		return "", fmt.Errorf("allocator: reset filtered workdir for %s: %w", agentID, err)
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", fmt.Errorf("allocator: create filtered workdir for %s: %w", agentID, err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "AGENTS.md"), []byte(prompt), 0o644); err != nil {
		return "", fmt.Errorf("allocator: write filtered AGENTS.md for %s: %w", agentID, err)
	}

	excludes := coordinator.LoadAgentExcludePaths(a.projectRoot)
	for _, assignedPath := range domain.AssignedPaths {
		if err := linkAssignedPath(a.projectRoot, workDirAbs, assignedPath, excludes); err != nil {
			return "", fmt.Errorf("allocator: prepare filtered workdir for %s: %w", agentID, err)
		}
	}
	return workDirAbs, nil
}

func linkAssignedPath(projectRoot, workDir, assignedPath string, excludes []string) error {
	rawPath := strings.Trim(strings.TrimSpace(assignedPath), "`\"'")
	if filepath.IsAbs(rawPath) {
		return fmt.Errorf("assigned path %q must be relative", assignedPath)
	}
	rel := normalizeWorkspaceRelPath(assignedPath)
	if rel == "" {
		return nil
	}
	if coordinator.IsAgentExcludedPath(rel, excludes) {
		return fmt.Errorf("assigned path %q is excluded", assignedPath)
	}
	projectRootAbs, err := filepath.Abs(projectRoot)
	if err != nil {
		return err
	}
	target := filepath.Join(projectRootAbs, rel)
	targetRel, err := filepath.Rel(projectRootAbs, target)
	if err != nil {
		return err
	}
	if targetRel == ".." || strings.HasPrefix(targetRel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("assigned path %q escapes project root", assignedPath)
	}

	linkPath := filepath.Join(workDir, rel)
	linkRel, err := filepath.Rel(workDir, linkPath)
	if err != nil {
		return err
	}
	if linkRel == ".." || strings.HasPrefix(linkRel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("assigned path %q escapes filtered workdir", assignedPath)
	}

	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Lstat(linkPath); err == nil {
		if err := os.Remove(linkPath); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	if _, err := os.Stat(target); err == nil {
		return os.Symlink(target, linkPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	if looksLikeFilePath(rel) {
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
	} else if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	return os.Symlink(target, linkPath)
}

func normalizeWorkspaceRelPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "`\"'")
	path = filepath.ToSlash(filepath.Clean(path))
	path = strings.TrimPrefix(path, "./")
	if path == "." || path == "/" || path == "" {
		return ""
	}
	return strings.TrimPrefix(path, "/")
}

func looksLikeFilePath(path string) bool {
	base := filepath.Base(path)
	return strings.Contains(base, ".")
}

type AgentInfo struct {
	AgentID  string `json:"agent_id"`
	DomainID string `json:"domain_id"`
	State    string `json:"state"`
}

func (a *Allocator) AgentInfos() []AgentInfo {
	a.mu.Lock()
	defer a.mu.Unlock()

	infos := make([]AgentInfo, 0, len(a.activeAgents))
	for agentID, agent := range a.activeAgents {
		infos = append(infos, AgentInfo{
			AgentID:  agentID,
			DomainID: agent.DomainID(),
			State:    agent.State().String(),
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
	agent.Stop()
	time.Sleep(100 * time.Millisecond)

	if err := agent.Start(ctx); err != nil {
		return fmt.Errorf("allocator: start revived agent: %w", err)
	}
	return nil
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
			a.dispatchReadyTasks()
			converged, err := a.checkConvergence()
			if err != nil {
				log.Printf("allocator: monitor error: %v", err)
				a.emitStatus(fmt.Sprintf("Monitor error: %v", err), "error")
				continue
			}
			if converged {
				log.Printf("allocator: DAG execution converged (all nodes in terminal states)")
				a.emitStatus("✅ System converged — all nodes in terminal states", "ok")
				return nil
			}
		}
	}
}

func (a *Allocator) checkConvergence() (bool, error) {
	snap := a.dagManager.Snapshot()

	if len(snap.Nodes) == 0 {
		return false, nil // Empty DAG is not considered converged if we just started
	}

	for _, node := range snap.Nodes {
		if !node.Status.IsTerminal() {
			return false, nil // Found a non-terminal node
		}
	}

	return true, nil
}

// dispatchReadyTasks queries the DAG for ready nodes and dispatches them to idle agents.
func (a *Allocator) dispatchReadyTasks() {
	snap := a.dagManager.Snapshot()
	readyNodes := a.dagManager.GetReadyNodes()

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

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, node := range readyNodes {
		agentID := fmt.Sprintf("agent-%s", node.DomainID)

		agent, exists := a.activeAgents[agentID]
		if !exists || agentBusy[agentID] {
			continue // agent offline or busy
		}

		log.Printf("allocator: dispatching node %s to agent %s", node.ID, agentID)

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
		b.WriteString("\nYour current working directory is a filtered source workspace for your assigned domain. Treat it as the project source view; do not cd outside it or inspect parent/runtime directories.\n")
		b.WriteString("Do not read, search, or modify excluded runtime/generated paths such as `.aion/`, `.git/`, `.agents/`, `.codex/`, dependency caches, build outputs, or paths listed in `.aionignore`.\n")

		msg, err := hub.NewMessage(hub.MessageType("task_dispatch"), "orchestrator", agentID, b.String())
		if err != nil {
			log.Printf("allocator: failed to construct task dispatch for %s: %v", node.ID, err)
			a.emitStatus(fmt.Sprintf("Dispatch construction failed for node %s: %v", node.ID, err), "warn")
			continue
		}

		if err := agent.DeliverContext(*msg); err != nil {
			log.Printf("allocator: failed to deliver task to agent %s: %v", agentID, err)
			a.emitStatus(fmt.Sprintf("⚠ Dispatch failed for agent %s: %v", agentID, err), "warn")
		} else {
			a.emitStatus(fmt.Sprintf("Dispatched node %s → %s", node.ID, agentID), "info")
		}
	}
}
