package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"aion-kernel/internal/hub"
	"aion-kernel/internal/session"
	"aion-kernel/internal/supervisor"
)

// PiCoordinator is an implementation of Coordinator that uses a persistent
// Pi Agent to engage in refinement chat and produce a plan.
type PiCoordinator struct {
	agent             *supervisor.AgentSupervisor
	plannerAgent      *supervisor.AgentSupervisor
	config            PiCoordinatorConfig
	projectRoot       string
	sessionRT         *session.Runtime
	statusFn          func(text, level string)
	broadcastFn       func(msg hub.Message)
	traceFn           func(text string)
	plannerMu         sync.Mutex
	plannerCapture    *planCapture
	plannerAttemptID  string
	plannerSessionDir string
	plannerGatewayMu  sync.Mutex
	plannerGatewayCh  chan struct{}
	plannerGatewayOn  bool
	lastUserText      string
}

// PiCoordinatorConfig configures the Pi-backed coordinator.
type PiCoordinatorConfig struct {
	Binary                     string
	SessionDir                 string
	SessionStoreDir            string
	Provider                   string
	Model                      string
	Endpoint                   string
	SkillPaths                 []string
	ExtensionPaths             []string
	Env                        []string
	PlannerStartTimeout        time.Duration
	PlannerFirstRequestTimeout time.Duration
	PlannerArtifactTimeout     time.Duration
}

type SolutionArchitectMetadata struct {
	UserGoal       string   `json:"user_goal,omitempty"`
	Decisions      []string `json:"decisions,omitempty"`
	OpenQuestions  []string `json:"open_questions,omitempty"`
	SpecStatus     string   `json:"spec_status"`
	BuildSpecPath  string   `json:"build_spec_path"`
	HandoffStatus  string   `json:"handoff_status"`
	RelevantFiles  []string `json:"relevant_files,omitempty"`
	LastSummary    string   `json:"last_architect_summary,omitempty"`
	HandoffAt      string   `json:"handoff_at,omitempty"`
	HandoffSummary string   `json:"handoff_summary,omitempty"`
}

type planCapture struct {
	mu     sync.Mutex
	done   chan struct{}
	text   string
	closed bool
}

type planningInputArtifact struct {
	Type            string       `json:"type"`
	AttemptHint     string       `json:"attempt_hint,omitempty"`
	ProjectRoot     string       `json:"project_root"`
	BuildSpecPath   string       `json:"build_spec_path"`
	BuildSpec       string       `json:"build_spec"`
	ProjectScan     *ProjectScan `json:"project_scan"`
	OutputPath      string       `json:"output_path"`
	ExcludedPaths   []string     `json:"excluded_paths"`
	ValidationRules []string     `json:"validation_rules"`
}

func newPlanCapture() *planCapture {
	return &planCapture{done: make(chan struct{})}
}

func (p *planCapture) setText(text string) {
	p.mu.Lock()
	if strings.TrimSpace(text) != "" {
		p.text = text
	}
	p.mu.Unlock()
}

func (p *planCapture) close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.done)
	}
	p.mu.Unlock()
}

func (p *planCapture) wait(ctx context.Context) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.text, nil
	}
}

const (
	architectSpecDiscovering     = "discovering"
	architectSpecClarifying      = "clarifying"
	architectSpecDrafting        = "drafting"
	architectSpecReadyForBuild   = "ready_for_build"
	architectSpecHandoffComplete = "handoff_complete"
)

func (c *PiCoordinator) SetStatusFunc(fn func(text, level string)) {
	c.statusFn = fn
}

// SetBroadcastFunc registers a hub broadcast callback for planner and architect events.
func (c *PiCoordinator) SetBroadcastFunc(fn func(msg hub.Message)) {
	c.broadcastFn = fn
}

// SetTraceFunc registers a callback for persisting the coordinator planning trace.
func (c *PiCoordinator) SetTraceFunc(fn func(text string)) {
	c.traceFn = fn
}

// NewPiCoordinator creates a new Pi-backed coordinator.
func NewPiCoordinator(projectRoot string, config PiCoordinatorConfig) *PiCoordinator {
	return &PiCoordinator{
		projectRoot: projectRoot,
		config:      config,
	}
}

// StartArchitect spawns the persistent Pi Agent for the architect phase.
func (c *PiCoordinator) StartArchitect(ctx context.Context) error {
	log.Println("coordinator: starting Pi architect agent")

	agentDir := projectPath(c.projectRoot, c.config.SessionDir, "orchestrator")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		return fmt.Errorf("pi_coordinator: create agent dir: %w", err)
	}

	prompt := GenerateArchitectInstruction("")
	storeDir := c.config.SessionStoreDir
	if storeDir == "" {
		storeDir = filepath.Join(c.projectRoot, ".aion", "agent_sessions")
	}
	store, err := session.NewFileStore(projectPath(c.projectRoot, storeDir))
	if err != nil {
		return fmt.Errorf("pi_coordinator: create session store: %w", err)
	}
	c.sessionRT = session.NewRuntime(store, solutionArchitectPolicy{
		projectRoot: c.projectRoot,
		bootstrap:   prompt,
	}, c.statusFn)
	start, err := c.sessionRT.StartOrResume()
	if err != nil {
		return fmt.Errorf("pi_coordinator: start/resume architect session: %w", err)
	}
	prompt = start.Prompt

	c.agent = supervisor.NewAgentSupervisor(supervisor.AgentConfig{
		AgentID:          "orchestrator",
		DomainID:         "architect",
		InitialPrompt:    prompt,
		HeartbeatTimeout: 30 * time.Minute, // Architect takes time to think/wait for user
		ProgressTimeout:  1 * time.Hour,
		PiAgent: supervisor.PiAgentConfig{
			Binary:         c.config.Binary,
			SessionDir:     agentDir,
			ResumeSession:  start.Resumed,
			WorkingDir:     c.projectRoot,
			Provider:       c.config.Provider,
			Model:          c.config.Model,
			Endpoint:       c.config.Endpoint,
			SkillPaths:     c.config.SkillPaths,
			ExtensionPaths: c.config.ExtensionPaths,
			Env:            append(c.config.Env, "AION_AGENT_ID=orchestrator", "AION_DOMAIN_ID=architect"),
		},
	})
	c.agent.SetLifecycleFunc(c.handleArchitectLifecycle)

	return c.agent.Start(ctx)
}

func (c *PiCoordinator) ensurePlannerAgent(ctx context.Context, attemptID string) error {
	attemptID = plannerAttemptID(attemptID)
	c.plannerMu.Lock()
	if c.plannerAgent != nil && c.plannerAgent.State() != supervisor.StateStopped && c.plannerAttemptID == attemptID {
		c.plannerMu.Unlock()
		return nil
	}
	previous := c.plannerAgent
	c.plannerAgent = nil
	c.plannerMu.Unlock()
	if previous != nil {
		_ = previous.Stop()
	}

	agentDir := projectPath(c.projectRoot, c.config.SessionDir, "coordinator", attemptID)
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		return fmt.Errorf("pi_coordinator: create coordinator planner dir: %w", err)
	}

	planner := supervisor.NewAgentSupervisor(c.plannerSupervisorConfig(agentDir))
	if c.broadcastFn != nil {
		planner.SetBroadcastFunc(c.broadcastFn)
	}
	if c.statusFn != nil {
		planner.SetStatusFunc(c.statusFn)
	}
	planner.SetLifecycleFunc(c.handlePlannerLifecycle)

	timeout := c.config.PlannerStartTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := make(chan error, 1)
	go func() {
		started <- planner.Start(startCtx)
	}()

	select {
	case err := <-started:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		_ = planner.Stop()
		return ctx.Err()
	case <-time.After(timeout):
		cancel()
		_ = planner.Stop()
		return fmt.Errorf("coordinator planner startup timed out after %s", timeout)
	}

	c.plannerMu.Lock()
	defer c.plannerMu.Unlock()
	if c.plannerAgent != nil && c.plannerAgent.State() != supervisor.StateStopped {
		_ = planner.Stop()
		return nil
	}
	c.plannerAgent = planner
	c.plannerAttemptID = attemptID
	c.plannerSessionDir = agentDir
	if c.traceFn != nil {
		c.traceFn("coordinator planner process started")
	}
	return nil
}

// RecordPlannerGatewayActivity records gateway request activity for the
// Coordinator planner. It is used as a prompt-delivery handshake during
// /build-spec planning.
func (c *PiCoordinator) RecordPlannerGatewayActivity(agentID, domainID, phase string) {
	if agentID != "coordinator" && domainID != "coordinator" {
		return
	}
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "queued", "admitted", "forwarding", "active", "completed":
	default:
		return
	}
	c.plannerGatewayMu.Lock()
	defer c.plannerGatewayMu.Unlock()
	if c.plannerGatewayOn {
		c.plannerGatewayOn = false
		close(c.plannerGatewayCh)
	}
}

func (c *PiCoordinator) beginPlannerGatewayWatch() <-chan struct{} {
	c.plannerGatewayMu.Lock()
	defer c.plannerGatewayMu.Unlock()
	c.plannerGatewayCh = make(chan struct{})
	c.plannerGatewayOn = true
	return c.plannerGatewayCh
}

func (c *PiCoordinator) waitForPlannerGatewayActivity(ctx context.Context, gatewayCh <-chan struct{}, outputPath string) error {
	timeout := c.config.PlannerFirstRequestTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if _, err := os.Stat(outputPath); err == nil {
			if c.traceFn != nil {
				c.traceFn("coordinator plan artifact appeared before first gateway request was observed")
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gatewayCh:
			if c.traceFn != nil {
				c.traceFn("coordinator inference request observed at gateway")
			}
			return nil
		case <-timer.C:
			return fmt.Errorf("no coordinator inference request observed within %s after prompt dispatch; planner_raw_tail=%q", timeout, c.tailPlannerRawLog(12))
		case <-ticker.C:
		}
	}
}

func (c *PiCoordinator) sendPlanningPromptWithHandshake(ctx context.Context, prompt, outputPath string) error {
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		if c.traceFn != nil {
			c.traceFn(fmt.Sprintf("coordinator prompt send attempt %d starting", attempt))
		}
		c.plannerMu.Lock()
		c.plannerCapture = nil
		planner := c.plannerAgent
		c.plannerMu.Unlock()

		if planner == nil {
			return fmt.Errorf("pi_coordinator: planner agent not started")
		}

		gatewayCh := c.beginPlannerGatewayWatch()
		if err := planner.SendPrompt(prompt); err != nil {
			if c.traceFn != nil {
				c.traceFn(fmt.Sprintf("send prompt error: %v", err))
			}
			return fmt.Errorf("pi_coordinator: send planning prompt: %w", err)
		}
		if c.statusFn != nil {
			c.statusFn("Coordinator prompt dispatched; awaiting first inference request...", "info")
		}
		if c.broadcastFn != nil {
			if msg, err := hub.NewMessage(hub.MsgContextShare, "coordinator", "tui", map[string]string{
				"type":    "text",
				"content": "Coordinator prompt dispatched to the live planner.",
				"role":    "assistant",
			}); err == nil {
				c.broadcastFn(*msg)
			}
		}

		if err := c.waitForPlannerGatewayActivity(ctx, gatewayCh, outputPath); err == nil {
			return nil
		} else {
			lastErr = err
			if c.traceFn != nil {
				c.traceFn(fmt.Sprintf("coordinator prompt attempt %d did not reach gateway: %v", attempt, err))
			}
		}

		if attempt == 1 {
			if c.statusFn != nil {
				c.statusFn("Coordinator prompt did not reach inference gateway; restarting planner once...", "warn")
			}
			if c.traceFn != nil {
				c.traceFn("restarting coordinator planner after prompt-delivery timeout")
			}
			_ = c.StopPlanner()
			c.plannerMu.Lock()
			attemptID := c.plannerAttemptID
			c.plannerMu.Unlock()
			if err := c.ensurePlannerAgent(ctx, attemptID); err != nil {
				return fmt.Errorf("pi_coordinator: restart planner after prompt-delivery timeout: %w", err)
			}
		}
	}
	return lastErr
}

func (c *PiCoordinator) tailPlannerRawLog(maxLines int) string {
	c.plannerMu.Lock()
	dir := c.plannerSessionDir
	c.plannerMu.Unlock()
	if strings.TrimSpace(dir) == "" {
		dir = projectPath(c.projectRoot, c.config.SessionDir, "coordinator")
	}
	path := filepath.Join(dir, "pi_raw.log")
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("unavailable: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if maxLines > 0 && len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return strings.Join(lines, "\n")
}

func (c *PiCoordinator) plannerArtifactsPreparedMessage(paths planningArtifactPaths) {
	if c.traceFn != nil {
		c.traceFn(fmt.Sprintf("planning input artifact written: %s", paths.InputPath))
		c.traceFn(fmt.Sprintf("planning output artifact expected: %s", paths.OutputPath))
	}
	if c.broadcastFn != nil {
		if msg, err := hub.NewMessage(hub.MsgContextShare, "coordinator", "tui", map[string]string{
			"type":    "text",
			"content": "Coordinator planning artifacts prepared; starting live planner.",
			"role":    "assistant",
		}); err == nil {
			c.broadcastFn(*msg)
		}
	}
	if c.statusFn != nil {
		c.statusFn("Coordinator planning artifacts prepared; starting planner...", "info")
	}
}

func (c *PiCoordinator) plannerPromptRenderedMessage(prompt string, req PlanRequest) {
	if c.traceFn != nil {
		var files, modules int
		if req.ProjectScan != nil {
			files = req.ProjectScan.FileCount
			modules = req.ProjectScan.ModuleCount
		}
		c.traceFn(fmt.Sprintf("prompt rendered: bytes=%d files=%d modules=%d", len(prompt), files, modules))
		c.traceFn("prompt preview:\n" + truncateForTrace(prompt, 2000))
	}
}

func (c *PiCoordinator) plannerReasoningMessage() {
	if c.broadcastFn != nil {
		if msg, err := hub.NewMessage(hub.MsgContextShare, "coordinator", "tui", map[string]string{
			"type":    "thinking",
			"content": "Coordinator is reasoning over the build spec and project scan.",
			"role":    "assistant",
		}); err == nil {
			c.broadcastFn(*msg)
		}
	}
	if c.statusFn != nil {
		c.statusFn("Coordinator reasoning over build-spec input...", "info")
	}
}

func (c *PiCoordinator) preparePlannerForPlanning(ctx context.Context, attemptID string) error {
	if c.traceFn != nil {
		c.traceFn("starting coordinator planner process")
	}
	if err := c.ensurePlannerAgent(ctx, attemptID); err != nil {
		return err
	}
	return nil
}

func (c *PiCoordinator) plannerSupervisorConfig(agentDir string) supervisor.AgentConfig {
	return supervisor.AgentConfig{
		AgentID:          "coordinator",
		DomainID:         "coordinator",
		InitialPrompt:    "",
		HeartbeatTimeout: 30 * time.Minute,
		ProgressTimeout:  1 * time.Hour,
		PiAgent: supervisor.PiAgentConfig{
			Binary:         c.config.Binary,
			SessionDir:     agentDir,
			WorkingDir:     c.projectRoot,
			Provider:       c.config.Provider,
			Model:          c.config.Model,
			Endpoint:       c.config.Endpoint,
			SkillPaths:     c.config.SkillPaths,
			ExtensionPaths: c.config.ExtensionPaths,
			Env:            append(c.config.Env, "AION_AGENT_ID=coordinator", "AION_DOMAIN_ID=coordinator"),
		},
	}
}

// Plan asks the Coordinator Pi agent to reason over the finalized spec and
// project scan, then parses the structured plan response.
func (c *PiCoordinator) Plan(ctx context.Context, req PlanRequest) (*PlanResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	specText := strings.TrimSpace(req.UserPrompt)
	if specText == "" {
		specPath := filepath.Join(c.projectRoot, "docs", "build_spec.md")
		data, err := os.ReadFile(specPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("pi_coordinator: docs/build_spec.md is missing; ask the Solution Architect to finalize the spec and create docs/build_spec.md before running /build-spec")
			}
			return nil, fmt.Errorf("pi_coordinator: failed to read docs/build_spec.md: %w", err)
		}
		specText = strings.TrimSpace(string(data))
		if specText == "" {
			return nil, fmt.Errorf("pi_coordinator: docs/build_spec.md is empty; ask the Solution Architect to write the finalized spec before running /build-spec")
		}
	}

	if strings.TrimSpace(c.config.Binary) == "" {
		if c.traceFn != nil {
			c.traceFn("planner binary missing; refusing static build-spec planning")
		}
		return nil, fmt.Errorf("pi_coordinator: planner binary is not configured")
	}
	req.AttemptID = plannerAttemptID(req.AttemptID)

	artifactPaths, err := c.preparePlanningArtifacts(specText, req)
	if err != nil {
		return nil, err
	}
	c.plannerArtifactsPreparedMessage(artifactPaths)

	prompt := GenerateCoordinatorPlanningInstruction(specText, req.ProjectScan, artifactPaths.InputPath, artifactPaths.OutputPath)
	c.plannerPromptRenderedMessage(prompt, req)

	if err := c.preparePlannerForPlanning(ctx, req.AttemptID); err != nil {
		return nil, fmt.Errorf("pi_coordinator: ensure planner agent: %w", err)
	}
	defer func() { _ = c.StopPlanner() }()
	c.plannerReasoningMessage()

	if err := c.sendPlanningPromptWithHandshake(ctx, prompt, artifactPaths.OutputPath); err != nil {
		if c.traceFn != nil {
			c.traceFn(fmt.Sprintf("planning prompt handshake failed: %v", err))
		}
		return nil, fmt.Errorf("pi_coordinator: planning prompt was not accepted by coordinator runtime: %w", err)
	}
	if c.statusFn != nil {
		c.statusFn("Coordinator prompt dispatched; awaiting plan response...", "info")
	}

	plan, err := c.waitForPlanArtifact(ctx, artifactPaths.OutputPath)
	if err != nil {
		if c.traceFn != nil {
			c.traceFn(fmt.Sprintf("plan artifact wait/read failed: %v", err))
		}
		return nil, fmt.Errorf("pi_coordinator: planning failed: %w", err)
	}
	if err := ValidatePlanResponse(plan); err != nil {
		if c.traceFn != nil {
			c.traceFn(fmt.Sprintf("initial validation failed: %v", err))
		}
		plan, err = c.repairPlanArtifact(ctx, artifactPaths.OutputPath, err)
		if err != nil {
			if c.traceFn != nil {
				c.traceFn(fmt.Sprintf("validation failed: %v", err))
			}
			return nil, fmt.Errorf("pi_coordinator: planning validation failed: %w", err)
		}
	}
	if c.traceFn != nil {
		c.traceFn(fmt.Sprintf("plan valid: domains=%d nodes=%d edges=%d", len(plan.Domains), len(plan.Nodes), len(plan.Edges)))
	}
	return plan, nil
}

type planningArtifactPaths struct {
	Dir        string
	InputPath  string
	OutputPath string
	ErrorPath  string
}

func (c *PiCoordinator) planningArtifactPaths(attemptID string) planningArtifactPaths {
	dir := filepath.Join(c.projectRoot, "docs", "aion", "planning", plannerAttemptID(attemptID))
	return planningArtifactPaths{
		Dir:        dir,
		InputPath:  filepath.Join(dir, "planning_input.json"),
		OutputPath: filepath.Join(dir, "plan_response.json"),
		ErrorPath:  filepath.Join(dir, "plan_validation_error.txt"),
	}
}

func (c *PiCoordinator) preparePlanningArtifacts(specText string, req PlanRequest) (planningArtifactPaths, error) {
	paths := c.planningArtifactPaths(req.AttemptID)
	if err := os.MkdirAll(paths.Dir, 0755); err != nil {
		return paths, fmt.Errorf("pi_coordinator: create planning artifact dir: %w", err)
	}
	for _, path := range []string{paths.OutputPath, paths.ErrorPath} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return paths, fmt.Errorf("pi_coordinator: clear planning artifact %s: %w", path, err)
		}
	}
	input := planningInputArtifact{
		Type:          "aion_coordinator_planning_input",
		AttemptHint:   req.AttemptID,
		ProjectRoot:   c.projectRoot,
		BuildSpecPath: filepath.Join(c.projectRoot, "docs", "build_spec.md"),
		BuildSpec:     specText,
		ProjectScan:   req.ProjectScan,
		OutputPath:    paths.OutputPath,
		ExcludedPaths: DefaultProjectScanExcludes(),
		ValidationRules: []string{
			"domains must be non-empty",
			"nodes must be non-empty",
			"domain IDs and node IDs must be stable, non-empty, and non-placeholder",
			"each node must reference an existing domain",
			"edges must reference existing nodes and must not be self-referential",
			"the dependency graph must be acyclic",
			"broad fallback path '.' must not be assigned to multiple domains",
			"milestones are optional but recommended; prefer 3-8 coarse capability milestones and map each node to at most one milestone",
		},
	}
	data, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return paths, fmt.Errorf("pi_coordinator: marshal planning input: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(paths.InputPath, data, 0644); err != nil {
		return paths, fmt.Errorf("pi_coordinator: write planning input: %w", err)
	}
	return paths, nil
}

func (c *PiCoordinator) waitForPlanArtifact(ctx context.Context, outputPath string) (*PlanResponse, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	timeout := c.config.PlannerArtifactTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var lastErr error
	var lastErrText string
	lastTraceAt := time.Time{}
	for {
		plan, err := readPlanArtifact(outputPath)
		if err == nil {
			if c.traceFn != nil {
				c.traceFn(fmt.Sprintf("plan artifact read: domains=%d nodes=%d edges=%d", len(plan.Domains), len(plan.Nodes), len(plan.Edges)))
			}
			return plan, nil
		}
		if !os.IsNotExist(err) {
			lastErr = err
			if c.traceFn != nil {
				errText := err.Error()
				now := time.Now()
				if errText != lastErrText || now.Sub(lastTraceAt) >= 10*time.Second {
					c.traceFn(fmt.Sprintf("plan artifact not ready: %v", err))
					lastErrText = errText
					lastTraceAt = now
				}
			}
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, ctx.Err()
		case <-timer.C:
			if lastErr != nil {
				return nil, fmt.Errorf("plan artifact was not written within %s; last_read_error=%v; planner_raw_tail=%q", timeout, lastErr, c.tailPlannerRawLog(20))
			}
			return nil, fmt.Errorf("plan artifact was not written within %s; planner_raw_tail=%q", timeout, c.tailPlannerRawLog(20))
		case <-ticker.C:
		}
	}
}

func readPlanArtifact(path string) (*PlanResponse, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wrapped struct {
		Type string `json:"type"`
		PlanResponse
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && (wrapped.Type == "" || strings.EqualFold(wrapped.Type, "plan_response")) {
		plan := wrapped.PlanResponse
		return &plan, nil
	}
	var plan PlanResponse
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func (c *PiCoordinator) repairPlanArtifact(ctx context.Context, outputPath string, validationErr error) (*PlanResponse, error) {
	c.plannerMu.Lock()
	planner := c.plannerAgent
	c.plannerMu.Unlock()

	if planner == nil {
		return nil, validationErr
	}

	dir := filepath.Dir(outputPath)
	paths := planningArtifactPaths{
		Dir:        dir,
		InputPath:  filepath.Join(dir, "planning_input.json"),
		OutputPath: outputPath,
		ErrorPath:  filepath.Join(dir, "plan_validation_error.txt"),
	}
	_ = os.WriteFile(paths.ErrorPath, []byte(validationErr.Error()+"\n"), 0644)
	if err := os.Remove(paths.OutputPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	repairPrompt := strings.TrimSpace(fmt.Sprintf(`The plan artifact at %s failed validation:

Error:
%s

Read the planning input artifact at %s again and overwrite the output artifact with a valid, non-empty plan_response JSON object:
%s

Do not answer with the plan in chat. Write the corrected artifact directly.`, outputPath, validationErr.Error(), paths.InputPath, outputPath))
	if c.traceFn != nil {
		c.traceFn("requesting coordinator plan artifact repair after validation failure")
	}
	if c.statusFn != nil {
		c.statusFn("Coordinator plan artifact failed validation; requesting repair...", "warn")
	}
	// The planner may be idle after writing the rejected artifact. A direct
	// prompt starts a new turn reliably in both active and idle Pi sessions.
	if err := planner.SendPrompt(repairPrompt); err != nil {
		if c.traceFn != nil {
			c.traceFn(fmt.Sprintf("repair prompt error: %v", err))
		}
		return nil, validationErr
	}

	plan, err := c.waitForPlanArtifact(ctx, outputPath)
	if err != nil {
		return nil, err
	}
	if err := ValidatePlanResponse(plan); err != nil {
		return nil, err
	}
	return plan, nil
}

// Replan re-evaluates a subgraph.
func (c *PiCoordinator) Replan(ctx context.Context, req ReplanRequest) (*ReplanResponse, error) {
	// Simple no-op for now
	return &ReplanResponse{
		ModifiedNodes: req.CurrentNodes,
	}, nil
}

// Refine delivers a user message to the architect agent.
func (c *PiCoordinator) Refine(ctx context.Context, text string) error {
	if c.agent == nil {
		return fmt.Errorf("pi_coordinator: architect agent not started")
	}
	c.lastUserText = text
	if c.sessionRT != nil {
		c.updateMetadataFromUser(text)
		if _, _, err := c.sessionRT.BeginRequest(text, "", session.SideEffectNone); err != nil {
			return fmt.Errorf("pi_coordinator: record architect request: %w", err)
		}
	}

	msg, _ := hub.NewMessage(hub.MsgContextShare, "user", "orchestrator", text)
	return c.agent.DeliverContext(*msg)
}

func (c *PiCoordinator) RetryLast(ctx context.Context) error {
	if c.agent == nil {
		return fmt.Errorf("pi_coordinator: architect agent not started")
	}
	if c.sessionRT == nil {
		return fmt.Errorf("pi_coordinator: session runtime not initialized")
	}
	text, err := c.sessionRT.RetryLastRequest()
	if err != nil {
		return err
	}
	c.lastUserText = text
	msg, _ := hub.NewMessage(hub.MsgContextShare, "user", "orchestrator", text)
	return c.agent.DeliverContext(*msg)
}

func (c *PiCoordinator) Continue(ctx context.Context) error {
	if c.agent == nil {
		return fmt.Errorf("pi_coordinator: architect agent not started")
	}
	if c.sessionRT == nil {
		return fmt.Errorf("pi_coordinator: session runtime not initialized")
	}
	text, err := c.sessionRT.ContinuePrompt()
	if err != nil {
		return err
	}
	return c.agent.SendFollowUp(text)
}

func (c *PiCoordinator) Resume(ctx context.Context) error {
	if c.agent == nil {
		return fmt.Errorf("pi_coordinator: architect agent not started")
	}
	if c.sessionRT == nil {
		return fmt.Errorf("pi_coordinator: session runtime not initialized")
	}
	prompt, err := c.sessionRT.ReconcilePrompt()
	if err != nil {
		return err
	}
	return c.agent.SendFollowUp(prompt)
}

func (c *PiCoordinator) handlePlannerLifecycle(event supervisor.AgentLifecycleEvent) {
	c.plannerMu.Lock()
	capture := c.plannerCapture
	c.plannerMu.Unlock()

	switch event.Kind {
	case "text":
		if capture != nil {
			capture.setText(event.Content)
		}
		if c.traceFn != nil && strings.TrimSpace(event.Content) != "" {
			c.traceFn("planner text:\n" + truncateForTrace(event.Content, 2000))
		}
	case "thinking":
		if c.traceFn != nil && strings.TrimSpace(event.Content) != "" {
			c.traceFn("planner thinking:\n" + truncateForTrace(event.Content, 2000))
		}
		// Keep the latest transcript text for parsing, but preserve thinking in the UI.
	case "turn_end", "agent_end", "agent_error", "agent_stream_closed":
		if capture != nil && event.Content != "" {
			capture.setText(event.Content)
		}
		if capture != nil {
			capture.close()
		}
	}
}

func (c *PiCoordinator) StopPlanner() error {
	c.plannerMu.Lock()
	planner := c.plannerAgent
	c.plannerAgent = nil
	capture := c.plannerCapture
	c.plannerCapture = nil
	c.plannerMu.Unlock()

	if capture != nil {
		capture.close()
	}
	if planner == nil {
		return nil
	}
	if c.traceFn != nil {
		c.traceFn("stopping coordinator planner after build-spec attempt ended")
	}
	return planner.Stop()
}

func truncateForTrace(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit] + "\n...<truncated>..."
}

func (c *PiCoordinator) SessionStatus() string {
	if c.sessionRT == nil {
		return "Solution Architect session runtime is not initialized"
	}
	return c.sessionRT.StatusText()
}

func (c *PiCoordinator) ReplayArchitectHistory() []hub.Message {
	if c.sessionRT == nil {
		return nil
	}
	events, err := c.sessionRT.ReplayEvents(0)
	if err != nil {
		return nil
	}
	messages := make([]hub.Message, 0, len(events))
	for _, event := range events {
		if msg, ok := sessionEventToHubMessage(event); ok {
			messages = append(messages, msg)
		}
	}
	return messages
}

func (c *PiCoordinator) MarkBuildSpecHandoff() {
	if c.sessionRT != nil {
		c.sessionRT.MarkHandoff("User issued /build-spec; Coordinator handoff started")
	}
}

func sessionEventToHubMessage(event session.SessionEvent) (hub.Message, bool) {
	switch event.Kind {
	case session.EventUserMessage:
		return newReplayHubMessage(event, hub.MsgContextShare, "user", "orchestrator", map[string]string{
			"type":    "text",
			"content": event.Content,
			"role":    "user",
		}), true
	case session.EventAssistantText:
		return newReplayHubMessage(event, hub.MsgContextShare, "orchestrator", "tui", map[string]string{
			"type":    "text",
			"content": event.Content,
			"role":    "assistant",
		}), true
	case session.EventAssistantThinking:
		return newReplayHubMessage(event, hub.MsgContextShare, "orchestrator", "tui", map[string]string{
			"type":    "thinking",
			"content": event.Content,
			"role":    "assistant",
		}), true
	case session.EventToolStart:
		return newReplayHubMessage(event, hub.MsgContextShare, "orchestrator", "tui", map[string]string{
			"type":    "tool_start",
			"tool":    event.ToolName,
			"input":   event.ToolInput,
			"summary": event.Summary,
		}), true
	case session.EventFailure:
		text := event.Summary
		if text == "" {
			text = event.Content
		}
		return newReplayHubMessage(event, hub.MsgSystemStatus, "orchestrator", "", hub.SystemStatusPayload{
			Text:  text,
			Level: "error",
		}), true
	case session.EventStatus:
		return newReplayHubMessage(event, hub.MsgSystemStatus, "orchestrator", "", hub.SystemStatusPayload{
			Text:  event.Content,
			Level: "info",
		}), true
	default:
		return hub.Message{}, false
	}
}

func newReplayHubMessage(event session.SessionEvent, msgType hub.MessageType, from, to string, payload interface{}) hub.Message {
	data, _ := json.Marshal(payload)
	id := event.EventID
	if id == "" {
		id = fmt.Sprintf("replay-%d", time.Now().UnixNano())
	}
	return hub.Message{
		ID:        id,
		Type:      msgType,
		FromAgent: from,
		ToAgent:   to,
		Payload:   data,
		Timestamp: event.Timestamp,
	}
}

func (c *PiCoordinator) ShutdownCheckpoint() {
	if c.sessionRT != nil {
		c.sessionRT.ShutdownCheckpoint()
	}
}

func (c *PiCoordinator) StopArchitect() error {
	if c.agent == nil {
		return nil
	}
	if c.sessionRT != nil {
		c.sessionRT.ShutdownCheckpoint()
	}
	err := c.agent.Stop()
	c.agent = nil
	c.sessionRT = nil
	return err
}

// GetArchitectAgent returns the underlying supervisor.
func (c *PiCoordinator) GetArchitectAgent() interface{} {
	return c.agent
}

func (c *PiCoordinator) handleArchitectLifecycle(event supervisor.AgentLifecycleEvent) {
	if c.sessionRT == nil {
		return
	}
	switch event.Kind {
	case "thinking":
		c.sessionRT.RecordAssistantThinking(event.Content)
	case "text":
		c.updateMetadataFromArchitect(event.Content)
		c.sessionRT.RecordAssistantText(event.Content)
	case "tool_start":
		level := sideEffectForTool(event.ToolName)
		c.updateMetadataFromTool(event.ToolName, event.ToolInput, event.ToolSummary)
		c.sessionRT.RecordToolStart(event.ToolName, event.ToolInput, event.ToolSummary, level)
	case "turn_end":
		c.sessionRT.CompleteActiveRequest()
	case "tool_error":
		c.sessionRT.FailActiveRequest(session.FailureToolFailure, event.Error, true)
	case "agent_error":
		c.sessionRT.FailActiveRequest(session.FailureAgentProcessCrash, event.Error, true)
	case "agent_stream_closed":
		c.sessionRT.FailActiveRequest(session.FailureAgentStreamClosed, event.Error, true)
	case "network_timeout":
		c.sessionRT.FailActiveRequest(session.FailureNetworkTimeout, event.Error, true)
	case "provider_unavailable":
		c.sessionRT.FailActiveRequest(session.FailureProviderUnavailable, event.Error, true)
	case "provider_auth_error":
		c.sessionRT.FailActiveRequest(session.FailureProviderAuthError, event.Error, false)
	}
}

type solutionArchitectPolicy struct {
	projectRoot string
	bootstrap   string
}

func (p solutionArchitectPolicy) Role() session.AgentRole { return session.RoleSolutionArchitect }
func (p solutionArchitectPolicy) AgentID() string         { return "orchestrator" }
func (p solutionArchitectPolicy) Scope() string           { return "workspace" }

func (p solutionArchitectPolicy) NewSessionMetadata() map[string]interface{} {
	meta := SolutionArchitectMetadata{
		SpecStatus:    architectSpecDiscovering,
		BuildSpecPath: filepath.Join("docs", "build_spec.md"),
		HandoffStatus: "not_started",
	}
	return map[string]interface{}{
		"spec_status":     meta.SpecStatus,
		"build_spec_path": meta.BuildSpecPath,
		"handoff_status":  meta.HandoffStatus,
	}
}

func (p solutionArchitectPolicy) BootstrapPrompt() string {
	return p.bootstrap
}

func (p solutionArchitectPolicy) ResumePrompt(ctx session.ResumeContext) string {
	var b strings.Builder
	b.WriteString("You are resuming the Aion-Kernel Solution Architect session.\n\n")
	b.WriteString("Do not restart discovery or repeat the initial onboarding prompt unless the user explicitly asks.\n")
	b.WriteString("Continue from the restored session state and wait for the user's next instruction if no action is required.\n\n")
	b.WriteString("Session state:\n")
	b.WriteString("- session_id: " + ctx.Session.SessionID + "\n")
	b.WriteString("- status: " + string(ctx.Session.Status) + "\n")
	b.WriteString("- build_spec_path: docs/build_spec.md\n")
	writeMetadataLine(&b, ctx.Session.Metadata, "user_goal")
	writeMetadataLine(&b, ctx.Session.Metadata, "spec_status")
	writeMetadataLine(&b, ctx.Session.Metadata, "handoff_status")
	writeMetadataLine(&b, ctx.Session.Metadata, "open_questions")
	writeMetadataLine(&b, ctx.Session.Metadata, "relevant_files")
	writeMetadataLine(&b, ctx.Session.Metadata, "last_architect_summary")
	writeMetadataLine(&b, ctx.Session.Metadata, "rolling_summary")
	if ctx.LastRequest != nil {
		b.WriteString("- last_request_status: " + string(ctx.LastRequest.Status) + "\n")
		b.WriteString("- last_user_request: " + ctx.LastRequest.UserText + "\n")
	}
	if ctx.Checkpoint != nil && ctx.Checkpoint.Summary != "" {
		b.WriteString("- latest_checkpoint: " + ctx.Checkpoint.Summary + "\n")
	}
	b.WriteString("\nRecent session events:\n")
	for _, event := range ctx.Events {
		text := event.Summary
		if text == "" {
			text = event.Content
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		if len(text) > 500 {
			text = text[:500] + "..."
		}
		b.WriteString("- " + string(event.Kind) + ": " + text + "\n")
	}
	b.WriteString("\nResume quietly. If the prior user request was incomplete, explain that the session was restored and ask whether to retry or continue.\n")
	return b.String()
}

func (p solutionArchitectPolicy) CheckpointSummary(events []session.SessionEvent) string {
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Kind == session.EventAssistantText && strings.TrimSpace(event.Content) != "" {
			text := strings.TrimSpace(event.Content)
			if len(text) > 300 {
				text = text[:300] + "..."
			}
			return text
		}
	}
	return "Solution Architect session checkpoint"
}

func sideEffectForTool(name string) session.SideEffectLevel {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "read"), strings.Contains(n, "list"), strings.Contains(n, "search"), strings.Contains(n, "grep"):
		return session.SideEffectReadOnly
	case strings.Contains(n, "write"), strings.Contains(n, "edit"), strings.Contains(n, "patch"):
		return session.SideEffectWritePossible
	default:
		return session.SideEffectUnknown
	}
}

func (c *PiCoordinator) updateMetadataFromUser(text string) {
	if c.sessionRT == nil {
		return
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return
	}
	c.sessionRT.UpdateMetadata(func(metadata map[string]interface{}) {
		if _, ok := metadata["user_goal"]; !ok {
			metadata["user_goal"] = truncateForMetadata(trimmed, 500)
		}
		if strings.Contains(trimmed, "?") {
			metadata["spec_status"] = architectSpecClarifying
		}
	})
}

func (c *PiCoordinator) updateMetadataFromArchitect(text string) {
	if c.sessionRT == nil {
		return
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return
	}
	c.sessionRT.UpdateMetadata(func(metadata map[string]interface{}) {
		metadata["last_architect_summary"] = truncateForMetadata(trimmed, 700)
		lower := strings.ToLower(trimmed)
		switch {
		case strings.Contains(lower, "/build-spec"), strings.Contains(lower, "ready to build"):
			metadata["spec_status"] = architectSpecReadyForBuild
		case strings.Contains(lower, "docs/build_spec.md"), strings.Contains(lower, "build_spec.md"):
			metadata["spec_status"] = architectSpecDrafting
		case strings.Contains(trimmed, "?"):
			metadata["spec_status"] = architectSpecClarifying
		}
		if questions := extractQuestions(trimmed); len(questions) > 0 {
			metadata["open_questions"] = mergeStringList(metadata["open_questions"], questions, 10)
		}
		if decisions := extractDecisionLines(trimmed); len(decisions) > 0 {
			metadata["decisions"] = mergeStringList(metadata["decisions"], decisions, 20)
		}
	})
}

func (c *PiCoordinator) updateMetadataFromTool(name, input, summary string) {
	if c.sessionRT == nil {
		return
	}
	files := extractFileRefs(input + "\n" + summary)
	if len(files) == 0 {
		return
	}
	c.sessionRT.UpdateMetadata(func(metadata map[string]interface{}) {
		metadata["relevant_files"] = mergeStringList(metadata["relevant_files"], files, 50)
	})
}

func writeMetadataLine(b *strings.Builder, metadata map[string]interface{}, key string) {
	if metadata == nil {
		return
	}
	if value, ok := metadata[key]; ok {
		b.WriteString("- " + key + ": " + fmt.Sprint(value) + "\n")
	}
}

func truncateForMetadata(text string, max int) string {
	text = strings.TrimSpace(text)
	if len(text) <= max {
		return text
	}
	return text[:max] + "..."
}

func extractQuestions(text string) []string {
	var questions []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "-"))
		if strings.Contains(line, "?") {
			questions = append(questions, truncateForMetadata(line, 240))
		}
	}
	return questions
}

func extractDecisionLines(text string) []string {
	var decisions []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "-"))
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "decision:") || strings.Contains(lower, "we decided") || strings.Contains(lower, "agreed") {
			decisions = append(decisions, truncateForMetadata(line, 240))
		}
	}
	return decisions
}

var fileRefPattern = regexp.MustCompile(`(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+\.[A-Za-z0-9_.-]+`)

func extractFileRefs(text string) []string {
	matches := fileRefPattern.FindAllString(text, -1)
	return mergeStringList(nil, matches, 20)
}

func mergeStringList(existing interface{}, additions []string, limit int) []string {
	seen := map[string]struct{}{}
	var out []string
	switch values := existing.(type) {
	case []string:
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, ok := seen[value]; !ok {
				seen[value] = struct{}{}
				out = append(out, value)
			}
		}
	case []interface{}:
		for _, raw := range values {
			value := strings.TrimSpace(fmt.Sprint(raw))
			if value == "" {
				continue
			}
			if _, ok := seen[value]; !ok {
				seen[value] = struct{}{}
				out = append(out, value)
			}
		}
	}
	for _, value := range additions {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			out = append(out, value)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func projectPath(projectRoot string, parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if filepath.IsAbs(part) {
			cleaned = cleaned[:0]
		}
		cleaned = append(cleaned, part)
	}
	if len(cleaned) == 0 {
		return projectRoot
	}
	if filepath.IsAbs(cleaned[0]) {
		return filepath.Join(cleaned...)
	}
	return filepath.Join(append([]string{projectRoot}, cleaned...)...)
}

func plannerAttemptID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "plan_" + time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	value = regexp.MustCompile(`[^A-Za-z0-9._-]+`).ReplaceAllString(value, "-")
	value = strings.Trim(value, ".-")
	if value == "" {
		return "plan"
	}
	if len(value) > 120 {
		return value[:120]
	}
	return value
}
