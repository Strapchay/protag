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
	"time"

	"aion-kernel/internal/hub"
	"aion-kernel/internal/session"
	"aion-kernel/internal/supervisor"
)

// PiCoordinator is an implementation of Coordinator that uses a persistent
// Pi Agent to engage in refinement chat and produce a plan.
type PiCoordinator struct {
	agent        *supervisor.AgentSupervisor
	config       PiCoordinatorConfig
	projectRoot  string
	sessionRT    *session.Runtime
	statusFn     func(text, level string)
	lastUserText string
}

// PiCoordinatorConfig configures the Pi-backed coordinator.
type PiCoordinatorConfig struct {
	Binary          string
	SessionDir      string
	SessionStoreDir string
	Provider        string
	Model           string
	SkillPaths      []string
	Env             []string
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
			Binary:     c.config.Binary,
			SessionDir: agentDir,
			WorkingDir: c.projectRoot,
			Provider:   c.config.Provider,
			Model:      c.config.Model,
			SkillPaths: c.config.SkillPaths,
			Env:        c.config.Env,
		},
	})
	c.agent.SetLifecycleFunc(c.handleArchitectLifecycle)

	return c.agent.Start(ctx)
}

// Plan reads the finalized spec from docs/build_spec.md and parses it.
func (c *PiCoordinator) Plan(ctx context.Context, req PlanRequest) (*PlanResponse, error) {
	specPath := filepath.Join(c.projectRoot, "docs", "build_spec.md")
	data, err := os.ReadFile(specPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("pi_coordinator: docs/build_spec.md is missing; ask the Solution Architect to finalize the spec and create docs/build_spec.md before running /build-spec")
		}
		return nil, fmt.Errorf("pi_coordinator: failed to read docs/build_spec.md: %w", err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return nil, fmt.Errorf("pi_coordinator: docs/build_spec.md is empty; ask the Solution Architect to write the finalized spec before running /build-spec")
	}

	// Parse the markdown spec into a PlanResponse.
	return ParseSpecMarkdown(string(data)), nil
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
