package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
)

const buildSpecStaleAfter = 25 * time.Minute

func (d *Daemon) buildSpecAttemptIsActive() bool {
	d.buildSpecMu.Lock()
	defer d.buildSpecMu.Unlock()
	if d.buildSpecAttempt == nil {
		return false
	}
	switch d.buildSpecAttempt.Status {
	case BuildSpecAttemptPlanning, BuildSpecAttemptCommitting, BuildSpecAttemptAllocating:
		return true
	default:
		return false
	}
}

func (d *Daemon) seedBuildSpecAttemptContext(attempt *BuildSpecAttempt) {
	if attempt == nil || d == nil || d.server == nil {
		return
	}
	switch attempt.Status {
	case BuildSpecAttemptActive, BuildSpecAttemptPaused, BuildSpecAttemptAllocating, BuildSpecAttemptPlanning, BuildSpecAttemptCommitting:
	default:
		return
	}
	d.server.SeedHubSnapshot(buildSpecAttemptHubMessages(d.runState.Root, attempt))
}

func buildSpecAttemptHubMessages(runRoot string, attempt *BuildSpecAttempt) []hub.Message {
	if attempt == nil {
		return nil
	}
	now := time.Now().UTC()
	messages := make([]hub.Message, 0, 4)

	addText := func(idSuffix, content string) {
		content = strings.TrimSpace(content)
		if content == "" {
			return
		}
		msg, err := hub.NewMessage(hub.MsgContextShare, "coordinator", "tui", map[string]string{
			"type":    "text",
			"content": content,
			"role":    "assistant",
		})
		if err != nil {
			return
		}
		msg.ID = fmt.Sprintf("buildspec-replay-%s-%s", attempt.AttemptID, idSuffix)
		msg.Timestamp = now
		messages = append(messages, *msg)
	}

	addText("status", buildSpecAttemptReplaySummary(attempt))
	if attempt.Plan != nil {
		addText("plan", buildSpecPlanReplaySummary(attempt.Plan))
	}
	if trace, err := readBuildSpecTrace(runRoot); err == nil {
		addText("trace", "Recent Coordinator trace:\n"+tailLines(trace, 24))
	}
	return messages
}

func buildSpecAttemptReplaySummary(attempt *BuildSpecAttempt) string {
	var b strings.Builder
	b.WriteString("Loaded persisted build-spec attempt.\n")
	b.WriteString("- status: " + string(attempt.Status) + "\n")
	b.WriteString("- spec_path: " + attempt.SpecPath + "\n")
	b.WriteString("- attempt_id: " + attempt.AttemptID + "\n")
	b.WriteString(fmt.Sprintf("- created_nodes: %d\n", len(attempt.CreatedNodeIDs)))
	b.WriteString(fmt.Sprintf("- created_edges: %d\n", len(attempt.CreatedEdgeIDs)))
	b.WriteString(fmt.Sprintf("- allocated_domains: %d", len(attempt.AllocatedDomainIDs)))
	if attempt.FailureReason != "" {
		b.WriteString("\n- failure_reason: " + attempt.FailureReason)
	}
	return b.String()
}

func buildSpecPlanReplaySummary(plan *coordinator.PlanResponse) string {
	if plan == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("Persisted Coordinator plan response is loaded.\n")
	b.WriteString(fmt.Sprintf("- domains: %d\n", len(plan.Domains)))
	for _, domain := range plan.Domains {
		b.WriteString(fmt.Sprintf("  - %s: %s\n", domain.DomainID, truncateSingleLine(domain.Description, 100)))
	}
	b.WriteString(fmt.Sprintf("- nodes: %d\n", len(plan.Nodes)))
	for i, node := range plan.Nodes {
		if i >= 12 {
			b.WriteString(fmt.Sprintf("  - ... %d more node(s)\n", len(plan.Nodes)-i))
			break
		}
		b.WriteString(fmt.Sprintf("  - %s [%s]: %s\n", node.ID, node.DomainID, truncateSingleLine(node.TaskSpec, 120)))
	}
	b.WriteString(fmt.Sprintf("- edges: %d", len(plan.Edges)))
	return b.String()
}

func tailLines(text string, maxLines int) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if maxLines <= 0 || len(lines) <= maxLines {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[len(lines)-maxLines:], "\n")
}

func truncateSingleLine(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

func (d *Daemon) TriggerBuildSpec(specPath string, specData []byte) error {
	attempt, err := d.prepareBuildSpecAttempt(specPath, specData)
	if err != nil {
		return err
	}
	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("attempt %s started for %s", attempt.AttemptID, specPath))

	attemptCtx, cancel := context.WithCancel(d.ctx)
	currentAttempt := attempt
	d.buildSpecMu.Lock()
	if d.buildSpecCancel != nil {
		d.buildSpecCancel()
	}
	d.buildSpecCancel = cancel
	d.buildSpecAttempt = attempt
	d.buildSpecMu.Unlock()

	go func() {
		defer func() {
			d.buildSpecMu.Lock()
			if d.buildSpecAttempt == currentAttempt {
				d.buildSpecCancel = nil
				d.buildSpecAttempt = nil
			}
			d.buildSpecMu.Unlock()
		}()

		if err := d.executeBuildSpecAttempt(attemptCtx, attempt, specData); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(attemptCtx.Err(), context.Canceled) {
				_ = appendBuildSpecTrace(d.runState.Root, "attempt canceled")
				d.stopCoordinatorPlanner("build-spec canceled")
				_ = d.finalizeBuildSpecAttemptCancellation(attempt, "build-spec canceled")
				return
			}
			_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("attempt failed: %v", err))
			d.stopCoordinatorPlanner("build-spec failed")
			_ = d.finalizeBuildSpecAttemptFailure(attempt, err.Error(), BuildSpecAttemptFailed)
			log.Printf("daemon: build-spec failed: %v", err)
			d.server.BroadcastStatus(fmt.Sprintf("Planning failed: %v", err), "error")
			return
		}
	}()

	return nil
}

func (d *Daemon) prepareBuildSpecAttempt(specPath string, specData []byte) (*BuildSpecAttempt, error) {
	d.buildSpecMu.Lock()
	defer d.buildSpecMu.Unlock()

	loaded, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil {
		return nil, fmt.Errorf("load build-spec attempt: %w", err)
	}

	snapshot := d.dagManager.Snapshot()
	if loaded != nil {
		if loaded.Status == BuildSpecAttemptActive || loaded.Status == BuildSpecAttemptPaused {
			return nil, fmt.Errorf("build-spec already active for this run")
		}
		if loaded.Status == BuildSpecAttemptPlanning || loaded.Status == BuildSpecAttemptCommitting || loaded.Status == BuildSpecAttemptAllocating {
			if time.Since(loaded.UpdatedAt) <= buildSpecStaleAfter {
				return nil, fmt.Errorf("build-spec attempt already in progress")
			}
			d.server.BroadcastStatus("Stale build-spec attempt detected; clearing partial artifacts...", "warn")
			if err := d.resetBuildSpecArtifactsLocked(); err != nil {
				return nil, err
			}
			_ = deleteBuildSpecAttempt(d.runState.Root)
			loaded = nil
		}
	}

	if loaded == nil && len(snapshot.Nodes) > 0 {
		return nil, fmt.Errorf("current run already has %d DAG node(s) without build-spec attempt metadata; refusing automatic reset", len(snapshot.Nodes))
	}

	attempt := newBuildSpecAttempt(d.runState.RunID, specPath, specData)
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return nil, fmt.Errorf("save build-spec attempt: %w", err)
	}

	_ = d.updateBuildSpecAttemptStatus(attempt, BuildSpecAttemptPlanning, "")
	d.server.BroadcastAgentStatus("coordinator", "Coordinator is planning build-spec output...", "info")
	return attempt, nil
}

func (d *Daemon) executeBuildSpecAttempt(ctx context.Context, attempt *BuildSpecAttempt, specData []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	log.Printf("daemon: scanning project at %s", d.projectRoot)
	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("scan started for project root %s", d.projectRoot))
	d.server.BroadcastAgentStatus("coordinator", "Coordinator is scanning project files...", "info")
	scan, err := coordinator.ScanProject(d.projectRoot)
	if err != nil {
		_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("scan failed: %v", err))
		return fmt.Errorf("project scan failed: %w", err)
	}
	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("scan complete: files=%d modules=%d languages=%v", scan.FileCount, scan.ModuleCount, scan.Languages))

	if err := ctx.Err(); err != nil {
		return err
	}

	log.Printf("daemon: requesting plan from coordinator")
	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("coordinator prompt dispatched; spec_bytes=%d files=%d modules=%d", len(specData), scan.FileCount, scan.ModuleCount))
	d.server.BroadcastAgentStatus("coordinator", "Coordinator is planning a DAG from the spec...", "info")
	req := coordinator.PlanRequest{
		AttemptID:   attempt.AttemptID,
		UserPrompt:  string(specData),
		ProjectRoot: d.projectRoot,
		ProjectScan: scan,
	}
	attempt.PlannerSessionDir = filepath.Join(d.runState.PiSessionsDir, "coordinator", attempt.AttemptID)
	attempt.PlanningArtifactDir = filepath.Join(d.projectRoot, "docs", "aion", "planning", attempt.AttemptID)
	attempt.UpdatedAt = time.Now().UTC()
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return fmt.Errorf("save coordinator attempt metadata: %w", err)
	}
	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("coordinator attempt scoped: session=%s artifacts=%s", attempt.PlannerSessionDir, attempt.PlanningArtifactDir))

	plan, err := d.coordinator.Plan(ctx, req)
	if err != nil {
		_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("coordinator planning error: %v", err))
		return fmt.Errorf("planning failed: %w", err)
	}
	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("coordinator plan received: domains=%d nodes=%d edges=%d", len(plan.Domains), len(plan.Nodes), len(plan.Edges)))
	if err := coordinator.ValidatePlanResponse(plan); err != nil {
		_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("plan validation failed: %v", err))
		return fmt.Errorf("planning validation failed: %w", err)
	}

	attempt.Plan = plan
	attempt.SpecHash = attemptSpecHash(specData)
	attempt.Status = BuildSpecAttemptCommitting
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return fmt.Errorf("save attempt plan: %w", err)
	}
	d.refreshBuildProgress("plan_saved")
	_ = appendBuildSpecTrace(d.runState.Root, "plan saved; committing DAG")
	d.server.BroadcastAgentStatus("coordinator", fmt.Sprintf("Coordinator produced %d domains, %d nodes, %d edges. Committing DAG...", len(plan.Domains), len(plan.Nodes), len(plan.Edges)), "info")
	log.Printf("daemon: committing planned DAG (nodes: %d, edges: %d)", len(plan.Nodes), len(plan.Edges))

	if err := d.commitPlan(plan, attempt); err != nil {
		attempt.Status = BuildSpecAttemptCommitFailed
		attempt.FailureReason = err.Error()
		_ = saveBuildSpecAttempt(d.runState.Root, attempt)
		d.refreshBuildProgress("commit_failed")
		return err
	}
	d.refreshBuildProgress("dag_committed")

	if err := ctx.Err(); err != nil {
		return err
	}

	attempt.Status = BuildSpecAttemptAllocating
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return fmt.Errorf("save attempt allocating: %w", err)
	}
	d.refreshBuildProgress("allocating")
	_ = appendBuildSpecTrace(d.runState.Root, "dag committed; allocating agents")

	log.Printf("daemon: generating initial system instructions")
	d.server.BroadcastAgentStatus("coordinator", "Coordinator is generating agent system instructions...", "info")
	prompts, err := coordinator.GenerateSystemInstructions(plan, "", string(specData))
	if err != nil {
		return fmt.Errorf("generate prompts failed: %w", err)
	}

	log.Printf("daemon: allocating agents...")
	d.server.BroadcastAgentStatus("coordinator", "Coordinator is allocating domain agents...", "info")
	attempt.AllocatedDomainIDs = attempt.AllocatedDomainIDs[:0]
	for _, domain := range plan.Domains {
		attempt.AllocatedDomainIDs = append(attempt.AllocatedDomainIDs, domain.DomainID)
	}
	if err := d.allocator.Allocate(ctx, plan.Domains, prompts); err != nil {
		attempt.Status = BuildSpecAttemptAllocationFailed
		attempt.FailureReason = err.Error()
		_ = saveBuildSpecAttempt(d.runState.Root, attempt)
		_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("allocation failed: %v", err))
		d.server.BroadcastAgentStatus("coordinator", fmt.Sprintf("Coordinator allocation failed: %v", err), "error")
		d.refreshBuildProgress("allocation_failed")
		return fmt.Errorf("allocation failed: %w", err)
	}

	attempt.Status = BuildSpecAttemptActive
	attempt.FailureReason = ""
	now := time.Now().UTC()
	attempt.CompletedAt = &now
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return fmt.Errorf("finalize build-spec attempt: %w", err)
	}
	d.refreshBuildProgress("active")
	_ = appendBuildSpecTrace(d.runState.Root, "allocation complete; build-spec active")
	if handoff, ok := d.coordinator.(interface{ MarkBuildSpecHandoff() }); ok {
		handoff.MarkBuildSpecHandoff()
	}
	d.server.BroadcastAgentStatus("coordinator", "Coordinator build-spec planning and allocation complete.", "ok")
	d.startBuildSpecExecutionMonitor()
	return nil
}

func (d *Daemon) startBuildSpecExecutionMonitor() {
	d.startBuildSpecExecutionMonitorWithInterval(2 * time.Second)
}

func (d *Daemon) startBuildSpecExecutionMonitorWithInterval(checkInterval time.Duration) {
	if d == nil || d.allocator == nil {
		return
	}
	if checkInterval <= 0 {
		checkInterval = 2 * time.Second
	}

	d.executionMonitorMu.Lock()
	if d.executionMonitorActive {
		d.executionMonitorMu.Unlock()
		return
	}
	monitorCtx := d.ctx
	if monitorCtx == nil {
		monitorCtx = context.Background()
	}
	monitorCtx, cancel := context.WithCancel(monitorCtx)
	d.executionMonitorRun++
	run := d.executionMonitorRun
	allocator := d.allocator
	d.executionMonitorCancel = cancel
	d.executionMonitorActive = true
	d.executionMonitorMu.Unlock()

	go func() {
		err := allocator.MonitorExecution(monitorCtx, checkInterval)

		d.executionMonitorMu.Lock()
		if d.executionMonitorRun == run {
			d.executionMonitorCancel = nil
			d.executionMonitorActive = false
		}
		d.executionMonitorMu.Unlock()

		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("daemon: build-spec execution monitor failed: %v", err)
			if d.server != nil {
				d.server.BroadcastStatus(fmt.Sprintf("Execution monitor failed: %v", err), "error")
			}
		}
	}()
}

func (d *Daemon) stopBuildSpecExecutionMonitor() {
	if d == nil {
		return
	}
	d.executionMonitorMu.Lock()
	cancel := d.executionMonitorCancel
	d.executionMonitorRun++
	d.executionMonitorCancel = nil
	d.executionMonitorActive = false
	d.executionMonitorMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (d *Daemon) commitPlan(plan *coordinator.PlanResponse, attempt *BuildSpecAttempt) error {
	d.buildSpecMu.Lock()
	defer d.buildSpecMu.Unlock()

	if d.dagManager == nil {
		return fmt.Errorf("dag manager is not initialized")
	}

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
			return fmt.Errorf("commit node %s: %w", n.ID, err)
		}
		attempt.CreatedNodeIDs = append(attempt.CreatedNodeIDs, n.ID)
	}

	for _, rawEdge := range plan.Edges {
		e := dag.DagEdge{
			FromNode: rawEdge.FromNode,
			ToNode:   rawEdge.ToNode,
			Type:     dag.EdgeDependency,
		}
		if err := d.dagManager.AddEdge(e); err != nil {
			return fmt.Errorf("commit edge %s->%s: %w", e.FromNode, e.ToNode, err)
		}
		attempt.CreatedEdgeIDs = append(attempt.CreatedEdgeIDs, edgeID(e))
	}

	return saveBuildSpecAttempt(d.runState.Root, attempt)
}

func (d *Daemon) cancelBuildSpecAttempt() error {
	d.buildSpecMu.Lock()
	cancel := d.buildSpecCancel
	attempt := d.buildSpecAttempt
	d.buildSpecMu.Unlock()

	if attempt != nil {
		if err := d.finalizeBuildSpecAttemptCancellation(attempt, "build-spec canceled by user"); err != nil {
			return err
		}
	}
	d.stopCoordinatorPlanner("build-spec canceled by user")
	if cancel != nil {
		cancel()
	}
	return nil
}

func (d *Daemon) stopCoordinatorPlanner(reason string) {
	stoppable, ok := d.coordinator.(interface{ StopPlanner() error })
	if !ok {
		return
	}
	_ = appendBuildSpecTrace(d.runState.Root, "stopping coordinator planner: "+reason)
	if err := stoppable.StopPlanner(); err != nil {
		_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("stop coordinator planner failed: %v", err))
		log.Printf("daemon: stop coordinator planner failed: %v", err)
	}
}

func (d *Daemon) finalizeBuildSpecAttemptCancellation(attempt *BuildSpecAttempt, reason string) error {
	if attempt == nil {
		return nil
	}
	if attempt.Status == BuildSpecAttemptCanceled && attempt.CompletedAt != nil {
		return nil
	}
	attempt.Status = BuildSpecAttemptCanceled
	attempt.FailureReason = reason
	now := time.Now().UTC()
	attempt.CanceledAt = &now
	attempt.CompletedAt = &now
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return err
	}
	if err := d.resetBuildSpecArtifactsLocked(); err != nil {
		return err
	}
	d.server.BroadcastStatus("Build-spec planning canceled; partial artifacts cleared.", "warn")
	return nil
}

func (d *Daemon) finalizeBuildSpecAttemptFailure(attempt *BuildSpecAttempt, reason string, status BuildSpecAttemptStatus) error {
	if attempt == nil {
		return nil
	}
	attempt.Status = status
	attempt.FailureReason = reason
	now := time.Now().UTC()
	attempt.CompletedAt = &now
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return err
	}
	return nil
}

func (d *Daemon) updateBuildSpecAttemptStatus(attempt *BuildSpecAttempt, status BuildSpecAttemptStatus, reason string) error {
	if attempt == nil {
		return nil
	}
	attempt.Status = status
	if reason != "" {
		attempt.FailureReason = reason
	}
	return saveBuildSpecAttempt(d.runState.Root, attempt)
}

func (d *Daemon) resetBuildSpecArtifactsLocked() error {
	if d.dagManager != nil {
		d.dagManager.Close()
	}
	if err := clearBuildSpecArtifacts(d.runState); err != nil {
		return err
	}
	dagMgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: d.runState.DagFile,
		WalFilePath:   d.runState.WalFile,
		MaxNodes:      d.config.Orchestrator.MaxActiveNodes,
		FlushDeadline: d.config.FlushDeadline(),
	})
	if err != nil {
		return fmt.Errorf("reset dag manager: %w", err)
	}
	d.dagManager = dagMgr
	d.allocator = NewAllocator(d.config, d.projectRoot, d.hubRouter, dagMgr)
	d.server.ClearAgentCapabilities()
	d.configureAllocatorCallbacks()
	d.server.SetRuntimeSubsystems(dagMgr, d.lockManager, d.stubRegistry, d.memoryStore)
	d.server.ClearHubHistory()
	return nil
}

func (d *Daemon) continueBuildSpecAgents() error {
	d.buildSpecMu.Lock()
	armed := d.buildSpecContinueArmed
	d.buildSpecMu.Unlock()
	if !armed {
		return fmt.Errorf("build-spec continuation must be triggered explicitly by /continue-agents")
	}

	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil {
		return fmt.Errorf("load build-spec attempt: %w", err)
	}
	if attempt == nil {
		return fmt.Errorf("no persisted build-spec attempt to continue")
	}
	if attempt.Plan == nil {
		return fmt.Errorf("no saved build-spec plan for the current attempt")
	}
	if attempt.Status != BuildSpecAttemptActive && attempt.Status != BuildSpecAttemptPaused && attempt.Status != BuildSpecAttemptAllocating {
		return fmt.Errorf("build-spec agents are only resumable after allocation has started")
	}
	live := d.allocator.AgentInfos()
	if len(live) == 0 {
		d.buildSpecMu.Lock()
		d.buildSpecAttempt = attempt
		d.buildSpecMu.Unlock()
		return d.resumeActiveBuildSpecAgents(attempt)
	}
	return d.continueActiveBuildSpecAgents(attempt, live)
}

func (d *Daemon) resumeActiveBuildSpecAgents(attempt *BuildSpecAttempt) error {
	if attempt == nil || attempt.Plan == nil {
		return nil
	}
	if attempt.Status != BuildSpecAttemptActive && attempt.Status != BuildSpecAttemptPaused && attempt.Status != BuildSpecAttemptAllocating {
		return nil
	}
	if len(attempt.Plan.Domains) == 0 {
		return nil
	}
	if len(d.allocator.AgentInfos()) > 0 {
		return nil
	}

	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("resuming build-spec agents from persisted attempt %s", attempt.AttemptID))
	d.server.BroadcastAgentStatus("coordinator", "Resuming persisted build-spec domain agents...", "info")

	specText, err := os.ReadFile(filepath.Join(d.projectRoot, "docs", "build_spec.md"))
	if err != nil {
		return fmt.Errorf("read build spec for resumed agents: %w", err)
	}
	prompts, err := coordinator.GenerateSystemInstructions(attempt.Plan, "", string(specText))
	if err != nil {
		return fmt.Errorf("generate resumed prompts: %w", err)
	}
	if err := d.allocator.AllocateWithOptions(d.ctx, attempt.Plan.Domains, prompts, AllocationOptions{
		Mode:          AllocationModeResume,
		ResumeMessage: buildSpecDomainAgentResumeMessage(attempt),
	}); err != nil {
		return fmt.Errorf("allocate resumed agents: %w", err)
	}
	if attempt.Status == BuildSpecAttemptAllocating || attempt.Status == BuildSpecAttemptPaused {
		attempt.Status = BuildSpecAttemptActive
		attempt.FailureReason = ""
		now := time.Now().UTC()
		attempt.CompletedAt = &now
		_ = saveBuildSpecAttempt(d.runState.Root, attempt)
	}
	d.server.BroadcastAgentStatus("coordinator", "Persisted build-spec agents resumed.", "ok")
	d.refreshBuildProgress("agents_resumed")
	return nil
}

func (d *Daemon) stopBuildSpecAgents() error {
	if d == nil || d.allocator == nil || d.runState == nil {
		return fmt.Errorf("domain-agent runtime is not initialized")
	}
	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil {
		return fmt.Errorf("load build-spec attempt: %w", err)
	}
	if attempt == nil || attempt.Plan == nil {
		return fmt.Errorf("no allocated build-spec agents to stop")
	}
	switch attempt.Status {
	case BuildSpecAttemptActive, BuildSpecAttemptAllocating, BuildSpecAttemptPaused:
	default:
		return fmt.Errorf("build-spec agents cannot be stopped while attempt is %s", attempt.Status)
	}
	if attempt.Status == BuildSpecAttemptPaused && len(d.allocator.AgentInfos()) == 0 {
		return nil
	}

	_ = appendBuildSpecTrace(d.runState.Root, "user requested domain-agent pause")
	d.server.BroadcastTransientStatus("Stopping active domain agents...", "info")
	d.stopBuildSpecExecutionMonitor()
	d.allocator.StopAll()
	if latest, loadErr := loadBuildSpecAttempt(d.runState.Root); loadErr == nil && latest != nil {
		attempt = latest
	}

	d.buildSpecMu.Lock()
	attempt.Status = BuildSpecAttemptPaused
	attempt.FailureReason = ""
	attempt.UpdatedAt = time.Now().UTC()
	d.buildSpecAttempt = attempt
	err = saveBuildSpecAttempt(d.runState.Root, attempt)
	d.buildSpecMu.Unlock()
	if err != nil {
		return fmt.Errorf("persist paused build-spec attempt: %w", err)
	}
	d.server.BroadcastAgentStatus("coordinator", "Domain-agent execution paused. Use /continue-agents to resume.", "warn")
	d.refreshBuildProgress("agents_paused")
	return nil
}

func buildSpecDomainAgentResumeMessage(attempt *BuildSpecAttempt) string {
	attemptID := ""
	if attempt != nil {
		attemptID = attempt.AttemptID
	}
	return strings.TrimSpace(fmt.Sprintf(`Resume your existing Aion domain-agent Pi session for build-spec execution.

Build-spec attempt: %s

The server was restarted or the agent set was explicitly continued. Keep your existing context and continue pending DAG work for your assigned domain.
Your current working directory is the isolated writable view for your assigned domain. Other domain and kernel runtime paths are not mounted.
Do not re-run the original onboarding/system prompt. Use orchestrator-cli to read the DAG, acquire locks, update task status, create stubs, and coordinate through the context hub.
If you are unsure what is pending, inspect the DAG first and continue only work assigned to you.`, attemptID))
}

func (d *Daemon) continueActiveBuildSpecAgents(attempt *BuildSpecAttempt, live []AgentInfo) error {
	reviveIDs := d.buildSpecFailedAgentIDs(attempt, live)
	if len(reviveIDs) == 0 {
		d.server.BroadcastStatus("No failed build-spec agents need continuation.", "ok")
		return nil
	}

	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("continuing failed build-spec agents: %s", strings.Join(reviveIDs, ", ")))
	d.server.BroadcastStatus(fmt.Sprintf("Continuing %d failed build-spec agent(s)...", len(reviveIDs)), "info")

	for _, agentID := range reviveIDs {
		if err := d.allocator.ReviveAgent(d.ctx, agentID); err != nil {
			return fmt.Errorf("revive build-spec agent %s: %w", agentID, err)
		}
	}
	d.server.BroadcastStatus("Failed build-spec agents continued.", "ok")
	d.refreshBuildProgress("agents_continued")
	return nil
}

func (d *Daemon) buildSpecFailedAgentIDs(attempt *BuildSpecAttempt, live []AgentInfo) []string {
	var reviveIDs []string
	for _, info := range live {
		state := strings.TrimSpace(info.State)
		if state == "" && attempt != nil {
			if persisted, ok := attempt.AgentStates[info.AgentID]; ok {
				state = persisted.State
			}
		}
		if isFailedBuildSpecAgentState(state) {
			reviveIDs = append(reviveIDs, info.AgentID)
		}
	}
	sort.Strings(reviveIDs)
	return reviveIDs
}

func isFailedBuildSpecAgentState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "crashed", "stopped", "failed", "failed_terminal":
		return true
	default:
		return false
	}
}

func (d *Daemon) buildSpecStatus() string {
	d.buildSpecMu.Lock()
	defer d.buildSpecMu.Unlock()

	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil || attempt == nil {
		return "No active build-spec attempt"
	}
	b, _ := json.Marshal(attempt)
	return string(b)
}

func (d *Daemon) buildSpecPlan() (string, error) {
	d.buildSpecMu.Lock()
	defer d.buildSpecMu.Unlock()

	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil {
		return "", err
	}
	if attempt == nil || attempt.Plan == nil {
		return "", fmt.Errorf("no saved build-spec plan for the current attempt")
	}
	b, err := json.MarshalIndent(attempt.Plan, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (d *Daemon) buildSpecTrace() (string, error) {
	if d.runState == nil {
		return "", fmt.Errorf("no active run")
	}
	return readBuildSpecTrace(d.runState.Root)
}

func attemptSpecHash(specData []byte) string {
	sum := sha256.Sum256(specData)
	return hex.EncodeToString(sum[:])
}

func edgeID(edge dag.DagEdge) string {
	return edge.FromNode + "->" + edge.ToNode
}
