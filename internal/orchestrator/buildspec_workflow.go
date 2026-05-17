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
	"time"

	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
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
		if loaded.Status == BuildSpecAttemptActive {
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
		UserPrompt:  string(specData),
		ProjectRoot: d.projectRoot,
		ProjectScan: scan,
	}

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
	_ = appendBuildSpecTrace(d.runState.Root, "plan saved; committing DAG")
	d.server.BroadcastAgentStatus("coordinator", fmt.Sprintf("Coordinator produced %d domains, %d nodes, %d edges. Committing DAG...", len(plan.Domains), len(plan.Nodes), len(plan.Edges)), "info")
	log.Printf("daemon: committing planned DAG (nodes: %d, edges: %d)", len(plan.Nodes), len(plan.Edges))

	if err := d.commitPlan(plan, attempt); err != nil {
		attempt.Status = BuildSpecAttemptCommitFailed
		attempt.FailureReason = err.Error()
		_ = saveBuildSpecAttempt(d.runState.Root, attempt)
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	attempt.Status = BuildSpecAttemptAllocating
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return fmt.Errorf("save attempt allocating: %w", err)
	}
	_ = appendBuildSpecTrace(d.runState.Root, "dag committed; allocating agents")

	log.Printf("daemon: generating initial system instructions")
	d.server.BroadcastAgentStatus("coordinator", "Coordinator is generating agent system instructions...", "info")
	prompts, err := coordinator.GenerateSystemInstructions(plan, "", string(specData))
	if err != nil {
		return fmt.Errorf("generate prompts failed: %w", err)
	}

	log.Printf("daemon: allocating agents...")
	d.server.BroadcastAgentStatus("coordinator", "Coordinator is allocating domain agents...", "info")
	if err := d.allocator.Allocate(ctx, plan.Domains, prompts); err != nil {
		attempt.Status = BuildSpecAttemptAllocationFailed
		attempt.FailureReason = err.Error()
		_ = saveBuildSpecAttempt(d.runState.Root, attempt)
		_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("allocation failed: %v", err))
		d.server.BroadcastAgentStatus("coordinator", fmt.Sprintf("Coordinator allocation failed: %v", err), "error")
		return fmt.Errorf("allocation failed: %w", err)
	}

	attempt.Status = BuildSpecAttemptActive
	attempt.FailureReason = ""
	now := time.Now().UTC()
	attempt.CompletedAt = &now
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		return fmt.Errorf("finalize build-spec attempt: %w", err)
	}
	_ = appendBuildSpecTrace(d.runState.Root, "allocation complete; build-spec active")
	if handoff, ok := d.coordinator.(interface{ MarkBuildSpecHandoff() }); ok {
		handoff.MarkBuildSpecHandoff()
	}
	d.server.BroadcastAgentStatus("coordinator", "Coordinator build-spec planning and allocation complete.", "ok")
	return nil
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
	d.configureAllocatorCallbacks()
	d.server.SetRuntimeSubsystems(dagMgr, d.lockManager, d.stubRegistry, d.memoryStore)
	d.server.ClearHubHistory()
	return nil
}

func (d *Daemon) continueBuildSpecAgents() error {
	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil {
		return fmt.Errorf("load build-spec attempt: %w", err)
	}
	if attempt == nil {
		return fmt.Errorf("no persisted build-spec attempt to continue")
	}
	if attempt.Status != BuildSpecAttemptActive && attempt.Status != BuildSpecAttemptAllocating {
		return fmt.Errorf("build-spec agents are only resumable after allocation has started")
	}
	return d.resumeActiveBuildSpecAgents(attempt)
}

func (d *Daemon) resumeActiveBuildSpecAgents(attempt *BuildSpecAttempt) error {
	if attempt == nil || attempt.Plan == nil {
		return nil
	}
	if attempt.Status != BuildSpecAttemptActive && attempt.Status != BuildSpecAttemptAllocating {
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
	if err := d.allocator.Allocate(d.ctx, attempt.Plan.Domains, prompts); err != nil {
		return fmt.Errorf("allocate resumed agents: %w", err)
	}
	if attempt.Status == BuildSpecAttemptAllocating {
		attempt.Status = BuildSpecAttemptActive
		attempt.FailureReason = ""
		now := time.Now().UTC()
		attempt.CompletedAt = &now
		_ = saveBuildSpecAttempt(d.runState.Root, attempt)
	}
	d.server.BroadcastAgentStatus("coordinator", "Persisted build-spec agents resumed.", "ok")
	return nil
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
