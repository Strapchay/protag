package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	coordination "aion-coordination"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
)

const maxAcceptanceRounds = 2

type buildAcceptanceInput struct {
	Type            string                    `json:"type"`
	AttemptID       string                    `json:"attempt_id"`
	ReviewRound     int                       `json:"review_round"`
	BuildSpec       string                    `json:"build_spec"`
	Plan            *coordinator.PlanResponse `json:"accepted_plan"`
	DAG             dag.DagData               `json:"dag"`
	Contracts       []coordination.Contract   `json:"contracts"`
	Integration     *IntegrationRunStatus     `json:"integration,omitempty"`
	ValidationRules []string                  `json:"validation_rules"`
}

type buildAcceptanceRequirement struct {
	ID       string   `json:"id"`
	Status   string   `json:"status"`
	Evidence string   `json:"evidence"`
	NodeIDs  []string `json:"node_ids,omitempty"`
}

type buildAcceptanceRework struct {
	NodeID      string   `json:"node_id"`
	Instruction string   `json:"instruction"`
	TargetFiles []string `json:"target_files,omitempty"`
}

type buildAcceptanceCandidate struct {
	Type         string                       `json:"type"`
	AttemptID    string                       `json:"attempt_id"`
	Verdict      string                       `json:"verdict"`
	Summary      string                       `json:"summary"`
	Requirements []buildAcceptanceRequirement `json:"requirements"`
	ReopenNodes  []buildAcceptanceRework      `json:"reopen_nodes"`
	AddNodes     []coordinator.TaskNode       `json:"add_nodes"`
	AddEdges     []coordinator.TaskEdge       `json:"add_edges"`
	Risks        []string                     `json:"risks"`
}

type acceptancePlanPatch struct {
	reopen    []buildAcceptanceRework
	addNodes  []coordinator.TaskNode
	addEdges  []coordinator.TaskEdge
	candidate *buildAcceptanceCandidate
	resultDAG *dag.DagData
}

func buildAcceptanceTemplate(attemptID string) []byte {
	template := buildAcceptanceCandidate{
		Type:      "aion_build_acceptance",
		AttemptID: attemptID,
		Verdict:   "complete",
		Summary:   "Replace with the acceptance conclusion.",
		Requirements: []buildAcceptanceRequirement{{
			ID: "R1", Status: "met", Evidence: "Replace with concrete source or integration evidence.", NodeIDs: []string{"replace-node-id"},
		}},
		Risks: []string{"Replace with a residual risk or remove this entry."},
	}
	data, _ := json.MarshalIndent(template, "", "  ")
	return data
}

func (d *Daemon) reviewBuildAcceptance(attempt *BuildSpecAttempt) (bool, error) {
	if d == nil || attempt == nil || attempt.Plan == nil {
		return true, nil
	}
	if attempt.AcceptanceStatus == "passed" {
		return true, nil
	}
	if attempt.AcceptanceStatus == "attention" {
		return false, nil
	}
	reviewer, ok := d.coordinator.(coordinator.BuildReviewer)
	if !ok {
		_ = appendBuildSpecTrace(d.runState.Root, "acceptance review skipped: coordinator does not implement BuildReviewer")
		return true, nil
	}
	d.acceptanceMu.Lock()
	defer d.acceptanceMu.Unlock()

	artifactDir := filepath.Join(d.projectRoot, "docs", "aion", "acceptance", attempt.AttemptID)
	sandboxDir := "/workspace/docs/aion/acceptance/" + attempt.AttemptID
	sessionDir := filepath.Join(d.runState.PiSessionsDir, "review", attempt.AttemptID)
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return false, err
	}
	specData, err := os.ReadFile(d.buildSpecPath(attempt))
	if err != nil {
		return d.acceptanceAttention(attempt, fmt.Errorf("read build spec: %w", err))
	}
	var contracts []coordination.Contract
	if d.coordinationManager != nil {
		contracts, _ = d.coordinationManager.List(context.Background(), coordination.Principal{Role: coordination.RoleAdmin})
	}
	input := buildAcceptanceInput{
		Type:        "aion_build_acceptance_input",
		AttemptID:   attempt.AttemptID,
		ReviewRound: attempt.AcceptanceRounds + 1,
		BuildSpec:   string(specData),
		Plan:        attempt.Plan,
		DAG:         *d.dagManager.Snapshot(),
		Contracts:   contracts,
		ValidationRules: []string{
			"verdict complete requires every build-spec requirement to be implemented and evidenced",
			"verdict rework must name concrete existing nodes to reopen or ownership-safe new nodes",
			"existing domain and path ownership is immutable",
			"add_nodes target files must stay beneath their domain assigned_paths",
			"do not invent documentation-only or duplicate work",
			"report only real missing or unverified requirements",
		},
	}
	if d.integrationRunner != nil {
		integration := d.integrationRunner.Status()
		input.Integration = &integration
	}
	inputData, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return d.acceptanceAttention(attempt, err)
	}
	inputData = append(inputData, '\n')
	if err := os.WriteFile(filepath.Join(artifactDir, "acceptance_input.json"), inputData, 0o644); err != nil {
		return d.acceptanceAttention(attempt, err)
	}
	if err := os.WriteFile(filepath.Join(artifactDir, "acceptance_template.json"), buildAcceptanceTemplate(attempt.AttemptID), 0o644); err != nil {
		return d.acceptanceAttention(attempt, err)
	}

	attempt.AcceptanceStatus = "running"
	attempt.AcceptanceSummary = ""
	d.saveBuildSpecAttempt(attempt)
	d.server.BroadcastAgentStatus("coordinator", "Build acceptance review started against the assembled implementation.", "info")

	ctx := d.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	result, reviewErr := reviewer.ReviewBuild(ctx, coordinator.BuildReviewRequest{
		AttemptID: attempt.AttemptID, ArtifactDir: artifactDir, SandboxDir: sandboxDir, SessionDir: sessionDir,
	})
	if reviewErr != nil {
		return d.acceptanceAttention(attempt, fmt.Errorf("acceptance review failed: %w", reviewErr))
	}
	specAfter, err := os.ReadFile(d.buildSpecPath(attempt))
	if err != nil || hashBytes(specAfter) != attempt.SpecHash {
		return d.acceptanceAttention(attempt, fmt.Errorf("acceptance review modified the build spec"))
	}
	candidateData, err := os.ReadFile(result.OutputPath)
	if err != nil {
		return d.acceptanceAttention(attempt, fmt.Errorf("read acceptance review: %w", err))
	}
	patch, validateErr := d.validateAcceptanceCandidate(attempt, candidateData)
	if validateErr != nil {
		repairResult, repairErr := reviewer.ReviewBuild(ctx, coordinator.BuildReviewRequest{
			AttemptID: attempt.AttemptID, ArtifactDir: artifactDir, SandboxDir: sandboxDir, SessionDir: sessionDir,
			Repair: true, ValidationError: validateErr.Error(),
		})
		if repairErr != nil {
			return d.acceptanceAttention(attempt, fmt.Errorf("acceptance review repair failed: %w", repairErr))
		}
		candidateData, err = os.ReadFile(repairResult.OutputPath)
		if err != nil {
			return d.acceptanceAttention(attempt, fmt.Errorf("read repaired acceptance review: %w", err))
		}
		patch, validateErr = d.validateAcceptanceCandidate(attempt, candidateData)
		if validateErr != nil {
			return d.acceptanceAttention(attempt, fmt.Errorf("acceptance review invalid after repair: %w", validateErr))
		}
	}
	attempt.AcceptanceSummary = patch.candidate.Summary
	attempt.AcceptanceReviewPath = filepath.Join(artifactDir, "acceptance_output.json")
	if patch.candidate.Verdict == "complete" {
		attempt.AcceptanceStatus = "passed"
		d.saveBuildSpecAttempt(attempt)
		_ = appendBuildSpecTrace(d.runState.Root, "acceptance review passed: "+patch.candidate.Summary)
		d.server.BroadcastAgentStatus("coordinator", "Build acceptance review passed: "+patch.candidate.Summary, "ok")
		return true, nil
	}
	if attempt.AcceptanceRounds >= maxAcceptanceRounds {
		return d.acceptanceAttention(attempt, fmt.Errorf("acceptance review still reports missing requirements after %d rework rounds: %s", attempt.AcceptanceRounds, patch.candidate.Summary))
	}
	if err := d.applyAcceptancePatch(attempt, patch); err != nil {
		return d.acceptanceAttention(attempt, err)
	}
	attempt.AcceptanceStatus = "rework"
	attempt.AcceptanceRounds++
	attempt.Status = BuildSpecAttemptActive
	attempt.FailureReason = ""
	attempt.CompletedAt = nil
	d.saveBuildSpecAttempt(attempt)
	_ = appendBuildSpecTrace(d.runState.Root, fmt.Sprintf("acceptance review requested rework (round %d): %s", attempt.AcceptanceRounds, patch.candidate.Summary))
	d.server.BroadcastStatus(fmt.Sprintf("Acceptance review requested rework: %s", patch.candidate.Summary), "warn")
	d.refreshBuildProgress("acceptance_rework")
	if len(d.allocator.AgentInfos()) == 0 {
		if err := d.resumeActiveBuildSpecAgents(attempt); err != nil {
			return d.acceptanceAttention(attempt, err)
		}
	}
	d.startBuildSpecExecutionMonitor()
	return false, nil
}

func (d *Daemon) buildSpecPath(attempt *BuildSpecAttempt) string {
	if attempt == nil {
		return ""
	}
	if filepath.IsAbs(attempt.SpecPath) {
		return attempt.SpecPath
	}
	return filepath.Join(d.projectRoot, attempt.SpecPath)
}

func (d *Daemon) saveBuildSpecAttempt(attempt *BuildSpecAttempt) {
	if attempt == nil || d.runState == nil {
		return
	}
	d.buildSpecMu.Lock()
	d.buildSpecAttempt = attempt
	err := saveBuildSpecAttempt(d.runState.Root, attempt)
	d.buildSpecMu.Unlock()
	if err != nil {
		log.Printf("daemon: save acceptance state: %v", err)
	}
}

func (d *Daemon) acceptanceAttention(attempt *BuildSpecAttempt, cause error) (bool, error) {
	attempt.AcceptanceStatus = "attention"
	attempt.Status = BuildSpecAttemptPaused
	attempt.FailureReason = "acceptance review requires attention: " + cause.Error()
	d.saveBuildSpecAttempt(attempt)
	if d.allocator != nil {
		d.allocator.StopAll()
	}
	if d.server != nil {
		d.server.BroadcastAgentStatus("coordinator", attempt.FailureReason, "error")
	}
	_ = appendBuildSpecTrace(d.runState.Root, attempt.FailureReason)
	return false, cause
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (d *Daemon) validateAcceptanceCandidate(attempt *BuildSpecAttempt, data []byte) (*acceptancePlanPatch, error) {
	if attempt == nil || attempt.Plan == nil {
		return nil, fmt.Errorf("accepted build plan is unavailable")
	}
	var candidate buildAcceptanceCandidate
	if err := json.Unmarshal(data, &candidate); err != nil {
		return nil, fmt.Errorf("acceptance output is not valid JSON: %w", err)
	}
	if candidate.Type != "aion_build_acceptance" || candidate.AttemptID != attempt.AttemptID {
		return nil, fmt.Errorf("acceptance output identity does not match the active attempt")
	}
	if candidate.Verdict != "complete" && candidate.Verdict != "rework" {
		return nil, fmt.Errorf("acceptance verdict must be complete or rework")
	}
	if strings.TrimSpace(candidate.Summary) == "" || strings.HasPrefix(strings.TrimSpace(candidate.Summary), "Replace with") {
		return nil, fmt.Errorf("acceptance summary is required")
	}
	if len(candidate.Requirements) == 0 {
		return nil, fmt.Errorf("acceptance requirements are required")
	}
	for _, requirement := range candidate.Requirements {
		switch requirement.Status {
		case "met", "missing", "unclear":
		default:
			return nil, fmt.Errorf("requirement %q has invalid status %q", requirement.ID, requirement.Status)
		}
		if strings.TrimSpace(requirement.Evidence) == "" || strings.HasPrefix(strings.TrimSpace(requirement.Evidence), "Replace with") {
			return nil, fmt.Errorf("requirement %q requires concrete evidence", requirement.ID)
		}
	}
	if candidate.Verdict == "complete" {
		if len(candidate.ReopenNodes) > 0 || len(candidate.AddNodes) > 0 || len(candidate.AddEdges) > 0 {
			return nil, fmt.Errorf("complete verdict cannot request rework")
		}
		return &acceptancePlanPatch{candidate: &candidate}, nil
	}
	if len(candidate.ReopenNodes) == 0 && len(candidate.AddNodes) == 0 {
		return nil, fmt.Errorf("rework verdict requires reopened or new nodes")
	}

	patch := &acceptancePlanPatch{candidate: &candidate, reopen: candidate.ReopenNodes, addNodes: candidate.AddNodes, addEdges: candidate.AddEdges}
	current := d.dagManager.Snapshot()
	result := &dag.DagData{Header: current.Header, Nodes: append([]dag.DagNode(nil), current.Nodes...), Edges: append([]dag.DagEdge(nil), current.Edges...)}
	nodesByID := make(map[string]dag.DagNode, len(result.Nodes))
	for _, node := range result.Nodes {
		nodesByID[node.ID] = node
	}
	planNodes := make(map[string]coordinator.TaskNode, len(attempt.Plan.Nodes))
	for _, node := range attempt.Plan.Nodes {
		planNodes[node.ID] = node
	}
	domains := make(map[string]coordinator.Domain, len(attempt.Plan.Domains))
	owned := make(map[string][]string, len(attempt.Plan.Domains))
	for _, domain := range attempt.Plan.Domains {
		domains[domain.DomainID] = domain
		owned[domain.DomainID] = append([]string(nil), domain.AssignedPaths...)
	}
	now := time.Now().UnixMilli()
	reopened := make(map[string]bool)
	for _, rework := range candidate.ReopenNodes {
		planNode, ok := planNodes[rework.NodeID]
		if !ok {
			return nil, fmt.Errorf("rework references unknown plan node %q", rework.NodeID)
		}
		node, ok := nodesByID[rework.NodeID]
		if !ok {
			return nil, fmt.Errorf("rework references unknown DAG node %q", rework.NodeID)
		}
		if strings.TrimSpace(rework.Instruction) == "" || strings.HasPrefix(strings.TrimSpace(rework.Instruction), "Replace with") {
			return nil, fmt.Errorf("rework node %q requires a concrete instruction", rework.NodeID)
		}
		targets := rework.TargetFiles
		if len(targets) == 0 {
			targets = planNode.TargetFiles
		}
		for _, target := range targets {
			if !pathOwnedByDomain(target, owned[planNode.DomainID]) {
				return nil, fmt.Errorf("rework node %q target %q is outside immutable domain ownership", rework.NodeID, target)
			}
		}
		taskSpec := strings.TrimSpace(planNode.TaskSpec)
		if taskSpec == "" {
			taskSpec = strings.TrimSpace(node.TaskSpec)
		}
		taskSpec += "\n\nACCEPTANCE REWORK:\n" + strings.TrimSpace(rework.Instruction)
		for index := range result.Nodes {
			if result.Nodes[index].ID != rework.NodeID {
				continue
			}
			result.Nodes[index].TaskSpec = taskSpec
			result.Nodes[index].TargetFiles = append([]string(nil), targets...)
			result.Nodes[index].Status = dag.StatusPending
			result.Nodes[index].AssignedAgent = ""
			result.Nodes[index].StartedAt = 0
			result.Nodes[index].CompletedAt = 0
			result.Nodes[index].UpdatedAt = now
		}
		planNode.TaskSpec = taskSpec
		planNode.TargetFiles = append([]string(nil), targets...)
		attempt.Plan.Nodes = replacePlanNode(attempt.Plan.Nodes, planNode)
		reopened[rework.NodeID] = true
	}
	allIDs := make(map[string]bool, len(result.Nodes))
	for _, node := range result.Nodes {
		allIDs[node.ID] = true
	}
	for _, proposed := range candidate.AddNodes {
		if strings.TrimSpace(proposed.ID) == "" || allIDs[proposed.ID] || reopened[proposed.ID] {
			return nil, fmt.Errorf("new acceptance node ID %q is empty or already exists", proposed.ID)
		}
		if _, ok := domains[proposed.DomainID]; !ok {
			return nil, fmt.Errorf("new acceptance node %q references unknown domain %q", proposed.ID, proposed.DomainID)
		}
		if len(proposed.TargetFiles) == 0 {
			return nil, fmt.Errorf("new acceptance node %q requires target files", proposed.ID)
		}
		for _, target := range proposed.TargetFiles {
			if !pathOwnedByDomain(target, owned[proposed.DomainID]) {
				return nil, fmt.Errorf("new acceptance node %q target %q is outside immutable domain ownership", proposed.ID, target)
			}
		}
		result.Nodes = append(result.Nodes, dag.DagNode{
			ID: proposed.ID, DomainID: proposed.DomainID, TaskSpec: proposed.TaskSpec, Status: dag.StatusPending,
			AssignedAgent: "agent-" + proposed.DomainID, TargetFiles: append([]string(nil), proposed.TargetFiles...),
			Priority: proposed.Priority, CreatedAt: now, UpdatedAt: now,
		})
		allIDs[proposed.ID] = true
	}
	for _, edge := range candidate.AddEdges {
		if !allIDs[edge.FromNode] || !allIDs[edge.ToNode] {
			return nil, fmt.Errorf("new acceptance edge %s->%s references an unknown node", edge.FromNode, edge.ToNode)
		}
		if edge.FromNode == edge.ToNode {
			return nil, fmt.Errorf("new acceptance edge %s->%s is self-referential", edge.FromNode, edge.ToNode)
		}
		exists := false
		for _, currentEdge := range result.Edges {
			if currentEdge.FromNode == edge.FromNode && currentEdge.ToNode == edge.ToNode {
				exists = true
				break
			}
		}
		if !exists {
			result.Edges = append(result.Edges, dag.DagEdge{FromNode: edge.FromNode, ToNode: edge.ToNode, Type: dag.EdgeDependency})
		}
	}
	if err := dag.ValidateAll(result); err != nil {
		return nil, fmt.Errorf("acceptance DAG validation failed: %w", err)
	}
	patch.resultDAG = result
	return patch, nil
}

func replacePlanNode(nodes []coordinator.TaskNode, replacement coordinator.TaskNode) []coordinator.TaskNode {
	out := append([]coordinator.TaskNode(nil), nodes...)
	for index := range out {
		if out[index].ID == replacement.ID {
			out[index] = replacement
			break
		}
	}
	return out
}

func (d *Daemon) applyAcceptancePatch(attempt *BuildSpecAttempt, patch *acceptancePlanPatch) error {
	if patch == nil || patch.candidate == nil || patch.resultDAG == nil {
		return fmt.Errorf("acceptance patch is incomplete")
	}
	if err := d.dagManager.BulkLoad(patch.resultDAG); err != nil {
		return err
	}
	existingNodes := make(map[string]bool, len(attempt.Plan.Nodes))
	for _, node := range attempt.Plan.Nodes {
		existingNodes[node.ID] = true
	}
	for _, node := range patch.addNodes {
		if existingNodes[node.ID] {
			continue
		}
		attempt.Plan.Nodes = append(attempt.Plan.Nodes, node)
		attempt.CreatedNodeIDs = append(attempt.CreatedNodeIDs, node.ID)
		existingNodes[node.ID] = true
	}
	existingEdges := make(map[string]bool, len(attempt.Plan.Edges))
	for _, edge := range attempt.Plan.Edges {
		existingEdges[edge.FromNode+"\x00"+edge.ToNode] = true
	}
	for _, edge := range patch.addEdges {
		key := edge.FromNode + "\x00" + edge.ToNode
		if existingEdges[key] {
			continue
		}
		attempt.Plan.Edges = append(attempt.Plan.Edges, edge)
		attempt.CreatedEdgeIDs = append(attempt.CreatedEdgeIDs, edge.FromNode+"->"+edge.ToNode)
		existingEdges[key] = true
	}
	return nil
}
