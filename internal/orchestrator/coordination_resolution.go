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
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
)

type resolutionInputArtifact struct {
	Type            string                       `json:"type"`
	Resolution      coordination.Resolution      `json:"resolution"`
	Contract        coordination.Contract        `json:"contract"`
	TermsHistory    []coordination.ContractTerms `json:"terms_history"`
	BuildSpec       string                       `json:"build_spec"`
	Ownership       []coordinator.Domain         `json:"immutable_domain_ownership"`
	AffectedNodes   []dag.DagNode                `json:"affected_nodes"`
	RelevantEdges   []dag.DagEdge                `json:"relevant_edges"`
	PublicArtifact  *resolutionPublicArtifact    `json:"public_contract_artifact,omitempty"`
	ExpandedDAG     *dag.DagData                 `json:"expanded_dag,omitempty"`
	OutputSchema    map[string]interface{}       `json:"output_schema"`
	ValidationRules []string                     `json:"validation_rules"`
}

type resolutionPublicArtifact struct {
	MediaType string `json:"media_type,omitempty"`
	Content   string `json:"content"`
}

type resolutionOutputArtifact struct {
	Type             string                 `json:"type"`
	ResolutionID     string                 `json:"resolution_id"`
	Rationale        string                 `json:"rationale"`
	Evidence         []string               `json:"evidence"`
	Actions          []resolutionAction     `json:"actions"`
	RequestedContext []string               `json:"requested_context,omitempty"`
	Terms            resolutionTermsOutput  `json:"terms"`
	AddNodes         []coordinator.TaskNode `json:"add_nodes,omitempty"`
	AddEdges         []coordinator.TaskEdge `json:"add_edges,omitempty"`
	RemoveEdges      []coordinator.TaskEdge `json:"remove_edges,omitempty"`
	Risks            []string               `json:"risks,omitempty"`
}

type resolutionAction struct {
	Kind    string `json:"kind"`
	Summary string `json:"summary"`
}

type resolutionTermsOutput struct {
	Details             coordination.ContractDetails `json:"details"`
	ProducerObligations []string                     `json:"producer_obligations,omitempty"`
	ConsumerObligations []string                     `json:"consumer_obligations,omitempty"`
	Tradeoffs           []string                     `json:"tradeoffs,omitempty"`
}

func (d *Daemon) processNextCoordinationResolution() {
	if d == nil || d.coordinationManager == nil || d.ctx == nil {
		return
	}
	d.coordinationResolutionMu.Lock()
	if d.coordinationResolutionOn {
		d.coordinationResolutionMu.Unlock()
		return
	}
	d.coordinationResolutionOn = true
	d.coordinationResolutionMu.Unlock()
	go func() {
		defer func() {
			d.coordinationResolutionMu.Lock()
			d.coordinationResolutionOn = false
			d.coordinationResolutionMu.Unlock()
		}()
		if err := d.runNextCoordinationResolution(); err != nil && d.ctx.Err() == nil {
			log.Printf("coordination: reconciliation worker: %v", err)
		}
	}()
}

func (d *Daemon) runNextCoordinationResolution() error {
	leaseFor := time.Duration(d.config.Coordination.LeaseTimeoutSec) * time.Second
	resolution, err := d.coordinationManager.ClaimNextResolution(d.ctx, leaseFor)
	if err != nil || resolution == nil {
		return err
	}
	if resolution.Status == coordination.ResolutionApplying {
		return d.recoverResolutionApply(resolution)
	}
	if resolution.Status == coordination.ResolutionValidated {
		return d.recoverValidatedResolution(resolution)
	}
	resolver, ok := d.coordinator.(coordinator.ResolutionCoordinator)
	if !ok {
		_, failErr := d.coordinationManager.AwaitResolutionUser(d.ctx, resolution.ID, resolution.LeaseToken, "configured coordinator cannot reconcile sealed artifacts")
		return failErr
	}

	artifactDir := filepath.Join(d.runState.CoordinationDir, "resolutions", resolution.ID)
	sessionDir := filepath.Join(d.runState.PiSessionsDir, "resolution", resolution.ID)
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	input, err := d.buildResolutionInput(resolution, false)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	inputData, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	inputData = append(inputData, '\n')
	inputPath := filepath.Join(artifactDir, "resolution_input.json")
	if err := os.WriteFile(inputPath, inputData, 0o600); err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	if templateData, templateErr := resolutionTemplateData(input); templateErr == nil {
		if err := os.WriteFile(filepath.Join(artifactDir, "resolution_template.json"), templateData, 0o600); err != nil {
			return d.failResolutionProcess(resolution, err)
		}
	}
	if _, err := d.coordinationManager.RecordResolutionInput(d.ctx, resolution.ID, resolution.LeaseToken, inputData); err != nil {
		return d.failResolutionProcess(resolution, err)
	}

	result, err := d.resolveWithLease(resolver, resolution, coordinator.ResolutionRequest{
		ResolutionID: resolution.ID, ArtifactDir: artifactDir, SessionDir: sessionDir,
	}, leaseFor)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	candidateData, err := os.ReadFile(result.OutputPath)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	candidate, candidateDAG, err := d.validateResolutionCandidate(resolution, candidateData)
	if err != nil {
		failed, recordErr := d.coordinationManager.RecordResolutionValidationFailure(d.ctx, resolution.ID, resolution.LeaseToken, err)
		if recordErr != nil {
			return recordErr
		}
		if failed.Status == coordination.ResolutionAwaitingUser {
			d.drainCoordinationEffects()
			return nil
		}
		result, repairErr := d.resolveWithLease(resolver, resolution, coordinator.ResolutionRequest{
			ResolutionID: resolution.ID, ArtifactDir: artifactDir, SessionDir: sessionDir,
			Repair: true, ValidationError: err.Error(),
		}, leaseFor)
		if repairErr != nil {
			return d.failResolutionProcess(resolution, repairErr)
		}
		candidateData, repairErr = os.ReadFile(result.OutputPath)
		if repairErr == nil {
			candidate, candidateDAG, repairErr = d.validateResolutionCandidate(resolution, candidateData)
		}
		if repairErr != nil {
			_, recordErr = d.coordinationManager.RecordResolutionValidationFailure(d.ctx, resolution.ID, resolution.LeaseToken, repairErr)
			d.drainCoordinationEffects()
			return recordErr
		}
	}
	if len(candidate.RequestedContext) > 0 {
		return d.handleResolutionContextRequest(resolver, resolution, candidate, artifactDir, sessionDir, leaseFor)
	}
	return d.applyResolutionCandidate(resolution, candidate, candidateDAG, candidateData)
}

func (d *Daemon) handleResolutionContextRequest(resolver coordinator.ResolutionCoordinator, resolution *coordination.Resolution, candidate *resolutionOutputArtifact, artifactDir, sessionDir string, leaseFor time.Duration) error {
	if _, err := d.coordinationManager.RecordContextExpansion(d.ctx, resolution.ID, resolution.LeaseToken); err != nil {
		_, awaitErr := d.coordinationManager.AwaitResolutionUser(d.ctx, resolution.ID, resolution.LeaseToken, "coordinator requested context beyond the bounded public expansion")
		d.drainCoordinationEffects()
		return awaitErr
	}
	input, err := d.buildResolutionInput(resolution, true)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	data, _ := json.MarshalIndent(input, "", "  ")
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(artifactDir, "resolution_input.json"), data, 0o600); err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	if _, err := d.coordinationManager.RecordResolutionInput(d.ctx, resolution.ID, resolution.LeaseToken, data); err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	result, err := d.resolveWithLease(resolver, resolution, coordinator.ResolutionRequest{
		ResolutionID: resolution.ID, ArtifactDir: artifactDir, SessionDir: sessionDir,
		Repair: true, ValidationError: "A single public-context expansion was granted. Produce a final resolution; do not request more context.",
	}, leaseFor)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	candidateData, err := os.ReadFile(result.OutputPath)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	finalCandidate, candidateDAG, err := d.validateResolutionCandidate(resolution, candidateData)
	if err != nil || len(finalCandidate.RequestedContext) > 0 {
		if err == nil {
			err = fmt.Errorf("coordinator requested a second context expansion")
		}
		_, recordErr := d.coordinationManager.RecordResolutionValidationFailure(d.ctx, resolution.ID, resolution.LeaseToken, err)
		d.drainCoordinationEffects()
		return recordErr
	}
	return d.applyResolutionCandidate(resolution, finalCandidate, candidateDAG, candidateData)
}

func (d *Daemon) resolveWithLease(resolver coordinator.ResolutionCoordinator, resolution *coordination.Resolution, req coordinator.ResolutionRequest, leaseFor time.Duration) (*coordinator.ResolutionResult, error) {
	ctx, cancel := context.WithCancel(d.ctx)
	defer cancel()
	var renewMu sync.Mutex
	var lastRenew time.Time
	req.Progress = func() {
		renewMu.Lock()
		defer renewMu.Unlock()
		if !lastRenew.IsZero() && time.Since(lastRenew) < leaseFor/4 {
			return
		}
		if err := d.coordinationManager.RenewResolution(ctx, resolution.ID, resolution.LeaseToken, leaseFor); err == nil {
			lastRenew = time.Now()
		}
	}
	return resolver.ResolveContract(ctx, req)
}

func (d *Daemon) failResolutionProcess(resolution *coordination.Resolution, cause error) error {
	_, err := d.coordinationManager.FailResolutionAttempt(d.ctx, resolution.ID, resolution.LeaseToken, cause.Error())
	d.drainCoordinationEffects()
	if err != nil {
		return err
	}
	return cause
}

func (d *Daemon) buildResolutionInput(resolution *coordination.Resolution, expanded bool) (*resolutionInputArtifact, error) {
	contract, err := d.coordinationManager.Get(d.ctx, coordination.Principal{Role: coordination.RoleAdmin}, resolution.ContractID)
	if err != nil {
		return nil, err
	}
	terms, err := d.coordinationManager.Terms(d.ctx, coordination.Principal{Role: coordination.RoleAdmin}, resolution.ContractID)
	if err != nil {
		return nil, err
	}
	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil || attempt == nil || attempt.Plan == nil {
		return nil, fmt.Errorf("coordination: accepted build plan is unavailable")
	}
	specPath := attempt.SpecPath
	if !filepath.IsAbs(specPath) {
		specPath = filepath.Join(d.projectRoot, specPath)
	}
	specData, err := os.ReadFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("coordination: read accepted build spec: %w", err)
	}
	snapshot := d.dagManager.Snapshot()
	affected := make(map[string]bool)
	for _, id := range resolution.AffectedNodeIDs {
		affected[id] = true
	}
	var nodes []dag.DagNode
	for _, node := range snapshot.Nodes {
		if affected[node.ID] {
			nodes = append(nodes, node)
		}
	}
	var edges []dag.DagEdge
	for _, edge := range snapshot.Edges {
		if affected[edge.FromNode] || affected[edge.ToNode] {
			edges = append(edges, edge)
		}
	}
	var publicArtifact *resolutionPublicArtifact
	if contract.Revision > 0 {
		_, content, fetchErr := d.coordinationManager.Fetch(d.ctx, coordination.Principal{Role: coordination.RoleAdmin}, contract.ID, contract.Revision)
		if fetchErr == nil {
			media := ""
			if contract.Artifact != nil {
				media = contract.Artifact.MediaType
			}
			publicArtifact = &resolutionPublicArtifact{MediaType: media, Content: string(content)}
		}
	}
	input := &resolutionInputArtifact{
		Type: "aion_contract_resolution_input", Resolution: *resolution, Contract: *contract,
		TermsHistory: terms, BuildSpec: string(specData), Ownership: append([]coordinator.Domain(nil), attempt.Plan.Domains...),
		AffectedNodes: nodes, RelevantEdges: edges, PublicArtifact: publicArtifact,
		OutputSchema: resolutionOutputSchema(),
		ValidationRules: []string{
			"existing node domains, agents, target files, and task definitions are immutable",
			"domain ownership paths are immutable and may not be expanded or transferred",
			"new nodes may only target existing owned paths in an affected domain",
			"edge changes must touch an affected or newly proposed node and remain acyclic",
			"contract edges cannot be removed",
			"rationale, evidence, and action summaries are required",
		},
	}
	if expanded {
		input.ExpandedDAG = snapshot
	}
	return input, nil
}

func resolutionOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "aion_contract_resolution", "resolution_id": "must match input",
		"rationale": "non-empty decision rationale", "evidence": []string{"public evidence used"},
		"actions":           []map[string]string{{"kind": "inspect|decide", "summary": "observable action summary"}},
		"requested_context": []string{"optional: expanded_dag"},
		"terms": map[string]interface{}{
			"details": "complete ContractDetails object", "producer_obligations": []string{},
			"consumer_obligations": []string{}, "tradeoffs": []string{},
		},
		"add_nodes": []interface{}{}, "add_edges": []interface{}{}, "remove_edges": []interface{}{}, "risks": []string{},
	}
}

func resolutionTemplateData(input *resolutionInputArtifact) ([]byte, error) {
	if input == nil {
		return nil, fmt.Errorf("coordination: resolution input is required")
	}
	template := resolutionOutputArtifact{
		Type:         "aion_contract_resolution",
		ResolutionID: input.Resolution.ID,
		Rationale:    "Replace with one sentence describing the reconciliation decision.",
		Evidence:     []string{"Replace with one item of public evidence from the sealed input."},
		Actions:      []resolutionAction{{Kind: "decide", Summary: "Replace with one observable action summary."}},
		Terms: resolutionTermsOutput{
			Details:             input.Contract.Details,
			ProducerObligations: []string{"Replace with the producer obligation."},
			ConsumerObligations: []string{"Replace with the consumer obligation."},
			Tradeoffs:           []string{"Replace with the accepted tradeoff."},
		},
		Risks: []string{"Replace with one residual risk."},
	}
	return json.MarshalIndent(template, "", "  ")
}

func (d *Daemon) validateResolutionCandidate(resolution *coordination.Resolution, data []byte) (*resolutionOutputArtifact, *dag.DagData, error) {
	var candidate resolutionOutputArtifact
	if err := json.Unmarshal(data, &candidate); err != nil {
		return nil, nil, fmt.Errorf("resolution output is not valid JSON: %w", err)
	}
	if candidate.Type != "aion_contract_resolution" || candidate.ResolutionID != resolution.ID {
		return nil, nil, fmt.Errorf("resolution output identity does not match the active resolution")
	}
	if strings.TrimSpace(candidate.Rationale) == "" || len(candidate.Evidence) == 0 || len(candidate.Actions) == 0 {
		return nil, nil, fmt.Errorf("resolution rationale, evidence, and action summaries are required")
	}
	if len(candidate.RequestedContext) > 0 {
		for _, request := range candidate.RequestedContext {
			if request != "expanded_dag" {
				return nil, nil, fmt.Errorf("unsupported public context request %q", request)
			}
		}
		return &candidate, nil, nil
	}
	if candidate.Terms.Details.Name == "" || candidate.Terms.Details.Kind == "" {
		return nil, nil, fmt.Errorf("resolved contract terms require name and kind")
	}
	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil || attempt == nil || attempt.Plan == nil {
		return nil, nil, fmt.Errorf("accepted build plan is unavailable")
	}
	current := d.dagManager.Snapshot()
	result := &dag.DagData{Header: current.Header, Nodes: append([]dag.DagNode(nil), current.Nodes...), Edges: append([]dag.DagEdge(nil), current.Edges...)}
	nodeByID := make(map[string]dag.DagNode, len(current.Nodes))
	affectedDomains := make(map[string]bool)
	affectedIDs := make(map[string]bool)
	for _, id := range resolution.AffectedNodeIDs {
		affectedIDs[id] = true
	}
	for _, node := range current.Nodes {
		nodeByID[node.ID] = node
		if affectedIDs[node.ID] {
			affectedDomains[node.DomainID] = true
		}
	}
	owned := make(map[string][]string)
	for _, domain := range attempt.Plan.Domains {
		owned[domain.DomainID] = append([]string(nil), domain.AssignedPaths...)
	}
	newIDs := make(map[string]bool)
	now := time.Now().UnixMilli()
	for _, proposed := range candidate.AddNodes {
		existing := nodeByID[proposed.ID]
		if proposed.ID == "" || newIDs[proposed.ID] {
			return nil, nil, fmt.Errorf("new resolution node ID %q is empty or already exists", proposed.ID)
		}
		if existing.ID != "" {
			if resolution.Status != coordination.ResolutionApplying || !sameResolutionNode(existing, proposed) {
				return nil, nil, fmt.Errorf("new resolution node ID %q is empty or already exists", proposed.ID)
			}
			newIDs[proposed.ID] = true
			continue
		}
		if !affectedDomains[proposed.DomainID] {
			return nil, nil, fmt.Errorf("new node %s is outside the affected domains", proposed.ID)
		}
		if len(proposed.TargetFiles) == 0 {
			return nil, nil, fmt.Errorf("new node %s requires target files", proposed.ID)
		}
		for _, target := range proposed.TargetFiles {
			if !pathOwnedByDomain(target, owned[proposed.DomainID]) {
				return nil, nil, fmt.Errorf("new node %s target %q is outside immutable domain ownership", proposed.ID, target)
			}
		}
		newIDs[proposed.ID] = true
		result.Nodes = append(result.Nodes, dag.DagNode{ID: proposed.ID, DomainID: proposed.DomainID,
			TaskSpec: proposed.TaskSpec, Status: dag.StatusPending, AssignedAgent: "agent-" + proposed.DomainID,
			TargetFiles: append([]string(nil), proposed.TargetFiles...), Priority: proposed.Priority, CreatedAt: now, UpdatedAt: now})
	}
	for _, remove := range candidate.RemoveEdges {
		removed := false
		filtered := result.Edges[:0]
		for _, edge := range result.Edges {
			if edge.FromNode == remove.FromNode && edge.ToNode == remove.ToNode {
				if edge.Type == dag.EdgeContract {
					return nil, nil, fmt.Errorf("contract edge %s->%s cannot be removed", edge.FromNode, edge.ToNode)
				}
				if !affectedIDs[edge.FromNode] && !affectedIDs[edge.ToNode] {
					return nil, nil, fmt.Errorf("edge removal does not touch the affected subgraph")
				}
				removed = true
				continue
			}
			filtered = append(filtered, edge)
		}
		if !removed && resolution.Status != coordination.ResolutionApplying {
			return nil, nil, fmt.Errorf("edge %s->%s does not exist", remove.FromNode, remove.ToNode)
		}
		result.Edges = filtered
	}
	allIDs := make(map[string]bool, len(result.Nodes))
	for _, node := range result.Nodes {
		allIDs[node.ID] = true
	}
	for _, add := range candidate.AddEdges {
		if !allIDs[add.FromNode] || !allIDs[add.ToNode] {
			return nil, nil, fmt.Errorf("new edge %s->%s references an unknown node", add.FromNode, add.ToNode)
		}
		if !affectedIDs[add.FromNode] && !affectedIDs[add.ToNode] && !newIDs[add.FromNode] && !newIDs[add.ToNode] {
			return nil, nil, fmt.Errorf("new edge does not touch the affected subgraph")
		}
		alreadyExists := false
		for _, edge := range result.Edges {
			if edge.FromNode == add.FromNode && edge.ToNode == add.ToNode {
				alreadyExists = true
				break
			}
		}
		if alreadyExists {
			if resolution.Status == coordination.ResolutionApplying {
				continue
			}
			return nil, nil, fmt.Errorf("new edge %s->%s already exists", add.FromNode, add.ToNode)
		}
		result.Edges = append(result.Edges, dag.DagEdge{FromNode: add.FromNode, ToNode: add.ToNode, Type: dag.EdgeDependency})
	}
	if err := dag.ValidateAll(result); err != nil {
		return nil, nil, fmt.Errorf("resolution DAG validation failed: %w", err)
	}
	return &candidate, result, nil
}

func sameResolutionNode(existing dag.DagNode, proposed coordinator.TaskNode) bool {
	if existing.DomainID != proposed.DomainID || existing.TaskSpec != proposed.TaskSpec || existing.Priority != proposed.Priority || existing.AssignedAgent != "agent-"+proposed.DomainID {
		return false
	}
	if len(existing.TargetFiles) != len(proposed.TargetFiles) {
		return false
	}
	for i := range existing.TargetFiles {
		if existing.TargetFiles[i] != proposed.TargetFiles[i] {
			return false
		}
	}
	return true
}

func pathOwnedByDomain(target string, assigned []string) bool {
	target = filepath.Clean(strings.TrimSpace(target))
	if target == "." || filepath.IsAbs(target) || target == ".." || strings.HasPrefix(target, ".."+string(filepath.Separator)) {
		return false
	}
	for _, root := range assigned {
		root = filepath.Clean(strings.TrimSpace(root))
		if root == "." || target == root || strings.HasPrefix(target, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func (d *Daemon) applyResolutionCandidate(resolution *coordination.Resolution, candidate *resolutionOutputArtifact, candidateDAG *dag.DagData, candidateData []byte) error {
	if _, err := d.coordinationManager.StageResolutionOutput(d.ctx, resolution.ID, resolution.LeaseToken, candidateData, candidate.Rationale); err != nil {
		return err
	}
	if _, err := d.coordinationManager.BeginResolutionApply(d.ctx, resolution.ID, resolution.LeaseToken); err != nil {
		return err
	}
	if err := d.dagManager.BulkLoad(candidateDAG); err != nil {
		return err
	}
	if err := d.persistResolutionPlanAdditions(candidate); err != nil {
		return err
	}
	terms := coordination.ContractTerms{Details: candidate.Terms.Details,
		ProducerObligations: candidate.Terms.ProducerObligations, ConsumerObligations: candidate.Terms.ConsumerObligations,
		Tradeoffs: candidate.Terms.Tradeoffs, Rationale: candidate.Rationale}
	if _, err := d.coordinationManager.FinalizeResolution(d.ctx, resolution.ID, resolution.LeaseToken, terms); err != nil {
		return err
	}
	d.drainCoordinationEffects()
	d.refreshBuildProgress("coordination_resolution")
	return nil
}

func (d *Daemon) recoverResolutionApply(resolution *coordination.Resolution) error {
	data, err := d.coordinationManager.ReadResolutionArtifact(resolution, true)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	candidate, candidateDAG, err := d.validateResolutionCandidate(resolution, data)
	if err != nil {
		_, awaitErr := d.coordinationManager.AwaitResolutionUser(d.ctx, resolution.ID, resolution.LeaseToken, "persisted resolution output failed recovery validation: "+err.Error())
		return awaitErr
	}
	if err := d.dagManager.BulkLoad(candidateDAG); err != nil {
		return err
	}
	if err := d.persistResolutionPlanAdditions(candidate); err != nil {
		return err
	}
	terms := coordination.ContractTerms{Details: candidate.Terms.Details,
		ProducerObligations: candidate.Terms.ProducerObligations, ConsumerObligations: candidate.Terms.ConsumerObligations,
		Tradeoffs: candidate.Terms.Tradeoffs, Rationale: candidate.Rationale}
	_, err = d.coordinationManager.FinalizeResolution(d.ctx, resolution.ID, resolution.LeaseToken, terms)
	if err == nil {
		d.drainCoordinationEffects()
	}
	return err
}

func (d *Daemon) recoverValidatedResolution(resolution *coordination.Resolution) error {
	data, err := d.coordinationManager.ReadResolutionArtifact(resolution, true)
	if err != nil {
		return d.failResolutionProcess(resolution, err)
	}
	candidate, candidateDAG, err := d.validateResolutionCandidate(resolution, data)
	if err != nil {
		_, awaitErr := d.coordinationManager.AwaitResolutionUser(d.ctx, resolution.ID, resolution.LeaseToken, "persisted validated output failed recovery validation: "+err.Error())
		return awaitErr
	}
	if _, err := d.coordinationManager.BeginResolutionApply(d.ctx, resolution.ID, resolution.LeaseToken); err != nil {
		return err
	}
	resolution.Status = coordination.ResolutionApplying
	if err := d.dagManager.BulkLoad(candidateDAG); err != nil {
		return err
	}
	if err := d.persistResolutionPlanAdditions(candidate); err != nil {
		return err
	}
	terms := coordination.ContractTerms{Details: candidate.Terms.Details,
		ProducerObligations: candidate.Terms.ProducerObligations, ConsumerObligations: candidate.Terms.ConsumerObligations,
		Tradeoffs: candidate.Terms.Tradeoffs, Rationale: candidate.Rationale}
	_, err = d.coordinationManager.FinalizeResolution(d.ctx, resolution.ID, resolution.LeaseToken, terms)
	if err == nil {
		d.drainCoordinationEffects()
	}
	return err
}

func (d *Daemon) persistResolutionPlanAdditions(candidate *resolutionOutputArtifact) error {
	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil || attempt == nil || attempt.Plan == nil {
		return fmt.Errorf("accepted build plan is unavailable")
	}
	nodes := make(map[string]bool)
	for _, node := range attempt.Plan.Nodes {
		nodes[node.ID] = true
	}
	for _, node := range candidate.AddNodes {
		if !nodes[node.ID] {
			attempt.Plan.Nodes = append(attempt.Plan.Nodes, node)
			attempt.CreatedNodeIDs = append(attempt.CreatedNodeIDs, node.ID)
			nodes[node.ID] = true
		}
	}
	edges := make(map[string]bool)
	for _, edge := range attempt.Plan.Edges {
		edges[edge.FromNode+"\x00"+edge.ToNode] = true
	}
	for _, remove := range candidate.RemoveEdges {
		filtered := attempt.Plan.Edges[:0]
		for _, edge := range attempt.Plan.Edges {
			if edge.FromNode != remove.FromNode || edge.ToNode != remove.ToNode {
				filtered = append(filtered, edge)
			}
		}
		attempt.Plan.Edges = filtered
	}
	for _, edge := range candidate.AddEdges {
		key := edge.FromNode + "\x00" + edge.ToNode
		if !edges[key] {
			attempt.Plan.Edges = append(attempt.Plan.Edges, edge)
			edges[key] = true
		}
	}
	sort.Strings(attempt.CreatedNodeIDs)
	return saveBuildSpecAttempt(d.runState.Root, attempt)
}
