package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	coordination "aion-coordination"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
)

type resolutionWorkflowCoordinator struct {
	output resolutionOutputArtifact
}

func (c *resolutionWorkflowCoordinator) Plan(context.Context, coordinator.PlanRequest) (*coordinator.PlanResponse, error) {
	return nil, nil
}

func (c *resolutionWorkflowCoordinator) Replan(context.Context, coordinator.ReplanRequest) (*coordinator.ReplanResponse, error) {
	return nil, nil
}

func (c *resolutionWorkflowCoordinator) Refine(context.Context, string) error { return nil }

func (c *resolutionWorkflowCoordinator) GetArchitectAgent() interface{} { return nil }

func (c *resolutionWorkflowCoordinator) ResolveContract(_ context.Context, req coordinator.ResolutionRequest) (*coordinator.ResolutionResult, error) {
	if req.Progress != nil {
		req.Progress()
	}
	c.output.ResolutionID = req.ResolutionID
	data, err := json.Marshal(c.output)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(req.ArtifactDir, "resolution_output.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, err
	}
	return &coordinator.ResolutionResult{OutputPath: path}, nil
}

func TestResolutionCandidateEnforcesImmutableOwnership(t *testing.T) {
	d := testBuildSpecDaemon(t)
	for _, node := range []dag.DagNode{
		{ID: "alpha-node", DomainID: "alpha", TaskSpec: "alpha", Status: dag.StatusBlocked, AssignedAgent: "agent-alpha", TargetFiles: []string{"alpha/main.go"}},
		{ID: "beta-node", DomainID: "beta", TaskSpec: "beta", Status: dag.StatusBlocked, AssignedAgent: "agent-beta", TargetFiles: []string{"beta/main.go"}},
	} {
		if err := d.dagManager.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	attempt := newBuildSpecAttempt(d.runState.RunID, filepath.Join(d.projectRoot, "docs", "build_spec.md"), []byte("spec"))
	attempt.Plan = &coordinator.PlanResponse{Domains: []coordinator.Domain{
		{DomainID: "alpha", AssignedPaths: []string{"alpha/"}},
		{DomainID: "beta", AssignedPaths: []string{"beta/"}},
	}, Nodes: []coordinator.TaskNode{{ID: "alpha-node", DomainID: "alpha"}, {ID: "beta-node", DomainID: "beta"}}}
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		t.Fatal(err)
	}
	resolution := &coordination.Resolution{ID: "res-test", Status: coordination.ResolutionRunning, AffectedNodeIDs: []string{"alpha-node", "beta-node"}}

	invalid := validResolutionOutput(resolution.ID)
	invalid.AddNodes = []coordinator.TaskNode{{ID: "beta-extra", DomainID: "beta", TaskSpec: "escape", TargetFiles: []string{"alpha/escape.go"}}}
	data, _ := json.Marshal(invalid)
	if _, _, err := d.validateResolutionCandidate(resolution, data); err == nil || !strings.Contains(err.Error(), "outside immutable domain ownership") {
		t.Fatalf("expected ownership rejection, got %v", err)
	}

	valid := validResolutionOutput(resolution.ID)
	valid.AddNodes = []coordinator.TaskNode{{ID: "beta-extra", DomainID: "beta", TaskSpec: "adapter", TargetFiles: []string{"beta/adapter.go"}, Priority: 2}}
	valid.AddEdges = []coordinator.TaskEdge{{FromNode: "alpha-node", ToNode: "beta-extra"}}
	data, _ = json.Marshal(valid)
	candidate, candidateDAG, err := d.validateResolutionCandidate(resolution, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.AddNodes) != 1 || len(candidateDAG.Nodes) != 3 || candidateDAG.Nodes[2].AssignedAgent != "agent-beta" {
		t.Fatalf("candidate=%#v dag=%#v", candidate, candidateDAG)
	}
}

func TestResolutionCandidateRejectsUnrelatedDAGEdits(t *testing.T) {
	d := testBuildSpecDaemon(t)
	for _, node := range []dag.DagNode{
		{ID: "alpha-node", DomainID: "alpha", Status: dag.StatusBlocked, AssignedAgent: "agent-alpha", TargetFiles: []string{"alpha/main.go"}},
		{ID: "beta-node", DomainID: "beta", Status: dag.StatusBlocked, AssignedAgent: "agent-beta", TargetFiles: []string{"beta/main.go"}},
		{ID: "gamma-node", DomainID: "gamma", Status: dag.StatusPending, AssignedAgent: "agent-gamma", TargetFiles: []string{"gamma/main.go"}},
		{ID: "gamma-test", DomainID: "gamma", Status: dag.StatusPending, AssignedAgent: "agent-gamma", TargetFiles: []string{"gamma/test.go"}},
	} {
		if err := d.dagManager.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	attempt := newBuildSpecAttempt(d.runState.RunID, filepath.Join(d.projectRoot, "docs", "build_spec.md"), []byte("spec"))
	attempt.Plan = &coordinator.PlanResponse{Domains: []coordinator.Domain{
		{DomainID: "alpha", AssignedPaths: []string{"alpha/"}}, {DomainID: "beta", AssignedPaths: []string{"beta/"}}, {DomainID: "gamma", AssignedPaths: []string{"gamma/"}},
	}}
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		t.Fatal(err)
	}
	resolution := &coordination.Resolution{ID: "res-test", Status: coordination.ResolutionRunning, AffectedNodeIDs: []string{"alpha-node", "beta-node"}}
	output := validResolutionOutput(resolution.ID)
	output.AddEdges = []coordinator.TaskEdge{{FromNode: "gamma-node", ToNode: "gamma-test"}}
	data, _ := json.Marshal(output)
	if _, _, err := d.validateResolutionCandidate(resolution, data); err == nil || !strings.Contains(err.Error(), "affected subgraph") {
		t.Fatalf("expected unrelated edge rejection, got %v", err)
	}
}

func TestCoordinationResolutionWorkflowPersistsAndAppliesValidatedCandidate(t *testing.T) {
	d := testBuildSpecDaemon(t)
	d.ctx = context.Background()
	manager, err := coordination.Open(coordination.Options{
		StateDir: d.runState.CoordinationDir, MaxNegotiationRounds: 2,
		MaxProcessAttempts: 2, MaxRepairAttempts: 1, MaxContextExpansions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	d.coordinationManager = manager
	d.server.SetRuntimeSubsystems(d.dagManager, nil, manager, nil)
	for _, node := range []dag.DagNode{
		{ID: "alpha-node", DomainID: "alpha", TaskSpec: "alpha", Status: dag.StatusBlocked, AssignedAgent: "agent-alpha", TargetFiles: []string{"alpha/main.go"}},
		{ID: "beta-node", DomainID: "beta", TaskSpec: "beta", Status: dag.StatusBlocked, AssignedAgent: "agent-beta", TargetFiles: []string{"beta/main.go"}},
	} {
		if err := d.dagManager.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	for _, agentID := range []string{"agent-alpha", "agent-beta"} {
		d.hubRouter.RegisterAgent(agentID, &coordinationTestAgent{id: agentID})
	}
	specPath := filepath.Join(d.projectRoot, "docs", "build_spec.md")
	if err := os.MkdirAll(filepath.Dir(specPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(specPath, []byte("# Build specification\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	attempt := newBuildSpecAttempt(d.runState.RunID, specPath, []byte("# Build specification\n"))
	attempt.Plan = &coordinator.PlanResponse{
		Domains: []coordinator.Domain{
			{DomainID: "alpha", AssignedPaths: []string{"alpha/"}},
			{DomainID: "beta", AssignedPaths: []string{"beta/"}},
		},
		Nodes: []coordinator.TaskNode{
			{ID: "alpha-node", DomainID: "alpha", TargetFiles: []string{"alpha/main.go"}},
			{ID: "beta-node", DomainID: "beta", TargetFiles: []string{"beta/main.go"}},
		},
	}
	if err := saveBuildSpecAttempt(d.runState.Root, attempt); err != nil {
		t.Fatal(err)
	}
	admin := coordination.Principal{Role: coordination.RoleAdmin}
	contract, err := manager.Request(d.ctx, admin, coordination.RequestCommand{Contract: coordination.Contract{
		ID: "contract-workflow", ProducerAgentID: "agent-alpha", ProducerNodeID: "alpha-node",
		ConsumerAgentID: "agent-beta", ConsumerNodeID: "beta-node",
		Details: coordination.ContractDetails{Name: "Boundary", Kind: "interface"}, Status: coordination.StatusRequested,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ProposeTerms(d.ctx, admin, coordination.ProposeTermsCommand{
		ContractID: contract.ID, ExpectedRevision: 1, Details: contract.Details, Rationale: "incompatible public shape",
	}); err != nil {
		t.Fatal(err)
	}
	resolution, err := manager.DeclareStandstill(d.ctx, admin, coordination.StandstillCommand{
		ContractID: contract.ID, TermsRevision: 2, Reason: "participants require incompatible shapes",
	})
	if err != nil {
		t.Fatal(err)
	}
	d.coordinator = &resolutionWorkflowCoordinator{output: resolutionOutputArtifact{
		Type: "aion_contract_resolution", Rationale: "introduce an owned adapter",
		Evidence: []string{"public contract lineage"}, Actions: []resolutionAction{{Kind: "add_task", Summary: "add an alpha-owned adapter"}},
		Terms:    resolutionTermsOutput{Details: coordination.ContractDetails{Name: "Boundary", Kind: "interface", Compatibility: "adapter"}},
		AddNodes: []coordinator.TaskNode{{ID: "alpha-adapter", DomainID: "alpha", TaskSpec: "implement adapter", TargetFiles: []string{"alpha/adapter.go"}, Priority: 2}},
		AddEdges: []coordinator.TaskEdge{{FromNode: "alpha-node", ToNode: "alpha-adapter"}},
	}}
	if err := d.runNextCoordinationResolution(); err != nil {
		t.Fatal(err)
	}
	resolved, err := manager.GetResolution(d.ctx, resolution.ID)
	if err != nil || resolved.Status != coordination.ResolutionResolved || resolved.ProcessAttempts != 1 {
		t.Fatalf("resolved state = %#v, err=%v", resolved, err)
	}
	adapter, err := d.dagManager.GetNode("alpha-adapter")
	if err != nil || adapter.AssignedAgent != "agent-alpha" || adapter.Status != dag.StatusPending {
		t.Fatalf("adapter node = %#v, err=%v", adapter, err)
	}
	for _, nodeID := range []string{"alpha-node", "beta-node"} {
		node, err := d.dagManager.GetNode(nodeID)
		if err != nil || node.Status != dag.StatusInProgress {
			t.Fatalf("resumed node %s = %#v, err=%v", nodeID, node, err)
		}
	}
	persisted, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil || persisted == nil || len(persisted.Plan.Nodes) != 3 || len(persisted.Plan.Edges) != 1 {
		t.Fatalf("persisted plan = %#v, err=%v", persisted, err)
	}
	d.server.mu.Lock()
	defer d.server.mu.Unlock()
	for _, message := range d.server.hubHistory {
		if message.Type != "ContractTermsAgreed" {
			continue
		}
		var terms coordination.ContractTerms
		if json.Unmarshal(message.Payload, &terms) == nil && terms.ProposerAgentID == "coordinator" {
			t.Fatal("coordinator resolution terms leaked into main dashboard history")
		}
	}
}

func TestResolutionCapabilityIssuerBindsInferencePolicy(t *testing.T) {
	config := &Config{}
	applyDefaults(config)
	config.Execution.Mode = "gateway"
	config.Inference.Coordinator = ProviderConfig{UseProfile: "oracle"}
	config.Inference.Models = map[string]ModelProfile{
		"oracle": {Provider: "provider-test", Model: "model-test"},
	}
	server := NewServer(nil, nil, nil, nil)
	env, err := resolutionCapabilityIssuer(server, config)()
	if err != nil || len(env) != 1 || !strings.HasPrefix(env[0], "AION_AGENT_CAPABILITY=") {
		t.Fatalf("credential env = %#v, err=%v", env, err)
	}
	token := strings.TrimPrefix(env[0], "AION_AGENT_CAPABILITY=")
	policy, ok := server.ResolveAgentCapability(token)
	if !ok || policy.AgentID != "coordinator-resolution" || policy.DomainID != "coordinator" || policy.Profile != "oracle" || policy.Provider != "provider-test" || policy.Model != "model-test" {
		t.Fatalf("resolution policy = %#v, ok=%v", policy, ok)
	}
}

func validResolutionOutput(id string) resolutionOutputArtifact {
	return resolutionOutputArtifact{
		Type: "aion_contract_resolution", ResolutionID: id, Rationale: "compatible bounded contract",
		Evidence: []string{"accepted build plan"}, Actions: []resolutionAction{{Kind: "decide", Summary: "selected compatible terms"}},
		Terms: resolutionTermsOutput{Details: coordination.ContractDetails{Name: "Lookup", Kind: "api"}},
	}
}

func TestResolutionTemplateCarriesContractDetails(t *testing.T) {
	input := &resolutionInputArtifact{
		Resolution: coordination.Resolution{ID: "res_template"},
		Contract: coordination.Contract{Details: coordination.ContractDetails{
			Name: "alpha-public-api", Kind: "interface", Language: "python",
			PublicEntrypoint: "from alpha import Ledger", HandshakeRequired: true,
		}},
	}
	data, err := resolutionTemplateData(input)
	if err != nil {
		t.Fatal(err)
	}
	var candidate resolutionOutputArtifact
	if err := json.Unmarshal(data, &candidate); err != nil {
		t.Fatalf("template is not valid JSON: %v", err)
	}
	if candidate.Type != "aion_contract_resolution" || candidate.ResolutionID != "res_template" {
		t.Fatalf("template identity = %#v", candidate)
	}
	if candidate.Terms.Details.Name != "alpha-public-api" || !candidate.Terms.Details.HandshakeRequired {
		t.Fatalf("template details = %#v", candidate.Terms.Details)
	}
	if len(candidate.Evidence) == 0 || len(candidate.Actions) == 0 {
		t.Fatalf("template placeholders = %#v", candidate)
	}
}
