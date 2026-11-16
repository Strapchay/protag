package orchestrator

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	coordination "aion-coordination"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/locking"
	"aion-kernel/internal/messaging"
)

type coordinationTestAgent struct {
	id       string
	mu       sync.Mutex
	messages []hub.Message
}

func (a *coordinationTestAgent) AgentID() string { return a.id }

func (a *coordinationTestAgent) DeliverContext(message hub.Message) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.messages = append(a.messages, message)
	return nil
}

func (a *coordinationTestAgent) hasMessage(messageType hub.MessageType) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, message := range a.messages {
		if message.Type == messageType {
			return true
		}
	}
	return false
}

type coordinationKernelFixture struct {
	manager      *coordination.Manager
	dag          *dag.Manager
	server       *Server
	daemon       *Daemon
	router       *hub.Router
	capabilities map[string]string
	agents       map[string]*coordinationTestAgent
}

func newCoordinationKernelFixture(t *testing.T, nodes ...dag.DagNode) *coordinationKernelFixture {
	t.Helper()
	root := t.TempDir()
	dagManager, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.bin"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
		MaxNodes:      100,
	})
	if err != nil {
		t.Fatalf("create DAG manager: %v", err)
	}
	for _, node := range nodes {
		if err := dagManager.AddNode(node); err != nil {
			t.Fatalf("add node %s: %v", node.ID, err)
		}
	}
	coordinationManager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(root, "coordination")})
	if err != nil {
		t.Fatalf("open coordination manager: %v", err)
	}
	router := hub.NewRouter(filepath.Join(root, "logs"))
	server := NewServer(dagManager, locking.NewManager(nil), coordinationManager, nil)
	server.SetLogsDir(filepath.Join(root, "logs"))
	daemon := &Daemon{
		dagManager:          dagManager,
		coordinationManager: coordinationManager,
		hubRouter:           router,
		server:              server,
		ctx:                 context.Background(),
	}
	server.coordinationEffectsCb = daemon.drainCoordinationEffects

	fixture := &coordinationKernelFixture{
		manager:      coordinationManager,
		dag:          dagManager,
		server:       server,
		daemon:       daemon,
		router:       router,
		capabilities: make(map[string]string),
		agents:       make(map[string]*coordinationTestAgent),
	}
	for _, node := range nodes {
		agentID := node.AssignedAgent
		if agentID == "" {
			agentID = expectedAgentForNode(node.DomainID, "")
		}
		if _, exists := fixture.capabilities[agentID]; exists {
			continue
		}
		capability, err := server.IssueAgentCapability(agentID)
		if err != nil {
			t.Fatalf("issue capability for %s: %v", agentID, err)
		}
		agent := &coordinationTestAgent{id: agentID}
		router.RegisterAgent(agentID, agent)
		fixture.capabilities[agentID] = capability
		fixture.agents[agentID] = agent
	}
	t.Cleanup(func() {
		router.Close()
		if err := coordinationManager.Close(); err != nil {
			t.Errorf("close coordination manager: %v", err)
		}
		if err := dagManager.Close(); err != nil {
			t.Errorf("close DAG manager: %v", err)
		}
	})
	return fixture
}

func coordinationNode(id, domain, agent string) dag.DagNode {
	return dag.DagNode{
		ID:            id,
		DomainID:      domain,
		AssignedAgent: agent,
		TaskSpec:      "test " + id,
		Status:        dag.StatusInProgress,
	}
}

func coordinationContract(id, producerAgent, producerNode, consumerAgent, consumerNode string) coordination.Contract {
	return coordination.Contract{
		ID:              id,
		ProducerAgentID: producerAgent,
		ProducerNodeID:  producerNode,
		ConsumerAgentID: consumerAgent,
		ConsumerNodeID:  consumerNode,
		Details: coordination.ContractDetails{
			Name:       "PublicBoundary",
			Kind:       "interface",
			Acceptance: []string{"consumer integration test passes"},
		},
	}
}

func callCoordinationServer(t *testing.T, server *Server, method, capability string, params any) Response {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("encode %s request: %v", method, err)
	}
	return server.handleRequest(Request{ID: method + "-test", Method: method, Capability: capability, Params: raw})
}

func decodeCoordinationResult[T any](t *testing.T, response Response) T {
	t.Helper()
	if response.Error != "" {
		t.Fatalf("coordination request failed: %s", response.Error)
	}
	raw, err := json.Marshal(response.Result)
	if err != nil {
		t.Fatalf("encode coordination response: %v", err)
	}
	var result T
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode coordination response: %v", err)
	}
	return result
}

func TestKernelCoordinationLifecycleAuthorizesArtifactsAndDefersCompletion(t *testing.T) {
	producerAgent := "agent-provider"
	consumerAgent := "agent-consumer"
	fixture := newCoordinationKernelFixture(t,
		coordinationNode("provider-node", "provider", producerAgent),
		coordinationNode("consumer-node", "consumer", consumerAgent),
	)
	contract := coordinationContract("contract-lifecycle", producerAgent, "provider-node", consumerAgent, "consumer-node")

	requested := decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-request", fixture.capabilities[consumerAgent], map[string]any{
		"command_id": "request-lifecycle",
		"contract":   contract,
	}))
	if requested.Status != coordination.StatusRequested || !fixture.agents[producerAgent].hasMessage(hub.MsgContractRequested) {
		t.Fatalf("request was not projected to producer: %+v", requested)
	}

	unauthorized := callCoordinationServer(t, fixture.server, "contract-publish", fixture.capabilities[consumerAgent], map[string]any{
		"contract_id": contract.ID,
		"content":     []byte("forbidden"),
	})
	if unauthorized.Error == "" || !strings.Contains(unauthorized.Error, "assigned producer") {
		t.Fatalf("consumer publication should be rejected, got %+v", unauthorized)
	}

	deferred := decodeCoordinationResult[struct {
		Status           string   `json:"status"`
		PendingContracts []string `json:"pending_contracts"`
	}](t, callCoordinationServer(t, fixture.server, "update-node", fixture.capabilities[producerAgent], map[string]any{
		"node_id":  "provider-node",
		"agent_id": producerAgent,
		"status":   "Done",
	}))
	if deferred.Status != "completion_deferred" || len(deferred.PendingContracts) != 1 {
		t.Fatalf("expected deferred completion, got %+v", deferred)
	}
	if node, _ := fixture.dag.GetNode("provider-node"); node.Status != dag.StatusBlocked {
		t.Fatalf("provider node status = %s, want Blocked", node.Status)
	}

	produced := decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-publish", fixture.capabilities[producerAgent], map[string]any{
		"command_id":        "publish-lifecycle",
		"contract_id":       contract.ID,
		"expected_revision": 0,
		"content":           []byte("versioned interface artifact"),
		"media_type":        "text/plain",
	}))
	if produced.Revision != 1 || !fixture.agents[consumerAgent].hasMessage(hub.MsgContractProduced) {
		t.Fatalf("artifact publication was not projected to consumer: %+v", produced)
	}

	fetched := decodeCoordinationResult[struct {
		Content []byte `json:"content"`
	}](t, callCoordinationServer(t, fixture.server, "contract-fetch", fixture.capabilities[consumerAgent], map[string]any{
		"contract_id": contract.ID,
		"revision":    1,
	}))
	if string(fetched.Content) != "versioned interface artifact" {
		t.Fatalf("fetched artifact = %q", fetched.Content)
	}

	fulfilled := decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-validate", fixture.capabilities[consumerAgent], map[string]any{
		"command_id":  "validate-lifecycle",
		"contract_id": contract.ID,
		"revision":    1,
		"evidence":    map[string]string{"integration": "passed"},
	}))
	if fulfilled.Status != coordination.StatusFulfilled {
		t.Fatalf("contract status = %s, want Fulfilled", fulfilled.Status)
	}
	if node, _ := fixture.dag.GetNode("provider-node"); node.Status != dag.StatusDone {
		t.Fatalf("provider node status = %s, want Done after contract fulfillment", node.Status)
	}
	retriedCompletion := callCoordinationServer(t, fixture.server, "update-node", fixture.capabilities[producerAgent], map[string]any{
		"node_id":  "provider-node",
		"agent_id": producerAgent,
		"status":   "Done",
	})
	if retriedCompletion.Error != "" {
		t.Fatalf("retry completed node: %s", retriedCompletion.Error)
	}
	if node, _ := fixture.dag.GetNode("provider-node"); node.Status != dag.StatusDone {
		t.Fatalf("completion retry regressed provider node to %s", node.Status)
	}
	if err := fixture.dag.UpdateNode("provider-node", dag.StatusBlocked); err != nil {
		t.Fatalf("simulate regressed completion projection: %v", err)
	}
	if err := recoverAcknowledgedNodeCompletions(fixture.dag, fixture.manager); err != nil {
		t.Fatalf("recover acknowledged completion: %v", err)
	}
	if node, _ := fixture.dag.GetNode("provider-node"); node.Status != dag.StatusDone {
		t.Fatalf("completion receipt recovery left provider node %s", node.Status)
	}
	if fixture.agents[producerAgent].hasMessage(hub.MsgContractFulfilled) {
		t.Fatal("status-only ContractFulfilled effect started a producer agent turn")
	}
	fixture.server.mu.Lock()
	defer fixture.server.mu.Unlock()
	foundFulfilled := false
	for _, message := range fixture.server.hubHistory {
		if message.Type == hub.MsgContractFulfilled && message.ToAgent == producerAgent {
			foundFulfilled = true
			break
		}
	}
	if !foundFulfilled {
		t.Fatal("ContractFulfilled effect was not projected to observable Hub history")
	}
}

func TestKernelCoordinationAuthorizesExpectedAgentBeforeNodeDispatch(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		dag.DagNode{ID: "provider-node", DomainID: "provider", Status: dag.StatusPending},
		dag.DagNode{ID: "consumer-node", DomainID: "consumer", Status: dag.StatusPending},
	)
	contract := coordinationContract(
		"contract-pre-dispatch",
		"agent-provider", "provider-node",
		"agent-consumer", "consumer-node",
	)
	requested := decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(
		t, fixture.server, "contract-request", fixture.capabilities["agent-consumer"], map[string]any{
			"command_id": "request-pre-dispatch", "contract": contract,
		},
	))
	if requested.Status != coordination.StatusRequested {
		t.Fatalf("contract status = %s, want Requested", requested.Status)
	}
	published := decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(
		t, fixture.server, "contract-publish", fixture.capabilities["agent-provider"], map[string]any{
			"command_id": "publish-pre-dispatch", "contract_id": contract.ID,
			"expected_revision": 0, "content": []byte("public boundary"),
		},
	))
	if published.Status != coordination.StatusProduced {
		t.Fatalf("contract status = %s, want Produced", published.Status)
	}
}

func TestResumeCoordinationNodePreservesAllocationBoundary(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		dag.DagNode{ID: "planned-consumer", DomainID: "consumer", Status: dag.StatusBlocked},
		dag.DagNode{ID: "paused-consumer", DomainID: "consumer", AssignedAgent: "agent-consumer", Status: dag.StatusBlocked},
	)

	if err := fixture.daemon.resumeCoordinationNode("planned-consumer"); err != nil {
		t.Fatalf("resume unassigned consumer: %v", err)
	}
	planned, err := fixture.dag.GetNode("planned-consumer")
	if err != nil {
		t.Fatalf("read unassigned consumer: %v", err)
	}
	if planned.Status != dag.StatusPending || planned.AssignedAgent != "" {
		t.Fatalf("unassigned consumer = %+v, want Pending without an owner", planned)
	}

	if err := fixture.daemon.resumeCoordinationNode("paused-consumer"); err != nil {
		t.Fatalf("resume assigned consumer: %v", err)
	}
	paused, err := fixture.dag.GetNode("paused-consumer")
	if err != nil {
		t.Fatalf("read assigned consumer: %v", err)
	}
	if paused.Status != dag.StatusInProgress || paused.AssignedAgent != "agent-consumer" {
		t.Fatalf("assigned consumer = %+v, want InProgress with its owner", paused)
	}
}

func TestRecoverUnassignedInProgressNodes(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		dag.DagNode{ID: "orphaned-active", DomainID: "consumer", Status: dag.StatusInProgress},
		dag.DagNode{ID: "owned-active", DomainID: "provider", AssignedAgent: "agent-provider", Status: dag.StatusInProgress},
	)

	if err := recoverUnassignedInProgressNodes(fixture.dag); err != nil {
		t.Fatalf("recover unassigned active nodes: %v", err)
	}
	orphaned, _ := fixture.dag.GetNode("orphaned-active")
	if orphaned.Status != dag.StatusPending {
		t.Fatalf("orphaned node status = %s, want Pending", orphaned.Status)
	}
	owned, _ := fixture.dag.GetNode("owned-active")
	if owned.Status != dag.StatusInProgress || owned.AssignedAgent != "agent-provider" {
		t.Fatalf("owned active node changed: %+v", owned)
	}
}

func TestKernelCoordinationHardWaitCycleBlocksNodesAndSurfacesEvent(t *testing.T) {
	agentA := "agent-a"
	agentB := "agent-b"
	fixture := newCoordinationKernelFixture(t,
		coordinationNode("node-a", "a", agentA),
		coordinationNode("node-b", "b", agentB),
	)

	contractAB := coordinationContract("contract-a-needs-b", agentB, "node-b", agentA, "node-a")
	contractAB.HardWait = true
	decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-request", fixture.capabilities[agentA], map[string]any{
		"command_id": "request-a-needs-b",
		"contract":   contractAB,
	}))

	contractBA := coordinationContract("contract-b-needs-a", agentA, "node-a", agentB, "node-b")
	contractBA.HardWait = true
	decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-request", fixture.capabilities[agentB], map[string]any{
		"command_id": "request-b-needs-a",
		"contract":   contractBA,
	}))

	for _, nodeID := range []string{"node-a", "node-b"} {
		node, err := fixture.dag.GetNode(nodeID)
		if err != nil || node.Status != dag.StatusBlocked {
			t.Fatalf("cycle node %s was not blocked: node=%+v err=%v", nodeID, node, err)
		}
	}
	snapshot, err := fixture.manager.Snapshot(context.Background())
	if err != nil || len(snapshot.Cycles) != 1 {
		t.Fatalf("expected one durable cycle, got %+v err=%v", snapshot.Cycles, err)
	}

	fixture.server.mu.Lock()
	defer fixture.server.mu.Unlock()
	found := false
	for _, message := range fixture.server.hubHistory {
		if message.Type == hub.MsgContractCycle {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("cycle was not surfaced in the main TUI Hub stream")
	}
}

func TestKernelStandstillBlocksAndPrivatelyNotifiesOnlyAffectedParticipants(t *testing.T) {
	producerAgent := "agent-provider"
	consumerAgent := "agent-consumer"
	fixture := newCoordinationKernelFixture(t,
		coordinationNode("provider-node", "provider", producerAgent),
		coordinationNode("consumer-node", "consumer", consumerAgent),
		coordinationNode("unrelated-node", "unrelated", "agent-unrelated"),
	)
	contract := coordinationContract("contract-standstill", producerAgent, "provider-node", consumerAgent, "consumer-node")
	decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-request", fixture.capabilities[consumerAgent], map[string]any{
		"command_id": "request-standstill", "contract": contract,
	}))
	decodeCoordinationResult[coordination.ContractTerms](t, callCoordinationServer(t, fixture.server, "contract-propose-terms", fixture.capabilities[producerAgent], map[string]any{
		"command_id": "terms-2", "contract_id": contract.ID, "expected_revision": 1,
		"details": contract.Details, "rationale": "producer requires typed failures",
	}))
	decodeCoordinationResult[coordination.ContractTerms](t, callCoordinationServer(t, fixture.server, "contract-propose-terms", fixture.capabilities[consumerAgent], map[string]any{
		"command_id": "terms-3", "contract_id": contract.ID, "expected_revision": 2,
		"details": contract.Details, "rationale": "consumer cannot accept that failure contract",
	}))
	resolution := decodeCoordinationResult[coordination.Resolution](t, callCoordinationServer(t, fixture.server, "contract-standstill", fixture.capabilities[producerAgent], map[string]any{
		"command_id": "standstill", "contract_id": contract.ID, "terms_revision": 3,
		"reason": "participants cannot satisfy one compatibility guarantee", "evidence": []string{"contract test mismatch"},
	}))
	if resolution.Status != coordination.ResolutionPending {
		t.Fatalf("resolution = %#v", resolution)
	}
	for _, nodeID := range []string{"provider-node", "consumer-node"} {
		node, err := fixture.dag.GetNode(nodeID)
		if err != nil || node.Status != dag.StatusBlocked {
			t.Fatalf("affected node %s = %#v, err=%v", nodeID, node, err)
		}
	}
	if node, _ := fixture.dag.GetNode("unrelated-node"); node.Status != dag.StatusInProgress {
		t.Fatalf("unrelated node status = %s", node.Status)
	}
	if !fixture.agents[producerAgent].hasMessage(hub.MsgContractWait) || !fixture.agents[consumerAgent].hasMessage(hub.MsgContractWait) {
		t.Fatal("affected participants did not receive private reconciliation pause notices")
	}
	if fixture.agents["agent-unrelated"].hasMessage(hub.MsgContractWait) {
		t.Fatal("unrelated participant received a reconciliation pause notice")
	}
	fixture.server.mu.Lock()
	defer fixture.server.mu.Unlock()
	for _, message := range fixture.server.hubHistory {
		if strings.HasPrefix(message.ID, "resolution-notice:") {
			t.Fatal("private resolution detail leaked into the main orchestration stream")
		}
	}
}

func TestContractReadCommandsAllowHostWithoutCapability(t *testing.T) {
	fixture := newCoordinationKernelFixture(t)
	contract := coordinationContract("contract-host-read", "agent-a", "a-node", "agent-b", "b-node")
	if _, err := fixture.manager.Request(context.Background(), coordination.Principal{Role: coordination.RoleAdmin}, coordination.RequestCommand{Contract: contract}); err != nil {
		t.Fatalf("seed contract: %v", err)
	}

	listed := callCoordinationServer(t, fixture.server, "contract-list", "", map[string]any{})
	if listed.Error != "" {
		t.Fatalf("host contract-list error = %s", listed.Error)
	}
	listedContracts, ok := listed.Result.(map[string]interface{})["contracts"].([]coordination.Contract)
	if !ok || len(listedContracts) != 1 || listedContracts[0].ID != contract.ID {
		t.Fatalf("host contract-list result = %#v", listed.Result)
	}

	status := callCoordinationServer(t, fixture.server, "contract-status", "", map[string]any{"contract_id": contract.ID})
	if status.Error != "" {
		t.Fatalf("host contract-status error = %s", status.Error)
	}

	terms := callCoordinationServer(t, fixture.server, "contract-terms", "", map[string]any{"contract_id": contract.ID})
	if terms.Error != "" {
		t.Fatalf("host contract-terms error = %s", terms.Error)
	}

	invalid := callCoordinationServer(t, fixture.server, "contract-list", "not-a-capability", map[string]any{})
	if !strings.Contains(invalid.Error, "valid agent capability") {
		t.Fatalf("invalid capability error = %q", invalid.Error)
	}

	mutation := callCoordinationServer(t, fixture.server, "contract-request", "", map[string]any{"contract": contract})
	if !strings.Contains(mutation.Error, "valid agent capability") {
		t.Fatalf("host mutation error = %q", mutation.Error)
	}
}

func TestContractCommandsFailClosedWithoutCoordinationManager(t *testing.T) {
	server, _ := startTestServer(t)
	capability, err := server.IssueAgentCapability("agent-a")
	if err != nil {
		t.Fatal(err)
	}
	response := callCoordinationServer(t, server, "contract-list", capability, map[string]any{})
	if response.Error != "coordination manager is unavailable" {
		t.Fatalf("unexpected unavailable-manager response: %+v", response)
	}
}

func TestContractValidationHandshakeGateUsesStructuredField(t *testing.T) {
	producerAgent := "agent-provider"
	consumerAgent := "agent-consumer"
	fixture := newCoordinationKernelFixture(t,
		coordinationNode("provider-node", "provider", producerAgent),
		coordinationNode("consumer-node", "consumer", consumerAgent),
	)
	store, err := messaging.Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	fixture.server.messageStore = store

	create := func(id, resource string, handshake bool) coordination.Contract {
		contract := coordinationContract(id, producerAgent, "provider-node", consumerAgent, "consumer-node")
		contract.Details.Name = resource
		contract.Details.HandshakeRequired = handshake
		decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-request", fixture.capabilities[consumerAgent], map[string]any{
			"command_id": "request-" + id,
			"contract":   contract,
		}))
		return decodeCoordinationResult[coordination.Contract](t, callCoordinationServer(t, fixture.server, "contract-publish", fixture.capabilities[producerAgent], map[string]any{
			"command_id":        "publish-" + id,
			"contract_id":       id,
			"expected_revision": 0,
			"content":           []byte("artifact " + id),
		}))
	}
	validate := func(id string, params map[string]any) Response {
		params["command_id"] = "validate-" + id
		params["contract_id"] = id
		params["revision"] = 1
		return callCoordinationServer(t, fixture.server, "contract-validate", fixture.capabilities[consumerAgent], params)
	}

	create("contract-gated", "GatedBoundary", true)
	if response := validate("contract-gated", map[string]any{}); response.Error == "" || !strings.Contains(response.Error, "--message-id") {
		t.Fatalf("gated validation without a message was accepted: %+v", response)
	}

	if err := store.Submit(t.Context(), messaging.Message{
		ID: "msg-handshake", FromAgent: consumerAgent, ToAgent: producerAgent,
		Kind: messaging.KindRequest, Payload: "CONSUMER PLAN GatedBoundary",
	}); err != nil {
		t.Fatal(err)
	}
	if response := validate("contract-gated", map[string]any{"message_id": "msg-handshake"}); response.Error == "" {
		t.Fatal("gated validation accepted an unacknowledged request")
	}
	ack := callCoordinationServer(t, fixture.server, "message-ack", fixture.capabilities[producerAgent], map[string]any{
		"id": "msg-handshake", "state": messaging.ActorAcked, "note": "plan received",
	})
	if ack.Error != "" {
		t.Fatalf("ack handshake request: %s", ack.Error)
	}
	fulfilled := decodeCoordinationResult[coordination.Contract](t, validate("contract-gated", map[string]any{"message_id": "msg-handshake"}))
	if fulfilled.Status != coordination.StatusFulfilled {
		t.Fatalf("gated contract status = %s", fulfilled.Status)
	}

	create("contract-ungated", "PlainBoundary", false)
	ungated := decodeCoordinationResult[coordination.Contract](t, validate("contract-ungated", map[string]any{}))
	if ungated.Status != coordination.StatusFulfilled {
		t.Fatalf("ungated contract status = %s", ungated.Status)
	}
}
