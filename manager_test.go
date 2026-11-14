package coordination

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testManager(t *testing.T) *Manager {
	t.Helper()
	manager, err := Open(Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return manager
}

func principal(agentID string, nodeIDs ...string) Principal {
	return Principal{Role: RoleAgent, AgentID: agentID, NodeIDs: nodeIDs}
}

func contractFixture(id, producerAgent, producerNode, consumerAgent, consumerNode string) Contract {
	return Contract{
		ID:              id,
		ProducerAgentID: producerAgent,
		ProducerNodeID:  producerNode,
		ConsumerAgentID: consumerAgent,
		ConsumerNodeID:  consumerNode,
		Details: ContractDetails{
			Name:           "PublicBoundary",
			Kind:           "interface",
			Language:       "go",
			Acceptance:     []string{"consumer test passes"},
			ErrorBehavior:  []string{"returns a typed error"},
			Preconditions:  []string{"input is validated"},
			Postconditions: []string{"result is stable"},
		},
	}
}

func TestLifecycleCorrectionArtifactHistoryAndCompletionIntent(t *testing.T) {
	manager := testManager(t)
	ctx := context.Background()
	consumer := principal("agent-consumer", "node-consumer")
	producer := principal("agent-producer", "node-producer")

	requested, err := manager.Request(ctx, consumer, RequestCommand{
		CommandID: "request-1",
		Contract:  contractFixture("contract-1", producer.AgentID, "node-producer", consumer.AgentID, "node-consumer"),
	})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if requested.Status != StatusRequested || requested.Revision != 0 {
		t.Fatalf("unexpected requested contract: %+v", requested)
	}

	firstContent := []byte("revision one")
	produced, err := manager.Publish(ctx, producer, PublishCommand{
		CommandID:        "publish-1",
		ContractID:       requested.ID,
		ExpectedRevision: 0,
		Content:          firstContent,
		MediaType:        "text/plain",
	})
	if err != nil {
		t.Fatalf("Publish revision 1: %v", err)
	}
	if produced.Status != StatusProduced || produced.Revision != 1 || produced.Artifact == nil {
		t.Fatalf("unexpected produced contract: %+v", produced)
	}

	completion, err := manager.RequestNodeCompletion(ctx, producer, CompletionCommand{
		CommandID: "complete-producer",
		NodeID:    "node-producer",
		Metadata:  json.RawMessage(`{"completed_at":1}`),
	})
	if err != nil {
		t.Fatalf("RequestNodeCompletion: %v", err)
	}
	if completion.Ready || len(completion.PendingContracts) != 1 {
		t.Fatalf("completion should be deferred: %+v", completion)
	}

	corrected, err := manager.Correct(ctx, consumer, CorrectCommand{
		CommandID:  "correct-1",
		ContractID: requested.ID,
		Revision:   1,
		Reason:     "missing boundary behavior",
	})
	if err != nil {
		t.Fatalf("Correct: %v", err)
	}
	if corrected.Status != StatusCorrectionRequested {
		t.Fatalf("expected correction state, got %s", corrected.Status)
	}

	secondContent := []byte("revision two")
	produced, err = manager.Publish(ctx, producer, PublishCommand{
		CommandID:        "publish-2",
		ContractID:       requested.ID,
		ExpectedRevision: 1,
		Content:          secondContent,
		MediaType:        "text/plain",
	})
	if err != nil {
		t.Fatalf("Publish revision 2: %v", err)
	}
	if produced.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", produced.Revision)
	}

	fulfilled, err := manager.Validate(ctx, consumer, ValidateCommand{
		CommandID:  "validate-2",
		ContractID: requested.ID,
		Revision:   2,
		Evidence:   json.RawMessage(`{"test":"pass"}`),
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if fulfilled.Status != StatusFulfilled || fulfilled.Validation == nil || !fulfilled.Validation.Accepted {
		t.Fatalf("unexpected fulfillment: %+v", fulfilled)
	}

	_, fetchedFirst, err := manager.Fetch(ctx, consumer, requested.ID, 1)
	if err != nil || !bytes.Equal(fetchedFirst, firstContent) {
		t.Fatalf("Fetch revision 1: content=%q err=%v", fetchedFirst, err)
	}
	_, fetchedSecond, err := manager.Fetch(ctx, consumer, requested.ID, 2)
	if err != nil || !bytes.Equal(fetchedSecond, secondContent) {
		t.Fatalf("Fetch revision 2: content=%q err=%v", fetchedSecond, err)
	}

	effects, err := manager.PendingEffects(ctx, 100)
	if err != nil {
		t.Fatalf("PendingEffects: %v", err)
	}
	completionEffectID := ""
	for _, effect := range effects {
		if effect.Type == EffectNodeCompletionReady && effect.TargetNodeID == "node-producer" {
			completionEffectID = effect.ID
		}
	}
	if completionEffectID == "" {
		t.Fatal("missing durable NodeCompletionReady effect")
	}
	if err := manager.AckEffect(ctx, completionEffectID); err != nil {
		t.Fatalf("acknowledge completion effect: %v", err)
	}
	receipts, err := manager.AcknowledgedNodeCompletionEffects(ctx)
	if err != nil {
		t.Fatalf("AcknowledgedNodeCompletionEffects: %v", err)
	}
	if len(receipts) != 1 || receipts[0].ID != completionEffectID || receipts[0].TargetNodeID != "node-producer" {
		t.Fatalf("unexpected completion receipts: %+v", receipts)
	}
}

func TestCommandIdempotencyRevisionAndAuthorization(t *testing.T) {
	manager := testManager(t)
	ctx := context.Background()
	consumer := principal("agent-consumer", "node-consumer")
	producer := principal("agent-producer", "node-producer")
	contract := contractFixture("contract-idempotent", producer.AgentID, "node-producer", consumer.AgentID, "node-consumer")
	command := RequestCommand{CommandID: "same-command", Contract: contract}

	first, err := manager.Request(ctx, consumer, command)
	if err != nil {
		t.Fatalf("first Request: %v", err)
	}
	second, err := manager.Request(ctx, consumer, command)
	if err != nil {
		t.Fatalf("idempotent Request: %v", err)
	}
	if first.ID != second.ID || !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("idempotent result changed: first=%+v second=%+v", first, second)
	}

	changed := command
	changed.Contract.Details.Name = "DifferentBoundary"
	if _, err := manager.Request(ctx, consumer, changed); err == nil || !strings.Contains(err.Error(), "different input") {
		t.Fatalf("expected command reuse rejection, got %v", err)
	}

	wrongProducer := principal("agent-other", "node-producer")
	if _, err := manager.Publish(ctx, wrongProducer, PublishCommand{ContractID: contract.ID, Content: []byte("x")}); err == nil {
		t.Fatal("unauthorized producer published an artifact")
	}
	objectCount := 0
	if err := filepath.WalkDir(manager.objectDir, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			objectCount++
		}
		return err
	}); err != nil {
		t.Fatalf("inspect object store: %v", err)
	}
	if objectCount != 0 {
		t.Fatalf("unauthorized publication left %d artifact objects", objectCount)
	}
	if _, err := manager.Publish(ctx, producer, PublishCommand{ContractID: contract.ID, ExpectedRevision: 4, Content: []byte("x")}); err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatalf("expected revision conflict, got %v", err)
	}
	if _, err := manager.Get(ctx, principal("agent-outsider", "node-outsider"), contract.ID); err == nil {
		t.Fatal("unrelated agent read contract")
	}
}

func TestConcurrentDuplicateRequestProducesOneContract(t *testing.T) {
	manager := testManager(t)
	ctx := context.Background()
	consumer := principal("agent-consumer", "node-consumer")
	command := RequestCommand{
		CommandID: "concurrent-command",
		Contract:  contractFixture("contract-concurrent", "agent-producer", "node-producer", consumer.AgentID, "node-consumer"),
	}

	const goroutines = 12
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := manager.Request(ctx, consumer, command)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Request: %v", err)
		}
	}
	contracts, err := manager.List(ctx, Principal{Role: RoleAdmin})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(contracts) != 1 {
		t.Fatalf("expected one contract, got %d", len(contracts))
	}
}

func TestRestartRecoversContractsArtifactsAndPendingEffects(t *testing.T) {
	stateDir := t.TempDir()
	ctx := context.Background()
	consumer := principal("agent-consumer", "node-consumer")
	producer := principal("agent-producer", "node-producer")

	manager, err := Open(Options{StateDir: stateDir})
	if err != nil {
		t.Fatalf("Open first: %v", err)
	}
	contract, err := manager.Request(ctx, consumer, RequestCommand{Contract: contractFixture("contract-restart", producer.AgentID, "node-producer", consumer.AgentID, "node-consumer")})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	content := []byte("persistent artifact")
	if _, err := manager.Publish(ctx, producer, PublishCommand{ContractID: contract.ID, Content: content}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	manager, err = Open(Options{StateDir: stateDir})
	if err != nil {
		t.Fatalf("Open second: %v", err)
	}
	defer manager.Close()
	recovered, fetched, err := manager.Fetch(ctx, consumer, contract.ID, 1)
	if err != nil {
		t.Fatalf("Fetch after restart: %v", err)
	}
	if recovered.Status != StatusProduced || !bytes.Equal(fetched, content) {
		t.Fatalf("unexpected recovered state: %+v content=%q", recovered, fetched)
	}
	effects, err := manager.PendingEffects(ctx, 100)
	if err != nil || len(effects) != 1 || effects[0].Type != EffectContractProduced {
		t.Fatalf("pending effects were not recovered: count=%d err=%v", len(effects), err)
	}
	var mode string
	if err := manager.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || strings.ToLower(mode) != "wal" {
		t.Fatalf("SQLite WAL mode not active: mode=%q err=%v", mode, err)
	}
}

func TestArtifactCorruptionIsDetected(t *testing.T) {
	manager := testManager(t)
	ctx := context.Background()
	consumer := principal("agent-consumer", "node-consumer")
	producer := principal("agent-producer", "node-producer")
	contract, err := manager.Request(ctx, consumer, RequestCommand{Contract: contractFixture("contract-corrupt", producer.AgentID, "node-producer", consumer.AgentID, "node-consumer")})
	if err != nil {
		t.Fatal(err)
	}
	produced, err := manager.Publish(ctx, producer, PublishCommand{ContractID: contract.ID, Content: []byte("valid")})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(manager.objectDir, produced.Artifact.Digest[:2], produced.Artifact.Digest)
	if err := os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.Fetch(ctx, consumer, contract.ID, 1); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("expected digest mismatch, got %v", err)
	}
}

func TestExpiryProducesTerminalEffect(t *testing.T) {
	manager := testManager(t)
	ctx := context.Background()
	consumer := principal("agent-consumer", "node-consumer")
	expires := time.Now().UTC().Add(50 * time.Millisecond)
	contract := contractFixture("contract-expiry", "agent-producer", "node-producer", consumer.AgentID, "node-consumer")
	contract.ExpiresAt = &expires
	if _, err := manager.Request(ctx, consumer, RequestCommand{Contract: contract}); err != nil {
		t.Fatal(err)
	}
	count, err := manager.ExpireDue(ctx, expires.Add(time.Second))
	if err != nil || count != 1 {
		t.Fatalf("ExpireDue: count=%d err=%v", count, err)
	}
	recovered, err := manager.Get(ctx, consumer, contract.ID)
	if err != nil || recovered.Status != StatusExpired {
		t.Fatalf("expected expired contract, got %+v err=%v", recovered, err)
	}
}
