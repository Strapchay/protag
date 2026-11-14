package coordination

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTermsNegotiationAndStandstillLifecycle(t *testing.T) {
	manager, err := Open(Options{StateDir: t.TempDir(), MaxNegotiationRounds: 2, MaxProcessAttempts: 2, MaxRepairAttempts: 1, MaxContextExpansions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	ctx := context.Background()
	consumer := Principal{Role: RoleAgent, AgentID: "agent-consumer", NodeIDs: []string{"consumer-node"}}
	producer := Principal{Role: RoleAgent, AgentID: "agent-producer", NodeIDs: []string{"producer-node"}}
	contract, err := manager.Request(ctx, consumer, RequestCommand{Contract: Contract{
		ID: "contract-1", ProducerAgentID: producer.AgentID, ProducerNodeID: "producer-node",
		ConsumerAgentID: consumer.AgentID, ConsumerNodeID: "consumer-node",
		Details: ContractDetails{Name: "Lookup", Kind: "api"}, Status: StatusRequested,
	}})
	if err != nil {
		t.Fatal(err)
	}
	terms, err := manager.Terms(ctx, consumer, contract.ID)
	if err != nil || len(terms) != 1 || terms[0].Status != TermsAgreed || len(terms[0].AcceptedBy) != 2 {
		t.Fatalf("initial terms = %#v, err=%v", terms, err)
	}
	counter, err := manager.ProposeTerms(ctx, producer, ProposeTermsCommand{
		ContractID: contract.ID, ExpectedRevision: 1,
		Details:             ContractDetails{Name: "Lookup", Kind: "api", Compatibility: "v2"},
		ProducerObligations: []string{"return typed errors"}, ConsumerObligations: []string{"handle unavailable"},
		Tradeoffs: []string{"no implicit retries"}, Rationale: "avoid retry amplification",
	})
	if err != nil {
		t.Fatal(err)
	}
	if counter.Status != TermsProposed || counter.Revision != 2 {
		t.Fatalf("counter = %#v", counter)
	}
	if _, err := manager.Publish(ctx, producer, PublishCommand{ContractID: contract.ID, ExpectedRevision: 0, Content: []byte("x")}); err == nil {
		t.Fatal("publish should require agreed latest terms")
	}
	resolution, err := manager.DeclareStandstill(ctx, consumer, StandstillCommand{
		ContractID: contract.ID, TermsRevision: counter.Revision, Reason: "retry behavior is incompatible", Evidence: []string{"consumer test rejects retries"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Status != ResolutionPending || len(resolution.AffectedNodeIDs) != 2 {
		t.Fatalf("resolution = %#v", resolution)
	}
	claimed, err := manager.ClaimNextResolution(ctx, time.Minute)
	if err != nil || claimed == nil || claimed.Status != ResolutionRunning || claimed.ProcessAttempts != 1 {
		t.Fatalf("claim = %#v, err=%v", claimed, err)
	}
	if _, err := manager.RecordResolutionInput(ctx, claimed.ID, claimed.LeaseToken, []byte(`{"input":true}`)); err != nil {
		t.Fatal(err)
	}
	failed, err := manager.RecordResolutionValidationFailure(ctx, claimed.ID, claimed.LeaseToken, errors.New("invalid candidate"))
	if err != nil || failed.RepairAttempts != 1 || failed.Status != ResolutionRunning {
		t.Fatalf("failure = %#v, err=%v", failed, err)
	}
	if _, err := manager.StageResolutionOutput(ctx, claimed.ID, claimed.LeaseToken, []byte(`{"output":true}`), "bounded compromise"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginResolutionApply(ctx, claimed.ID, claimed.LeaseToken); err != nil {
		t.Fatal(err)
	}
	resolved, err := manager.FinalizeResolution(ctx, claimed.ID, claimed.LeaseToken, ContractTerms{
		Details:             ContractDetails{Name: "Lookup", Kind: "api", Compatibility: "resolved"},
		ProducerObligations: []string{"typed errors"}, ConsumerObligations: []string{"bounded retry"},
		Tradeoffs: []string{"one retry"}, Rationale: "bounded compromise",
	})
	if err != nil || resolved.Status != ResolutionResolved {
		t.Fatalf("resolved = %#v, err=%v", resolved, err)
	}
	history, err := manager.Terms(ctx, consumer, contract.ID)
	if err != nil || len(history) != 3 || history[2].Status != TermsAgreed {
		t.Fatalf("history = %#v, err=%v", history, err)
	}
}

func TestResolutionLeaseExpiryIsBounded(t *testing.T) {
	manager, err := Open(Options{StateDir: t.TempDir(), MaxNegotiationRounds: 2, MaxProcessAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	ctx := context.Background()
	admin := Principal{Role: RoleAdmin}
	contract, err := manager.Request(ctx, admin, RequestCommand{Contract: Contract{
		ID: "contract-expiry", ProducerAgentID: "producer", ProducerNodeID: "producer-node",
		ConsumerAgentID: "consumer", ConsumerNodeID: "consumer-node",
		Details: ContractDetails{Name: "Wire", Kind: "schema"}, Status: StatusRequested,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ProposeTerms(ctx, admin, ProposeTermsCommand{ContractID: contract.ID, ExpectedRevision: 1,
		Details: contract.Details, Rationale: "incompatible wire format"}); err != nil {
		t.Fatal(err)
	}
	resolution, err := manager.DeclareStandstill(ctx, admin, StandstillCommand{ContractID: contract.ID, TermsRevision: 2, Reason: "wire mismatch"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := manager.ClaimNextResolution(ctx, time.Nanosecond)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %#v %v", claimed, err)
	}
	time.Sleep(time.Millisecond)
	if err := manager.ExpireResolutionLeases(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	current, err := manager.GetResolution(ctx, resolution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != ResolutionAwaitingUser {
		t.Fatalf("status = %s", current.Status)
	}
}

func TestResolutionRestartRecoveryPreservesValidatedState(t *testing.T) {
	stateDir := t.TempDir()
	options := Options{StateDir: stateDir, MaxNegotiationRounds: 2, MaxProcessAttempts: 2}
	manager, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	admin := Principal{Role: RoleAdmin}
	contract, err := manager.Request(ctx, admin, RequestCommand{Contract: Contract{
		ID: "contract-restart", ProducerAgentID: "producer", ProducerNodeID: "producer-node",
		ConsumerAgentID: "consumer", ConsumerNodeID: "consumer-node",
		Details: ContractDetails{Name: "Wire", Kind: "schema"}, Status: StatusRequested,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ProposeTerms(ctx, admin, ProposeTermsCommand{
		ContractID: contract.ID, ExpectedRevision: 1, Details: contract.Details, Rationale: "version mismatch",
	}); err != nil {
		t.Fatal(err)
	}
	resolution, err := manager.DeclareStandstill(ctx, admin, StandstillCommand{
		ContractID: contract.ID, TermsRevision: 2, Reason: "wire mismatch",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := manager.ClaimNextResolution(ctx, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %#v %v", claimed, err)
	}
	if _, err := manager.StageResolutionOutput(ctx, claimed.ID, claimed.LeaseToken, []byte(`{"valid":true}`), "bounded resolution"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	manager, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.RecoverResolutionLeases(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	recovered, err := manager.ClaimNextResolution(ctx, time.Minute)
	if err != nil || recovered == nil {
		t.Fatalf("recovered claim: %#v %v", recovered, err)
	}
	if recovered.ID != resolution.ID || recovered.Status != ResolutionValidated || recovered.ProcessAttempts != 1 {
		t.Fatalf("recovered resolution = %#v", recovered)
	}
}
