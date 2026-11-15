package messaging

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreLifecycleAndActorAck(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	request := Message{
		ID: "msg-request", FromAgent: "agent-report", ToAgent: "agent-ledger",
		Kind: KindRequest, Payload: "CONSUMER PLAN",
	}
	if err := store.Submit(ctx, request); err != nil {
		t.Fatal(err)
	}
	pending, err := store.Pending(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if pending[0].ActorState != ActorAwaitingAck {
		t.Fatalf("actor state=%q", pending[0].ActorState)
	}

	if err := store.MarkDelivered(ctx, request.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(ctx, request.ID, ActorAcked, "planned"); err != nil {
		t.Fatal(err)
	}
	status, err := store.Status(ctx, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.TransportState != TransportDelivered || status.ActorState != ActorAcked {
		t.Fatalf("status=%#v", status)
	}
	if status.Note != "planned" || status.AcknowledgedAt.IsZero() {
		t.Fatalf("ack metadata=%#v", status)
	}

	if err := store.Ack(ctx, request.ID, ActorCompleted, "done"); err != nil {
		t.Fatal(err)
	}
	status, _ = store.Status(ctx, request.ID)
	if status.Payload != "" {
		t.Fatalf("completed request retained payload: %q", status.Payload)
	}
}

func TestStoreListsBySenderAndTarget(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	for _, message := range []Message{
		{ID: "msg-1", FromAgent: "agent-report", ToAgent: "agent-ledger", Kind: KindNotify, Payload: "notice"},
		{ID: "msg-2", FromAgent: "agent-ledger", ToAgent: "agent-report", Kind: KindNotify, Payload: "response"},
	} {
		if err := store.Submit(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	from, err := store.List(ctx, ListFilter{FromAgent: "agent-report"})
	if err != nil || len(from) != 1 || from[0].ID != "msg-1" {
		t.Fatalf("from=%#v err=%v", from, err)
	}
	to, err := store.List(ctx, ListFilter{ToAgent: "agent-ledger"})
	if err != nil || len(to) != 1 || to[0].ID != "msg-1" {
		t.Fatalf("to=%#v err=%v", to, err)
	}
}

func TestPruneTerminalHasNoWallClockTTL(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	old := time.Now().Add(-30 * 24 * time.Hour)
	for _, id := range []string{"msg-old", "msg-new"} {
		if err := store.Submit(ctx, Message{
			ID: id, FromAgent: "agent-report", ToAgent: "agent-ledger", Kind: KindNotify,
			Payload: "notice", CreatedAt: old, UpdatedAt: old, NextAttemptAt: old,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkDelivered(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PruneTerminal(ctx, 1); err != nil {
		t.Fatal(err)
	}
	remaining, err := store.List(ctx, ListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("remaining=%#v", remaining)
	}
}
