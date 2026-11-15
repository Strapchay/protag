package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"aion-kernel/internal/hub"
	"aion-kernel/internal/messaging"
)

func TestMessageDeliveryLoopMarksDeliveredAndDeadLetters(t *testing.T) {
	store, err := messaging.Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := &Server{}
	delivered := 0
	server.SetMessageDeliverer(func(msg hub.Message) error {
		if msg.ToAgent == "agent-missing" {
			return fmt.Errorf("target unavailable")
		}
		delivered++
		return nil
	})
	daemon := &Daemon{messages: store, server: server}
	ctx := context.Background()

	if err := store.Submit(ctx, messaging.Message{
		ID: "msg-ok", FromAgent: "agent-report", ToAgent: "agent-ledger", Kind: messaging.KindNotify, Payload: "notice",
	}); err != nil {
		t.Fatal(err)
	}
	daemon.deliverPendingMessages(ctx)
	status, err := store.Status(ctx, "msg-ok")
	if err != nil || status.TransportState != messaging.TransportDelivered || delivered != 1 {
		t.Fatalf("status=%#v delivered=%d err=%v", status, delivered, err)
	}

	if err := store.Submit(ctx, messaging.Message{
		ID: "msg-bad", FromAgent: "agent-report", ToAgent: "agent-missing", Kind: messaging.KindNotify, Payload: "notice",
	}); err != nil {
		t.Fatal(err)
	}
	// The first failed delivery scheduled a future retry. Fast-forward the
	// attempt budget without waiting on wall-clock backoff.
	for i := 0; i < maxMessageAttempts-1; i++ {
		if err := store.MarkRetry(ctx, "msg-bad", time.Now().Add(-time.Second), fmt.Errorf("retry")); err != nil {
			t.Fatal(err)
		}
	}
	daemon.deliverPendingMessages(ctx)
	status, _ = store.Status(ctx, "msg-bad")
	if status.TransportState != messaging.TransportDeadLetter {
		t.Fatalf("status=%#v", status)
	}
}
