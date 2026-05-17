package orchestrator

import (
	"encoding/json"
	"errors"
	"net"
	"testing"

	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/locking"
	"aion-kernel/internal/stub"
)

func TestArchitectCommandHandlers(t *testing.T) {
	s := NewServer(newTestDagManager(t), locking.NewManager(nil), stub.NewRegistry(), nil)
	retryCalled := false
	continueCalled := false
	resumeCalled := false
	continueAgentsCalled := false
	resetCalled := false
	s.SetArchitectStatusCallback(func() string { return "status=awaiting_user" })
	s.SetArchitectRetryCallback(func() error {
		retryCalled = true
		return nil
	})
	s.SetArchitectContinueCallback(func() error {
		continueCalled = true
		return nil
	})
	s.SetArchitectResumeCallback(func() error {
		resumeCalled = true
		return nil
	})
	s.SetBuildSpecContinueCallback(func() error {
		continueAgentsCalled = true
		return nil
	})
	s.SetArchitectShowSpecCallback(func() (string, error) { return "# Spec", nil })
	s.SetArchitectResetCallback(func() error {
		resetCalled = true
		return nil
	})

	if resp := s.handleRequest(Request{ID: "1", Method: "architect-status"}); resp.Error != "" {
		t.Fatalf("architect-status error: %s", resp.Error)
	}
	if resp := s.handleRequest(Request{ID: "2", Method: "architect-retry"}); resp.Error != "" || !retryCalled {
		t.Fatalf("architect-retry resp=%#v called=%v", resp, retryCalled)
	}
	if resp := s.handleRequest(Request{ID: "3", Method: "architect-continue"}); resp.Error != "" || !continueCalled {
		t.Fatalf("architect-continue resp=%#v called=%v", resp, continueCalled)
	}
	if resp := s.handleRequest(Request{ID: "4", Method: "architect-resume"}); resp.Error != "" || !resumeCalled {
		t.Fatalf("architect-resume resp=%#v called=%v", resp, resumeCalled)
	}
	if resp := s.handleRequest(Request{ID: "5", Method: "architect-show-spec"}); resp.Error != "" {
		t.Fatalf("architect-show-spec error: %s", resp.Error)
	}
	if resp := s.handleRequest(Request{ID: "6", Method: "build-spec-continue-agents"}); resp.Error != "" || !continueAgentsCalled {
		t.Fatalf("build-spec-continue-agents resp=%#v called=%v", resp, continueAgentsCalled)
	}
	if resp := s.handleRequest(Request{ID: "7", Method: "architect-reset"}); resp.Error != "" || !resetCalled {
		t.Fatalf("architect-reset resp=%#v called=%v", resp, resetCalled)
	}
}

func TestArchitectCommandHandlerError(t *testing.T) {
	s := NewServer(newTestDagManager(t), locking.NewManager(nil), stub.NewRegistry(), nil)
	s.SetArchitectRetryCallback(func() error { return errors.New("no retry") })
	resp := s.handleRequest(Request{ID: "1", Method: "architect-retry"})
	if resp.Error != "no retry" {
		t.Fatalf("expected retry error, got %#v", resp)
	}
}

func TestTailHubEventsReplaysPersistedHistory(t *testing.T) {
	dir := t.TempDir()
	seed := NewServer(newTestDagManager(t), locking.NewManager(nil), stub.NewRegistry(), nil)
	seed.SetLogsDir(dir)
	seed.BroadcastHubEvent(hubMessageForTest(t, "coordinator", "agent-api", "hello one"))
	seed.BroadcastHubEvent(hubMessageForTest(t, "agent-api", "tui", "hello two"))

	srv := NewServer(newTestDagManager(t), locking.NewManager(nil), stub.NewRegistry(), nil)
	srv.SetLogsDir(dir)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleTailHubEvents(Request{}, json.NewEncoder(serverConn), serverConn)
	}()

	dec := json.NewDecoder(clientConn)
	var first, second hub.Message
	if err := dec.Decode(&first); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if err := dec.Decode(&second); err != nil {
		t.Fatalf("decode second: %v", err)
	}
	if first.FromAgent != "coordinator" || second.FromAgent != "agent-api" {
		t.Fatalf("unexpected replay order: %#v %#v", first, second)
	}
	srv.mu.Lock()
	for ch := range srv.hubSubs {
		close(ch)
		delete(srv.hubSubs, ch)
	}
	srv.mu.Unlock()
	_ = clientConn.Close()
	<-done
}

func hubMessageForTest(t *testing.T, from, to, text string) hub.Message {
	t.Helper()
	msg, err := hub.NewMessage(hub.MsgContextShare, from, to, map[string]string{"content": text})
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	return *msg
}

func newTestDagManager(t *testing.T) *dag.Manager {
	t.Helper()
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: t.TempDir() + "/dag.bin",
		WalFilePath:   t.TempDir() + "/dag.wal",
		MaxNodes:      10,
	})
	if err != nil {
		t.Fatalf("new dag manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}
