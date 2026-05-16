package orchestrator

import (
	"errors"
	"testing"

	"aion-kernel/internal/dag"
	"aion-kernel/internal/locking"
	"aion-kernel/internal/stub"
)

func TestArchitectCommandHandlers(t *testing.T) {
	s := NewServer(newTestDagManager(t), locking.NewManager(nil), stub.NewRegistry(), nil)
	retryCalled := false
	continueCalled := false
	resumeCalled := false
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
	if resp := s.handleRequest(Request{ID: "6", Method: "architect-reset"}); resp.Error != "" || !resetCalled {
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
