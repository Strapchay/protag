package locking

import "testing"

func TestAcquireAndRelease(t *testing.T) {
	mgr := NewManager(nil)

	if err := mgr.Acquire("file.go", "agent-1", nil); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	locked, owner := mgr.IsLocked("file.go")
	if !locked || owner != "agent-1" {
		t.Fatalf("expected locked by agent-1, got locked=%v owner=%s", locked, owner)
	}

	if err := mgr.Release("file.go", "agent-1"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	locked, _ = mgr.IsLocked("file.go")
	if locked {
		t.Fatal("expected unlocked after release")
	}
}

func TestLockConflict(t *testing.T) {
	mgr := NewManager(nil)

	mgr.Acquire("file.go", "agent-1", nil)

	if err := mgr.Acquire("file.go", "agent-2", nil); err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestIdempotentReacquire(t *testing.T) {
	mgr := NewManager(nil)

	mgr.Acquire("file.go", "agent-1", nil)

	// Same agent re-acquiring should be idempotent
	if err := mgr.Acquire("file.go", "agent-1", nil); err != nil {
		t.Fatalf("re-acquire should be idempotent: %v", err)
	}
}

func TestReleaseByNonOwner(t *testing.T) {
	mgr := NewManager(nil)

	mgr.Acquire("file.go", "agent-1", nil)

	if err := mgr.Release("file.go", "agent-2"); err == nil {
		t.Fatal("expected error releasing by non-owner")
	}
}

func TestIdempotentRelease(t *testing.T) {
	mgr := NewManager(nil)

	// Releasing an unlocked file should be fine
	if err := mgr.Release("file.go", "agent-1"); err != nil {
		t.Fatalf("release of unlocked file should be idempotent: %v", err)
	}
}

func TestSharedFileRejection(t *testing.T) {
	mgr := NewManager([]string{"go.mod", "go.sum", "main.go"})

	if err := mgr.Acquire("go.mod", "agent-1", nil); err == nil {
		t.Fatal("expected shared file rejection")
	}

	// Non-shared file should work
	if err := mgr.Acquire("auth.go", "agent-1", nil); err != nil {
		t.Fatalf("non-shared file should be lockable: %v", err)
	}
}

func TestReleaseAll(t *testing.T) {
	mgr := NewManager(nil)

	mgr.Acquire("a.go", "agent-1", nil)
	mgr.Acquire("b.go", "agent-1", nil)
	mgr.Acquire("c.go", "agent-2", nil)

	released := mgr.ReleaseAll("agent-1")
	if released != 2 {
		t.Fatalf("expected 2 released, got %d", released)
	}

	if mgr.ActiveLockCount() != 1 {
		t.Fatalf("expected 1 remaining lock, got %d", mgr.ActiveLockCount())
	}

	// agent-2's lock should remain
	locked, owner := mgr.IsLocked("c.go")
	if !locked || owner != "agent-2" {
		t.Fatal("agent-2's lock should remain")
	}
}

func TestListLocks(t *testing.T) {
	mgr := NewManager(nil)

	mgr.Acquire("a.go", "agent-1", nil)
	mgr.Acquire("b.go", "agent-2", nil)

	locks := mgr.ListLocks()
	if len(locks) != 2 {
		t.Fatalf("expected 2 locks, got %d", len(locks))
	}
}

func TestIsSharedFile(t *testing.T) {
	mgr := NewManager([]string{"go.mod"})

	if !mgr.IsSharedFile("go.mod") {
		t.Fatal("go.mod should be shared")
	}
	if mgr.IsSharedFile("auth.go") {
		t.Fatal("auth.go should not be shared")
	}
}
