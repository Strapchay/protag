package orchestrator

import (
	"strings"
	"testing"
)

func TestProjectLockRejectsSecondOwner(t *testing.T) {
	root := t.TempDir()
	lock, err := acquireProjectLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireProjectLock(root); err == nil || !strings.Contains(err.Error(), "already owned") {
		t.Fatalf("second lock err = %v", err)
	}
	lock.Close()
	next, err := acquireProjectLock(root)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	next.Close()
}
