package dag

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempWalFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test.wal")
}

func TestWalAppendAndReplay(t *testing.T) {
	path := tempWalFile(t)
	wal, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal.Close()

	// Append entries synchronously
	nodePayload, _ := json.Marshal(DagNode{ID: "n1", DomainID: "auth", TaskSpec: "Task 1"})
	edgePayload, _ := json.Marshal(DagEdge{FromNode: "n1", ToNode: "n2"})

	if err := wal.AppendSync(WalEntry{Type: MutAddNode, Payload: nodePayload}); err != nil {
		t.Fatalf("AppendSync node: %v", err)
	}
	if err := wal.AppendSync(WalEntry{Type: MutAddEdge, Payload: edgePayload}); err != nil {
		t.Fatalf("AppendSync edge: %v", err)
	}

	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Type != MutAddNode {
		t.Fatalf("expected MutAddNode, got %s", entries[0].Type)
	}
	if entries[1].Type != MutAddEdge {
		t.Fatalf("expected MutAddEdge, got %s", entries[1].Type)
	}
}

func TestWalReplayEmpty(t *testing.T) {
	path := tempWalFile(t)
	wal, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal.Close()

	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestWalPersistenceAcrossReopen(t *testing.T) {
	path := tempWalFile(t)

	// Write and close
	wal1, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	payload, _ := json.Marshal(DagNode{ID: "persist-1"})
	if err := wal1.AppendSync(WalEntry{Type: MutAddNode, Payload: payload}); err != nil {
		t.Fatalf("AppendSync: %v", err)
	}
	wal1.Close()

	// Reopen and replay
	wal2, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL reopen: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after reopen, got %d", len(entries))
	}
}

func TestWalCorruptionDetection(t *testing.T) {
	path := tempWalFile(t)

	wal, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	payload, _ := json.Marshal(DagNode{ID: "corrupt-1"})
	if err := wal.AppendSync(WalEntry{Type: MutAddNode, Payload: payload}); err != nil {
		t.Fatalf("AppendSync: %v", err)
	}
	wal.Close()

	// Corrupt the file: flip a byte in the middle
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) > 10 {
		data[10] ^= 0xFF // flip bits
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Replay should detect corruption
	wal2, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal2.Close()

	_, err = wal2.Replay()
	if err == nil {
		t.Fatal("expected corruption error on replay")
	}
}

func TestWalGroupCommit(t *testing.T) {
	path := tempWalFile(t)
	wal, err := NewWAL(path, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal.Close()

	// Append many entries without explicit flush
	for i := 0; i < 10; i++ {
		payload, _ := json.Marshal(DagNode{ID: "batch"})
		if err := wal.Append(WalEntry{Type: MutAddNode, Payload: payload}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Wait for flush deadline
	time.Sleep(300 * time.Millisecond)

	// Replay should see all entries
	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 10 {
		t.Fatalf("expected 10 entries after group commit, got %d", len(entries))
	}
}

func TestWalManyEntries(t *testing.T) {
	path := tempWalFile(t)
	wal, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal.Close()

	const N = 100
	for i := 0; i < N; i++ {
		payload, _ := json.Marshal(DagNode{ID: "n", Priority: int32(i)})
		if err := wal.AppendSync(WalEntry{Type: MutAddNode, Payload: payload}); err != nil {
			t.Fatalf("AppendSync %d: %v", i, err)
		}
	}

	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != N {
		t.Fatalf("expected %d entries, got %d", N, len(entries))
	}
}

func TestWalTimestamp(t *testing.T) {
	path := tempWalFile(t)
	wal, err := NewWAL(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal.Close()

	before := time.Now().UnixMilli()
	payload, _ := json.Marshal(DagNode{ID: "ts"})
	if err := wal.AppendSync(WalEntry{Type: MutAddNode, Payload: payload}); err != nil {
		t.Fatalf("AppendSync: %v", err)
	}
	after := time.Now().UnixMilli()

	entries, _ := wal.Replay()
	if len(entries) != 1 {
		t.Fatal("expected 1 entry")
	}
	if entries[0].Timestamp < before || entries[0].Timestamp > after {
		t.Fatalf("timestamp %d not in range [%d, %d]", entries[0].Timestamp, before, after)
	}
}
