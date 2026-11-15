package memory

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestChromemStore_Lifecycle(t *testing.T) {
	// Create a temporary directory for Chromem persistent storage
	tempDir, err := os.MkdirTemp("", "chromem-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "memory")

	// Use Mock embedder for tests to avoid hitting actual APIs
	embedder := NewMockEmbedder()

	// Initialize the store
	store, err := NewStore(dbPath, embedder)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	defer store.Close()

	// 1. Write an entry
	ctx := context.Background()
	timestamp := time.Now().Unix()

	entry1 := MemoryEntry{
		ID:        "node-xyz",
		Text:      "This is a test node describing auth implementation",
		AgentID:   "agent-123",
		TaskID:    "task-abc",
		ProjectID: "default",
		Timestamp: timestamp,
	}

	if err := store.Write(ctx, entry1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 2. Query the exact text (or similar text)
	// Mock embedder always returns [1.0, 0.0, 0.0], so cosine similarity will be 1.0 (perfect match)
	results, err := store.Query(ctx, "auth query", 5)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 result, got 0")
	}

	res := results[0]
	if res.ID != entry1.ID {
		t.Errorf("expected ID %s, got %s", entry1.ID, res.ID)
	}
	if res.Text != entry1.Text {
		t.Errorf("expected text %s, got %s", entry1.Text, res.Text)
	}
	if res.AgentID != entry1.AgentID {
		t.Errorf("expected AgentID %s, got %s", entry1.AgentID, res.AgentID)
	}
	// Verify memory struct unpacked the timestamp string correctly
	if res.Timestamp != timestamp {
		t.Errorf("expected timestamp %d, got %d", timestamp, res.Timestamp)
	}
}

func TestChromemStore_QueryVector(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "memory")

	embedder := NewMockEmbedder()
	store, err := NewStore(dbPath, embedder)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	entry := MemoryEntry{
		ID:        "node-vector",
		Text:      "Pre-embedded content",
		Vector:    []float32{1.0, 0.0, 0.0},
		AgentID:   "agent-vec",
		Timestamp: time.Now().Unix(),
	}

	if err := store.Write(context.Background(), entry); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Query directly by vector
	results, err := store.QueryVector(context.Background(), []float32{1.0, 0.0, 0.0}, 2)
	if err != nil {
		t.Fatalf("QueryVector failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected results, got 0")
	}

	if results[0].ID != "node-vector" {
		t.Fatalf("expected node-vector, got %s", results[0].ID)
	}
}
