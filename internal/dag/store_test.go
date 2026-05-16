package dag

import (
	"os"
	"path/filepath"
	"testing"
)

func tempStoreFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test_dag.bin")
}

func TestStoreWriteRead(t *testing.T) {
	path := tempStoreFile(t)
	store, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	dag := NewDagData(200)
	dag.Nodes = append(dag.Nodes, DagNode{
		ID:       "node-001",
		DomainID: "auth",
		TaskSpec: "Implement JWT auth",
		Status:   StatusPending,
		Priority: 1,
	})
	dag.Edges = append(dag.Edges, DagEdge{
		FromNode: "node-001",
		ToNode:   "node-002",
		Type:     EdgeDependency,
	})

	if err := store.Write(dag); err != nil {
		t.Fatalf("Write: %v", err)
	}

	readDag, _, err := store.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if len(readDag.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(readDag.Nodes))
	}
	if readDag.Nodes[0].ID != "node-001" {
		t.Fatalf("expected node ID 'node-001', got '%s'", readDag.Nodes[0].ID)
	}
	if readDag.Nodes[0].TaskSpec != "Implement JWT auth" {
		t.Fatalf("expected task spec 'Implement JWT auth', got '%s'", readDag.Nodes[0].TaskSpec)
	}
	if len(readDag.Edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(readDag.Edges))
	}
}

func TestStoreReadOnlyRejectsWrite(t *testing.T) {
	path := tempStoreFile(t)

	// Create the file with a writable store first
	ws, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore (writable): %v", err)
	}
	ws.Close()

	// Open read-only
	rs, err := NewStore(path, DefaultStoreSize, false)
	if err != nil {
		t.Fatalf("NewStore (readonly): %v", err)
	}
	defer rs.Close()

	dag := NewDagData(200)
	if err := rs.Write(dag); err == nil {
		t.Fatal("expected error writing to read-only store")
	}
}

func TestStorePersistence(t *testing.T) {
	path := tempStoreFile(t)

	// Write data and close
	store1, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	dag := NewDagData(200)
	dag.Nodes = append(dag.Nodes, DagNode{
		ID:       "persist-001",
		DomainID: "data",
		TaskSpec: "Create models",
		Status:   StatusDone,
	})
	if err := store1.Write(dag); err != nil {
		t.Fatalf("Write: %v", err)
	}
	store1.Close()

	// Reopen and verify
	store2, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore reopen: %v", err)
	}
	defer store2.Close()

	readDag, _, err := store2.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if len(readDag.Nodes) != 1 {
		t.Fatalf("expected 1 node after reopen, got %d", len(readDag.Nodes))
	}
	if readDag.Nodes[0].ID != "persist-001" {
		t.Fatalf("expected node ID 'persist-001', got '%s'", readDag.Nodes[0].ID)
	}
}

func TestStoreEmptyRead(t *testing.T) {
	path := tempStoreFile(t)
	store, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	dag, _, err := store.Read()
	if err != nil {
		t.Fatalf("Read empty store: %v", err)
	}
	if len(dag.Nodes) != 0 {
		t.Fatalf("expected 0 nodes in empty store, got %d", len(dag.Nodes))
	}
}

func TestStoreSchemaVersion(t *testing.T) {
	path := tempStoreFile(t)
	store, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	dag := NewDagData(200)
	if err := store.Write(dag); err != nil {
		t.Fatalf("Write: %v", err)
	}

	readDag, _, err := store.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if readDag.Header.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("expected schema version %d, got %d", CurrentSchemaVersion, readDag.Header.SchemaVersion)
	}
}

func TestStoreConcurrentReaders(t *testing.T) {
	path := tempStoreFile(t)
	store, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	dag := NewDoresDag(t, store)

	// Open multiple read-only stores
	readers := make([]*Store, 4)
	for i := range readers {
		r, err := NewStore(path, DefaultStoreSize, false)
		if err != nil {
			t.Fatalf("NewStore reader %d: %v", i, err)
		}
		defer r.Close()
		readers[i] = r
	}

	// All readers should see the same data
	for i, r := range readers {
		readDag, _, err := r.Read()
		if err != nil {
			t.Fatalf("reader %d Read: %v", i, err)
		}
		if len(readDag.Nodes) != len(dag.Nodes) {
			t.Fatalf("reader %d: expected %d nodes, got %d", i, len(dag.Nodes), len(readDag.Nodes))
		}
	}
}

func TestStoreFileCreation(t *testing.T) {
	path := tempStoreFile(t)

	// File should not exist yet
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("expected file to not exist before NewStore")
	}

	store, err := NewStore(path, DefaultStoreSize, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	// File should now exist
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file should exist after NewStore: %v", err)
	}
	if info.Size() < int64(DefaultStoreSize) {
		t.Fatalf("file size %d should be at least %d", info.Size(), DefaultStoreSize)
	}
}

// Helper to write a test DAG to the store
func NewDoresDag(t *testing.T, store *Store) *DagData {
	t.Helper()
	dag := NewDagData(200)
	dag.Nodes = append(dag.Nodes,
		DagNode{ID: "n1", DomainID: "auth", TaskSpec: "Task 1", Status: StatusPending},
		DagNode{ID: "n2", DomainID: "data", TaskSpec: "Task 2", Status: StatusPending},
	)
	dag.Edges = append(dag.Edges, DagEdge{FromNode: "n1", ToNode: "n2", Type: EdgeDependency})
	if err := store.Write(dag); err != nil {
		t.Fatalf("Write test DAG: %v", err)
	}
	return dag
}
