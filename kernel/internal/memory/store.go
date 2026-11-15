package memory

import (
	"context"
	"fmt"

	"github.com/philippgille/chromem-go"
)

// MemoryEntry represents a single semantic memory record.
type MemoryEntry struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Vector    []float32 `json:"vector,omitempty"` // populated if using a mock/custom embedder before write
	AgentID   string    `json:"agent_id"`
	TaskID    string    `json:"task_id"`
	ProjectID string    `json:"project_id"`
	Timestamp int64     `json:"timestamp"`
}

// Store defines the interface for semantic memory storage.
type Store interface {
	Write(ctx context.Context, entry MemoryEntry) error
	Query(ctx context.Context, text string, topK int) ([]MemoryEntry, error)
	// QueryVector is an optional method if we already have the embedding
	QueryVector(ctx context.Context, vector []float32, topK int) ([]MemoryEntry, error)
	Close() error
}

type chromemStore struct {
	db         *chromem.DB
	collection *chromem.Collection
}

// NewStore creates or opens a Persistent Chromem-Go Memory Store.
func NewStore(dbPath string, embedder chromem.EmbeddingFunc) (Store, error) {
	// Initialize persistent DB. Chromem handles dir creation and loading state.
	db, err := chromem.NewPersistentDB(dbPath, false)
	if err != nil {
		return nil, fmt.Errorf("memory: init chromem db: %w", err)
	}

	// Create or get the semantic memory collection
	collectionName := "semantic_memory"

	collection, err := db.GetOrCreateCollection(collectionName, nil, embedder)
	if err != nil {
		return nil, fmt.Errorf("memory: get or create collection: %w", err)
	}

	return &chromemStore{
		db:         db,
		collection: collection,
	}, nil
}

func (s *chromemStore) Write(ctx context.Context, entry MemoryEntry) error {
	metadata := map[string]string{
		"agent_id":   entry.AgentID,
		"task_id":    entry.TaskID,
		"project_id": entry.ProjectID,
		"timestamp":  fmt.Sprintf("%d", entry.Timestamp),
	}

	doc := chromem.Document{
		ID:       entry.ID,
		Content:  entry.Text,
		Metadata: metadata,
	}

	// If a custom vector was provided manually, inject it so the embedder isn't called
	if len(entry.Vector) > 0 {
		doc.Embedding = entry.Vector
	}

	err := s.collection.AddDocument(ctx, doc)
	if err != nil {
		return fmt.Errorf("memory: add document: %w", err)
	}

	return nil
}

func (s *chromemStore) Query(ctx context.Context, text string, topK int) ([]MemoryEntry, error) {
	count := s.collection.Count()
	if count == 0 {
		return []MemoryEntry{}, nil
	}
	if topK > count {
		topK = count
	}

	// Chromem automatically Embeds the text using the configured EmbeddingFunc
	results, err := s.collection.Query(ctx, text, topK, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("memory: query exact text: %w", err)
	}

	return s.mapResults(results), nil
}

func (s *chromemStore) QueryVector(ctx context.Context, vector []float32, topK int) ([]MemoryEntry, error) {
	count := s.collection.Count()
	if count == 0 {
		return []MemoryEntry{}, nil
	}
	if topK > count {
		topK = count
	}

	results, err := s.collection.QueryEmbedding(ctx, vector, topK, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("memory: query exact vector: %w", err)
	}

	return s.mapResults(results), nil
}

func (s *chromemStore) mapResults(docs []chromem.Result) []MemoryEntry {
	var entries []MemoryEntry
	for _, res := range docs {
		// Safely extract metadata
		tsStr, _ := res.Metadata["timestamp"]
		var ts int64
		fmt.Sscanf(tsStr, "%d", &ts)

		entries = append(entries, MemoryEntry{
			ID:        res.ID,
			Text:      res.Content,
			Vector:    res.Embedding,
			AgentID:   res.Metadata["agent_id"],
			TaskID:    res.Metadata["task_id"],
			ProjectID: res.Metadata["project_id"],
			Timestamp: ts,
		})
	}
	return entries
}

func (s *chromemStore) Close() error {
	// Chromem-go persists to disk on every operation (or flush).
	// We can explicitly clear memory cache if desired, but there's no Close method on DB.
	return nil
}
