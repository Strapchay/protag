package memory

import (
	"context"
	"testing"
)

func TestEmbedder_Mock(t *testing.T) {
	cfg := EmbedderConfig{
		Type: "mock",
	}

	embedFunc, err := NewEmbedder(cfg)
	if err != nil {
		t.Fatalf("NewEmbedder mock failed: %v", err)
	}

	// The mock should just return a dummy vector immediately
	vec, err := embedFunc(context.Background(), "Any text at all")
	if err != nil {
		t.Fatalf("Mock embedding failed: %v", err)
	}

	if len(vec) == 0 {
		t.Fatal("Expected non-empty vector from mock")
	}

	if vec[0] != 1.0 {
		t.Errorf("Expected leading 1.0, got %f", vec[0])
	}
}

func TestEmbedder_InvalidConfig(t *testing.T) {
	// Missing token for codename-backed backend
	cfgOracle := EmbedderConfig{
		Type: "oracle",
	}
	_, err := NewEmbedder(cfgOracle)
	if err == nil {
		t.Error("expected error for missing oracle key")
	}

	// Invalid type
	cfgInvalid := EmbedderConfig{
		Type: "not-real",
	}
	_, err = NewEmbedder(cfgInvalid)
	if err == nil {
		t.Error("expected error for invalid type")
	}
}
