package orchestrator

import "testing"

func TestAgentCapabilityRegistryRotatesCredentials(t *testing.T) {
	registry := newAgentCapabilityRegistry()
	first, err := registry.issue("agent-api")
	if err != nil {
		t.Fatalf("issue first capability: %v", err)
	}
	if agentID, ok := registry.resolve(first); !ok || agentID != "agent-api" {
		t.Fatalf("resolve first capability = %q, %v", agentID, ok)
	}

	second, err := registry.issue("agent-api")
	if err != nil {
		t.Fatalf("issue replacement capability: %v", err)
	}
	if first == second {
		t.Fatal("replacement capability did not rotate")
	}
	if _, ok := registry.resolve(first); ok {
		t.Fatal("previous capability remained valid")
	}
	if agentID, ok := registry.resolve(second); !ok || agentID != "agent-api" {
		t.Fatalf("resolve replacement capability = %q, %v", agentID, ok)
	}

	registry.clear()
	if _, ok := registry.resolve(second); ok {
		t.Fatal("capability remained valid after clear")
	}
}
