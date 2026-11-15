package supervisor

import "testing"

func TestShouldPersistRawEventDropsAmplifiedMessageUpdates(t *testing.T) {
	if shouldPersistRawEvent("message_update") {
		t.Fatal("message_update should not be persisted to the diagnostic raw log")
	}
	for _, eventType := range []string{"message_end", "tool_execution_start", "tool_execution_end", "agent_end"} {
		if !shouldPersistRawEvent(eventType) {
			t.Fatalf("diagnostic event %q was dropped", eventType)
		}
	}
}
