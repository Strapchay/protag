package orchestrator

import (
	"testing"
)

func TestExecutionJournalPersistsRecentEvents(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 3; i++ {
		if err := appendExecutionJournalEvent(root, ExecutionJournalEvent{
			Kind:     "task_updated",
			Severity: "info",
			NodeID:   "node",
			Summary:  "event",
		}); err != nil {
			t.Fatalf("appendExecutionJournalEvent: %v", err)
		}
	}
	events, err := loadRecentExecutionJournalEvents(root, 2)
	if err != nil {
		t.Fatalf("loadRecentExecutionJournalEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0].EventID == "" || events[0].Timestamp.IsZero() {
		t.Fatalf("event metadata was not populated: %#v", events[0])
	}
}

func TestRecoveryRegistryUpsertAndResolve(t *testing.T) {
	root := t.TempDir()
	record := RecoveryRecord{
		Kind:      "task_failed",
		Severity:  "error",
		Status:    "open",
		NodeID:    "api-node",
		AgentID:   "agent-api",
		Summary:   "Task failed.",
		LastError: "first",
	}
	if err := upsertRecoveryRecord(root, record); err != nil {
		t.Fatalf("upsertRecoveryRecord: %v", err)
	}
	record.LastError = "second"
	if err := upsertRecoveryRecord(root, record); err != nil {
		t.Fatalf("second upsertRecoveryRecord: %v", err)
	}
	records, err := loadRecoveryRegistry(root)
	if err != nil {
		t.Fatalf("loadRecoveryRegistry: %v", err)
	}
	if len(records) != 1 || records[0].LastError != "second" {
		t.Fatalf("unexpected records after upsert: %#v", records)
	}
	if err := markRecoveryResolved(root, "task_failed", "api-node", ""); err != nil {
		t.Fatalf("markRecoveryResolved: %v", err)
	}
	records, _ = loadRecoveryRegistry(root)
	if records[0].Status != "recovered" {
		t.Fatalf("record status = %s, want recovered", records[0].Status)
	}
}
