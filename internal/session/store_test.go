package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileStoreSessionLifecycle(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	sess := &AgentSession{
		SessionID: "sess_test",
		Role:      RoleSolutionArchitect,
		AgentID:   "orchestrator",
		Scope:     "workspace",
		Status:    StatusNew,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := store.CreateSession(sess); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	loaded, err := store.LoadActiveSession(RoleSolutionArchitect, "orchestrator", "workspace")
	if err != nil {
		t.Fatalf("LoadActiveSession: %v", err)
	}
	if loaded.SessionID != sess.SessionID {
		t.Fatalf("expected session %s, got %s", sess.SessionID, loaded.SessionID)
	}

	if err := store.AppendEvent(SessionEvent{
		EventID:   "evt_1",
		SessionID: sess.SessionID,
		Timestamp: time.Now().UTC(),
		Kind:      EventUserMessage,
		Content:   "hello",
	}); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	events, err := store.ListRecentEvents(sess.SessionID, 10)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != 1 || events[0].Content != "hello" {
		t.Fatalf("unexpected events: %#v", events)
	}
}

func TestRuntimeStartOrResume(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	policy := testPolicy{}
	rt := NewRuntime(store, policy, nil)

	first, err := rt.StartOrResume()
	if err != nil {
		t.Fatalf("first StartOrResume: %v", err)
	}
	if first.Resumed {
		t.Fatal("first start should not be resumed")
	}
	if first.Prompt != "bootstrap" {
		t.Fatalf("expected bootstrap prompt, got %q", first.Prompt)
	}
	if _, _, err := rt.BeginRequest("build a thing", "", SideEffectNone); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	rt.RecordAssistantText("ok")
	rt.CompleteActiveRequest()

	rt2 := NewRuntime(store, policy, nil)
	second, err := rt2.StartOrResume()
	if err != nil {
		t.Fatalf("second StartOrResume: %v", err)
	}
	if !second.Resumed {
		t.Fatal("second start should resume existing session")
	}
	if second.Prompt != "" {
		t.Fatalf("normal resume should not inject a prompt, got %q", second.Prompt)
	}
	prompt, err := rt2.ReconcilePrompt()
	if err != nil {
		t.Fatalf("ReconcilePrompt: %v", err)
	}
	if !strings.Contains(prompt, "build a thing") {
		t.Fatalf("reconcile prompt did not include prior request: %q", prompt)
	}
}

func TestRuntimeRetrySideEffectGuard(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	rt := NewRuntime(store, testPolicy{}, nil)
	if _, err := rt.StartOrResume(); err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}
	if _, _, err := rt.BeginRequest("write files", "", SideEffectWriteDone); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	if _, err := rt.RetryLastRequest(); err == nil {
		t.Fatal("expected retry to be rejected for write_done side effect")
	}
}

func TestRuntimeContinueCreatesAuditableRequest(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	rt := NewRuntime(store, testPolicy{}, nil)
	start, err := rt.StartOrResume()
	if err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}

	if _, err := rt.ContinuePrompt(); err != nil {
		t.Fatalf("ContinuePrompt: %v", err)
	}
	req, err := store.LoadLastRequest(start.Session.SessionID)
	if err != nil {
		t.Fatalf("LoadLastRequest: %v", err)
	}
	if req.Command != "/continue" || req.Status != RequestRunning {
		t.Fatalf("unexpected continue request: %#v", req)
	}
	attempt, err := store.LoadLastAttempt(req.RequestID)
	if err != nil {
		t.Fatalf("LoadLastAttempt: %v", err)
	}
	if attempt.Status != RequestRunning {
		t.Fatalf("unexpected continue attempt: %#v", attempt)
	}
}

func TestRuntimeCheckpointAndStatusIncludeMetadata(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	rt := NewRuntime(store, testPolicy{}, nil)
	start, err := rt.StartOrResume()
	if err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}
	rt.UpdateMetadata(func(metadata map[string]interface{}) {
		metadata["user_goal"] = "ship session resilience"
	})
	rt.RecordAssistantText("summary")

	checkpoint, err := store.LoadLatestCheckpoint(start.Session.SessionID)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if checkpoint.Summary != "checkpoint" {
		t.Fatalf("unexpected checkpoint summary: %q", checkpoint.Summary)
	}
	if !strings.Contains(rt.StatusText(), "user_goal=ship session resilience") {
		t.Fatalf("status did not include metadata: %q", rt.StatusText())
	}
}

func TestValidationAndStatusTransitions(t *testing.T) {
	if err := (AgentSession{}).Validate(); err == nil {
		t.Fatal("expected invalid empty session")
	}
	if !CanTransition(StatusWaitingForModel, StatusStreaming) {
		t.Fatal("expected waiting_for_model -> streaming to be allowed")
	}
	if CanTransition(StatusHandoffComplete, StatusWaitingForModel) {
		t.Fatal("expected handoff_complete -> waiting_for_model to be rejected")
	}
}

func TestRuntimeCorruptActiveSessionCreatesRecoverySession(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	sess := &AgentSession{
		SessionID: "sess_corrupt",
		Role:      RoleSolutionArchitect,
		AgentID:   "orchestrator",
		Scope:     "workspace",
		Status:    StatusAwaitingUser,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := store.CreateSession(sess); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := os.WriteFile(filepath.Join(store.sessionDir(sess.SessionID), "session.json"), []byte("{not-json"), 0644); err != nil {
		t.Fatalf("corrupt session: %v", err)
	}

	var statuses []string
	rt := NewRuntime(store, testPolicy{}, func(text, level string) {
		statuses = append(statuses, level+":"+text)
	})
	result, err := rt.StartOrResume()
	if err != nil {
		t.Fatalf("StartOrResume recovery: %v", err)
	}
	if !result.Resumed || result.Session.Status != StatusFailedTerminal {
		t.Fatalf("expected failed recovery session, got %#v", result.Session)
	}
	if !strings.Contains(result.Session.LastError, "restore failed") {
		t.Fatalf("expected restore error, got %q", result.Session.LastError)
	}
	if len(statuses) == 0 || !strings.Contains(statuses[len(statuses)-1], "restore failed") {
		t.Fatalf("expected restore status, got %#v", statuses)
	}
}

func TestRuntimeShutdownCheckpointAndRollingSummary(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	rt := NewRuntime(store, testPolicy{}, nil)
	start, err := rt.StartOrResume()
	if err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}
	rt.RecordAssistantText("summary")
	rt.ShutdownCheckpoint()

	loaded, err := store.LoadActiveSession(RoleSolutionArchitect, "orchestrator", "workspace")
	if err != nil {
		t.Fatalf("LoadActiveSession: %v", err)
	}
	if loaded.Status != StatusStopped {
		t.Fatalf("expected stopped session, got %s", loaded.Status)
	}
	if loaded.Metadata["rolling_summary"] != "checkpoint" {
		t.Fatalf("expected rolling summary metadata, got %#v", loaded.Metadata)
	}
	events, err := store.ListRecentEvents(start.Session.SessionID, 1)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != 1 || events[0].Kind != EventCheckpoint {
		t.Fatalf("expected final checkpoint event, got %#v", events)
	}
}

func TestRuntimeResumeEventRetentionLimit(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	rt := NewRuntime(store, testPolicy{}, nil)
	start, err := rt.StartOrResume()
	if err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}
	for i := 0; i < ResumeEventLimit+5; i++ {
		if err := store.AppendEvent(SessionEvent{
			EventID:   newID("evt_test"),
			SessionID: start.Session.SessionID,
			Timestamp: time.Now().UTC(),
			Kind:      EventUserMessage,
			Content:   "event",
		}); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
	}
	events, err := store.ListRecentEvents(start.Session.SessionID, ResumeEventLimit)
	if err != nil {
		t.Fatalf("ListRecentEvents: %v", err)
	}
	if len(events) != ResumeEventLimit {
		t.Fatalf("expected %d events, got %d", ResumeEventLimit, len(events))
	}
}

func TestRuntimeClassifiesInFlightRequestOnResume(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	rt := NewRuntime(store, testPolicy{}, nil)
	if _, err := rt.StartOrResume(); err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}
	if _, _, err := rt.BeginRequest("unfinished", "", SideEffectNone); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}

	rt2 := NewRuntime(store, testPolicy{}, nil)
	start, err := rt2.StartOrResume()
	if err != nil {
		t.Fatalf("StartOrResume resume: %v", err)
	}
	if start.Session.Status != StatusFailedRetryable {
		t.Fatalf("expected failed_retryable, got %s", start.Session.Status)
	}
	if !strings.Contains(start.Session.LastError, "in-flight") {
		t.Fatalf("expected in-flight last error, got %q", start.Session.LastError)
	}
}

type testPolicy struct{}

func (testPolicy) Role() AgentRole { return RoleSolutionArchitect }
func (testPolicy) AgentID() string { return "orchestrator" }
func (testPolicy) Scope() string   { return "workspace" }
func (testPolicy) NewSessionMetadata() map[string]interface{} {
	return map[string]interface{}{"test": true}
}
func (testPolicy) BootstrapPrompt() string { return "bootstrap" }
func (testPolicy) ResumePrompt(ctx ResumeContext) string {
	var b strings.Builder
	b.WriteString("resume\n")
	if ctx.LastRequest != nil {
		b.WriteString(ctx.LastRequest.UserText)
	}
	return b.String()
}
func (testPolicy) CheckpointSummary(events []SessionEvent) string {
	return "checkpoint"
}
