package coordinator

import (
	"strings"
	"testing"

	"aion-kernel/internal/session"
)

func TestSolutionArchitectPolicyResumePromptIncludesMetadata(t *testing.T) {
	policy := solutionArchitectPolicy{bootstrap: "bootstrap"}
	prompt := policy.ResumePrompt(session.ResumeContext{
		Session: session.AgentSession{
			SessionID: "sess_1",
			Status:    session.StatusAwaitingUser,
			Metadata: map[string]interface{}{
				"user_goal":              "build a resumable architect",
				"spec_status":            architectSpecClarifying,
				"handoff_status":         "not_started",
				"open_questions":         []string{"Which files matter?"},
				"relevant_files":         []string{"docs/current/spec_resilience.md"},
				"last_architect_summary": "We need a reusable runtime.",
			},
		},
	})
	for _, want := range []string{
		"user_goal: build a resumable architect",
		"spec_status: clarifying",
		"open_questions: [Which files matter?]",
		"relevant_files: [docs/current/spec_resilience.md]",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("resume prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestSolutionArchitectPolicyFreshBootstrap(t *testing.T) {
	prompt := GenerateArchitectInstruction("")
	for _, want := range []string{
		"docs/build_spec.md",
		"If the `docs/` directory does not exist",
		"Do not ask the user to create this file manually",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("bootstrap prompt missing %q:\n%s", want, prompt)
		}
	}
	policy := solutionArchitectPolicy{bootstrap: prompt}
	meta := policy.NewSessionMetadata()
	if meta["spec_status"] != architectSpecDiscovering || meta["handoff_status"] != "not_started" {
		t.Fatalf("unexpected initial metadata: %#v", meta)
	}
}

func TestArchitectMetadataExtractionHelpers(t *testing.T) {
	questions := extractQuestions("Before building, which database should we use?\nDecision: use JSON first.")
	if len(questions) != 1 {
		t.Fatalf("expected one question, got %#v", questions)
	}
	decisions := extractDecisionLines("Decision: use JSON first.\nWe agreed to defer vector recall.")
	if len(decisions) != 2 {
		t.Fatalf("expected two decisions, got %#v", decisions)
	}
	files := extractFileRefs(`{"path":"docs/current/spec_resilience.md","other":"internal/session/runtime.go"}`)
	if len(files) != 2 {
		t.Fatalf("expected file refs, got %#v", files)
	}
}

func TestMarkBuildSpecHandoffMetadata(t *testing.T) {
	c := &PiCoordinator{}
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	c.sessionRT = session.NewRuntime(store, solutionArchitectPolicy{bootstrap: "bootstrap"}, nil)
	start, err := c.sessionRT.StartOrResume()
	if err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}

	c.MarkBuildSpecHandoff()
	c.MarkBuildSpecHandoff()
	loaded, err := store.LoadActiveSession(session.RoleSolutionArchitect, "orchestrator", "workspace")
	if err != nil {
		t.Fatalf("LoadActiveSession: %v", err)
	}
	if loaded.Status != session.StatusHandoffComplete {
		t.Fatalf("expected handoff complete, got %s", loaded.Status)
	}
	if loaded.Metadata["handoff_status"] != "complete" || loaded.Metadata["spec_status"] != "handoff_complete" {
		t.Fatalf("unexpected handoff metadata: %#v", loaded.Metadata)
	}
	if loaded.Metadata["handoff_at"] == "" {
		t.Fatalf("expected handoff timestamp: %#v", loaded.Metadata)
	}
	if _, err := store.LoadLatestCheckpoint(start.Session.SessionID); err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
}

func TestSessionStatusRestorationIncludesMetadata(t *testing.T) {
	c := &PiCoordinator{}
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	c.sessionRT = session.NewRuntime(store, solutionArchitectPolicy{bootstrap: "bootstrap"}, nil)
	if _, err := c.sessionRT.StartOrResume(); err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}
	c.updateMetadataFromUser("Build a reusable session runtime")

	status := c.SessionStatus()
	if !strings.Contains(status, "user_goal=Build a reusable session runtime") {
		t.Fatalf("status missing user goal: %s", status)
	}
}

func TestReplayArchitectHistory(t *testing.T) {
	c := &PiCoordinator{}
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	c.sessionRT = session.NewRuntime(store, solutionArchitectPolicy{bootstrap: "bootstrap"}, nil)
	if _, err := c.sessionRT.StartOrResume(); err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}
	if _, _, err := c.sessionRT.BeginRequest("hello", "", session.SideEffectNone); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	c.sessionRT.RecordAssistantText("hi")

	messages := c.ReplayArchitectHistory()
	if len(messages) == 0 {
		t.Fatal("expected replay messages")
	}
	var sawUser, sawAssistant bool
	for _, msg := range messages {
		if msg.FromAgent == "user" && msg.ToAgent == "orchestrator" {
			sawUser = true
		}
		if msg.FromAgent == "orchestrator" && msg.ToAgent == "tui" {
			sawAssistant = true
		}
	}
	if !sawUser || !sawAssistant {
		t.Fatalf("expected replayed user and assistant messages, got %#v", messages)
	}
}

func TestShutdownCheckpoint(t *testing.T) {
	c := &PiCoordinator{}
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	c.sessionRT = session.NewRuntime(store, solutionArchitectPolicy{bootstrap: "bootstrap"}, nil)
	if _, err := c.sessionRT.StartOrResume(); err != nil {
		t.Fatalf("StartOrResume: %v", err)
	}

	c.ShutdownCheckpoint()
	loaded, err := store.LoadActiveSession(session.RoleSolutionArchitect, "orchestrator", "workspace")
	if err != nil {
		t.Fatalf("LoadActiveSession: %v", err)
	}
	if loaded.Status != session.StatusStopped {
		t.Fatalf("expected stopped, got %s", loaded.Status)
	}
}
