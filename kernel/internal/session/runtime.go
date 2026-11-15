package session

import (
	"fmt"
	"strings"
	"time"
)

type Policy interface {
	Role() AgentRole
	AgentID() string
	Scope() string
	NewSessionMetadata() map[string]interface{}
	BootstrapPrompt() string
	ResumePrompt(ctx ResumeContext) string
	CheckpointSummary(events []SessionEvent) string
}

type Runtime struct {
	store    Store
	policy   Policy
	session  *AgentSession
	request  *AgentRequest
	attempt  *AgentRequestAttempt
	statusFn func(text, level string)
}

const (
	ResumeEventLimit     = 40
	CheckpointEventLimit = 25
)

type ResumeContext struct {
	Session     AgentSession
	Events      []SessionEvent
	Checkpoint  *SessionCheckpoint
	LastRequest *AgentRequest
	LastAttempt *AgentRequestAttempt
}

type StartResult struct {
	Session  *AgentSession
	Prompt   string
	Resumed  bool
	Recovery bool
}

func NewRuntime(store Store, policy Policy, statusFn func(text, level string)) *Runtime {
	return &Runtime{store: store, policy: policy, statusFn: statusFn}
}

func (r *Runtime) StartOrResume() (*StartResult, error) {
	now := time.Now().UTC()
	sess, err := r.store.LoadActiveSession(r.policy.Role(), r.policy.AgentID(), r.policy.Scope())
	if err != nil && err != ErrNotFound {
		return r.startRecoverySession(now, err)
	}
	if err == ErrNotFound {
		sess = &AgentSession{
			SessionID:     newID("sess"),
			Role:          r.policy.Role(),
			AgentID:       r.policy.AgentID(),
			Scope:         r.policy.Scope(),
			Status:        StatusNew,
			CreatedAt:     now,
			UpdatedAt:     now,
			LastStartedAt: now,
			Metadata:      r.policy.NewSessionMetadata(),
		}
		if err := r.store.CreateSession(sess); err != nil {
			return nil, fmt.Errorf("session runtime: create session: %w", err)
		}
		r.session = sess
		r.RecordStatus("Solution Architect session started", "ok")
		return &StartResult{Session: sess, Prompt: r.policy.BootstrapPrompt(), Resumed: false}, nil
	}

	sess.LastResumedAt = now
	sess.LastStartedAt = now
	sess.UpdatedAt = now
	if sess.Status == StatusStopped || sess.Status == "" {
		sess.Status = StatusIdle
	}
	if err := r.store.SaveSession(sess); err != nil {
		return nil, fmt.Errorf("session runtime: save resumed session: %w", err)
	}
	r.session = sess

	req, _ := r.store.LoadLastRequest(sess.SessionID)
	var attempt *AgentRequestAttempt
	if req != nil {
		attempt, _ = r.store.LoadLastAttempt(req.RequestID)
		r.request = req
		r.attempt = attempt
	}
	if req != nil && (req.Status == RequestRunning || req.Status == RequestPending) && sess.Status != StatusHandoffComplete {
		sess.Status = StatusFailedRetryable
		sess.LastError = "restored with in-flight request; retry or continue is available"
		sess.UpdatedAt = time.Now().UTC()
		_ = r.store.SaveSession(sess)
		r.RecordStatus("Solution Architect restored with an in-flight request; /retry or /continue available", "warn")
	}

	r.RecordEvent(EventResume, "runtime", "agent", "session restored", "Session restored", "", "", SideEffectNone)
	r.RecordStatus("Solution Architect session restored", "ok")
	return &StartResult{Session: sess, Resumed: true}, nil
}

func (r *Runtime) startRecoverySession(now time.Time, restoreErr error) (*StartResult, error) {
	sess := &AgentSession{
		SessionID:     newID("sess_recovery"),
		Role:          r.policy.Role(),
		AgentID:       r.policy.AgentID(),
		Scope:         r.policy.Scope(),
		Status:        StatusFailedTerminal,
		CreatedAt:     now,
		UpdatedAt:     now,
		LastStartedAt: now,
		LastError:     fmt.Sprintf("restore failed: %v", restoreErr),
		Metadata:      r.policy.NewSessionMetadata(),
	}
	if err := r.store.CreateSession(sess); err != nil {
		return nil, fmt.Errorf("session runtime: create recovery session: %w", err)
	}
	r.session = sess
	r.RecordEvent(EventFailure, "runtime", "tui", sess.LastError, string(FailureSessionCorrupt), "", "", SideEffectUnknown)
	r.RecordStatus("Solution Architect session restore failed; recovery session created", "error")
	return &StartResult{Session: sess, Resumed: true, Recovery: true}, nil
}

func (r *Runtime) BeginRequest(text, command string, sideEffect SideEffectLevel) (*AgentRequest, *AgentRequestAttempt, error) {
	if r.session == nil {
		return nil, nil, fmt.Errorf("session runtime: no active session")
	}
	now := time.Now().UTC()
	req := &AgentRequest{
		RequestID:       newID("req"),
		SessionID:       r.session.SessionID,
		UserText:        text,
		Command:         command,
		CreatedAt:       now,
		Status:          RequestRunning,
		AttemptCount:    1,
		SideEffectLevel: sideEffect,
	}
	attempt := &AgentRequestAttempt{
		AttemptID:       req.RequestID + "_attempt_1",
		RequestID:       req.RequestID,
		StartedAt:       now,
		Status:          RequestRunning,
		SideEffectLevel: sideEffect,
	}
	req.LastAttemptID = attempt.AttemptID
	if err := r.store.SaveRequest(*req); err != nil {
		return nil, nil, err
	}
	if err := r.store.SaveAttempt(*attempt); err != nil {
		return nil, nil, err
	}
	r.request = req
	r.attempt = attempt
	r.setSessionStatus(StatusWaitingForModel, "")
	r.RecordEvent(EventUserMessage, "user", "agent", text, "", "", "", sideEffect)
	return req, attempt, nil
}

func (r *Runtime) RetryLastRequest() (string, error) {
	if r.session == nil {
		return "", fmt.Errorf("session runtime: no active session")
	}
	req, err := r.store.LoadLastRequest(r.session.SessionID)
	if err != nil {
		return "", err
	}
	if !RetryAllowed(req.SideEffectLevel) {
		return "", fmt.Errorf("last request has side-effect level %q; retry is not automatic", req.SideEffectLevel)
	}
	req.AttemptCount++
	req.Status = RequestRunning
	req.LastError = ""
	attempt := AgentRequestAttempt{
		AttemptID:       fmt.Sprintf("%s_attempt_%d", req.RequestID, req.AttemptCount),
		RequestID:       req.RequestID,
		StartedAt:       time.Now().UTC(),
		Status:          RequestRunning,
		SideEffectLevel: req.SideEffectLevel,
	}
	req.LastAttemptID = attempt.AttemptID
	if err := r.store.SaveRequest(*req); err != nil {
		return "", err
	}
	if err := r.store.SaveAttempt(attempt); err != nil {
		return "", err
	}
	r.request = req
	r.attempt = &attempt
	r.RecordEvent(EventRetry, "user", "agent", req.UserText, "Retrying last request", "", "", req.SideEffectLevel)
	r.setSessionStatus(StatusWaitingForModel, "")
	return req.UserText, nil
}

func (r *Runtime) ContinuePrompt() (string, error) {
	if r.session == nil {
		return "", fmt.Errorf("session runtime: no active session")
	}
	text := "Continue from the current restored Solution Architect session. Do not restart discovery or repeat prior answers unless necessary."
	now := time.Now().UTC()
	req := &AgentRequest{
		RequestID:       newID("req"),
		SessionID:       r.session.SessionID,
		UserText:        text,
		Command:         "/continue",
		CreatedAt:       now,
		Status:          RequestRunning,
		AttemptCount:    1,
		SideEffectLevel: SideEffectNone,
	}
	attempt := &AgentRequestAttempt{
		AttemptID:       req.RequestID + "_attempt_1",
		RequestID:       req.RequestID,
		StartedAt:       now,
		Status:          RequestRunning,
		SideEffectLevel: SideEffectNone,
	}
	req.LastAttemptID = attempt.AttemptID
	_ = r.store.SaveRequest(*req)
	_ = r.store.SaveAttempt(*attempt)
	r.request = req
	r.attempt = attempt
	r.RecordEvent(EventContinue, "user", "agent", text, "Continue requested", "", "", SideEffectNone)
	r.setSessionStatus(StatusWaitingForModel, "")
	return text, nil
}

func (r *Runtime) MarkHandoff(summary string) {
	r.UpdateMetadata(func(metadata map[string]interface{}) {
		metadata["handoff_status"] = "complete"
		metadata["spec_status"] = "handoff_complete"
		metadata["handoff_summary"] = summary
		metadata["handoff_at"] = time.Now().UTC().Format(time.RFC3339)
	})
	r.RecordEvent(EventHandoff, "user", "coordinator", summary, "Build spec handoff", "", "", SideEffectNone)
	r.setSessionStatus(StatusHandoffComplete, "")
	r.SaveCheckpoint()
}

func (r *Runtime) ReconcilePrompt() (string, error) {
	if r.session == nil {
		return "", fmt.Errorf("session runtime: no active session")
	}
	events, _ := r.store.ListRecentEvents(r.session.SessionID, ResumeEventLimit)
	checkpoint, _ := r.store.LoadLatestCheckpoint(r.session.SessionID)
	req, _ := r.store.LoadLastRequest(r.session.SessionID)
	var attempt *AgentRequestAttempt
	if req != nil {
		attempt, _ = r.store.LoadLastAttempt(req.RequestID)
	}
	ctx := ResumeContext{
		Session:     *r.session,
		Events:      events,
		Checkpoint:  checkpoint,
		LastRequest: req,
		LastAttempt: attempt,
	}
	r.RecordEvent(EventResume, "user", "agent", "forced session reconciliation requested", "Forced resume reconciliation", "", "", SideEffectNone)
	return r.policy.ResumePrompt(ctx), nil
}

func (r *Runtime) ReplayEvents(limit int) ([]SessionEvent, error) {
	if r.session == nil {
		return nil, fmt.Errorf("session runtime: no active session")
	}
	return r.store.ListRecentEvents(r.session.SessionID, limit)
}

func (r *Runtime) CompleteActiveRequest() {
	if r.request == nil {
		r.setSessionStatus(StatusAwaitingUser, "")
		return
	}
	now := time.Now().UTC()
	r.request.Status = RequestCompleted
	if r.attempt != nil {
		r.attempt.Status = RequestCompleted
		r.attempt.EndedAt = now
		_ = r.store.SaveAttempt(*r.attempt)
	}
	_ = r.store.SaveRequest(*r.request)
	r.setSessionStatus(StatusAwaitingUser, "")
	r.SaveCheckpoint()
}

func (r *Runtime) FailActiveRequest(class FailureClass, msg string, retryable bool) {
	if r.request != nil {
		if class == FailureNetworkTimeout {
			r.request.Status = RequestTimedOut
		} else {
			r.request.Status = RequestFailed
		}
		r.request.LastError = msg
		_ = r.store.SaveRequest(*r.request)
	}
	if r.attempt != nil {
		r.attempt.Status = RequestFailed
		r.attempt.EndedAt = time.Now().UTC()
		r.attempt.FailureClass = class
		_ = r.store.SaveAttempt(*r.attempt)
	}
	status := StatusFailedTerminal
	if retryable {
		status = StatusFailedRetryable
	}
	r.RecordEvent(EventFailure, "runtime", "tui", msg, string(class), "", "", SideEffectUnknown)
	r.setSessionStatus(status, msg)
}

func (r *Runtime) RecordStatus(text, level string) {
	r.RecordEvent(EventStatus, "runtime", "tui", text, "", "", "", SideEffectNone)
	if r.statusFn != nil {
		r.statusFn(text, level)
	}
}

func (r *Runtime) RecordAssistantText(text string) {
	r.RecordEvent(EventAssistantText, r.policy.AgentID(), "user", text, "", "", "", SideEffectNone)
	r.setSessionStatus(StatusStreaming, "")
	r.SaveCheckpoint()
}

func (r *Runtime) RecordAssistantThinking(text string) {
	r.RecordEvent(EventAssistantThinking, r.policy.AgentID(), "user", text, "", "", "", SideEffectNone)
	r.setSessionStatus(StatusStreaming, "")
}

func (r *Runtime) RecordToolStart(name, input, summary string, sideEffect SideEffectLevel) {
	r.RecordEvent(EventToolStart, r.policy.AgentID(), "tool", "", summary, name, input, sideEffect)
	r.setSessionStatus(StatusRunningTool, "")
}

func (r *Runtime) RecordEvent(kind EventKind, source, audience, content, summary, toolName, toolInput string, sideEffect SideEffectLevel) {
	if r.session == nil {
		return
	}
	event := SessionEvent{
		EventID:         newID("evt"),
		SessionID:       r.session.SessionID,
		Timestamp:       time.Now().UTC(),
		Source:          source,
		Audience:        audience,
		Kind:            kind,
		Content:         content,
		Summary:         summary,
		ToolName:        toolName,
		ToolInput:       toolInput,
		SideEffectLevel: sideEffect,
	}
	if r.request != nil {
		event.RequestID = r.request.RequestID
	}
	if r.attempt != nil {
		event.AttemptID = r.attempt.AttemptID
	}
	_ = r.store.AppendEvent(event)
}

func (r *Runtime) SaveCheckpoint() {
	if r.session == nil {
		return
	}
	events, _ := r.store.ListRecentEvents(r.session.SessionID, CheckpointEventLimit)
	var ids []string
	for _, event := range events {
		ids = append(ids, event.EventID)
	}
	checkpoint := SessionCheckpoint{
		CheckpointID:   newID("chk"),
		SessionID:      r.session.SessionID,
		Timestamp:      time.Now().UTC(),
		Status:         r.session.Status,
		Summary:        r.policy.CheckpointSummary(events),
		RecentEventIDs: ids,
		RoleMetadata:   r.session.Metadata,
	}
	if r.request != nil {
		checkpoint.RequestID = r.request.RequestID
	}
	r.UpdateMetadata(func(metadata map[string]interface{}) {
		if checkpoint.Summary != "" {
			metadata["rolling_summary"] = checkpoint.Summary
		}
		metadata["last_checkpoint_at"] = checkpoint.Timestamp.Format(time.RFC3339)
	})
	_ = r.store.SaveCheckpoint(checkpoint)
}

func (r *Runtime) ShutdownCheckpoint() {
	if r.session == nil {
		return
	}
	r.RecordEvent(EventCheckpoint, "runtime", "storage", "shutdown checkpoint", "Shutdown checkpoint", "", "", SideEffectNone)
	r.SaveCheckpoint()
	r.setSessionStatus(StatusStopped, "")
}

func (r *Runtime) StatusText() string {
	if r.session == nil {
		return "No active session"
	}
	parts := []string{
		"session=" + r.session.SessionID,
		"role=" + string(r.session.Role),
		"status=" + string(r.session.Status),
	}
	if r.request != nil {
		parts = append(parts, "last_request="+r.request.RequestID, "request_status="+string(r.request.Status))
	}
	if r.session.LastError != "" {
		parts = append(parts, "last_error="+r.session.LastError)
	}
	for _, key := range []string{"spec_status", "handoff_status", "build_spec_path", "user_goal", "open_questions", "relevant_files"} {
		if value, ok := r.session.Metadata[key]; ok {
			parts = append(parts, fmt.Sprintf("%s=%v", key, value))
		}
	}
	return strings.Join(parts, "\n")
}

func (r *Runtime) UpdateMetadata(update func(map[string]interface{})) {
	if r.session == nil || update == nil {
		return
	}
	if r.session.Metadata == nil {
		r.session.Metadata = map[string]interface{}{}
	}
	update(r.session.Metadata)
	r.session.UpdatedAt = time.Now().UTC()
	_ = r.store.SaveSession(r.session)
}

func (r *Runtime) setSessionStatus(status AgentSessionStatus, lastErr string) {
	if r.session == nil {
		return
	}
	if !CanTransition(r.session.Status, status) {
		return
	}
	r.session.Status = status
	r.session.LastError = lastErr
	r.session.UpdatedAt = time.Now().UTC()
	_ = r.store.SaveSession(r.session)
}

func newID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
}
