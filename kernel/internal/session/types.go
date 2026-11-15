package session

import (
	"fmt"
	"time"
)

type AgentRole string

const (
	RoleSolutionArchitect AgentRole = "solution_architect"
	RoleCoordinator       AgentRole = "coordinator"
	RoleDomainAgent       AgentRole = "domain_agent"
)

type AgentSessionStatus string

const (
	StatusNew             AgentSessionStatus = "new"
	StatusIdle            AgentSessionStatus = "idle"
	StatusAwaitingUser    AgentSessionStatus = "awaiting_user"
	StatusWaitingForModel AgentSessionStatus = "waiting_for_model"
	StatusStreaming       AgentSessionStatus = "streaming_response"
	StatusRunningTool     AgentSessionStatus = "running_tool"
	StatusHandoffReady    AgentSessionStatus = "handoff_ready"
	StatusHandoffComplete AgentSessionStatus = "handoff_complete"
	StatusFailedRetryable AgentSessionStatus = "failed_retryable"
	StatusFailedTerminal  AgentSessionStatus = "failed_terminal"
	StatusStopped         AgentSessionStatus = "stopped"
)

type EventKind string

const (
	EventUserMessage       EventKind = "user_message"
	EventAssistantText     EventKind = "assistant_text"
	EventAssistantThinking EventKind = "assistant_thinking"
	EventToolStart         EventKind = "tool_start"
	EventToolEnd           EventKind = "tool_end"
	EventToolError         EventKind = "tool_error"
	EventStatus            EventKind = "status"
	EventCheckpoint        EventKind = "checkpoint"
	EventResume            EventKind = "resume"
	EventRetry             EventKind = "retry"
	EventContinue          EventKind = "continue"
	EventHandoff           EventKind = "handoff"
	EventFailure           EventKind = "failure"
)

type FailureClass string

const (
	FailureNetworkTimeout       FailureClass = "network_timeout"
	FailureProviderUnavailable  FailureClass = "provider_unavailable"
	FailureProviderAuthError    FailureClass = "provider_auth_error"
	FailureAgentProcessCrash    FailureClass = "agent_process_crash"
	FailureAgentStreamClosed    FailureClass = "agent_stream_closed"
	FailureToolFailure          FailureClass = "tool_failure"
	FailureToolTimeout          FailureClass = "tool_timeout"
	FailureContextRestoreFailed FailureClass = "context_restore_failed"
	FailureSessionCorrupt       FailureClass = "session_corrupt"
	FailureRequestCancelled     FailureClass = "request_cancelled"
	FailureUnknown              FailureClass = "unknown"
)

type SideEffectLevel string

const (
	SideEffectNone          SideEffectLevel = "none"
	SideEffectReadOnly      SideEffectLevel = "read_only"
	SideEffectWritePossible SideEffectLevel = "write_possible"
	SideEffectWriteDone     SideEffectLevel = "write_done"
	SideEffectUnknown       SideEffectLevel = "unknown"
)

type RequestStatus string

const (
	RequestPending   RequestStatus = "pending"
	RequestRunning   RequestStatus = "running"
	RequestCompleted RequestStatus = "completed"
	RequestFailed    RequestStatus = "failed"
	RequestTimedOut  RequestStatus = "timed_out"
)

type AgentSession struct {
	SessionID        string                 `json:"session_id"`
	Role             AgentRole              `json:"role"`
	AgentID          string                 `json:"agent_id"`
	Scope            string                 `json:"scope"`
	PiSessionID      string                 `json:"pi_session_id,omitempty"`
	Status           AgentSessionStatus     `json:"status"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	LastStartedAt    time.Time              `json:"last_started_at,omitempty"`
	LastResumedAt    time.Time              `json:"last_resumed_at,omitempty"`
	LastError        string                 `json:"last_error,omitempty"`
	TranscriptCursor string                 `json:"transcript_cursor,omitempty"`
	SummaryCursor    string                 `json:"summary_cursor,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

type SessionEvent struct {
	EventID         string          `json:"event_id"`
	SessionID       string          `json:"session_id"`
	RequestID       string          `json:"request_id,omitempty"`
	AttemptID       string          `json:"attempt_id,omitempty"`
	Timestamp       time.Time       `json:"timestamp"`
	Source          string          `json:"source,omitempty"`
	Audience        string          `json:"audience,omitempty"`
	Kind            EventKind       `json:"kind"`
	Role            string          `json:"role,omitempty"`
	Content         string          `json:"content,omitempty"`
	Summary         string          `json:"summary,omitempty"`
	ToolName        string          `json:"tool_name,omitempty"`
	ToolInput       string          `json:"tool_input,omitempty"`
	SideEffectLevel SideEffectLevel `json:"side_effect_level,omitempty"`
	RawPayloadRef   string          `json:"raw_payload_ref,omitempty"`
}

type SessionCheckpoint struct {
	CheckpointID   string                 `json:"checkpoint_id"`
	SessionID      string                 `json:"session_id"`
	RequestID      string                 `json:"request_id,omitempty"`
	Timestamp      time.Time              `json:"timestamp"`
	Status         AgentSessionStatus     `json:"status"`
	Summary        string                 `json:"summary,omitempty"`
	RecentEventIDs []string               `json:"recent_event_ids,omitempty"`
	OpenItems      []string               `json:"open_items,omitempty"`
	RoleMetadata   map[string]interface{} `json:"role_metadata,omitempty"`
}

type AgentRequest struct {
	RequestID       string          `json:"request_id"`
	SessionID       string          `json:"session_id"`
	UserText        string          `json:"user_text,omitempty"`
	Command         string          `json:"command,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	Status          RequestStatus   `json:"status"`
	AttemptCount    int             `json:"attempt_count"`
	LastAttemptID   string          `json:"last_attempt_id,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
	SideEffectLevel SideEffectLevel `json:"side_effect_level"`
}

type AgentRequestAttempt struct {
	AttemptID       string          `json:"attempt_id"`
	RequestID       string          `json:"request_id"`
	StartedAt       time.Time       `json:"started_at"`
	EndedAt         time.Time       `json:"ended_at,omitempty"`
	Status          RequestStatus   `json:"status"`
	PiSessionID     string          `json:"pi_session_id,omitempty"`
	FailureClass    FailureClass    `json:"failure_class,omitempty"`
	OutputEventIDs  []string        `json:"output_event_ids,omitempty"`
	SideEffectLevel SideEffectLevel `json:"side_effect_level"`
}

func RetryAllowed(level SideEffectLevel) bool {
	return level == "" || level == SideEffectNone || level == SideEffectReadOnly
}

func (s AgentSession) Validate() error {
	if s.SessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if s.Role == "" {
		return fmt.Errorf("session role is required")
	}
	if s.AgentID == "" {
		return fmt.Errorf("session agent id is required")
	}
	if s.Scope == "" {
		return fmt.Errorf("session scope is required")
	}
	if s.Status == "" {
		return fmt.Errorf("session status is required")
	}
	return nil
}

func (e SessionEvent) Validate() error {
	if e.EventID == "" {
		return fmt.Errorf("event id is required")
	}
	if e.SessionID == "" {
		return fmt.Errorf("event session id is required")
	}
	if e.Kind == "" {
		return fmt.Errorf("event kind is required")
	}
	return nil
}

func CanTransition(from, to AgentSessionStatus) bool {
	if from == "" || from == StatusNew {
		return true
	}
	if from == StatusFailedTerminal {
		return to == StatusStopped || to == StatusNew
	}
	if from == StatusHandoffComplete {
		return to == StatusHandoffComplete || to == StatusStopped
	}
	switch to {
	case StatusNew:
		return false
	case StatusIdle, StatusAwaitingUser, StatusWaitingForModel, StatusStreaming, StatusRunningTool, StatusHandoffReady, StatusHandoffComplete, StatusFailedRetryable, StatusFailedTerminal, StatusStopped:
		return true
	default:
		return false
	}
}
