package coordination

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusRequested           Status = "Requested"
	StatusProduced            Status = "Produced"
	StatusCorrectionRequested Status = "CorrectionRequested"
	StatusFulfilled           Status = "Fulfilled"
	StatusFailed              Status = "Failed"
	StatusExpired             Status = "Expired"
	StatusCancelled           Status = "Cancelled"
)

func (s Status) Terminal() bool {
	return s == StatusFulfilled || s == StatusFailed || s == StatusExpired || s == StatusCancelled
}

type Role string

const (
	RoleAgent  Role = "agent"
	RoleRunner Role = "runner"
	RoleAdmin  Role = "admin"
)

type Principal struct {
	Role    Role     `json:"role"`
	AgentID string   `json:"agent_id,omitempty"`
	NodeIDs []string `json:"node_ids,omitempty"`
}

type ContractDetails struct {
	Name               string          `json:"name"`
	Kind               string          `json:"kind"`
	Language           string          `json:"language,omitempty"`
	PublicEntrypoint   string          `json:"public_entrypoint,omitempty"`
	Signature          json.RawMessage `json:"signature,omitempty"`
	Preconditions      []string        `json:"preconditions,omitempty"`
	Postconditions     []string        `json:"postconditions,omitempty"`
	ErrorBehavior      []string        `json:"error_behavior,omitempty"`
	Compatibility      string          `json:"compatibility,omitempty"`
	Acceptance         []string        `json:"acceptance,omitempty"`
	IntegrationFixture json.RawMessage `json:"integration_fixture,omitempty"`
}

type TermsStatus string

const (
	TermsProposed TermsStatus = "Proposed"
	TermsAgreed   TermsStatus = "Agreed"
)

// ContractTerms is one immutable negotiation revision. The contract details
// remain machine-readable while obligations and tradeoffs make bilateral
// changes explicit before an artifact is produced.
type ContractTerms struct {
	ContractID          string          `json:"contract_id"`
	Revision            int64           `json:"revision"`
	ParentRevision      int64           `json:"parent_revision,omitempty"`
	ProposerAgentID     string          `json:"proposer_agent_id"`
	Details             ContractDetails `json:"details"`
	ProducerObligations []string        `json:"producer_obligations,omitempty"`
	ConsumerObligations []string        `json:"consumer_obligations,omitempty"`
	Tradeoffs           []string        `json:"tradeoffs,omitempty"`
	Rationale           string          `json:"rationale,omitempty"`
	Status              TermsStatus     `json:"status"`
	AcceptedBy          []string        `json:"accepted_by,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
}

type Artifact struct {
	Digest    string    `json:"digest"`
	Size      int64     `json:"size"`
	MediaType string    `json:"media_type,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Validation struct {
	Accepted bool            `json:"accepted"`
	Evidence json.RawMessage `json:"evidence,omitempty"`
	Reason   string          `json:"reason,omitempty"`
	ActorID  string          `json:"actor_id"`
	At       time.Time       `json:"at"`
}

type Contract struct {
	ID                string          `json:"id"`
	ProducerAgentID   string          `json:"producer_agent_id"`
	ProducerNodeID    string          `json:"producer_node_id"`
	ConsumerAgentID   string          `json:"consumer_agent_id"`
	ConsumerNodeID    string          `json:"consumer_node_id"`
	Details           ContractDetails `json:"details"`
	Status            Status          `json:"status"`
	Revision          int64           `json:"revision"`
	HardWait          bool            `json:"hard_wait"`
	Artifact          *Artifact       `json:"artifact,omitempty"`
	Validation        *Validation     `json:"validation,omitempty"`
	CorrectionReason  string          `json:"correction_reason,omitempty"`
	TerminalReason    string          `json:"terminal_reason,omitempty"`
	CompletionPending bool            `json:"completion_pending,omitempty"`
	ExpiresAt         *time.Time      `json:"expires_at,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type RequestCommand struct {
	CommandID string          `json:"command_id,omitempty"`
	Contract  Contract        `json:"contract"`
	Details   json.RawMessage `json:"details,omitempty"`
}

type PublishCommand struct {
	CommandID        string          `json:"command_id,omitempty"`
	ContractID       string          `json:"contract_id"`
	ExpectedRevision int64           `json:"expected_revision"`
	Content          []byte          `json:"content"`
	MediaType        string          `json:"media_type,omitempty"`
	Evidence         json.RawMessage `json:"evidence,omitempty"`
}

type ValidateCommand struct {
	CommandID  string          `json:"command_id,omitempty"`
	ContractID string          `json:"contract_id"`
	Revision   int64           `json:"revision"`
	Evidence   json.RawMessage `json:"evidence,omitempty"`
}

type CorrectCommand struct {
	CommandID  string `json:"command_id,omitempty"`
	ContractID string `json:"contract_id"`
	Revision   int64  `json:"revision"`
	Reason     string `json:"reason"`
}

type WaitCommand struct {
	CommandID  string `json:"command_id,omitempty"`
	ContractID string `json:"contract_id"`
	Hard       bool   `json:"hard"`
}

type TerminalCommand struct {
	CommandID  string `json:"command_id,omitempty"`
	ContractID string `json:"contract_id"`
	Reason     string `json:"reason,omitempty"`
}

type CompletionCommand struct {
	CommandID string          `json:"command_id,omitempty"`
	NodeID    string          `json:"node_id"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type CompletionResult struct {
	Ready            bool     `json:"ready"`
	PendingContracts []string `json:"pending_contracts,omitempty"`
}

type ProposeTermsCommand struct {
	CommandID           string          `json:"command_id,omitempty"`
	ContractID          string          `json:"contract_id"`
	ExpectedRevision    int64           `json:"expected_revision"`
	Details             ContractDetails `json:"details"`
	ProducerObligations []string        `json:"producer_obligations,omitempty"`
	ConsumerObligations []string        `json:"consumer_obligations,omitempty"`
	Tradeoffs           []string        `json:"tradeoffs,omitempty"`
	Rationale           string          `json:"rationale"`
}

type AcceptTermsCommand struct {
	CommandID  string `json:"command_id,omitempty"`
	ContractID string `json:"contract_id"`
	Revision   int64  `json:"revision"`
}

type StandstillCommand struct {
	CommandID               string   `json:"command_id,omitempty"`
	ContractID              string   `json:"contract_id"`
	TermsRevision           int64    `json:"terms_revision"`
	Reason                  string   `json:"reason"`
	Evidence                []string `json:"evidence,omitempty"`
	TerminalIncompatibility bool     `json:"terminal_incompatibility,omitempty"`
}

type ResolutionStatus string

const (
	ResolutionPending      ResolutionStatus = "Pending"
	ResolutionRunning      ResolutionStatus = "Running"
	ResolutionValidated    ResolutionStatus = "Validated"
	ResolutionApplying     ResolutionStatus = "Applying"
	ResolutionResolved     ResolutionStatus = "Resolved"
	ResolutionAwaitingUser ResolutionStatus = "AwaitingUser"
	ResolutionCancelled    ResolutionStatus = "Cancelled"
)

func (s ResolutionStatus) Terminal() bool {
	return s == ResolutionResolved || s == ResolutionAwaitingUser || s == ResolutionCancelled
}

type Resolution struct {
	ID                      string           `json:"id"`
	ContractID              string           `json:"contract_id"`
	TermsRevision           int64            `json:"terms_revision"`
	Status                  ResolutionStatus `json:"status"`
	Reason                  string           `json:"reason"`
	Evidence                []string         `json:"evidence,omitempty"`
	TerminalIncompatibility bool             `json:"terminal_incompatibility,omitempty"`
	AffectedNodeIDs         []string         `json:"affected_node_ids"`
	SemanticAttempts        int              `json:"semantic_attempts"`
	ProcessAttempts         int              `json:"process_attempts"`
	RepairAttempts          int              `json:"repair_attempts"`
	ContextExpansions       int              `json:"context_expansions"`
	MaxProcessAttempts      int              `json:"max_process_attempts"`
	MaxRepairAttempts       int              `json:"max_repair_attempts"`
	MaxContextExpansions    int              `json:"max_context_expansions"`
	LeaseToken              string           `json:"-"`
	LeaseExpiresAt          *time.Time       `json:"lease_expires_at,omitempty"`
	Input                   *Artifact        `json:"input,omitempty"`
	Output                  *Artifact        `json:"output,omitempty"`
	ValidationError         string           `json:"validation_error,omitempty"`
	Rationale               string           `json:"rationale,omitempty"`
	TerminalReason          string           `json:"terminal_reason,omitempty"`
	CreatedAt               time.Time        `json:"created_at"`
	UpdatedAt               time.Time        `json:"updated_at"`
}

type ClaimResolutionCommand struct {
	ResolutionID string        `json:"resolution_id"`
	LeaseFor     time.Duration `json:"lease_for"`
}

type EffectType string

const (
	EffectContractRequested   EffectType = "ContractRequested"
	EffectContractProduced    EffectType = "ContractProduced"
	EffectContractFulfilled   EffectType = "ContractFulfilled"
	EffectCorrectionRequested EffectType = "CorrectionRequested"
	EffectWaitDeclared        EffectType = "WaitDeclared"
	EffectWaitCleared         EffectType = "WaitCleared"
	EffectContractTerminal    EffectType = "ContractTerminal"
	EffectCycleDetected       EffectType = "CycleDetected"
	EffectNodeCompletionReady EffectType = "NodeCompletionReady"
	EffectTermsProposed       EffectType = "TermsProposed"
	EffectTermsAgreed         EffectType = "TermsAgreed"
	EffectResolutionRequested EffectType = "ResolutionRequested"
	EffectResolutionResolved  EffectType = "ResolutionResolved"
	EffectResolutionReview    EffectType = "ResolutionReviewRequired"
)

type Effect struct {
	ID            string          `json:"id"`
	ContractID    string          `json:"contract_id,omitempty"`
	Type          EffectType      `json:"type"`
	TargetAgentID string          `json:"target_agent_id,omitempty"`
	TargetNodeID  string          `json:"target_node_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Attempts      int             `json:"attempts"`
	CreatedAt     time.Time       `json:"created_at"`
	NextAttemptAt time.Time       `json:"next_attempt_at"`
}

type Cycle struct {
	ID          string    `json:"id"`
	NodeIDs     []string  `json:"node_ids"`
	ContractIDs []string  `json:"contract_ids"`
	DetectedAt  time.Time `json:"detected_at"`
}

type Snapshot struct {
	Contracts      []Contract      `json:"contracts"`
	Terms          []ContractTerms `json:"terms"`
	Resolutions    []Resolution    `json:"resolutions"`
	PendingEffects []Effect        `json:"pending_effects"`
	Cycles         []Cycle         `json:"cycles"`
}
