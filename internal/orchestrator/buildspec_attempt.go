package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"aion-kernel/internal/coordinator"
)

type BuildSpecAttemptStatus string

const (
	BuildSpecAttemptPlanning         BuildSpecAttemptStatus = "planning"
	BuildSpecAttemptCommitting       BuildSpecAttemptStatus = "committing"
	BuildSpecAttemptAllocating       BuildSpecAttemptStatus = "allocating"
	BuildSpecAttemptActive           BuildSpecAttemptStatus = "active"
	BuildSpecAttemptPaused           BuildSpecAttemptStatus = "paused"
	BuildSpecAttemptFailed           BuildSpecAttemptStatus = "failed"
	BuildSpecAttemptCommitFailed     BuildSpecAttemptStatus = "commit_failed"
	BuildSpecAttemptAllocationFailed BuildSpecAttemptStatus = "allocation_failed"
	BuildSpecAttemptCanceled         BuildSpecAttemptStatus = "canceled"
)

type BuildSpecAgentState struct {
	AgentID   string    `json:"agent_id"`
	DomainID  string    `json:"domain_id"`
	State     string    `json:"state"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type BuildSpecAttempt struct {
	AttemptID           string                         `json:"attempt_id"`
	RunID               string                         `json:"run_id"`
	SpecPath            string                         `json:"spec_path"`
	SpecHash            string                         `json:"spec_hash"`
	PlannerSessionDir   string                         `json:"planner_session_dir,omitempty"`
	PlanningArtifactDir string                         `json:"planning_artifact_dir,omitempty"`
	Status              BuildSpecAttemptStatus         `json:"status"`
	StartedAt           time.Time                      `json:"started_at"`
	UpdatedAt           time.Time                      `json:"updated_at"`
	CompletedAt         *time.Time                     `json:"completed_at,omitempty"`
	FailureReason       string                         `json:"failure_reason,omitempty"`
	Plan                *coordinator.PlanResponse      `json:"plan,omitempty"`
	CreatedNodeIDs      []string                       `json:"created_node_ids,omitempty"`
	CreatedEdgeIDs      []string                       `json:"created_edge_ids,omitempty"`
	AllocatedDomainIDs  []string                       `json:"allocated_domain_ids,omitempty"`
	AgentStates         map[string]BuildSpecAgentState `json:"agent_states,omitempty"`
	CanceledAt          *time.Time                     `json:"canceled_at,omitempty"`
}

func newBuildSpecAttempt(runID, specPath string, specData []byte) *BuildSpecAttempt {
	now := time.Now().UTC()
	sum := sha256.Sum256(specData)
	return &BuildSpecAttempt{
		AttemptID: fmt.Sprintf("buildspec_%s", now.Format("20060102T150405.000000000Z")),
		RunID:     runID,
		SpecPath:  specPath,
		SpecHash:  hex.EncodeToString(sum[:]),
		Status:    BuildSpecAttemptPlanning,
		StartedAt: now,
		UpdatedAt: now,
	}
}

func (a *BuildSpecAttempt) RecordAgentState(agentID, domainID, state, reason string) {
	if a == nil || strings.TrimSpace(agentID) == "" {
		return
	}
	if a.AgentStates == nil {
		a.AgentStates = make(map[string]BuildSpecAgentState)
	}
	a.AgentStates[agentID] = BuildSpecAgentState{
		AgentID:   agentID,
		DomainID:  domainID,
		State:     state,
		Reason:    reason,
		UpdatedAt: time.Now().UTC(),
	}
}

func buildSpecAttemptPath(runRoot string) string {
	return filepath.Join(runRoot, "build_spec_attempt.json")
}

func buildSpecTracePath(runRoot string) string {
	return filepath.Join(runRoot, "build_spec_trace.log")
}

func buildSpecDebugPath(runRoot string) string {
	if runRoot == "" {
		return "build_spec_debug.log"
	}
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(runRoot)))
	return filepath.Join(projectRoot, "build_spec_debug.log")
}

func loadBuildSpecAttempt(runRoot string) (*BuildSpecAttempt, error) {
	data, err := os.ReadFile(buildSpecAttemptPath(runRoot))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var attempt BuildSpecAttempt
	if err := json.Unmarshal(data, &attempt); err != nil {
		return nil, err
	}
	return &attempt, nil
}

func saveBuildSpecAttempt(runRoot string, attempt *BuildSpecAttempt) error {
	if attempt == nil {
		return fmt.Errorf("build-spec attempt: nil")
	}
	attempt.UpdatedAt = time.Now().UTC()
	return writeJSONFile(buildSpecAttemptPath(runRoot), attempt)
}

func deleteBuildSpecAttempt(runRoot string) error {
	path := buildSpecAttemptPath(runRoot)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func appendBuildSpecTrace(runRoot, line string) error {
	if strings.TrimSpace(line) == "" {
		return nil
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	payload := fmt.Sprintf("[%s] %s\n", ts, strings.TrimSpace(line))
	if err := appendTextFile(buildSpecTracePath(runRoot), payload); err != nil {
		return err
	}
	return appendTextFile(buildSpecDebugPath(runRoot), payload)
}

func appendTextFile(path, payload string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(payload)
	return err
}

func readBuildSpecTrace(runRoot string) (string, error) {
	data, err := os.ReadFile(buildSpecTracePath(runRoot))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

func clearBuildSpecArtifacts(runState *RunState) error {
	if runState == nil {
		return nil
	}
	for _, path := range []string{runState.DagFile, runState.WalFile} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
