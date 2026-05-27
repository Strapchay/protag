package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"aion-kernel/internal/supervisor"
)

const (
	behaviorLoopThreshold          = 7
	behaviorToolErrorThreshold     = 3
	behaviorResumeConfuseThreshold = 2
	behaviorLowInfoThreshold       = 5
)

type AgentBehaviorMetrics struct {
	AgentID                string            `json:"agent_id"`
	DomainID               string            `json:"domain_id,omitempty"`
	LastEventAt            time.Time         `json:"last_event_at"`
	LastMeaningfulProgress time.Time         `json:"last_meaningful_progress_at,omitempty"`
	LastToolFingerprint    string            `json:"last_tool_fingerprint,omitempty"`
	ConsecutiveToolRepeats int               `json:"consecutive_tool_repeats,omitempty"`
	ToolFingerprints       map[string]int    `json:"tool_fingerprints,omitempty"`
	ToolErrorCount         int               `json:"tool_error_count,omitempty"`
	OutOfDomainFileCount   int               `json:"out_of_domain_file_count,omitempty"`
	ResumeConfusionCount   int               `json:"resume_confusion_count,omitempty"`
	LowInfoTextCount       int               `json:"low_info_text_count,omitempty"`
	FileModifiedCount      int               `json:"file_modified_count,omitempty"`
	NodeUpdateCount        int               `json:"node_update_count,omitempty"`
	LockDeniedCount        int               `json:"lock_denied_count,omitempty"`
	InvalidNodeUpdateCount int               `json:"invalid_node_update_count,omitempty"`
	OpenFlags              map[string]bool   `json:"open_flags,omitempty"`
	LastEvidence           map[string]string `json:"last_evidence,omitempty"`
}

type AgentBehaviorStore map[string]*AgentBehaviorMetrics

func agentBehaviorMetricsPath(runRoot string) string {
	return filepath.Join(runRoot, "agent_behavior_metrics.json")
}

func loadAgentBehaviorStore(runRoot string) (AgentBehaviorStore, error) {
	data, err := os.ReadFile(agentBehaviorMetricsPath(runRoot))
	if err != nil {
		if os.IsNotExist(err) {
			return AgentBehaviorStore{}, nil
		}
		return nil, err
	}
	var store AgentBehaviorStore
	if err := json.Unmarshal(data, &store); err != nil {
		return nil, err
	}
	if store == nil {
		store = AgentBehaviorStore{}
	}
	return store, nil
}

func saveAgentBehaviorStore(runRoot string, store AgentBehaviorStore) error {
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(agentBehaviorMetricsPath(runRoot), append(data, '\n'))
}

func (d *Daemon) observeAgentLifecycleBehavior(agentID, domainID string, event supervisor.AgentLifecycleEvent) {
	if d == nil || d.runState == nil || strings.TrimSpace(agentID) == "" {
		return
	}
	store, err := loadAgentBehaviorStore(d.runState.Root)
	if err != nil {
		return
	}
	metrics := behaviorMetricsFor(store, agentID, domainID)
	now := time.Now().UTC()
	metrics.LastEventAt = now
	var recoveries []RecoveryRecord

	switch event.Kind {
	case "tool_start":
		fingerprint := behaviorToolFingerprint(event)
		if fingerprint != "" {
			if metrics.ToolFingerprints == nil {
				metrics.ToolFingerprints = map[string]int{}
			}
			metrics.ToolFingerprints[fingerprint]++
			if metrics.LastToolFingerprint == fingerprint {
				metrics.ConsecutiveToolRepeats++
			} else {
				metrics.LastToolFingerprint = fingerprint
				metrics.ConsecutiveToolRepeats = 1
			}
			if metrics.ConsecutiveToolRepeats >= behaviorLoopThreshold && !metrics.OpenFlags["agent_looping"] {
				metrics.OpenFlags["agent_looping"] = true
				metrics.LastEvidence["agent_looping"] = fingerprint
				recoveries = append(recoveries, behaviorRecovery("agent_looping", "warn", agentID, domainID, fmt.Sprintf("%s repeated the same tool action %d times without progress.", agentID, metrics.ConsecutiveToolRepeats), fingerprint, "/continue-agents"))
			}
		}
	case "tool_end":
		metrics.LastMeaningfulProgress = now
		clearBehaviorFlag(metrics, "agent_looping")
		d.resolveRecovery("agent_looping", "", agentID)
	case "tool_error":
		metrics.ToolErrorCount++
		if metrics.ToolErrorCount >= behaviorToolErrorThreshold && !metrics.OpenFlags["tool_failure_cluster"] {
			metrics.OpenFlags["tool_failure_cluster"] = true
			metrics.LastEvidence["tool_failure_cluster"] = event.Error
			recoveries = append(recoveries, behaviorRecovery("tool_failure_cluster", "warn", agentID, domainID, fmt.Sprintf("%s has repeated tool errors.", agentID), event.Error, "/continue-agents"))
		}
	case "file_modified":
		metrics.FileModifiedCount++
		metrics.LastMeaningfulProgress = now
		metrics.LowInfoTextCount = 0
		metrics.ToolErrorCount = 0
		clearBehaviorFlag(metrics, "tool_failure_cluster")
		clearBehaviorFlag(metrics, "no_effective_progress")
		d.resolveRecovery("tool_failure_cluster", "", agentID)
		d.resolveRecovery("no_effective_progress", "", agentID)
		path := strings.TrimSpace(event.Content)
		if path != "" && !d.pathWithinAgentDomain(domainID, path) {
			metrics.OutOfDomainFileCount++
			if metrics.OutOfDomainFileCount >= 2 && !metrics.OpenFlags["domain_boundary_violation"] {
				metrics.OpenFlags["domain_boundary_violation"] = true
				metrics.LastEvidence["domain_boundary_violation"] = path
				recoveries = append(recoveries, behaviorRecovery("domain_boundary_violation", "warn", agentID, domainID, fmt.Sprintf("%s modified files outside its assigned domain more than once.", agentID), path, "/progress"))
			}
		}
	case "text":
		text := strings.TrimSpace(event.Content)
		if looksLikeResumeConfusion(text) {
			metrics.ResumeConfusionCount++
			if metrics.ResumeConfusionCount >= behaviorResumeConfuseThreshold && !metrics.OpenFlags["resume_context_drift"] {
				metrics.OpenFlags["resume_context_drift"] = true
				metrics.LastEvidence["resume_context_drift"] = truncateEvidence(text)
				recoveries = append(recoveries, behaviorRecovery("resume_context_drift", "warn", agentID, domainID, fmt.Sprintf("%s appears confused after resume.", agentID), truncateEvidence(text), "/msg "+agentID+" <clarifying task context>"))
			}
		}
		if looksLowInformation(text) {
			metrics.LowInfoTextCount++
			if metrics.LowInfoTextCount >= behaviorLowInfoThreshold && !metrics.OpenFlags["no_effective_progress"] {
				metrics.OpenFlags["no_effective_progress"] = true
				metrics.LastEvidence["no_effective_progress"] = truncateEvidence(text)
				recoveries = append(recoveries, behaviorRecovery("no_effective_progress", "warn", agentID, domainID, fmt.Sprintf("%s is producing repeated low-information output without observable progress.", agentID), truncateEvidence(text), "/continue-agents"))
			}
		}
	}

	_ = saveAgentBehaviorStore(d.runState.Root, store)
	for _, recovery := range recoveries {
		d.recordRecovery(recovery)
		d.recordExecutionEvent(ExecutionJournalEvent{
			Kind:     recovery.Kind,
			Severity: recovery.Severity,
			AgentID:  agentID,
			DomainID: domainID,
			Summary:  recovery.Summary,
		})
		d.refreshBuildProgress(recovery.Kind)
	}
}

func (d *Daemon) observeAgentCoordinationBehavior(agentID, domainID, kind, evidence string) {
	if d == nil || d.runState == nil || strings.TrimSpace(agentID) == "" {
		return
	}
	store, err := loadAgentBehaviorStore(d.runState.Root)
	if err != nil {
		return
	}
	metrics := behaviorMetricsFor(store, agentID, domainID)
	metrics.LastEventAt = time.Now().UTC()
	var recovery *RecoveryRecord
	switch kind {
	case "node_update":
		metrics.NodeUpdateCount++
		metrics.LastMeaningfulProgress = metrics.LastEventAt
		metrics.LowInfoTextCount = 0
		clearBehaviorFlag(metrics, "no_effective_progress")
		d.resolveRecovery("no_effective_progress", "", agentID)
	case "lock_denied":
		metrics.LockDeniedCount++
		if metrics.LockDeniedCount >= 3 && !metrics.OpenFlags["coordination_misuse"] {
			metrics.OpenFlags["coordination_misuse"] = true
			metrics.LastEvidence["coordination_misuse"] = evidence
			r := behaviorRecovery("coordination_misuse", "warn", agentID, domainID, fmt.Sprintf("%s has repeated coordination errors.", agentID), evidence, "/msg "+agentID+" <correct coordination boundary>")
			recovery = &r
		}
	case "invalid_node_update":
		metrics.InvalidNodeUpdateCount++
		if metrics.InvalidNodeUpdateCount >= 2 && !metrics.OpenFlags["coordination_misuse"] {
			metrics.OpenFlags["coordination_misuse"] = true
			metrics.LastEvidence["coordination_misuse"] = evidence
			r := behaviorRecovery("coordination_misuse", "warn", agentID, domainID, fmt.Sprintf("%s attempted invalid DAG coordination more than once.", agentID), evidence, "/msg "+agentID+" <correct node assignment>")
			recovery = &r
		}
	}
	_ = saveAgentBehaviorStore(d.runState.Root, store)
	if recovery != nil {
		d.recordRecovery(*recovery)
		d.recordExecutionEvent(ExecutionJournalEvent{
			Kind:     recovery.Kind,
			Severity: recovery.Severity,
			AgentID:  agentID,
			DomainID: domainID,
			Summary:  recovery.Summary,
		})
		d.refreshBuildProgress(recovery.Kind)
	}
}

func behaviorMetricsFor(store AgentBehaviorStore, agentID, domainID string) *AgentBehaviorMetrics {
	metrics := store[agentID]
	if metrics == nil {
		metrics = &AgentBehaviorMetrics{
			AgentID:          agentID,
			ToolFingerprints: map[string]int{},
			OpenFlags:        map[string]bool{},
			LastEvidence:     map[string]string{},
		}
		store[agentID] = metrics
	}
	if strings.TrimSpace(domainID) != "" {
		metrics.DomainID = domainID
	}
	if metrics.ToolFingerprints == nil {
		metrics.ToolFingerprints = map[string]int{}
	}
	if metrics.OpenFlags == nil {
		metrics.OpenFlags = map[string]bool{}
	}
	if metrics.LastEvidence == nil {
		metrics.LastEvidence = map[string]string{}
	}
	return metrics
}

func behaviorRecovery(kind, severity, agentID, domainID, summary, evidence, command string) RecoveryRecord {
	return RecoveryRecord{
		FailureID:        recoveryID(kind, "", agentID),
		Kind:             kind,
		Severity:         severity,
		Status:           "open",
		AgentID:          agentID,
		DomainID:         domainID,
		Summary:          summary,
		LastError:        truncateEvidence(evidence),
		SuggestedCommand: command,
	}
}

func behaviorToolFingerprint(event supervisor.AgentLifecycleEvent) string {
	tool := strings.TrimSpace(event.ToolName)
	if tool == "" {
		return ""
	}
	input := strings.TrimSpace(event.ToolInput)
	if len(input) > 240 {
		input = input[:240]
	}
	return tool + ":" + input
}

func clearBehaviorFlag(metrics *AgentBehaviorMetrics, flag string) {
	if metrics == nil || metrics.OpenFlags == nil {
		return
	}
	delete(metrics.OpenFlags, flag)
	delete(metrics.LastEvidence, flag)
}

func looksLikeResumeConfusion(text string) bool {
	lower := strings.ToLower(text)
	if len(strings.TrimSpace(lower)) == 0 {
		return false
	}
	phrases := []string{
		"what should i do",
		"what would you like me to do",
		"please provide",
		"i need more context",
		"i don't have context",
		"i do not have context",
		"can you clarify",
		"what is my task",
	}
	for _, phrase := range phrases {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	return false
}

func looksLowInformation(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	if lower == "" || len(lower) > 220 {
		return false
	}
	phrases := []string{
		"i will proceed",
		"i'll proceed",
		"i will continue",
		"i'll continue",
		"let me inspect",
		"i will inspect",
		"i am checking",
		"i'll check",
	}
	for _, phrase := range phrases {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	return false
}

func truncateEvidence(text string) string {
	text = strings.TrimSpace(text)
	if len(text) > 300 {
		return text[:300]
	}
	return text
}

func (d *Daemon) pathWithinAgentDomain(domainID, path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return true
	}
	attempt, _ := loadBuildSpecAttempt(d.runState.Root)
	if attempt == nil || attempt.Plan == nil {
		return true
	}
	for _, domain := range attempt.Plan.Domains {
		if domain.DomainID != domainID {
			continue
		}
		if len(domain.AssignedPaths) == 0 {
			return true
		}
		for _, assigned := range domain.AssignedPaths {
			assigned = strings.TrimSpace(assigned)
			if assigned == "" {
				continue
			}
			if path == assigned || strings.HasPrefix(path, strings.TrimRight(assigned, "/")+"/") {
				return true
			}
		}
		return false
	}
	return true
}
