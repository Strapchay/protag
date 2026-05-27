package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"aion-kernel/internal/dag"
)

type BuildProgressSnapshot struct {
	AttemptID      string                  `json:"attempt_id,omitempty"`
	Status         string                  `json:"status"`
	OverallPercent int                     `json:"overall_percent"`
	DoneNodes      int                     `json:"done_nodes"`
	TotalNodes     int                     `json:"total_nodes"`
	Milestones     []MilestoneProgress     `json:"milestones"`
	OpenRecoveries []RecoveryRecord        `json:"open_recoveries,omitempty"`
	RecentEvents   []ExecutionJournalEvent `json:"recent_events,omitempty"`
	Warnings       []string                `json:"warnings,omitempty"`
	ClientSummary  string                  `json:"client_summary"`
	UpdatedAt      time.Time               `json:"updated_at"`
}

type MilestoneProgress struct {
	MilestoneID          string   `json:"milestone_id"`
	Title                string   `json:"title"`
	Summary              string   `json:"summary,omitempty"`
	Status               string   `json:"status"`
	Percent              int      `json:"percent"`
	DoneNodes            int      `json:"done_nodes"`
	TotalNodes           int      `json:"total_nodes"`
	ActiveNodes          []string `json:"active_nodes,omitempty"`
	FailedNodes          []string `json:"failed_nodes,omitempty"`
	BlockedNodes         []string `json:"blocked_nodes,omitempty"`
	NodeIDs              []string `json:"node_ids"`
	Weight               int      `json:"weight,omitempty"`
	ClassificationSource string   `json:"classification_source"`
	Warnings             []string `json:"warnings,omitempty"`
	ClientSummary        string   `json:"client_summary"`
}

type progressMilestoneSeed struct {
	ID                   string
	Title                string
	Summary              string
	NodeIDs              []string
	Weight               int
	ClassificationSource string
	Warnings             []string
}

func (d *Daemon) buildProgressSnapshot() (*BuildProgressSnapshot, error) {
	attempt, _ := loadBuildSpecAttempt(d.runState.Root)
	snapshot := computeBuildProgress(attempt, d.dagManager.Snapshot())
	snapshot.OpenRecoveries = d.openRecoveryRecords()
	snapshot.RecentEvents = d.recentExecutionEvents(10)
	applyRecoveryFacts(snapshot)
	snapshot.ClientSummary = buildProgressSummary(*snapshot)
	if err := saveBuildProgressSnapshot(d.runState.Root, snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (d *Daemon) refreshBuildProgress(reason string) {
	if d == nil || d.server == nil {
		return
	}
	snapshot, err := d.buildProgressSnapshot()
	if err != nil {
		logText := fmt.Sprintf("Build progress refresh failed: %v", err)
		d.server.BroadcastAgentStatus("coordinator", logText, "warn")
		return
	}
	if snapshot == nil || snapshot.TotalNodes == 0 {
		return
	}
	signature := progressEventSignature(snapshot)
	d.progressMu.Lock()
	if signature == "" || signature == d.lastProgressSignature {
		d.progressMu.Unlock()
		return
	}
	d.lastProgressSignature = signature
	d.progressMu.Unlock()

	text := "Build progress: " + snapshot.ClientSummary
	if strings.TrimSpace(reason) != "" {
		text += " [" + reason + "]"
	}
	d.server.BroadcastAgentStatus("coordinator", text, progressLevel(snapshot))
}

func buildProgressSnapshotPath(runRoot string) string {
	return filepath.Join(runRoot, "build_progress_snapshot.json")
}

func saveBuildProgressSnapshot(runRoot string, snapshot *BuildProgressSnapshot) error {
	if snapshot == nil || strings.TrimSpace(runRoot) == "" {
		return nil
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(buildProgressSnapshotPath(runRoot), append(data, '\n'))
}

func loadBuildProgressSnapshot(runRoot string) (*BuildProgressSnapshot, error) {
	data, err := os.ReadFile(buildProgressSnapshotPath(runRoot))
	if err != nil {
		return nil, err
	}
	var snapshot BuildProgressSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func computeBuildProgress(attempt *BuildSpecAttempt, snapshot *dag.DagData) *BuildProgressSnapshot {
	if snapshot == nil {
		snapshot = dag.NewDagData(0)
	}
	seeds, warnings := progressMilestoneSeeds(attempt, snapshot)
	nodesByID := make(map[string]dag.DagNode, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodesByID[node.ID] = node
	}

	progress := BuildProgressSnapshot{
		Status:    "not_started",
		UpdatedAt: time.Now().UTC(),
		Warnings:  warnings,
	}
	if attempt != nil {
		progress.AttemptID = attempt.AttemptID
		progress.Status = string(attempt.Status)
	}

	for _, seed := range seeds {
		mp := computeMilestoneProgress(seed, nodesByID)
		progress.DoneNodes += mp.DoneNodes
		progress.TotalNodes += mp.TotalNodes
		progress.Milestones = append(progress.Milestones, mp)
	}
	if progress.TotalNodes > 0 {
		progress.OverallPercent = percent(progress.DoneNodes, progress.TotalNodes)
	}
	progress.ClientSummary = buildProgressSummary(progress)
	return &progress
}

func progressMilestoneSeeds(attempt *BuildSpecAttempt, snapshot *dag.DagData) ([]progressMilestoneSeed, []string) {
	var seeds []progressMilestoneSeed
	var warnings []string
	assigned := map[string]string{}
	validNodes := map[string]dag.DagNode{}
	for _, node := range snapshot.Nodes {
		validNodes[node.ID] = node
	}

	if attempt != nil && attempt.Plan != nil {
		for _, raw := range attempt.Plan.Milestones {
			id := strings.TrimSpace(raw.ID)
			if id == "" {
				warnings = append(warnings, "Coordinator milestone without id was ignored")
				continue
			}
			seed := progressMilestoneSeed{
				ID:                   id,
				Title:                fallback(raw.Title, id),
				Summary:              raw.Summary,
				Weight:               raw.Weight,
				ClassificationSource: "coordinator",
			}
			for _, nodeID := range raw.NodeIDs {
				nodeID = strings.TrimSpace(nodeID)
				if _, ok := validNodes[nodeID]; !ok {
					seed.Warnings = append(seed.Warnings, fmt.Sprintf("unknown node %s ignored", nodeID))
					continue
				}
				if prior := assigned[nodeID]; prior != "" {
					warnings = append(warnings, fmt.Sprintf("node %s assigned to multiple milestones; kept %s", nodeID, prior))
					continue
				}
				assigned[nodeID] = id
				seed.NodeIDs = append(seed.NodeIDs, nodeID)
			}
			if len(seed.NodeIDs) > 0 {
				seeds = append(seeds, seed)
			}
		}
	}

	fallbacks := map[string]*progressMilestoneSeed{}
	for _, node := range snapshot.Nodes {
		if assigned[node.ID] != "" {
			continue
		}
		id, title := fallbackMilestoneForNode(node)
		seed := fallbacks[id]
		if seed == nil {
			seed = &progressMilestoneSeed{
				ID:                   id,
				Title:                title,
				ClassificationSource: "fallback",
				Warnings:             []string{"contains node(s) not assigned to a Coordinator milestone"},
			}
			fallbacks[id] = seed
		}
		seed.NodeIDs = append(seed.NodeIDs, node.ID)
		warnings = append(warnings, fmt.Sprintf("node %s was assigned to fallback milestone %s", node.ID, id))
	}
	var fallbackIDs []string
	for id := range fallbacks {
		fallbackIDs = append(fallbackIDs, id)
	}
	sort.Strings(fallbackIDs)
	for _, id := range fallbackIDs {
		seeds = append(seeds, *fallbacks[id])
	}

	sort.SliceStable(seeds, func(i, j int) bool { return seeds[i].ID < seeds[j].ID })
	return seeds, warnings
}

func computeMilestoneProgress(seed progressMilestoneSeed, nodesByID map[string]dag.DagNode) MilestoneProgress {
	mp := MilestoneProgress{
		MilestoneID:          seed.ID,
		Title:                seed.Title,
		Summary:              seed.Summary,
		NodeIDs:              append([]string(nil), seed.NodeIDs...),
		Weight:               seed.Weight,
		ClassificationSource: seed.ClassificationSource,
		Warnings:             seed.Warnings,
		Status:               "not_started",
	}
	for _, nodeID := range seed.NodeIDs {
		node, ok := nodesByID[nodeID]
		if !ok {
			continue
		}
		mp.TotalNodes++
		switch node.Status {
		case dag.StatusDone:
			mp.DoneNodes++
		case dag.StatusFailed:
			mp.FailedNodes = append(mp.FailedNodes, node.ID)
		case dag.StatusBlocked:
			mp.BlockedNodes = append(mp.BlockedNodes, node.ID)
		case dag.StatusInProgress:
			mp.ActiveNodes = append(mp.ActiveNodes, node.ID)
		}
	}
	if mp.TotalNodes > 0 {
		mp.Percent = percent(mp.DoneNodes, mp.TotalNodes)
	}
	mp.Status = milestoneStatus(mp)
	mp.ClientSummary = milestoneSummary(mp)
	return mp
}

func milestoneStatus(mp MilestoneProgress) string {
	switch {
	case mp.TotalNodes == 0:
		return "not_started"
	case len(mp.FailedNodes) > 0:
		return "failed"
	case len(mp.BlockedNodes) > 0:
		return "blocked"
	case mp.DoneNodes == mp.TotalNodes:
		return "complete"
	case len(mp.ActiveNodes) > 0 || mp.DoneNodes > 0:
		return "active"
	default:
		return "not_started"
	}
}

func fallbackMilestoneForNode(node dag.DagNode) (string, string) {
	lower := strings.ToLower(node.ID + " " + node.TaskSpec + " " + strings.Join(node.TargetFiles, " "))
	if strings.Contains(lower, "test") || strings.Contains(lower, "validat") || strings.Contains(lower, "verify") {
		return "validation", "Validation"
	}
	if strings.TrimSpace(node.DomainID) != "" {
		return "domain:" + node.DomainID, strings.Title(strings.ReplaceAll(node.DomainID, "-", " ")) + " work"
	}
	return "general-implementation", "General implementation"
}

func milestoneSummary(mp MilestoneProgress) string {
	if mp.TotalNodes == 0 {
		return mp.Title + " has no tracked tasks yet."
	}
	base := fmt.Sprintf("%s is %s. %d of %d task(s) are complete.", mp.Title, strings.ReplaceAll(mp.Status, "_", " "), mp.DoneNodes, mp.TotalNodes)
	if len(mp.FailedNodes) > 0 {
		return base + " Failed task(s): " + strings.Join(mp.FailedNodes, ", ") + "."
	}
	if len(mp.BlockedNodes) > 0 {
		return base + " Blocked task(s): " + strings.Join(mp.BlockedNodes, ", ") + "."
	}
	if len(mp.ActiveNodes) > 0 {
		return base + " Active task(s): " + strings.Join(mp.ActiveNodes, ", ") + "."
	}
	return base
}

func buildProgressSummary(snapshot BuildProgressSnapshot) string {
	if snapshot.TotalNodes == 0 {
		return "No build-spec tasks are currently tracked."
	}
	var active, blocked, failed, complete []string
	for _, milestone := range snapshot.Milestones {
		switch milestone.Status {
		case "active":
			active = append(active, milestone.Title)
		case "blocked":
			blocked = append(blocked, milestone.Title)
		case "failed":
			failed = append(failed, milestone.Title)
		case "complete":
			complete = append(complete, milestone.Title)
		}
	}
	var parts []string
	parts = append(parts, fmt.Sprintf("Overall build progress is %d%% (%d of %d task(s) complete).", snapshot.OverallPercent, snapshot.DoneNodes, snapshot.TotalNodes))
	if len(complete) > 0 {
		parts = append(parts, "Complete: "+strings.Join(complete, ", ")+".")
	}
	if len(active) > 0 {
		parts = append(parts, "Active: "+strings.Join(active, ", ")+".")
	}
	if len(blocked) > 0 {
		parts = append(parts, "Blocked: "+strings.Join(blocked, ", ")+".")
	}
	if len(failed) > 0 {
		parts = append(parts, "Failed: "+strings.Join(failed, ", ")+".")
	}
	if len(snapshot.OpenRecoveries) > 0 {
		parts = append(parts, fmt.Sprintf("Needs attention: %d open recovery item(s).", len(snapshot.OpenRecoveries)))
	}
	return strings.Join(parts, " ")
}

func applyRecoveryFacts(snapshot *BuildProgressSnapshot) {
	if snapshot == nil || len(snapshot.OpenRecoveries) == 0 {
		return
	}
	recoveryByNode := map[string][]RecoveryRecord{}
	for _, recovery := range snapshot.OpenRecoveries {
		if strings.TrimSpace(recovery.NodeID) != "" {
			recoveryByNode[recovery.NodeID] = append(recoveryByNode[recovery.NodeID], recovery)
		}
	}
	if len(recoveryByNode) == 0 {
		return
	}
	for i := range snapshot.Milestones {
		mp := &snapshot.Milestones[i]
		var recoverySummaries []string
		for _, nodeID := range mp.NodeIDs {
			for _, recovery := range recoveryByNode[nodeID] {
				recoverySummaries = append(recoverySummaries, recovery.Summary)
				switch recovery.Kind {
				case "task_failed":
					if !containsString(mp.FailedNodes, nodeID) {
						mp.FailedNodes = append(mp.FailedNodes, nodeID)
					}
				default:
					if !containsString(mp.BlockedNodes, nodeID) {
						mp.BlockedNodes = append(mp.BlockedNodes, nodeID)
					}
				}
			}
		}
		if len(recoverySummaries) > 0 {
			mp.Warnings = append(mp.Warnings, recoverySummaries...)
			mp.Status = milestoneStatus(*mp)
			mp.ClientSummary = milestoneSummary(*mp) + " Recovery: " + strings.Join(recoverySummaries, " ")
		}
	}
}

func (d *Daemon) recentExecutionEvents(limit int) []ExecutionJournalEvent {
	if d == nil || d.runState == nil {
		return nil
	}
	events, err := loadRecentExecutionJournalEvents(d.runState.Root, limit)
	if err != nil {
		return nil
	}
	return events
}

func (d *Daemon) openRecoveryRecords() []RecoveryRecord {
	if d == nil || d.runState == nil {
		return nil
	}
	records, err := loadRecoveryRegistry(d.runState.Root)
	if err != nil {
		return nil
	}
	var open []RecoveryRecord
	for _, record := range records {
		if record.Status == "open" {
			open = append(open, record)
		}
	}
	return open
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func progressEventSignature(snapshot *BuildProgressSnapshot) string {
	if snapshot == nil || snapshot.TotalNodes == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(snapshot.Status)
	b.WriteString("|")
	b.WriteString(fmt.Sprintf("overall:%d", snapshot.OverallPercent/10))
	for _, milestone := range snapshot.Milestones {
		b.WriteString("|")
		b.WriteString(milestone.MilestoneID)
		b.WriteString(":")
		b.WriteString(milestone.Status)
		b.WriteString(":")
		b.WriteString(fmt.Sprintf("%d", milestone.Percent/10))
		if len(milestone.FailedNodes) > 0 {
			b.WriteString(":failed=")
			b.WriteString(strings.Join(milestone.FailedNodes, ","))
		}
		if len(milestone.BlockedNodes) > 0 {
			b.WriteString(":blocked=")
			b.WriteString(strings.Join(milestone.BlockedNodes, ","))
		}
	}
	return b.String()
}

func progressLevel(snapshot *BuildProgressSnapshot) string {
	if snapshot == nil {
		return "info"
	}
	for _, recovery := range snapshot.OpenRecoveries {
		if recovery.Severity == "error" {
			return "error"
		}
	}
	if len(snapshot.OpenRecoveries) > 0 {
		return "warn"
	}
	hasActive := false
	for _, milestone := range snapshot.Milestones {
		switch milestone.Status {
		case "failed":
			return "error"
		case "blocked":
			return "warn"
		case "active":
			hasActive = true
		}
	}
	if snapshot.TotalNodes > 0 && snapshot.DoneNodes == snapshot.TotalNodes {
		return "ok"
	}
	if hasActive {
		return "info"
	}
	return "info"
}

func percent(done, total int) int {
	if total <= 0 {
		return 0
	}
	return int(float64(done)/float64(total)*100 + 0.5)
}

func fallback(value, fallbackValue string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallbackValue
}
