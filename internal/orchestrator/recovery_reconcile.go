package orchestrator

import (
	"strings"

	"aion-kernel/internal/dag"
)

// reconcilePersistedRecoveries closes recovery records whose authoritative DAG
// state has already moved beyond the failure that created them. This repairs
// registries written by older binaries that did not resolve blocked records on
// Pending/InProgress transitions.
func (d *Daemon) reconcilePersistedRecoveries() {
	if d == nil || d.runState == nil || d.dagManager == nil {
		return
	}
	records, err := loadRecoveryRegistry(d.runState.Root)
	if err != nil {
		return
	}
	for _, record := range records {
		if record.Status != "open" || strings.TrimSpace(record.NodeID) == "" {
			continue
		}
		node, err := d.dagManager.GetNode(record.NodeID)
		if err != nil {
			continue
		}
		switch record.Kind {
		case "task_blocked":
			if node.Status != dag.StatusBlocked {
				d.resolveRecovery(record.Kind, record.NodeID, record.AgentID)
			}
		case "task_failed":
			if node.Status != dag.StatusFailed {
				d.resolveRecovery(record.Kind, record.NodeID, record.AgentID)
			}
		}
	}
}
