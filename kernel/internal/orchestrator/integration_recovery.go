package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"aion-kernel/internal/dag"
)

type integrationFailureOwner struct {
	NodeID string
	Result IntegrationDirectiveResult
}

func (d *Daemon) prepareIntegrationFailureRecovery(status IntegrationRunStatus) ([]string, error) {
	if d == nil || d.dagManager == nil || d.allocator == nil || status.Status != IntegrationFailed {
		return nil, nil
	}
	owners, err := integrationFailureOwners(status)
	if err != nil {
		return nil, err
	}
	if len(owners) == 0 {
		return nil, nil
	}

	ctx := d.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	recovered := make([]string, 0, len(owners))
	for _, owner := range owners {
		node, err := d.dagManager.GetNode(owner.NodeID)
		if err != nil {
			return recovered, fmt.Errorf("integration recovery node %s: %w", owner.NodeID, err)
		}
		agentID := expectedAgentForNode(node.DomainID, node.AssignedAgent)
		if strings.TrimSpace(node.AssignedAgent) == "" {
			if err := d.dagManager.AssignNode(node.ID, agentID); err != nil {
				return recovered, fmt.Errorf("assign integration recovery node %s: %w", node.ID, err)
			}
		}
		changed := node.Status != dag.StatusInProgress
		if changed {
			if err := d.dagManager.UpdateNode(node.ID, dag.StatusInProgress); err != nil {
				return recovered, fmt.Errorf("reopen integration recovery node %s: %w", node.ID, err)
			}
			node, _ = d.dagManager.GetNode(node.ID)
			if node != nil && d.server != nil {
				d.server.recordNodeProgressFact(*node, dag.StatusInProgress)
			}
		}

		recovery := formatIntegrationRecovery(owner.Result, status.Error)
		delivered, err := d.allocator.DeliverNodeRecovery(ctx, owner.NodeID, recovery)
		if err != nil {
			log.Printf("integration: recovery delivery for node %s failed: %v", owner.NodeID, err)
		}
		if changed {
			d.recordRecovery(RecoveryRecord{
				FailureID:        recoveryID("integration_node_failed", owner.NodeID, agentID),
				Kind:             "integration_node_failed",
				Severity:         "error",
				Status:           "open",
				AgentID:          agentID,
				DomainID:         node.DomainID,
				NodeID:           owner.NodeID,
				Summary:          fmt.Sprintf("Integration directive %s reopened task %s for repair.", owner.Result.DirectiveID, owner.NodeID),
				LastError:        firstNonEmpty(owner.Result.Error, status.Error),
				SuggestedCommand: "/continue-agents",
			})
		}
		if delivered && d.server != nil {
			d.server.BroadcastAgentStatus(agentID, fmt.Sprintf("Integration recovery dispatched for node %s.", owner.NodeID), "warn")
		}
		recovered = append(recovered, owner.NodeID)
	}
	sort.Strings(recovered)
	return recovered, nil
}

func integrationFailureOwners(status IntegrationRunStatus) ([]integrationFailureOwner, error) {
	ownersByDirective := make(map[string][]string)
	if needsIntegrationManifestFallback(status.Results) && strings.TrimSpace(status.ManifestPath) != "" {
		data, err := os.ReadFile(status.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("read integration manifest: %w", err)
		}
		var manifest IntegrationManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, fmt.Errorf("parse integration manifest: %w", err)
		}
		for _, artifact := range manifest.Directives {
			ownersByDirective[artifact.Directive.ID] = append([]string(nil), artifact.Directive.OwnerNodeIDs...)
		}
	}

	byNode := make(map[string]integrationFailureOwner)
	for _, result := range status.Results {
		if result.Status == "passed" && result.Error == "" && result.ExitCode == 0 {
			continue
		}
		nodeIDs := append([]string(nil), result.OwnerNodeIDs...)
		if len(nodeIDs) == 0 {
			nodeIDs = append(nodeIDs, ownersByDirective[result.DirectiveID]...)
		}
		for _, nodeID := range nodeIDs {
			nodeID = strings.TrimSpace(nodeID)
			if nodeID == "" {
				continue
			}
			owner := integrationFailureOwner{NodeID: nodeID, Result: result}
			owner.Result.OwnerNodeIDs = append([]string(nil), nodeIDs...)
			byNode[nodeID] = owner
		}
	}
	owners := make([]integrationFailureOwner, 0, len(byNode))
	for _, owner := range byNode {
		owners = append(owners, owner)
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].NodeID < owners[j].NodeID })
	return owners, nil
}

func needsIntegrationManifestFallback(results []IntegrationDirectiveResult) bool {
	for _, result := range results {
		if len(result.OwnerNodeIDs) == 0 {
			return true
		}
	}
	return false
}

func formatIntegrationRecovery(result IntegrationDirectiveResult, statusError string) string {
	var b strings.Builder
	b.WriteString("The assembled integration runner rejected this node's published acceptance directive.\n")
	if result.DirectiveID != "" {
		b.WriteString("Directive: ")
		b.WriteString(result.DirectiveID)
		b.WriteString("\n")
	}
	if message := firstNonEmpty(result.Error, statusError); message != "" {
		b.WriteString("Runner error: ")
		b.WriteString(message)
		b.WriteString("\n")
	}
	if stderr := strings.TrimSpace(result.Stderr); stderr != "" {
		b.WriteString("Stderr:\n")
		b.WriteString(stderr)
		b.WriteString("\n")
	}
	if stdout := strings.TrimSpace(result.Stdout); stdout != "" {
		b.WriteString("Stdout:\n")
		b.WriteString(stdout)
		b.WriteString("\n")
	}
	b.WriteString("The runner executes from the assembled /workspace containing all public domain outputs. Correct the owned implementation or directive that caused this exact failure; do not inspect another agent's private workspace.")
	return strings.TrimSpace(b.String())
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func (d *Daemon) clearIntegrationNodeRecoveries(status IntegrationRunStatus) {
	if d == nil || d.allocator == nil {
		return
	}
	for _, result := range status.Results {
		for _, nodeID := range result.OwnerNodeIDs {
			d.allocator.ClearNodeRecoveryContext(nodeID)
			d.resolveRecovery("integration_node_failed", nodeID, "")
		}
	}
}
