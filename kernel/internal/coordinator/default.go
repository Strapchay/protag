package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
)

// DefaultCoordinator is retained for compatibility with old construction
// paths. Runtime build-spec planning must use PiCoordinator.
type DefaultCoordinator struct {
	// Config for Pi Agent spawning (future use)
	Provider string
	Model    string
}

// NewDefaultCoordinator creates the default Coordinator implementation.
func NewDefaultCoordinator() *DefaultCoordinator {
	return &DefaultCoordinator{}
}

// Plan refuses runtime planning because static coordinator planning can mutate
// the DAG without a live Coordinator Pi turn.
func (c *DefaultCoordinator) Plan(ctx context.Context, req PlanRequest) (*PlanResponse, error) {
	return nil, fmt.Errorf("coordinator: static runtime planning is disabled; configure the Pi-backed coordinator")
}

// Replan re-evaluates a subgraph.
func (c *DefaultCoordinator) Replan(ctx context.Context, req ReplanRequest) (*ReplanResponse, error) {
	log.Printf("coordinator: replanning %d affected nodes", len(req.AffectedNodes))

	// For now, just return the existing nodes as-is (no changes)
	return &ReplanResponse{
		ModifiedNodes:       req.CurrentNodes,
		NewEdges:            nil,
		DomainReassignments: nil,
	}, nil
}

func (c *DefaultCoordinator) Refine(ctx context.Context, text string) error {
	log.Printf("coordinator: heuristic refinement: %s", text)
	return nil
}

func (c *DefaultCoordinator) GetArchitectAgent() interface{} {
	return nil
}

func heuristicPlan(req PlanRequest) ([]Domain, []TaskNode, []TaskEdge) {
	scan := req.ProjectScan

	// Group entry points by top-level directory
	topDirs := make(map[string][]string) // dir → files
	for _, ep := range scan.EntryPoints {
		parts := splitPath(ep)
		topDir := "root"
		if len(parts) > 1 {
			topDir = parts[0]
		}
		topDirs[topDir] = append(topDirs[topDir], ep)
	}

	// If fewer than 2 domains, create a single domain
	if len(topDirs) < 2 {
		domain := Domain{
			DomainID:      "primary",
			Description:   fmt.Sprintf("Primary implementation domain for: %s", truncate(req.UserPrompt, 100)),
			AssignedPaths: []string{"."},
			AgentType:     "domain",
		}

		node := TaskNode{
			ID:          "task-primary-1",
			DomainID:    "primary",
			TaskSpec:    req.UserPrompt,
			TargetFiles: allSourceFiles(scan),
			Priority:    1,
		}

		return []Domain{domain}, []TaskNode{node}, nil
	}

	// Multiple domains
	var domains []Domain
	var nodes []TaskNode
	priority := int32(1)

	for dir, files := range topDirs {
		domainID := fmt.Sprintf("domain-%s", dir)
		domain := Domain{
			DomainID:      domainID,
			Description:   fmt.Sprintf("%s: implementation for %s/", truncate(req.UserPrompt, 60), dir),
			AssignedPaths: []string{dir + "/"},
			AgentType:     "domain",
		}
		domains = append(domains, domain)

		node := TaskNode{
			ID:          fmt.Sprintf("task-%s-1", dir),
			DomainID:    domainID,
			TaskSpec:    fmt.Sprintf("Implement %s related changes for: %s", dir, truncate(req.UserPrompt, 100)),
			TargetFiles: files,
			Priority:    priority,
		}
		nodes = append(nodes, node)
		priority++
	}

	// Add utility domain for shared files
	if len(scan.DependencyFiles) > 0 {
		utilDomain := Domain{
			DomainID:      "utility",
			Description:   "Manages shared files (go.mod, package.json, etc.)",
			AssignedPaths: scan.DependencyFiles,
			AgentType:     "utility",
		}
		domains = append(domains, utilDomain)

		utilNode := TaskNode{
			ID:          "task-utility-1",
			DomainID:    "utility",
			TaskSpec:    "Update shared dependency files after all domain agents complete",
			TargetFiles: scan.DependencyFiles,
			Priority:    priority,
		}
		nodes = append(nodes, utilNode)
	}

	return domains, nodes, nil
}

func splitPath(path string) []string {
	var parts []string
	for path != "" && path != "." {
		dir, file := filepath.Split(path)
		if file != "" {
			parts = append([]string{file}, parts...)
		}
		path = filepath.Clean(dir)
		if path == "." || path == "/" {
			break
		}
	}
	return parts
}

func allSourceFiles(scan *ProjectScan) []string {
	var files []string
	for _, ep := range scan.EntryPoints {
		files = append(files, ep)
	}
	return files
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func logJSON(prefix string, v interface{}) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Printf("%s: marshal error: %v", prefix, err)
		return
	}
	log.Printf("%s:\n%s", prefix, string(data))
}
