package dag

import "fmt"

// ValidateCycles checks the DAG for cycles using Kahn's algorithm (topological sort).
// Returns an error describing the cycle if one is found.
func ValidateCycles(nodes []DagNode, edges []DagEdge) error {
	if len(nodes) == 0 {
		return nil
	}

	// Build adjacency list and in-degree map
	nodeSet := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		nodeSet[n.ID] = true
	}

	inDegree := make(map[string]int, len(nodes))
	for id := range nodeSet {
		inDegree[id] = 0
	}

	adj := make(map[string][]string)
	for _, e := range edges {
		// Only count edges between existing nodes
		if !nodeSet[e.FromNode] || !nodeSet[e.ToNode] {
			continue
		}
		adj[e.FromNode] = append(adj[e.FromNode], e.ToNode)
		inDegree[e.ToNode]++
	}

	// Kahn's algorithm: start with nodes that have in-degree 0
	queue := make([]string, 0)
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++

		for _, neighbor := range adj[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if visited != len(nodeSet) {
		// Find nodes still with in-degree > 0 to report the cycle
		cycleNodes := make([]string, 0)
		for id, deg := range inDegree {
			if deg > 0 {
				cycleNodes = append(cycleNodes, id)
			}
		}
		return fmt.Errorf("dag: cycle detected involving nodes: %v", cycleNodes)
	}

	return nil
}

// ValidateAssignments verifies that every file target is assigned to exactly one domain.
// Returns an error if a file appears in multiple domains.
func ValidateAssignments(nodes []DagNode) error {
	fileOwner := make(map[string]string) // file path → domain ID

	for _, node := range nodes {
		for _, file := range node.TargetFiles {
			if existing, ok := fileOwner[file]; ok {
				if existing != node.DomainID {
					return fmt.Errorf("dag: file '%s' assigned to multiple domains: '%s' and '%s'", file, existing, node.DomainID)
				}
			}
			fileOwner[file] = node.DomainID
		}
	}

	return nil
}

// ValidateNodeCeiling checks that the active node count does not exceed the maximum.
func ValidateNodeCeiling(nodes []DagNode, maxNodes uint32) error {
	var activeCount uint32
	for _, node := range nodes {
		if !node.Status.IsTerminal() {
			activeCount++
		}
	}

	if activeCount > maxNodes {
		return fmt.Errorf("dag: active node count %d exceeds ceiling %d", activeCount, maxNodes)
	}

	return nil
}

// ValidateStubEdges checks that all stub edges reference valid nodes.
func ValidateStubEdges(nodes []DagNode, edges []DagEdge) error {
	nodeSet := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		nodeSet[n.ID] = true
	}

	for _, e := range edges {
		if e.Type != EdgeStubContract {
			continue
		}
		if !nodeSet[e.FromNode] {
			return fmt.Errorf("dag: stub edge references non-existent from_node '%s'", e.FromNode)
		}
		if !nodeSet[e.ToNode] {
			return fmt.Errorf("dag: stub edge references non-existent to_node '%s'", e.ToNode)
		}
	}

	return nil
}

// ValidateAll runs all validation checks on the DAG.
func ValidateAll(dag *DagData) error {
	if err := ValidateCycles(dag.Nodes, dag.Edges); err != nil {
		return err
	}
	if err := ValidateAssignments(dag.Nodes); err != nil {
		return err
	}
	if err := ValidateNodeCeiling(dag.Nodes, dag.Header.MaxNodes); err != nil {
		return err
	}
	if err := ValidateStubEdges(dag.Nodes, dag.Edges); err != nil {
		return err
	}
	return nil
}
