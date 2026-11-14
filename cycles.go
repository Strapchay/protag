package coordination

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

type waitEdge struct {
	contractID string
	from       string
	to         string
}

func detectCycles(edges []waitEdge) []Cycle {
	adj := make(map[string][]string)
	vertices := make(map[string]struct{})
	contractsByPair := make(map[string][]string)
	for _, edge := range edges {
		if edge.from == "" || edge.to == "" {
			continue
		}
		adj[edge.from] = append(adj[edge.from], edge.to)
		vertices[edge.from] = struct{}{}
		vertices[edge.to] = struct{}{}
		key := edge.from + "\x00" + edge.to
		contractsByPair[key] = append(contractsByPair[key], edge.contractID)
	}

	index := 0
	indices := make(map[string]int)
	lowlink := make(map[string]int)
	onStack := make(map[string]bool)
	stack := make([]string, 0, len(vertices))
	var components [][]string

	var strongConnect func(string)
	strongConnect = func(node string) {
		indices[node] = index
		lowlink[node] = index
		index++
		stack = append(stack, node)
		onStack[node] = true

		for _, next := range adj[node] {
			if _, seen := indices[next]; !seen {
				strongConnect(next)
				if lowlink[next] < lowlink[node] {
					lowlink[node] = lowlink[next]
				}
			} else if onStack[next] && indices[next] < lowlink[node] {
				lowlink[node] = indices[next]
			}
		}

		if lowlink[node] != indices[node] {
			return
		}
		var component []string
		for {
			last := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[last] = false
			component = append(component, last)
			if last == node {
				break
			}
		}
		components = append(components, component)
	}

	var nodes []string
	for node := range vertices {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		if _, seen := indices[node]; !seen {
			strongConnect(node)
		}
	}

	var cycles []Cycle
	for _, component := range components {
		member := make(map[string]bool, len(component))
		for _, node := range component {
			member[node] = true
		}
		isCycle := len(component) > 1
		var contractIDs []string
		for from := range member {
			for _, to := range adj[from] {
				if !member[to] {
					continue
				}
				if from == to {
					isCycle = true
				}
				contractIDs = append(contractIDs, contractsByPair[from+"\x00"+to]...)
			}
		}
		if !isCycle {
			continue
		}
		sort.Strings(component)
		sort.Strings(contractIDs)
		identity := strings.Join(component, "\x00") + "\x01" + strings.Join(contractIDs, "\x00")
		sum := sha256.Sum256([]byte(identity))
		cycles = append(cycles, Cycle{
			ID:          "cycle_" + hex.EncodeToString(sum[:8]),
			NodeIDs:     component,
			ContractIDs: contractIDs,
		})
	}
	sort.Slice(cycles, func(i, j int) bool { return cycles[i].ID < cycles[j].ID })
	return cycles
}
