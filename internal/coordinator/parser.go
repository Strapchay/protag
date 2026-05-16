package coordinator

import (
	"fmt"
	"strings"
)

// ParseSpecMarkdown parses a technical specification in Markdown format
// into a PlanResponse.
// It looks for sections like:
// ## Domains
// - DomainID: Description (Paths: path1, path2)
// ## Tasks
// - TaskID [DomainID]: Task description (Priority: N)
func ParseSpecMarkdown(markdown string) *PlanResponse {
	lines := strings.Split(markdown, "\n")
	resp := &PlanResponse{}
	
	currentSection := ""
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		
		if strings.HasPrefix(line, "## ") {
			currentSection = strings.ToLower(strings.TrimPrefix(line, "## "))
			continue
		}
		
		switch currentSection {
		case "domains":
			if strings.HasPrefix(line, "- ") {
				d := parseDomainLine(strings.TrimPrefix(line, "- "))
				if d != nil {
					resp.Domains = append(resp.Domains, *d)
				}
			}
		case "tasks":
			if strings.HasPrefix(line, "- ") {
				t := parseTaskLine(strings.TrimPrefix(line, "- "))
				if t != nil {
					resp.Nodes = append(resp.Nodes, *t)
				}
			}
		}
	}
	
	return resp
}

func parseDomainLine(line string) *Domain {
	// Format: DomainID: Description (Paths: p1, p2)
	parts := strings.SplitN(line, ":", 2)
	if len(parts) < 2 {
		return nil
	}
	id := strings.TrimSpace(parts[0])
	rest := strings.TrimSpace(parts[1])
	
	desc := rest
	var paths []string
	if idx := strings.LastIndex(rest, "(Paths:"); idx != -1 {
		desc = strings.TrimSpace(rest[:idx])
		pathStr := strings.TrimSuffix(strings.TrimPrefix(rest[idx:], "(Paths:"), ")")
		for _, p := range strings.Split(pathStr, ",") {
			paths = append(paths, strings.TrimSpace(p))
		}
	}
	
	return &Domain{
		DomainID:      id,
		Description:   desc,
		AssignedPaths: paths,
		AgentType:     "domain",
	}
}

func parseTaskLine(line string) *TaskNode {
	// Format: TaskID [DomainID]: Description (Priority: N)
	parts := strings.SplitN(line, ":", 2)
	if len(parts) < 2 {
		return nil
	}
	
	header := strings.TrimSpace(parts[0])
	rest := strings.TrimSpace(parts[1])
	
	id := header
	domainID := "general"
	if idx := strings.Index(header, "["); idx != -1 {
		id = strings.TrimSpace(header[:idx])
		domainID = strings.TrimSuffix(strings.TrimPrefix(header[idx:], "["), "]")
	}
	
	desc := rest
	var priority int32 = 1
	if idx := strings.LastIndex(rest, "(Priority:"); idx != -1 {
		desc = strings.TrimSpace(rest[:idx])
		fmt.Sscanf(strings.TrimPrefix(rest[idx:], "(Priority:"), "%d", &priority)
	}
	
	return &TaskNode{
		ID:       id,
		DomainID: domainID,
		TaskSpec: desc,
		Priority: priority,
	}
}
