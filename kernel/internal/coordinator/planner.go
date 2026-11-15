package coordinator

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"aion-kernel/internal/dag"
)

func PlanFromPromptAndScan(prompt string, scan *ProjectScan) (*PlanResponse, error) {
	if scan == nil {
		return nil, fmt.Errorf("coordinator: project scan required")
	}

	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, fmt.Errorf("coordinator: planning prompt required")
	}

	if structured := ParseSpecMarkdown(prompt); structured != nil && len(structured.Domains) > 0 && len(structured.Nodes) > 0 {
		if err := ValidatePlanResponse(structured); err == nil {
			return structured, nil
		}
	}

	if narrative := planFromNarrativeSpec(prompt, scan); narrative != nil {
		if err := ValidatePlanResponse(narrative); err == nil {
			return narrative, nil
		}
	}

	return nil, fmt.Errorf("coordinator: unable to derive a structured plan from the supplied spec")
}

// ParsePlanResponseText extracts a plan_response JSON object from raw agent output.
func ParsePlanResponseText(text string) (*PlanResponse, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("coordinator: empty plan response")
	}

	candidates := extractJSONCandidates(text)
	var lastErr error
	for _, candidate := range candidates {
		plan, err := decodePlanResponseJSON(candidate)
		if err == nil {
			return plan, nil
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("coordinator: no JSON candidate found in plan response; preview: %s", previewPlanResponse(text))
	}
	return nil, lastErr
}

func extractJSONCandidates(text string) []string {
	var candidates []string
	add := func(candidate string) {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			return
		}
		if !strings.HasPrefix(candidate, "{") || !strings.HasSuffix(candidate, "}") {
			return
		}
		for _, existing := range candidates {
			if existing == candidate {
				return
			}
		}
		candidates = append(candidates, candidate)
	}

	const beginMarker = "AION_PLAN_RESPONSE_JSON_BEGIN"
	const endMarker = "AION_PLAN_RESPONSE_JSON_END"
	if start := strings.Index(text, beginMarker); start >= 0 {
		start += len(beginMarker)
		if end := strings.Index(text[start:], endMarker); end >= 0 {
			add(text[start : start+end])
		}
	}

	fenceRe := regexp.MustCompile("(?s)```(?:json)?\\s*(.*?)\\s*```")
	for _, match := range fenceRe.FindAllStringSubmatch(text, -1) {
		if len(match) > 1 {
			add(match[1])
		}
	}

	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start >= 0 && end > start {
		add(text[start : end+1])
	}
	add(text)

	return candidates
}

func previewPlanResponse(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "<empty>"
	}
	const limit = 500
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "...<truncated>"
}

func decodePlanResponseJSON(raw string) (*PlanResponse, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("coordinator: empty JSON candidate")
	}

	type planWrapper struct {
		Type string `json:"type"`
		PlanResponse
	}

	var wrapped planWrapper
	if err := json.Unmarshal([]byte(raw), &wrapped); err == nil {
		if wrapped.Type == "" || strings.EqualFold(strings.TrimSpace(wrapped.Type), "plan_response") || len(wrapped.Domains) > 0 || len(wrapped.Nodes) > 0 || len(wrapped.Edges) > 0 {
			plan := wrapped.PlanResponse
			return &plan, nil
		}
	}

	var plan PlanResponse
	if err := json.Unmarshal([]byte(raw), &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func ValidatePlanResponse(plan *PlanResponse) error {
	if plan == nil {
		return fmt.Errorf("coordinator: plan is nil")
	}
	if len(plan.Domains) == 0 {
		return fmt.Errorf("coordinator: plan must contain at least one domain")
	}
	if len(plan.Nodes) == 0 {
		return fmt.Errorf("coordinator: plan must contain at least one task node")
	}

	domainIDs := make(map[string]struct{}, len(plan.Domains))
	domainPaths := make(map[string][]string, len(plan.Domains))
	type pathGrant struct {
		domainID string
		path     string
	}
	var grants []pathGrant
	for _, domain := range plan.Domains {
		id := strings.TrimSpace(domain.DomainID)
		if id == "" {
			return fmt.Errorf("coordinator: domain id is required")
		}
		if id == "..." {
			return fmt.Errorf("coordinator: domain id %q is a placeholder", id)
		}
		if _, exists := domainIDs[id]; exists {
			return fmt.Errorf("coordinator: duplicate domain id %q", id)
		}
		domainIDs[id] = struct{}{}
		for _, rawPath := range domain.AssignedPaths {
			if IsProjectScanExcluded(rawPath, DefaultProjectScanExcludes()) {
				return fmt.Errorf("coordinator: domain %q assigned excluded path %q", id, rawPath)
			}
			assignedPath, err := normalizeOwnedPath(rawPath)
			if err != nil {
				return fmt.Errorf("coordinator: domain %q assigned path %q: %w", id, rawPath, err)
			}
			for _, grant := range grants {
				if pathsOverlap(grant.path, assignedPath) {
					return fmt.Errorf(
						"coordinator: assigned paths %q (domain %q) and %q (domain %q) overlap",
						grant.path, grant.domainID, assignedPath, id,
					)
				}
			}
			grants = append(grants, pathGrant{domainID: id, path: assignedPath})
			domainPaths[id] = append(domainPaths[id], assignedPath)
		}
	}
	nodeIDs := make(map[string]struct{}, len(plan.Nodes))
	nodeDomains := make(map[string]string, len(plan.Nodes))
	nodeHasDirective := make(map[string]bool, len(plan.Nodes))
	for _, node := range plan.Nodes {
		id := strings.TrimSpace(node.ID)
		if id == "" {
			return fmt.Errorf("coordinator: node id is required")
		}
		if id == "..." {
			return fmt.Errorf("coordinator: node id %q is a placeholder", id)
		}
		if _, exists := nodeIDs[id]; exists {
			return fmt.Errorf("coordinator: duplicate node id %q", id)
		}
		if _, ok := domainIDs[strings.TrimSpace(node.DomainID)]; !ok {
			return fmt.Errorf("coordinator: node %q references unknown domain %q", id, node.DomainID)
		}
		for _, rawPath := range node.TargetFiles {
			if IsProjectScanExcluded(rawPath, DefaultProjectScanExcludes()) {
				return fmt.Errorf("coordinator: node %q targets excluded path %q", id, rawPath)
			}
			targetPath, err := normalizeOwnedPath(rawPath)
			if err != nil {
				return fmt.Errorf("coordinator: node %q target path %q: %w", id, rawPath, err)
			}
			nodeDomain := strings.TrimSpace(node.DomainID)
			if !pathCoveredByOwnership(targetPath, domainPaths[nodeDomain]) {
				return fmt.Errorf(
					"coordinator: node %q target %q is outside domain %q assigned paths",
					id, rawPath, nodeDomain,
				)
			}
			if isIntegrationDirectiveTarget(targetPath) && strings.ToLower(filepath.Ext(targetPath)) != ".json" {
				return fmt.Errorf("coordinator: integration directive target %q must use a .json extension", rawPath)
			}
			if isIntegrationDirectiveTarget(targetPath) {
				nodeHasDirective[id] = true
			}
		}
		nodeIDs[id] = struct{}{}
		nodeDomains[id] = strings.TrimSpace(node.DomainID)
	}

	edges := make(map[string]struct{}, len(plan.Edges))
	for _, edge := range plan.Edges {
		if edge.FromNode == "" || edge.ToNode == "" {
			return fmt.Errorf("coordinator: edge endpoints are required")
		}
		if edge.FromNode == edge.ToNode {
			return fmt.Errorf("coordinator: edge %q -> %q is self-referential", edge.FromNode, edge.ToNode)
		}
		if _, ok := nodeIDs[edge.FromNode]; !ok {
			return fmt.Errorf("coordinator: edge references unknown source node %q", edge.FromNode)
		}
		if _, ok := nodeIDs[edge.ToNode]; !ok {
			return fmt.Errorf("coordinator: edge references unknown target node %q", edge.ToNode)
		}
		edges[edge.FromNode+"\x00"+edge.ToNode] = struct{}{}
	}

	contracts := make(map[string]struct{}, len(plan.Contracts))
	for _, contract := range plan.Contracts {
		resource := strings.TrimSpace(contract.Resource)
		producer := strings.TrimSpace(contract.ProducerNode)
		consumer := strings.TrimSpace(contract.ConsumerNode)
		kind := strings.ToLower(strings.TrimSpace(contract.Kind))
		if kind == "" {
			kind = "interface"
		}
		if resource == "" || producer == "" || consumer == "" {
			return fmt.Errorf("coordinator: planned contract resource and producer/consumer nodes are required")
		}
		if len(contract.Acceptance) == 0 {
			return fmt.Errorf("coordinator: planned contract %q requires acceptance criteria", resource)
		}
		if kind == "interface" && strings.TrimSpace(contract.PublicEntrypoint) == "" {
			return fmt.Errorf("coordinator: planned interface contract %q requires a public_entrypoint", resource)
		}
		if producer == consumer {
			return fmt.Errorf("coordinator: planned contract %q cannot use the same producer and consumer node", resource)
		}
		if _, ok := nodeIDs[producer]; !ok {
			return fmt.Errorf("coordinator: planned contract %q references unknown producer node %q", resource, producer)
		}
		if _, ok := nodeIDs[consumer]; !ok {
			return fmt.Errorf("coordinator: planned contract %q references unknown consumer node %q", resource, consumer)
		}
		if nodeDomains[producer] == nodeDomains[consumer] {
			return fmt.Errorf("coordinator: planned contract %q must cross domain boundaries", resource)
		}
		if _, ok := edges[producer+"\x00"+consumer]; !ok {
			return fmt.Errorf("coordinator: planned contract %q requires dependency edge %s -> %s", resource, producer, consumer)
		}
		if !nodeHasDirective[consumer] {
			return fmt.Errorf("coordinator: planned contract %q consumer node %q requires an owned integration directive target", resource, consumer)
		}
		identity := producer + "\x00" + consumer + "\x00" + strings.ToLower(resource)
		if _, exists := contracts[identity]; exists {
			return fmt.Errorf("coordinator: duplicate planned contract %q between %s and %s", resource, producer, consumer)
		}
		contracts[identity] = struct{}{}
	}

	dagNodes := make([]dag.DagNode, 0, len(plan.Nodes))
	for _, node := range plan.Nodes {
		dagNodes = append(dagNodes, dag.DagNode{
			ID:          node.ID,
			DomainID:    node.DomainID,
			TargetFiles: node.TargetFiles,
		})
	}
	dagEdges := make([]dag.DagEdge, 0, len(plan.Edges))
	for _, edge := range plan.Edges {
		dagEdges = append(dagEdges, dag.DagEdge{FromNode: edge.FromNode, ToNode: edge.ToNode})
	}
	if err := dag.ValidateCycles(dagNodes, dagEdges); err != nil {
		return err
	}
	if err := dag.ValidateAssignments(dagNodes); err != nil {
		return err
	}

	return nil
}

func isIntegrationDirectiveTarget(path string) bool {
	clean := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(strings.TrimSpace(path))), "./")
	return strings.Contains("/"+clean, "/integration/directives/")
}

func normalizeOwnedPath(raw string) (string, error) {
	value := strings.Trim(strings.TrimSpace(raw), "`\"'")
	value = strings.ReplaceAll(value, "\\", "/")
	if value == "" {
		return "", fmt.Errorf("path is empty")
	}
	if filepath.IsAbs(value) || strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("path must be project-relative")
	}
	clean := filepath.ToSlash(filepath.Clean(value))
	if clean == "." {
		return "", fmt.Errorf("project root ownership is not allowed")
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path escapes the project root")
	}
	return strings.TrimPrefix(clean, "./"), nil
}

func pathsOverlap(left, right string) bool {
	return left == right ||
		strings.HasPrefix(left, right+"/") ||
		strings.HasPrefix(right, left+"/")
}

func pathCoveredByOwnership(target string, assigned []string) bool {
	for _, root := range assigned {
		if target == root || strings.HasPrefix(target, root+"/") {
			return true
		}
	}
	return false
}

type narrativeSection struct {
	Heading string
	Body    []string
}

func planFromNarrativeSpec(markdown string, scan *ProjectScan) *PlanResponse {
	sections := extractNarrativeSections(markdown)
	if len(sections) == 0 {
		return nil
	}

	plan := &PlanResponse{}
	entryPoints := append([]string(nil), scan.EntryPoints...)
	dependencyFiles := append([]string(nil), scan.DependencyFiles...)

	for i, section := range sections {
		domainID := slugify(section.Heading)
		if domainID == "" {
			domainID = "section-" + strconv.Itoa(i+1)
		}
		targetFiles := distributeFiles(entryPoints, i, len(sections))
		assignedPaths := assignedPathsFor(targetFiles)
		if len(assignedPaths) == 0 && len(targetFiles) == 0 && len(dependencyFiles) > 0 && i == len(sections)-1 {
			targetFiles = append([]string(nil), dependencyFiles...)
			assignedPaths = assignedPathsFor(targetFiles)
		}
		if len(targetFiles) == 0 || len(assignedPaths) == 0 {
			continue
		}

		plan.Domains = append(plan.Domains, Domain{
			DomainID:      domainID,
			Description:   summarizeSection(section),
			AssignedPaths: assignedPaths,
			AgentType:     agentTypeForSection(section.Heading, targetFiles),
		})
		plan.Nodes = append(plan.Nodes, TaskNode{
			ID:          domainID + "-task",
			DomainID:    domainID,
			TaskSpec:    summarizeSection(section),
			TargetFiles: targetFiles,
			Priority:    int32(i + 1),
		})
		if len(plan.Nodes) > 1 {
			plan.Edges = append(plan.Edges, TaskEdge{
				FromNode: plan.Nodes[len(plan.Nodes)-2].ID,
				ToNode:   plan.Nodes[len(plan.Nodes)-1].ID,
				Reason:   "narrative section ordering",
			})
		}
	}

	return plan
}

func extractNarrativeSections(markdown string) []narrativeSection {
	var sections []narrativeSection
	var current *narrativeSection
	skip := map[string]struct{}{
		"purpose":                {},
		"flow":                   {},
		"spec input":             {},
		"coordinator output":     {},
		"validation rules":       {},
		"current implementation": {},
		"handoff state":          {},
		"build attempt recovery": {},
		"cancellation":           {},
		"out of scope":           {},
	}

	flush := func() {
		if current == nil {
			return
		}
		if strings.TrimSpace(current.Heading) == "" {
			return
		}
		sections = append(sections, *current)
	}

	for _, raw := range strings.Split(markdown, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "## ") {
			flush()
			heading := strings.TrimSpace(strings.TrimPrefix(line, "## "))
			normalized := normalizeHeading(heading)
			if _, ok := skip[normalized]; ok {
				current = nil
				continue
			}
			current = &narrativeSection{Heading: heading}
			continue
		}
		if current == nil {
			continue
		}
		if strings.HasPrefix(line, "### ") {
			current.Body = append(current.Body, strings.TrimSpace(strings.TrimPrefix(line, "### ")))
			continue
		}
		if strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") {
			current.Body = append(current.Body, strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "- "), "* ")))
			continue
		}
		if line != "" && !strings.HasPrefix(line, "#") {
			current.Body = append(current.Body, line)
		}
	}
	flush()
	return sections
}

func summarizeSection(section narrativeSection) string {
	body := strings.Join(section.Body, " ")
	body = strings.TrimSpace(body)
	if body == "" {
		return section.Heading
	}
	fields := strings.Fields(body)
	if len(fields) > 32 {
		fields = fields[:32]
	}
	return section.Heading + ": " + strings.Join(fields, " ")
}

func distributeFiles(files []string, index, count int) []string {
	if len(files) == 0 || count <= 0 {
		return nil
	}
	var out []string
	for i := index; i < len(files); i += count {
		out = append(out, files[i])
	}
	return out
}

func assignedPathsFor(files []string) []string {
	seen := map[string]struct{}{}
	var paths []string
	for _, file := range files {
		if file == "" {
			continue
		}
		dir := filepath.Dir(file)
		if dir == "." {
			if _, ok := seen[file]; !ok {
				seen[file] = struct{}{}
				paths = append(paths, file)
			}
			continue
		}
		if dir == "/" || dir == "" {
			continue
		}
		top := strings.Split(dir, string(filepath.Separator))[0]
		path := top + string(filepath.Separator)
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return paths
}

func agentTypeForSection(heading string, files []string) string {
	lower := strings.ToLower(heading)
	if strings.Contains(lower, "utility") || strings.Contains(lower, "deployment") || strings.Contains(lower, "operations") {
		return "utility"
	}
	if len(files) > 0 {
		return "domain"
	}
	return "domain"
}

func normalizeHeading(heading string) string {
	heading = strings.ToLower(strings.TrimSpace(heading))
	heading = strings.TrimPrefix(heading, "#")
	heading = strings.TrimSpace(heading)
	fields := strings.Fields(heading)
	if len(fields) > 0 {
		first := fields[0]
		if strings.Count(first, ".") == 1 {
			if _, err := strconv.Atoi(strings.TrimSuffix(first, ".")); err == nil {
				fields = fields[1:]
			}
		}
	}
	return strings.Join(fields, " ")
}

func slugify(text string) string {
	text = strings.ToLower(strings.TrimSpace(text))
	var b strings.Builder
	lastDash := false
	for _, r := range text {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return ""
	}
	return out
}
