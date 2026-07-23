package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	coordination "aion-coordination"
	isolation "aion-isolation"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
)

const integrationManifestSchema = 1

type IntegrationPhaseStatus string

const (
	IntegrationDisabled IntegrationPhaseStatus = "disabled"
	IntegrationPending  IntegrationPhaseStatus = "pending"
	IntegrationRunning  IntegrationPhaseStatus = "running"
	IntegrationPassed   IntegrationPhaseStatus = "passed"
	IntegrationFailed   IntegrationPhaseStatus = "failed"
	IntegrationSkipped  IntegrationPhaseStatus = "skipped"
)

// IntegrationDirective is a domain-owned, versioned validation instruction.
// The containing assigned path determines ownership; the document cannot grant
// itself access to another domain.
type IntegrationDirective struct {
	SchemaVersion     int               `json:"schema_version"`
	ID                string            `json:"id"`
	Description       string            `json:"description,omitempty"`
	Command           []string          `json:"command"`
	WorkingDirectory  string            `json:"working_directory,omitempty"`
	RequiredContracts []string          `json:"required_contracts,omitempty"`
	OwnerNodeIDs      []string          `json:"owner_node_ids,omitempty"`
	TimeoutSec        int               `json:"timeout_sec,omitempty"`
	Environment       map[string]string `json:"environment,omitempty"`
}

type IntegrationDirectiveArtifact struct {
	DomainID  string               `json:"domain_id"`
	Path      string               `json:"path"`
	Digest    string               `json:"digest"`
	Directive IntegrationDirective `json:"directive"`
}

type IntegrationPathArtifact struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type IntegrationDomainArtifact struct {
	DomainID      string                    `json:"domain_id"`
	AssignedPaths []IntegrationPathArtifact `json:"assigned_paths"`
	NodeIDs       []string                  `json:"node_ids"`
}

type IntegrationContractArtifact struct {
	ContractID   string              `json:"contract_id"`
	Revision     int64               `json:"revision"`
	Status       coordination.Status `json:"status"`
	Kind         string              `json:"kind,omitempty"`
	Name         string              `json:"name,omitempty"`
	Digest       string              `json:"digest,omitempty"`
	Size         int64               `json:"size,omitempty"`
	MediaType    string              `json:"media_type,omitempty"`
	ArtifactPath string              `json:"artifact_path,omitempty"`
}

// IntegrationManifest is the immutable shared artifact contract consumed by
// the runner. It exposes public outputs and contract artifacts without granting
// any domain agent peer-workspace access.
type IntegrationManifest struct {
	SchemaVersion int                            `json:"schema_version"`
	RunID         string                         `json:"run_id"`
	AttemptID     string                         `json:"attempt_id"`
	InputDigest   string                         `json:"input_digest"`
	GeneratedAt   time.Time                      `json:"generated_at"`
	Domains       []IntegrationDomainArtifact    `json:"domains"`
	SharedFiles   []IntegrationPathArtifact      `json:"shared_files,omitempty"`
	Contracts     []IntegrationContractArtifact  `json:"contracts,omitempty"`
	Directives    []IntegrationDirectiveArtifact `json:"directives"`
}

type IntegrationDirectiveResult struct {
	DirectiveID  string    `json:"directive_id"`
	DomainID     string    `json:"domain_id"`
	OwnerNodeIDs []string  `json:"owner_node_ids,omitempty"`
	Status       string    `json:"status"`
	ExitCode     int       `json:"exit_code"`
	DurationMS   int64     `json:"duration_ms"`
	Stdout       string    `json:"stdout,omitempty"`
	Stderr       string    `json:"stderr,omitempty"`
	Error        string    `json:"error,omitempty"`
	CompletedAt  time.Time `json:"completed_at"`
}

type IntegrationRunStatus struct {
	Enabled         bool                         `json:"enabled"`
	Status          IntegrationPhaseStatus       `json:"status"`
	RunID           string                       `json:"run_id,omitempty"`
	AttemptID       string                       `json:"attempt_id,omitempty"`
	InputDigest     string                       `json:"input_digest,omitempty"`
	ManifestPath    string                       `json:"manifest_path,omitempty"`
	DirectiveCount  int                          `json:"directive_count"`
	ContractCount   int                          `json:"contract_count"`
	SharedFileCount int                          `json:"shared_file_count"`
	Results         []IntegrationDirectiveResult `json:"results,omitempty"`
	Error           string                       `json:"error,omitempty"`
	StartedAt       *time.Time                   `json:"started_at,omitempty"`
	CompletedAt     *time.Time                   `json:"completed_at,omitempty"`
	UpdatedAt       time.Time                    `json:"updated_at"`
}

type integrationBundle struct {
	manifest     IntegrationManifest
	root         string
	domains      []isolation.Mount
	shared       []isolation.Mount
	contracts    string
	manifestPath string
}

type IntegrationRunner struct {
	mu           sync.Mutex
	config       IntegrationConfig
	sharedFiles  []string
	projectRoot  string
	runRoot      string
	coordination *coordination.Manager
	isolation    isolation.Engine
	baseContext  context.Context
	cancel       context.CancelFunc
	status       IntegrationRunStatus
	preparing    bool
	executing    bool
	closed       bool
	onChange     func(IntegrationRunStatus)
}

func NewIntegrationRunner(config *Config, projectRoot string, runState *RunState, manager *coordination.Manager, engine isolation.Engine, baseContext context.Context) *IntegrationRunner {
	if baseContext == nil {
		baseContext = context.Background()
	}
	baseContext, cancel := context.WithCancel(baseContext)
	runner := &IntegrationRunner{
		projectRoot:  projectRoot,
		coordination: manager,
		isolation:    engine,
		baseContext:  baseContext,
		cancel:       cancel,
		status:       IntegrationRunStatus{Status: IntegrationPending, UpdatedAt: time.Now().UTC()},
	}
	if config != nil {
		runner.config = config.Integration
		runner.sharedFiles = append([]string(nil), config.Agents.SharedFiles...)
		runner.status.Enabled = config.Integration.Enabled
	}
	if runState != nil {
		runner.runRoot = runState.Root
	}
	if loaded, err := loadIntegrationRunStatus(runner.runRoot); err == nil && loaded != nil {
		runner.status = *loaded
	}
	return runner
}

func (r *IntegrationRunner) SetChangeFunc(fn func(IntegrationRunStatus)) {
	r.mu.Lock()
	r.onChange = fn
	r.mu.Unlock()
}

func (r *IntegrationRunner) Status() IntegrationRunStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneIntegrationStatus(r.status)
}

func (r *IntegrationRunner) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Gate starts validation once and reports whether the build may converge.
func (r *IntegrationRunner) Gate(ctx context.Context, snapshot *dag.DagData) (bool, error) {
	if r == nil {
		return true, nil
	}
	if !r.config.Enabled {
		r.setTerminalStatus(IntegrationDisabled, "", nil)
		return true, nil
	}
	attempt, err := loadBuildSpecAttempt(r.runRoot)
	if err != nil {
		return false, err
	}
	if attempt == nil || attempt.Plan == nil {
		return false, fmt.Errorf("integration: active build-spec plan is unavailable")
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false, fmt.Errorf("integration: runner is closed")
	}
	var priorTerminal *IntegrationRunStatus
	if r.status.AttemptID == attempt.AttemptID {
		switch r.status.Status {
		case IntegrationPassed, IntegrationSkipped:
			prior := cloneIntegrationStatus(r.status)
			priorTerminal = &prior
		case IntegrationFailed:
			r.mu.Unlock()
			return false, nil
		case IntegrationRunning:
			if r.executing {
				r.mu.Unlock()
				return false, nil
			}
			// A persisted running state has no process after restart.
			r.status.Status = IntegrationPending
		case IntegrationPending:
		}
	}
	if r.preparing || r.executing {
		r.mu.Unlock()
		return false, nil
	}
	r.preparing = true
	r.mu.Unlock()

	bundle, buildErr := r.prepareBundle(ctx, attempt, snapshot)
	r.mu.Lock()
	r.preparing = false
	if r.closed {
		r.mu.Unlock()
		return false, context.Canceled
	}
	if buildErr != nil {
		r.status = IntegrationRunStatus{
			Enabled: true, Status: IntegrationFailed, RunID: attempt.RunID, AttemptID: attempt.AttemptID,
			Error: buildErr.Error(), UpdatedAt: time.Now().UTC(),
		}
		now := r.status.UpdatedAt
		r.status.CompletedAt = &now
		status := cloneIntegrationStatus(r.status)
		r.persistLocked()
		callback := r.onChange
		r.mu.Unlock()
		notifyIntegrationChange(callback, status)
		return false, nil
	}
	if priorTerminal != nil && priorTerminal.InputDigest == bundle.manifest.InputDigest {
		r.status = *priorTerminal
		r.status.ManifestPath = bundle.manifestPath
		r.status.UpdatedAt = time.Now().UTC()
		r.persistLocked()
		r.mu.Unlock()
		return true, nil
	}
	if len(bundle.manifest.Directives) == 0 {
		r.status = statusForBundle(bundle, IntegrationSkipped)
		r.status.Error = "no integration directives were published"
		now := time.Now().UTC()
		r.status.CompletedAt = &now
		status := cloneIntegrationStatus(r.status)
		r.persistLocked()
		callback := r.onChange
		r.mu.Unlock()
		notifyIntegrationChange(callback, status)
		return true, nil
	}
	r.status = statusForBundle(bundle, IntegrationRunning)
	now := time.Now().UTC()
	r.status.StartedAt = &now
	r.status.UpdatedAt = now
	r.executing = true
	status := cloneIntegrationStatus(r.status)
	r.persistLocked()
	callback := r.onChange
	r.mu.Unlock()
	notifyIntegrationChange(callback, status)

	go r.executeBundle(bundle)
	return false, nil
}

func (r *IntegrationRunner) Retry() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("integration: runner is closed")
	}
	if !r.config.Enabled {
		return fmt.Errorf("integration: validation is disabled")
	}
	if r.executing || r.preparing {
		return fmt.Errorf("integration: validation is already running")
	}
	r.status.Status = IntegrationPending
	r.status.Error = ""
	r.status.Results = nil
	r.status.StartedAt = nil
	r.status.CompletedAt = nil
	r.status.UpdatedAt = time.Now().UTC()
	return r.persistLocked()
}

func (r *IntegrationRunner) executeBundle(bundle *integrationBundle) {
	results := make([]IntegrationDirectiveResult, 0, len(bundle.manifest.Directives))
	var runErr error
	for index, artifact := range bundle.manifest.Directives {
		result, err := r.executeDirective(bundle, artifact, index)
		results = append(results, result)
		if err != nil {
			runErr = err
			break
		}
	}

	r.mu.Lock()
	r.executing = false
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.status.Results = results
	now := time.Now().UTC()
	r.status.CompletedAt = &now
	r.status.UpdatedAt = now
	if runErr != nil {
		r.status.Status = IntegrationFailed
		r.status.Error = runErr.Error()
	} else {
		r.status.Status = IntegrationPassed
		r.status.Error = ""
	}
	status := cloneIntegrationStatus(r.status)
	r.persistLocked()
	callback := r.onChange
	r.mu.Unlock()
	notifyIntegrationChange(callback, status)
}

func (r *IntegrationRunner) executeDirective(bundle *integrationBundle, artifact IntegrationDirectiveArtifact, index int) (IntegrationDirectiveResult, error) {
	directive := artifact.Directive
	started := time.Now()
	result := IntegrationDirectiveResult{
		DirectiveID:  directive.ID,
		DomainID:     artifact.DomainID,
		OwnerNodeIDs: append([]string(nil), directive.OwnerNodeIDs...),
		Status:       "failed",
		ExitCode:     -1,
	}

	outputDir := filepath.Join(r.runRoot, "integration", "output", bundle.manifest.InputDigest)
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		result.Error = err.Error()
		return result, err
	}
	policy := isolation.Policy{
		ID:            fmt.Sprintf("integration-%s-%d", bundle.manifest.InputDigest[:12], index+1),
		Generation:    uint64(index + 1),
		WorkingDir:    directiveWorkingDir(directive.WorkingDirectory),
		Hostname:      "aion-integration",
		Network:       isolation.NetworkIsolated,
		SourceRoot:    r.projectRoot,
		DeniedSources: projectDeniedSources(r.projectRoot),
		ReadOnly:      append(append([]isolation.Mount(nil), bundle.domains...), bundle.shared...),
		Writable:      []isolation.Mount{{Source: outputDir, Target: "/state/integration", Kind: isolation.MountDirectory}},
		Environment: map[string]string{
			"AION_INTEGRATION_MANIFEST": "/context/integration-manifest.json",
			"AION_CONTRACT_ROOT":        "/contracts/artifacts",
			"AION_INTEGRATION_OUTPUT":   "/state/integration",
		},
	}
	policy.ReadOnly = append(policy.ReadOnly, isolation.Mount{Source: bundle.manifestPath, Target: "/context/integration-manifest.json", Kind: isolation.MountFile})
	if bundle.contracts != "" {
		policy.ReadOnly = append(policy.ReadOnly, isolation.Mount{Source: bundle.contracts, Target: "/contracts/artifacts", Kind: isolation.MountDirectory})
	}
	for name, value := range directive.Environment {
		policy.Environment[name] = value
	}

	timeout := time.Duration(r.config.TimeoutSec) * time.Second
	if directive.TimeoutSec > 0 && directive.TimeoutSec < r.config.TimeoutSec {
		timeout = time.Duration(directive.TimeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(r.baseContext, timeout)
	defer cancel()
	workspace, err := r.isolation.Prepare(ctx, policy)
	if err != nil {
		result.Error = err.Error()
		return finishDirectiveResult(result, started), err
	}
	defer workspace.Close()
	cmd, err := workspace.Command(ctx, isolation.CommandSpec{Path: directive.Command[0], Args: directive.Command[1:]})
	if err != nil {
		result.Error = err.Error()
		return finishDirectiveResult(result, started), err
	}
	var stdout, stderr cappedBuffer
	stdout.limit = r.config.MaxOutputBytes
	stderr.limit = r.config.MaxOutputBytes
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	result.ExitCode = 0
	if err != nil {
		result.Error = err.Error()
		var exitErr *os.PathError
		if !errors.As(err, &exitErr) && cmd.ProcessState != nil {
			result.ExitCode = cmd.ProcessState.ExitCode()
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Error = fmt.Sprintf("directive exceeded %s timeout", timeout)
		}
		return finishDirectiveResult(result, started), fmt.Errorf("integration directive %s failed: %s", directive.ID, result.Error)
	}
	result.Status = "passed"
	return finishDirectiveResult(result, started), nil
}

func (r *IntegrationRunner) prepareBundle(ctx context.Context, attempt *BuildSpecAttempt, snapshot *dag.DagData) (*integrationBundle, error) {
	if r.isolation == nil {
		return nil, fmt.Errorf("integration: isolation engine is unavailable")
	}
	if snapshot == nil {
		return nil, fmt.Errorf("integration: DAG snapshot is unavailable")
	}
	for _, node := range snapshot.Nodes {
		if node.Status == dag.StatusFailed {
			return nil, fmt.Errorf("integration: implementation node %s failed", node.ID)
		}
		if node.Status != dag.StatusDone {
			return nil, fmt.Errorf("integration: implementation node %s is not complete", node.ID)
		}
	}

	manifest := IntegrationManifest{SchemaVersion: integrationManifestSchema, RunID: attempt.RunID, AttemptID: attempt.AttemptID, GeneratedAt: time.Now().UTC()}
	bundle := &integrationBundle{manifest: manifest}
	nodesByDomain := make(map[string][]string)
	for _, node := range snapshot.Nodes {
		nodesByDomain[node.DomainID] = append(nodesByDomain[node.DomainID], node.ID)
	}
	plannedDirectives := plannedIntegrationDirectiveTargets(attempt.Plan, r.config.DirectiveDir)
	for _, domain := range attempt.Plan.Domains {
		domainArtifact, mounts, directives, err := r.domainArtifacts(ctx, domain, nodesByDomain[domain.DomainID], plannedDirectives[domain.DomainID])
		if err != nil {
			return nil, err
		}
		bundle.manifest.Domains = append(bundle.manifest.Domains, domainArtifact)
		bundle.manifest.Directives = append(bundle.manifest.Directives, directives...)
		bundle.domains = append(bundle.domains, mounts...)
	}
	if len(bundle.manifest.Directives) > r.config.MaxDirectives {
		return nil, fmt.Errorf("integration: %d directives exceeds configured maximum %d", len(bundle.manifest.Directives), r.config.MaxDirectives)
	}
	contracts, contractContents, err := r.contractArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	bundle.manifest.Contracts = contracts
	if err := validateDirectiveSet(bundle.manifest.Directives, snapshot, contracts); err != nil {
		return nil, err
	}
	sharedContents, sharedArtifacts, err := r.sharedArtifacts()
	if err != nil {
		return nil, err
	}
	bundle.manifest.SharedFiles = sharedArtifacts

	digest, err := integrationManifestDigest(bundle.manifest)
	if err != nil {
		return nil, err
	}
	bundle.manifest.InputDigest = digest
	bundle.root = filepath.Join(r.runRoot, "integration", "bundles", digest)
	bundle.contracts = filepath.Join(bundle.root, "contracts")
	bundle.manifestPath = filepath.Join(bundle.root, "manifest.json")
	if err := materializeIntegrationBundle(bundle, contractContents, sharedContents); err != nil {
		return nil, err
	}
	for _, artifact := range sharedArtifacts {
		source := filepath.Join(bundle.root, "shared", filepath.FromSlash(artifact.Path))
		target := filepath.ToSlash(filepath.Join("/workspace", artifact.Path))
		if integrationMountTargetCovered(target, bundle.domains) {
			continue
		}
		bundle.shared = append(bundle.shared, isolation.Mount{Source: source, Target: target, Kind: isolation.MountFile})
	}
	return bundle, nil
}

func (r *IntegrationRunner) domainArtifacts(ctx context.Context, domain coordinator.Domain, nodeIDs, plannedDirectivePaths []string) (IntegrationDomainArtifact, []isolation.Mount, []IntegrationDirectiveArtifact, error) {
	artifact := IntegrationDomainArtifact{DomainID: domain.DomainID, NodeIDs: append([]string(nil), nodeIDs...)}
	var mounts []isolation.Mount
	var directives []IntegrationDirectiveArtifact
	for _, raw := range domain.AssignedPaths {
		select {
		case <-ctx.Done():
			return artifact, nil, nil, ctx.Err()
		default:
		}
		rel, source, info, err := resolveAssignedPath(r.projectRoot, raw)
		if err != nil {
			return artifact, nil, nil, fmt.Errorf("integration: domain %s: %w", domain.DomainID, err)
		}
		digest, size, err := hashPath(source)
		if err != nil {
			return artifact, nil, nil, fmt.Errorf("integration: hash %s: %w", rel, err)
		}
		kind := "file"
		mountKind := isolation.MountFile
		if info.IsDir() {
			kind = "directory"
			mountKind = isolation.MountDirectory
		}
		artifact.AssignedPaths = append(artifact.AssignedPaths, IntegrationPathArtifact{Path: rel, Kind: kind, Digest: digest, Size: size})
		mounts = append(mounts, isolation.Mount{Source: source, Target: filepath.ToSlash(filepath.Join("/workspace", rel)), Kind: mountKind})
		if info.IsDir() {
			found, err := discoverIntegrationDirectives(source, rel, domain.DomainID, r.config.DirectiveDir)
			if err != nil {
				return artifact, nil, nil, err
			}
			directives = append(directives, found...)
		}
	}
	foundPaths := make(map[string]struct{}, len(directives))
	for _, directive := range directives {
		foundPaths[directive.Path] = struct{}{}
	}
	for _, plannedPath := range plannedDirectivePaths {
		if _, ok := foundPaths[plannedPath]; !ok {
			return artifact, nil, nil, fmt.Errorf("integration: domain %s did not publish planned directive %s", domain.DomainID, plannedPath)
		}
	}
	sort.Strings(artifact.NodeIDs)
	return artifact, mounts, directives, nil
}

func plannedIntegrationDirectiveTargets(plan *coordinator.PlanResponse, directiveDir string) map[string][]string {
	result := make(map[string][]string)
	if plan == nil {
		return result
	}
	marker := "/" + strings.Trim(filepath.ToSlash(filepath.Clean(directiveDir)), "/") + "/"
	seen := make(map[string]struct{})
	for _, node := range plan.Nodes {
		for _, raw := range node.TargetFiles {
			path := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(strings.TrimSpace(raw))), "./")
			if !strings.Contains("/"+path, marker) {
				continue
			}
			key := node.DomainID + "\x00" + path
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			result[node.DomainID] = append(result[node.DomainID], path)
		}
	}
	for domainID := range result {
		sort.Strings(result[domainID])
	}
	return result
}

func (r *IntegrationRunner) contractArtifacts(ctx context.Context) ([]IntegrationContractArtifact, map[string][]byte, error) {
	if r.coordination == nil {
		return nil, nil, nil
	}
	contracts, err := r.coordination.List(ctx, coordination.Principal{Role: coordination.RoleRunner})
	if err != nil {
		return nil, nil, fmt.Errorf("integration: list contracts: %w", err)
	}
	contents := make(map[string][]byte)
	artifacts := make([]IntegrationContractArtifact, 0, len(contracts))
	for _, contract := range contracts {
		entry := IntegrationContractArtifact{ContractID: contract.ID, Revision: contract.Revision, Status: contract.Status, Kind: contract.Details.Kind, Name: contract.Details.Name}
		if contract.Status == coordination.StatusFulfilled && contract.Artifact != nil {
			_, content, err := r.coordination.Fetch(ctx, coordination.Principal{Role: coordination.RoleRunner}, contract.ID, contract.Revision)
			if err != nil {
				return nil, nil, fmt.Errorf("integration: fetch contract %s: %w", contract.ID, err)
			}
			digest := sha256Hex(content)
			if digest != contract.Artifact.Digest {
				return nil, nil, fmt.Errorf("integration: contract %s digest mismatch", contract.ID)
			}
			key := safeArtifactKey(contract.ID)
			entry.Digest = digest
			entry.Size = int64(len(content))
			entry.MediaType = contract.Artifact.MediaType
			entry.ArtifactPath = "/contracts/artifacts/" + key + "/artifact"
			contents[key] = content
		}
		artifacts = append(artifacts, entry)
	}
	return artifacts, contents, nil
}

func (r *IntegrationRunner) sharedArtifacts() (map[string][]byte, []IntegrationPathArtifact, error) {
	contents := make(map[string][]byte)
	var artifacts []IntegrationPathArtifact
	for _, raw := range r.sharedFiles {
		rel, source, info, err := resolveAssignedPath(r.projectRoot, raw)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, nil, fmt.Errorf("integration: shared file: %w", err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		content, err := os.ReadFile(source)
		if err != nil {
			return nil, nil, err
		}
		contents[rel] = content
		artifacts = append(artifacts, IntegrationPathArtifact{Path: rel, Kind: "file", Digest: sha256Hex(content), Size: int64(len(content))})
	}
	return contents, artifacts, nil
}

func discoverIntegrationDirectives(assignedSource, assignedRel, domainID, directiveDir string) ([]IntegrationDirectiveArtifact, error) {
	root := filepath.Join(assignedSource, filepath.FromSlash(directiveDir))
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("integration: read directive directory %s: %w", root, err)
	}
	var artifacts []IntegrationDirectiveArtifact
	for _, entry := range entries {
		if entry.IsDir() || strings.ToLower(filepath.Ext(entry.Name())) != ".json" {
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("integration: directive %s must be a regular non-symlink file", path)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.DisallowUnknownFields()
		var directive IntegrationDirective
		if err := decoder.Decode(&directive); err != nil {
			return nil, fmt.Errorf("integration: parse directive %s: %w", path, err)
		}
		if err := validateDirective(directive); err != nil {
			return nil, fmt.Errorf("integration: directive %s: %w", path, err)
		}
		rel := filepath.ToSlash(filepath.Join(assignedRel, directiveDir, entry.Name()))
		artifacts = append(artifacts, IntegrationDirectiveArtifact{DomainID: domainID, Path: rel, Digest: sha256Hex(content), Directive: directive})
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Directive.ID < artifacts[j].Directive.ID })
	return artifacts, nil
}

func validateDirective(d IntegrationDirective) error {
	if d.SchemaVersion != integrationManifestSchema {
		return fmt.Errorf("unsupported schema_version %d", d.SchemaVersion)
	}
	if strings.TrimSpace(d.ID) == "" {
		return fmt.Errorf("id is required")
	}
	if len(d.Command) == 0 {
		return fmt.Errorf("command is required")
	}
	if len(d.OwnerNodeIDs) == 0 {
		return fmt.Errorf("owner_node_ids must contain at least one node")
	}
	path := filepath.Clean(strings.TrimSpace(d.Command[0]))
	if !filepath.IsAbs(path) || (!pathWithinRoot(path, "/usr") && !pathWithinRoot(path, "/bin")) {
		return fmt.Errorf("command executable must be an absolute /usr or /bin path")
	}
	if d.TimeoutSec < 0 {
		return fmt.Errorf("timeout_sec cannot be negative")
	}
	if d.WorkingDirectory != "" {
		rel := filepath.Clean(d.WorkingDirectory)
		if filepath.IsAbs(rel) || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("working_directory must be a project-relative path")
		}
		if len(d.RequiredContracts) > 0 {
			return fmt.Errorf("working_directory must be omitted for a cross-domain directive")
		}
	}
	return nil
}

func validatePlannedNodeDirectives(projectRoot, directiveDir string, node *dag.DagNode) error {
	if node == nil {
		return fmt.Errorf("node is required")
	}
	marker := "/" + strings.Trim(filepath.ToSlash(filepath.Clean(directiveDir)), "/") + "/"
	for _, target := range node.TargetFiles {
		rel := filepath.ToSlash(filepath.Clean(strings.TrimSpace(target)))
		if !strings.Contains("/"+rel, marker) {
			continue
		}
		resolvedRel, source, info, err := resolveAssignedPath(projectRoot, target)
		if err != nil {
			return fmt.Errorf("planned integration directive %s: %w", target, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("planned integration directive %s must be a regular file", resolvedRel)
		}
		content, err := os.ReadFile(source)
		if err != nil {
			return fmt.Errorf("read planned integration directive %s: %w", resolvedRel, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.DisallowUnknownFields()
		var directive IntegrationDirective
		if err := decoder.Decode(&directive); err != nil {
			return fmt.Errorf("parse planned integration directive %s: %w", resolvedRel, err)
		}
		if err := ensureJSONEOF(decoder); err != nil {
			return fmt.Errorf("parse planned integration directive %s: %w", resolvedRel, err)
		}
		if err := validateDirective(directive); err != nil {
			return fmt.Errorf("planned integration directive %s: %w", resolvedRel, err)
		}
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func validateDirectiveSet(directives []IntegrationDirectiveArtifact, snapshot *dag.DagData, contracts []IntegrationContractArtifact) error {
	seen := make(map[string]bool)
	nodeDomain := make(map[string]string)
	fulfilled := make(map[string]bool)
	for _, node := range snapshot.Nodes {
		nodeDomain[node.ID] = node.DomainID
	}
	for _, contract := range contracts {
		fulfilled[contract.ContractID] = contract.Status == coordination.StatusFulfilled && contract.Digest != ""
	}
	for _, artifact := range directives {
		id := artifact.Directive.ID
		if seen[id] {
			return fmt.Errorf("integration: duplicate directive id %q", id)
		}
		seen[id] = true
		for _, nodeID := range artifact.Directive.OwnerNodeIDs {
			if nodeDomain[nodeID] != artifact.DomainID {
				return fmt.Errorf("integration: directive %s cannot claim node %s outside domain %s", id, nodeID, artifact.DomainID)
			}
		}
		for _, contractID := range artifact.Directive.RequiredContracts {
			if !fulfilled[contractID] {
				return fmt.Errorf("integration: directive %s requires unfulfilled contract %s", id, contractID)
			}
		}
	}
	return nil
}

func materializeIntegrationBundle(bundle *integrationBundle, contracts map[string][]byte, shared map[string][]byte) error {
	if _, err := os.Stat(bundle.manifestPath); err == nil {
		return nil
	}
	tmp := bundle.root + fmt.Sprintf(".tmp-%d", time.Now().UnixNano())
	if err := os.MkdirAll(filepath.Join(tmp, "contracts"), 0o700); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for key, content := range contracts {
		dir := filepath.Join(tmp, "contracts", key)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "artifact"), content, 0o600); err != nil {
			return err
		}
	}
	for rel, content := range shared {
		path := filepath.Join(tmp, "shared", filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(bundle.manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmp, "manifest.json"), append(data, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(bundle.root), 0o700); err != nil {
		return err
	}
	if err := os.Rename(tmp, bundle.root); err != nil {
		if _, statErr := os.Stat(bundle.manifestPath); statErr == nil {
			return nil
		}
		return err
	}
	return nil
}

func integrationManifestDigest(manifest IntegrationManifest) (string, error) {
	manifest.InputDigest = ""
	manifest.GeneratedAt = time.Time{}
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	return sha256Hex(data), nil
}

func resolveAssignedPath(root, raw string) (string, string, os.FileInfo, error) {
	raw = strings.Trim(strings.TrimSpace(raw), "`\"")
	if raw == "" || filepath.IsAbs(raw) {
		return "", "", nil, fmt.Errorf("path %q must be project-relative", raw)
	}
	rel := filepath.Clean(raw)
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", nil, fmt.Errorf("path %q escapes project root", raw)
	}
	if isPrivateIntegrationPath(rel) {
		return "", "", nil, fmt.Errorf("private runtime path %q cannot be published", raw)
	}
	source := filepath.Join(root, rel)
	if !pathWithinRoot(source, root) || source == filepath.Clean(root) {
		return "", "", nil, fmt.Errorf("path %q escapes project root", raw)
	}
	if err := rejectIntegrationSymlinks(root, source); err != nil {
		return "", "", nil, err
	}
	info, err := os.Stat(source)
	if err != nil {
		return "", "", nil, err
	}
	return filepath.ToSlash(rel), source, info, nil
}

func rejectIntegrationSymlinks(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	current := filepath.Clean(root)
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("integration: symlink path component rejected: %s", current)
		}
	}
	return nil
}

func hashPath(path string) (string, int64, error) {
	hash := sha256.New()
	var size int64
	info, err := os.Lstat(path)
	if err != nil {
		return "", 0, err
	}
	if info.Mode().IsRegular() {
		file, err := os.Open(path)
		if err != nil {
			return "", 0, err
		}
		defer file.Close()
		n, err := io.Copy(hash, file)
		return hex.EncodeToString(hash.Sum(nil)), n, err
	}
	if !info.IsDir() {
		return "", 0, fmt.Errorf("unsupported file type")
	}
	var files []string
	err = filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink rejected: %s", current)
		}
		if entry.Type().IsRegular() {
			files = append(files, current)
		}
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	sort.Strings(files)
	for _, filePath := range files {
		rel, _ := filepath.Rel(path, filePath)
		hash.Write([]byte(filepath.ToSlash(rel)))
		hash.Write([]byte{0})
		file, err := os.Open(filePath)
		if err != nil {
			return "", 0, err
		}
		n, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		size += n
		if copyErr != nil {
			return "", 0, copyErr
		}
		if closeErr != nil {
			return "", 0, closeErr
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func projectDeniedSources(root string) []string {
	var paths []string
	for _, name := range []string{".aion", ".git", ".agents", ".codex"} {
		paths = append(paths, filepath.Join(root, name))
	}
	return paths
}

func isPrivateIntegrationPath(rel string) bool {
	first := strings.Split(filepath.ToSlash(rel), "/")[0]
	switch first {
	case ".aion", ".git", ".agents", ".codex":
		return true
	default:
		return false
	}
}

func integrationMountTargetCovered(target string, mounts []isolation.Mount) bool {
	for _, mount := range mounts {
		if pathWithinRoot(target, mount.Target) {
			return true
		}
	}
	return false
}

func directiveWorkingDir(rel string) string {
	if strings.TrimSpace(rel) == "" {
		return "/workspace"
	}
	return filepath.ToSlash(filepath.Join("/workspace", filepath.Clean(rel)))
}

func safeArtifactKey(value string) string { return sha256Hex([]byte(value))[:24] }
func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func statusForBundle(bundle *integrationBundle, status IntegrationPhaseStatus) IntegrationRunStatus {
	return IntegrationRunStatus{
		Enabled: true, Status: status, RunID: bundle.manifest.RunID, AttemptID: bundle.manifest.AttemptID,
		InputDigest: bundle.manifest.InputDigest, ManifestPath: bundle.manifestPath,
		DirectiveCount: len(bundle.manifest.Directives), ContractCount: len(bundle.manifest.Contracts),
		SharedFileCount: len(bundle.manifest.SharedFiles), UpdatedAt: time.Now().UTC(),
	}
}

func finishDirectiveResult(result IntegrationDirectiveResult, started time.Time) IntegrationDirectiveResult {
	result.DurationMS = time.Since(started).Milliseconds()
	result.CompletedAt = time.Now().UTC()
	return result
}

func integrationRunStatusPath(runRoot string) string {
	return filepath.Join(runRoot, "integration", "status.json")
}

func loadIntegrationRunStatus(runRoot string) (*IntegrationRunStatus, error) {
	data, err := os.ReadFile(integrationRunStatusPath(runRoot))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var status IntegrationRunStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (r *IntegrationRunner) persistLocked() error {
	if strings.TrimSpace(r.runRoot) == "" {
		return nil
	}
	data, err := json.MarshalIndent(r.status, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(integrationRunStatusPath(r.runRoot), append(data, '\n'))
}

func (r *IntegrationRunner) setTerminalStatus(status IntegrationPhaseStatus, message string, completed *time.Time) {
	r.mu.Lock()
	if r.status.Status == status && r.status.Error == message {
		r.mu.Unlock()
		return
	}
	r.status.Enabled = r.config.Enabled
	r.status.Status = status
	r.status.Error = message
	r.status.UpdatedAt = time.Now().UTC()
	if completed == nil {
		now := r.status.UpdatedAt
		completed = &now
	}
	r.status.CompletedAt = completed
	current := cloneIntegrationStatus(r.status)
	r.persistLocked()
	callback := r.onChange
	r.mu.Unlock()
	notifyIntegrationChange(callback, current)
}

func cloneIntegrationStatus(status IntegrationRunStatus) IntegrationRunStatus {
	status.Results = append([]IntegrationDirectiveResult(nil), status.Results...)
	for i := range status.Results {
		status.Results[i].OwnerNodeIDs = append([]string(nil), status.Results[i].OwnerNodeIDs...)
	}
	return status
}

func notifyIntegrationChange(callback func(IntegrationRunStatus), status IntegrationRunStatus) {
	if callback != nil {
		callback(status)
	}
}

type cappedBuffer struct {
	buffer bytes.Buffer
	limit  int
	cut    bool
}

func (b *cappedBuffer) Write(content []byte) (int, error) {
	original := len(content)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		if len(content) > remaining {
			content = content[:remaining]
			b.cut = true
		}
		_, _ = b.buffer.Write(content)
	} else {
		b.cut = true
	}
	return original, nil
}

func (b *cappedBuffer) String() string {
	text := b.buffer.String()
	if b.cut {
		text += "\n[output truncated]"
	}
	return text
}
