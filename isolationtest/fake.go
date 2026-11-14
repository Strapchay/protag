package isolationtest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	isolation "aion-isolation"
)

// FakeEngine is a deterministic test double. It does not provide isolation and
// must never be selected as a production fallback.
type FakeEngine struct {
	mu         sync.Mutex
	Policies   []isolation.Policy
	workspaces map[uint64]*fakeWorkspace
}

func (e *FakeEngine) Prepare(_ context.Context, policy isolation.Policy) (isolation.Workspace, error) {
	workspace := &fakeWorkspace{policy: policy, snapshot: isolation.Snapshot{
		ID:          policy.ID,
		Generation:  policy.Generation,
		Backend:     "fake-test-only",
		VirtualRoot: policy.WorkingDir,
		Network:     policy.Network,
		PreparedAt:  time.Now().UTC(),
	}}
	e.mu.Lock()
	e.Policies = append(e.Policies, policy)
	if e.workspaces == nil {
		e.workspaces = make(map[uint64]*fakeWorkspace)
	}
	e.workspaces[policy.Generation] = workspace
	e.mu.Unlock()
	return workspace, nil
}

// Closed reports whether a prepared generation has been closed. It is intended
// for executor lifecycle tests, not production observability.
func (e *FakeEngine) Closed(generation uint64) bool {
	e.mu.Lock()
	workspace := e.workspaces[generation]
	e.mu.Unlock()
	if workspace == nil {
		return false
	}
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return workspace.closed
}

type fakeWorkspace struct {
	mu       sync.Mutex
	policy   isolation.Policy
	snapshot isolation.Snapshot
	closed   bool
}

func (w *fakeWorkspace) Snapshot() isolation.Snapshot { return w.snapshot }

func (w *fakeWorkspace) Command(ctx context.Context, spec isolation.CommandSpec) (*exec.Cmd, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, fmt.Errorf("fake isolation workspace is closed")
	}
	path := w.hostPath(spec.Path)
	args := make([]string, len(spec.Args))
	for i, arg := range spec.Args {
		args[i] = w.hostPath(arg)
	}
	cmd := exec.CommandContext(ctx, path, args...)
	if workingDir := w.hostPath(w.policy.WorkingDir); filepath.IsAbs(workingDir) {
		if info, err := os.Stat(workingDir); err == nil && info.IsDir() {
			cmd.Dir = workingDir
		}
	}
	environment := append([]string(nil), os.Environ()...)
	for key, value := range w.policy.Environment {
		environment = append(environment, key+"="+w.hostPath(value))
	}
	for _, key := range w.policy.InheritEnvironment {
		if value, exists := os.LookupEnv(key); exists {
			environment = append(environment, key+"="+value)
		}
	}
	for key, value := range spec.Env {
		environment = append(environment, key+"="+w.hostPath(value))
	}
	cmd.Env = environment
	return cmd, nil
}

func (w *fakeWorkspace) hostPath(value string) string {
	type mapping struct{ target, source string }
	mappings := make([]mapping, 0, len(w.policy.Writable)+len(w.policy.ReadOnly))
	for _, mount := range append(append([]isolation.Mount(nil), w.policy.Writable...), w.policy.ReadOnly...) {
		mappings = append(mappings, mapping{target: filepath.Clean(mount.Target), source: filepath.Clean(mount.Source)})
	}
	sort.Slice(mappings, func(i, j int) bool { return len(mappings[i].target) > len(mappings[j].target) })
	cleaned := filepath.Clean(value)
	for _, mount := range mappings {
		if cleaned == mount.target {
			return mount.source
		}
		prefix := mount.target + string(os.PathSeparator)
		if strings.HasPrefix(cleaned, prefix) {
			return filepath.Join(mount.source, strings.TrimPrefix(cleaned, prefix))
		}
	}
	return value
}

func (w *fakeWorkspace) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}
