package isolation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BubblewrapConfig struct {
	Binary      string
	RuntimeBase string
}

type BubblewrapEngine struct {
	binary      string
	runtimeBase string
	probeOnce   sync.Once
	probeErr    error
}

type preparedMount struct {
	mount    Mount
	readOnly bool
	file     *os.File
}

type bubblewrapWorkspace struct {
	mu            sync.Mutex
	binary        string
	policy        Policy
	snapshot      Snapshot
	workspaceRoot string
	mounts        []preparedMount
	closed        bool
}

func NewBubblewrapEngine(config BubblewrapConfig) (*BubblewrapEngine, error) {
	binary := strings.TrimSpace(config.Binary)
	if binary == "" {
		binary = "bwrap"
	}
	resolved, err := exec.LookPath(binary)
	if err != nil {
		return nil, fmt.Errorf("isolation: locate bubblewrap: %w", err)
	}
	runtimeBase := strings.TrimSpace(config.RuntimeBase)
	if runtimeBase == "" {
		runtimeBase = filepath.Join(os.TempDir(), "aion-isolation")
	}
	runtimeBase, err = filepath.Abs(runtimeBase)
	if err != nil {
		return nil, fmt.Errorf("isolation: resolve runtime base: %w", err)
	}
	return &BubblewrapEngine{binary: resolved, runtimeBase: runtimeBase}, nil
}

func (e *BubblewrapEngine) Prepare(ctx context.Context, policy Policy) (Workspace, error) {
	e.probeOnce.Do(func() {
		e.probeErr = e.probe(ctx)
	})
	if e.probeErr != nil {
		return nil, e.probeErr
	}
	policy, err := normalizePolicy(policy)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(e.runtimeBase, 0o700); err != nil {
		return nil, fmt.Errorf("isolation: create runtime base: %w", err)
	}
	runtimeParent := filepath.Join(e.runtimeBase, policy.ID)
	if err := os.MkdirAll(runtimeParent, 0o700); err != nil {
		return nil, fmt.Errorf("isolation: create workspace parent: %w", err)
	}
	runtimeDir, err := os.MkdirTemp(runtimeParent, fmt.Sprintf("generation-%d-", policy.Generation))
	if err != nil {
		return nil, fmt.Errorf("isolation: create workspace runtime directory: %w", err)
	}
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		_ = os.RemoveAll(runtimeDir)
		return nil, fmt.Errorf("isolation: restrict workspace runtime directory: %w", err)
	}

	workspace := &bubblewrapWorkspace{
		binary:        e.binary,
		policy:        policy,
		workspaceRoot: filepath.Join(runtimeDir, "workspace-root"),
		snapshot: Snapshot{
			ID:          policy.ID,
			Generation:  policy.Generation,
			Backend:     "bubblewrap",
			VirtualRoot: policy.WorkingDir,
			RuntimeDir:  runtimeDir,
			Network:     policy.Network,
			PreparedAt:  time.Now().UTC(),
		},
	}
	if err := workspace.prepareMounts(); err != nil {
		_ = workspace.Close()
		return nil, err
	}
	if err := workspace.writeManifest(); err != nil {
		_ = workspace.Close()
		return nil, err
	}
	return workspace, nil
}

func (e *BubblewrapEngine) probe(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	args := []string{
		"--die-with-parent",
		"--unshare-user",
		"--unshare-pid",
		"--unshare-ipc",
		"--unshare-uts",
		"--ro-bind", "/usr", "/usr",
		"--symlink", "usr/bin", "/bin",
		"--symlink", "usr/lib", "/lib",
	}
	if _, statErr := os.Stat("/usr/lib64"); statErr == nil {
		args = append(args, "--symlink", "usr/lib64", "/lib64")
	}
	args = append(args,
		"--proc", "/proc",
		"--dev", "/dev",
		"--tmpfs", "/tmp",
		"--", "/bin/true",
	)
	if output, err := exec.CommandContext(probeCtx, e.binary, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("isolation: bubblewrap is unavailable in strict mode: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (w *bubblewrapWorkspace) Snapshot() Snapshot {
	return w.snapshot
}

func (w *bubblewrapWorkspace) prepareMounts() error {
	for _, mount := range w.policy.Writable {
		prepared, err := prepareMount(mount, false)
		if err != nil {
			return err
		}
		w.mounts = append(w.mounts, prepared)
	}
	for _, mount := range w.policy.ReadOnly {
		prepared, err := prepareMount(mount, true)
		if err != nil {
			return err
		}
		w.mounts = append(w.mounts, prepared)
	}
	return w.prepareWorkspaceRoot()
}

func (w *bubblewrapWorkspace) prepareWorkspaceRoot() error {
	if err := os.MkdirAll(w.workspaceRoot, 0o700); err != nil {
		return fmt.Errorf("isolation: create workspace root: %w", err)
	}
	for _, mount := range w.mounts {
		if !pathWithin(mount.mount.Target, "/workspace") || mount.mount.Target == "/workspace" {
			continue
		}
		relative := strings.TrimPrefix(mount.mount.Target, "/workspace/")
		target := filepath.Join(w.workspaceRoot, filepath.FromSlash(relative))
		if !pathWithin(target, w.workspaceRoot) || target == w.workspaceRoot {
			return fmt.Errorf("isolation: workspace target %q escapes the runtime root", mount.mount.Target)
		}
		info, err := mount.file.Stat()
		if err != nil {
			return fmt.Errorf("isolation: stat workspace mount %q: %w", mount.mount.Source, err)
		}
		if info.IsDir() {
			if err := os.MkdirAll(target, 0o700); err != nil {
				return fmt.Errorf("isolation: create workspace mount point %q: %w", mount.mount.Target, err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return fmt.Errorf("isolation: create workspace mount parent %q: %w", mount.mount.Target, err)
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil && !os.IsExist(err) {
			return fmt.Errorf("isolation: create workspace file mount point %q: %w", mount.mount.Target, err)
		}
		if file != nil {
			if err := file.Close(); err != nil {
				return fmt.Errorf("isolation: close workspace file mount point %q: %w", mount.mount.Target, err)
			}
		}
	}
	return nil
}

func prepareMount(mount Mount, readOnly bool) (preparedMount, error) {
	if err := ensureMountSource(mount); err != nil {
		return preparedMount{}, err
	}
	file, err := os.Open(mount.Source)
	if err != nil {
		return preparedMount{}, fmt.Errorf("isolation: open mount source %q: %w", mount.Source, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return preparedMount{}, fmt.Errorf("isolation: stat mount source %q: %w", mount.Source, err)
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		file.Close()
		return preparedMount{}, fmt.Errorf("isolation: mount source %q is not a regular file or directory", mount.Source)
	}
	if mount.Kind == MountDirectory && !info.IsDir() {
		file.Close()
		return preparedMount{}, fmt.Errorf("isolation: mount source %q is not a directory", mount.Source)
	}
	if mount.Kind == MountFile && !info.Mode().IsRegular() {
		file.Close()
		return preparedMount{}, fmt.Errorf("isolation: mount source %q is not a regular file", mount.Source)
	}
	return preparedMount{mount: mount, readOnly: readOnly, file: file}, nil
}

func ensureMountSource(mount Mount) error {
	if err := rejectSymlinkComponents(mount.Source); err != nil {
		if !errors.Is(err, os.ErrNotExist) || !mount.Create {
			return err
		}
		parent := filepath.Dir(mount.Source)
		if err := makeDirectoriesWithoutSymlinks(parent); err != nil {
			return err
		}
		switch mount.Kind {
		case MountDirectory:
			if err := os.Mkdir(mount.Source, 0o755); err != nil && !os.IsExist(err) {
				return fmt.Errorf("isolation: materialize directory %q: %w", mount.Source, err)
			}
		case MountFile:
			file, createErr := os.OpenFile(mount.Source, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if createErr != nil && !os.IsExist(createErr) {
				return fmt.Errorf("isolation: materialize file %q: %w", mount.Source, createErr)
			}
			if file != nil {
				file.Close()
			}
		default:
			return fmt.Errorf("isolation: cannot materialize %q with kind %q", mount.Source, mount.Kind)
		}
	}
	return rejectSymlinkComponents(mount.Source)
}

func makeDirectoriesWithoutSymlinks(path string) error {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return fmt.Errorf("isolation: path %q must be absolute", path)
	}
	current := string(os.PathSeparator)
	for _, component := range strings.Split(strings.TrimPrefix(path, string(os.PathSeparator)), string(os.PathSeparator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		switch {
		case err == nil && info.Mode()&os.ModeSymlink != 0:
			return fmt.Errorf("isolation: symlink component rejected at %q", current)
		case err == nil && !info.IsDir():
			return fmt.Errorf("isolation: path component %q is not a directory", current)
		case err == nil:
			continue
		case !errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("isolation: inspect path %q: %w", current, err)
		}
		if err := os.Mkdir(current, 0o755); err != nil && !os.IsExist(err) {
			return fmt.Errorf("isolation: materialize directory %q: %w", current, err)
		}
	}
	return nil
}

func rejectSymlinkComponents(path string) error {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return fmt.Errorf("isolation: path %q must be absolute", path)
	}
	current := string(os.PathSeparator)
	for _, component := range strings.Split(strings.TrimPrefix(path, string(os.PathSeparator)), string(os.PathSeparator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("isolation: inspect path %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("isolation: symlink component rejected at %q", current)
		}
	}
	return nil
}

func (w *bubblewrapWorkspace) writeManifest() error {
	environmentKeys := make([]string, 0, len(w.policy.Environment)+len(w.policy.InheritEnvironment))
	for key := range w.policy.Environment {
		environmentKeys = append(environmentKeys, key)
	}
	environmentKeys = append(environmentKeys, w.policy.InheritEnvironment...)
	sort.Strings(environmentKeys)
	manifest := struct {
		Snapshot        Snapshot `json:"snapshot"`
		WorkingDir      string   `json:"working_dir"`
		Hostname        string   `json:"hostname"`
		SourceRoot      string   `json:"source_root,omitempty"`
		DeniedSources   []string `json:"denied_sources,omitempty"`
		Writable        []Mount  `json:"writable,omitempty"`
		ReadOnly        []Mount  `json:"read_only,omitempty"`
		EnvironmentKeys []string `json:"environment_keys,omitempty"`
	}{
		Snapshot:        w.snapshot,
		WorkingDir:      w.policy.WorkingDir,
		Hostname:        w.policy.Hostname,
		SourceRoot:      w.policy.SourceRoot,
		DeniedSources:   w.policy.DeniedSources,
		Writable:        w.policy.Writable,
		ReadOnly:        w.policy.ReadOnly,
		EnvironmentKeys: environmentKeys,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("isolation: marshal workspace manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(w.snapshot.RuntimeDir, "workspace.json"), data, 0o600); err != nil {
		return fmt.Errorf("isolation: write workspace manifest: %w", err)
	}
	return nil
}

func (w *bubblewrapWorkspace) Command(ctx context.Context, spec CommandSpec) (*exec.Cmd, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, fmt.Errorf("isolation: workspace %s is closed", w.policy.ID)
	}
	path, err := cleanSandboxPath(spec.Path)
	if err != nil {
		return nil, fmt.Errorf("isolation: command path: %w", err)
	}

	args := []string{
		"--die-with-parent",
		"--unshare-user",
		"--unshare-pid",
		"--unshare-ipc",
		"--unshare-uts",
		"--unshare-cgroup-try",
		"--hostname", w.policy.Hostname,
		"--ro-bind", "/usr", "/usr",
		"--symlink", "usr/bin", "/bin",
		"--symlink", "usr/sbin", "/sbin",
		"--symlink", "usr/lib", "/lib",
	}
	if _, statErr := os.Stat("/usr/lib64"); statErr == nil {
		args = append(args, "--symlink", "usr/lib64", "/lib64")
	}
	args = append(args,
		"--ro-bind", "/etc", "/etc",
		"--proc", "/proc",
		"--dev", "/dev",
		"--tmpfs", "/tmp",
		"--ro-bind", w.workspaceRoot, "/workspace",
		"--dir", "/state",
		"--dir", "/context",
		"--dir", "/contracts",
		"--dir", "/integration",
		"--dir", "/runtime",
		"--dir", "/home",
		"--dir", "/home/agent",
		"--dir", "/run",
	)
	if w.policy.Network == NetworkIsolated {
		args = append(args, "--unshare-net")
	}

	for _, parent := range mountParents(w.mounts) {
		if pathWithin(parent, "/workspace") {
			continue
		}
		args = append(args, "--dir", parent)
	}
	extraFiles := make([]*os.File, 0, len(w.mounts))
	for _, mount := range w.mounts {
		fd := 3 + len(extraFiles)
		option := "--bind-fd"
		if mount.readOnly {
			option = "--ro-bind-fd"
		}
		args = append(args, option, strconv.Itoa(fd), mount.mount.Target)
		extraFiles = append(extraFiles, mount.file)
	}

	environment := map[string]string{
		"HOME":   "/home/agent",
		"LANG":   "C.UTF-8",
		"PATH":   "/usr/local/bin:/usr/bin:/bin",
		"TMPDIR": "/tmp",
	}
	for _, key := range w.policy.InheritEnvironment {
		value, exists := os.LookupEnv(key)
		if !exists {
			return nil, fmt.Errorf("isolation: inherited environment %q is not set", key)
		}
		environment[key] = value
	}
	for key, value := range w.policy.Environment {
		environment[key] = value
	}
	for key, value := range spec.Env {
		if !envNamePattern.MatchString(key) {
			return nil, fmt.Errorf("isolation: invalid command environment name %q", key)
		}
		environment[key] = value
	}
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args = append(args, "--chdir", w.policy.WorkingDir, "--", path)
	args = append(args, spec.Args...)

	cmd := exec.CommandContext(ctx, w.binary, args...)
	cmd.ExtraFiles = extraFiles
	cmd.Env = make([]string, 0, len(keys))
	for _, key := range keys {
		cmd.Env = append(cmd.Env, key+"="+environment[key])
	}
	return cmd, nil
}

func mountParents(mounts []preparedMount) []string {
	set := make(map[string]struct{})
	for _, mount := range mounts {
		parent := filepath.Dir(mount.mount.Target)
		for parent != "/" && !isReservedTarget(parent) {
			set[filepath.ToSlash(parent)] = struct{}{}
			next := filepath.Dir(parent)
			if next == parent {
				break
			}
			parent = next
		}
	}
	parents := make([]string, 0, len(set))
	for parent := range set {
		parents = append(parents, parent)
	}
	sort.Slice(parents, func(i, j int) bool {
		depthI := strings.Count(parents[i], "/")
		depthJ := strings.Count(parents[j], "/")
		if depthI == depthJ {
			return parents[i] < parents[j]
		}
		return depthI < depthJ
	})
	return parents
}

func (w *bubblewrapWorkspace) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	mounts := w.mounts
	w.mounts = nil
	runtimeDir := w.snapshot.RuntimeDir
	w.mu.Unlock()

	var closeErr error
	for _, mount := range mounts {
		if err := mount.file.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if err := os.RemoveAll(runtimeDir); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	return closeErr
}
