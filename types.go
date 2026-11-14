package isolation

import (
	"context"
	"os/exec"
	"time"
)

type NetworkMode string

const (
	NetworkShared   NetworkMode = "shared"
	NetworkIsolated NetworkMode = "isolated"
)

type MountKind string

const (
	MountAuto      MountKind = "auto"
	MountDirectory MountKind = "directory"
	MountFile      MountKind = "file"
)

// Mount maps one host path into the sandbox. Missing writable paths are only
// materialized when Create is true and Kind is explicit.
type Mount struct {
	Source string    `json:"source"`
	Target string    `json:"target"`
	Kind   MountKind `json:"kind,omitempty"`
	Create bool      `json:"create,omitempty"`
}

// Policy is an immutable description of one isolated execution generation.
type Policy struct {
	ID                 string            `json:"id"`
	Generation         uint64            `json:"generation"`
	WorkingDir         string            `json:"working_dir,omitempty"`
	Hostname           string            `json:"hostname,omitempty"`
	Network            NetworkMode       `json:"network,omitempty"`
	SourceRoot         string            `json:"source_root,omitempty"`
	DeniedSources      []string          `json:"denied_sources,omitempty"`
	Writable           []Mount           `json:"writable,omitempty"`
	ReadOnly           []Mount           `json:"read_only,omitempty"`
	Environment        map[string]string `json:"environment,omitempty"`
	InheritEnvironment []string          `json:"inherit_environment,omitempty"`
}

// NewWorkspaceMount converts a project-relative ownership path into a direct
// writable mount under /workspace. The project root itself is intentionally
// not assignable: an isolated domain must own a narrower subtree or file.
func NewWorkspaceMount(sourceRoot, assignedPath string, kind MountKind) (Mount, error) {
	return newWorkspaceMount(sourceRoot, assignedPath, kind)
}

// CommandSpec describes the command as it should appear inside the sandbox.
type CommandSpec struct {
	Path string
	Args []string
	Env  map[string]string
}

// Snapshot describes prepared isolation resources. Mutable agent and process
// lifecycle state remains the responsibility of the composing executor.
type Snapshot struct {
	ID          string      `json:"id"`
	Generation  uint64      `json:"generation"`
	Backend     string      `json:"backend"`
	VirtualRoot string      `json:"virtual_root"`
	RuntimeDir  string      `json:"runtime_dir"`
	Network     NetworkMode `json:"network"`
	PreparedAt  time.Time   `json:"prepared_at"`
}

type Engine interface {
	Prepare(context.Context, Policy) (Workspace, error)
}

// Workspace is a prepared, non-running sandbox generation. Command constructs
// a process but never starts or monitors it.
type Workspace interface {
	Snapshot() Snapshot
	Command(context.Context, CommandSpec) (*exec.Cmd, error)
	Close() error
}
