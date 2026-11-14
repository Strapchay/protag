package isolation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizePolicyRejectsUnsafeConfiguration(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	tests := []struct {
		name   string
		policy Policy
		want   string
	}{
		{
			name:   "invalid ID",
			policy: Policy{ID: "agent/one"},
			want:   "invalid policy ID",
		},
		{
			name: "relative source",
			policy: Policy{ID: "agent-one", SourceRoot: root, Writable: []Mount{
				{Source: "relative", Target: "/workspace/code"},
			}},
			want: "must be absolute",
		},
		{
			name: "writable context",
			policy: Policy{ID: "agent-one", Writable: []Mount{
				{Source: root, Target: "/context/code"},
			}},
			want: "must be under /workspace or /state",
		},
		{
			name: "reserved target",
			policy: Policy{ID: "agent-one", ReadOnly: []Mount{
				{Source: root, Target: "/etc/override"},
			}},
			want: "reserved",
		},
		{
			name: "overlapping targets",
			policy: Policy{ID: "agent-one", SourceRoot: root, Writable: []Mount{
				{Source: filepath.Join(root, "one"), Target: "/workspace/code", Kind: MountDirectory, Create: true},
				{Source: filepath.Join(root, "two"), Target: "/workspace/code/sub", Kind: MountDirectory, Create: true},
			}},
			want: "overlapping mount target",
		},
		{
			name: "implicit materialization kind",
			policy: Policy{ID: "agent-one", SourceRoot: root, Writable: []Mount{
				{Source: filepath.Join(root, "missing"), Target: "/workspace/code", Create: true},
			}},
			want: "explicit kind",
		},
		{
			name:   "invalid inherited environment",
			policy: Policy{ID: "agent-one", InheritEnvironment: []string{"NOT-VALID"}},
			want:   "invalid inherited environment",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizePolicy(test.policy)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("normalizePolicy() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestNormalizePolicyCopiesMutableInputs(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	policy := Policy{
		ID:         "agent-one",
		SourceRoot: root,
		Writable: []Mount{{
			Source: filepath.Join(root, "code"),
			Target: "/workspace/code",
		}},
		Environment:        map[string]string{"VISIBLE": "original"},
		InheritEnvironment: []string{"ALLOWED"},
	}
	normalized, err := normalizePolicy(policy)
	if err != nil {
		t.Fatal(err)
	}

	policy.Writable[0].Target = "/workspace/changed"
	policy.Environment["VISIBLE"] = "changed"
	policy.InheritEnvironment[0] = "CHANGED"
	if normalized.Writable[0].Target != "/workspace/code" || normalized.Environment["VISIBLE"] != "original" || normalized.InheritEnvironment[0] != "ALLOWED" {
		t.Fatalf("normalized policy retained caller-owned mutable storage: %#v", normalized)
	}
}

func TestNewWorkspaceMountRejectsRootAndEscape(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for _, assigned := range []string{".", "../outside", "/absolute"} {
		if _, err := NewWorkspaceMount(root, assigned, MountDirectory); err == nil {
			t.Fatalf("NewWorkspaceMount(%q) succeeded", assigned)
		}
	}
	mount, err := NewWorkspaceMount(root, "internal/store", MountDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if mount.Source != filepath.Join(root, "internal", "store") || mount.Target != "/workspace/internal/store" || !mount.Create {
		t.Fatalf("unexpected workspace mount: %#v", mount)
	}
}

func TestNormalizePolicyRejectsDeniedWorkspaceSource(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	controlPath := filepath.Join(root, ".control")
	_, err := normalizePolicy(Policy{
		ID:            "agent-one",
		SourceRoot:    root,
		DeniedSources: []string{controlPath},
		Writable: []Mount{{
			Source: controlPath,
			Target: "/workspace/control",
			Kind:   MountDirectory,
			Create: true,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "overlaps denied source") {
		t.Fatalf("normalizePolicy() error = %v, want denied source rejection", err)
	}
}

func TestPrepareRejectsSymlinkMountSource(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	realPath := filepath.Join(root, "real")
	if err := os.Mkdir(realPath, 0o755); err != nil {
		t.Fatal(err)
	}
	symlinkPath := filepath.Join(root, "link")
	if err := os.Symlink(realPath, symlinkPath); err != nil {
		t.Fatal(err)
	}
	_, err := prepareMount(Mount{Source: symlinkPath, Target: "/workspace/code"}, false)
	if err == nil || !strings.Contains(err.Error(), "symlink component rejected") {
		t.Fatalf("prepareMount() error = %v, want symlink rejection", err)
	}
}

func TestPrepareMaterializesExplicitNestedSources(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	directory := filepath.Join(root, "nested", "code")
	prepared, err := prepareMount(Mount{
		Source: directory,
		Target: "/workspace/code",
		Kind:   MountDirectory,
		Create: true,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.file.Close()
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		t.Fatalf("materialized directory stat = %v, %v", info, err)
	}
}
