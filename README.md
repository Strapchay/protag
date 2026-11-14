# Aion Isolation

`aion-isolation` is a composable Go library for launching an existing command
inside a strict, rootless Linux filesystem and process boundary. It does not
run a daemon, supervise agents, or maintain a global workspace registry.

The composing executor owns process lifecycle and mutable execution state. The
library owns policy validation, FD-backed mounts, Bubblewrap command
construction, and cleanup of one prepared workspace generation.

## Properties

- Bubblewrap and unprivileged user namespaces are required; preparation fails
  closed when they are unavailable.
- Writable mounts are limited to `/workspace` and `/state`.
- `/workspace` is a read-only project skeleton; only explicitly assigned
  descendants can be modified.
- Other host paths are absent unless explicitly mounted read-only.
- Host symlinks in policy sources are rejected.
- Mount sources are held by file descriptor between validation and launch.
- The child environment is cleared and reconstructed from explicit policy.
- Shared host networking is the version-one default; isolated networking is
  available for commands that do not require host services.
- No direct or passthrough production backend exists.

## Composition

The executor creates one engine and prepares one `Workspace` for each process
generation:

```go
engine, err := isolation.NewBubblewrapEngine(isolation.BubblewrapConfig{
    RuntimeBase: "/tmp/aion-isolation",
})
workspace, err := engine.Prepare(ctx, policy)
cmd, err := workspace.Command(ctx, isolation.CommandSpec{
    Path: "/runtime/bin/pi",
    Args: []string{"--mode", "rpc"},
})
```

`Command` constructs but does not start or supervise the process. The composing
executor remains responsible for process groups, health checks, retries, and
persisting a monotonic generation counter. It must call `Workspace.Close`
after every failed launch, crash, normal stop, and before preparing a
replacement generation.

`Snapshot` exposes immutable runtime metadata for observability without moving
process ownership into this library. Mutable registries of agents and active
workspaces belong to the composing executor.

## Boundary

Only explicit policy mounts exist inside the sandbox. A writable project mount
must be a strict descendant of `SourceRoot`; the project root itself cannot be
assigned. `DeniedSources` reject ownership that overlaps runtime or repository
metadata. Read-only mounts are intended for explicitly approved tools,
contracts, and integration artifacts.

Commands retain `/workspace` as the stable project-relative working directory.
The library binds a private read-only skeleton there and overlays only the
policy's assigned descendants. This lets a command address an owned path such
as `ledger/domain.py` without gaining a writable catch-all workspace root.

Shared networking deliberately retains the host network namespace so a child
can reach a local gateway. Select isolated networking when no host service is
required. Neither mode changes the filesystem, PID, IPC, or UTS boundary.

## Harness

```bash
go run ./cmd/isolation-harness --policy policy.json -- /bin/sh -c 'pwd; ls -la'
```

The harness is intended for standalone adversarial and model tests before the
library is composed into the kernel supervisor.

Run the library verification suite with:

```bash
go test -race -count=1 ./...
```
