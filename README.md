# protag

`protag` is the monorepo for the Aion multi-agent orchestration stack. It hosts
three tightly coupled Go modules that were previously developed side by side:

| Module | Path | Purpose |
| --- | --- | --- |
| Kernel | `kernel/` | Orchestrator daemon, DAG/locking, supervision, acceptance, integration, and the TUI/CLI. |
| Coordination | `coordination/` | Contracts, negotiation, deadlock escalation, resolutions, and durable effects. |
| Isolation | `isolation/` | Per-agent filesystem/process isolation policies and bubblewrap workspaces. |

Repository history for each module is preserved in this monorepo. `kernel/`
history follows the tree move via rename tracking; `coordination/` and
`isolation/` were imported with `git subtree`, so their original commits remain
ancestors.

## Requirements

- Go 1.25.7+
- Linux with `bubblewrap` (`bwrap`) and unprivileged user namespaces for
  domain-agent execution
- `flatc` and the protobuf plugins for schema generation (`make install-tools`)

## Getting started

```bash
# Build kernel binaries into kernel/bin
make build

# Test every module
make test

# Kernel unit tests only
make test-core
```

The kernel keeps its detailed documentation and build targets in
[`kernel/README.md`](kernel/README.md) and [`kernel/Makefile`](kernel/Makefile).

## Test targets

| Target | What it runs |
| --- | --- |
| `make test` | Unit tests for all three modules. |
| `make test-scenarios` | Deterministic scenario matrix (acceptance, recovery, integration, contracts, deadlock). |
| `make test-scenarios-race` | The deterministic matrix under the race detector. |
| `make test-scenarios-shard SHARD=n SHARDS=m` | One deterministic CI shard. |
| `make test-scenarios-process` | Real server startup, preflight gating, and project-lock rejection. |
| `make test-scenarios-flake RUNS=n` | Repeated runs with per-run JSONL and lifetime flake history. |
| `make test-scenarios-live` | Opt-in real-model live suite (spends tokens). |

## Layout

```text
protag/
  go.work            # workspace for the three modules
  Makefile           # root convenience targets
  kernel/            # orchestrator + CLI + scenarios
  coordination/      # contracts and resolutions
  isolation/         # sandboxing
```

`docs/` is intentionally **not** part of this monorepo. It remains a separate
repository and is ignored at the root.

## Runtime ownership

One kernel owns a project directory at a time. A second kernel started on the
same path exits with a project-lock error naming the owning PID.
