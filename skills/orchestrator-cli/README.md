# orchestrator-cli

Coordination tool for Aion-Kernel multi-agent orchestration.

## Installation

The binary is compiled from `kernel/cmd/orchestrator-cli/`:

```bash
cd kernel && go build -o skills/orchestrator-cli/bin/orchestrator-cli ./cmd/orchestrator-cli/
```

## Quick Reference

| Command | Required Flags | Description |
|---------|---------------|-------------|
| `acquire-lock` | `--file`, `--agent-id` | Exclusive file lock |
| `release-lock` | `--file`, `--agent-id` | Release file lock |
| `update-node` | `--node-id`, `--status`; agent identity via `--agent-id` or env | Update an assigned task status |
| `contract-request` | `--contract` | Request a versioned cross-domain artifact |
| `contract-publish` | `--contract-id`, `--artifact-file` | Publish the next immutable artifact revision |
| `contract-fetch` | `--contract-id`, `--output <file-path>` | Materialize a public artifact descriptor locally |
| `contract-validate` | `--contract-id`, `--revision` | Accept a produced revision |
| `contract-correct` | `--contract-id`, `--revision`, `--reason` | Reject a revision and request correction |
| `contract-propose-terms` | `--contract-id`, `--revision`, `--terms`, `--rationale` | Propose immutable bilateral terms |
| `contract-accept-terms` | `--contract-id`, `--revision` | Accept the latest terms revision |
| `contract-terms` | `--contract-id` | Read terms proposal history |
| `contract-standstill` | `--contract-id`, `--revision`, `--reason` | Request bounded Coordinator reconciliation |
| `contract-wait` | `--contract-id`; optional `--clear` | Declare or clear a hard wait |
| `contract-status` | `--contract-id` | Read one durable contract |
| `contract-list` | none | List visible durable contracts |
| `contract-fail` / `contract-cancel` | `--contract-id` | Terminate a contract explicitly |
| `inject-edge` | `--from`, `--to` | Report dependency |
| `split-node` | `--node-id`, `--into` | Split task |
| `read-dag` | (optional: `--node-id`) | Read DAG state |
| `heartbeat` | `--agent-id` | Health heartbeat |
| `debug-status` | optional `--pretty`, `--key` | Runtime diagnostics |
| `set-gateway-capacity` | `--capacity` | Change runtime inference concurrency |
| `stop-agents` | none | Pause active Domain Agents |

## Environment Variables

- `AION_ORCHESTRATOR_CORE_ADDR` — Core orchestrator address resolved from the runtime env
- `AION_ORCHESTRATOR_ADDR` — Explicit override when you want to bypass the resolved core address
- `AION_AGENT_ID` — Default agent ID for `--agent-id` flags; node updates are rejected unless the node is assigned to this agent
- `AION_AGENT_CAPABILITY` — Opaque runtime credential injected by the supervisor and consumed automatically; do not print, persist, or pass it manually

Contract mutations are authorized from this capability. Cross-domain artifacts are content-addressed and revisioned; use `--command-id` to make retries idempotent and `--expected-revision` when publishing to reject stale writers.

## Output Format

All commands output JSON to stdout. Errors output JSON to stderr with exit code 1.

Success:
```json
{"status": "acquired", "file": "internal/auth/handler.go"}
```

Error:
```json
{"error": "lock: file 'auth.go' already locked by agent-123"}
```

See [SKILL.md](SKILL.md) for full documentation and the execution loop protocol.
