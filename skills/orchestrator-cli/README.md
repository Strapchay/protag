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
| `create-stub` | `--contract` | Create stub contract |
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
