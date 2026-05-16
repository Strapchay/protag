---
name: orchestrator-cli
description: Aion-Kernel coordination tools for multi-agent orchestration. Provides file locking, DAG state management, stub contracts, and heartbeat monitoring.
---

# orchestrator-cli Skill

This skill provides the `orchestrator-cli` tool for coordinating with the Aion-Kernel Orchestrator. Every Domain Agent MUST use these tools to interact with the kernel.

## Environment

| Variable | Description | Default |
|----------|-------------|---------|
| `AION_ORCHESTRATOR_CORE_ADDR` | Resolved core orchestrator TCP address | `env-resolved` |
| `AION_ORCHESTRATOR_ADDR` | Explicit override address | Required only when bypassing the core address |
| `AION_AGENT_ID` | Your agent UUID (set by supervisor) | Required |

## Commands

### `orchestrator-cli acquire-lock --file <path> --agent-id <uuid>`

Acquire an exclusive write lock on a file. **You MUST acquire a lock before writing any file.**

- Returns `{"status": "acquired"}` on success
- Returns error if file locked by another agent or is a shared file
- Shared files (go.mod, main.go, etc.) cannot be locked — coordinate via stub contracts

### `orchestrator-cli release-lock --file <path> --agent-id <uuid>`

Release a previously acquired lock. **Always release locks after writing.**

### `orchestrator-cli update-node --node-id <uuid> --status <status>`

Update your task node status. Status values: `Pending`, `InProgress`, `Done`, `Failed`.

- Call with `InProgress` when you begin a task
- Call with `Done` when the task is complete
- Call with `Failed` if the task cannot be completed

### `orchestrator-cli create-stub --contract '<json>'`

Create a stub contract when you need a resource from another domain that doesn't exist yet.

Contract JSON format:
```json
{
  "id": "stub-unique-id",
  "producer_id": "agent-uuid-that-should-create-it",
  "consumer_id": "your-agent-uuid",
  "contract": {
    "name": "FunctionName",
    "kind": "function",
    "inputs": ["string", "int"],
    "outputs": ["error"],
    "file_path": "internal/pkg/file.go"
  }
}
```

### `orchestrator-cli inject-edge --from <node-id> --to <node-id>`

Report a newly discovered dependency between tasks. The Orchestrator will validate this doesn't create a cycle.

### `orchestrator-cli split-node --node-id <uuid> --into '<json>'`

Split a task into sub-tasks when you discover it's too large.

### `orchestrator-cli read-dag [--node-id <uuid>]`

Read current DAG state. Without `--node-id`, returns the full DAG snapshot. With `--node-id`, returns a single node.

### `orchestrator-cli heartbeat --agent-id <uuid>`

Send a heartbeat to the Orchestrator. **Call every 15-30 seconds during long operations** to avoid being marked unresponsive.

## Execution Loop

Every Domain Agent follows this loop for each assigned task:

### 1. ORIENT
- Read your current task via `read-dag --node-id <your-task-id>`
- Understand the task_spec, target_files, and dependencies
- Check if any dependencies are still pending

### 2. DECOMPOSE
- If the task is too large, use `split-node` to break it down
- If you discover a dependency on another domain, use `inject-edge`
- If you need a resource from another domain, use `create-stub`

### 3. ACT
- `acquire-lock` on each file before writing
- Write the code
- `release-lock` after writing
- Call `heartbeat` periodically during long operations

### 4. VALIDATE
- Run tests or verification for the code you wrote
- If a stub you depend on has been fulfilled (you'll receive a `follow_up` notification), validate the fulfillment matches your expectations

### 5. CHECKPOINT
- `update-node --status Done` when the task is complete
- `update-node --status Failed` if it cannot be completed (with explanation)

## Handling Context Messages

The Orchestrator may send you `follow_up` messages containing context from other agents:

- **StubFulfilled**: A dependency you were waiting for is now available. Validate it and continue.
- **CorrectionRequest**: Your stub implementation doesn't match the contract. Fix it.
- **ContextShare**: Additional context from a peer agent. Incorporate into your understanding.

## Coordination Rules

1. **Never write without a lock.** Always `acquire-lock` → write → `release-lock`.
2. **Never lock shared files.** Files like `go.mod`, `main.go`, `package.json` are managed by the Utility Agent.
3. **Create stubs early.** If you need something from another domain, create a stub contract immediately.
4. **Heartbeat regularly.** Call `heartbeat` every 15-30 seconds during long operations.
5. **Report new dependencies.** If you discover that your task depends on another task, call `inject-edge`.
6. **Mark tasks promptly.** Call `update-node` as soon as you start (`InProgress`) or finish (`Done`/`Failed`).
