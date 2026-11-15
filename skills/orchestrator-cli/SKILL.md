---
name: orchestrator-cli
description: Aion-Kernel coordination tools for multi-agent orchestration. Provides DAG state management, durable artifact contracts, and heartbeat monitoring.
---

# orchestrator-cli Skill

This skill provides the `orchestrator-cli` tool for coordinating with the Aion-Kernel Orchestrator. Every Domain Agent MUST use these tools to interact with the kernel.

## Environment

| Variable | Description | Default |
|----------|-------------|---------|
| `AION_ORCHESTRATOR_CORE_ADDR` | Resolved core orchestrator TCP address | `env-resolved` |
| `AION_ORCHESTRATOR_ADDR` | Explicit override address | Required only when bypassing the core address |
| `AION_AGENT_ID` | Your agent UUID (set by supervisor) | Required |
| `AION_AGENT_CAPABILITY` | Opaque authorization credential (set and consumed automatically) | Required for mutations |

## Commands

### `orchestrator-cli work-inbox [--current]`

Read the kernel-owned projection of your active, ready, waiting, and recently completed work. `--current` returns the active item when one exists, otherwise the highest-ranked dispatched contract item. A pending DAG node remains waiting until the allocator assigns it and changes it to `active`; never claim or execute an unassigned node. Run this command at the start of every dispatched turn and process only the returned item.

For a `dag_node` item, `id` and `node_id` are the same raw DAG node ID; pass
that value to both `update-node --node-id` and `work-checkpoint --item`. For a
contract item, pass `contract_id` to lifecycle commands and `id` to
`work-checkpoint`.

Inbox order is correction, validation, publication, then ready DAG work. An active DAG node is never preempted. The projection is rebuilt from DAG and coordination WAL state, so it remains authoritative across restarts.

### `orchestrator-cli work-checkpoint --item <work-item-id> --summary <text> [--next <text>]`

Persist a concise recovery summary before yielding incomplete work. Checkpoints do not replace or duplicate DAG and contract state; the referenced item must be visible in your inbox.

### `orchestrator-cli update-node --node-id <uuid> --status <status> [--agent-id <uuid>]`

Update your task node status. Status values: `Pending`, `InProgress`, `Done`, `Failed`.

The CLI uses the supervisor-issued capability automatically. The orchestrator derives your identity from it and rejects updates when the node is not assigned to you. Never print or persist the capability.

- Call with `InProgress` when you begin a task
- Call with `Done` when the task is complete
- Call with `Failed` if the task cannot be completed
- `Done` is deferred while an outbound artifact contract from that node is non-terminal

### `orchestrator-cli contract-ensure --producer-node <id> --consumer-node <id> --resource <name> [--details '<json>'|--details-file <path>] [--wait]`

Create or retrieve the stable contract for a discovered cross-domain dependency. The kernel derives participant identities from DAG ownership, rejects requests from agents that do not own the consumer node, and returns the producer work item's queue state. Use a stable resource name and retry the same command safely after transport failure. The kernel generates the durable `contract_id` from the resource and participant node IDs. Never hardcode, derive, or guess a kernel contract ID; use the `contract_id` returned by this command for every lifecycle operation.

Use `--wait` only when the consumer cannot continue without the artifact. After ensuring a hard-wait contract, checkpoint the current item and end the turn. Do not poll or fabricate the dependency; the kernel wakes the correct agent when publication, validation, or correction becomes ready.

### `orchestrator-cli contract-request --contract '<json>'` (advanced)

Request a fully specified versioned artifact with an explicit contract ID. Prefer `contract-ensure` during ordinary domain work. This lower-level command is retained for callers that already own stable contract identities. Producer and consumer node IDs are mandatory so the kernel can authorize both parties, detect hard-wait cycles, and gate node completion.

```json
{
  "id": "contract-unique-id",
  "producer_agent_id": "agent-provider",
  "producer_node_id": "provider-node",
  "consumer_agent_id": "agent-consumer",
  "consumer_node_id": "consumer-node",
  "details": {
    "name": "FunctionName",
    "kind": "function",
    "language": "go",
    "public_entrypoint": "package.FunctionName",
    "signature": {"inputs": ["string", "int"], "outputs": ["error"]},
    "acceptance": ["returns a typed error for invalid input"]
  }
}
```

### Artifact contract lifecycle

```bash
# Producer publishes a concrete artifact. Revision 0 means none exists yet.
orchestrator-cli contract-publish --contract-id <id> --artifact-file <path> --expected-revision 0

# Consumer materializes an immutable public descriptor at a regular file path.
# The artifact describes the public surface; producer source stays isolated and
# behavioral acceptance runs later in the assembled integration workspace.
orchestrator-cli contract-fetch --contract-id <id> --revision 1 --output /tmp/aion-contract-<id>-r1.json

# Consumer accepts it or requests a corrected revision.
orchestrator-cli contract-validate --contract-id <id> --revision 1 --evidence '{"tests":"passed"}'
orchestrator-cli contract-correct --contract-id <id> --revision 1 --reason <text>

# Either participant can propose a bounded tradeoff before publication.
orchestrator-cli contract-propose-terms --contract-id <id> --revision 1 \
  --terms '{"details":{"name":"FunctionName","kind":"function"},"producer_obligations":["typed errors"],"consumer_obligations":["bounded retry"],"tradeoffs":["one retry"]}' \
  --rationale "prevent retry amplification"
orchestrator-cli contract-accept-terms --contract-id <id> --revision 2
orchestrator-cli contract-terms --contract-id <id>

# Escalate only after the configured round limit, or use --terminal for a
# concrete incompatibility that cannot be negotiated safely.
orchestrator-cli contract-standstill --contract-id <id> --revision 3 \
  --reason "incompatible guarantees" --evidence '["failing consumer contract test"]'

# Declare or clear a hard execution wait. Hard waits participate in cycle detection.
orchestrator-cli contract-wait --contract-id <id>
orchestrator-cli contract-wait --contract-id <id> --clear

# Inspect or terminate contracts.
orchestrator-cli contract-status --contract-id <id>
orchestrator-cli contract-list
orchestrator-cli contract-fail --contract-id <id> --reason <text>
orchestrator-cli contract-cancel --contract-id <id> --reason <text>
```

Every mutation is capability-authorized and idempotent. Use a stable `--command-id` when retrying after a transport failure. Publishing requires the revision you observed in `--expected-revision`; stale writers are rejected.
Publication is also rejected while the latest terms revision is not accepted
by both participants. A standstill pauses only the affected nodes while the
kernel runs bounded reconciliation; inspect delivered term events and continue
unrelated work. Agents never own the reconciliation lease or modify ownership.

### `orchestrator-cli send-message --agent-id <agent-id> --text <text>`

Send a Context Hub message to a peer agent, for example `agent-ledger` or
`agent-report`. Use it for consumption plans, compatibility notices, and
acknowledgements. The message is durable in the peer's context stream; it is
queued while the peer is executing a tool and delivered after that tool result.
Do not include credentials or secrets.

### Other orchestration commands

- `inject-edge --from <node-id> --to <node-id>` reports a newly discovered dependency.
- `split-node --node-id <uuid> --into '<json>'` splits a task that is too large.
- `read-dag [--node-id <uuid>]` reads the current DAG or one node.
- `heartbeat --agent-id <uuid>` reports liveness during long local operations.

### Integration directives

Create a schema-v1 integration directive only when its exact
`<assigned-path>/integration/directives/*.json` path appears in the current
node's `target_files`. Cross-domain directives belong to the consumer node.
Producers publish public contract descriptors and focused unit tests instead of
inventing an unplanned directive.

Use this exact schema. Unknown fields are rejected:

```json
{
  "schema_version": 1,
  "id": "stable-id",
  "command": ["/usr/bin/env", "python3", "-m", "unittest", "discover"],
  "required_contracts": ["<contract_id returned by contract-ensure>"],
  "owner_node_ids": ["<your-node-id>"],
  "timeout_sec": 120
}
```

- `command[0]` must be an absolute `/usr` or `/bin` executable.
- `required_contracts` contains kernel `contract_id` values, not resource names.
- Omit `working_directory` for a cross-domain directive.
- The timeout field is `timeout_sec` (not `timeout_seconds`).
- The kernel runs directives only after DAG and coordination work are terminal,
  in a network-isolated assembled `/workspace` where all domain outputs and
  contracts are read-only.
- Validate the JSON and your owned unit tests, then complete the node; do not
  search for peer source or execute a cross-domain directive from the Domain
  Agent workspace. Do not run `integration-run` from a Domain Agent; retry
  authority is an operator action.

## Execution Loop

Every Domain Agent follows this loop for each assigned task:

### 1. ORIENT
- Run `work-inbox --current` and process only the returned item
- For DAG work, read the node via `read-dag --node-id <your-task-id>`
- Understand the task spec, target files, and dependencies
- Check whether dependencies are pending

### 2. DECOMPOSE
- If the task is too large, use `split-node`
- If you discover a task dependency, use `inject-edge`
- If you need a cross-domain resource, use `contract-ensure`; add `--wait` only for a hard dependency

### 3. ACT
- Write only the current node's target files beneath your isolated writable domain
- Call `heartbeat` periodically during long local operations

### 4. VALIDATE
- Run tests or verification for the code you wrote
- Keep focused tests under an owned domain path such as `/workspace/<domain>/tests/`
- Fetch and validate every produced artifact revision before relying on it
- Request correction with a concrete reason when acceptance criteria fail

### 5. CHECKPOINT
- Use `work-checkpoint` before yielding incomplete work
- `update-node --status Done` when the task is complete
- `update-node --status Failed` if it cannot be completed

## Handling Context Messages

The Orchestrator delivers durable coordination events as a new prompt while
Pi is idle or queues them behind the active turn:

- **ContractRequested**: A consumer needs an artifact from your domain. Publish a concrete revision.
- **ContractProduced**: A requested revision is available. Fetch and validate it.
- **ContractFulfilled**: The consumer accepted the revision.
- **CorrectionRequested**: The consumer rejected a revision. Address the reason and publish the next revision.
- **ContractTerminal**: A contract failed, expired, or was cancelled. Do not continue waiting on it.
- **ContractTermsProposed**: The other participant proposed revised obligations or tradeoffs. Inspect with `contract-terms`, then accept or counter.
- **ContractTermsAgreed**: Both participants accepted the latest immutable terms; publication may continue.
- **CycleDetected**: Hard waits form a cycle. Stop dependent work until the kernel reports recovery.
- **ContextShare**: Additional context from a peer agent. Incorporate it.

## Sandbox Boundary

Your process is sandboxed to the project workspace and the mounted runtime
skill/extension paths. Kernel source, kernel binaries, host home directories,
and runtime internals are not mounted. Do not use `strings`, binary inspection,
or source inspection to discover kernel schemas; use this skill. If a detail is
missing, state the assumption or request it through the kernel rather than
reading outside the workspace.

## Coordination Rules

1. **Stay inside ownership.** Write only the active node's targets beneath the isolated domain mount.
2. **Coordinate shared boundaries.** Use explicit contracts rather than writing another domain or shared entrypoint.
3. **Request contracts early.** Do not invent another domain's interface or inspect its workspace.
4. **Validate revisions.** A produced artifact is not fulfilled until the consumer accepts it.
5. **Use stable command IDs for retries.** A conflicting reuse is rejected.
6. **Heartbeat during local work.** Gateway queue and active inference states are tracked separately by the kernel.
7. **Mark tasks promptly.** Call `update-node` as soon as you start or finish.
8. **Escalate bounded standstills.** Counter or accept terms until the configured limit; use `contract-standstill` only with concrete evidence. Never request ownership changes.
9. **Honor the work inbox.** Process only `work-inbox --current`; never reorder work from transcript memory.
10. **Yield hard waits.** Ensure the contract, checkpoint the consumer item, and end the turn rather than polling.

### Peer message status and acknowledgement

`send-message` returns a durable `message_id`. Messages are either `notify`
(transport only) or `request` (`--expects-ack`). A request is not complete until
the recipient explicitly acknowledges it.

Sender commands:

```bash
orchestrator-cli send-message --agent-id agent-ledger --text "..." --expects-ack
orchestrator-cli message-status --id <message-id>
orchestrator-cli message-status --id <message-id> --wait --timeout 2m
orchestrator-cli message-list --from-me --state awaiting_ack
```

`message-status` is always a point query by default. `--wait` accepts a duration the sender chooses (`30s`, `2m`, `10m`) with a
15-minute safety ceiling; it is not
required. Requests also push a `MESSAGE STATUS` ContextShare to the sender when
the actor acknowledges or reaches a terminal state, so the sender can continue
other work and consume the update later. Never block a turn indefinitely waiting
for a peer.

Recipient commands:

```bash
orchestrator-cli message-inbox --state awaiting_ack
orchestrator-cli message-ack --id <message-id> --state acknowledged --note "received"
orchestrator-cli message-update --id <message-id> --state in_progress|completed|failed --note "..."
```

When a delivered ContextShare has `kind=request`, acknowledge it with
`message-ack` before ending the turn or starting the requested work. Use
`message-update` for later progress and completion. If an agent respawns,
recover inbound requests with `message-inbox` and outbound requests with
`message-list --from-me`; do not rely on transcript memory.

Peer messages are durable for the lifetime of the run. There is no wall-clock
TTL; terminal rows are pruned by count, so a paused or hibernated run does not
lose open requests.
