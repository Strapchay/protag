# Aion Coordination

`aion-coordination` is a composable Go library for durable cross-domain
artifact contracts. It does not run a daemon, spawn workers, own agent
processes, mutate a DAG, or call an inference provider. The composing kernel is
the only runtime and owns process lifecycle, Hub routing, DAG projection, and
recovery policy.

## Properties

- SQLite in WAL mode with `FULL` synchronous durability.
- Distinct producer/consumer agent and DAG node identities.
- Lifecycle: `Requested -> Produced -> Fulfilled`, with correction revisions
  and explicit failed, expired, and cancelled terminal states.
- Immutable bilateral term revisions with producer and consumer acceptance.
- Bounded standstill declarations create one durable reconciliation lineage.
- Leased resolution attempts persist process, repair, and public-context
  expansion budgets across restarts.
- Resolution input/output artifacts are immutable and content-addressed; the
  composing kernel remains responsible for deterministic ownership and DAG
  validation before application.
- Immutable SHA-256 content-addressed artifacts with digest verification.
- Capability-derived principals and producer/consumer authorization.
- Revision checks reject stale publishers and validators.
- Stable command IDs make mutation retries idempotent and reject conflicting
  reuse.
- A durable outbox persists effects until the kernel acknowledges delivery.
- Obsolete effects are superseded transactionally when later contract state
  proves they are no longer actionable.
- Producer node completion is deferred until every outbound contract is
  terminal.
- Tarjan strongly connected components detect hard-wait cycles.
- No hidden goroutines, listeners, timers, or mutable global registry.

## Composition

```go
manager, err := coordination.Open(coordination.Options{
    StateDir: run.CoordinationDir,
    MaxNegotiationRounds: 3,
    MaxProcessAttempts: 2,
    MaxRepairAttempts: 1,
    MaxContextExpansions: 1,
})
defer manager.Close()

// The composing kernel is the sole runtime. On process startup it releases
// leases owned by the prior kernel before starting a worker.
err = manager.RecoverResolutionLeases(ctx, time.Now().UTC())

contract, err := manager.Request(ctx, consumer, coordination.RequestCommand{
    CommandID: "request-public-api-v1",
    Contract: coordination.Contract{
        ID: "public-api",
        ProducerAgentID: "agent-api",
        ProducerNodeID: "api-node",
        ConsumerAgentID: "agent-web",
        ConsumerNodeID: "web-node",
        Details: coordination.ContractDetails{
            Name: "PublicAPI",
            Kind: "interface",
            Acceptance: []string{"consumer contract tests pass"},
        },
    },
})
```

The initial request terms are accepted by both participants for backward
compatibility. Either participant may subsequently call `ProposeTerms`; the
other must call `AcceptTerms` before publication. At the configured round
limit, or for an explicitly terminal incompatibility, either participant may
call `DeclareStandstill`. The kernel claims the resulting resolution with a
lease, records sealed input/output artifacts, and either finalizes agreed terms
or moves the resolution to `AwaitingUser` after bounded failure.

Only kernel-observed progress should renew a resolution lease. In the Aion
composition, temporary Coordinator lifecycle events and inference-gateway
queue/forward/active pulses provide that signal. `Validated` and `Applying`
artifacts retain their state across lease expiry or kernel restart so recovery
does not consume another semantic attempt.

The library never issues inference credentials. The composing kernel rotates a
capability for each temporary Coordinator process and binds it to the intended
inference policy.

The kernel polls `PendingEffects`, applies each effect to its DAG, Hub, and
supervisor, then calls `AckEffect`. Failed delivery remains pending with a
kernel-selected retry time through `FailEffect`. Notifications are at-least-once;
stable effect IDs and idempotent contract commands make replay safe.

Artifact publication and fetching transfer bytes through the kernel control
socket. Isolated agents do not receive access to the coordination database,
object store, peer source, or host run directory.

## Harness

Run the same state directory twice to verify current-state recovery:

```bash
state_dir="$(mktemp -d)"
go run ./cmd/coordination-harness --state-dir "$state_dir"
go run ./cmd/coordination-harness --state-dir "$state_dir"
```

Run deterministic and race-enabled verification with:

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```
