# Declarative Reconcile Engine (Go, in-memory)

A local, dependency-free declarative reconciliation engine. Clients submit
**desired state**; a pool of workers converges an **in-memory simulated target
environment** to that state. No external services are required.

It is designed to give **consistent and explainable** results even under
concurrent submissions, version conflicts, injected backend failures/timeouts,
configuration drift, and dependency-ordered lifecycle changes.

- Module: `github.com/h3yow/declarative-reconcile-engine-go`
- Package: `reconcile`
- Demo: `go run ./cmd/demo`

---

## 1. Architecture

```
            Submit(Declaration)                     periodic full resync
 clients ┌──────────────────────┐                 ┌──────────────────────┐
 ───────▶│ Declared (desired)   │                 │  every ResyncInterval│
         │  - spec / phase      │                 │  enqueue every key   │
         │  - optimistic version│                 └──────────┬───────────┘
         │  - dependency graph  │                            │
         └──────────┬───────────┘                            │
                    │ enqueue id (+ affected dependents)     │
                    ▼                                         │
         ┌──────────────────────┐      Get/Done      ┌───────▼────────┐
         │  dedup work Queue     │◀──────────────────▶│  Worker pool    │
         │  (dirty-during-flight │                    │ (N workers)     │
         │   keys requeued once) │                    └───────┬─────────┘
         └──────────────────────┘                            │ snapshot
                                                    ┌─────────▼─────────┐
                                                    │    Reconciler      │
                                                    │ desired vs actual  │
                                                    │ dependency gating  │
                                                    │ delete protection  │
                                                    └─────────┬─────────┘
                                                              │ create/update
                                                              │ /delete (ctx)
                                                    ┌─────────▼─────────┐
                                                    │  Store (actual)    │
                                                    │ in-memory target   │
                                                    │ fault/drift/hang   │
                                                    │ injection points   │
                                                    └────────────────────┘
```

Three layers, each with a single responsibility:

| Layer | Responsibility |
|-------|----------------|
| `Declared` | Desired-state truth: validation, deterministic defaulting, optimistic versions, immutable dependency-graph snapshots. |
| `Queue` + `Engine` | Scheduling: dedup work queue, worker pool, per-attempt timeout, exponential backoff, resync, graceful shutdown. |
| `Reconciler` + `Store` | One-resource convergence decisions and the simulated target environment (actual state). |

---

## 2. Reconciliation protocol

### 2.1 Declarations and defaulting

A `Declaration` contains `ID`, `Spec`, `Phase` (`Present`/`Absent`),
`DependsOn`, and the optimistic token `ExpectedVersion`.

Defaulting is applied **on submit** and is fixed and deterministic:

- `Spec.Replicas <= 0` → `DefaultReplicas` (currently `1`);
- nil `Spec.Labels` → empty non-nil map (maps are copied, never aliased);
- `Spec.Image` is required for `Present` and is never invented.

Because defaulting happens before storage and comparison, a submission that
omits defaults is byte-for-byte equivalent to a fully specified one.

### 2.2 Idempotent, versioned writes

`Declared.Submit` returns `(newVersion, changed, err)`:

- **Create**: `ExpectedVersion == 0`. A new resource becomes version `1`.
- **Update / delete**: echo the version returned by the previous successful
  submit. Each accepted change bumps the version by `1`.
- **Idempotent no-op**: if `(spec, phase, deps)` are equal after defaulting,
  the same version is returned with `changed == false` and **no version
  bump** and no side effect.
- **Delete of an already-absent id** succeeds idempotently
  (`0, false, nil`).

Rejected writes never mutate stored state.

### 2.3 Distinct rejection reasons

Every rejected submit is a `*ConflictError` (or `*DependencyError`) wrapping a
distinguishable sentinel. Use `errors.Is`:

| Sentinel | When |
|----------|------|
| `ErrAlreadyExists` | create (`ExpectedVersion==0`) but the id exists |
| `ErrVersionConflict` | update/delete whose `ExpectedVersion` differs from the current version (a stale write) — never silently overwritten |
| `ErrNotFound` | update/delete against an id that does not exist |
| `ErrInvalidSpec` | e.g. empty `Image` for a `Present` resource |
| `ErrDependencyCycle` | the new edges would create a cycle among live declarations |
| `ErrDependencyMissing` | referenced dependency id does not resolve (treated as a wait at reconcile time; cycle is rejected at submit time) |
| `ErrStopped` | submit after the engine began shutting down |

The error message includes both `expectedVersion` and `currentVersion` so
clients can tell "too old" from any other cause and re-read before retrying.

### 2.4 One reconcile pass per resource

For each key, the engine takes an immutable snapshot and the reconciler:

1. **Desired absent**: if the resource is physically present, delete it,
   unless a **live dependent** (currently declared AND physically present)
   still depends on it, in which case the delete is blocked and re-checked.
2. **Desired present**:
   - if a transitive dependency id is not declared → wait (requeue);
   - if a direct dependency is absent or not `Ready` → wait (requeue);
   - else compare desired vs actual: create if missing, update on spec drift
     / not-ready, no-op if equal.

Decisions are emitted as structured `Event`s (`create/update/delete/noop/
blocked/wait_deps/error`) with a human-readable reason — the basis for
explainability and test logs.

### 2.5 Dependency ordering and lifecycle

- **Create ordering**: a resource is only created/updated once every direct
  prerequisite exists and is `Ready`. This yields a topological create order.
- **Delete protection**: a prerequisite cannot be physically removed while a
  live dependent still needs it (no orphans).
- **Delete cascade**: when a resource is physically removed, its former
  prerequisites that are *themselves desired absent* are re-evaluated one tick
  later (so they observe the freshly committed declared state), producing a
  reverse-topological teardown.
- **Graph retention on delete**: a deleted declaration keeps a retained graph
  vertex (incoming edges for protection, outgoing edges for cascade) until
  referencing vertices are also deleted. Cycle checks only consider live
  declarations, so retained vertices never create false cycles.
- **Update + delete race**: the declared layer linearizes competing writes
  with optimistic versions; the loser receives `ErrVersionConflict`. The
  eventual actual state is always one of the consistent possibilities —
  never "child exists without its dependency" or "dependency deleted while a
  live child needs it".

---

## 3. Concurrency, retries and continuous reconciliation

### 3.1 Deduplicating, lossless work queue

- A key is queued at most once while pending and at most once in flight.
- `Add(key)` while `key` is processing is recorded as **dirty**; `Done`
  requeues it exactly once, so events that arrive mid-reconcile are never
  lost.
- The queue is closed for new `Add`s at shutdown but still drains pending and
  in-flight keys (including dirty requeues); `Get` returns false only after a
  full drain.

### 3.2 Isolation between resources

- N workers process different keys concurrently; the same key is never
  processed concurrently (dedup + in-flight marking).
- Every reconcile attempt gets its own `context.WithTimeout`
  (`ReconcileTimeout`). A hung/timed-out resource does not block other
  resources.
- Backend operations take the caller's context; injected "hangs" block
  **without holding the store lock**, so a timeout on one resource leaves the
  rest of the system fully schedulable.

### 3.3 Retry / backoff rules

| Outcome | Engine reaction |
|---------|-----------------|
| success / no-op | clear the key's retry budget |
| `ErrTransient` (injected fault) | exponential backoff `BaseBackoff << n`, capped at `MaxBackoff`; counter increments |
| attempt timeout (`context.DeadlineExceeded`) | counted as timeout + error, same capped exponential backoff |
| waiting on dependency / blocked delete | short fixed re-delay (`RequeueDelay`), does **not** grow the failure backoff |

Backoff is per key, so one failing resource cannot starve or slow others.

### 3.4 Continuous reconciliation and drift repair

A resync loop enqueues every declared key **and every physically present key**
every `ResyncInterval`. Consequently:

- out-of-band changes (`Store.InjectDrift`) are detected and repaired;
- a retry whose scheduling signal was somehow missed is still picked up;
- actual resources with no declaration (orphans) are deleted on resync;
- a converged system stays stable — equal desired/actual yields `noop` with no
  version churn.

There is no maximum retry count: transient failures and dependency waits
retry until success, so a resource cannot be permanently stuck while it has a
valid desired state. (The resync loop is the safety net guaranteeing progress
after recovery.)

### 3.5 Graceful shutdown and in-flight work

`Shutdown(grace)` (or cancelling the context passed to `Run`) does the
following deterministically:

1. new submits are rejected with `ErrStopped`;
2. the engine context is cancelled, so no *new* attempt starts;
3. already in-flight attempts have their context cancelled promptly and end
   with a definite applied / not-applied result (e.g. a cancelled create
   leaves no resource behind);
4. pending and dirty keys are drained and workers exit;
5. pending backoff/cascade timers are cancelled.

If workers cannot drain within the grace period, `Shutdown` returns an error.

---

## 4. Simulated target environment and fault injection

`Store` is a linearized in-memory backend. Test hooks:

- `InjectFailures(op, n)` — next `n` `create`/`update`/`delete`/`transition`
  calls return an error wrapping `ErrTransient`, then recover automatically;
- `InjectHang(id, n)` / `ReleaseHangs()` — next `n` mutating calls for an id
  block until context cancellation or release (simulates a hung backend
  without blocking other resources);
- `InjectDrift(id, spec)` — the next reads show out-of-band state until the
  next reconcile update repairs it.

`Stats()` exposes `Reconciles / Errors / Timeouts / Requeues / InFlight /
QueueLen / Stopped` for assertions.

---

## 5. Verification

All checks run offline and require no external services.

```bash
# unit + integration style tests (verbose logs print inputs and the
# decision basis for each scenario)
go test ./... -v

# race detector and repeated runs for concurrency scenarios
go test -race ./... -count=3

# static checks
go vet ./...

# end-to-end walkthrough: defaults, idempotence, dependency order,
# distinct conflict reason, injected-failure retry, drift repair, teardown
go run ./cmd/demo
```

### Test coverage map

| Area | Tests |
|------|-------|
| declare/update/delete, idempotence, deterministic defaults | `TestDeclaredCRUDIdempotenceAndDefaults`, `TestSpecDefaultingDeterministic` |
| distinguishable conflict reasons, rejected writes don't mutate state | `TestDeclaredConflictReasonsAreDistinguishable` |
| invalid spec, cycle rejection | `TestDeclaredInvalidSpecAndCycles` |
| queue dedup / dirty-requeue / close-drain | `TestQueue*` |
| create/update/delete, dependency gating, delete protection, transient retry | `TestReconciler*` |
| concurrent submissions + contended version updates | `TestEngineConcurrentSubmissionsConverge` |
| transient fault recovery, resource isolation | `TestEngineFaultRecovery` |
| periodic drift auto-repair and resync stability | `TestEngineDriftAutoRepair` |
| per-resource timeout does not stall others, recovers after release | `TestEngineTimeoutDoesNotStall` |
| topological create order and reverse teardown | `TestEngineDependencyOrdering` |
| racing update vs delete leaves no orphans/illegal states | `TestEngineConcurrentDeleteVsUpdateNoOrphans` |
| graceful shutdown, post-stop reject, fail-closed restart | `TestEngineGracefulShutdownAndReject` |
| in-flight reconcile cancelled consistently on shutdown | `TestEngineShutdownCancelsInFlightConsistently` |
| `Run` returns on context cancellation | `TestEngineRunBlocksUntilContextCancel` |
| delete retried after faults; no resurrection on resync | `TestEngineDeleteFailureRetriedAndResyncStable` |
| missing dependency arrives later → task not lost | `TestEngineMissingDependencyArrivesLater` |
| repeated identical reconciliation is stable (no churn) | `TestEngineRepeatedReconcileIsStable` |

Tests call `t.Logf` with both the **input** (ids, specs, expected versions,
injected faults) and the **decision basis** (rejected reason, expected vs
current version, wait/block/noop reason, counters), visible with `-v`.

---

## 6. Minimal usage

```go
decl := reconcile.NewDeclared()
store := reconcile.NewStore()
rec := reconcile.NewReconciler(store)
eng := reconcile.NewEngine(decl, rec, reconcile.DefaultConfig())
if err := eng.Start(); err != nil { /* ... */ }

v, _, err := eng.Submit(reconcile.Declaration{
    ID: "db", Spec: reconcile.Spec{Image: "db:1"}, Phase: reconcile.PhasePresent,
})
// ... subsequent updates must echo v; a stale value returns an error
// wrapping reconcile.ErrVersionConflict with currentVersion for retry.

_ = eng.Shutdown(5 * time.Second)
```
