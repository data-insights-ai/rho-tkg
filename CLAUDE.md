# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately – don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One tack per subagent for focused execution

### 3. Self-Improvement Loop
- **Session start**: Read `tasks/lessons.md` and `tasks/todo.md` before doing any work
- After ANY correction from the user: update `tasks/lessons.md` with the pattern
- **Session end**: Update `tasks/lessons.md` with new lessons, clean up `tasks/todo.md`
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes – don't over-engineer
- Challenge your own work before presenting it

### 6. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests – then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo.md`
6. **Capture Lessons**: Update `tasks/lessons.md` after corrections

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.
- **Security First**: Every input is untrusted, every dependency is a potential attack surface. Security-first TDD — write tests proving the attack vector is blocked before implementing the defense.

## Project Overview

**Temporal Knowledge Graph v3** — a Go library defining core domain types for a temporal knowledge graph. This is a pure library (no main binary). All entity identification uses `snowflake.ID` from the internal `snowflake-2026` package.

Module: `gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3`
Go: 1.26.0
Dependencies: `rho/snowflake-2026` (IDs), `rho/kit` (service toolkit)

## Build & Test Commands

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector enabled — always run for concurrent code
make test-integration  # integration tests (long-running)
make cover          # coverage report → coverage.html
make check          # pre-commit: vet + build + test
make ci             # full pipeline: fmt-check + vet + build + test-race + security + vulncheck
make fmt            # format code
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

Single test: `go test -run TestFoo ./pkg/types/`

### TDD Discipline

Follow strict TDD: write the test first, implement to make it pass, run tests and fix until green. If a bug is reported, write a failing test that reproduces it before touching implementation code. Use table-driven tests for edge cases. Run `make test` before and after every change. Keep tests alongside code in `_test.go` files.

### Testing Rules (hard requirements)

These rules exist because every single one was violated at least once. Do not skip them.

1. **Every public method gets a direct test.** Indirect coverage via delegation (e.g., `Node.SetProperty` → `PropertySlice.Set`) does NOT count. If `Properties()` is a method, there must be a `TestNodeProperties*` and a `TestRelProperties*`. Run `go tool cover -func=coverage.out` after implementation — any public method at 0% is a blocker.

2. **Node and Relationship must have test parity.** When a test exists for Node (e.g., `TestNodeVersion`), the equivalent MUST exist for Relationship (`TestRelVersion`). These types are structural mirrors — test gaps on one always mean test gaps on the other. After adding tests to one, grep for the pattern and add the mirror.

3. **Every type-switch branch gets its own test.** When `deepCopyValue` has a `case []int64:` branch, there must be a `TestDeepCopyInt64SliceIsIndependent`. No exceptions — trivially correct code still needs proof. Coverage percentage is the check: if a branch shows 0% in `cover`, add a test.

4. **Sentinel errors are tested with `errors.Is`, not just `err != nil`.** Any function returning a sentinel error (e.g., `ErrReservedPrefix`) must have a test asserting `errors.Is(err, ErrReservedPrefix) == true`. This applies at every call layer — if `Node.SetProperty` wraps `PropertySlice.Set`, the node-level test must also check `errors.Is`.

5. **Fallback/reflect paths must be tested or removed.** If a function has a `default:` branch that uses reflection, generics, or any "catch-all" logic, it must have at least one test exercising that exact path with a type that doesn't match any explicit case. Untested fallback code is dead code with a false guarantee.

6. **Deep copy means deep copy — no half-measures.** If a function claims to deep-copy, it must truly clone all nested reference types. A reflect-based copy that only copies one level is NOT a deep copy — it must be documented as "shallow element copy" or actually recurse. Never write a doc comment that says "deep copy" when the implementation is shallow.

7. **Run `make cover` before marking any step complete.** Check the per-function report. Any public method at 0% or any new code path below 80% must be addressed before moving on.

8. **Validation must be recursive/adversarial.** When a function validates values (e.g., rejecting pointers/structs), the validation MUST traverse container types (slices, maps, `any` interfaces) to check nested values. A top-level-only check like `reflect.TypeOf(value).Kind()` is bypassed by `[]any{&myStruct{}}`. Always ask: "What can an attacker nest inside allowed types?" Write tests with nested prohibited values, not just top-level ones.

9. **Test nil values in reflect-based code.** `reflect.ValueOf(nil)` returns the zero `reflect.Value`. `reflect.Value.SetMapIndex` with a zero Value **deletes the key** — silent data loss. Any reflect code that handles maps must have a test with nil values. This is a well-known Go gotcha.

10. **One-time warnings must use `sync.Once`.** Log warnings that trigger on a threshold (e.g., "approaching capacity") must fire exactly once, not on every subsequent call. Use `sync.Once` — it survives non-sequential allocation and is idempotent. Never use `>=` for a warning that should be one-shot.

11. **No empty stubs when the spec defines the fields.** "Phase N" stubs are acceptable only when the field list is genuinely unknown. If the spec already defines `TemporalMetadata` fields, populate the struct. Empty stubs create false confidence that the type is "done."

12. **Public method return types must not leak dependencies.** If a method returns a type from an external package (e.g., `snowflake.ID`), that dependency leaks into consumers. Use unexported wrapper types (`type nodeID snowflake.ID`, NOT `type nodeID int64`) to keep the dependency boundary clean while preserving the semantic contract. Never substitute `int64` for `snowflake.ID` — that violates the "snowflake.ID everywhere" invariant. Audit all public method signatures when adding a new dependency.

13. **Config fields must be used or removed.** Never accept a config field that does nothing (`SnowflakeNodeID` accepted but generators commented out). Unused config misleads callers into thinking they're configuring something. Either implement it or don't accept it.

### Security Scanning

Run `make security` (gosec) and `make vulncheck` (govulncheck) as part of CI. Treat vulnerable deps as failing tests. Never string-concatenate user input into queries or shell commands. Never leak stack traces, file paths, or system details to external callers.

## Architecture

Current packages (evolving):

### `pkg/types`

| File | Purpose |
|---|---|
| `node.go` | Node (graph vertex) — `nodeID` (wraps `snowflake.ID`), primary + extra labels as `labelToken`, properties, version, temporal, integrity |
| `relationship.go` | Relationship (directed edge) — `relID` (wraps `snowflake.ID`), `relTypeToken`, start/end as `nodeID`, properties, version, temporal, integrity |
| `propertyslice.go` | Sorted key-value store with binary search; recursive validation rejects `tkg_` prefix keys and pointer/struct values at any nesting depth |
| `shadow.go` | Constants for virtual read-only properties (`tkg_*`) managed by the graph layer |
| `temporal.go` | `Instant` type (Unix ms), `TemporalMetadata` struct (validity, transaction, audit, provenance, version chain) |
| `integrity.go` | `NodeIntegrity` / `RelIntegrity` structs (hash chain: `Hash`, `PrevHash`) |

### `pkg/graph`

| File | Purpose |
|---|---|
| `graph.go` | Graph struct with Config, dual snowflake generators (`NextNodeID`/`NextRelID`), label/reltype registry ownership, string resolution methods |
| `label_registry.go` | Thread-safe label string ↔ uint16 token registry (RWMutex, double-check, `sync.Once` capacity warning) |
| `reltype_registry.go` | Thread-safe relationship type string ↔ uint16 token registry |
| `doc.go` | Package documentation |

## Critical Design Invariants

**Pure-data structs (core architectural rule)**: Node and Relationship **never** hold references to the Graph, registries, or any resolver. They are self-contained data containers that hold tokens internally. String resolution is **always** the responsibility of the Graph layer, Cypher engine, or serialization layer — never on entities. No `SetGraph()`, no injected resolvers. This ensures entities are safely serializable, cacheable, testable standalone, and safe across goroutines.

**snowflake.ID everywhere**: All entity IDs are backed by `snowflake.ID`. Internally, `Node.id` is `nodeID` (wraps `snowflake.ID`), `Relationship.id` is `relID` (wraps `snowflake.ID`), and `startID`/`endID` are `nodeID`. These opaque wrappers prevent external packages from constructing or comparing IDs directly. Constructors accept `snowflake.ID`; the graph layer generates IDs via `NextNodeID()`/`NextRelID()`. Never use plain `int64` or `string` for entity IDs.

**Dual snowflake generators**: Graph holds two separate generators — one for nodes, one for relationships — to guarantee independent ID spaces. Epoch: `2026-01-01`. Default: 10-bit node (1024 instances), 12-bit step (4096 IDs/ms). Each concurrent graph instance **must** use a different `SnowflakeNodeID`. Generators are stateless — no counter persistence, no crash recovery.

**Strict encapsulation**: All struct fields are unexported. Access is through methods only. Constructors are `NewNode(id, primaryLabel, extraLabels)` and `NewRelationship(id, relType, startID, endID)`.

**Defensive copying**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `PropertiesMap()`, `ToMap()`, and `DeepCopy()` always return fully independent copies — never internal references. "Independent" means mutating the returned value must never affect the original, including nested slices and maps. Tests enforce this at every layer. When adding a new accessor that returns reference types, always deep-copy and always add a mutation-independence test.

**Token interning**: Labels (`labelToken`) and relationship types (`relTypeToken`) are `uint16`. **Token 0 is reserved** as zero/invalid and must never be assigned — `HasLabelToken(0)` and `HasTypeToken(0)` always return false.

**No pointers or structs in properties**: `PropertySlice.Set()` recursively validates values at insertion time, rejecting any pointer or struct at any nesting depth (`ErrUnsupportedValueType`). A `[]any{&myStruct{}}` is rejected just like a top-level `&myStruct{}`. Graph databases store data, not application memory references. Only primitives, slices of primitives, and maps with primitive keys/values are accepted. Validation traverses slices, maps, and `any`/interface wrappers to reach leaf values.

**Shadow property protection**: The `tkg_` prefix is reserved for graph-layer virtual properties. `PropertySlice.Set()` rejects any key starting with `tkg_` — security boundary preventing clients from spoofing internal metadata.

**PropertySlice sorted invariant**: Properties are maintained in sorted-by-key order. Always use `Set()` to add/update — never modify the slice directly.

## Registries (pkg/graph)

Two independent registries with independent token namespaces. A label `"KNOWS"` and a relationship type `"KNOWS"` get independent tokens.

- **labelRegistry**: `map[string]labelToken` + `[]string` reverse lookup. Thread-safe (RWMutex, double-check on write miss).
- **relTypeRegistry**: Same structure with `relTypeToken`.
- Methods: `GetOrCreate(string)`, `Resolve(token)`, `ResolveAll([]token)`, `Lookup(string) (token, bool)`
- Growth warning logged at 60K tokens (92% of uint16). `GetOrCreate` returns error at 65535.
- Persisted to Badger as `meta/label_tokens` and `meta/reltype_tokens` (msgpack `[]string`).

## String Resolution Ownership

The Graph layer is the **sole owner** of string resolution. Three consumers, all with registry access:

| Consumer | Resolution methods |
|---|---|
| Graph layer | `NodeLabels(n)`, `NodePrimaryLabel(n)`, `RelationshipType(r)`, `ResolveNodeProperty(n, key)`, `ResolveRelProperty(r, key)` |
| Cypher engine | Resolves label/type tokens once per query via `Lookup()`, then matches with integer comparison |
| REST/gRPC API | Calls Graph resolution methods before JSON encoding |

All internal operations (index lookups, label matching, adjacency traversal) work with tokens directly.

## Shadow Properties (final 15)

All resolve to user-meaningful data via the Graph layer. No internal IDs exposed.

| Key | Type | Applies To | Category |
|---|---|---|---|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both | Temporal |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both | Temporal |
| `tkg_created_at`, `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both | Temporal |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both | Provenance |
| `tkg_version` | `int` | Both | Provenance |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both | Integrity |
| `tkg_base_entity` | `int64` | Both | Version chain |

**Removed**: `tkg_id`, `tkg_start_id`, `tkg_end_id` — internal snowflake IDs have no user purpose.

## Badger Key Layout

All keys use fixed-width binary encoding. Snowflake IDs stored as big-endian int64 (8B) for correct sort order and temporal clustering.

| Key pattern | Purpose | Key size |
|---|---|---|
| `n/<8B nodeID>` | Node entity | 9B |
| `r/<8B relID>` | Relationship entity | 9B |
| `l/<2B labelToken>/<8B nodeID>` | Label index | 10B |
| `rt/<2B relTypeToken>/<8B relID>` | Type index | 10B |
| `out/<8B startID>/<2B relType>/<8B endID>/<8B relID>` | Outgoing adjacency | 26B |
| `in/<8B endID>/<2B relType>/<8B startID>/<8B relID>` | Incoming adjacency | 26B |
| `h/n/<8B nodeID>/<8B version>` | Node history | varies |
| `h/r/<8B relID>/<8B version>` | Rel history | varies |
| `tv/n/<8B validFrom>/<8B nodeID>` | Temporal index | varies |
| `meta/label_tokens`, `meta/reltype_tokens` | Registry persistence | varies |

No `meta/next_node_id` or `meta/next_rel_id` — snowflake generation is stateless.

## Implementation Phases

1. **Core Types & Registries** ✅ — `pkg/types` (Node, Relationship, PropertySlice, shadow constants, temporal, integrity) + `pkg/graph` (labelRegistry, relTypeRegistry, dual snowflake generators, string resolution). Opaque ID types (`nodeID`/`relID`), recursive property validation, `Instant` timestamps.
2. **Snowflake ID Generation & Serialization** — snowflake generators ✅. Remaining: msgpack wire formats (`nodeWire`/`relWire`), Badger persistence, registry persist/restart.
3. **Index Migration** — new fixed-width Badger key formats, index maintenance. Tests: label/type index scans, adjacency prefix scans, history ordering, big-endian sort verification.
4. **Cypher & Graph API Integration** — Graph resolution methods, Cypher token-based matching, new `AddNode`/`AddRelationship` signatures (no user-supplied IDs). Tests: all 15 shadow keys resolve, label/type matching, nonexistent label returns empty result.

## rho/kit Integration

tkg-v3 lives in the `rho/` ecosystem alongside `rho/kit` (`gitlab2024.bds421-cloud.com/bds421/rho/kit`), the standard Go service toolkit. When tkg-v3 grows beyond pure types into services, storage, or APIs, follow kit's conventions:

### Error Handling — use kit/apperror

Use structured error types from `kit/apperror`, not raw `errors.New` or `fmt.Errorf` for domain errors:
- `apperror.NewNotFound(entity, id)` — entity lookup misses
- `apperror.NewValidation(msg)` / `apperror.NewFieldValidation(fields...)` — input validation failures
- `apperror.NewConflict(msg)` — duplicate or lock contention
- `apperror.NewPermanent(msg)` — non-retryable failures (skip retry queues)
- Check with `apperror.IsNotFound(err)`, `apperror.IsValidation(err)`, etc.

### Service Bootstrap — kit/app Builder

Services use the fluent `app.Builder` pattern. Wire infrastructure via `WithPostgres`, `WithMariaDB`, `WithRabbitMQ`, `WithJWT`, then pass an `app.Infrastructure` to a router function. Never construct DB pools, message brokers, or health checks manually.

### Middleware Stack — kit/middleware/stack

Use `stack.Default(router, logger, ...)` for canonical middleware ordering: metrics → request ID → tracing → logging → handler. Add CSRF/auth as outer middleware.

### Observability — always first-class

- **Logging**: `kit/logging` (slog + JSON to stdout). Use `logging.FromContext(ctx)` for request-scoped loggers. Never use `log.Println` or `fmt.Printf` for application logging.
- **Tracing**: `kit/tracing` (OpenTelemetry). Noop when no endpoint configured — zero overhead.
- **Metrics**: Prometheus via `kit/health`. Resource names used as Prometheus labels must be small and static — never embed user IDs or request IDs.

### Resilience Patterns

- **Retry**: `kit/retry` with `Policy{MaxRetries, BaseDelay, MaxDelay, Factor}` — exponential backoff with jitter.
- **Circuit Breaker**: `kit/circuitbreaker` (sony/gobreaker) — failfast on repeated failures.
- **Health Checks**: `kit/health.DependencyCheck` interface. Critical deps block server start; non-critical report degraded.

### Data Access

- **Database**: `kit/database` for config + DSN + pool metrics. `kit/database/gormdb` for GORM setup. Supports MariaDB and PostgreSQL.
- **Redis**: `kit/redis` with auto-reconnect. Sub-packages: `redis/cache`, `redis/stream`, `redis/queue`.
- **Messaging**: `kit/messaging` for RabbitMQ with topology declaration, publisher/consumer, transactional outbox.
- **Cache**: `kit/cache.Cache` interface (Get/Set/Delete/Clear) — backend-agnostic. Key validation: non-empty, no null bytes, ≤1024 chars.

### Configuration

All config via environment variables (12-factor). Use `configutil.LoadBaseConfig(port)` for universal fields. DB-specific: `database.LoadMariaDBFields`, `database.LoadPostgresFields`. Docker secrets supported via `_FILE` suffix.

### Testing with Testcontainers

Integration tests use `kit/testutil` helpers:
- `testutil/dbtest` — MariaDB, PostgreSQL containers
- `testutil/redistest` — Redis containers
- `testutil/rabbitmqtest` — RabbitMQ containers

### Key Conventions

- **Fail fast**: Panic on programmer errors (nil JWT provider, missing required config). Validate in `Builder.Run()`.
- **Security**: `kit/encrypt`, `kit/signing`, `kit/masking`. Never leak internals in error responses. mTLS via `kit/tlsutil`.
- **Validation**: `kit/validate` wraps `go-playground/validator/v10`, converts to `apperror.ValidationError`.
- **Private registry**: `go env -w GOPRIVATE="gitlab2024.bds421-cloud.com/*"` to bypass public proxy.
