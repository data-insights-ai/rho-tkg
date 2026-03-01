# Lessons Learned

Patterns that caused review findings. Rules to prevent recurrence.

## 2026-02-27 — Pre-release review v3.0.3 (6 findings)

### Shallow validation allows nested prohibited types (BLOCKER)
**Problem:** `Set()` checks `reflect.TypeOf(value).Kind()` on top-level only. `[]any{&myStruct{}}` bypasses because the slice kind is not Ptr/Struct. Attacker can inject pointers into graph state via container nesting.
**Root cause:** Happy-path validation — "reject pointers" instead of "reject anything containing pointers at any depth."
**Rule:** Validation of property values must be recursive. Extract a `validatePropertyValue()` that traverses slices, maps, and `reflect.Interface` wrappers to leaf values. `reflect.Interface` elements must be unwrapped with `.Elem()`. Write tests with nested prohibited values: `[]any{&s{}}`, `map[string]any{"k": &s{}}`, `[]any{map[string]any{"k": &s{}}}` (3 levels).

### reflect.ValueOf(nil) causes silent data loss in DeepCopy (BLOCKER)
**Problem:** `reflectCopyValue` line: `cp.SetMapIndex(key, reflect.ValueOf(deepCopyValue(iter.Value().Interface())))`. When map value is nil, `deepCopyValue(nil)` returns nil, `reflect.ValueOf(nil)` is a zero `reflect.Value`, and `SetMapIndex` with zero Value *deletes the key*. Silent data loss.
**Root cause:** No test for nil values in maps on the reflect path. The explicit `map[string]any` path handles nil fine (plain Go assignment).
**Rule:** Any reflect code handling maps must test nil values. Fix: check if copied value is nil before `SetMapIndex`; if nil, use `reflect.Zero(rv.Type().Elem())`.

### Log spam: capacity warning fires 5,535 times (MAJOR)
**Problem:** `if int(tok) >= tokenCapacityWarning` fires on every token from 60000 to 65534.
**Root cause:** `>=` threshold without one-shot guard.
**Rule:** Use `sync.Once` for capacity warnings. Better than `==` because it survives non-sequential allocation after registry restore.

### Empty stubs shipped when spec defined the fields (MINOR)
**Problem:** `TemporalMetadata{}`, `NodeIntegrity{}`, `RelIntegrity{}` are empty structs. Spec Section 8.2 defines all fields.
**Root cause:** "Phase 1" excuse to defer work that was already specified.
**Rule:** Only use stubs when the field list is genuinely unknown. If the spec defines it, implement it.

### InternalID() leaks snowflake.ID dependency (MAJOR)
**Problem:** `InternalID()` returns `snowflake.ID` from external package. Spec Section 2: "All four types are unexported." Consumers are forced to import `snowflake-2026`.
**Root cause:** No dependency audit on public method signatures.
**Rule:** Use unexported wrapper types (`nodeID`, `relID`) for IDs returned by public methods. Constructors accept `snowflake.ID` and cast internally. Audit all public signatures for leaked external types.

### Opaque ID wrappers must wrap snowflake.ID, not int64 (CORRECTION)
**Problem:** Introduced `type nodeID int64` and `type relID int64`, violating "snowflake.ID everywhere" invariant. This replaced the snowflake dependency with raw int64 — exactly what the invariant forbids.
**Root cause:** Focused on "make it unexported" without checking that the underlying type preserves the snowflake.ID contract.
**Rule:** Opaque ID types must be `type nodeID snowflake.ID` and `type relID snowflake.ID`. The wrapping hides the type from external packages but preserves the snowflake.ID semantics internally. Never substitute int64 for snowflake.ID.

### Config accepted but unused (BLOCKER)
**Problem:** `Config.SnowflakeNodeID` flows in but generators are `// Phase 2` commented out. Graph cannot generate IDs.
**Root cause:** Incremental development left the API incomplete.
**Rule:** Config fields must be used or removed. Never ship an API that accepts input it ignores.

---

## 2026-02-27 — Pre-release review round 2 (v3.0.4)

### Implicit snowflake defaults are fragile (BLOCKER)
**Problem:** `snowflake.NewNode(config.SnowflakeNodeID)` relies on library defaults matching spec (epoch, node bits, step bits). If the library changes defaults, IDs silently break.
**Rule:** Always pass explicit options: `WithEpoch(snowflakeEpoch)`, `WithNodeBits(10)`, `WithStepBits(12)`. Define the epoch as a package-level variable for clarity.

### Denylist validation misses unknown-unknown types (MAJOR)
**Problem:** `validateReflectValue` only blocked Ptr/Struct. Arrays, channels, functions, unsafe pointers all passed through the `default: return nil` branch. An attacker can embed pointers inside `[1]*MyStruct{}` to bypass validation.
**Root cause:** Denylist thinking — "what should I block?" instead of "what should I allow?"
**Rule:** Always use allowlists for security-sensitive validation. Explicitly enumerate safe kinds (primitives, containers). Everything else defaults to rejection. This is the same pattern as firewall rules: deny-all, allow-specific.

### Off-by-one on uint16 capacity boundary (MINOR)
**Problem:** `r.nextToken >= tokenCapacityMax` (65535) prevents token 65535 from being assigned. uint16 max is 65535, so the range is 1-65535 (65535 usable tokens), not 1-65534.
**Root cause:** Checking the counter instead of actual occupancy.
**Rule:** For capacity checks, use `len(collection) > max` instead of counter comparisons. This checks actual occupancy and avoids uint16 overflow concerns when the counter wraps.

### Zero-alloc paths matter for hot-path graph operations (MAJOR)
**Problem:** `NodeHasLabel` called `AllLabelTokens()` which allocates a new slice on every call. In graph traversal, this is a hot path.
**Rule:** Provide raw-type accessor methods (e.g., `HasLabelTokenRaw(uint16)`) for performance-critical paths across package boundaries. The opaque type is still the primary API; the raw method is the zero-alloc escape hatch.

---

## 2026-02-27 — Pre-release review v3.0.2

### Coverage gaps between mirrored types
**Problem:** Node had `TestNodeVersion` but Relationship had no version test. Node had no `Properties()` test while Relationship did.
**Rule:** Node and Relationship are structural mirrors. After writing a test for one, immediately grep for the pattern and write the equivalent for the other. Run `make cover` to verify — 0% on any public method is a blocker.

### Untested type-switch branches
**Problem:** Added `[]int64` and `[]bool` cases to `deepCopyValue` without isolation tests. Coverage showed 68.8%.
**Rule:** Every `case` branch in a type switch gets its own test. No exceptions, even for trivially correct code. The test IS the proof.

### Reflect fallback claimed deep copy but was shallow
**Problem:** `reflectCopyValue` copied containers but not nested elements. Doc said "deep copy" but implementation was one-level.
**Rule:** If a function claims deep copy, it must recurse into nested reference types. If it can't (reflect limitations), the doc must say "shallow element copy" explicitly. Never let a doc comment overpromise.

### Sentinel errors checked with `err != nil` instead of `errors.Is`
**Problem:** Node/Relationship tkg_ rejection tests checked `err != nil` but not `errors.Is(err, ErrReservedPrefix)`. If the wrapping chain broke, no test would catch it.
**Rule:** Every sentinel error test must use `errors.Is`. Every call layer that propagates a sentinel gets its own `errors.Is` assertion.

### Passthrough methods still need tests
**Problem:** `LookupLabel` and `LookupRelType` on Graph are one-line passthroughs to registry methods. Initial coverage run showed 0%.
**Rule:** Every public method gets a test, even trivial passthroughs. The coverage check catches these — never skip `make cover` before marking done.

### Missing coverage check before marking complete
**Problem:** Multiple 0% coverage methods shipped because `make cover` wasn't run as a gate.
**Rule:** Run `make cover` and review `go tool cover -func=coverage.out` before marking any task complete. Any public method at 0% is not done.

---

## 2026-02-27 — Pre-release review round 3 (v3.0.5)

### Recursive functions without depth limits are stack-overflow bombs (MAJOR)
**Problem:** `validateReflectValue`, `deepCopyValue`, and `reflectCopyValue` recurse into nested containers without any depth guard. A self-referential `[]any` (or any deeply-nested structure) causes infinite recursion → stack overflow → process crash.
**Root cause:** Recursive validation was added to fix shallow checks (round 1), but nobody asked "what stops the recursion besides the data running out?"
**Rule:** Every recursive function that processes untrusted input MUST have a depth parameter and a hard limit. Return an error (validation) or stop recursing (copy) when exceeded. Use a package-level constant (e.g., `maxPropertyDepth = 32`) so the limit is discoverable and testable. Write boundary tests: exactly-at-limit succeeds, one-over-limit fails.

### Empty-string inputs create ambiguous state in registries (MAJOR)
**Problem:** `GetOrCreate("")` silently assigns a token to the empty string. Then `Resolve(0)` (unregistered) and `Resolve(emptyToken)` both return `""` — indistinguishable. Callers cannot tell "not found" from "registered empty."
**Root cause:** No input validation at the registry boundary. The function assumed all callers would pass meaningful strings.
**Rule:** Validate inputs at system boundaries before they enter internal state. Registries must reject empty names with a sentinel error (`ErrEmptyName`). This is the same principle as "never trust external data" applied to internal APIs.

### Doc comments and error messages drift after behavior changes (MINOR)
**Problem:** `ErrUnsupportedValueType` still said "pointer and struct values" after the allowlist rewrite that also rejects arrays, funcs, channels. `Set()` doc comment was equally stale. Misleading messages confuse callers.
**Root cause:** Changed the implementation but didn't grep for all references to the old behavior.
**Rule:** After changing validation behavior, search the entire file for mentions of the old behavior — error messages, doc comments, inline comments. Update all of them. A stale error message is a lie to the caller.

### Identical initialization parameters create dead code (MINOR)
**Problem:** Both snowflake generators use the same `WithEpoch/WithNodeBits/WithStepBits`. If the first succeeds, the second always succeeds. The second error path is unreachable dead code (85.7% coverage).
**Root cause:** Copy-paste initialization without considering that identical params guarantee identical outcomes.
**Rule:** When code initializes multiple instances with identical params, document why the error handling exists despite being unreachable. "Defensive correctness" is valid but must be explicit, not accidental.

### Single-variant test coverage misses multi-path behavior (MINOR)
**Problem:** `TestGraphNodeHasLabel` only tested a single-label node. The extra-label hit path through the graph layer was untested.
**Root cause:** Wrote the simplest test that made coverage nonzero, without asking "what other inputs exercise different code paths?"
**Rule:** After writing the happy-path test, ask: "What variants of the input exercise different branches?" For `NodeHasLabel`: primary-label hit, extra-label hit, miss. All three need a test.

### Silent integer overflow must be documented (MINOR)
**Problem:** After assigning token 65535, `nextToken++` wraps `uint16` to 0. Safe today because `len(collection)` protects against further assignments, but undocumented. A future maintainer might add logic that trusts `nextToken` without knowing it can be 0.
**Root cause:** Relying on implicit overflow behavior without a comment.
**Rule:** When integer overflow is expected and safe, add a comment explaining why. Implicit "it works because X" is a future bug when X changes.

---

## 2026-02-27 — Phase 2A: Store, MemoryStore, Entity Management, Shadow Resolution

### Self-loop in cascade delete needs dedup (PATTERN)
**Problem:** `DeleteNode` collects outgoing and incoming rels separately. A self-loop (A→A) appears in both lists. Deleting it in the outgoing pass means the incoming pass hits `ErrRelNotFound`.
**Solution:** The incoming loop skips `ErrRelNotFound` — not an error, just a self-loop already handled. This is a correctness pattern, not a bug.
**Rule:** When cascade-deleting bidirectional index entries, always guard against double-delete on self-referential edges.

### Bulk construction before ID generation prevents wasted snowflake IDs (PATTERN)
**Problem:** If `AddNode` generates an ID first and then property validation fails, the snowflake ID is consumed but never stored — a gap in the ID sequence.
**Solution:** `NewPropertySlice(props)` runs before `g.NextNodeID()`. Validation failures return early with no wasted ID.
**Rule:** In entity creation flows, validate all input before generating irreversible resources (IDs, timestamps).

---

## 2026-02-27 — Post-release review v3.0.7 (3 findings)

### Map iteration produces non-deterministic query results (MAJOR)
**Problem:** `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships` iterate Go maps and return slices in random order. Snowflake IDs are time-ordered, but that ordering is thrown away.
**Root cause:** Map iteration order is randomized by spec. Collecting from a hash-set into a slice without sorting produces shuffled results.
**Rule:** Any Store method returning entity slices from map iteration MUST sort by snowflake.ID before returning. This gives deterministic, time-dominant results with zero additional metadata. Write determinism tests: insert in reverse order, verify ascending ID order on retrieval. Test with multiple calls to prove idempotent ordering.

### Cascade delete must tolerate pre-deleted rels in ALL loops (BLOCKER)
**Problem:** `DeleteNode` outgoing loop hard-failed on `ErrRelNotFound`. The incoming loop correctly skipped it for self-loops, but the outgoing loop did not. Under concurrency, a goroutine can delete a relationship between the fetch and the cascade — causing `DeleteNode` to abort with a partially severed node.
**Root cause:** Only considered self-loops (incoming path) as a source of `ErrRelNotFound`. Did not consider concurrent external deletes.
**Rule:** In cascade-delete patterns, tolerate `ErrRelNotFound` in EVERY deletion loop, not just the one that handles self-loops. The reason is broader than self-loops: any concurrent operation can remove relationships from under you. Guard pattern: `if errors.Is(err, ErrRelNotFound) { continue }`.

### Per-call locking creates TOCTOU windows in multi-step operations (MAJOR)
**Problem:** `DeleteNode` calls `OutgoingRelationships`, then `DeleteRelationship` N times, then `DeleteNode`. Each call acquires/releases the `RWMutex` independently. Between any two calls, a concurrent `AddRelationship` can create a new edge to the node being deleted — dangling edge.
**Root cause:** Store has no transactional API. `RWMutex` serializes individual operations but not multi-step workflows.
**Rule:** Document TOCTOU limitations explicitly with `// TODO` comments when the Store lacks a transaction API. The Badger implementation MUST wrap cascade-delete in a single serialized `Update()` transaction. Never assume per-call locking provides multi-step atomicity.

---

## 2026-02-28 — BadgerStore LRU Cache + Async Persistence (v3.0.10)

### OCC contention on shared counter keys makes concurrent writes fail (BLOCKER)
**Problem:** `incrCounter()` in every Badger Update() transaction touched shared meta keys. Under Badger's OCC (Optimistic Concurrency Control), concurrent writes all conflict — 100 concurrent PutNode → 99 ErrConflict with no retry logic.
**Solution:** Replace with `atomic.Int64` fields on the BadgerStore struct. Counters are persisted by the flush loop piggyback, never inside any Badger transaction. Zero OCC contention.
**Rule:** Never put counters inside Badger transactions. Use `sync/atomic` for in-memory counters and persist them out-of-band.

### In-memory indexes must be rebuilt from Badger on startup (PATTERN)
**Problem:** With the LRU cache architecture, in-memory indexes are the source of truth while running. But they start empty. If the DB has existing data, queries would return empty results until entities are accessed.
**Solution:** `loadIndexes()` performs a single `db.View()` scanning all index key prefixes (keys-only) to rebuild `nodeIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx` and load counter values.
**Rule:** When switching from transactional reads to in-memory state, always rebuild the complete index on startup. A partial index is worse than no index.

### Entity locks must use ascending shard order for deadlock prevention (PATTERN)
**Problem:** Concurrent `LockTwo(A, B)` and `LockTwo(B, A)` on different goroutines can deadlock if locks are acquired in parameter order.
**Solution:** `LockTwo` normalizes to ascending shard index before acquiring. Same-shard: single lock. Self-loops (A→A): works correctly.
**Rule:** When locking multiple resources, always sort by a consistent ordering key (shard index, not entity ID) before acquiring. Unlock in reverse order by convention.

### Async flush means tests must call Flush() or Close() before cross-DB reads (PATTERN)
**Problem:** With async batch writes, data written via `PutNode()` is in the LRU cache and write buffer but not yet in Badger. Tests that close and reopen the DB to verify persistence will find empty data.
**Solution:** Tests call `bs.Flush()` or `bs.Close()` (which triggers final flush) before reopening. In-memory reads (cache-first) work immediately without flush.
**Rule:** Tests verifying durable persistence must explicitly flush or close before reopening. Tests verifying in-memory behavior work immediately.

### Tombstones prevent stale reads after delete (PATTERN)
**Problem:** Without tombstones, deleting an entity from the in-memory index would cause a cache miss → Badger read → returning the deleted entity (if the delete hasn't been flushed yet).
**Solution:** `MarkDeleted()` sets a dirty tombstone in the cache. `GetNode`/`GetRelationship` return `ErrNotFound` on `cacheDeleted` without ever touching Badger.
**Rule:** In cache-first architectures with async persistence, always use tombstones for deletes. A cache miss must not fall through to stale durable storage.

### CollectDirty must not modify state — use version-aware MarkFlushed (BLOCKER)
**Problem:** `CollectDirty()` both returned dirty entries AND cleared their dirty flags. During async flush, a concurrent writer could dirty an entry between CollectDirty and WriteBatch.Flush(). The newly-dirtied entry had its dirty flag stripped by CollectDirty, so under eviction pressure the unflushed entity vanished from cache — catastrophic data loss.
**Root cause:** Combining "read" and "mutate" in one operation creates a race window in any async pipeline.
**Rule:** `CollectDirty()` must be read-only — return snapshots with monotonic version stamps (`dirtyVer uint64`). Separate `MarkFlushed(map[ID]uint64)` only clears dirty on entries whose version still matches. Re-dirtied entries (higher `dirtyVer`) survive untouched. This is the same pattern as optimistic locking: check-and-set, not blind-set.

### Flush retry must not replay stale writes over newer ones (MAJOR)
**Problem:** On WriteBatch failure, `flush()` prepended failed ops (`append(ops, bs.pending...)`) back into the pending buffer. If a concurrent write added a newer version of the same key, the prepend placed the older value AFTER it — replaying in FIFO order meant the stale value overwrote the fresh one.
**Root cause:** Using a slice (ordered) for a buffer that needs last-write-wins semantics.
**Rule:** Use a `map[string]writeOp` for pending writes — natural last-write-wins dedup. On retry, `requeueOps()` only re-adds failed ops if no newer version already exists in the current pending map. Never prepend/append failed ops blindly.

### Cascade-delete error path must scrub ALL indexes (MAJOR)
**Problem:** `DeleteNodeCascade()` removed the node from `nodeIDs` before calling `getNodeLocked()` to retrieve label tokens for `labelIdx` cleanup. If `getNodeLocked()` failed (corrupted/missing entity data), the error path left ghost entries in `labelIdx` — permanent in-memory index pollution pointing to a deleted node.
**Root cause:** The cleanup path assumed getNodeLocked always succeeds after nodeIDs confirms existence.
**Rule:** When an entity's existence is confirmed in the index but its data is unreadable, the error path must still scrub ALL indexes. For nodes: scan all `labelIdx` entries for the node ID. This is O(labels) — acceptable on the error path. Never leave index entries pointing to deleted entities.

---

## 2026-02-28 — Async Flush Hardening Round 2 (v3.0.12)

---

## 2026-02-28 — Pre-release code review v3.0.13 (5 findings)

### Bare `continue` in query loops swallows corruption errors (MAJOR)
**Problem:** `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships` used bare `continue` on any `GetNode`/`GetRelationship` error. Index orphans (ErrNotFound) were correctly skipped, but I/O and corruption errors were silently eaten — data corruption becomes invisible.
**Root cause:** Copy-paste of `continue` without distinguishing sentinel errors from real failures. Same pattern already fixed in `DeleteNodeCascade` but not retrofitted to query methods.
**Rule:** Every error-handling `continue` in a loop must check for the specific sentinel first. `if errors.Is(err, ErrNodeNotFound) { continue }` — any other error propagates. A bare `continue` on error is always a code smell.

### sync.Once vs nil-guard for idempotent Close() (MAJOR)
**Problem:** `Graph.Close()` used `g.closeFn = nil` for idempotency. Two concurrent `Close()` calls race on the nil check and nil assignment — `-race` fires. The `BadgerStore.Close()` already used `sync.Once` correctly, but `Graph.Close()` didn't.
**Root cause:** Different idempotency patterns across the same codebase. `closeFn = nil` is thread-safe only under a mutex — `sync.Once` is the correct zero-coordination pattern.
**Rule:** Always use `sync.Once` for idempotent shutdown methods. Never rely on nil-guarding a function pointer across goroutines. Audit all `Close()` methods in a package for consistent patterns.

### Counters must be in the same atomic batch as data (BLOCKER)
**Problem:** `flush()` called `wb.Flush()` for data, then `persistCounters()` in a separate `db.Update()` transaction. Process crash between the two lines commits entities without updating counters. On restart, `loadIndexes()` reads stale counters — permanent size lie.
**Root cause:** Treating "persist counters" as a separate step instead of part of the atomic data batch.
**Rule:** Move counter keys (`meta/node_count`, `meta/rel_count`) into the same WriteBatch as entity ops. Snapshot counters under `idxMu.RLock()` alongside the pending swap. Delete `persistCounters()` entirely — counters have no independent lifecycle. If they can't go in the batch, they shouldn't exist.

### Cascade delete must propagate non-ErrRelNotFound errors (MAJOR)
**Problem:** `DeleteNodeCascade` swallowed ALL errors from `deleteRelLocked()`, not just `ErrRelNotFound`. If a rel had corrupted msgpack data (unmarshal error), the loop silently skipped it — no `writeOpDelete` queued for type/adjacency/entity keys. The corrupted rel permanently strands in Badger pointing to a deleted node.
**Root cause:** `continue` without asserting the error type. "Tolerate already-deleted" was over-generalized to "tolerate anything."
**Rule:** In cascade-delete loops, only tolerate `errors.Is(err, ErrRelNotFound)`. Any other error (corruption, I/O failure) must propagate immediately. Failing loudly on unreadable data is better than silently creating dangling references.

### Cache eviction must not restart from Back() per eviction (BLOCKER)
**Problem:** `evictClean()` used nested loops — outer loop checked `len > capacity`, inner loop scanned from `Back()` to find one clean entry. After evicting it, the outer loop restarted the inner from `Back()`. Under dirty-heavy pressure (N dirty entries), each Put triggers a full O(N) scan. Over K puts, total work is O(K*N) = O(N²).
**Root cause:** Restarting the inner scan from scratch after each successful eviction.
**Rule:** Single pass from `Back()` to `Front()`, evicting clean entries as found. Track position with a local pointer. When `len <= capacity`, stop. O(N) worst case regardless of dirty/clean ratio. Never restart a scan from the beginning of a data structure when a continuation pointer suffices.

### Close() must flush even when flushLoop was never started (MAJOR)
**Problem:** When `FlushInterval == 0` (InMemory mode or disabled-flush tests), `flushLoop` is never spawned and `flushDone` is pre-closed. `Close()` passed through `<-flushDone` immediately, then called `persistCounters()` and `db.Close()`. The pending buffer was silently dropped.
**Root cause:** Assuming flushLoop's final flush was sufficient. Didn't account for the code path where flushLoop never existed.
**Rule:** `Close()` must explicitly call `flush()` after `<-flushDone`. If flushLoop ran, the second flush is a no-op (pending is empty). If flushLoop was never started, this is the only flush. Always code defensive close paths — never assume a goroutine exists to do cleanup.

---

## 2026-03-01 — External Review Analysis (v3.0.13 → v3.0.14)

Two contradictory external reviews were cross-checked against the actual codebase. Below are the confirmed bugs and root-cause analyses.

### Background loop errors must never be silently discarded (MAJOR)
**Problem:** `flushLoop()` uses `_ = bs.flush()` at badgerstore.go:892,895 with `#nosec G104` annotations. If Badger fails persistently (disk full, permission error, corruption), the error vanishes. The pending buffer grows unbounded until OOM kills the process. No log entry, no metric, no backpressure signal.
**Root cause:** Treating background loops as "fire and forget." The re-queue logic (`requeueOps`) gave false confidence that "everything is handled internally." The `#nosec` annotation actively suppressed the linter that would have caught this. Background error handling was conflated with "the data will retry" — but observability was ignored entirely.
**Why it survived 13 rounds of review:** Every review checked correctness of the retry path (requeueOps) and confirmed that data isn't lost on transient failure. Nobody asked "what happens if the failure is permanent?" or "who is notified?" The `#nosec` annotation short-circuited the one tool that would have flagged it.
**Rule:** Never use `_ = fn()` in background goroutines. At minimum, log the error with `slog.Error`. For persistence loops, add: (1) slog logging on every failure, (2) exponential backoff instead of blind ticker retry, (3) a hard cap on `len(pending)` to apply backpressure to writers. The `#nosec G104` annotation is only valid when the error is genuinely unactionable (e.g., `io.Closer` on read-only cleanup) — never for write operations.

### Entity pointers must not be shared between cache and caller (MAJOR)
**Problem:** `Graph.AddNode` creates a `*types.Node`, passes it to `PutNode` which stores the pointer in `nodeCache.Put(id, n)` (badgerstore.go:361), then returns the same pointer to the caller (graph.go:269). Cache and caller hold the same pointer. If the caller mutates the node via `SetProperty`, the cached entity is silently corrupted. `MemoryStore` has the identical pattern — `ms.nodes[id] = n` stores the raw pointer. `GetNode` returns the cached pointer directly (badgerstore.go:389, memorystore.go:45).
**Root cause:** The "pure-data struct" design principle (Node has no locks, is a value container) was correctly applied to the type itself. But the **store boundary** was not treated as a trust boundary. The defensive-copying principle was applied to Node *accessors* (`Properties()`, `AllLabelTokens()`) but not to the **store ingestion and retrieval path**. Nobody asked "who else holds a reference to this pointer after PutNode?"
**Why it survived 13 rounds of review:** All reviews focused on Node's accessor methods (defensive copies) and the store's index correctness. The pointer aliasing between caller and cache was invisible because the Graph API doesn't expose mutation-after-creation as a first-class operation. Tests never exercised "add node, then mutate the returned node, then read from cache" because the API doesn't encourage it. The bug requires a caller to violate the implied contract (treat returned entities as read-only) — but that contract is nowhere documented.
**Rule:** Store boundaries are trust boundaries. `PutNode`/`PutRelationship` must deep-copy the entity before caching: `bs.nodeCache.Put(id, n.DeepCopy())`. Alternatively, `GetNode`/`GetRelationship` can deep-copy on retrieval. Pick one side — never share pointers across the boundary. When a type is designed as "pure data, no locks," the system that caches it is responsible for isolation. Add a test: `AddNode → mutate returned node → GetNode → verify cached node is unmodified`.

### Multi-step mutations must be all-or-nothing, not mutate-as-you-go (MAJOR)
**Problem:** `DeleteNodeCascade` (badgerstore.go:810-817) calls `deleteRelLocked(relID)` in a loop. Each call modifies in-memory indexes (`relIDs`, `typeIdx`, `outIdx`, `inIdx`) and queues `writeOp` entries. If the Nth call fails (non-ErrRelNotFound — e.g., corrupted msgpack data), relationships 1..N-1 are already deleted from indexes but the node and remaining relationships are still alive. The graph is left in a permanently split state. No rollback mechanism exists.
**Root cause:** Incremental mutation was chosen for simplicity — each `deleteRelLocked` is a self-contained unit. But a loop of self-contained mutations without rollback is only safe when every iteration is guaranteed to succeed. The only failure mode is corrupted data on disk, which is rare but not impossible. The v3.0.11 lesson ("cascade-delete error path must scrub ALL indexes") fixed the *node* cleanup path but didn't address the *relationship* loop's partial-mutation risk.
**Why it survived 13 rounds of review:** Reviews focused on the error *type* (`ErrRelNotFound` vs others) and index cleanup. The partial-mutation scenario requires a specific failure (corrupted rel data mid-cascade) that existing tests don't simulate. The fix for v3.0.11 (scrub labelIdx on getNodeLocked failure) addressed a symptom of the same class of bug but didn't generalize the solution.
**Rule:** Multi-step mutations on shared state must use a two-phase pattern: (1) **preflight phase** — read all data needed for the operation, fail fast if any read fails, mutate nothing; (2) **apply phase** — apply all changes as a single block with no error exits. For `DeleteNodeCascade`: collect all rel data via `getRelLocked` first; if any fails, return the error immediately with zero state changes; then apply all deletes in a non-failing loop. This is the same pattern as database transactions: validate → commit.

### Close() must preserve all errors, not just the first (MINOR)
**Problem:** `BadgerStore.Close()` (badgerstore.go:1049) uses `if e != nil && err == nil { err = e }` for `db.Close()`. If `flush()` already returned an error, the `db.Close()` error is silently dropped. A failing disk could cause both `flush()` and `db.Close()` to error — only the flush error surfaces.
**Root cause:** Ad-hoc multi-error handling. Go's `errors.Join` (available since Go 1.20) wasn't used.
**Rule:** When a shutdown sequence has multiple fallible steps, use `errors.Join(err, e)` to preserve all errors. Never use `if err == nil { err = e }` — it's a lossy pattern that drops secondary errors.

### Cascade returning nil on data corruption hides failures (MINOR)
**Problem:** `DeleteNodeCascade` (badgerstore.go:820-840) handles `getNodeLocked()` failure by scanning all `labelIdx` entries to clean up, then returns `nil`. The caller has no idea the node's data was unreadable. Corruption is silently absorbed.
**Root cause:** The v3.0.11 fix for ghost entries in `labelIdx` (lessons.md line 198-201) correctly prioritized "never leave orphaned index entries." But it over-corrected by returning success — the cleanup is correct, but hiding corruption from the caller prevents alerting and diagnosis.
**Rule:** When an operation succeeds structurally (indexes cleaned, node removed) but encounters data corruption, return a wrapped error: `fmt.Errorf("graph: cascade completed with corrupt node data: %w", err)`. The caller can log/alert while knowing the operation completed. Distinguish "operation failed" (rollback needed) from "operation completed with warnings" (cleanup succeeded but corruption detected).

---

## 2026-03-01 — v3.0.14 Bug Fix Implementation

### Adding DeepCopy breaks pointer-identity tests across the codebase (PATTERN)
**Problem:** After adding `DeepCopy()` to Put/Get paths, tests in `store_test.go` and `graph_test.go` that asserted `got == n` (pointer equality) failed. These tests assumed the store returns the exact same pointer.
**Rule:** When adding defensive copying to a store layer, grep the entire test suite for pointer comparisons (`!= n`, `!= r`, `returned different pointer`, `returned same pointer`) and update them to compare by ID or field values. Pointer identity tests are incompatible with copy-on-store semantics.

### Extract mutation-only helpers when splitting read+write operations (PATTERN)
**Problem:** `deleteRelLocked` was a single function doing read (getRelLocked) + mutations. The two-phase cascade fix needed the mutation part without the read. Copy-pasting the mutations would create duplication.
**Solution:** Extract `deleteRelByInfo(info relDeleteInfo)` containing only the mutation logic. `deleteRelLocked` becomes read + call helper. `DeleteNodeCascade` preflight reads, then calls the helper directly.
**Rule:** When refactoring to two-phase operations, separate the "get data" step from the "apply mutations" step into independent functions. The mutation function should accept pre-read data and perform no reads — guaranteeing it cannot fail mid-mutation.

---

## 2026-03-01 — Tutorial Implementation

### Never write code against an API you haven't fully analyzed (BLOCKER)
**Problem:** Started writing tutorial code assuming `Graph` had a `Store()` accessor method and that adjacency queries were available through Graph. Neither is true. The first attempt produced code that wouldn't compile.
**Root cause:** Jumped to implementation without exhaustive API analysis. Read `graph.go` but didn't verify every assumption before writing code.
**Rule:** Before writing ANY code that consumes an API (tutorials, integration code, test harnesses), catalog every public method on every relevant type with full signatures. Verify: (1) method exists, (2) parameter types match, (3) return types match, (4) unexported wrapper types are handled correctly. Document the API surface, THEN write code. Never assume a method exists because it "should."

### API gaps should be fixed, not worked around (PATTERN)
**Problem:** `OutgoingRelationships` and `IncomingRelationships` existed only on the `Store` interface — no Graph passthrough. The initial instinct was to inject the store and keep a separate reference, but this breaks the principle that Graph is the sole interface for external code.
**Solution:** Added `Graph.OutgoingRelationships(nodeID, typeName)` and `Graph.IncomingRelationships(nodeID, typeName)` as passthroughs with string→token resolution (empty string = all types, same convention as `NodesByLabel`). This maintains the Graph as the sole external API.
**Rule:** When external code needs Store functionality, add a Graph-level passthrough rather than exposing the Store. The Graph layer owns string resolution and should be the only interface external packages touch. Read-only queries don't need entity locks, so passthroughs are safe.

---

## 2026-03-01 — Phase 1d: Bulk Query Methods (v3.0.15)

### Adding interface methods requires all implementations to compile (PATTERN)
**Problem:** Adding 4 methods to the `Store` interface immediately broke compilation — `BadgerStore` didn't satisfy the interface. MemoryStore tests couldn't even run until BadgerStore was implemented.
**Solution:** Implemented all Store implementations in the same step rather than TDD-ing one at a time. Both MemoryStore and BadgerStore were implemented together so the code compiles before any tests run.
**Rule:** When extending a Go interface, all implementations must be updated in the same step. Plan for this when scoping TDD cycles — "write test, make it pass" requires the code to compile first, which means all interface implementations need at least stub methods.

### Bulk query patterns mirror existing index query patterns exactly (PATTERN)
**Problem:** Could have invented new patterns for AllNodes/AllRelationships, but the codebase already had `NodesByLabel` demonstrating exactly the right approach: snapshot IDs under lock, release lock, fetch via public Get methods, sort results.
**Rule:** Before implementing a new Store method, find the closest existing method and replicate its pattern. For MemoryStore: `NodesByLabel` (line 300) — `RLock`, iterate set, DeepCopy, sort. For BadgerStore: `NodesByLabel` (line 730) — snapshot IDs under `idxMu.RLock`, fetch via public `GetNode`, `errors.Is(err, ErrNodeNotFound)` continue, sort. Consistent patterns reduce review burden.

### Missing IDs should be skipped, not errored (DESIGN DECISION)
**Problem:** `GetNodesByIDs` could either return an error on missing IDs or silently skip them. Both are defensible.
**Decision:** Skip missing IDs — consistent with `NodesByLabel`'s orphan-skip pattern and more useful for bulk callers (export, snapshot) that don't want one missing entity to fail the entire batch.
**Rule:** Bulk query methods that accept caller-provided IDs should skip not-found entries rather than erroring. This matches the established orphan-skip convention in `NodesByLabel`/`RelationshipsByType` and is more practical for callers doing partial reads.

### Pre-existing test timeouts are not caused by your changes (PATTERN)
**Problem:** `make test-race` timed out on `TestBadgerStoreRecoveryAfterAbruptShutdown` — a pre-existing Badger WriteBatch deadlock unrelated to bulk query methods. Initial reaction was to investigate whether the new code caused the failure.
**Rule:** When a CI-style test fails, check (1) is the failing test one you wrote or modified, and (2) does it fail on the main branch too. If the answer to both is no, document it as pre-existing and verify your changes separately with a targeted test run.

---

## 2026-03-01 — Gap Tests & Tutorial 005

### FlushInterval: 0 gets overridden to 100ms for on-disk stores (BLOCKER)
**Problem:** `TestBadgerStoreRecoveryAfterAbruptShutdown` used `FlushInterval: 0` to prevent auto-flushing, but `NewBadgerStore` coerces zero to `defaultFlushInterval` (100ms) when `!cfg.InMemory`. The flush loop was running, and `close(stopCh)` triggered flushLoop's shutdown flush which persisted the "unflushed" node — defeating the test's purpose.
**Root cause:** Config defaulting logic: `if flushInt == 0 && !cfg.InMemory { flushInt = defaultFlushInterval }`. Zero is not "disabled" — it's "use default."
**Rule:** To prevent auto-flushing in on-disk tests, use a very large `FlushInterval` (e.g., `10 * time.Minute`), not zero. To simulate crash data loss, clear the pending buffer (`bs.pending = make(map[string]writeOp)`) before calling `Close()` — this makes the shutdown flush a no-op. Never assume zero means "disabled" in config structs without checking the defaulting logic.

### Badger has a 1MB MaxValueSize limit for WriteBatch (MAJOR)
**Problem:** `TestBadgerStoreLargeStringProperty` used a 1MB string (`1<<20`). After msgpack serialization with node metadata overhead, the total value exceeded Badger's 1048576-byte `MaxValueSize`. `Flush()` → `WriteBatch.SetEntry()` failed.
**Root cause:** Badger's default value size limit isn't documented in our codebase. Msgpack adds ~40 bytes of overhead per node, pushing a 1MB string over the limit.
**Rule:** Large value tests must account for serialization overhead. Use 500KB or less to stay safely under Badger's 1MB default. If truly large values are needed, configure `badger.Options.ValueLogFileSize` and `MaxValueSize` explicitly.

### AddNode returns *types.Node, not *graph.Node (PATTERN)
**Problem:** Tutorial 005 used `*graph.Node` in helper functions and struct fields. Node is defined in `pkg/types`, not `pkg/graph`. The code wouldn't compile.
**Root cause:** Assumed the Node type was in the same package as the Graph API without verifying the import path.
**Rule:** Before writing code that uses API return types, verify the exact package. `AddNode` returns `*types.Node`, `AddRelationship` returns `*types.Relationship`. Always check function signatures, not assumptions. This was already a lesson from "Never write code against an API you haven't fully analyzed" but was violated again in a tutorial context.
