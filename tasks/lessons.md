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
**Rule:** Any Store method returning entity slices from map iteration MUST sort by snowflake.ID before returning. This gives deterministic, chronological results with zero additional metadata. Write determinism tests: insert in reverse order, verify ascending ID order on retrieval. Test with multiple calls to prove idempotent ordering.

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
