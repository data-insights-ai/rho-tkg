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
