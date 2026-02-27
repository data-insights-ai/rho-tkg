# tkg-v3 — Current Tasks

## Completed: Pre-release review rounds 2-4 (v3.0.5)

- [x] Explicit snowflake initialization params — WithEpoch, WithNodeBits, WithStepBits
- [x] Zero-alloc `HasLabelTokenRaw(uint16)` on Node, `HasTypeTokenRaw(uint16)` on Relationship
- [x] Allowlist property validation (arrays, funcs, chans, unsafe pointers rejected)
- [x] Recursion depth limit (maxPropertyDepth=32, ErrMaxDepthExceeded)
- [x] Registry empty-string rejection (ErrEmptyName)
- [x] Off-by-one capacity fix (token 65535 assignable)
- [x] `deepCopyValue` nil short-circuit
- [x] Shared-pointer intent documented on `Temporal()`/`Integrity()`
- [x] Stale docs fixed (doc.go, error messages, Set() comment)
- [x] Resolve out-of-range tests, registered-but-wrong-type test
- [x] CHANGELOG merged v3.0.4 + v3.0.5 into single v3.0.5
- [x] Verification: make ci green, 97.1% coverage, zero 0% public methods

## Completed: Sync README.md and CLAUDE.md with v3.0.5

- [x] README: PropertySlice description updated (allowlist + depth limit)
- [x] README: Token 0 bullet updated with `*Raw` variants
- [x] README: 4 new invariant bullets (zero-alloc, depth limit, ErrEmptyName, shared-pointer)
- [x] CLAUDE.md: propertyslice.go row updated
- [x] CLAUDE.md: Token interning invariant updated with `*Raw` variants
- [x] CLAUDE.md: Renamed + rewrote "No pointers" → "Allowlist property validation"
- [x] CLAUDE.md: 2 new invariants (shared-pointer, zero-alloc)
- [x] CLAUDE.md: ErrEmptyName added to registries section
- [x] Verification: `make test` green

## Next up

- Phase 2 continued: msgpack wire formats (nodeWire/relWire), Badger persistence
- Registry persistence to Badger (meta/label_tokens, meta/reltype_tokens)
