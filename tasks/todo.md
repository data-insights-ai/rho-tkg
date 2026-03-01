# tkg-v3 — Current Tasks

### Confirmed Design Observations (document only)

- [ ] DOC: `FlushInterval: 0` = "use default" in disk mode (cannot disable periodic flush)
- [ ] DOC: No `context.Context` — intentional for embedded library
- [ ] DOC: `time.Time` rejected — by design, use `Instant`

### Coverage Gaps (confirmed)

- [ ] MINOR: wire.go `propertyTypeTag` at 56% — uint, uint8-32, float32 branches untested
- [ ] MINOR: wire.go `toInt64`/`toUint64` at ~50% — integer conversion branches untested
- [ ] MINOR: wire.go `normalizeIntegersRecursive` at 40% — backward-compat fallback untested
- [ ] MINOR: badgerstore.go `flush()` at 73% — WriteBatch error recovery paths untested
- [ ] MINOR: ImportNames — no validation for empty/duplicate entries in persisted data

## Status

Library at v3.0.14. Coverage: 92.9%.
