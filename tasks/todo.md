# v3.0.61 Pre-Release Hardening

## Tasks

- [x] Task 1: `CloneBytes` helper + deep-copy `Signature` in integrity structs
- [x] Task 2: Clone `sig` at input boundary in `extractProvenance`
- [x] Task 3: Clone `Signature` in wire encode/decode
- [x] Task 4: Fix `SetEventBus`/`SetAsyncEventBus`/`GetEventBus` data race
- [x] Task 5: Fix `SetTemporalConstraints`/`AddTemporalConstraint` data race
- [x] Task 6: Move event dispatch outside mutation locks
- [x] Task 7: Config/contract fixes (docs + ShardWindow validation)
- [x] Task 8: Test coverage for uncovered critical paths
- [x] Task 9: TODO tags + CHANGELOG
- [x] Verification: `go vet` + `go test -race` all green
- [x] Documentation updated (CHANGELOG.md, AGENTS.md)
