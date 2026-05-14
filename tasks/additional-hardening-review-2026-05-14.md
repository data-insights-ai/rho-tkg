# Additional Hardening Review - 2026-05-14

Scope: read-only follow-up after W1 was fixed and W2-W6 were dismissed. No code changes and no tests/builds were run in this pass.

## Result

No new confirmed defects were found in the inspected surfaces.

This pass focused on areas where a correctness bug would be high-impact even if the common path passes: IO import rollback, tiered cross-shard mutation rollback, transaction/batch API shape, and custom property serialization/copying.

## Inspected Surfaces

### IO import rollback

Status: no confirmed issue.

Evidence:

- `pkg/graph/internal/core/import.go:72` stages the complete stream before taking the graph write lock, then replays under `c.mu`.
- `pkg/graph/internal/core/import.go:122` creates an import rollback log before replay; `:128-132` invokes rollback on any replay error.
- Current node/rel import paths validate checked wire, registry token membership, property limits, duplicate stream records, and capture rollback state before store writes (`pkg/graph/internal/core/import.go:482-583`).
- History import paths use conflict checks before `Put*Version`, preventing silent overwrite on diverging re-import (`pkg/graph/internal/core/import.go:519-636`).
- Rollback captures pre-import current rows and history suffixes (`pkg/graph/internal/core/import.go:700-833`), restores rels before nodes, then restores registries (`pkg/graph/internal/core/import.go:836-883`).

Conclusion: the suspected hybrid/partial import failure mode was not confirmed. The current code has explicit snapshots, idempotent duplicate checks, history trim fallback, and registry restore.

### Tiered archive/restore and cross-shard rollback

Status: no confirmed issue.

Evidence:

- Archive and restore pin source/destination shards, preflight destination node/relationship placement, write the destination node, then migrate relationship placement with a committed-move rollback log (`pkg/graph/store/tiered/tieredstore_write_archive.go:31-210`).
- Relationship placement planning pins endpoint owner shards and skips orphan relationship IDs discovered from stale adjacency (`pkg/graph/store/tiered/tieredstore_write_archive.go:273-338`).
- Destination preflight rejects live collisions and purges/rejects destination adjacency before moving rows (`pkg/graph/store/tiered/tieredstore_write_archive.go:341-354`).
- Tiered relationship batch deletes validate/deduplicate IDs, classify by owner shard, pre-read relationship snapshots, and roll back committed deletes through `putRelationshipLocked` (`pkg/graph/store/tiered/tieredstore_write_rel.go:535-677`).
- Tiered node batch deletes validate/deduplicate IDs, preflight connected relationships across every shard before mutation, pre-read rows, and roll back prior shard buckets on later failure (`pkg/graph/store/tiered/tieredstore_write_node.go:513-610`).

Conclusion: the specific cross-shard partial-write concerns I checked are already guarded by preflight and rollback paths.

### Transaction and batch API shape

Status: no confirmed issue.

Evidence:

- `TxAPI.Run` and `RunContext` reject nil callbacks, wrap the whole transaction with deferred rollback, and release the graph write lock even when the callback panics (`pkg/graph/subapi.go:47-123`).
- `BatchAPI.New`, `Run`, and `RunContext` nil-guard the API/core and callbacks; `Run` drops the builder without `Execute` when the build callback returns an error (`pkg/graph/subapi.go:142-205`).
- `BatchBuilder.Execute` serializes the builder, re-checks graph closure after acquiring the graph write lock, and defers lock/event-buffer cleanup (`pkg/graph/internal/core/batch_execute.go:27-62`).
- Batch node create handles label rollback, partial post-write errors, unavailable queued endpoints, and keeps returned skeletons aligned with committed rows (`pkg/graph/internal/core/batch_execute.go:75-208`).
- Batch relationship create refreshes endpoint hashes under endpoint locks, restores queue-time state on non-commit paths, and rolls back newly allocated rel-type tokens around store failures (`pkg/graph/internal/core/batch_execute.go:212-360`).

Conclusion: I did not find a remaining tx/batch API parity or lock-release bug in the inspected paths.

### Custom property copy and wire format

Status: no confirmed issue.

Evidence:

- `RegisterPropertyStructType` rejects untyped nil, non-structs, types without `HashableValue`, types without `DeepCopier`, and wire-name collisions (`pkg/types/property_registry.go:123-164`).
- Runtime validation checks the actual value form implements both custom-property contracts, so pointer-receiver-only types do not get accepted in value form (`pkg/types/property_registry.go:175-212`).
- Wire encoding records custom type name and pointer/value shape, msgpack-encodes the value, then requires a decode round trip whose `HashBytes` matches the source (`pkg/graph/internal/storeutil/wire_value.go:406-455`).
- Custom `HashBytes` panics are converted to errors during wire round-trip validation (`pkg/graph/internal/storeutil/wire_value.go:458-465`).
- Wire decode reconstructs only registered custom types and preserves pointer/value shape (`pkg/graph/internal/storeutil/wire_value.go:472-557`).

Conclusion: the custom-property persistence path has the required registration, copy-shape, wire-shape, and hash-stability guards for the reviewed failure modes.

## Residual Notes

- This is a code-inspection result only. I did not run tests, race tests, benchmarks, or fault-injection harnesses.
- I intentionally did not re-list W1 or the dismissed W2-W6 items unless this inspection contradicted them. It did not.
