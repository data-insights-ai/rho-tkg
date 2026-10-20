# v5 scale analysis (2026-10-16), worktree v5-takeover-wt2 @ fc76fb77

## Probe
scale_probe_test.go + engine.go (write hook) via `go test -overlay scale/overlay.json -run TestScaleProbe ./pkg/graph/`
(no repo file touched). Mix 60% nodes (label + 3 string props), 40% rels (18 string props), distinct values.
6000 ops, 1 op/commit, in-memory: 91.8 records/op, 33 KB logical/op, RetainedRecords 568,612; per-commit time flat
(~3.4 ms) from commit 500 to 6000; key tree (0x10) pages bounded 2.6-4.9 KB (splits).
Batch 8: 84.9 rec/op (only key tree/presence/descriptor/commit rows amortise).

| family | rec/op | B/op | per | avoidable? |
|---|---|---|---|---|
| 0x08 component, 0x09 directory, 0x0b patch | 3 x 10.6 | 7.7 KB | each component cell (presence, label, each prop) gets its own key+directory+patch page | yes: inline single-piece cells (-21) |
| 0x19 equality posting | 9.0 | 1.0 KB | every property value, unconditional (component_indexes.go:226-244) | yes: opt-in index (-9) |
| 0x05 value, 0x06 bucket, 0x07 bucket member | 3 x 7.8 | 2.2 KB | each distinct value (catalog.go:935) | partly |
| gr6 identity counter, gr7 identity record | 2 x 7.8 | 2.3 KB | each distinct value, partition identity authority (partition_identity.go:107-135) duplicates value dictionary in single partition | yes (-16..-31) |
| 0x03 entity, 0x04 life, 0x1a revision, 0x11 descriptor, 0x18 label, 0x13 incident, 0x15 presence, 0x10 key tree | ~8 | ~7 KB | per entity / per commit | mostly required |
| pkg/graph instant + allocator | 2/commit | | per commit | no |
Rel create ~180 records, node create ~45.

## Caps
- RetainedApplicationRecords counts ALL immutable versions (application.go:16-21, measure :345-374 = writes+3 per install);
  1<<20 is pkg/graph's choice (pkg/graph/engine.go:243-251); validate allows up to 1<<40 (application.go:59,
  generation 1<<40 application_generation.go:~99). Not an architectural ceiling, a quota.
- Records are live, not garbage: retention GC (CollectApplicationRetention application_retention_collect.go:276,
  driven only by internal/dist/graphapply/retained_graph_gc.go) frees superseded versions only; create-heavy import
  produces almost none. And pkg/graph Known()/At(cut) read old ApplicationViews (engine.go:633-660) -> GC would
  expire them (ErrSnapshotExpired).
- Second hidden ceiling: raft log never trimmed. pkg/graph never calls ReclaimApplication (application.go:716-742;
  only internal/dist/replica/driver.go:372,819,893 does). Persist refuses at LogCount > MaxRetainedEntries=1,000,000
  commits or 256 MiB log (storage.go:905-908, DefaultLimits storage.go:36-37).
- Pebble tuned tiny: CacheBytes 4 MiB, MemTable 1 MiB (storage.go:37); max 1 GiB / 64 MiB.
- Policy must match exactly on reopen (storage.go:200): changing caps needs a store-format/policy version.

## Projection 5M ops (ai-soc 2.5 GB)
~73 rec/op real -> ~365M records (needs cap ~2^29+), ~5.6 KB/op on disk (80 MB/14.3k ops) -> ~28 GB (11x v4).
Time at 157-176 ops/s -> 8-9 h if per-op cost stays flat on a 28 GB pebble (unproven beyond 20k ops).
