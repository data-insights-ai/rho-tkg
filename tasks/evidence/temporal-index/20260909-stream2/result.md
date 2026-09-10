# R0–R2 evidence: indexed temporal replay on the current rho-tkg ingest API

Date: 2026-09-09. Plan: tasks/plan-temporal-index-ingestion.md (amended today:
producers 1 and 4 only, batch-size sweep, exact durability-syscall counting).

## Revision and environment

- Source: main at `fdddc3dd7a3a3f778437079f5243d899e6e2e0a7` (v4.35.0) with 37 dirty
  paths that are NOT this stream's: gosec `#nosec` annotations across core/index/store,
  workflow/Makefile/docs edits, the go directive bump 1.26.1→1.26.7 with the toolchain
  line removed. Left untouched. This stream added only `bench/temporalreplay/`
  (harness, contract tests, run.sh, table.py) and this evidence directory.
- Host: 32 CPUs, 124 GiB, NVMe root (`/dev/nvme1n1p5`), go1.26.7 linux/amd64.
- Graph options (the AI-SOC consumer's own, `engine/convergent.go OpenConvergent`):
  BadgerDir on NVMe, `SyncWrites: true` (Badger `WithSyncWrites(true)`, rho flush per
  mutation), `LabelIndexOnDisk`, `PropertyIndexOnDisk`, `AdjacencyIndexOnDisk`,
  `DisablePlannerStats`, `AllowSelfLoops`, one `UniqueCurrent` constraint per label on
  `key`. Session options: `Sync: true`; `Concurrent: true` for the concurrent door;
  `DeclareLabels/DeclareRelTypes` set (see the durability defect below).
- Watchdog: every run under `timeout 900` and `ulimit -v 16 GiB` (run.sh). No run hit either.

## R0 — consumer-shaped workload (bench/temporalreplay/workload_test.go)

Deterministic (seed 20260909) 3,000 rows, 750 distinct facts (skewed: 10% of facts take
50% of rows), 8 hot hosts shared by every producer + 64 private hosts per producer,
strictly increasing event time with ms jitter. Per row: Asset get-or-create, Fact
get-or-create OR last_seen = max(last_seen, ts) (read-modify-write), Occurrence node,
OCCURRENCE_OF and OBSERVED_ON relationships, Inbox entry. tx door allocates a global
per-tenant admission sequence in the same commit (the consumer's `admitInTx`); the
session doors can only allocate a lane-local sequence — the Session API has no
read-modify-write, so identities are resolved by a read OUTSIDE the group and a losing
racer is repaired after Submit (reconcile = re-read every create, re-link).

Durable output digest is computed after Close + reopen from the relationships (not
from labels): per fact the exact multiset of occurrence times, last_seen, host counts.
At one producer all three doors produce the identical digest `76d0d1df0923`; at four
producers tx and strong-b1 agree (`3cba6f33dacd`). The p1 and p4 workloads differ
only in host/source naming (rows are assigned per producer), so compare within a
producer count.

## R1 — sweep (rows=3000, facts=750; timing pass, and an identical counting pass under strace -c)

| mode | producers | batch | rows/s | commit p50 ms | p99 ms | peak RSS MiB | dir MiB | unique retries | lost RMW facts | msync | fsync | write+pwrite | msync/row | digest | phase rows/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| concurrent | 1 | 1 | 46.3 | 22.3 | 27.6 | 209 | 5 | 0 | 0 | 30216 | 11 | 39 | 10.07 | 76d0d1df0923 | [46, 45, 48, 47] |
| concurrent | 1 | 1024 | 44.4 | 22897.7 | 23234.4 | 136 | 5 | 0 | 0 | 30216 | 11 | 103 | 10.07 | 76d0d1df0923 | [45, 48, 41] |
| concurrent | 1 | 128 | 46.4 | 2764.9 | 2950.0 | 133 | 5 | 0 | 0 | 30216 | 11 | 34 | 10.07 | 76d0d1df0923 | [45, 45, 50, 47] |
| concurrent | 1 | 16 | 47.0 | 336.3 | 381.1 | 208 | 5 | 0 | 0 | 30216 | 11 | 48 | 10.07 | 76d0d1df0923 | [48, 47, 48, 45] |
| concurrent | 4 | 1 | 46.1 | 85.3 | 139.4 | 132 | 5 | 0 | 1 | 16702 | 11 | 78 | 5.57 | 30a6f249ebe1 | [45, 47, 48, 45] |
| concurrent | 4 | 1024 | 46.9 | 62727.6 | 63936.2 | 140 | 5 | 4 | 67 | 16880 | 11 | 45 | 5.63 | 661e548fbd14 | [12, 8680, 3753, 621] |
| concurrent | 4 | 128 | 45.3 | 10933.3 | 12726.6 | 207 | 5 | 19 | 33 | 17756 | 11 | 68 | 5.92 | 521a7b69b0ec | [28, 61, 44, 61] |
| concurrent | 4 | 16 | 45.5 | 1408.5 | 1640.6 | 132 | 5 | 12 | 10 | 17358 | 11 | 76 | 5.79 | f91f05564d38 | [43, 44, 46, 49] |
| strong | 1 | 1 | 109.7 | 10.4 | 11.6 | 133 | 5 | 0 | 0 | 22754 | 11 | 22 | 7.58 | 76d0d1df0923 | [116, 110, 107, 106] |
| strong | 1 | 1024 | 65.4 | 14962.7 | 19715.6 | 126 | 5 | 0 | 0 | 16760 | 11 | 74 | 5.59 | 76d0d1df0923 | [92, 68, 48] |
| strong | 1 | 128 | 153.3 | 847.5 | 889.5 | 130 | 5 | 0 | 0 | 16802 | 11 | 40 | 5.60 | 76d0d1df0923 | [169, 155, 148, 146] |
| strong | 1 | 16 | 135.9 | 115.4 | 240.2 | 131 | 5 | 0 | 0 | 17130 | 11 | 34 | 5.71 | 76d0d1df0923 | [130, 143, 137, 133] |
| strong | 4 | 1 | 47.2 | 56.2 | 334.7 | 133 | 5 | 0 | 0 | 21824 | 11 | 27 | 7.27 | 3cba6f33dacd | [40, 38, 62, 57] |
| strong | 4 | 1024 | 96.2 | 26396.9 | 31198.6 | 128 | 4 | 3 | 44 | 14020 | 11 | 28 | 4.67 | 2aa1b7b80c04 | [87, 58, 155, 156] |
| strong | 4 | 128 | 88.3 | 5709.2 | 8463.2 | 204 | 5 | 15 | 26 | 15874 | 11 | 58 | 5.29 | eb270c356cfc | [93, 90, 59, 149] |
| strong | 4 | 16 | 82.3 | 763.5 | 1487.1 | 130 | 5 | 17 | 9 | 16672 | 11 | 16 | 5.56 | be6d882dc501 | [89, 82, 78, 80] |
| tx | 1 | 1 | 71.8 | 14.4 | 16.4 | 154 | 6 | 0 | 0 | 36238 | 11 | 51 | 12.08 | 76d0d1df0923 | [72, 72, 72, 71] |
| tx | 1 | 1024 | 87.8 | 11601.1 | 11686.7 | 136 | 5 | 0 | 0 | 30244 | 11 | 22 | 10.08 | 76d0d1df0923 | [88, 88, 88] |
| tx | 1 | 128 | 84.9 | 1455.3 | 1941.8 | 135 | 5 | 0 | 0 | 30286 | 11 | 26 | 10.10 | 76d0d1df0923 | [84, 88, 81, 87] |
| tx | 1 | 16 | 87.0 | 183.6 | 201.8 | 134 | 5 | 0 | 0 | 30614 | 11 | 47 | 10.20 | 76d0d1df0923 | [86, 88, 87, 87] |
| tx | 4 | 1 | 56.7 | 57.6 | 191.5 | 154 | 6 | 0 | 0 | 36582 | 11 | 25 | 12.19 | 3cba6f33dacd | [55, 67, 44, 68] |
| tx | 4 | 1024 | 96.6 | 23720.9 | 31060.9 | 208 | 5 | 0 | 0 | 27700 | 11 | 26 | 9.23 | 3cba6f33dacd | [86, 99, 101, 102] |
| tx | 4 | 128 | 93.4 | 5381.3 | 7151.7 | 132 | 5 | 0 | 0 | 28478 | 11 | 22 | 9.49 | 3cba6f33dacd | [89, 94, 95, 94] |
| tx | 4 | 16 | 87.0 | 727.9 | 849.1 | 136 | 5 | 0 | 0 | 30454 | 11 | 44 | 10.15 | 3cba6f33dacd | [83, 88, 88, 88] |

msync is Badger v4's WAL sync (`MS_SYNC` over the memtable mapping) under
SyncWrites; fsync (11 per run) is registry/manifest/open/close. write+pwrite are
the vlog/SST writes. Columns "unique retries" = Submit groups that returned
ErrUniqueViolation and were reconciled; "lost RMW facts" = facts whose durable
last_seen is BELOW the true maximum of their occurrences.

### Findings, each measured

1. **Durability operations dominate wall time, and batching does not reduce them.**
   CPU profiles of tx and strong at p1/b128 show 2.1 s and 1.4 s of CPU in 101 s and
   63 s of wall (2%); the process is waiting. Direct msync wall latency (strace -T,
   strong p1 b128, 300 rows): n=1758, p50 3.3 ms, p99 17.6 ms, Σ 6.5 s of a ~6.6 s
   run. Physical syncs per row are constant across batch sizes: tx ≈10 msync/row,
   strong ≈5.6, concurrent ≈10. A "commit group" is NOT one physical durability
   operation — every store door inside the group (each relationship create, each
   update, the adjacency/index writes) flushes on its own under SyncWrites. The plan's
   R3 condition ("durability cost dominates") is therefore TRUE.
2. **Best existing door: strong Sync session, ONE producer, batch 128 — 153 rows/s,
   1.8× the tx door (85–88 rows/s) — on one physical mechanism: 5.6 vs 10 msync/row.**
   Concurrent sessions are the slowest door at every configuration (44–47 rows/s,
   ≈10 msync/row: the self-applying path flushes per entity).
3. **Four producers never beat one.** strong p4 b128 88 rows/s vs p1 153; tx p4 ≈ p1;
   concurrent p4 = p1. Consistent with AI-SOC P4. The 8/16 runs were dropped by the
   amendment and would not have changed the decision.
4. **Session doors lose read-modify-write under parallel producers.** strong p4: 9–44
   facts with a stale last_seen; concurrent p4: 1–67; tx p4: 0 (the RMW sits inside the
   transaction). At ONE producer the session doors lose nothing. Unique-violation
   retries only appear with several producers (12–19 groups per run).
5. **RETRACTED (follow-up, same day): "strong b1024 falls to 65 rows/s".** Two re-runs
   of strong p1 b1024 give 153.6 and 155.4 rows/s with flat phases (165/150/147 and
   168/152/147), equal to b128 (151–153). Its CPU profile is the same shape as b128
   (990 ms CPU in 19.8 s wall, 5%; skiplist put, memmove, syscall). The sweep's 65
   and 55 rows/s (timing and counting pass, 09:09–09:11 UTC) sit inside a host-wide
   slowdown that is visible INSIDE the preceding b128 counting run too (phases
   165→148→87→44) and that also hit the first post-fix control runs (tx 29.7, strong
   50.9 rows/s at 11:4x local, load avg 2.9 with the parallel streams' test runs;
   the same binaries gave 69/151 minutes later at load 1.7). Mechanism: msync latency
   on the shared NVMe under concurrent writers, not anything in the b1024 path. Batch
   size above 128 buys nothing and costs commit latency (p50 6.5 s), so 128 stays
   the recommendation.
6. **Retained history:** phase throughput (4 quarters of each run) is flat for tx and
   strong-b128 over 3,000 rows / ~750 facts; no growth-with-history effect is visible
   at this scale. Not a claim about millions of rows.
7. **Peak RSS 126–209 MiB, index directory 4–6 MiB** for every configuration.

### Contract tests (bench/temporalreplay/contract_test.go, run with -race)

| plan case | test | result |
|---|---|---|
| Two workers create the same logical identity | TestTwoWritersSameIdentity (strong, concurrent) | PASS — one identity; loser gets ErrUniqueViolation for the whole group; loser's occurrence/inbox are committed, its relationships short-circuited; Submit does not say which intent failed, so the adapter re-reads every create and re-links; no occurrence lost |
| Repeated updates on one fact | TestRepeatedUpdatesRetainExactTimesAndMultiplicity | PASS — 5 occurrences incl. two at equal time, exact times, fact version chain retains each increasing maximum and none for the equal-time repeat |
| Invalid operation among valid operations | TestInvalidOpAmongValid_SyncSubmitReportsAndKeepsSurvivors | PASS — Submit returns the error (checkpoint cannot advance), survivors committed, violator not duplicated |
| Kill before/after durable acknowledgment | TestKillBeforeAndAfterDurableAck (tx, strong-without-declare) | PASS — SIGKILLed child after 6 acks; every acked batch complete on reopen; replay of all batches converges to one copy per row with all relationships (adapter must repair rows whose node survived a kill without its relationships — done via degree check) |
| Same, concurrent door | TestKillBeforeAndAfterDurableAck/concurrent | SKIPPED — blocked by the defect below |
| Async repeated waits and eviction | TestAsyncRepeatedWaitNeverTurnsFailureIntoSuccess | FAILS on the current API (second WaitApplied on a failed token returns nil, prune-on-read); gated behind TKG_REPLAY_R4=1 because the adapter uses SYNC sessions only — documented R4 hazard, not a blocker |
| ENOSPC / flush failure, backpressure, snowflake restart | — | not written this round (the existing core suites cover backpressure and flush-failure attribution: TestIngestBackpressureBounded, TestIngestFlakyFlushThroughPipeline) |

### THE demonstrated gap — one red test: `TestDeclaredRelTypesAreDurableBeforeAck`

`IngestOptions.DeclareRelTypes` (and the concurrent door's declare-on-prepare)
interns names through `predeclareVocabulary`, which calls
`persistRegistriesIfDirtyLockedPanicSafe` — a no-op unless a PREVIOUS registry save
failed (`registryDirty` is only set on failure, core.go:1480/1486). The batch and
concurrent apply paths then see an already-known token and never checkpoint it. A
relationship under a declared type is acknowledged by a Sync Submit while its type
token is not durable. After SIGKILL the row survives (Rels().All = 1) but
CountByType/OutgoingDegree for that type = 0 — every acknowledged relationship loses
its type. Reproduced for strong+DeclareRelTypes and for concurrent without any option.
The tx door is not affected (checkpointRegistriesOnCommit persists on size change).
Strong WITHOUT DeclareRelTypes is not affected (the create path persists new names).

Narrow fix (NOT implemented here, per R2): predeclareVocabulary must persist when it
interned anything, mirroring `registriesChangedSinceBegin` on the tx door. One
function, one test already red.

## R2 — mode decision

**Initial AI-SOC replay adapter: ONE writer, strong Sync session (`Sync: true`,
`Concurrent: false`), batch ≈128 rows, NO `DeclareRelTypes` until the red test is
green.** It passes every correctness case at one producer, is 1.8× the transaction
door on the identical durable workload, and its acknowledgement is a real durability
point except for the declared-vocabulary defect above. Keep the transaction door for
read-modify-write that must be atomic with its read (the global admission counter,
any future multi-writer convergence) — the session door has no MERGE and measurably
loses maxima under parallel producers. Do not use the concurrent door: slowest,
per-entity flushing, and the declare-on-prepare defect.

**No new lanes, no new batch API.** What R1 demonstrates is instead the R3 condition
(contract specified below, follow-up of the same day):
≈5.6 physical syncs per row on the best door, batch-invariant, at ~1–3 ms each on
NVMe. Neither door can exceed ~150 rows/s on this hardware; the corpus-scale lever is
a group-commit contract that makes one submitted group ONE durable operation across
every store door it touches (node batch, relationship creates, updates, on-disk
index and adjacency writes, registry) with the accepted/failed/durable distinctions
the plan lists — "never by turning off SyncWrites". R3 is now unblocked by evidence;
its specification is the next step, not this one.

## R3 contract — group commit as ONE durable operation (specified; IMPLEMENTED the same day, see "R3 implemented" below)

**Measured basis (followup/strong-128.txt, strong-256.txt; tx-*.txt).** rho `flush()`
calls per 128-row strong group, attributed by a temporary env-gated hook (reverted):

| store door ← core caller | flushes per 128-row group | mechanism |
|---|---|---|
| `putNodesBatchInternal` ← `putGeneratedNodesBatchPreEncoded` | 1 | the node batch already commits once |
| `putRelationship` ← `putGeneratedRelationship` | 256 | one per relationship (2 per row) — the batch path creates rels through the per-entity door, each calling `flushIfNeeded` |
| `replaceNodeWithHistoryRouted` ← `updateNodePreparedInternal` | 126 | one per property update (the last_seen maximum) |
| `CreatePropertyIndex`, registry saves, Close | 5 + 1 + 1 per graph, not per group | |

Total ≈383 rho flushes ≈ 3.0 per row; Badger issues ≈1.9 msync per rho flush (5.6
msync/row). The tx door additionally flushes every node put (`putNodeRouted`, 273 per
group ≈ 5.1 flushes/row, 10.1 msync/row). `TestGroupCommitIsOneDurableOperation`
measures the whole open + one 128-row group + close under strace: **833 sync calls
against a budget of 32 — RED**, the executable pin for this section.

**Contract.** For one `Session.Submit` of one group G (strong door; the concurrent
door is out of scope — it is per-entity by design):

1. *Durable-before-success.* `Submit` (Sync) returns nil, or `WaitApplied(token)`
   returns nil, only after every accepted mutation of G AND the metadata it
   references — registry tokens (labels, rel types, property keys), entity rows,
   history versions, label/property/adjacency index entries, change-log records and
   counters — are in ONE committed Badger WriteBatch that has been synced. The
   acknowledgement is the durability point; nothing acknowledged may be lost to
   SIGKILL. (Today true per entity; violated for declared vocabulary until this
   morning's fix.)
2. *Failure cannot advance the consumer cut.* If any part of G is rejected, `Submit`
   returns the error and the consumer must not advance its durable replay cursor past
   G. Survivors of a partially failed group are committed and reported as today
   (keep-survivors), but the acknowledgement carries a per-intent outcome
   (`[]BatchError` keyed by the prepared entity ID) so the adapter can repair without
   re-reading every create — the gap `TestTwoWritersSameIdentity` documents.
3. *Three distinct properties, never conflated.* (a) **atomic visibility**: readers
   see all of G or none of it — provided by the strong applier's exclusive lock today;
   (b) **partial batch error**: which intents were rejected — reported per intent;
   (c) **physical durability**: G is on disk — exactly ONE sync per applied commit
   group (the applier may coalesce several submitted groups into one commit group;
   then one sync covers all of them and every submitter is acked after it). A
   commit-group flush count of 1 is the invariant; `Store.PendingWriteCount` reaching
   0 after the ack is the observable.
4. *What changes in the store, and only that.* The batch apply path holds
   `flushIfNeeded` for the duration of the commit group and issues one `flush()` at
   the end (the same idxMu/flushMu snapshot discipline `flushIndexLocked` already
   implements), instead of once per `putRelationship` / `replaceNodeWithHistory`.
   Registry saves stay write-ahead (before the batch, as now). Change-log LSNs stay
   minted per group and land in the same WriteBatch (already the case for the node
   batch). SyncWrites semantics are unchanged for every non-batch door.
5. *Never by turning off SyncWrites.* A run that reaches the budget with
   `SyncWrites=false`, a longer `FlushInterval`, or async acks does not satisfy the
   test; the child in `TestGroupCommitIsOneDurableOperation` opens the graph with the
   consumer's SyncWrites config and that must stay.
6. *Kill tests that must pass afterwards, unchanged:* `TestKillBeforeAndAfterDurableAck`
   (every acked batch complete, replay idempotent) and `TestDeclaredRelTypesAreDurableBeforeAck`.

Expected effect, from the counts: ≈3.0 → ≈1 rho flush per 128-row group on the strong
door, i.e. ~5.6 → ~0.05 msync/row; the ~1–3 ms msync then stops bounding throughput
and the CPU profile (2–5% today) becomes the limit. That is a forecast from the
measured mechanism, not a measurement.

## Checklist

- [x] R0 benchmark on the current API, backend options and revision recorded
- [x] R1 tx vs strong vs concurrent, 1 and 4 producers, batch 1/16/128/1024, identical
      durable output verified by digest, exact msync/fsync/write counts, latency, RSS,
      phase (retained-history) throughput
- [x] R2 decision (above); the demonstrated gap (declared vocabulary durability) FIXED
      the same day under R2 with its SIGKILL pin green
- [x] R3 contract specified above; IMPLEMENTED (René's approval, same day) —
      `store.GroupCommitCapability`, Badger implementation, strong-applier wiring;
      pin green at 2.0 sync calls per group steady state
- [ ] R4 — not required (sync-only adapter); hazard pinned behind TKG_REPLAY_R4
- [ ] R5 — qualification of the chosen mode inside the AI-SOC adapter (AI-SOC stream)

Raw artifacts: `sweep/` (every JSON result, strace -c per configuration, env.txt,
the msync -T latency trace). Reproduce: `bench/temporalreplay/run.sh <out> 3000 750`,
`python3 bench/temporalreplay/table.py <out>`.

## R3 implemented — group commit on the strong door (same day, after approval)

**Change (rho-tkg only, no commit):** `store.GroupCommitCapability`
(`BeginGroupCommit` / `EndGroupCommit`) in `pkg/graph/store/capabilities.go`; Badger
implements it (`groupCommit` atomic flag; `flushIfNeeded` returns without flushing inside
the window; `CommitLogScope` leaves its records in `pendingLog` for the window's flush;
`EndGroupCommit` clears the flag and issues ONE `flush()`). Core: `BatchBuilder.groupCommit`
is set ONLY by the strong applier (`applyCommitGroup`); `Batch.Execute` opens the window
after taking `txMu`+`mu` and the change-log scope, closes it after `CommitLogScope`, and
an `EndGroupCommit` error is a whole-batch error (`result == nil`, every coalesced
submitter's token fails, ops stay pending in the store). Panic/early-return paths close
the window in the deferred cleanup. The `g.Batch()`, transaction and concurrent doors
never set the flag; memory/tiered/sharded stores do not implement the capability and
keep per-mutation behaviour. SyncWrites stays on everywhere.

**Tests added (all -race green):**

| property | test | what it pins |
|---|---|---|
| physical durability = one operation | `bench/temporalreplay.TestGroupCommitIsOneDurableOperation` | strace child at 0/1/3 groups: open+close 61, first group +10 (incl. registry write-aheads), steady state **2.0 sync calls per 128-row group** (budget 4) — was 833 for one group |
| atomic visibility | `bench/temporalreplay.TestGroupCommitAtomicVisibility` | concurrent `CountByLabel` sampler over 200 groups of 64 never sees a non-multiple of 64 |
| partial batch error ≠ durability | `core.TestGroupCommitPartialErrorStillCommitsOnce` | a broken relationship among valid intents: Submit errors, survivors committed, exactly one window |
| failure cannot advance the cut | `core.TestGroupCommitFailureFailsEverySubmitterAndCannotAck` | injected EndGroupCommit failure: Sync Submit and async WaitApplied return it, AppliedSeq still advances (no wedge), next group recovers |
| only the strong door | `core.TestGroupCommitWindowIsOpenedOnlyByTheStrongApplier` | batch/tx/concurrent doors open 0 windows; strong door 1 per submitted group |
| store methods directly (Rule 1) | `badger.TestGroupCommitHoldsSyncFlushesUntilEnd`, `…RowsAreDurableAfterEnd`, `…EndOnClosedStoreErrors`, `…EndClearsWindowEvenOnError` | hold/flush/reopen/closed-store/flag-cleared |
| SIGKILL after ack, replay idempotent | `TestKillBeforeAndAfterDurableAck` (tx, strong, concurrent, declared vocabulary), `TestDeclaredRelTypesAreDurableBeforeAck` | unchanged, green with the group commit on |

Gates: `go vet ./...` clean; `go test -short ./...` green except
`tiered.TestCheckAndCleanArchiveNodeDestination_PurgesOrphanedAdjacency`, which fails 1 in 3
on a clean `HEAD` worktree as well (pre-existing flake, not touched); `-race` on
`internal/core`, `store/badger`, `pkg/graph`, `pkg/graph/ingest`, `bench/temporalreplay` green.

**Measured after (groupcommit/; 3,000 rows, 750 facts, p1 unless stated, same binary
config as the sweep; tx re-run as the control on the same host at the same time):**

| mode | producers | batch | rows/s | commit p50 ms | p99 ms | peak RSS MiB | dir MiB | unique retries | lost RMW facts | msync | fsync | write+pwrite | msync/row | digest | phase rows/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| strong | 1 | 1024 | 6009.0 | 163.2 | 179.0 | 124 | 4 | 0 | 0 | 82 | 11 | 12 | 0.03 | 76d0d1df0923 | [6520, 5719, 5834] |
| strong | 1 | 128 | 8815.2 | 14.2 | 15.8 | 130 | 4 | 0 | 0 | 124 | 11 | 12 | 0.04 | 76d0d1df0923 | [8465, 9103, 8565, 9085] |
| tx | 1 | 1024 | 83.8 | 11766.0 | 13049.7 | 134 | 5 | 0 | 0 | 30244 | 11 | 28 | 10.08 | 76d0d1df0923 | [87, 78, 87] |
| tx | 1 | 128 | 66.0 | 1702.3 | 4143.1 | 134 | 5 | 0 | 0 | 30286 | 11 | 21 | 10.10 | 76d0d1df0923 | [64, 63, 55, 88] |
| strong | 4 | 128 | 7946.7 | 66.6 | 97.7 | 129 | 4 | 17 | 30 | 158 | 11 | 14 | 0.05 | bd84a0c7e78c | [6888, 8117, 8168, 8585] |

- **strong p1 b128: 153 → 8,815 rows/s (57×); msync per row 5.6 → 0.04; commit p50 848 ms → 14 ms;
  digest identical (`76d0d1df0923`).** b1024: 6,009 rows/s, p50 163 ms — b128 stays the knee.
  Strong without declared vocabulary: 8,543 rows/s (same digest). tx control unchanged
  (66–84 rows/s, 10.1 msync/row, same digest; its first phases ran under load 4.5 right after
  the race gate).
- Sync calls: steady state 2 per group (one Badger WriteBatch = two msync). The absolute
  per-run count is 124–128 msync for 24 groups: 70 open/close/index/registry + ~2×24.
- **Residuals.** (1) The multi-producer picture is unchanged in kind: strong p4 b128 reaches
  7,947 rows/s but still loses read-modify-write maxima (30 facts) and needs 17 unique
  reconciliations — the adapter stays one writer; identity RMW that must be atomic with its
  read stays on the tx door. (2) The tx door itself is untouched and still flushes per mutation
  (10.1 msync/row); giving it the same window is a separate decision. (3) A CPU profile of the
  new steady state has not been taken; the limit has moved from msync to CPU and is not yet
  named. (4) `CLAUDE.md`'s stable capability list does not yet mention
  `GroupCommitCapability` — that file carries someone else's uncommitted edits, so the line
  is left for its owner (CHANGELOG [Unreleased] carries the entry).
