# rho-tkg backlog

**Single todo/roadmap file.** Done work is **dropped** from here — `CHANGELOG.md`
is the source of truth for what shipped, why, and measured numbers. Keep only
genuinely open items (and short pointers to closed epics / reopen criteria).
Item numbers are stable (code comments cite them); a closed item keeps its number in the Closed table.

This file tracks only rho-tkg work. External orchestration and product-layer
RPCs that already have local primitives here are out of scope.

**Severity legend:** CRITICAL = crash / data loss / replica divergence / silent
corruption. HIGH = silent wrong answer or reachable correctness bug. MEDIUM =
concurrency edge / perf cliff / contract inconsistency. LOW = smell / doc drift.
TEST-GAP = real behavior unverified (may hide a bug). FEATURE = plausible
capability not yet built. DO-NOT-BUILD = decided against; reopen criteria only.

**Open work (verified against code, CHANGELOG and git at v4.55.3, 2026-10-15):** no reproduced CRITICAL item. This table is the ONE open-work list; `tasks/todo.md` points here. Items consumers asked for are built when they ask; the rest is parked. Closed items are one line in the Closed table below.

| # | Type / severity | Item | Status |
|---|---|---|---|
| 0 | FEATURE | Column segments on NVMe (ADR-0011) | S0-S3, S5 done (S2/S5 in v4.39.0, S3 + P7 in v4.40.0); S4 merge, S6, S7 and sub-points (a), (b), (d)-(i) open |
| 1 | MEDIUM (improvement, deferred locking) | Import-under-a-scope | open; `IO().Import` still takes only `c.mu` (no `c.txMu`), stopgap in place |
| 2 | TEST-GAP | Old BACKLOG 22: six TEST-GAP research items (22a-22f) | open; prioritise only when a consumer needs the confidence |
| 3 | MEDIUM (perf, unmeasured) | Temporal adjacency scan cost (v4.35.0 follow-up) | open; measure first |
| 4 | HIGH? / MEDIUM? (traced, not reproduced) | Temporal semantics review 2026-09-24: traced findings | open; each needs a failing two-phase test first; code unchanged at the cited sites (`resolveOpenEndInstant`, `validInstantAfter` in `core/temporal.go`) |
| 5 | MEDIUM (perf) | P7 row compaction follow-ups | open; options (a)-(d) unbuilt (P7 itself shipped in v4.40.0) |
| 7 | FEATURE (limitation) | Caller-instant delete of a row with a scheduled close | open; `DeleteWithTx` refuses `ValidTo >= t` with `ErrTxOrder` (R8, v4.44.0) |
| 10 | FEATURE | Tiered store: remaining work after composite + rel temporal indexes | open: (b) rel property indexes (`CreateRelPropertyIndex` declines on tiered) and node `TemporalCandidateCapability`; (a) done v4.51.6 |
| 13 | MEDIUM | Ingest applier attributes group errors by numeric entity id, not by kind | open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`) |
| 16 | MEDIUM (contract) | Range folds under an interval: predicate-anywhere or value at the resolved version? | open; decision pending; behaviour documented in the v4.44.0 CHANGELOG |
| 20 | LOW (TEST-GAP, perf) | Effective-read cost: RelAtTx benchmark targets unmeasured | fixes 1a (v4.44.1), 1b (v4.46.0), 1c (v4.47.0), 2a (v4.46.0) shipped; only the RelAtTx benchmarks remain |
| 23 | LOW (perf, unmeasured) | `changeFeedPage` prefetches past its page | open; `PrefetchValues = true`, no `opts.Prefix` (`badgerstore_changelog.go:648-659`) |
| 25 | MEDIUM | A future close followed by a delete changes the answer at an earlier pin | open; `stampDeleteTombstone` still clamps (`core/temporal.go:133-137`) |
| 26 | LOW (known limit) | Old chains with collided versions keep answering in the old version order | open; schedule only if a consumer holds such chains |
| 31 | LOW | Bulk and point as-of disagree with the cascade oracle on version gaps | open; fix when a consumer holds such chains |
| 37 | LOW | Tiered error-rollback restore leaves a window with empty history | open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`) |
| 39 | LOW (not tested) | Per-entity compaction stubs survive `Admin().Reset` | open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`) |
| 40 | LOW (cosmetic) | A cascade resumption may end at an earlier life's own bound | open |
| 45 | MEDIUM (API) | Internal sentinels reach callers but have no exported alias; `ErrMixedNumericColumn` is outside the errors inventory test | open; found by the doc review of `docs/errors.md` 2026-10-10 |
| 46 | LOW (stale comments) | Code comments that cite ADRs no longer in `docs/adr/` or old lesson/B-numbers | open; still stale: `tieredstore_property_stats.go:19`, `bench/ingest_pipeline_test.go:13,101` (ADR-0005/0006), `badgerstore_property_stats.go:38` (lesson 62 vs 63), `store/store.go:30` and `docs/architecture.md` (B-numbers), `effective_timeline.go` header |
| 47 | LOW (unverified claims) | `NodesByLabelAt` vs K1; a RAM budget for the property sidecars | open; each needs a real check before it becomes work |
| 71 | FEATURE (sigma round 8, optional) | Valid-time hull per label / relationship type: earliest valid-from and latest valid-to of the current entities, exact and cheap | open, not built in round 8. No structure holds it exactly: a temporal index keeps per-ID envelopes that only widen (`Extend` unions, `Remove` is a no-op, deletes and excises leave them), so its hull is an upper bound over history, and only where a temporal index exists. Exact needs a per-label ordered multiset of valid-from and valid-to (a write cost on every node write: opt-in per label, as `TrackRelTypeDegrees`) or a walk. Design first |

**v5:** the next engine generation is planned on branch `v5` (`docs/v5/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md`, revised 2026-10-09 after `tasks/review-v5-plan-vs-code-20261009.md`; branch created 2026-10-09 from main `32568c4`). v4 keeps taking consumer features (decision René 2026-10-09, the earlier fixes-only rule is deleted). ADR-0011 S2+ waited for owner decision D6 in that plan. **DECIDED 2026-09-25 (René): S2 and S5 ship on v4 as v4.39.0**, because the AI-SOC cross-check needs them now (ADR-0011 §6 gate met: 24.0 / 24.8 / 25.7 B/HOP at 790 K / 3.15 M / 12.6 M); v5 carries the segments forward from this code.

---

## Open — v5 V0–V7 acceptance

Specification and evidence checklist: [PLAN.md §8](../docs/v5/PLAN.md#8-implementation-sequence).
Review [HANDOVER.md](../docs/v5/HANDOVER.md) at each integration checkpoint and
after branch fetches. Reconcile new entries with the reviewed PLAN before acting;
the handover does not replace phase acceptance gates or authorize release/main
publication. Last checked revision: `eaa339c3` (2026-10-12).
Handover release-tag correction: its proposed `v5/v5.0.0` prefix conflicts with
[Go's major-version-directory rules](https://go.dev/ref/mod#vcs-version).
For the declared module path, use `v5.0.0` when V7 is accepted and publication is
authorized; see [the release policy](../docs/stability.md#unreleased-v5-module).
The handover remains a historical review, not release authority.
Owner deployment decision, 2026-10-12: run distributed fault/scaling tests on
this machine with separate processes or Docker containers and isolated durable
storage, as specified in [PLAN §8](../docs/v5/PLAN.md#8-implementation-sequence).
External physical machines are no longer an acceptance prerequisite. Earlier
physical-host requirements below are superseded by this decision; historical
evidence limitations remain accurate. Keep the full logical topology, fault
and cost matrix, and label results as single-machine distributed tests.
Pinned fixtures: [consumer contracts](../docs/v5/reference/consumer-contracts.json)
and [migration obligations](../docs/v5/reference/consumer-migration.json) are
reviewed specification/inventory evidence; consumer integration acceptance stays open.
The [45-family v4→v5 mapping](../docs/v5-analysis.md#5-v4--v5-door-mapping)
and the manifests' current v4.51.5 source delta at `c23c8b04` distinguish public
successors, internal machinery and unresolved API contracts. Their consumer proofs belong to
V4/V6; historical acceptance pins below remain unchanged.
The integrated v4.51.0 source and V6 preflight pass [hosted CI at `3ede096d`](evidence/e17-ci-deadline-20261012/hosted-ci-3ede096d.json):
all eight applicable jobs, including 2,180 core and 451 graphapply names in
exhaustive race batches plus the other packages. Push-only lint is skipped;
local pinned lint evidence is separate. This checkpoint does not qualify
subsequent paired-fault or creation-factory changes.
The later main revision `dbd74cf4` (v4.51.1), identified by the external
[handover review](../docs/v5/HANDOVER.md), is integrated with updated source-only
consumer mappings. The [v4.51.1 migration preflight qualification](../tools/v5-migrate/evidence/main4511-993e89a6.json)
passes all ten narrow gates on Go 1.26.9, including race and pinned scanners;
coverage is 96.2%, with both public preflight functions at 100%. The source
manifest matches the integrated reader. [Hosted CI at `b2143582`](evidence/e17-ci-deadline-20261012/hosted-ci-b2143582.json)
also passes all eight applicable jobs for this integration, including the new
graph lifecycle factory: 2,182 core and 456 graphapply names pass exhaustive race
batches, together with the other packages. Push-only lint remains skipped and
requires its separate local evidence. This does not qualify the pending native
catch-up fix or close remaining phase gates. The v4.51.0 receipts and source
delta remain historical evidence.
Main `cddc0997` (v4.51.2) is integrated as a later source baseline: tiered
history across re-imported lives, ex-adjacency and valid-end bounded effective
scans. Its store marker is 3; row encoding is unchanged. The v4.51.1 preflight
qualification above remains historical: update the consumer mapping and qualify
the migration reader against this revision before claiming v4.51.2 acceptance.
[Hosted integration at `92c6415b`](evidence/e17-ci-deadline-20261012/hosted-ci-92c6415b.json)
passes all eight applicable jobs for this source baseline, including both
modules' normal/race, coverage and security gates. Its migration/comparison
jobs pass their existing scope; they do not establish the new consumer mapping,
destination import or subsequently integrated replacement activation.
Main `c23c8b04`/v4.51.5 is now source-integrated with scoped relationship
retention, state count/column cuts, zero temporal ordinals and history acceleration
obligations. Its [source receipt](evidence/main4515-integration-20261014/receipt.json)
records exact upstream CI/source equivalence. Existing retained-state preflight
cases now pass all ten narrow gates at `da5d32e6` ([receipt](../tools/v5-migrate/evidence/main4515-da5d32e6.json),
96.2%, both public functions 100%); new retention/refusal and consumer/destination
qualification remain pending. The ten-gate v4.51.4 migration receipt
and [three finite reader lanes](evidence/current-reader-9e805ddc-20261014/receipt.json)
remain immutable historical proof at `9e805ddc`, without whole-root/V6 acceptance.
Main `2a72753d`/v4.55.3 is now source-integrated, without runtime qualification.
The new reader baseline includes binary integrity hashes and newer store markers,
void pieces for valid-time excision, implicit validity from `TxFrom`, and newer
index/read capabilities, including sorted equality/composite postings and keyset
page push-down. Consumer source-delta manifests and the migration reader
must be refreshed for these contracts during V4/V6; their older pins and all
historical receipts remain unchanged. The main integration changes no V2 engine
or comparison paths. No compile, test, measurement or CI run accompanies it:
the owner's implementation-first sequence below remains in force.
Main `1b0a50a3`, including release `a08c0d52`/v4.56.0, is now source-integrated
without runtime qualification. Its persisted `Config.UntimedValidStart` keeps
untimed first-version policy separate from explicit validity; conversion is
one-way and resumable, marker 7 fences older writers, `ChangeMeta` replicates
the mode and export `uu` carries it. V4/V6 source-delta mapping and importer
qualification must preserve those policies, interrupted-conversion refusals,
current endpoint-life bounds and Reset migration markers. Earlier source pins
and receipts remain historical. No V2 runtime or CI gate was run for this merge.
Main `0fa37786`/v4.56.1 is source-integrated at `dee0c7f2`, preserving its
one-pass property-sidecar builds/open-image restoration and complete-history
temporal-envelope pruning, including re-created/re-typed relationship IDs.
All 26 affected v4 source/test blobs match upstream; no v5 source changed.
Consumer source-delta mapping and reader qualification remain V4/V6 work, and
the recorded upstream performance evidence is not v5 measurement or acceptance.
No compile, test, measurement or CI run accompanies this merge.
[Substrate evaluation](../docs/v5/reference/substrate-evaluation.json) is
research/execution-plan evidence only; no engine selection or V2/V3 acceptance.
The original five recovered `docs/v5/reference/` files are byte-verified and
reviewed as the accepted historical 16-case/52-assertion subset and bounded
design examples. They remain unchanged; this subset does not establish broader
V0 or production/distributed acceptance. The reviewed PLAN/DISCUSSION/RESEARCH
and reference JSON specify contracts/evidence, not completed engine gates.

| Phase | Status and remaining acceptance evidence |
|---|---|
| V0 | Specification accepted: independent criterion review and refreshed reference/design checks pass. Contracts, profile/unit/import policy, current-v4 delta and reproducible resource/dataset test profiles are reviewed below. Implementation, consumer execution and measured capacity retain their separate phase gates |
| V1 | Values/state accepted: unchanged 16/52 and revised rho-owned cases pass through typed primitives and `Plan`/`Project`, including immutable interpretation/native-axis roles, attached descriptor/source-revision history, graph-qualified refusals, independent state/complete-CDC capacity assertions and the pure Q/ms default-axis binding. Node/relationship retained-view exact sets, boundary/differential/fuzz/codec refusals and direct new-method coverage pass. Durable default registration is covered by the V2 checkpoint below; request/dependency installation and graph/cut integration remain V2, consumer adapters V4 and provenance-aware import V6 |
| V2 | Active, not accepted. Checked partition-local writes, same-graph cross-partition prepare/decision/resolve and certified current/historical graph reads are accepted on both declared partitions of one GraphID, with three disk-backed voters each, retained history, nonempty snapshot activation, failover and reopen. Remote E15/E16 and finite E17 range handoff pass with authenticated leader sources and six native voters. Default-axis designation, shared allocation and explicit local authority are preserved. E17 hosted CI passes after its scheduling correction. Remote E18 index-starting hydration and its hosted validation are accepted. Package 4 still requires broader ownership recovery, production identity reuse/fencing, application GC, remaining process/network/storage fault schedules and comparable transactional-KV fault/cost evidence with an explicit engine decision |
| V3 | Coordinate timeblock candidate independently validated (local commit `fa0c982`; scoped temporal+block coverage 95.7%, no exported method at 0%). Full engine seal/merge/recovery, paged structures and all-in budget/byte-ledger measurements remain open; resident-buffer prototype timings do not close V3 |
| V4 | Production read/change APIs, historical cross-door parity, gap-free leased feed handoff and pinned sigma access-contract build and nonempty mutation-then-historical tests (the current signature/empty smoke is insufficient) |
| V5 | Distributed access across 1/2/4/8 partitions, cross-edge/stall/rebalance/feed cases and sigma workload parity |
| V6 | Existing retained-state preflight cases narrowly qualify v4.51.5 at `da5d32e6` (ten gates, 96.2%, both public functions 100%). New retention/refusal cases, destination importer, consumer-repository migrations, retained-cut/CDC agreement and restore on a different topology remain open; historical qualifications are unchanged |
| V7 | Versioned contracts/limits, full CI/race/coverage/security, distributed matrix and consumer suites; measured comparative space/time acceptance against Neo4j/TigerGraph/Memgraph plus v4, with reviewed excess-cost exceptions |

### Weighted delivery packages (2026-10-16)

Deadline priority: finish V2 on 2026-10-16. All three implementation agents now
work only on V2; the authored V3 rational-block candidate is frozen and further
V6 work is parked. The explicit source-validity preflight subset is integrated,
but it does not close the current-reader migration package. The complete V2
measurement matrix remains outstanding and is a material deadline risk.

The original V0–V7 exits remain unchanged. The smaller packages below make completion visible; each is closed only with evidence for its stated scope. These are **new planning weights**, totaling 100 points per phase, not estimates of hours spent or time remaining. Only accepted packages earn points; partially authored or currently failing packages earn none. Historical receipts retain their exact source scope. There is no unweighted overall-project percentage.

| Phase | Accepted points | Remaining points | Current focus |
|---|---:|---:|---|
| V0 Specification | 100% | 0% | Accepted specification baseline |
| V1 Temporal values and state | 100% | 0% | Accepted values/state baseline |
| V2 Distributed correctness and engine decision | 42% | 58% | Finish demonstrated integration failures, remaining fault schedules, comparison and engine decision |
| V3 Compact engine | 10% | 90% | Rational block candidate frozen; V2 takes priority |
| V4 Public reads and changes | 0% | 100% | Public graph/read/change surface and actual Sigma adapter |
| V5 Integrated distributed access | 10% | 90% | Generalize finite foundations to integrated multi-partition access |
| V6 Migration and operations | 10% | 90% | Explicit valid-start preflight integrated; further V6 work parked |
| V7 Release qualification | 0% | 100% | Release, full comparative measurements and consumer acceptance |

Every open requirement is assigned to a package below. Detailed existing acceptance receipts and the V2 A–F queue remain authoritative for the underlying evidence; a package weight does not waive those requirements. V2.1–6 and V5.1 deliberately name finite foundations rather than claiming full fault-family or public-access acceptance. V6.1 is historical preflight only.

**V0 — Specification**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V0.1 Reviewed specification and independent models | 100% | accepted | Temporal concepts and rho/sigma boundary; units/profiles/import policy; consumer inventory; reference cases, failure outcomes and resource/dataset profiles. |

**V1 — Temporal values and state**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V1.1 Values/state acceptance | 100% | accepted | Exact coordinates/profiles/regions, reducers, ordering and descriptor preservation; reviewed corpus, differential/boundary/fuzz/codec and node/relationship historical parity. |

**V2 — Distributed correctness and engine decision**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V2.1 Replicated substrate baseline | 8% | accepted | Recorded native log/VFS, nonempty snapshot activation, durable reopen and bounded recovery baseline; broad fault campaigns remain V2.9. |
| V2.2 Local writes and allocation baseline | 8% | accepted | Graph genesis/default axis, partition-local writes, shared allocation and accepted finite P08 issuance/exhaustion evidence; broader reuse/fencing/reclamation remain V2.8. |
| V2.3 Cross-partition transaction kernel | 10% | accepted | Recorded two-partition/three-replica prepare/decision/resolve, retained originals, conflict fencing and atomic graph effects; remaining schedules remain V2.7/9. |
| V2.4 Certified read foundations | 8% | accepted | Accepted finite E15/E16 current/historical reads and E18 index-starting hydration with authenticated native sources and cuts. |
| V2.5 Finite range handoff baseline | 4% | accepted | Accepted finite E17 handoff, retained history and certified source/target lineage; interruptions/GC and general rebalance remain open. |
| V2.6 Selected recovery/storage components | 4% | accepted | Recorded P03/P06 and selected WAL/manifest/OPTIONS recovery components; evidence applies to its exact source/cases, not the full families. |
| V2.7 Remaining transaction fault schedules | 10% | active | Complete P01–P06 requirement-to-case coverage: outside-scope coordinator; prepare/fence ordering; partial installation; frozen participants/digest/decision; absent keys/ranges, temporal predicates and endpoint-life ABA; conflicts through failover. Selected rotated/reordered P01, P02 and P06 cases pass; P03/P04/P05 and state recovery still have open qualification. |
| V2.8 Ownership, identities and reclamation | 10% | active | P07/P09 interruptions during transfer/publication, production identity reuse and stale fencing, retry-payload refusal, snapshot/feed lineage, expiration and application GC; preserve original results and expired/unavailable versus empty. |
| V2.9 Composed network, scheduler and storage faults | 10% | open | Loss/duplication/reorder/asymmetry, isolated leaders/quorum/domain loss; pause/ticks/clock jumps; torn/short writes, EIO/ENOSPC/sync/rename/manifest/corrupt pages; bounded conflicting exploration followed by recorded-seed soak and independent oracles. |
| V2.10 Equivalent FoundationDB fault/publication evidence | 8% | open | Actual role/domain loss, unknown commits, pins/GC and chunk/manifest failures; complete histories/index/CDC, owned cleanup and all-client/server accounting under equivalent declared fault/durability semantics. |
| V2.11 Eight-workload functional comparison | 5% | open | All declared native/FDB workloads, both layouts, writers 1/8/32, native 1M multi-grant setup and bounded comparator state. Tiny one-writer KV/columns and eight-writer KV pairs pass only their finite cells. |
| V2.12 Complete measurement matrix and improvements | 10% | open | 10k/100k/1M facts, cross 0/10/50/100%, writers 1/8/32, five fresh repetitions, all declared workloads/layouts/transports: 11,520 paired cells / 23,040 engine runs. Record bytes, memory, latency, throughput, retries/recovery and complete outputs; correct measured regressions and remeasure affected scope. |
| V2.13 Written engine decision | 3% | open | Choose native versus FoundationDB from measured correctness, deployment, storage/time, maintenance and embedding evidence; record limitations and justified costs. |
| V2.14 Coordinated V2 acceptance | 2% | open | One integrated requirement/evidence audit and required current gates after implementation/correctness/measurement/improvement; all A–F closure rows accepted. |

**V3 — Compact engine**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V3.1 Integer coordinate-block candidate | 10% | accepted | Accepted bounded int64 Z/integer-valued Q candidate and three layouts at recorded source; this is not the integrated storage engine. |
| V3.2 Exact rational coordinate blocks | 10% | active | Fractional/wide canonical coordinates in bounded arenas/offsets with shared axis, exact membership and owned/borrowed access; canonicality/axis/limit/ownership adversaries. Standalone internal implementation started; integration remains later packages. |
| V3.3 Other typed blocks and MVCC memtable | 15% | open | Arena-backed mutations; typed values/dictionaries/ordinals; wide integers, regions, uncertainty/correlation and symbolic descriptors; compact point-only rows, occupancy, mixed/sparse layouts and materialized-row costs. |
| V3.4 Paged catalog and index integration | 15% | open | Wire compact blocks into identity/location/catalog/adjacency/revision/point/interval indexes; predicate pruning and possible-support bounds; long spans and distinct current/historical paths. |
| V3.5 Seal, merge and bounded recovery | 20% | open | Applied log to memtable/immutable segments/atomic durable manifest, merge and safe reclamation; retained cuts, multiplicity, knowledge and provenance; bounded replay; integrity-before-use and scrub; no second graph-truth WAL. |
| V3.6 Aggregate database budgets | 10% | open | Bound memtables/caches/dictionaries/indexes/locks/transactions/reads/changes/retained metadata together; spill/backpressure, concurrent partitions/scans and bounded catalog/open/manifest growth. |
| V3.7 Large-data correctness and byte ledger | 10% | open | 790K/3.15M/12.6M source rows with correct HOP denominators; seal/merge/crash/reopen oracles; heap/RSS/PSS/mmap/page-cache/disk/log/index/catalog/replica steady and peak costs, no per-fact resident growth and bounded replay; actual active-versus-ended-population gate including historical access. |
| V3.8 Physical choices and v4 fast path | 10% | open | Homogeneous/sparse/tagged thresholds, metadata amplification and catalog/backend candidates; equivalent durable single-partition v4 comparison, cold/warm/restart/compaction, five runs/dispersion and reviewed regressions. |

**V4 — Public reads and changes**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V4.1 Public graph, identity and write surface | 15% | open | Graph open/close, opaque IDs, create/correct/close/retract and 45-family successor-or-explicit-decline mapping. |
| V4.2 Scoped snapshot/session API | 20% | open | Fresh/Stable/At/After, cut/owner lifetimes, bounded rows/bytes, cancellation, borrow lifetime and resumable cursors. |
| V4.3 Typed projected access and negotiation | 20% | open | Lookup/Scan/Expand; label/type/property and native/residual capability negotiation; capabilities, statistics and ExplainAccess. |
| V4.4 Complete histories and cross-door parity | 15% | open | Timeline/Occurrences and named/generic/row/column exact sets; deleted/recreated entities, declared/effective views, phantom refusals and mutation-then-old-cut tests. |
| V4.5 Public changes and leased handoff | 20% | open | Changes/WatchChanges, atomic before/after selector entry/exit groups, gap-free scan-at-C to feed-after-C, replay/expiry/concurrency/backpressure. |
| V4.6 Pinned Sigma access adapter | 10% | open | Actual pinned access build, nonempty NodeByIDAt, column/ordinal/epoch/history/runtime/writer contracts and E09–E13/E21 input preservation; consumer-repository proofs. |

**V5 — Integrated distributed access**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V5.1 Finite two-partition access foundation | 10% | accepted | Accepted E15–E18 certified historical reads, remote hydration, incidence/canonical parity and finite handoff; arbitrary partition counts remain open. |
| V5.2 Locality and endpoint ownership | 15% | open | General 1/2/4/8-partition routing, one canonical relationship owner, synchronous in/out postings and placement/epoch/rebalance; any async alternative needs complete coverage/delta/fallback. |
| V5.3 Distributed temporal/history access | 15% | open | Selectors/projections/profiles and explicit declared/effective views; current/complete history, corrections/deletions/long spans/uncertain support; exact decline and statistics. |
| V5.4 Distributed read doors and pagination | 20% | open | Lookup/Scan/Expand/Timeline/Occurrences, deterministic merge ordering and cursors bound to cut, selector/profile versions and partition lineage; global row/byte/buffer limits, cancellation and backpressure. |
| V5.5 Cuts and snapshot-to-feed integration | 15% | open | Scoped modes, Fresh-cost choices, gap-free leased handoff, complete transaction envelopes/completed cuts, selector changes, deduplication/replay/expiry and rebalance lineage; no external exactly-once claim. |
| V5.6 Distributed acceptance matrix | 15% | open | All read/expand/feed doors at 1/2/4/8 partitions with three replicas, cross 0/10/50/100%, stalls/cross-edges/pagination/leader loss/rebalance, local and injected 1/20/100ms RTT plus observed jitter, complete outputs and aggregate costs. |
| V5.7 Pinned Sigma workload parity | 10% | open | Owning-repository adapters/build, identical inputs/results, complete historical/supported-profile data, nonempty mutation-then-historical workloads and output stress; attribute reasoning-layer failures to Sigma. |

**V6 — Migration and operations**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V6.1 Historical narrow migration preflight | 10% | accepted | Only the recorded retained-state v4.51.7 preflight qualification: raw owned identity/audit/one-tick provenance/ambiguity; not current-v4 or destination/history-complete migration. |
| V6.2 Current-v4 reader and source policy | 20% | active | Current v4.56.1 binary hashes/store markers, void/retention watermarks/refusals, valid-start/unbounded policy, interrupted conversion/reset and writer/format provenance; fresh qualification. Explicit valid-start/provenance subset integrated at d70da434; broader current-reader qualification remains parked. |
| V6.3 Destination importer | 25% | open | IDs/endpoints/lives/types/raw evidence, versioned hashes and synthetic import cuts, explicit collisions/ambiguity/latest-only behavior and rollback; do not infer atomic transaction groups from TxFrom. |
| V6.4 Graph backup and different-topology restore | 20% | open | Versioned full/delta backup, base identity/corruption/replay refusal and restore under different topology with retained-cut/CDC agreement. |
| V6.5 Retention, exact erase and reset | 10% | open | Closure across current/history/indexes and feed leases; explicit expired/unavailable versus absent; public operational contracts beyond internal GC. |
| V6.6 Segment tiering/archive | 5% | open | Cold/warm/archive restoration, answer equality and bounded resource proof. |
| V6.7 Actual consumer migrations | 10% | open | Sigma and ai-soc repository commits and adapter tests for every named semantic change; consumer migration is not complete from rho-only smoke tests. |

**V7 — Release qualification**

| Package | Weight in phase | State | Required scope / remaining work |
|---|---:|---|---|
| V7.1 Versioned contracts and limits | 15% | open | Public API/wire/descriptors, compatibility and documented operational/resource limits. |
| V7.2 Complete current quality gates | 15% | open | Current integrated CI/race/coverage/security/scanners and direct public-method coverage; no old source-only green status substituted. |
| V7.3 Full distributed correctness/scaling evidence | 20% | open | Specified partition/replica/RTT/process/VFS and cleanup/resource ledgers, combining V2 and V5 requirements. |
| V7.4 Pinned consumer/integration suites | 10% | open | Existing consumer suites plus agreed nonempty integration fixtures at recorded versions. |
| V7.5 Native v5 versus v4 comparison | 10% | open | Complete correctness/resource adapters and representative datasets; distinct source-row/graph-fact denominators. |
| V7.6 Graph-vendor functional adapters | 10% | open | Actual supported Neo4j/TigerGraph/Memgraph editions/versions/durability and current/history/restart adapters, beyond the existing unfinished TigerGraph candidate. |
| V7.7 Measurements, reviewed costs and release | 20% | open | At least five randomized runs/profile, p50/p95/p99/useful throughput/full outputs, all service/client/replica space and steady/peak memory without double counting; comparative matrix, causally isolated excess-cost justification, settled acceptance thresholds and separately authorized release. |

Independent V3.2 and V6.2 work is parked under the current V2 deadline priority. V3 storage integration/format decisions, V4 public freeze, V5 integration, destination import and final comparative/release acceptance keep their original dependencies. Consumer reasoning remains Sigma-owned.

### V2 closure queue (2026-10-14)

Exit authority remains [PLAN §8](../docs/v5/PLAN.md#8-implementation-sequence)
and the [substrate obligations](../docs/v5/reference/substrate-evaluation.json).
Existing E15–E18, native P08 and selected P07/P09 receipts remain valid for their
recorded scope. Close the remaining work in the following packages; a passing
component does not close its whole fault family or V2.

| Order | Remaining package | Closing evidence |
|---|---|---|
| A | Selected recovery component gates closed. [P06's four A/B × COMMIT/ABORT cells](evidence/v2-p06-first-yes-process-20261014/receipt.json) pass local normal/race/scoped coverage/static gates and independent serial witnesses; [hosted P06 qualification](evidence/v2-p06-first-yes-process-20261014/hosted-b9a4baa0.json) passes the same four cells in all three modes. [P03's A/B coordinator-leader-loss cells](evidence/v2-p03-coordinator-loss-20261014/receipt.json) pass local normal/race/scoped coverage/static gates; all six genuine captures independently replay with exact old-cut/CDC witnesses, and [hosted P03 qualification](evidence/v2-p03-coordinator-loss-20261014/hosted-404a9596.json) passes both cells in all three modes with v5 lint/security | Exact original decisions, restored conflict fencing, complete graph/index/CDC and old cuts after election/rejoin; targeted normal/race/coverage/static gates and reviewed integrated source. These finite components do not close the broader P03/P06 families or the coordinated V2 integration gate |
| B | Complete P01–P06 production schedules: coordinator outside read scope; prepare/fence ordering; partial installation; frozen participant/digest and immutable decision; historical dependencies including absent keys, empty ranges, temporal predicates and endpoint-life ABA; conflicting transactions through failover | A requirement-to-case ledger covering every P01–P06 clause, with authentic native histories and the independent serial/cut oracle; no mixed, phantom or acknowledged-lost state |
| C | Complete ownership, identity and reclamation: P07/P09 process interruptions during transfer/publication, stale authority and retry payload rejection, retained snapshot/feed lineage, expiration and application GC. Reuse P08 issuance/exhaustion proof unless changed source invalidates it | Same-directory recovery, original IDs/results, stale-owner refusal, pinned old-cut/CDC survival and safe reclamation; expired/unavailable remain distinct from exact-empty |
| D | Finish graph-composed network, scheduler and storage campaigns | Lost/duplicate/reordered messages, asymmetric partitions, isolated leaders and quorum/domain loss; pause/unequal ticks/clock jumps; short/torn writes, EIO, ENOSPC, file/directory sync, snapshot rename/manifest and corrupt pages. Durable-image and real process evidence agree with the oracle. Bounded conflicting-transaction/fence/transfer exploration precedes recorded-seed soak |
| E | Source inventory covered at `0a04e977`; execution and qualification open. Equivalent FoundationDB fault/publication controllers, eight workload drivers and native/FDB matrix routes are present | Actual role/domain-loss and unknown-commit recovery, pins/GC and chunk/manifest failures; writer 8/32, native 1M multi-grant setup, both FDB layouts and bounded comparison state. Complete historical/index/CDC parity, cleanup and all-client/server resource accounting before accepting a cost cell |
| F | Execute the declared matrix and select the engine, then run one coordinated V2 integration gate | 10k/100k/1M × 0/10/50/100% cross × 1/8/32 writers, five fresh repetitions per layout, all declared workloads and RTT profiles. Record total bytes, memory, throughput, latency, retries/recovery and complete oracle results. Written native/FDB deployment/cost/maintenance verdict; no missing acceptance rows or unresolved correctness failures |

Execution plan to finish V2 (2026-10-14; keep the A–F scope above):

Owner execution ordering, 2026-10-14: finish and integrate the full remaining V2
functionality first, then execute correctness qualification, measurements and
evidence-driven improvements. The package order below describes dependencies;
it does not require normal/race/coverage/static acceptance before starting the
next implementation package. Source review establishes coherent wiring during
implementation; author direct tests alongside changes, then compile and execute
them at the integrated qualification checkpoint. Do not start test, measurement
or CI runs during this implementation stage. Keep existing qualification scopes and
all A–F exit requirements; implementation status never implies acceptance.

1. **Freeze the acceptance inventory and close the near-ready B components.**
   Map each P01–P09 requirement and declared fault/workload to its existing
   production test, source revision and receipt, or a concrete missing case.
   Reuse accepted E15–E18/P08/P03/P06/P07/P09 evidence within its scope; a new
   case is needed only for an uncovered requirement or an invalidated source.
   The [finite P02 component](evidence/v2-p02-fence-election-20261014/receipt.json)
   is now locally accepted and integrated: all three process schedules pass
   normal/race/scoped coverage and affected statics, all 111 qualified child
   incarnations are reaped, and [independent root review](evidence/v2-p02-fence-election-20261014/root-review.json)
   verifies all 94 archive members and replays all nine genuine traces. Broader
   P02 clauses remain separate. [Focused hosted qualification at `e8a33c4d`](evidence/v2-ci-cost-scope-20261014/hosted-e8a33c4d.json)
   passes the selected P01/P02 and finite cost parents in normal/race/coverage,
   with affected build/static jobs. Its source pin predates P05 and the later
   runner changes; it is not qualification of those implementations.
   P01's [Fresh/After outside-scope-coordinator component](evidence/v2-p01-outside-scope-20261014/receipt.json)
   is locally accepted and integrated after normal/race/scoped coverage and
   affected statics; [root review](evidence/v2-p01-outside-scope-20261014/root-review.json)
   verifies its 71 archive members and 77 original external hashes. This is a
   six-disk-voter in-process component, not an OS-process fault or independently
   streamed serial-history proof. The OS-child source bridge at `58d74561` adds
   genuine A-only Fresh/After reads with B holding the immutable decision while
   A retains YES, unavailable-authority refusal, same-original recovery and
   old-certificate At reads after same-directory cohort replacement. Its complete
   native-prefix/literal/serial checker is authored, not executed; this does not
   extend the older acceptance receipt. The
   [finite P04/cost-response checkpoint](evidence/v2-p04-cost-response-20261014/receipt.json)
   is integrated with its frozen qualification; the [clone-only follow-up](evidence/v2-p04-clone-followup-20261014/receipt.json)
   is closed by [hosted qualification at `5cc915c7`](evidence/v2-p04-clone-followup-20261014/hosted-5cc915c7.json):
   its direct normal/race/scoped coverage checks and v5 lint/security pass.
   Audit the remaining clauses separately rather than reopening these selected
   components without a changed source or uncovered requirement.
2. **Close B's remaining serialization requirements.** The P05 source now
   implements a historical read followed by a dependent write,
   absent keys, empty ranges, temporal predicates, endpoint-life/revision ABA,
   and topology/catalog/profile dependencies. Enumerate existing coverage first;
   add the missing production schedules and fix any demonstrated shared cause.
   The opt-in read captures an authenticated complete dependency
   condition before returning data; dependent writes bind that original condition
   to durable request identity and validate it again at actual prepare application.
   Unchanged reads must really commit; changed state or authority must require
   caller restart, and unavailable authority must remain unavailable. Ordinary
   unconditional writes retain their contract. Source-only checkpoints are not
   accepted implementation or P05 evidence.
   Finish uncovered P03/P04/P06 clauses from the same inventory, including
   partial installation, decision races, complete predicate locking and recovery.
   B closes only when every clause has an authentic history accepted by the
   independent serial/cut oracle, exact graph/index/CDC answers and required
   failure/reopen checks. Atomic installation alone is insufficient.
3. **Close C, then D, using the freed protocol worker.** Complete P07/P09
   interruptions around capture, staging, durable handoff, activation and pin
   publication; reject stale owners and changed retry payloads, preserve original
   results and pinned old-cut/feed lineage through reclamation, and exercise
   explicit expiry. Then compose the existing native graph path with the
   declared message, quorum/domain, scheduler and VFS/durable-image faults.
   Portable pin publication must finish all participant installs before its
   final authenticated live-owner observation pass; proofs captured before
   later installs cannot establish a common live publication point. Native
   logical GC requires a fresh retention/snapshot contract: the existing AS6
   format assumes continuous retained indexes and cannot describe deleted roots
   or CDC. Preserve ordinary formats; bind sparse retained lineage, floors and
   resumable deletion accounting explicitly, protecting held views and pins.
   Freeze the finite transition/schedule list before running it; perform the
   specified bounded ordering exploration before a recorded-seed soak. C/D close
   only with exact oracle agreement, no acknowledged durable loss, recovery after
   restored authority and all owned processes/handles reaped. SIGKILL does not
   substitute for torn-write or power-loss-model evidence.
4. **Build E's executable comparison prerequisites in parallel.** The
   [native multiple-grant setup](evidence/v2-e-multiple-grants-20261014/receipt.json)
   is locally accepted and integrated for the compact profile guard and actual
   17-fact local/cross process cases; [root review](evidence/v2-e-multiple-grants-20261014/root-review.json)
   verifies all 86 archive members, 90 original external hashes and the six
   actual grant streams. This does not execute the 1M cell. The
   [private quota-limited comparison spool](evidence/v2-comparison-spool-20261014/root-review.json)
   is integrated as a helper with five direct tests passing independently; it
   is now wired in source to the raw comparison checker, with runtime acceptance
   deferred. Source now also wires six-process same-directory reopen,
   original-outcome/CDC replay and bounded native index/endpoint paging, then
   wire FDB history/current/CDC streaming into the comparison checker with
   complete raw-byte/identity/order/watermark checks. The
   [finite FDB stream helper](evidence/v2-fdb-cost-stream-20261014/receipt.json)
   is now accepted and integrated after real SDK/server normal/race/scoped
   coverage, vet/lint/security; [root review](evidence/v2-fdb-cost-stream-20261014/root-review.json)
   verifies all 494 raw members, 27 frozen source inputs, actual terminals and
   86.01% helper coverage. The later raw artifact writer/checker is integrated
   source, not SDK-qualified output; neither establishes a capacity cell.
   Native source now implements genuine 8/32 clients with request-bound acknowledgements,
   bounded admission, phase barriers and immutable terminal/pending attempt lineage;
   execution is deferred. Implement the equivalent FDB writer paths.
   Preserve the engines' actual conflict and ordering semantics. Qualify tiny
   paired runs before executing larger measurements. Separately
   finish equivalent FDB role/domain faults, unknown commits, pins/GC and
   chunk/manifest publication; native prepare-stage hooks are not FDB evidence.
   E closes when both FDB layouts and the native runner execute those cases with
   complete historical/index/CDC parity, cleanup and all-process resource counts.
5. **Complete a matched 10k cell, then execute F.** Diagnose any remaining slow
   path from measured costs and change only the demonstrated cause. Require a
   terminal old/current/index/CDC oracle, clean reopen and complete client/server
   accounting in the native and matched FDB cell before running larger cells.
   Then execute the full declared sizes/cross-percent/writer matrix, five fresh
   repetitions, all workloads and delay profiles. Keep raw failures and costs;
   missing or unsupported cells remain open. Record the native/FDB engine verdict
   from comparable correctness, durability, total cost, deployment and maintenance
   evidence before V3 format freeze.
6. **Run one coordinated V2 acceptance gate.** Reconcile main at a recorded
   cutoff (integrated main is `0fa37786`, including release
   `5d8b5917`/v4.56.1),
   review relevant handover/consumer/CI deltas and integrate required changes.
   The effective-page, history-epoch/head-cache, correction/rollback fast-path
   and compaction-gap version-allocation contracts belong in that source/consumer
   reconciliation. The latest additive delta also includes numeric property
   bounds, range reads/counts at a pin, label/type time statistics and scans
   restricted by ordinal bitsets. No v5 implementation or shared CI file changed
   in that latest delta. Under `UntimedFromTxFrom`, `Eternal()` alone still
   cannot remove the valid-time filter; `UntimedUnbounded` instead stores an
   explicit -inf first-version start. Preserve the selected persisted mode,
   including its upgrade and conversion rules. The upstream handover is not
   release evidence.
   Preserve historical qualification rather than relabelling it as current.
   On the final integrated revision run full phase CI/race/coverage/security and
   the complete V2 requirement/evidence audit once; repeat only gates invalidated
   by a subsequent correction. V2 closes with every A–F row accepted, the engine
   decision recorded, no unresolved correctness failure, and the exact branch
   revision pushed with passing applicable hosted gates. V3–V7 remain separate.

Implementation handoff: P05 is source-integrated at `9e677135` with canonical
schema setup and at `99ef2d7c` with conditional recovery-credit sizing and a
fixed-operation history verifier. Native reopen/index source through `80cf0e03`
now has independent 8/32 clients and immutable attempts at `2e481ff5`/`d9e6f289`.
Authenticated remote capacity recovery and the released native child dispatch
are integrated at `b02ff3e2`/`0fcefb65`; source review also corrected its
secondary-footprint admission. The FDB raw writer/checker from `66a8e36b` has
typed chunks and immutable manifests at `3afc715a`, plus durable pin/reference
ownership and bounded derived-generation collection at `bbf27fcc`; `45939437`
preserves the caller's owner ticket on every unsuccessful hidden-handle cleanup. Source KV
history, cuts and CDC remain retained; derived collection does not close source
application GC. All these later sources await runtime acceptance. Earlier P05
compiler/setup failures remain recorded failures; the earlier compilation-only
checkpoint does not qualify this new source. Next source packages cover portable
cut/feed publication and source GC, distributed native column/cost wiring, then
the remaining fault/workload integration. Follow the owner's implementation-first ordering
above; qualify the integrated result afterward.
Bounded incident continuation is source-integrated at `18d28001` with callback
ownership corrected at `266ef78d`; `62764628` hydrates extra typed cells on that
same active read, and the existing compact guard now calls the routed APIs
directly. The owner retains one original certified cut, one genuine private
cursor and one last chunk with cumulative prepaid work/RPC/output limits.
Its current process-local lifetime follows the actual source TTL (at most one
minute). Native cost IPC wiring is integrated at `8e151e20`/`2418b0cf`: one
child-owned cookie and monotone sequence, bounded replay, cancellation and Close
on the existing reserved cleanup lane. Receipts include completed source work,
cumulative prepaid allowances and actual source RPC/frame bytes. Durable pin
integration and large-degree qualification remain open. FDB's explicit column-runner lane is source-integrated
at `0233899d`: old/current publication, held old reads, genuine cache bytes and
bounded derived collection precede a fresh source-history/CDC census. Controller
and independent column verification are source-integrated at `570dc8f7`, including
exact original-source/binary/cache/owner/EOF agreement and the actual live,
tombstone and retired-lineage logical-byte census. The
lane compares derived FDB columns with native logical/KV answers; it does not
claim native column-layout parity, source KV reclamation or a qualified cost cell.
Native terminal cleanup-credit storage is source-integrated at `6232f4ce`, with
AS6 snapshot binding and actual Ready activation at `b56db530`/`ffa905e7` plus
the matching authored guard correction. A bounded separate bank reserves cleanup
alternatives within existing quotas independently of transient transaction
recovery. Snapshot admission funds both banks before staging and binds exact
imported credit state. The participant graph pin ledger and bound Host admission
are source-integrated at `9c8a27a1`/`ee5257e8`: exact original cut/feed ownership,
monotone feed advancement, replicated lease epochs, funded Release/Expire/Close
and immutable original outcomes. All-participant portable publication is
source-integrated at `eda4e2af`, with post-install live-owner observations,
authenticated retained-source opens, exact original-outcome recovery and
prepaid terminal cleanup. Its compact direct guards, including the finite-quota
correction at `23ebabc1`, are authored only. The reviewed shared process harness
is released for distributed column integration. Source logical GC,
fresh-profile handoff fault composition and runtime qualification remain open. The native sparse
retention configuration/contract is source-integrated at `51407447`: opt-in
RLM9 metadata binds separate history floors and exact semantic/data anchors
within existing quotas. AS7 publication-header/export verification is
source-integrated at `1d28230a`, binding actual family counts, required anchor
hashes and recovery state; `700f68f2` also requires the final recovery root,
so an extra old row cannot compensate for a missing required suffix endpoint.
AS7 import/prepared/actual Ready activation and AD6 descriptor wiring are
source-integrated at `edf266bd`, with exact owned anchor charging, incoming
transient/terminal reservations and local-cursor reset. Authenticated native floor
admission is source-integrated at `24a5db50`, with exact committed command/source
and owner checks, captured independent expiry floors and a separate image-anchor
door. `b8c8a0f6` covers the recovery-owner floor; `5f0fbb80` also prepays local
cursor backing in explicit recovery creation. Operative native collection and
sparse scrub are source-integrated at `cad0b51b`: bounded pages preserve exact
floor predecessors and data anchors, co-sync namespace debits and a separately
accounted resumable cursor, and retire a publication only when represented rows
are deleted. Same-index sparse republication keeps the genuine term/hash and
changes the cut identity without business or CDC progress. Durable column-owner
process wiring is source-integrated below; logical-page reclamation remains
V3/V6 work under PLAN §8, as detailed below. All
these new sources await execution. The bounded captured-view plan accessor is
source-integrated at `3fcf9591`; it grants no anchor or deletion authority.
The fixed-size publication identity accessor is integrated at `12b2293f`.
`f9a3a13a` separates committed floor installation from nondurable local
reader/export counts; those owners still prevent physical deletion and retain
their captured floors and export headers. Graph floor/source/original wiring is
integrated at `178d4537` with ordinary-credit dispatch at `fe80a2b4`:
authenticated pin and recovery rosters bound all six requested floors, installed
Applied/Blocked/Stale results remain immutable, original PLMs are preserved,
and current source selection uses the genuine installed plan anchor.
V2's P07/P09 reclamation gate still requires actual native root/MVCC/CDC
collection with active old-cut/feed pins and exact original retries. Reclaiming
unreachable immutable graph pages/catalog entries belongs to V3's compact
engine and V6's retention operations (PLAN §8); current-root-reachable temporal
assertions remain live data. Deleting request originals first requires the
declared deduplication lease/known-expired identity contract (PLAN §5.2).
Retained logical pages and originals stay in V2's storage/cost ledger and remain
subject to explicit admission/backpressure. These guards are authored only.
Remote classification is integrated at `860f6b32`: the fresh collected profile
preserves authenticated graph/native history expiry independently of pin
expiry, unavailable authority and exact-empty data. Older response bytes and
cause-bit meanings remain unchanged; old agreements reject the new status.
Fresh-profile handoff is source-integrated at `55fa7fe`: genuine original
logical groups retain native pin ownership and protected root/feed data, while
the target stores an immutable, authenticated lineage snapshot. Fresh GMP3
binds that complete packet to capture/import/activation/restore; older formats
and configured limits stay unchanged. The fresh materializer constructor is
now wired. Missing participants blocked by a move remain nonterminal pending,
so the exact partial publication can finish afterward; existing originals and
funded terminal operations still recover. Queued authenticated expiry yields
normal pin-expired or internal-round-stale refusals without poisoning apply.
The six-voter handoff/reopen/collection and refusal guards are authored only;
broader process/fault composition and qualification remain open.
Durable native column-generation ownership is source-integrated at `317b384`.
A fresh explicit contract journals genuine issued originals, dependency minima,
complete pin publication and the original token before exposing derived reads.
Reopen authenticates original keepers; transient Close preserves durable ownership.
Independent stores have distinct native pin identities even with the same local
generation ID. Explicit retirement closes live owners and orders authenticated
cancellation fences against delayed publication. Four terminal exchanges are
prepaid in the persisted policy; spent uncertain attempts are not refunded, and
exhaustion preserves retiring state with an explicit limit refusal. Legacy
contracts/bytes stay unchanged. The two native guard parents are authored only.
Fresh column admission and source-failure accounting are source-integrated at
`d385f2cd`: a separately versioned aggregate-row allowance binds the immutable
workload admission; failed distributed captures retain actual post-Close RPC
and wire counters. Legacy zero-version bytes and allowances remain unchanged.
Large-prefix continuation is source-integrated at `690ec000`: the fresh policy
journals dependency positions/minimum feed indexes and prepays every attempted
page. Genuine transient-source renewal preserves the same query deadline,
durable generation, private cursor, sequence and cumulative accounting; the
next cursor position commits only with its complete cached reply. Legacy
zero-version journals and cache admission retain their existing contract.
Genuine OS-child selection and interruption wiring are source-integrated at
`feb661e9`: six same-directory child restarts preserve exact originals/tokens;
partial retirement resumes the existing generation with spent attempts intact.
The authored parent separately checks local retired-generation refusal and the
actual original token's refusal by native keepers. Retirement uses ordinary
dispatch so nested terminal cleanup can use its reserved lane; fresh response
codes are accepted only by the explicit new profile. Its bounded correctness
policy is not a benchmark capacity claim. Compilation and all runtime guards
remain pending. This does not retire graph logical key families or immutable originals.
`ab650bbc` also refuses a replacement plan
while native collection has unfinished cursor progress. Fresh initialization preserves retention
configuration and prepays/publishes its actual recovery-root census at
`9741c403`/`9f9116b5`; older initialization profiles retain their existing bytes.
Complete retained terminal-bank comparison is
source-integrated at `045517ab`; graph Restore can detect unknown extra native
owners instead of inspecting only the transient recovery owner. Its direct
historical guard and existing unknown-version guard updates are authored only.
These new packages have source review and exact
integration hashes only; compilation, authored guards and runtime qualification
remain deferred to the integrated checkpoint.
Native derived change columns are source-integrated at `4e6da1db`, with genuine
source-prefix frames, authenticated earlier originals for replay entries,
bounded durable publication/cache and retained-cut reads. This slice is
in-process and retain-all. Distributed child-owned publication/read/cache and
physical-restart wiring is source-integrated at `5e6ca422`, with authenticated
full cuts, disjoint local prefixes and both genuine sealed EOFs before joining.
The fresh column artifact has its own version; the ordinary KV artifact is
unchanged. Bounded independent checker/controller integration is source-integrated
at `84225cc0`: exact raw originals, binary typed rows, initial/old/final cuts,
sealed prefixes and actual cache bytes/ledgers share the existing finite spool.
Owned native disk accounting streams directory entries at `fb5156b3`, retaining
checked logical-byte/POSIX-block totals without a whole cache-file inventory.
Durable pin-backed ownership and its OS-child wiring are source-integrated above;
source-GC qualification remains open.
Native cost replay export/comparison is source-integrated at `2f46278f`: a later
entry keeps its actual prefix/root and empty current CDC, joins the authentic
earlier original frames, and is excluded from duplicate business folding only
after that binding succeeds. The compact native and offline guards are unrun.
FDB independent 8/32 writers are source-integrated at `45c61d84`, with bounded
workers, actual paired retry/abort originals, completion-order-independent
comparison and genuine post-commit policy observation (`e76c49c7`). Compact
guards are authored, not executed. The fresh contention/empty-range family is
source-integrated at `f9e99ca5`: true native read-modify-increment checks and
isolated competing empty-ID claims use 1/8/32 callers, immutable uncertain retries
and authenticated ABORT-only replanning. Its separate artifacts/checker bind
complete originals, old/current state, CDC, column ownership and derived GC;
fixed probes share history walks and make no indexed-point latency claim.
The selector/source family is source-integrated at `ae6866cd`: genuine
Fresh/Stable/After/At selection acquires an authenticated retained owner before
data access. Stable selects from a complete bounded native inventory without
publishing a new fence; After authenticates its original receipt. A linear
current-key/original-prefix census proves semantic scan completeness; deep-old
predecessor traversal has an explicit finite policy. Fixed batched Points uses
one full scan and makes no indexed-point latency claim. Cleanup retains the
same owner, cumulative allowance and bounded pending handle, independently of a
cancelled read context. The fresh driver supplies actual 1/8/32 business callers,
old/current and exact-scope observations, complete ID/request/source inventories,
both layouts and genuine column/cache/lifetime operations. Separate actual
Open/Scan/Points timings bind mode, cut and EOF cardinality, with fixed per-cell
sample counts; write latency stays separate. Legacy wrappers/formats remain
unchanged. The direct API and independent checker guards are authored only.
Native contention/empty-range source is integrated at `690ec000`: genuine
1/8/32 callers share two actual Run admissions, keep complete frozen conditions,
and replan only after known refusals/ABORTs. All six voter prefixes, physical
restart, immutable original/grant replay and old/current/phantom answers join a
bounded independent checker. A finite owned transport delays the real native
Raft/source frames and records queue, framing and cancellation costs. Explicit
chosen run budgets bind the original deadline; they are not performance promises.
The controller supports native local/1/20/100ms and paired local FDB answers.
Its retain-all record allowance is corrected at `7d4f62c9`: each entry funds
256 keyed/control writes plus Root/Outcome/CDC and the enabled recovery envelope
(260 records). The authored guard derives that extra record from the native
recovery API; neither this quota nor the correction is runtime qualification.
The dispatcher schedules all eight families and local/1/20/100ms profiles.
FDB has source drivers for all eight families: create/update, contention,
selectors, snapshot-to-feed, cross-edge uniqueness, immutable columns,
late correction and ownership transfer;
their fresh owned transport adapters cover
local/1/20/100ms. Native has create/update, contention, cross-edge, snapshot-to-feed,
selector, immutable-column and late-correction source drivers across
local/1/20/100ms, with local paired comparisons. Unsupported semantics
or transport refuse before effects.
Each family/transport matrix contains 360 fresh cells; the complete eight-family,
four-transport matrix contains 11,520 cells per engine.
1M or 32-writer cells require explicit finite comparison disk/cache grants.
No new cell, guard or runtime gate has run. The native transfer/retained-pins
driver remains implementation work. Its compact recovery prerequisite is
source-integrated at `e9f3322`: explicitly selected recovery3/retention1/Contract6
uses at most eight disjoint exact-next credit spans, with checked aggregate
application/log bytes and records. RS3/RY5/RQ3 and AS8/AD7 carry the same remaining
capacity through queued replay, old roots, portable snapshots and receiver reopen;
TB1 terminal funding stays separate. Actual terminal installation alone retires
unused suffixes. Recovery1/2 and older formats remain unchanged; no quota ceiling
was raised. All 22 frozen blobs and 19 complete parents were verified. Compact
graph transfer formats are source-integrated at `1cf21b1e`: fresh GMI2/GMQ2/GMO2/
GMY3/GMP4 bind that same explicit agreement; at most eight credit spans fund an
explicit page grant up to 2^24 without raising store/log quotas. GMP4 references
the actual immutable source H0 header and retained packet, hydrated once for the
authenticated target manifest. GEB1 batches at most 32 genuine GEP1 subframes
within 64 KiB and one shared read/output ledger. Its outer seal is prepaid;
nonfinal fragments end the batch so continuations read installed predecessors.
Identical native/inherited original aliases coalesce without refunds; conflicting
bytes refuse before publication. Fresh remote source opening, feed classification,
inactive AS8 auditing and ascending partial undo use the same agreement. All 23
frozen source blobs and 21 complete current parents were verified. Direct native
fragment, reference, conflict, source-lease, audit and undo/reopen guards are
authored only, not executed. The actual native transfer workload, full fault
composition and matrix qualification remain open. Source feasibility review
found that graph content still emits one logical record per page: at 1M facts,
even the first HOME range at 100% cross has at least 5,097,148 graph pages before
preparation/history. Import plus undo would exceed the hard 10M retained-log
entry ceiling. Bounded graph batching is now source-integrated at `6b1eea94`:
an explicit further agreement uses RMB1 envelopes and GMP5 progress, separating
logical cursor positions from native import/undo counts. One shared stage sees
earlier tree and fragment writes; source page/graph work and output allowances
accumulate across the batch. Packing separates record families, limits component
envelopes to eight subpages and all envelopes to 32/64 KiB; the existing
128-write undo ceiling remains. All 13 frozen blobs and nine complete current
parents were verified, with pinned parser/format and whitespace checks only.
Actual transfer and capacity remain open: import plus undo reservation bytes
must also fit the unchanged 1 TiB ceiling. Capacity qualification must include
actual packing, ordinary retained state, the completed first move and the
second move's active reservations; it does not block authoring the driver. Source review
rules out smaller page grants alone for the mandatory 1M/100%-cross shape:
full rewritten index paths enter both the retained manifest and local kernel.
Those R/K records alone have a conservative 32,734,243,584-byte lower bound
after the first 262,144 creates. Current per-page import/undo credits already
reserve more than 1 TiB for those bytes at every supported page size up to
64 KiB, before graph pages and other retained state: with page bytes P, each
import/undo credit is 81,920 + 16P, so their combined multiplier is at least
34.5. Resolve this reservation/representation cost without raising ceilings or
discarding originals. The bounded recovery primitive
is source-integrated at `c5da369a`: explicit fresh recovery4 byte pools retain
unchanged per-install maxima. Actual
committed installations spend retained totals; queued entries project their
actual encoded log charges separately. An additional bounded pending-install
window must be protected from ordinary traffic before reservation, with distinct
live queued uses charged against that window once. Speculatively subtracting
earlier maximum retained charges is unsound: maxima 80/80 would otherwise strand
valid actual growth 10/70 inside a total budget of 100. Original retries cannot
refill a pool; unused surplus and its window retire only at actual terminal
installation. Contract7/AS9, AD8 and RS4/RY6/RQ4 preserve exact remaining totals
through native history, queued recovery and snapshot transfer; TB1 and older
agreements remain unchanged. Source review verified all 23 frozen owned blobs,
20 complete current parents and three new-file absences; the patch changes
17 existing files and adds three. Pinned Go1.26.9 parser/format and whitespace
checks passed only. The new direct native reopen, snapshot-plus-suffix,
near-quota, terminal and duplicate guards remain unexecuted. Range-move wiring
is source-integrated at `bef39c2e`: fresh GMI3/GMQ3/GMO3 and GMP6 use independent
describe/import/undo hard limits. Every accepted import funds its complete
before-image undo bound; refused pages preserve the accepted prefix and an
installable refusal until genuine source ABORT. Immutable GM/V originals bind
the full attempted command and outcome, including retries after terminal abort.
Scalar reopen checks and incremental AS9 undo/control folding keep validation
bounded. Shared compact baseline validation now uses reference bytes and native
graph/replay counts. All 21 frozen source blobs, 19 current parents and two
new-file absences were verified with pinned parser/format and whitespace only.
Direct commit, partial undo, refusal, lost-reply, same-original and snapshot guards
are authored, not executed. Both-move capacity and actual Cost12 remain open.
At 1M, HOME issuance also
spans two actual range publications, both of which must move under distinct
original identities before the driver claims the full routing transition.
FDB feed business accounting is source-integrated at `4218c9a7`: an explicit
optional extension joins create/update phase walls,
including each seed and complete worker drain, to raw begin/end markers and
useful originals. Same-clock queued-start/worker-stop samples supply complete
operation percentiles through the bounded spool. Historical helpers keep the
extension omitted and their earlier scope. Existing scan and feed-call durations
remain separate; parallel operation durations cannot supply phase throughput.
These accounting guards and the workload have not been compiled or executed.
Cross-engine allocator/retention matching, full matrix qualification and the
engine decision remain open.
The create/update family keeps a strict-local 0% baseline and explicitly named
mixed/cross 10/50/100% variants, as clarified in the substrate reference.
Nonzero rows must join actual multi-participant business read/write footprints;
remote client/processing placement or coordination metadata cannot supply that
percentage. Report their throughput separately from the partition-local baseline.
The FDB driver is source-integrated at `17410187`: its immutable proposal and
paired original joins authenticate the complete business footprint, while
phase wall surrounds the seed plus real worker drain. Separate complete-operation
samples supply bounded external-spool percentiles. Fixed fixture IDs claim no
allocator issuance; old/current/raw/CDC and native/cache column observations
remain required. The native counterpart is source-integrated at `4937fdcc`:
genuine local Submit at 0%, cross Run at mixed/cross percentages, all 1/8/32
callers and native transport profiles, complete phase walls and independently
joined operation percentiles. Each atomic update performs Set6 followed by
same-life Present correction to check existing relation and endpoint coverage;
ordinary Set keeps SEM06. The additional presence revision is charged in the
3+4N row census. Updates read remote endpoints but write only their owner;
report logical read/write and protocol participants separately. Five actual
old/current/reopened observations, raw originals, installed grants and native/cache
columns precede local paired answer comparison. Nonlocal paired execution still
refuses. The authored guards and workload have not been compiled or executed.
The independent reference validation remains historical at its recorded source
digests; refresh it during the deferred correctness stage after this clarification.
The FDB transfer prerequisite is source-integrated at `d13cc3eb`: a fresh
owner-catalog namespace maps stable logical buckets to processing owners and
monotone epochs. Native execution checks current authority before replay and
the complete captured route before new effects; successor recovery preserves
the immutable request body. Move reservation also requires the caller's captured
epoch, fencing delayed A1 requests after A1-to-B2-to-A3 ownership reuse. Atomic
freeze/activation retain the genuine source owner and metadata; funded cancel
releases that same owner. The freeze is conservative across the whole graph.
The finite one-writer core and its authored SDK guards are unexecuted. The
fresh Cost8/catalog2 prerequisite at `011b978f` adds explicitly bounded matrix
admission, native HOME block reservations/high-water, one-use cursor installation
and actual local issuance. Lost installation cannot recreate a cursor; native
closure burns its uncertain suffix and transfer waits for durable session drain.
Committed IDs retain first-use provenance. Successor resolution preserves an
existing COMMIT or records a fenced ABORT only with both originals absent in the
same native transaction. Already-funded move/grant cleanup has eight bounded
admissions and an immutable deadline ceiling after ordinary run expiry; it
cannot authorize new allocations or writes. Catalog1 caps remain unchanged.
These authored guards are unexecuted. The fresh census at `b6dadb9d` binds a
complete bounded mutable-head inventory and both-prefix raw EOF to the frozen
source owner; its seal and route activation share one native transaction. SAME
last-page recovery retains cumulative work, including full delivered SDK head
windows after the correction at `772e9391`. Its separately counted controls and
raw census are logical key/value bytes, not physical disk or RSS measurements.
Catalog2's mandatory full census is a conservative harness certification barrier,
not an inherent FDB copy or placement requirement. Record its actual wall time
separately from complete transfer latency; material overhead requires an explicit
baseline refinement before the engine verdict, not inferred throughput from
subtracted or overlapping times.
The actual transfer workload/oracle and matrix wiring are source-integrated at
`d3be95bd`. Two source/target SDK losses recover the SAME committed K across A1
to B2 processing ownership; a prior frozen request gets an authentic fenced
ABORT before a linked replan. Actual Next-backed blocks, held C, irreversible
feed-only release, physical source-history GC and complete retained CDC join
bounded independent original/column/native-cost inventories. Terminal Cancel
advances the catalog control version to 3 and permits only its five exact head
transitions after the frozen storage census. Caller cancellation reaches native
collector Commit/OnError through the same deletion reducer. These cases have
not run; native transfer parity and equal allocator/retention costs remain open.
Stable shared-KV keys do not establish physical replica transfer.
The fresh FDB feed3 prerequisite at `29f00cbe` authenticates snapshot Close and
the complete token-reader inventory before atomically relinquishing its physical
pin. Native retained-feed identities and funded cleanup then use the existing
complete Page/ACK reducer after physical history collection. Snapshot reopening
explicitly refuses after this irreversible transition. The old feed formats
retain their physical-pin contract. Its direct guards are authored only; this
does not qualify process replacement, the transfer workload or matched costs.
Native currently retains snapshot C with its feed: equivalent retention must be
established before an engine comparison can claim parity or equal-retention cost.
The FDB cross-edge family is source-integrated at `78f11fc8`: distinct contenders
share one scalar uniqueness claim, endpoint close/reopen masks the old life,
and closing/replaying the old canonical relation preserves the replacement claim.
The independent fold joins exact originals, history/current/CDC, retained At
views and native/binary-cache columns. The review follow-up at `e76eef12` binds
every writer's exact ordinal assignments and streams complete-operation timing,
including same-original/replan waits, into bounded percentile calculations.
Setup, business and diagnostic durations remain separate. Its adversarial guards
are authored only; all runtime qualification and native parity acceptance remain open.
The native cross-edge source and local paired controller are integrated at
`e48ebc13`: frozen empty-range/old-close originals, actual endpoint-life ABA,
complete voter/CDC/index histories, native/cache columns and same-directory restart
join one bounded independent oracle. Exact caller assignments and original/body
joins prevent incomplete or misclassified traces from passing. Streamed timings
include admission and same-original/replan waits. Owned local/1/20/100ms transport
is source-integrated at `e8ae7462`, binding both actual process cohorts to one
binary, graph declaration, finite budget and deadline. The matrix selects this
fresh admission explicitly; the original direct local format stays unchanged.
Its guards are unexecuted; other native families, nonlocal paired comparison and
the full matrix remain open.
The FDB transport extension is source-integrated at `452c8913`: contention,
snapshot selectors and cross-edge uniqueness now bind the same original
transport/deadline through native policy, captures, retries and retained-original
fast paths. Old formats remain unchanged. The authored guards and actual
local/RTT execution remain unqualified.
The immutable-column family is source-integrated at `866ffb9b`: a dedicated
Cost6 binding, genuine concurrent create/update originals, retained old source
and column owners, final publication, native/cache walks, scan-based probes and
authenticated derived collection share complete raw source/original checks.
The KV layout stays a source-scan reference. Complete-call timing includes its
actual artifact callbacks, while fixed samples stay bounded. Existing selector
formats and filenames are unchanged. Source KV history remains retained;
execution, native family parity and capacity acceptance remain open.
The native immutable-column workload is source-integrated at `2e94e9b5`: genuine
publication, native/cache scans and fixed scan-based typed probes join complete
originals, historical/current source, CDC and same-directory restart. An old
reader held across guarded updates causes actual Retire refusal with unchanged
native journal records, followed by Close, retirement and keeper refusal. The
independent oracle authenticates the reader identity and complete binary EOF;
future test selection includes every shared caller. KV remains a source-scan
reference. Native derived chunks stay retained and counted after journal/pin
retirement; no equal physical reclamation with FDB is claimed. This is source
integration only, with no compilation, execution or efficiency acceptance.
The late-correction family is source-integrated at `f580f765`: half the facts
change from 5 to 6 after a retained old cut; the other half remain 5. One bounded
source scan per cut serves six out/in posting predicates, including phantom 7,
with complete original/history/current joins and exact corrected/unchanged sets.
This is scan-based posting selection, not a value-index latency claim. Its Cost7
admission, actual partial-update counts, immutable-column lifecycle and owned
local/RTT dispatch retain separate family artifacts. The checker requires the
actual business-participant fraction rather than inferring it from endpoint IDs;
setup and physical transport costs are separately attributed. Authored guards,
runtime qualification and native parity remain pending.
The native late-correction workload is source-integrated at `185fcbc4`: only
the first half receives guarded scalar corrections; the retained cut stays5
and current state contains corrected6 plus unchanged5. Three genuine endpoint
continuations per cut provide complete outgoing/incoming5/6/phantom7 predicates,
with original-bound hydration and independent exact-set joins. Its fresh policy
funds nine scan-equivalent passes under five query originals; prior profiles
keep their original allowances. Complete native/cache columns, held-reader
retirement refusal and same-directory restart use the half-cohort row/provenance
census. Native actual RTT routes and local paired comparison are wired. No
compilation, execution, capacity or efficiency acceptance is claimed.
The FDB snapshot-to-feed source is integrated at `5c14f42c`/`bb867775`:
Its fresh owned transport adapter is source-integrated at `7f4a4fb8`: actual
receiver-ingress delay setup, probes, same-deadline client replacement, cleanup
and raw process/cgroup accounting are wired across five owned containers.
The native binding and retained feed original authenticate that fixed admission.
Tool builds, Linux/FDB execution and all authored guards remain unqualified;
transport support for the two remaining families and the complete matrix remain open.
Begin journals its actual selected cut and factory intent; Publish atomically
acquires the pin, snapshot owner and feed cursor. Bounded complete CDC pages
replay until an exact immutable ACK advances their vectors. Independently
reserved cleanup tickets survive ordinary quota exhaustion and recover the same
saved owner after client loss or pin expiry. The driver includes a real
snapshot/write race, SDK client kill/replacement, both layouts and a bounded
independent source/page/ACK/control verifier. Source review corrected owned
cleanup-work reporting. Normal-IO deadline propagation through shared scanners
and column helpers is source-integrated at `ed38403f`: configuration, cuts,
scans, columns and native retry/commit waits share the original run lifetime
and any shorter caller cancellation. Legacy nil-context paths retain their
ordinary IO. Fresh column cleanup retains its same pending owner and separate
finite first-close deadline/callback allowance. The workload container now
receives the same admitted deadline as its monitor. Compilation, runtime and
capacity remain unqualified.
The explicitly selected FDB source-history
collector is source-integrated at `5685b659`: durable pre-deletion admission,
held-read/pin/reference protection, resumable bounded deletion and exact original
revision checks preserve current anchors, retry outcomes and retained CDC.
Fresh runner/artifact wiring is source-integrated at `e34d9783`: genuine held old
reads, expired-new-open refusal, bounded durable collector resume, complete
retained originals and COMMIT/ABORT replay bind separate version6 artifacts.
Its bounded controller/checker is source-integrated at `690ede3c`, joining
held old reads, actual collection progress/seal and unique original/replay
inventories on the existing comparison spool. Pre-GC logical/column census stays
separate from post-GC physical anchors; native retain-all and FDB source-retirement
profiles do not establish equal storage efficiency. Runtime qualification remains open; default
retain-all behavior and ordinary formats stay unchanged. These integrations close source packages,
not V2 E/F qualification or comparative efficiency acceptance.
Existing accepted components stay closed unless their source or required
behavior changes.

FDB observed role/domain faults are source-integrated at `35e75e4c`: seven fresh
cases in both coordinator orientations bind live co-resident roles, actual
process/network actions, same-volume recovery and immutable request replay to
complete old/current/index/CDC observations. Native Commit-issued caller loss
grants no inferred decision. `e3b879b2` corrects its verifier to authenticate the
actual Go byte-slice scope encoding. Publisher and derived-column collector
process interruptions are source-integrated at `964e3555`: partial uploads,
verified-source EOF and Commit-issued manifest replies recover the same
generation; the actual held owner survives token expiry until authenticated
Close, and a replacement SDK process resumes committed deletion progress.
The independent verifier joins complete raw history/current/CDC and cached
column bytes to original outcomes and exact deletion debits. Source-history
process replacement is source-integrated at `0217659c`: the actual saved reader
identity authenticates and closes its existing owner after token expiry, and a
fresh SDK process resumes the exact committed physical-deletion cursor. Before
and after streams bind nine original effects, six retained anchors and five
unchanged COMMIT originals/CDC groups. This finite package does not cover ABORT,
server loss or power loss. Remaining directed-message/workload schedules and
execution stay open; this does not close E.
The role/domain and retained-owner composition harness is source-integrated at
`092fa478`: sixty fixed cases join six actual role/domain faults to five existing
publisher/source-owner/partial-collector seams in both coordinator orientations.
All targeted faults precede the same SDK caller's reap; same-volume healing
precedes genuine replacement. The complete unchanged raw/history/CDC/cache/GC
finalizers still validate original identities, generations, owners and cursors.
Source-held and column-held fixtures remain separate namespaces. Five frozen
blobs, both complete current parents and 1,266 source inputs were verified;
no controller, test or fault run executed. This completes additional harness
source, not database functionality, inside-outage progress or E acceptance.

The paired native cost/fault driver composes caller loss before Decide,
committed Decide reply loss and one HOME leader SIGKILL with a healthy quorum,
followed by clean six-volume reopen and original-request replay. The bounded
composed-fault source package at `2d9082e3` adds partial A installation with
coordinator loss and delayed Resolve, immutable conflicting-original refusal,
real packet drop/duplicate/reorder/directed partition and finite delays, matched
WAL short-write/EIO/ENOSPC/file-sync failures, and directory-sync failure during
genuine same-directory reopen. Its raw-prefix/original/serial bridge and owned
process/queue accounting are authored, not executed. The queued capacity
COMMIT/ABORT first-decision source package is integrated at `1ba59c78`: the
original child retains two genuinely constructed private candidates, both actual
native admissions precede either application's progress, and both queue orders
bind persisted inputs to the immutable first decision, complete typed CDC,
same-original retries and replacement state. The optional observation seam owns
prepaid detached copies; cancellation never supplies a vote or decision.
Its process parents and independent prefix/serial checker remain unexecuted.
The composed-D family is source-integrated at `a2091976`: old-leader packets,
directed loss, unequal ticks, quorum/logical-domain loss, permanent lease expiry
across clock rollback, interrupted snapshot activation and genuine OPTIONS
rename failure use the actual graph/replica paths. A separately bounded VFS
model preserves synchronized file bytes and directory-name identities; recovery
selects the sealed post-reap image, while pre-kill capture records lineage only.
Required active WAL/manifest corruption is checked through native Open refusal
and same-image repair. These are authored model cases, not physical power-loss
evidence or measured costs. Six OS-child conflict/fence/real-transfer causal
cases retain exact originals, business CDC and both activation-lineage CDC
records; the existing native-host fence-first stale-original case is separately
attributed. The checker requires that declared finite inventory and does not
claim exhaustive arbitrary orderings. Direct FS/lifetime/copy and negative
grammar guards are authored only. Compilation, execution, broader P01-P09
transition composition, seeded soak and qualification remain open. The remaining
cost workload dispatches and RTT/matrix integration also remain open; a finite
packet delay is not a measured RTT profile. This source package does not close
B or D.

The storage-boundary source at `bcf74578` adds before/after actual VFS selectors
and four authored WAL Write/SyncData process cases. Receipts distinguish returned
operations from guaranteed synchronization; queued `SyncTo` and unmatched arms
cannot qualify a case. Same-original, complete CDC and old-cut recovery reuse the
independent history checker after strict new process/IO admission. These cases
do not attribute background IO to a particular Prepare/Decide or simulate power
loss. Logical transaction-phase source is integrated at `755b8e5b`: twelve
before-submit/after-applied process cases bind genuine commands and native
entry/root/outcome/CDC evidence to same-original recovery and retained old cuts.
Recorded-seed replay uses fresh children and only minimizes the same independent
semantic failure; unavailable trials cannot establish a minimal case. The
repository-owned process watchdog now supplies the workload controllers' default
finite lifetime and cleanup owner. Transfer transitions are source-integrated at
`c6594a5a`: 34 phase/participant/page/side cases retain the genuine pre-fence
factory intent through drained authenticated leader reassociation, actual child
loss/replacement, terminal replay, old cuts and both activation lineages. The
fixed-profile placement checker now matches the actual split reservation ending
at ID1. Cut transitions are source-integrated at `a11f8c62`: eight actual
FreshFence/Certify participant/side cases retain a concurrent committed original,
same-request recovery and old-cut checks across child replacement. Repeated-round
controls are identified explicitly; they do not qualify first publication of a
new certificate. Native pin-call transitions are source-integrated at `d48bac42`:
30 feasible phase/participant/side cases retain the genuine issued request through
whole-cohort same-directory replacement, authenticated resume, old-cut reads,
terminal refusal and ordered cancellation. The checker joins complete native
prefixes, original control/business CDC, exact call identities and the independent
serial witness. Invalid, limit, corruption and unexpected joined errors cannot
qualify an interruption. These are authored cases, not executed recovery proof.
First-certificate publication is source-integrated at `424756d5`: four actual
Certify participant/side cases retain distinct empty, node-A and node-A+B cuts,
the stored first-publication index despite repeated replies, both immutable
business originals and same-directory old/current reads. Its independent checker
joins the complete native prefix and serial witness. Namespace publication is
source-integrated at `b94307e4`: eight actual OPTIONS rename and manifest-marker
Create/Close/directory-Sync sides interrupt same-directory Open before Ready.
The precise marker Sync selector requires the actual pinned native caller and
its created, successfully closed file; generic directory Sync remains separate.
The replacement reads genuine old/current cuts before catch-up and replays the
immutable original against the complete prefix/serial checker. These are process
interruption cases, not power-loss evidence. Recorded composition is
source-integrated at `d2a29188`: the six genuine two-conflict/fence/transfer
schedules admit bounded recorded Tick and same-original duplicate actions.
Native checkpoint vectors bind each action to its actual causal stage. Fresh
cohorts replay the recipe; deletion minimization requires the same independently
authenticated semantic failure. Missing evidence, expiry, failed cleanup and
resource refusal cannot qualify a counterexample or deletion. This is authored
controller/oracle wiring, not an executed soak or whole-D acceptance. The
P01–P09 source inventory identifies existing logical schedule coverage; no
additional B/C production method gap was found in that audit. Applicable
per-schedule storage conjunction remains in implementation. The internal seam
at `f18f9cf9` binds Ready persistence and application installation to the actual
unique entry, complete ordered entry-manifest hash and native batch payload
hash. Prepaid, immutable before/after Sync observations grant no operation ACK
or power-loss proof; unsupported/ambiguous/unfunded diagnostics leave native
persistence unchanged, and default filesystems do no diagnostic hashing.
Graph-driver wiring at `20aeba6b` adds 24 before/after Ready/install cells for
the six applicable cross-protocol commands. Exact native entry/batch receipts
precede real child self-SIGKILL; same-directory replacement, immutable original
recovery and held old cuts join the complete prefix and independent checker.
Only an all-zero response with an actual pipe-loss cause and the expected
persisted receipt can drop a failed Step delivery; native refusals, corruption,
limits and caller expiry still propagate. Hash/event work, serialized receipts
and parent receipt reads have separate finite grants. The state-batch seam at
`922ebf69` adds actual initialization/checkpoint/publication, import-page/seal/
retirement, activation and reclamation/cleanup identities, with exact complete
before/after metadata and batch commitments. Each observation follows one actual
atomic batch; import chunks and deletion pages retain distinct commits. Native
source/cut/manifest/plan coordinates grant no graph certificate or ACK authority.
Nil/default filesystems keep ordinary commits; optional diagnostics prepay their
encoding, hashing and by-value output. Authored guards compare exact prior/post
metadata and retain old data through activation, collection and dormant cleanup.
Reusable process wiring and broader applicable P01–P09 leader/message/conflict
composition remain in implementation. None of these new guards ran.
The first P06/native-batch conjunction Go driver is source-integrated at
`5c336e50`: 16 coordinator/outcome/Ready-or-install/before-or-after cases combine
first-YES leader loss, prepared-lock refusal and genuine endpoint conflict with
the selected Decide interruption and SAME-original recovery. Successful replies
and the exact unanswered request have separate bounded capture; the target's
actual pipe failure and persisted batch receipt precede interruption eligibility.
Fresh bootstrap, RPCs, snapshot catch-up and boundary waits share the original
deadline, with separate terminal cleanup; nil/default fixture behavior remains.
The independent checker/guards and future CI routes are source-integrated at
`7a82352a`: actual retained originals, complete source prefixes, exact old/current
sets and an independent serial witness authenticate the finite conjunctions.
No compilation, tests or CI execution has occurred. Recovery Prepare/Resolve
conjunctions are additionally source-integrated at `1162207a`: 64 separately
versioned cases preserve the original Decide recipe, bind the selected actual
participant and original authority proofs, and authenticate all stored originals
before terminal replay. ABORT may skip a participant already resolved after a
lost response; an unanswered submission never becomes a successful capture.
Terminal-replay conjunctions are source-integrated at `80810c3`: 48 separately
versioned cases bind ordinary Prepare/Resolve retries to the stored terminal
original, zero duplicate CDC and unchanged graph image. Actual lost Propose
replies remain distinct from returned append packets followed by a failed Step;
the checker joins native entry/term/packet identity and the killed incarnation.
Initial Register interruption is additionally source-integrated at `d8cdd83`:
16 fresh cases lose the actual registration submission before first YES, retain
the surviving coordinator's same private manifest, restore its native target,
then exercise first-YES leader loss, original decision recovery and complete
old/current/index/CDC observations. Replacement historical reads select both
child and parent routes to the actual local source; source review corrected a
parent-route mismatch before integration. Existing profiles and formats remain
unchanged. Initial first-participant Prepare interruption is source-integrated
at `59036e1`: 16 fresh cases retain the actual unknown reply. An after-install
Sync loss joins the selected entry to the surviving native ApplicationRecord
before mutation/retry, without inventing a successful source reply or second
crash. Other boundaries retain the actual later/repeated YES original. A shared
typed completion preserves the Register profile's original artifact/call/event
order and full original, old/current/index/CDC and same-directory replay checks.
Broader participant/leader/message variants, other P-row and applicable
publication/import/GC conjunctions remain source work. All five finite profiles
remain unexecuted; these checkpoints do not complete P06 or D. Source review
found that ordinary fixture reconnect rejects retained pending runs.
The correction is source-integrated at `0f6f10a`: fresh NCR1 routing retains the
same coordinator, private pending manifests, certified engine and unchanged-peer
streams, replacing routes only after all ownership and idle-work checks pass.
Ordinary reconnect still refuses pending runs; the original fixture deadline
and consumed source allowances remain. Three direct guards are authored, not
executed. The affected finite profiles still require runtime qualification.
The first actual state/process composition is source-integrated at `7081f9ff`:
twelve import Begin/Append/seal/activation/unclaimed-retirement/physical-clear
cells select the real native batch before or after Commit(Sync), persist its
exact observation, self-kill and recover the same directory. The independent
checker joins the actual AS5 stream, complete EOF, original request/prefix and
old/current serial witnesses. Physical-clear targets come from the same live
Begin observation and acknowledged import owner. Initialization is explicitly
unsupported by this finite recipe. Broader applicable
protocol/message/leader/conflict combinations remain implementation work;
no runtime qualification or whole-D closure is implied.
Actual publication composition is source-integrated at `cb483e5c`: before/after
Commit(Sync) self-kill/reap and same-directory recovery assert the exact native
compaction boundary, immutable request and local old cut before catch-up.
Application checkpoint persistence already co-installs with the native entry;
this does not invent a second graph checkpoint Sync. Retained-reader/GC
composition is source-integrated at `dd83b824`: a genuine reader acquired after
proposal and before quorum installation survives the committed floor and blocks
physical collection. After Close, before/after GC-page Sync interruption,
self-kill and same-directory recovery authenticate the same installed original
and native cursor through physical EOF. Old roots then expire while current
state and semantic anchors remain exact. This finite recipe claims member1's
physical collection and the all-A committed floor; broader applicable
protocol/message/leader/conflict combinations remain open. No new guard or
process case ran.

Native snapshot-to-feed ownership is source-integrated at `4868f0f7`. One genuine
issued pin retains the snapshot and full-vector feed; complete original-round
business groups, bounded protocol-prefix progress, immutable Page/ACK replay and
durably admitted operation ceilings share actual native authority. Snapshot
reads retain their admitted ledger and deadline until Close. This fresh semantic
agreement leaves older formats unchanged. The native OS-child workload and
matrix wiring are source-integrated at `1eb3df54`, with the exact point-revision
oracle correction at `7f67e7ba`. Genuine 1/8/32 writers retain snapshot C across
updates, replay Page/ACK after actual client loss and same-directory cohort
replacement, and join complete original, CDC, typed old/current and column/cache
answers. The local FDB comparison uses the same snapshot-plus-feed retention
contract; broader transfer retention and the remaining workload families still
need matching implementations. These cases and their finite resource grants have
not been executed. Full API tracing found that existing Issue already recovers
an unissued saved original; the bootstrap correction at `051ea682` clamps
Begin/Resume/Publish/Open to the saved feed deadline and configured limits before
native work, preserving separately funded terminal cleanup. Its authored recovery
and refusal guards remain unexecuted. The source-review correction at
`0de7a1ad` authenticates new retries against each keeper's actual acknowledged
cursor, preserving immutable original results after raw sidecar collection
without emitting duplicate business changes. Its authored guard distinguishes
that new retry from an expired historical frame. Source integration is not
runtime qualification.

Native retained-only feed ownership is source-integrated at `411f7069`.
Its fresh agreement installs an irreversible transition and protects a genuine
rolling ACK predecessor plus the unread native prefix. New token snapshot opens
refuse; held native views still prevent physical deletion. A private
unchanged-participant payload re-anchor requires the immutable PLM round and
full image to equal the authenticated ACK anchor; it grants no snapshot,
original-result or routing authority. Authored guards cover partial/lost
transition recovery, immutable retry refusal, held-view collection barriers,
actual GC, incoming-bank audit, reopen and the next unread before/after values.
Ordinary predecessor error coordinates and older feed formats remain unchanged.
The Go OS-child workload/GC lifecycle is source-integrated at `1afd0335`:
all six native collectors, an actual unread suffix across collection, SAME
Page/ACK after client loss, same-directory cohort recovery and a genuine post-GC
checkpoint column generation. C-dependent generations retire before collection;
setup transactions remain separate from useful create/update timing. Explicit
fresh snapshot allowances and common presence-row validation preserve the older
profiles. The independent sparse-source checker and owned launcher/matrix route
are source-integrated at `1c8f0b0`. Complete all-voter originals, sparse retained
prefixes, genuine checkpoint columns and physical collector cursors are joined
under finite grants. The matching FDB adapter and complete local paired join
are now source-integrated at `a32e34e`: fresh Cost10/Transfer3 uses actual HOME
reservation, recipient sessions and local Cursor.Next; the held C read occurs
after mutation, then authenticated physical-owner release and source collection
precede a genuine unread Page/SAME ACK through SDK SIGKILL/replacement. Complete
raw originals, old/current exact sets, physical deletion/anchor accounting and
actual event chronology precede every parity digest. Fresh interval columns
bind checkpoint Initial.Groups through Policy2/Generation2/RHOCLM03; old formats
remain unchanged. All 24 frozen source blobs, 15 complete parent blobs and 962
author-input hashes were verified before integration. Neither workload nor its
controllers/checkers has been compiled or executed. Native rolling-ACK retention,
additional generic At availability and installed-grant references remain distinct
from FDB detached CDC/physical reclamation and actual local Next costs. The
minimum-contract integration is not runtime or capacity acceptance.

Matched native retained-C feed and selectors are source-integrated at `59bcef6d`,
with CI dependency associations at `0138196b`. Explicit Cost13/14 and Spec2 apply
Set6 before same-life Correct, preserving ordinary Set and the old Cost5/6
contracts. Actual extra presence rows, endpoint-read footprints, old/current
revisions, complete feed/index/column observations and all-voter originals join
before local FDB parity. Business phase walls exclude setup and include worker
queues, retries and drain; native and FDB physical participant/allocator costs
remain distinct. Fresh feed terminal cleanup has an absolute original deadline
plus 30 seconds and 128 nonrenewable calls, with no new read/source allowance.
All 28 frozen blobs, 22 current parent blobs and 961 author-input hashes were
verified before integration. Direct guards and full native RTT routes are
source only; none has been compiled or executed. Complete owned local/RTT
comparisons for seven available workload families and the retained-only feed
variant are source-integrated at `b64b3ea2`, with the direct fixture correction
at `549166dd`. Explicit `--paired-owned` requires the actual owned FDB adapter
and finite transport/resource/spool grants. Both full raw oracles run in one
namespaced spool before every typed old/current, index, feed and before/after
answer join. Actual native scheduler and FDB ingress policy, tool/probe/process
EOFs, original deadlines and resource layers remain separately authenticated;
configured delay equality does not imply equal observed latency or physical
retention/allocator costs. Old local defaults remain unchanged. Nine final
source blobs, six complete current parents and 966 original author inputs were
verified; no checker/controller/guard was executed. The transfer pairing source
is integrated at `331ba938`: optional shared-spool FDB export follows complete
raw/original/lifecycle/GC/transport verification, then joins typed old/current
and acknowledged before/after values. Actual pre/post read/write groups and
processing authority remain engine-specific. The owned pair separately admits
native describe/import/undo/page grants and FDB catalog/census grants before
effects. All five frozen blobs and current parents were verified with whitespace
checks only; no Python import or execution occurred. The native Cost12 driver
and explicit matrix/CI routes are source-integrated at `0ea16cc2`. Real
Recipient/Cursor issuance, BOTH HOME moves, held-C reads, original-result
lookups, rolling-ACK collection, client replacement and cohort reopen share one
finite admission. All 23 source blobs, 16 full current parents and 874 frozen
input hashes were independently verified. The independent Python controller/
oracle is source-integrated at `d907d096`: genuine native commands and copied
original fragments join complete old/current queries, feed/ACK, collector and
process records before pairing. All seven final files and three complete
current parents were verified. The Recovery4 state-bootstrap helper remains a
separate integration dependency. The follow-up at `d70dee0a` distinguishes
useful business pages from move-lineage pages when selecting the unread update across collection and
client replacement; lineage is still emitted and acknowledged. Both complete
parent blobs and the two final files were verified. Integrated qualification
remains open. No compilation, workload, checker, test or capacity run occurred.

The native selector family is source-integrated at `330aaf89`: fourteen actual
Fresh/Stable/After/At, scoped-refusal and same-token reopen observations join
complete historical/current postings, typed points, all-voter raw originals and
native column/cache generations. Independent bounded joins authenticate actual
reservation/install originals and each fresh claim's installed grant. Whole
Open/Scan-through-EOF/Points wall times include retries and artifact callbacks.
The retained-C contract supports native local/1/20/100ms and paired local only.
Normal source cancellation and terminal cleanup have separate fixed admission;
the outer worker correction at `260b4a38` routes canonical `release` and `close`
through the reserved terminal lane, rejecting ordinary or malformed queries.
This source package has not been compiled or executed.

The scalar native workload factory builds fresh claims from authenticated,
installed grant references; it does not call `Recipient`/`Cursor.Next`.
The feed checker now reuses the selector's bounded reserve/install-original and
fresh-claim join at `edec78fa`, before raw source verification. Existing selector
wrappers retain their exact semantics. Reservation, installation, application
claims and cursor issuance remain distinct costs; neither planned numeric IDs
nor artifact hashes establish cursor issuance. These checker guards are authored,
not executed. Selector artifact bootstrap is corrected at `b8b3f141`: the
controller owns a fresh absolute engine directory and supplies the same path to
both output variables; existing or linked destinations refuse. Exact family cut
decoding is corrected at `09dd9653`: selectors admit their durable-column
agreement and ordered one/two-participant scopes; raw export uses its exact
admitted agreement and two participants. Cost5 retains its original decoder
contract. Ordinary nonunique property Set does not require remote endpoint lives
(SEM06); matched create/update rows require a present relationship and endpoint
life coverage at every percentage in both engines. Native workload integration
uses genuine planned checks while preserving ordinary Set. Count actual read/write
participants, not empty cross-protocol manifests or endpoint placement, before
accepting any affected scalar cost row.

The P07–P09 owned-state transition package is source-integrated at `17d69788`:
139 native boundaries cover move/pin Ready and application sync, allocator
reserve/install, collector sync and publication after actual collection. The
parent checks original retry outcomes, stale-owner refusal, actual held old-cut
values, full feed/ACK vectors and physical record reclamation after same-volume
restart. Its independent checker and guards are authored, not executed. These
cases select the existing Recovery3 transfer agreement; they cannot qualify
Recovery4 merely by changing a profile label. Existing accepted P08 evidence
retains its original scope. Source/hash, pinned parser/format and whitespace
checks passed; compilation, runtime, coverage and C acceptance remain open.

The Recovery4 counterpart is source-integrated at `d77378d2` and `b648da67`.
It reuses the same 139 boundaries with an explicit fresh artifact agreement.
Actual native submissions carry before/after observations from one view per
observation, exact immutable originals and a bounded same-key undo window.
The independent checker joins the complete native prefix, original outcomes,
live undo debt and every diagnostic-owner call. Consumed immutable undo rows may
remain retained; retiring debt does not claim physical deletion. A missing
service reply remains unknown until the same installed original is observed.
All four source hashes and complete current parents were checked; pinned Go
formatting and Git whitespace passed. Compilation and execution remain deferred.

The P01–P06 native storage composition is source-integrated at `10aa770f`:
524 finite adapter recipes bind actual original calls, native Ready/application
boundaries and same-directory replacement to the complete serial/cut oracle.
They cover scope-only certification, three prepare/fence orderings, durable
decision and partial-install recovery, frozen queued originals, historical
dependency mutations and first-live prepared-lock recovery. Conditional fixture
bootstrap and process-error masks now match their actual child agreements and
wire encoding. Complete P03/P05 traces use separately admitted bounded sidecars;
the main stream retains its full original-prefix census and EOF. All 16 frozen
blobs and 11 current parents were verified with pinned parser/format and whitespace
only. The P01–P06 leader/message extension is source-integrated at `a0f7e0f6`:
actual elections establish the original and rotated peers; delayed, reordered
and duplicated native packets retain their configured identities. Full
original, historical-dependency, queued-candidate and serial/cut checks use
authenticated process/election context. All 15 final files and 11 complete
current parents were verified. That extension covers P06's first-live parent.
The remaining recovery, terminal-replay, initial-register, first-prepare, Decide
and ordinary initial-batch programs are source-integrated at `488a738e`.
They reuse 184 literal cases and add 1,288 actual leader/message cases. Genuine
elections and successor observations bind the original source/member identities;
Ready cases use already-emitted Tick packets, while application cases use the
selected command's actual native packets. The existing complete semantic,
original, CDC, history and serial oracles receive separately authenticated role
context. All 15 source files, 11 complete current parents and 1,337 frozen input
blobs were verified with pinned formatting and whitespace only; nothing was
compiled or executed. Twenty local-leader Ready recipes
precede their own packet emission. Their separate service/predecessor companion
is source-integrated at `1b598c5b`: exactly 20 recipes × two actual leader layouts
× three delivery modes. Actual selected service delay, immediate same-service
cached predecessor retransmission or already-emitted Tick packets precede the
original Ready loss; no selected-command emission is fabricated. The complete
existing semantic/original/CDC/old-cut oracle precedes the independent companion
joins. All five frozen files, the complete current owner parent and pinned
format/whitespace were checked. P07–P09 state leader/message composition is
source-integrated at `e007efbe`: the 131 non-allocation recipes retain their
complete state/original/CDC/GC oracle under two actual elected layouts and four
message modes. Accepted P08 issuance evidence remains separate. GC uses a
genuine committed K predecessor, and publication uses its actual native local
proposal; neither invents a collector packet. The checker retains the original
deadline. All seven source hashes and complete existing parents were checked.
No compilation, execution or B/C/D acceptance is claimed. The bounded composed
exploration/replay controller is source-integrated at `73d18533`: all six real
causal recipes precede one or two recorded seeds and fresh exact replays under
one 16-trial, original-deadline and cumulative resource allowance. Only a
reproduced, independently classified semantic failure admits optional-action
deletion. Late resource/source refusals override earlier verdicts; missing final
measurements remain missing evidence. Guards are authored, not executed. This
finite component does not close fault applicability, sustained soak or B/C/D.
Sustained fixed-seed execution is source-integrated at `5d07c0ce` in the same
controller. Explicit admission preserves old 16-trial defaults and bounds the
fresh mode to 64 trials under one original deadline and cumulative resource
grant. Six causal cases precede recorded fresh-cohort replay pairs; both the
requested successful-round count and active duration must complete. Eleven
trial slots protect failure replay and bounded deletion. Exhaustion and late
cleanup refusal cannot pass. Source review and whitespace checks passed;
guards and sustained execution remain unexecuted. Fresh-cohort repetition does
not establish long-lived-store endurance or full D acceptance.

Actual snapshot-sender loss is source-integrated at `2203f916`: two cases kill
the captured sender process before or after the first nonfinal Append, after
the receiver has acknowledged Begin. The partial import leaves the old active
graph intact; actual Close, same-directory sender restart, stale send-handle
refusal and complete retry/activation precede the same original/history/CDC
oracle. Leader attribution uses actual leader-only ReadIndex calls rather than
the hard-state vote field. All seven source hashes and six complete existing
parents were checked with pinned formatting and whitespace. The first integrated
qualification checkpoint is recorded below.

Source expansion closed at `099c846c`. Focused correctness work now fixes
observed failures in the integrated implementation; new campaigns are not
required merely because other source remains unexecuted. The existing CI
selection/dependency associations are integrated at `4c336669`, with their
original workflow/run budgets unchanged.

The [first integrated checkpoint](evidence/v2-integrated-checkpoint-20261016/receipt.json)
records compilation and initial guard results. The [P06/sender follow-up](evidence/v2-integrated-checkpoint-20261016/followup-659bdad5.json)
passes the selected rotated/reordered decision case and both snapshot-sender
interruptions, including independent history/cut oracles. The [five storage leaves](evidence/v2-integrated-checkpoint-20261016/qualification-264ec502.json)
pass current/synced WAL recovery, WAL/manifest corruption and OPTIONS rename in Go
and explicit independent Python checks. This receipt corrects the earlier claim
that Go automatically invoked the WAL checker; original logs remain unchanged.

The [current query/recovery checkpoint](evidence/v2-integrated-checkpoint-20261016/qualification-b09a2784.json)
records the following finite scope:

- The 20-fact, 100%-cross, one-writer KV/local native workload completes all
  five old/current/reopened observations. Its complete original/source/voter,
  typed history and CDC artifact independently passes on integrated source.
  The original controller receipt that failed its old checker is preserved;
  the new proof is separate. Hydration now prepays bounded same-cut captures
  from one whole-query allowance, with unchanged per-invocation RPC cap and
  deadline. Its unoptimized read costs are recorded, not efficiency acceptance.
- Pooled publication and bootstrap/cleanup guards pass. The allocator proposal
  is captured before election, and initial/replacement snapshot roles agree.
  An explicit 19-entry D-only tail window admits the unchanged recovery pools
  within 256 MiB; native overflow backpressures and drain preserves the owner.
  Import0 now preserves actual installed-prefix totals separately from its
  authenticated immutable source manifest. Focused import/reopen/tampering
  checks pass. Collected, span and batched moves complete with exact retained
  old/current answers; batch reducers validate ownership once on the same
  captured view while retaining all per-record charges and original limits.
  The process case remains open. Recovery now catches up the actual committed
  and applied prefix without requesting allocation authority while a move is
  fenced. This grants no new read certificate or current-leader authority.
  The next allocator transfer preserves the exact observed native view count,
  view bytes and pins on the same process incarnation; direct added/dropped-owner
  guards and scoped race checks pass. The actual case advances through both
  corrections and the exact changed-K refusal. A preceding SAME-K replay now
  waits for its actual newly installed Resolve originals on all six voters,
  keeping the changed-K zero-effect assertion strict. Capture locks release
  before voter RPCs. The next repeated held snapshot projection reaches a
  source resource limit: the same retained endpoints reach their cumulative
  1,024-call limit while row, byte and output budgets remain available.
  No full state-case pass is established. All eight owned processes exit without forced
  cleanup. Six focused import/replay parents also pass
  race checks; their partial coverage does not establish the full coverage gate.
- FoundationDB's first actual prebuild exposed seven compiler diagnostics.
  Their fixes compile; the pinned tool build additionally needed `xz-utils`.
  Exact Docker capability spellings and inert kernel-default links now pass
  strict transport admission. The actual 20-fact KV/local counterpart completes
  43 commits with no aborts; its complete original/history/CDC/transport oracle
  passes an independent root replay with an identical proof. Terminal and
  cleanup exit zero, and the original receipt remains unchanged. This is one
  standalone correctness component. A fresh native controller using the same
  run identity now also passes the complete existing pair comparator: 40 typed
  answers agree, including retained history/original/CDC checks, with all 13
  native process identities exited. Root independently replays the full pair
  to the identical proof. The [columns counterpart](evidence/v2-integrated-checkpoint-20261016/columns-root-review-49f295f3.json)
  also passes the full FoundationDB oracle and independent root replay, with
  successful cleanup. Native columns' original old-seal failure is preserved
  with all seven owned processes exited. Both constructors now account for the
  complete native key/value read while retaining the configuration value limit;
  root's five focused codec/publication/reopen/retirement and old-cut parents
  pass. Allocation metadata preserves exact CDC and originals without business
  rows; durable generation and peer binding retain their actual owner. The
  corrected native columns workload initially completes in 61.65 seconds;
  its original controller receipt remains failed because the old checker
  conflates Close and CancelIssue for the same pin. The checker now retains
  each authenticated action original separately; the complete unchanged artifact
  passes independent root replay. A fresh controller then completes in 67.38
  seconds, including verification, and the full native/FDB column comparator
  matches 40 typed answers. All 13 owned native processes exit without forced
  cleanup; the original FDB and failed native receipts remain unchanged.
  The eight-writer KV tiny pair also passes the full comparator
  and independent root replay: 40 typed answers, all 13 native process identities
  exited, FDB 43 commits/51 aborted attempts and successful cleanup. Larger
  runs and comparative cost remain open. Selected rotated/reordered P01,
  P02 and first-live P06 cases now pass complete native execution and independent
  full-artifact checks; P06 retains its exact prior native originals and the
  edge/close-a serial witness. A newly executed P03 case does not reach its
  selected native batch and remains unqualified. P01 keeps the held source on surviving voter 2 with its original
  grant; only an unrelated fresh caller moves off its two-worker source. The
  relay omits success payloads on actual transport errors, and strict checkers
  preserve actual caller roles, native GR3 root modes and P02 phase coordinates.
  P04 preserves the original recipient owner. Its real capacity request
  returns authenticated graphstore/graphstate resource-limit causes before the
  first candidate, reproduced by a genuine in-process native capacity read.
  Single canonical kernel inspection now removes the duplicate decode under the
  same original grant, with 100% coverage of the combined inspector and scoped
  race passes. The actual case advances through both candidates and the selected
  kill; its next read remains unavailable. P05 reaches the selected real process kill and healthy quorum
  apply, then exhausts the SAME held source's 1,024 cumulative calls during its
  second full observation; row, byte and output allowances remain available.
  Invocation-local scalar and complete-page reuse preserves the original
  source/cut/lifetime and budgets. P05 now completes the native workflow but its
  checker must validate captured GSQ/GSO controls alongside transactions; state
  now passes that held projection using bounded token-scoped exact source-Get
  reuse, then fails a later feed-page read. P05 GSQ/GSO and Register projection
  joins now pass the full oracle; a fresh complete ABA case and four additional
  short representatives pass, while a later Resolve recipe remains open.
  The P04 observer now charges its unchanged candidate bound only to actual
  capacity traffic, with its original-case rerun pending. All open failures retain their
  original deadlines and quotas.

These components do not close their full fault families, the process inventory,
engine comparison, measurements or V2. Original deadlines, quotas, complete
matrix dimensions and A–F acceptance requirements remain in force.
FDB retains stable
logical-home key namespaces while its catalog fences and transfers processing
authority; these prefixes do not establish physical server placement. Source
review found no ownership violation requiring key relocation. Native movement
relocates current graph data, and both HOME moves make its guarded updates local.
Pair equivalent logical answers, retained cuts/originals and authority semantics;
report each engine's actual pre/post business read/write footprint and physical
cost separately. Do not infer equal physical participants from nominal cross
percentage or add copies solely to imitate native placement.
The program adapter and native driver share the explicit Recovery4
fixture-selector handoff integrated at `0ea16cc2` and `a0f7e0f6`; the actual
bootstrap agreement binds state/retained-source validation. Old Recovery3
fixtures retain their agreement. The native transfer workload and independent
checker are source-integrated at `0ea16cc2`, `d70dee0a` and `d907d096`.
Shared graph batching, recovery byte pools and range-move pool admission are
source-integrated above. These sources compile but remain runtime-unqualified;
the pooled-state counterpart is integrated above. A complete
static 1M workload bound is not a prerequisite to authoring its driver. Preserve
all required matrix cells and their capacity gates: safe resource refusal does
not qualify a successful cost cell, and an incomplete upper bound proves
neither capacity nor infeasibility. The
coordinator owns the inventory, source review, integration and acceptance
decision. Keep one active source package per lane. Implement in parallel, but
bound concurrent local correctness jobs by their combined CPU/RSS/disk grants;
run comparative cost measurements on a quiet host under the shared resource lease.
Finish the full remaining V2 implementation before execution: correctness tests,
then measurements, then demonstrated improvements. Running CI is not a source
integration gate. Do not rerun unchanged full suites per patch. Report integrated/accepted packages and remaining
requirements, rather than a percentage inferred from the number of tests written.

The [latest native 10k run](evidence/v2-native-capacity-probe-20261014/receipt.json)
reached its fixed 30-minute limit before final
historical observations. Its ACK work dominates captured request timing. The
[bounded ACK timing and poll change](evidence/v2-ack-poll-20261014/receipt.json)
passes the exact 17-fact local cell, routing/marker guards, normal/race/scoped
coverage and affected static checks under Go1.26.9. Queue/Step timings overlap
client timings; the earlier Go1.27 runs are historical only. No speed improvement
or 10k cause is qualified. Establish the measured cause and complete one matched
10k cell before launching
larger/repeated cells. Preserve the full matrix and final history oracle.
The base matrix alone is 180 runs per layout, before workload/RTT variants;
this is substantial remaining execution, not an almost-finished gate. V3's
compact-engine ledger, V4 public/consumer APIs and V7 vendor comparison remain
their own phase obligations. No additional machines or owner input is needed
to continue under the approved single-machine deployment.

V0 specification acceptance, 2026-10-10:

| Criterion | Accepted evidence | Remaining phase |
|---|---|---|
| Contracts/ownership and IDs/cuts/formats | [PLAN §§2–5/8](../docs/v5/PLAN.md) separates preservation, native access and sigma evaluation, with explicit failure outcomes | V1 implementation; V2–V5 database/storage/access |
| Independent corpus/models | [Reference validation](../docs/v5/reference/independent/validation.json): 38 tests, unchanged 16/52, 42 revised records, 18,192 pairs, 600 mutations, 88 accepted/39 refused protocol histories; ordinary freshness check passes | V1 Go differential/attachments; V2 durable fault/cut evidence |
| Units and compatibility | PLAN §2.3: Q/ms/POSIX1970 default policy, exact integral-ms Instant codec, explicit other axes and conversion/refusal boundaries | V1 default-axis factory/binding; V2 persistence; V6 importer |
| Versioned consumer/data inventory | [Pinned doors](../docs/v5/reference/consumer-contracts.json), [migration obligations](../docs/v5/reference/consumer-migration.json), PLAN §8a `95a1de39`/v4.50.0 delta at acceptance and declared datasets | V4 actual adapters; V6 migration; V7 representative runs |
| Resource profiles and limits | [Comparison protocol](../docs/v5/reference/graph-db-comparison.json): future native test configurations, one-factor/pinned-mixed dataset controls, separate local functional lanes; initial numerical limits distinguished from proposed performance thresholds | V2–V3 fault/cost evidence; V7 measured comparative capacity |
| Import provenance | PLAN §9 and reconciled [v4 handover](v4-changes-for-v5-importer-20261010.md): verified writer/format provenance; dates/one-tick width alone are insufficient | V6 implementation/ambiguity report |

Fresh `check.py --write-validation`, ordinary `check.py` and original
`design_checks.py` pass. Original design checks retain 16/52, 20,000 domain pairs,
480 schedules/80 certified-read examples and the weak-cut counterexample.
Corpus/oracle/consumer pins are unchanged. This closes specification V0 only;
declared resources are test configurations, not deployed/proven capacity or an
owner SLO. No solver, engine, consumer-integration or performance acceptance follows.

V1 value/state acceptance, 2026-10-10: independent source and criterion reviews
pass on the v4.50.0 rebase. The unchanged 16-case/52-assertion corpus and revised
rho-owned cases include actual graph attachments and retained-view tests; symbolic
placement remains an explicit native type/adapter exclusion, with no sigma solver
execution claimed. Go metadata/CDC limits have independent exact-cost controls;
they are not heap/RSS or comparative capacity measurements.

Go 1.26.9 `make fmt-check`, `make check`, full `make test-race` and `make cover`
pass. Final test additions were rechecked with the graphstate race suite and full
coverage: 20,005/22,595 statements (88.5%), all new public methods 100%, no changed
production file below 80%. Pinned golangci-lint v2.13.2, gosec v2.29.0 and
govulncheck v1.7.0 pass. Three 10-second fuzz campaigns pass: opaque descriptors
1,254,206 executions, graphstate/history parity 12,781 and state codec 1,392,848.
The final validation input manifest is SHA-256
`635a1e46131227dffd014f7da133d251fadabf1ae79d4052db4823ee14a0b2c3`;
the coalesced coverage profile is
`e072d3037d7aa8b6ef7b90048f872bad1e53bbbb77daefec3ec05284d0de7ba2`.
These close V1 only. Hosted [CI run 38073307322](https://github.com/data-insights-ai/rho-tkg/actions/runs/38073307322)
also passes on `725a8b78`, including both modules' race, security and coverage
jobs; push-only lint is skipped there and covered by the pinned local run above.
Engine/distributed integration, consumer execution and measured capacity retain
their later acceptance gates.
V2 is active. Independent later-phase components may proceed in parallel;
V3 format freeze/performance tuning and each phase's acceptance still require
their documented predecessors. The V6 [retained-state preflight](../tools/v5-migrate/README.md)
is integrated for both nodes and relationships. Its [qualification](../tools/v5-migrate/evidence/qualification.json)
records passing normal/race/build/vet/format and compiler-matched pinned scanners,
96.2% coverage and 100% coverage of both public functions against the v4.50.1
reader. Independent review matched all six input hashes, 462 source files and
14 raw command receipts. Main v4.51.0 has since been integrated; its
[hosted preflight job at `3ede096d`](../tools/v5-migrate/evidence/hosted-main451-3ede096d.json)
passes all ten narrow gates, retaining 96.2% coverage and both public functions
at 100%. The combined root/v5 CI is still running at this checkpoint. This preflight owns raw
evidence and refuses ambiguous histories; destination import and consumer gates
remain open.

V2 default-axis genesis prerequisite accepted, 2026-10-10: one graph's two
declared partitions, with three disk-backed voters each, preserve the exact
HOME-selected default descriptor through leader change, retry, reopen and actual
snapshot activation. Wrong or missing retained-view bindings fail closed.
Parent build/vet/normal/race/coverage and pinned lint/security/vulnerability gates
pass; final combined coverage is 88.7%, every changed production file exceeds
80%, and all three new public helpers have direct tests. The additive registered
intent codecs remain inert. This checkpoint grants no graph-write or certified-cut
authority and does not close V2.
Hosted [CI run 38076813365](https://github.com/data-insights-ai/rho-tkg/actions/runs/38076813365)
passes on `55dc0e30`, including both modules' race, security and coverage jobs;
push-only lint is skipped there and covered by the pinned local run above.

V2 completion sequence: accept each package only after independent review and
validation. Non-overlapping implementation and comparison work may run in
parallel, with one owner of the bounded local heavy-test resources at a time.

1. **Accepted 2026-10-11 — checked partition-local graph writes.** Both declared
   partitions of one GraphID admit typed writes, preserving canonical, declared
   and current-presence indexes plus complete ordered CDC. All nine dependency
   kinds have checked authority; unknown/remote/stale/corrupt authority refuses
   without partial graph effects. Shared consumptive representation admission
   covers request ownership through result composition without widening caps.
   Retained histories, six-voter disk reopen, nonempty snapshot activation and
   leader failover pass. Independent review found and verified corrections for
   fresh scalar-copy admission and regressed ownership epochs. This closes the
   local-write package only; a local miss does not establish global absence.
   [Acceptance evidence](evidence/v2-partition-routing/acceptance.json) records
   the full-module normal/race gates, final 23,237/26,280 statement coverage
   (88.4%), direct public coverage, pinned static/security checks and eight
   independently executed correction/recovery controls. The additive captured-
   history API commit has its own normal/race and final full coverage checks;
   the earlier whole-module race applies to unchanged existing code. No physical
   multi-host, cross-partition transaction or capacity acceptance is implied.
   Hosted runs [38099226753](https://github.com/data-insights-ai/rho-tkg/actions/runs/38099226753)
   and [38101440673](https://github.com/data-insights-ai/rho-tkg/actions/runs/38101440673)
   passed the v5 jobs but timed out in the v4 core race package; both terminal
   failure logs are retained. Removing the explicit two-CPU override alone was
   insufficient. CI-only `fd37fa0a` also permits four test scenarios on the
   four-CPU runner, preserving package serialization, memory policy, all tests
   and timeouts. Its [hosted run 38103685212](https://github.com/data-insights-ai/rho-tkg/actions/runs/38103685212)
   now passes both modules' normal/race, coverage and security jobs plus comparison
   correctness. Push-only lint is skipped and covered by the accepted local
   pinned run. This closes the hosted scheduling follow-up for this checkpoint;
   later cross-partition work still requires its own combined validation.
2. **Accepted 2026-10-11 — cross-partition graph prepare/decision/resolve.**
   One GraphID, two declared partitions and three disk-backed voters each now
   install actual graph dependencies/effects, including both cross-edge endpoint
   postings, through durable prepare/decision/resolve. Prepared kernels and locks,
   immutable decisions, exact retries, retained history/CDC, snapshot activation,
   failover and reopen pass. Installed and queued reservations protect completion;
   admission refusal never substitutes for an authoritative abort vote.
   The same independent reviewer verified both blocking corrections: transaction
   IDs now bind graph/coordinator/original retry key, and pending-install admission
   charges native recovery envelopes. The separately arriving AS5 contract reader
   preserves the complete recovery policy.
   [Acceptance](evidence/v2-cross-partition/acceptance.json) and
   [final combined evidence](evidence/v2-cross-partition/combined-final.json)
   record 27,153/30,788 statements (88.19%), all 46 changed production files
   above 80%, all 47 added exported declarations directly exercised, exhaustive
   normal/race scope and passing pinned scanners. Six unchanged package/dependency
   profiles are carried by verified hashes; all six affected packages were rerun.
   [Original failures and corrections](evidence/v2-cross-partition/review-corrections.json)
   retain actual REDs and qualified fixture failures. Earlier checkpoint archives
   remain available in the same evidence directory. This closes this write
   package only; certified distributed reads, multi-host durability, capacity
   acceptance and full V2 remain open. [Hosted CI at `c61d1124`](evidence/v2-cross-partition/hosted-ci-c61d1124.json)
   passes for the integrated package-2/main/mapping source; the conditional
   pull-request lint job was skipped, with author lint recorded separately.
3. **Accepted locally 2026-10-11 — certified current and retained graph reads.**
   Fresh/Stable/At/After bind graph, exact scope, topology, configuration and
   durable transaction evidence on the actual two-partition/six-voter path.
   Read fences remain separate from installed rounds, and unresolved prepares
   cannot be hidden by larger observed rounds. Routing and graph changes retain
   verified round-prefix mappings with bounded predecessor seeking.
   Mutation-then-old-cut tests preserve nodes, relationships, endpoint lives,
   properties and both adjacency directions; retained uniqueness candidates are
   checked with exact residual projection. Followers wait for certificate
   application; reopen and genuine snapshot activation preserve held old views.
   Scope, lifetime, cancellation and cumulative source/output limits refuse
   without partial views or poisoning a healthy replica. Same-reviewer corrections
   cover an early ABORT's never-prepared participant, scope admission before
   allocation, and After receipts from a different immutable authority.
   [Acceptance](evidence/v2-certified-reads/acceptance.json) pins source, exhaustive
   normal/race lanes, qualified unchanged-source carry, coverage, pinned scanners,
   direct-method tests and independent review. This accepts internal read
   machinery, not a public graph API, physical multi-host durability or full V2.
   [Hosted CI at `0f430e44`](evidence/v2-certified-reads/hosted-ci-0f430e44.json)
   passes all v5 jobs and comparison correctness; the overall run fails because
   the unchanged v4 core race package reaches its ten-minute timeout. The earlier
   scheduling adjustment did not eliminate that recurrence; its terminal failure
   log remains retained. The reviewed [shared race dispatcher](evidence/v2-certified-reads/root-core-batches.json)
   assigns the compiled core Test/Fuzz/Example inventory to four exhaustive,
   sequential CI batches, keeping each parent and its subtests together. The local
   default, all other packages, race flags and resource/time limits are unchanged.
   Its 22 dispatch/error guards and static checks pass. [Hosted CI at `5edb2792`](evidence/v2-certified-reads/hosted-ci-5edb2792.json)
   now passes all applicable jobs, including all 2,174 core names in groups of
   544/544/543/543 and the other 44 root packages; push-conditional lint is skipped.
   This closes the CI timeout follow-up. It covers package 3 and preclaim
   integration, not the later registry materializer or unfinished process transport.
   Subsequent [prepared-snapshot preclaim integration](evidence/v2-certified-reads/preclaim-integration.json)
   accepts external `0ebb025f` with the existing AS5/recovery contracts preserved:
   13 gates pass, the new Check is directly covered at 100%, and a genuine AS5
   reader/claim/activation guard passes. This nonmutating check creates no
   readiness or activation authority and does not advance package 4 acceptance.
   External `637e1816` is now [integrated locally with focused unit acceptance](evidence/v2-certified-reads/registry-materializer-integration.json):
   the private registry materializer preserves reciprocal originals and validates
   Created/Original/Conflict outcomes. All 50 selected tests pass normal/race;
   eight finite gates pass, with codec/materializer coverage 96.2%/88.7%.
   The [finite process-orchestration acceptance](evidence/v2-process/first-slice-acceptance.json)
   now closes its combined affected graphapply normal/race/coverage and static
   gates. Driver/factory/preclaim/Host readiness remains unwired and unaccepted.
   **Prepared-snapshot audit integration accepted locally**, at `f8b24261`,
   incorporating external `2a7d5144` with existing AS5/recovery hooks preserved.
   Three reproduced defects are corrected: AD4 descriptor parsing, stable
   recovery-entry work admission, and retained-prefix work after a real
   post-install restore failure and reopen. Short budgets refuse before claim;
   only actual consensus Ready activates an audited snapshot. The
   [acceptance record](evidence/v2-prepared-audit/acceptance.json) pins 438 source
   files, 108 replica and four graph regression parents passing normal/race,
   exact six-child cleanup, direct-method/changed-code coverage above 80%,
   pinned scanners and the same independent reviewer. Unchanged raftlog evidence
   is explicitly carried; rejected hypotheses are not regression proof.
   Logical work reservation does not increase the owned-memory bound. Host,
   factory and registry readiness remain unwired; remote certified cuts and
   full package 4 remain open. [Hosted CI at `b9da041b`](evidence/v2-prepared-audit/hosted-ci-b9da041b.json)
   passes all applicable root/v5 test/race, coverage, security/vulnerability and
   comparison jobs; push-conditional lint is skipped, with pinned author lint
   recorded separately. This closes the integration's hosted-CI follow-up only.
4. Pass the same-graph E15–E18 serial-history/cut oracle under process and message
   faults, recovery and ownership transfer; complete the equivalent transactional-KV
   comparison and record the engine decision. Only then can V2 close under
   PLAN §8; V3–V7 retain their own acceptance requirements.
   The [first six-process counterexample](evidence/v2-process/first-slice-red.json)
   is independently reviewed: local controls pass in normal/race, while the
   cross-edge acceptance fails with the correct local remote-participant refusal.
   Neither graph nor CDC has partial effects; subsequent writes remain healthy
   and all six children are reaped. The archived test patch establishes the next
   missing orchestration path; it is not installed as a failing branch test.
   Its quiescent local observations do not establish certified cuts or E15–E18.
   **The finite six-process orchestration slice is accepted**, 2026-10-11,
   at `883b6122`: bounded authenticated retained sources and a child-owned
   coordinator complete the cross-edge through native prepare/decision/resolve.
   Original intent and durable decisions survive uncertain replies; absent
   authority remains pending. The [acceptance record](evidence/v2-process/first-slice-acceptance.json)
   pins 640 passing test names in each normal/race mode, four qualified child or
   measurement entries, 88.10% coverage, direct-method witnesses, two killed
   implementation mutants and passing static/scanner gates. Six graph callers
   use leader sources 1/4; all six native voters separately verify outcomes,
   CDC, retained roots and cleanup. This closes the first orchestration slice,
   not remote certified cuts, E15–E18, ownership/GC, physical-host durability or
   the engine comparison. Those remain the next package-4 acceptance work.
   **CI cancellation/completion follow-up closed:**
   [hosted CI at `dbb27c69`](evidence/v2-process/hosted-ci-dbb27c69.json) exposed
   a test-fixture cancellation/completion race; root tests were cancelled by
   that matrix failure. The [reviewed correction at `2d51bfea`](evidence/v2-process-ci/acceptance.json)
   waits for the original worker terminal within a finite bound. All 189
   production files are unchanged. Eleven focused tests pass in normal/race
   modes on macOS and Linux/arm64, including four six-child scenarios and
   exact cleanup; the original RED, mutant and static gates are preserved.
   [Full hosted CI at `7e9caf6f`](evidence/v2-process-ci/hosted-ci-7e9caf6f.json)
   passed root/v5 build, vet, normal/race, coverage, security/vulnerability and
   comparison gates. Push-workflow lint is PR-only; the separate pinned author
   lint pass remains recorded. This closes the fixture CI follow-up only.
   **Remote E15 certified reads accepted locally**, 2026-10-11, at
   `07520fb7`: after durable COMMIT and only A's resolution, a second process's
   Fresh read resolves delayed B before returning the exact complete graph and
   both endpoint postings. Nonempty retained cuts remain unchanged. Leader
   sources 1/4 serve the reads; all six native voters verify persisted
   certificates, original outcomes/CDC and cleanup. The
   [acceptance record](evidence/v2-remote-e15/acceptance.json) binds the source,
   same independent reviewer and 249 sealed evidence artifacts, including the
   explicitly tracked final coverage profile. Normal and race inventories each
   contain 676 passing parents and four qualified child/measurement skips;
   71 affected parents also pass both modes after mechanical scanner fixes.
   Coverage is 18,517/21,273 statements in the affected packages, all 14 changed
   production files exceed 80%, and three deliberate implementation errors are
   detected. Pinned static/security gates pass. Six reproduced correctness
   defects are closed, including scope-refusal poisoning and control-work
   undercharging. E16–E18, ownership/GC, physical hosts, engine comparison and
   full V2 remain open. [Hosted CI at `30dd0881`](evidence/v2-remote-e15/hosted-ci-30dd0881.json)
   passed root/v5 normal, race, coverage and security gates plus comparison
   correctness; PR-only lint is covered by the pinned local pass.
   **Remote E16 accepted locally**, 2026-10-11, at `01f38811`: actual
   prepared votes at floor 89 remain unresolved after a read observes fence
   100, so certification refuses until the original COMMIT at 90 is resolved.
   Fresh then includes that transaction; held and reopened old cuts retain the
   exact prior graph. The [acceptance record](evidence/v2-remote-e16/acceptance.json)
   binds the same independent review, seven passing race parents and a deliberate
   pending-outcome error caught by the new test. All six child processes are
   successfully reaped. This is test-only evidence: 196 production/module files
   match the accepted base, so unchanged E15 validation is carried explicitly.
   [Hosted CI at `40e12ef6`](evidence/v2-remote-e16/hosted-ci-40e12ef6.json)
   also passes root/v5 normal, race, coverage and security gates plus comparison
   correctness; PR-only lint remains covered by the pinned local evidence.
   **Finite E17 range handoff accepted locally**, 2026-10-12: native admission
   fences, bounded content/history transfer, original durable decisions and
   resumable placement activation preserve IDs, typed values, component revisions,
   endpoint incidence and held/reopened old certified cuts. The
   [acceptance record](evidence/v2-range-e17/acceptance.json) binds 517 Go files,
   749 passing parents plus four qualified skips in each normal/race mode,
   all 43 changed production files at least 80% covered, 22 directly tested
   exported declarations and two detected implementation omissions. Three
   test-only assertion amendments have fresh normal/race evidence; unchanged
   production evidence is explicitly carried. Six-process schedules include
   target-follower kill/restart during import and exact child cleanup; leaders
   remain live. Partial activation and delayed stale writes are also checked.
   The 1,024 content-page ceiling, required original keeper groups and unrun
   large measurements remain explicit limits. Hosted checkpoint run 38189378747
   reached the graphapply package race timeout; the reviewed
   [four-batch dispatcher](evidence/v2-certified-reads/v5-graphapply-batches.json)
   preserves the complete compiled inventory and existing limits.
   [Hosted CI at `13612eb3`](evidence/v2-range-e17/hosted-ci-13612eb3.json)
   passes all seven applicable root/v5/comparison jobs, including both exhaustive
   race gates; PR-only lint is covered separately by pinned local evidence.
   A later hosted run at `fae9d0d9` exposed the process helper refusing its own
   10-second invocation cancellation. The [reviewed correction](evidence/e17-ci-deadline-20261012/review.json)
   resumes the same original terminal Pending and still requires actual COMMIT
   and both Finish statuses. Seven focused parents pass normal/race, with exact
   placement CDC and complete child cleanup; fmt/vet and pinned lint pass.
   All 218 production Go files and the time/resource bounds are unchanged.
   [Hosted CI at `f9e0eb63`](evidence/e17-ci-deadline-20261012/hosted-ci-f9e0eb63.json)
   now passes all seven applicable root/v5/comparison jobs, including both
   exhaustive race gates. Push-only lint remains covered by the pinned local run.
   **Finite remote E18 accepted**, 2026-10-12: unique-property and
   endpoint indexes supply candidates for same-cut typed value/component/life
   hydration on six native OS processes. Held and reopened historical cuts retain
   exact pre-correction answers; current reads reject stale effective matches.
   The [acceptance record](evidence/v2-index-e18/acceptance.json) binds 520 Go files,
   22 passing parent tests in each normal/race mode, an isolated index-omission
   check, pinned static/security gates and complete child cleanup. All production
   code and source/session limits are unchanged. The fixture uses seven explicitly
   admitted read operations and a stated aggregate IPC allowance, not a production
   cost guarantee. [Hosted CI at `47d98010`](evidence/v2-index-e18/hosted-ci-47d98010.json)
   passes all seven applicable jobs, including 438 selected graphapply names in
   four race batches and the 11 other v5 packages. Broader selector/scan parity,
   ownership/GC, physical-host durability, engine comparison and other V2 exit
   requirements remain open.
   A focused [native P08 range guard](evidence/v5-p08-native-range-companion-20261012/README.md)
   now verifies unused-ID refusal after completed ownership transfer with the
   recipient still current, delayed grant recovery, exact retries and old cuts.
   Final focused normal/race checks pass; removing the Life allocation fence
   makes its assertion fail. Production is unchanged. This uses six in-process
   voters; the separate **native P08 process scope is now accepted locally**.
   Its [acceptance record](evidence/v2-allocation-p08/acceptance.json) verifies
   child-owned cached issuance, pre/post-quorum reservation crashes, delayed grant
   delivery, recipient/allocator restarts, stale Entity/Life/Value refusal and
   exact committed graph retries. All 52 selected tests plus one fuzz seed pass
   in each normal/race mode; the recipient-session omission is detected. Production
   is unchanged. Combined [hosted CI at `fee9b82d`](evidence/v2-native-graph-lifecycle/hosted-ci-fee9b82d.json)
   passes root/v5 test/race, coverage, security/vulnerability and comparison
   gates; push-only lint is covered by the pinned local runs. Full V2 stays open.
   The [local FoundationDB graph fault launcher](../tools/v5-substrate/fdb/evidence/launcher-20261012-review.json)
   now passes real client kills, one-zone-loss progress and all-server same-volume
   restart, with exact retained history/CDC and bounded containers. Its 29 native
   race tests pass; this is local functional evidence, not cross-engine parity.
   The [native seven-step lifecycle trace](evidence/v2-native-graph-lifecycle/acceptance.json)
   is now accepted as fixed functional comparison input: six OS processes,
   atomic edge/property creation, correction, endpoint/edge closure, fresh-life
   reopen, retained identity/property, exact CDC and held/reopened old cuts.
   All eight selected parents pass normal/race; production is unchanged. The
   entity census covers issued IDs 1–32 and the declared fixture components;
   the held read is explicitly edge-only. The [matching FDB lifecycle comparison](../tools/v5-substrate/fdb/evidence/lifecycle-20261012-review.json)
   is now accepted locally: actual stored history, physical edge/out/in records,
   original outcomes and historical cuts produce equal answers for all seven
   operations, eight logical changes and four old reads against both native
   traces. All 31 FDB race-test parents pass, with 83.8% module coverage and
   changed statement-block coverage above 80%; the v5 library is unchanged.
   The [paired FDB fault fixture](../tools/v5-substrate/fdb/evidence/paired-final-20261012.json)
   now passes 33 normal/race parents, pinned static checks and 84.38% new-file
   statement coverage. Its actual six-operation fault trace independently passes
   the FDB-side checker, including original outcomes, complete CDC and retained
   reads after same-volume reopen. The native paired trace and cross-engine
   equality remain pending. The final test configuration is bounded at 310 seconds;
   the historical external-fault receipt retains its actual 490-second setting.
   Next is native paired local fault-trace adjudication, followed by the comparable
   cost/layout matrix and engine decision; physical-host and other V2 exit
   gates remain open. The [internal creation/reopen factory](evidence/v2-graph-lifecycle-admission/receipt.json)
   now mints new logical GraphIDs internally, provisions identical candidate genesis
   through trusted one-time embedding-authenticated per-voter provisioning,
   and reuses rho's durable pending ownership seed and actual committed Init as
   its provisioned/initialized states. Pending seeds bind identity, membership and
   declaration; schema/default-axis/routing/maxBlock choices become immutable only
   through committed Init. Five selected parents pass normal/race, with 84.55%
   new-file coverage and scoped vet. Tests exercise both graph partitions, partial
   provisioning, retained node/relationship history and allocator high-water after
   reopen. Integrated factory gates pass in the hosted CI recorded above;
   surviving-quorum-authorized replacement remains open. Its
   [pending admission component](evidence/v2-graph-lifecycle-admission/replacement-admission-receipt.json)
   now binds an actual native quorum-read completion to the authenticated source
   session and replacement voter/incarnation after embedding-managed retirement.
   It creates a cancellable pending owner; it does not create a receiving Store,
   activate a snapshot or expose a ready Host. The 101 selected tests and a
   separate native compaction/refusal/retry test pass normal and race runs;
   new-file coverage is 88.8%, with scoped vet passing. This includes the existing
   E18 six-process compatibility test, not whole-package or physical-host acceptance.
   Hosted integration at `a931071d` [failed](evidence/e17-ci-deadline-20261012/hosted-ci-a931071d.json)
   in the crash-test helper: closing stdin before SIGKILL allowed a clean child
   exit. The [ordering repair](evidence/v2-process-shutdown-order/receipt.json)
   preserves the strict killed-child assertion; its direct regression and 15
   unchanged process-test parents pass normal/race. Hosted integration at
   [`995f95d7`](evidence/e17-ci-deadline-20261012/hosted-ci-995f95d7.json)
   passes all eight applicable jobs, including both modules' normal/race,
   coverage and security gates, migration and comparison checks. The actual
   race inventories include the previously failing parent and its new ordering
   regression. This confirms the repair and receiving/progress integration;
   the later snapshot auditor and replacement activation retain separate gates.
   [Receiving-only term persistence](evidence/v2-application-receiving-20261012/receipt.json)
   is integrated: an empty bound store durably records a genuine receiving term
   while retaining inactive imports and refusing active membership/application.
   It grants no replacement authority. The [receiving protocol](evidence/v2-replacement-receiver-20261012/receipt.json)
   is integrated with exact source/term/binding checks, durable-term-before-reply
   ordering and genuine source-driven snapshot offers. Eight focused parents
   pass normal/race, new-file coverage is 85.3%, and format/vet/lint pass. Ordinary
   receiving traffic grants no activation authority. The [declared-snapshot semantic
   auditor](evidence/declared-snapshot-auditor-20261012/receipt.json) now shares
   Restore's initialized-state checks and reads retained
   baselines from the same prepared inactive import. It releases all readers
   before Claim and constructs no live materializer or Host. Seven focused
   parents pass normal/race; new-file coverage is 93.4%, the shared validator
   84.2%, and scoped vet/lint pass. [Hosted integration at
   `ee8d88ae`](evidence/e17-ci-deadline-20261012/hosted-ci-ee8d88ae.json)
   passes all eight applicable jobs, including both modules' normal/race,
   coverage and security gates. This qualifies the integrated auditor.
   [Replacement activation](evidence/v2-replacement-activation-20261012/receipt.json)
   is now integrated and locally qualified: the original receiving RawNode's
   actual snapshot Ready is durably installed, then Restore, same-Ready Advance
   and unique Driver/Host adoption preserve retained graph state. A live
   association owns the restored Host independently of the request context;
   synchronous cancellation gates and reentrant cleanup preserve ownership.
   Thirteen focused parents and 51 subtests pass normal/race/coverage with no
   failures/skips; new receiving and graph-owner files cover 84.0% and 86.8%.
   Actual process-kill and injected write/sync failures withhold acknowledgements
   and recover whole old/new durable states. Scoped vet/lint pass. This closes
   the local activation component; hosted integration, broader ownership/GC,
   remaining fault/cost evidence and V2 acceptance retain their own gates.
   Hosted activation run `38258858745` found an established admission-closure
   assertion failure in all four agreements: the new lifetime refusal omitted
   `context.Canceled`. The [cancellation-cause correction](evidence/v2-replacement-admission-cancellation-20261012/receipt.json)
   preserves owner/request context causes without changing the old assertions.
   The exact admission parent plus activation/revocation tests pass normal/race
   (14 parents, 55 subtests); no full-module rerun was used for this correction.
   Remaining broad CI work was cancelled following the owner instruction to use
   focused iteration checks. Full phase/release gates remain required.
   The v4.51.2 source integration is qualified by the `92c6415b` CI record above;
   subsequent activation source retains its own hosted integration gate.
   The [bounded recovery fix](evidence/native-ready-progress-20261012/qualification.json)
   now makes progress when an ordinary native Ready exceeds available application
   capacity but its ordered prefixes fit. It retains the original Ready until
   durable completion, preserves the commit boundary and withholds output on errors.
   Both complete storage/replica package normal, race and coverage runs pass, as
   do scoped static gates; the receipt distinguishes final equivalent selector
   and comment edits. The [ordered recovery and actual paired-fault qualification](evidence/native-paired-fault-20261012/qualified-final.json)
   is now integrated. Prospective completion capacity is bound to the exact
   applied or earlier queued reservation. New work behind a retiring owner is
   reclassified only after the genuine committed predecessor is installed;
   callback/read/close/I/O errors never grant retry authority. Persisted recipes
   contain only Reserve/Use, separately from transient dependency observations.
   Focused normal/race and affected compatibility tests pass; new-file coverage
   is 87.5%, each new file at least 80%, with scoped vet and configured lint clean.
   Actual native normal/race traces match the retained FDB six-operation lane;
   all 19 adversarial comparison checks pass. Every successful native lane reaps
   all 15 incarnations. This closes the selected recovery/paired-fault component,
   not remaining fault/cost matrices or the engine decision. Prefix fallback may
   require additional syncs and still has no performance claim.
   [Source publication and pin reclamation](evidence/v2-source-publication-p09-20261012/receipt.json)
   and the [combined P07 handoff](evidence/v2-p07-combined-handoff-cdc-20261012/receipt.json)
   are now integrated and locally qualified. Publication refuses cancellation
   after capture, returning no cookie and releasing its native views. Actual AS5
   activation preserves a captured old bank until the last owner releases it,
   with the original retained CDC start intact. P07 combines a lost committed
   reply, partial participant installation, exact retry, handoff, target follower
   reopen, stale authority refusals and genuine recovery-prefix reclamation;
   exact old/current graph and index answers and complete original CDC survive.
   P09's eight focused parents and P07's single composed parent pass normal,
   race and scoped coverage; configured scoped vet/lint pass and the new
   publication guard is fully covered. These are in-process six-voter component
   proofs; portable public feeds, remaining fault/cost matrices and V2 closure
   remain open. Routine hosted recovery integration is qualified by the
   [focused CI record](evidence/change-aware-ci-20261012/hosted-ci-bb439e35.json);
   the [combined P07/P09 checkpoint](evidence/change-aware-ci-20261012/hosted-ci-c13f50be.json)
   passes all 20 selected parents in normal/race/scoped coverage, plus the v5
   build and static gates. Unrelated full suites were not repeated.
   The later [native-runner hosted checkpoint](evidence/focused-root-partitions-20261014/hosted-060.json)
   passes 33 parents and 49 parent/subcases in each v5 mode, with two explicit
   skips. Migration, lint and security pass; the bundled root job reaches its
   30-minute limit before completing race and coverage. Graph, core, badger and
   memory race results pass. The [root CI partition correction](evidence/focused-root-partitions-20261014/receipt.json)
   preserves affected packages and exhaustive core parents in separate jobs.
   Its 29 Python/fake-Go guards pass; hosted partition proof awaits a genuine
   root source change. Full phase gates remain separate and unchanged.
   The [FoundationDB cost-runner source](evidence/v2-fdb-cost-final-source-20261013/receipt.json)
   is now integrated: its two selected SDK parents pass normal/race, and the
   actual instrumented two-fact cross-partition run preserves old/current
   results, postings and CDC. Current-source coverage is 82.0%/80.9% for the
   two new implementation files; configured lint/security/vulnerability gates
   pass on the same executable source. Owned runtime resources are removed.
   This is functional qualification only. Matched uninstrumented cost cells,
   the full matrix and engine selection remain open; the
   [focused-CI adapter](../tools/v5-substrate/fdb/evidence/cost-final-source-20261013.json)
   preserves that distinction and the earlier failed attempts.
   The [native cost runner](evidence/v2-native-cost-runner/receipt.json) now
   checkpoints at most 16 actual acknowledgements through genuine certified
   cuts, checking original records on all six voters. Its metadata comes from
   retained application history and controls, preserving inspection after Raft
   prefix compaction. Twelve selected parents pass normal/race/scoped coverage;
   the later two-fact fixtures pass separately. The complete CDC comparator
   checks every original group, effect and physical posting; genuine two-fact
   LOCAL/CROSS pairs match seven groups and nine effects each. Seventeen-fact
   native histories exercise full and partial batches. These finite proofs
   preserve the [earlier incomplete 10,000-fact trial](evidence/v2-real-substrate-cost-20261012/receipt.json),
   which reached the time limit before final retained-point verification.
   Larger layouts, concurrent writers, resource qualification and the engine
   decision remain open; comparison state currently grows with the fact count.
   The
   [bounded voting/control model](evidence/v2-graph-lifecycle-admission/replacement-model.py)
   passes 5,040 event schedules and 11 finite checks, independently rerun against
   the [recorded result](evidence/v2-graph-lifecycle-admission/replacement-model-result.json).
   It binds the exact replacement voter and local request lifetime, rejects
   snapshots below the receiver's known term, and permits delayed valid snapshots
   without assuming instant knowledge of a remote leadership change. Native
   receiving/activation durability and graph-state recovery tests
   remain implementation obligations; this model does not close them.
   No separate lifecycle journal is required.
   Construction grants no graph-operation authority; existing committed-Init and
   consensus gates must remain enforced. Partial provisioning may burn the new
   namespace, but cleanup must never delete storage that may have served data. Old identities
   must use retained-state reopen or surviving-quorum-authorized replacement,
   never fresh initialization; one missing voter must not block healthy-quorum
   operation. Raw storage/bootstrap primitives remain privileged, distinct from
   supported lifecycle admission. Source seams: `raftlog.Open` / `Store.Initialize`
   (`v5/internal/raftlog/storage.go`), `BootstrapBoundOwnership` and
   `BootstrapBoundSinglePartition` (`v5/internal/graphstore/{partition_initialization,topology}.go`),
   materializer/Host construction (`v5/internal/graphapply/{declared_materializer,allocation_host}.go`),
   and `replica.Open` / `ReadIndex` (`v5/internal/replica/driver.go`). Existing
   `graphPeerWireTransport` is graph-qualified post-genesis traffic, not provisioning;
   registered registry materialization still grants no lifecycle readiness.
   **Source counterexample, unexecuted:** fresh directories with the old graph,
   declaration and group pass selected-directory binding/seed checks; HOME
   initialization can then call `idalloc.NewState` and restart its high-water,
   losing old history. This is not a runtime RED or acceptance claim. Identical
   copies of an entire voter identity/credential set cannot be distinguished from
   retained bytes alone: the trusted embedding authenticates unique live replica
   incarnations; global clone detection is outside this crash-fault model.
   A Host constructor may precede a quorum provided its graph operations remain
   fail-closed; constructor success is not operation authority.

The phase table above records current acceptance. The following entries retain
the scope of their historical component validations.

The [independent reference suite](../docs/v5/reference/independent/README.md)
is integrated locally. `python3 -B docs/v5/reference/independent/check.py` passes
20 temporal and 18 protocol tests, the unchanged historical 16/52, 42 revised
case records (corpus revision 3), 18,192 interval pairs, 600 component mutations,
88 accepted protocol histories and 39 required refusals. Generated artifacts
are deterministic. These are independent mathematical/declarative checks;
remaining production differential/attachment doors, durable capture and phase
acceptance remain open.

The historical independent revised component/primitive comparison against a
SHA-pinned schema-owner overlay passed 22 of the 42 records, with 16 required integrations
and four declarative obligations explicitly open. The unchanged full E02
same-name node/relationship property golden and new E19 strict-life correction
case passed. Owner-qualified schema support is committed as `844e065`; that
does not make the overlay comparison or its supplementary tests canonical.
This historical 22/42 comparison is separate from the accepted canonical native
adapter described below.
Its supplementary tests checked 64 complete historical answers, 12 atomic
refusals and four candidate faults with 128 retained-answer rechecks; scoped
normal/race/vet passed at 85.9% graphstate coverage. These remain historical
overlay evidence, not engine or V1 acceptance. Remaining native doors include
interpretation/role metadata, graph-qualified references, descriptor/knowledge/
rational graph values and public default-axis/import adapters.

The canonical revised-corpus adapter (`ac45629`, extended by `6efc9a3`) now
checks 21 native executable contracts, seven byte-preservation-only cases, five
real unsupported-predicate refusals and nine pending records. Seven additions
execute numerical helper contracts only; graph default-axis selection and importer
integration remain open; explicit-axis Instant helpers are separately accepted below.
Nominal/opaque refusals
are explicitly type-level probes. Four graph cases use immutable test map
views, not durable storage or certified cuts. The original historical 16/52
and independent 42-case revision-3 corpus remain unchanged. Independent isolated
graphstate+temporal race and v5 build/vet/coverage/pinned gates pass
(90.2% total coverage; 121 frozen source/config/fixture hashes checked). The
first full-race run failed an asynchronous self-SIGKILL harness; its exact failure
is retained, and a serial retry passed. Remaining attachments, public APIs,
store/cut integration and import mapping remain open; no V0–V7 gate is closed.

Three independent regression findings are resolved by local commit `e5712e1`:
unreachable allocator receipts (`first < sequence`), omitted ScopeValue axis
metadata in read/delta ledgers, and repeated questions sharing an older pending
ReadIndex barrier. A separate Git-archive checkout of that exact commit passes
Go 1.26.9 full-package race tests and vet for idalloc, graphstate, txnproto and
replica, including the committed regression tests and six-process harness.
The [candidate patches](../docs/v5/reference/independent/regressions/README.md)
remain historical review alternatives and must not be applied again; committed
implementations differ. Broader graph differential coverage, fresh all-module
CI and full V0–V7 acceptance remain pending. These checks do not establish
distributed per-issuer allocation or full Fresh-cut orchestration.

Further replica input-admission regressions are resolved in committed `1a446a8`:
empty/malformed proposals and invalid snapshot membership could panic before
ordinary error handling, and nonconsecutive append indexes could acknowledge a
missing entry. The committed guards preserve valid follower forwarding and
protobuf nonzero-boolean behavior and reject exhausted finite terms. The exact
candidate committed as `1a446a8` was independently validated with nested
build/vet/race/coverage and pinned Docker gates on Go 1.26.9. The earlier
portable guard patch remains historical review data and must not be reapplied.
This is input-admission evidence, not a claim of Byzantine safety or divergence
from a valid peer schedule.

The application output-capacity finding is resolved in canonical local source
`eb2f421`: Scan charges returned row-slice capacity and visited-key work, while
point/root/change/outcome copies expose exact capacity. The reproduced 651-byte
page retaining at least 1,018 bytes, 103-byte envelope returning capacity 112,
and 3-byte root returning/retaining capacity 8 are now regression cases.
The [portable output-capacity patch](../docs/v5/reference/independent/regressions/application-output-capacity.patch)
and [validation](../docs/v5/reference/independent/regressions/application-output-capacity-validation.json)
remain immutable historical evidence against `c99e1dc`, not current-source patches.
These are caller-visible capacities, not physical heap/RSS measurements.

Recipient fence replay is resolved locally as `a4d2330`: after genuine proof
validation, delayed same-session fences cannot regress active phase or reset
sequence. Independent isolated v5 gates pass with 100% changed-block coverage;
a source-owned six-process uncheckpointed SIGKILL/reopen regression passes.
The [portable recipient replay patch](../docs/v5/reference/independent/regressions/recipient-fence-replay.patch)
and [validation](../docs/v5/reference/independent/regressions/recipient-fence-replay-validation.json)
remain immutable historical review evidence against `de9e0b7`, not patches to
reapply. Full graph identity reuse/fencing and V2 acceptance remain open.

`State.Slice` (`4d22038`) is an independently validated immutable bounded
primitive (92.9% direct coverage, 89.8% nested-module total). It preserves full
cells and gaps and checks source/window/working/output policy. A 64→4,096-piece
benchmark shows constant allocations and logarithmic seeking; it is not a
full-engine or comparative capacity result.

Explicit numerical helpers `ConvertUnits`/`InstantMillis` are accepted locally
as `22f80a4`. They provide bounded exact identical-unit or microsecond/millisecond
scalar conversion and signed int64 integral-millisecond encoding, with explicit
fraction/range/domain/mapping refusals. Reference/origin identity remains the
caller's responsibility. Parent isolated full v5 build/vet/race/coverage and
pinned Docker lint/security/vulnerability gates pass; 119 snapshot sources
match. Common microbenchmark paths allocate zero, without a production capacity
claim. Explicit-axis `types.Instant` binding/encoding helpers are accepted as
`5e87390`: Z/Q millisecond axis identity is supplied by the caller, with exact
mismatch/fraction/range/Q×N refusals. Graph default-axis selection and importer
integration remain open.
The separately accepted `6efc9a3` adapter update executes seven numerical
helper contracts without closing those integration obligations.

Crash-harness correction `0dd55f2` blocks children after successful self-SIGKILL
requests at four seams; the scalar parent now requires actual SIGKILL without
deadline expiry. Recovery assertions and deadlines remain intact. Twenty
repetitions across six crash modes (120 subtests), full serial race, coverage,
build/vet and pinned gates pass; the parent independently checked 122 hashes
and focused race. This corrects test evidence, not product persistence behavior.
Accepted catalog/assertion, component-page and association prerequisites are
recorded below; full graph/materializer/Host integration remains open.

`ApplicationPolicy.Preflight` (`738719e`) is a pure shared batch shape/work
check with logical retained-byte/record output and zero usage on error. Concrete
valid Raft limits are required; quota reservation, root freshness and committed
eligibility remain installation/admission responsibilities. Parent raftlog race,
v5 build/vet/coverage and pinned gates pass (Preflight 100%, total 89.9%).

Historical [actual Host captures](../docs/v5/reference/independent/actual-host-captures/README.md)
are preserved as `06c7fa2`. The parent matched the historical sources against
archived `1a446a8`, applied the unchanged review patch independently and reran
normal/race capture tests and vet. Each run's five histories, 14 decisions,
nine complete observations and five final maps pass the unchanged serial-history
oracle; five synthetic sensitivity alterations are refused. These use simulation
and CrashableMemFS, not real process durability or full `Evidence.check`.
They do not establish current-engine or full V2 acceptance; the historical
16-case/52-assertion fixture remains unchanged.

Local accepted prerequisites also include bounded state/change codecs
(`a00a544`), the native FoundationDB functional comparator (`04d4fb3`),
atomic retained ApplicationBatch/root/CDC/outcome storage in the same Pebble
batch (`c99e1dc`) and cached ID recipient fencing across the actual two-group,
six-process scalar protocol (`de9e0b7`). Initial application storage is
singleton-voter with retained-data quota backpressure; bounded dormant export/import
(`7caf9d3`) is accepted; the later receiver-only activation is recorded below.
End-to-end Driver snapshot traffic, Raft application multi-voter integration,
application GC and production graph assembly remain open.
Allocation-service
authority is separate from recipient/session epochs; production graph identity
reuse validation remains open. Full graph/materializer/Host integration, graph
default-axis selection and importer integration remain open.

Accepted local prerequisites `667e9c1` (pure assertion records/codecs) and
`a308f8f` (bounded namespace catalogs/root/staging) remain local foundations.
Bounded component pages (`a2bd71c`) and stored assertion associations (`e1d38da`)
are now accepted separately below; no full graph/materializer/Host exists.
`AssertedRelation` is the sixth interpretation: a supplied relationship fact,
not a constraint or inference. Parent combined isolated v5 build/vet/race/coverage
passes
(90.2%, 137 stable hashes); individual assertion coverage is 95.4% and catalog
88.6%. Worker pinned Docker gates pass; catalog vulnerability checks find zero
reachable and four uncalled advisories. Provisional catalog bytes are root 116,
node 142, LINK relationship 146 and integer PointScopeValue 191 (15-byte reference,
10-byte unit), excluding keys/application frames/log/CDC/backend/replicas.
Repeated axis bodies cost 84 bytes; this is not comparative capacity acceptance.
Separate coordinator verification of actual `a308f8f` compared all 134 archived
v5 files with their Git blobs; Go 1.26.9 full v5 race (ten packages) and vet pass.
The committed comparison-input checker passes 17 tests, 93 complete outputs and
three code-mutant refusals, including input/answer metadata tamper checks. These
are source/fixture checks, not graph materializer or vendor benchmark acceptance.

`ApplicationView.ReadLimits` (`4d64350`) reports immutable configured per-call
row/byte maxima, not remaining quota, a valid view or a lease. Shared catalog
operation caps (`00c978c`) charge nested lookups and value materialization under
one budget narrowed by smaller application limits; parent graphstore/raftlog
race and scoped vet pass. These are logical/capacity bounds, not heap/RSS.

Dormant application snapshot transfer (`7caf9d3`) is independently accepted:
451 review-input hashes match; worker full gates pass (90.2% v5 coverage), and
parent raftlog race/coverage passes (87.4%). Export of the captured applied checkpoint and retained history uses bounded
canonical chunks to import into an inactive bank; even verified imports never
change active state on their own. End-to-end Driver activation, Raft application
multi-voter integration, certified cuts and production topology transfer remain open.

Component pages (`a2bd71c`) are accepted as a bounded local directory/checkpoint/
patch prototype: 154 snapshot hashes match; parent graphstore race coverage is
85.9%; worker new production coverage is 83.28%. Historical reads, exact before images,
addressability and ordered CDC groups are checked. Limits are provisional:
On an Apple M4 Max, the 64-cell-leaf fixture with 31 patches measured about
1.1 MB/4,142 allocations per read versus checkpoint-only about 70 KB/262
allocations. This small fixture is not efficiency or vendor acceptance.

Assertion associations (`e1d38da`) are accepted with independently retained
revisions/retractions, explicit immutable primary bindings and bounded target
postings. The 167-file snapshot matches; parent assertion/graphstore race
coverage is 95.4%/86.6%, new association production code 89.06%, and every public
method has direct coverage at least 82.1%. Association scope does not change
node/relationship life coverage automatically. One posting per page is a
prototype, not a tuned access path. Full graph/materializer/Host integration,
efficiency/vendor acceptance and all V0–V7 phase gates remain open; rho owns
database preservation/access and sigma owns reasoning.

Historical entity-declaration review artifacts (`a538b08`) remain unapplied to
canonical Go; they do not close attachment or phase acceptance. The revised
adapter split remains 21 native / 7 preservation / 5 refusal / 9 pending.

Comparison [input fixtures](../docs/v5/reference/independent/comparison-inputs/README.md)
are accepted as `7d8b75d`: the standalone input-integrity comparator passes
17 tests, 93 complete outputs and three sensitivity mutants; the fresh main
independent suite also passes. The parent verified all 116 artifact hashes.
This is a benchmark fixture/input-integrity prerequisite, not a vendor benchmark
or space/latency acceptance result.

TigerGraph's documented Docker image targets x86-64. ARM hosts require
emulation, which does not substitute for native-x86 performance acceptance.
Vendor adapters must preserve edge multiplicity, property presence and
complete query results, with reverse-edge and service costs counted explicitly.

The five follow-ups through `9752e7c` are accepted local foundations; their
Ready/lifecycle/binding, bootstrap and generation contracts are in CHANGELOG Unreleased.
Generation snapshot checks pin 1,966 inputs and pass Go 1.26.9 full v5
race/vet/build/coverage (89.6%) and pinned lint/gosec/govulncheck; parent independent
raftlog/replica/graphstore race coverage is 87.8%/90.5%/87.0%. All V0–V7 gates
remain open; later local index/receiver increments below do not certify graph
cuts, end-to-end multi-voter integration or physical capacity. Rho preserves/accesses data and database consistency; sigma reasons.

Accepted integration prerequisites now include private allocation/request reduction
(`96cbfe7`), durable application publication (`c3d3705`), exact published export
(`2f5fb6b`), receiver-only Store activation (`e665ace`) and real Fatal-seam
publication-worker cleanup tests (`d076dcd`). Requests replay by exact identity/hash
before epoch/grant checks. Export verifies complete retained history at the stable
published application reference with bounded visited/output work; receiver activation
requires a verified live claim and actual Raft Ready and retains old generations.
Driver traffic remains fenced; these references are not certified graph cuts.

Complete local graphstore (`b3c7de1`) supplies fresh-only Full indexes, one complete
ReadView identity and Plan-running atomic graph-effect staging, including exact
canonical alias handling, historical raw uniqueness candidates and deduplicated
incident multiplicity. Frozen Go 1.26.9 gates pass (89.2% whole v5, 84.71% new
production; every new file at least 80%). Exact-set historical/reopen, budget and
late-cancellation regressions are covered. Outer graphapply materializer/Driver
composition into one final root/CDC/outcome ApplicationBatch remains open, as do
certified distributed cuts/transactions, the PLAN §4.2 10-active versus 10k/100k-ended
access measurement, seal/merge/recovery and database-wide physical heap/RSS/page-cache
and comparative capacity gates. Representation ledgers do not prove physical memory
bounds. These are prerequisites; no V0–V7 phase or public engine release is closed.

Binding validation (`0e4c96b`) adds accepted correctness cost; optimization
remains pending. Five interleaved Go 1.26.9/M4 Max/GOMAXPROCS=4
MemFS/Pebble runs measured checkpoint reads 179.979→189.265 µs (+5.16%),
4,096-point current reads 212.333→221.788 µs (+4.45%) and old-cut reads
24.128→35.347 µs (+46.5%): same cells, +5 records/+1,477 charged bytes and
+81–82 allocations. Tail-31 timing ranges overlap; no speedup or V3 acceptance.

Reviewed foundations are committed locally as `6ef233c`; the coordinate block
candidate is `fa0c982`. Neither is a v5 release or phase-completion claim.

Go 1.26.9 review checkpoint: the parent independently SHA-pinned and validated
49 temporal/state/raftlog/replica files with build, vet, race, `make cover`, lint,
gosec and govulncheck: combined coverage 90.5%, no exported method at 0%.
Those hashes identify that historical foundation only. Root v4 build/vet/lint/security/vulncheck,
MetaKV checks and coverage gates pass (pkg coverage 86.5%); its full race run
failed only the version-metadata assertion, then focused ingest/docs race passed
after the metadata/private-helper fix. These are separate checks, not one
consolidated all-module CI run. Graphstate's independent 54-source/config
snapshot matches and passes build/vet/race/coverage/lint/gosec/govulncheck:
combined temporal+state+graphstate coverage 89.7%, every exported method covered
(worker graphstate-only coverage 83.5%). Txnproto's independent 24-source/config
snapshot matches and passes the same checks: raftlog+replica+txnproto coverage
86.3%, no exported method at 0% (worker txnproto-only 88.2%; bounded fuzz 33,284
executions). The 25-file six-process snapshot is independently accepted as
`f3a1ee7`: focused txnproto race, snapshot coverage and pinned lint pass (86.3%).
It checks whole transactions and old cuts after correction/reopen with durable
private directories, owned child kill/wait and reversed/duplicate delivery at
prepare, decision, partial-application, unresolved-checkpoint and quorum-loss
boundaries. Reproduce from `v5/`:
`go test -race -count=1 -run '^TestTwoGroupsSixProcessesDurableBoundaryRecovery$' ./internal/txnproto`.
This is process-crash functional evidence, not power-loss, multi-host or global
cross-process cut transport acceptance. Native FDB functional comparison is
accepted as `04d4fb3`;
comparative costs/faults remain pending. Full graph transaction/cut integration
remains open.
Idalloc's independent Go 1.26.9 build/vet/race/coverage/lint/gosec/govulncheck pass
at 99.3%; eight source/config hashes match the reviewed snapshot.
Full V1/V2 remain open and no engine is selected. Candidate adapter tests do not
establish cross-partition correctness or complete certified cuts. Pebble's fatal
storage paths stop the embedding process; persistent Create/EIO and ENOSPC reopen
faults required a supervisor deadline, with actual injection markers and the
prior acknowledged prefix preserved. This tradeoff remains part of selection.

Earlier candidate checkpoint through `eb2f421`: independent nested-module
build/vet/full race/coverage and pinned Docker lint/gosec/govulncheck pass
(89.7% total coverage; zero reachable vulnerabilities, two imported-package
and two required-module advisories unreachable). This supersedes earlier
scoped validation only for that candidate; historical artifacts stay
unchanged. Final full-root+v5 CI, physical capacity, distributed matrix and
consumer acceptance remain pending. Every V0–V7 phase gate remains open.

Embedded-first fallback release eligibility requires its applicable local gates;
it leaves V2/V5 and distributed V6/V7 pending. Full V0–V7 completion requires all
of them. Owner criterion (2026-10-09): large-data space/time efficiency; space
at least comparable to Neo4j/TigerGraph/Memgraph, preferably better, with any
excess quantified, causally isolated and reviewed. No fixed deployment/SLO was
requested. The [comparison protocol](../docs/v5/reference/graph-db-comparison.json)
is research/planned measurement only: licenses/configurations, runnable harness,
representative data, five-run byte/latency evidence and excess-cost ledger remain
pending. The 10%/70% thresholds are provisional, not owner-accepted gates.
Reported day sizes are inventory inputs and 400–700 rows/s is an unverified
arrival shape. Baseline sizes count source signal rows; their HOP counts are
107,113/408,282/1,584,150. Initial implementation numerical bounds specified are 64 KiB input/value/descriptor bytes,
4,096 magnitude bits and 4,096 region pieces; V1 validation remains pending
before V2 format freeze, with no implied performance acceptance.

---

## Open

### 0. Column segments on NVMe (ADR-0011)

*FEATURE* — S0-S3, S5 done (S2/S5 in v4.39.0, S3 + P7 in v4.40.0); S4 merge, S6, S7 and sub-points (a), (b), (d)-(i) open.

**Column segments on NVMe (ADR-0011, accepted 2026-09-24)** — FEATURE: steps S0–S7 in `docs/adr/0011-column-segments.md` §6, each with a failing test first and a gate measured at the three synthday sizes; integrity block size configurable (`IntegrityBlockRows`, default 64). Goal: resident memory independent of the day size for declared bulk relationship types (~744 B/rel today). **Progress:** S0 done (baseline harness `bench/segment_baseline_test.go`; memory 718–723 B/rel, badger lean does not reproduce 191 — measures 301), S1 done (codec `pkg/graph/internal/segment`; 22–25 B/HOP on disk; decode of full rows below the memory store's zero-copy scan rate, columns alone 24–28 M rows/s), S2 done on the rho-tkg side (memory store seals declared types into in-RAM segments on a background sealer; `Config.RelSegments`; segments share store-level endpoint and string dictionaries; P6 HOP resident 24.0 / 24.8 / 25.7 B/HOP beyond the memtable, gate ≤ 60 met; legacy 70–71 B/HOP over it because of `support`), S5 done on the rho-tkg side (`ScanRelSegments`, `ScanRelColumns` from segment columns) with the sigma branch `seg/s5-columns` built and tested, not released. **Open from S2/S5:** (a) the ai-soc half of S2's gate — xcheck AGREE on the three synthday days and on BA on Flux with `engine.Open` declaring HOP (ai-soc work, after P6); (b) the per-segment adjacency directory (a node code and two CSR offsets per endpoint per segment, 0.22 → 0.68 B/HOP from 1 to 5 segments) — S4's merge bounds it; (c) DONE: v4.39.0 carries S2/S5 and sigma-tkgd pins the latest tag; (d) the rel property / temporal indexes and the lazily built belief-watermark and rel-type tx-membership sidecars keep one entry per row when a caller creates or triggers them — check at the ai-soc gate whether ai-soc's queries build them; (e) the row doors on sealed rows stay 3–20× below the zero-copy row store: consumers of bulk types read `ScanRelSegments`. **S3 done on the rho-tkg side (2026-09-25):** `Config.SegmentDir` — self-contained segment files, manifest, crash recovery, mmap reads; Go heap beyond the memtable 0.3–0.7 B/HOP at the three sizes (gate ≤ 1 B/HOP growth met), write median 7.86 s against S2 7.87 s (5 alternating runs each, pinned; gate met). **Open from S3:** (f) the BA day on Flux (masked twin in git) — ai-soc passes `SegmentDir`; (g) per-segment node hashes and dictionaries make files 27–28 B/HOP at 12.6 M (4–5 segments) — S4's merge; (h) Q8 (page cache under cgroup `MemoryMax`) — measure on Flux; (i) a reopen verifies every integrity group (~0.46 M rows/s): fine for a day, a cost for a year of segments. Next: S4.

### 1. Import-under-a-scope

*MEDIUM (improvement, deferred locking)* — open; `IO().Import` still takes only `c.mu` (no `c.txMu`), stopgap in place.

Carried over from the retired `tasks/todo.md` (`4f71fc3`).

- **Import-under-a-scope** (the proper fix beyond the import stopgap): wrap
  `IO().Import` in a `TxChangeLogScope` so a FAILED import emits NOTHING, not just
  "no poison". Import emits change-log records IN-BACKEND eagerly, outside any
  scope, so `importRollback.restoreRegistries` is still gated on
  `changeLogEnabled` — the append-only stopgap — rather than de-allocating tokens
  exactly. Needs import to also take `c.txMu` (it takes only `c.mu.Lock` today, so
  a between-mutations open tx scope would collide with import's `BeginLogScope`);
  a locking change, deferred. The current stopgap is correct — see
  `tasks/lessons.md` 55.

  **Measured 2026-08-05** (`BenchmarkImport_ChangeLogCost`, 5,000 nodes + 5,000
  rels, variance <1%):

  | | time | allocs |
  |---|---|---|
  | ChangeLog off | 216ms | 1.4M |
  | ChangeLog on | 621ms | 7.0M |

  The STOPGAP itself costs nothing: `restoreRegistries` is reached only from
  `rollback()`, i.e. only on a FAILED import, and its change-log branch is an early
  `return nil` — cheaper, not dearer. Nothing on the success path touches it.

  The eager emission the stopgap works around costs 2.9x, so the prize is real —
  but a SECOND BLOCKER, not previously recorded here, is that the fix may not
  collect it. A scope BUFFERS records in memory (badger: `bs.scopeLog`, an
  unbounded `[][]byte` released only at commit). A transaction is small; an import
  is the whole graph, and bootstrap-importing into a change-log-enabled store is
  exactly the case that would hold every record at once. So the item trades a 2.9x
  time cost for a memory cost proportional to the entire import, and wants a
  spill/chunk story before the locking change is even worth planning.

### 2. Old BACKLOG 22: six TEST-GAP research items (22a-22f)

*TEST-GAP* — open; prioritise only when a consumer needs the confidence.

Rescued 2026-08-05 when the June 2026 `.harden/coverage.md` ledger was deleted.
**TEST-GAP / research**, not known defects. The wire-decode and import-amplification
bugs that ledger found shipped as v4.9.2 / v4.9.3. These six were the "attack next"
remainder and were never numbered into BACKLOG 6–21. Partial later coverage is
noted per item.

Prioritize only when a consumer needs the confidence or when touching the named
subsystem.

#### 22a. [TEST-GAP] Tiered crash-fault injection between cross-shard writes

Process-kill between cross-shard split-writes (E→R / R→E), mid-flush, mid-cascade.
Happy-path and a few residue paths exist (`tieredstore_write_rel_crash_residue_test.go`,
`tiered_registry_crash_test.go`); no systematic fault-injection matrix that kills at
each ordering step and asserts reopen + `RunRepair` / rollback leave a consistent
neighborhood.

#### 22b. [TEST-GAP] Clock-skew vs hot→warm rotation and cold demotion

Rotation / cold demotion / `ShardWindow` edges under non-monotonic or skewed wall
clock (tests must not use sub-millisecond windows). Atomic catalog rotation tests
exist; deliberate clock jumps across window boundaries do not.

#### 22c. [TEST-GAP] Fuzz tiered metadata decoders independently

Tiered catalog / registry / temporal-index / vector-index definition files
(`registry_file.go`, `temporal_index_file.go`, `vector_index_file.go`, catalog
JSON) are not fuzzed. Badger meta goes through `SafeUnmarshal` but is not
independently fuzzed. Goal: native Go fuzz + committed `testdata/fuzz/` crashers,
same discipline as `FuzzWireTo*Checked` / `FuzzImport`.

#### 22d. [TEST-GAP] Property / vector index queries under adversarial values

NaN / ±Inf / huge-dim / mixed-type values on **query** paths (not only write
validation). Partial coverage exists (dim-mismatch sentinels, some type-class
handling, vector tests); not an exhaustive battery or fuzz on the query door.

#### 22e. [TEST-GAP] Long-running concurrency soak under the race detector

Mixed standalone + tx + batch + concurrent ingest for minutes under `-race`,
beyond targeted race tests. Opt-in / manual (or a long CI job) — not `make test`
short mode.

#### 22f. [TEST-GAP] Resource-exhaustion cliffs on huge-but-valid inputs

Max labels / properties / containers / blobs at `ValidationLimits` ceilings —
latency/alloc cliffs. Valid inputs must not OOM or hang; fail closed with clear
sentinels where a limit exists.

### 3. Temporal adjacency scan cost (v4.35.0 follow-up)

*MEDIUM (perf, unmeasured)* — open; measure first.

`Rels().ForEachAdjacentRelAt` / `ForEachAdjacentEndpointAt` under a valid-time
filter now resolve relationship VERSIONS (`forEachAdjacentRelVersionLocked`,
CHANGELOG 4.35.0). Two costs were accepted for correctness and are UNMEASURED:

- the badger inline-stamp decode-skip (OPT15) is bypassed under a filter, so
  every adjacent row is decoded and rows whose live version fails the filter
  pay one chain read;
- the deleted-rel fold (`forEachRelAdjacencyCandidateID`) is O(deleted rels)
  per call — the same as `OutgoingRelsAt`, but a consumer's hop expansion
  calls the scan door once per source node, so a graph with many deleted
  edges pays it per node per hop.

**Next action:** measure first, on the consumer's temporal hop benchmarks
(sigma-tkgd `stream_spine` / count-reach fast paths) with a graph that has a
deleted-edge population, before building anything. If the fold shows, the
mechanism is a per-node deleted-adjacency index (deleted rel ids keyed by
endpoint) so the candidate set becomes O(degree + deleted-at-this-node). If
the decode shows, re-admit the stamp as a candidate PRUNER only: a stamp that
passes yields the live row, a stamp that fails still resolves the chain.

### 4. Temporal semantics review 2026-09-24: traced findings

*HIGH? / MEDIUM? (traced, not reproduced)* — open; each needs a failing two-phase test first; code unchanged at the cited sites (`resolveOpenEndInstant`, `validInstantAfter` in `core/temporal.go`).

Found by a code trace during the v1.3-handoff review. The three reproduced findings (delete after close,
correction template, endpoint masking) shipped in v4.38.0 (merge `4473705` of `fix/v4-temporal-semantics`).
The items below are TRACED, NOT YET REPRODUCED: write the failing two-phase test first; drop the item if the
test passes.

- **(HIGH?) `NodesDuring` / `RelsDuring` open end.** `end == 0` is resolved to now + 1
  (`c.resolveOpenEndInstant`, `temporal.go`; now = `c.readNow()` since 2026-09-24, so a
  version stamped ahead of the wall is no longer missed), so an entity valid only in the
  future is missed; `NodesRelating` keeps the open end as +inf.
- **(HIGH?) Future transaction time from `validInstantAfter`.** Update/CloseVersion/Delete of a
  row whose explicit ValidFrom is in the future stamps `TxFrom/TxTo = ValidFrom + 1` without
  advancing the floor, contradicting "every committed entity has TxFrom <= NowTx()"
  (`txtime.go` ~L159) and SPEC.md ~L439.
- **(MEDIUM?) As-of vs point resolver order.** `SelectAsOf` picks the newest candidate by
  version; the point resolver breaks overlaps by (TxFrom, version). After a cascade whose rows
  carry a lower TxFrom than a later-version Update, `NodeAsOf` and `NodeAtTx` can pick
  different rows (`asof_select_test.go` ~L119-129 shows the inversion shape). Since v4.46.0 the
  record doors answer "the newest row recorded by the pin" by design and the one version allocator
  (item 18) orders versions by write; re-check whether any divergence remains beyond that documented split.
- **(MEDIUM?) `NodeMatchesValidTime` on a current row with unset ValidFrom** answers "valid since
  mint", so a consumer post-filtering current rows accepts today's properties for times
  before the last update, where `NodeAt` returns the older version.
- **(LOW) Snowflake ID horizon.** 48-bit microseconds from 2026-01-01 end on 2034-12-02
  (arithmetic). Needs a plan before v4 data outlives it; v5 drops clock bits from IDs.
- **(KNOWN LIMITATION) Future-scheduled close, then delete** — tracked as item 25 (and item 7).

### 5. P7 row compaction follow-ups

*MEDIUM (perf)* — open; options (a)-(d) unbuilt (P7 itself shipped in v4.40.0).

**P7 row compaction — follow-ups** (MEDIUM, perf; P7 itself shipped in v4.40.0: 608 → 388 B per P6 HOP row, 531 → 401 B per node). Remaining measured terms per P6 HOP row: struct 80, compact metadata 96, properties 128, `rels` 22, `typeIdx` 21, adjacency ~35. Options, none built: (a) property keys as tokens in a frozen row's own representation (−32 B at 4 properties; every property accessor needs a second path, and a process-wide key table must be bounded); (b) compact form for updated / history versions (today only first versions compact; matters for update-heavy workloads — measure one first); (c) the badger entity cache could cache `CompactFrozenCopy` rows too (its budget uses `ApproxHeapBytes`, which already knows the compact size); (d) store-side interning of repeated string value boxes (legacy schema: ~16 B per `actor`; ai-soc P6 already shares boxes, so no consumer needs it now). (The 3.15 M `ByType` gap seen on a loaded host did not reproduce on a quiet one: median 9.07 → 9.56 M rows/s, CHANGELOG.) "Lazy adjacency" was not built: compact sets save always, and a lazy build would come back on the first adjacency read (sigma's hop scans).

### 7. Caller-instant delete of a row with a scheduled close

*FEATURE (limitation)* — open; `DeleteWithTx` refuses `ValidTo >= t` with `ErrTxOrder` (R8, v4.44.0).

**Caller-instant delete of a row with a scheduled close** (FEATURE, follows item 6): `DeleteWithTx(id, t)` refuses when the recorded `ValidTo >= t`, because one tombstone row cannot end belief at `t` and keep the close that pins before `t` believed (decision 2026-10-09, tx-backfill handover §6.11 (removed, in git history); the refusal shipped in v4.44.0, `core/tx_order.go`). Lifting it needs a tombstone that keeps the believed `ValidTo` beside the deletion instant (no clamp for a caller instant), with the normalizer (`normalizeTemporalVisibleAtTxTime`, `core/txtime.go:495`) and the valid-time history reads agreeing. Write the two-phase red test first.

### 10. Tiered store: remaining work after composite + rel temporal indexes

*FEATURE* — (a) sequential-scan rebuild done in v4.51.6. Open: (b) relationship property indexes (`CreateRelPropertyIndex` declines on tiered, `store/tiered/tieredstore_write.go:181`) and the node-side `TemporalCandidateCapability`. Composite and rel temporal indexes shipped in v4.45.0 (rel temporal index lives on reference + hot + warm shards; ~5.8-9.2 GB per indexed type at `ColdAfter` = 1 day).

### 13. Ingest applier attributes group errors by numeric entity id, not by kind

*MEDIUM* — open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`).

**Ingest applier attributes group errors by numeric entity id, not by kind** (MEDIUM, found 2026-10-09 writing `TestSessionSetVersionInterval_FailedGroupShape`): the strong-async applier keys group-error attribution by the numeric id, so a missing node id and a missing rel id with the same number attribute errors to the wrong group. Minted snowflake ids never collide across the two generators, but caller-supplied ids (`AddByID`, `Import`) can (the rollback-snapshot rule in AGENTS.md "Data Model" already keys by kind + id for the same reason). Red test first: two groups, a node and a rel with the same numeric id, each missing; each group gets its own sentinel. Fix: key by (kind, id).

### 16. Range folds under an interval: predicate-anywhere or value at the resolved version?

*MEDIUM (contract)* — open; decision pending; behaviour documented in the v4.44.0 CHANGELOG.

**Range folds under an interval: predicate-anywhere or value at the resolved version?** (MEDIUM, contract inconsistency, found reviewing item C scan-temporal-opts 2026-10-09). Decide whether all range folds (ordered included) pass the range into the resolver's predicate (predicate-anywhere) like `ByLabelAndProperty`. Today, under `ValidStart`+`ValidEnd`, `Nodes().ForEachByLabelPropertyRange` / `Rels().ForEachByTypePropertyRange` and the ordered / prefix siblings (`forEachNodeInRangeTemporal`, `forEachNodeValueOrderedTemporal` and rel mirrors) test the range on the ONE version `ByLabel` / `ByType` resolve (the most recent overlapping one), while `ByLabelAndProperty` / `ByTypeAndProperty` pass the property into `findNodeVersionForOpts`'s predicate and match a value held anywhere in the interval (`graph_property_query.go:87-95`). A value in range only during an earlier part of the interval is found by the equality door and missed by the range doors. Point opts (`ValidAt`, `TxAt`, `TxPin`) agree. If predicate-anywhere is chosen: the selecting predicate must be EXACT (an over-selecting one could pick a version fn then rejects while an earlier one matched), and the ordered doors sort on the value of the matching version. Red test first: rule-16 shape (value in range on the earlier version only, interval spanning both) across all range doors.

### 20. Effective-read cost: RelAtTx benchmark targets unmeasured

*LOW (TEST-GAP, perf)* — open; only the RelAtTx benchmarks remain (everything else in the sigma request shipped, see Closed).

Missing: `BenchmarkRelAtTx/{plain,cascaded}/{hot,cold}` with targets < 1 µs plain hot row, cold row + 1 µs, ≤ 6 µs cascaded via the skeleton path (2.0 / 9.5 / 18.5 µs at v4.43.0, sigma-tkgd stream "effective"). No such benchmark exists in the tree. Add it (ReportAllocs, bench-gate; profile first: 0.15 µs of the 2.0 µs is the row read) and measure on the current release before deciding whether any work remains. Original spec: `git log -- tasks/handover-effective-read-cost-20261009.md`.

### 23. `changeFeedPage` prefetches past its page

*LOW (perf, unmeasured)* — open; `PrefetchValues = true`, no `opts.Prefix` (`badgerstore_changelog.go:648-659`).

**`changeFeedPage` prefetches past its page** (LOW, perf, found by the review of the history-prefix fix 2026-10-09; not measured): `store/badger/badgerstore_changelog.go:654-659` iterates with `PrefetchValues = true`, no `opts.Prefix`, `Seek(start)` then `ValidForPrefix(ChangeLogPrefix())`: a small-limit poll near the tail prefetches up to 100 values it never uses and may read past the change-log keyspace. Fix: `opts.Prefix = prefix` before `NewIterator`, and `PrefetchValues = limit == 0 || limit > 100`. Measure with a polling benchmark first (replica watchers poll it).

### 25. A future close followed by a delete changes the answer at an earlier pin

*MEDIUM* — open; `stampDeleteTombstone` still clamps (`core/temporal.go:133-137`).

**A future close followed by a delete changes the answer at an earlier pin** (MEDIUM, pre-existing on main, found by the review of the cascade-correctness branch 2026-10-09): `CloseVersion(far-1)` → pin → `Delete`: `AsOf(pin)` changes from `[1000,far-1)` to `[1000,inf)` and `AtTx(far, pin)` from absent to present, because `stampDeleteTombstone` clamps the close to the delete instant (`core/temporal.go:136`) and the as-of rewind (`normalizeTemporalVisibleAtTxTime`, `core/txtime.go:495`) reopens it to 0 (the same "known limitation: future-scheduled close, then delete" of the 2026-09-24 review, now that the life-end cap exists the delete could keep a finite ValidTo). Red test first: two-phase on all four backends, node and rel; fix: the tombstone keeps the believed ValidTo beside the deletion instant (see item 7). (Seen again on a re-imported ID's earlier life, item L review 2026-10-09.)

### 26. Old chains with collided versions keep answering in the old version order

*LOW (known limit)* — open; schedule only if a consumer holds such chains.

**Old chains with collided versions keep answering in the old version order** (LOW, known limit, documented in the v4.46.0 CHANGELOG): chains written before the version allocator (backlog 18b) that already hold two rows with one version cannot be repaired by the read rule: e.g. old S2 followed by two old Updates answers the patch row at the pin after the first Update. A one-off repair tool (walk chains, renumber) is possible without a format change; schedule only if a consumer holds such chains.

### 31. Bulk and point as-of disagree with the cascade oracle on version gaps

*LOW* — open; fix when a consumer holds such chains.

**Bulk and point as-of disagree with the cascade oracle on version gaps** (LOW, pre-existing, found by the review of the bulk as-of change 2026-10-09): on a chain with a version gap above the current row (direct `Store.TruncateNodeHistory`, or a re-import over a compacted old life stored by v4.43–v4.47 (since item 38 a re-import starts above the chain's top)) both `NodeAsOf` doors miss a row above a gap past current+1 (56 cases against `cascadeAsofWant` in the reviewer's scratch test, identical on main and the branch). The allocator (item G) never creates such a gap; the density assumption is documented in `core/version_alloc.go` and AGENTS.md. Fix when a consumer holds such chains: either close the gap at truncate/re-import time (renumber under the entity lock) or make the doors scan for the max version above current. Red test first (the reviewer's five gap chains, node and rel, with/without reopen, 17 pins).


### 37. Tiered error-rollback restore leaves a window with empty history

*LOW* — open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`).

**Tiered error-rollback restore leaves a window with empty history** (LOW, pre-existing, found by the review of the point-door race fix 2026-10-09): `rollbackDeletedRelationships` → `restoreRelHistorySnapshot` (`store/tiered/tieredstore_write_history.go:899`) runs `TruncateRelHistory(0)` and then `PutRelVersion`; on that error-rollback path a concurrent reader can see an empty history between the two calls. Fix by restoring in one shard call or under a lock readers respect (the publish-history-first rule of AGENTS.md "Version History"). Red test first: a hook between the truncate and the put, plus a pinned `RelAtTx`/`History` read there, on the tiered store.

### 39. Per-entity compaction stubs survive `Admin().Reset`

*LOW (not tested)* — open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`).

**Per-entity compaction stubs survive `Admin().Reset`** (LOW, found by the review of item L 2026-10-09; not tested): `reapCompactionForReset` deliberately leaves per-entity stubs (it assumes IDs are never reused); on memory an `Import` of an old ID after `Admin().Reset` can still restart at version 0 under a leftover stub, because Reset zeroes the watermark that gates the stub probe (`stubLifeStart`). Red test first: compact → Reset → Import(old ID) → `Verify*Chain` on memory/badger/tiered, node and rel. Fix: Reset reaps the stubs (it already wipes the rows), or the stub probe does not depend on the watermark.

### 40. A cascade resumption may end at an earlier life's own bound

*LOW (cosmetic)* — open.

**A cascade resumption may end at an earlier life's own bound** (LOW, cosmetic, item L review 2026-10-09): `temporal_cascade.go:334,352` (`nodeResumptionEnd` / `relResumptionEnd`) considers bounds of rows before the last tombstone, which adds an extra split row; reads are unchanged (life oracle). Fix only if row count matters: consider only rows after the last tombstone. Red first: a row-count assertion on a cascade after a re-import.

### 45. Internal sentinels reach callers but have no exported alias

*MEDIUM (API)* — found by the doc review of `docs/errors.md` (group C, 2026-10-10).

`ErrNilCallback` (`pkg/graph/internal/grapherr/errors.go:13`), `ErrCommitClockExhausted` and `ErrForeignStampImplausible`
(`pkg/graph/internal/core/core.go`) can be returned to callers, but no public package re-exports them, so `errors.Is` cannot
name them; `pkg/graph/temporal/api.go:474` even tells callers to expect `ErrNilCallback`. `graph.ErrMixedNumericColumn`
(`pkg/graph/column_scan.go`) lives outside the `errors.go` inventory that `errors_doc_test.go` / `errors_identity_test.go` pin.
Red tests first: for each sentinel, a public-layer test that `errors.Is(err, graph.ErrX)` holds for a real failing call (nil callback on a
Tx/Temporal door, clock exhaustion by `AdvanceClock` to the limit, an implausible foreign stamp on replica apply, a mixed numeric column
scan); the inventory test fails when an exported error is missing from `errors.go` or `docs/errors.md`. Fix: export aliases in
`pkg/graph/errors.go` (additive), move `ErrMixedNumericColumn` into the inventory, add rows to `docs/errors.md`.

### 46. Code comments that contradict the code or cite ADRs that no longer exist

*LOW (stale comments)* — found by the doc reviews 2026-10-10. Fixed since: the "88 bytes" note in `pkg/types/node.go`, the lane range in `ingest_lanes.go`.

Still stale: ADR-0005 / ADR-0006 citations (`pkg/graph/store/tiered/tieredstore_property_stats.go:19`, `bench/ingest_pipeline_test.go:13,101`; recover with `git log --all -- docs/adr/` or state the rule); `pkg/graph/store/badgerstore_property_stats.go:38` cites lesson 62 (it is 63); old B-numbering in `pkg/graph/store/store.go:30` and `docs/architecture.md`; the `effective_timeline.go` header describes a point resolver, the code (`effectivePieces`) uses one max-heap sweep.

### 47. Claims that need a real check before they become work

*LOW (unverified)* — carried from the 2026-10-10 handover and left unfiled by the doc review of the tasks files (group E).

(a) "`NodesByLabelAt` still folds all history instead of using the K1 sidecar": `NodesByLabelAt` is a valid-time door and K1 is
transaction-time membership; verify whether the named door can use K1 at all (a two-phase test with a churned label, memory/badger)
before filing a fix. (b) "A RAM budget for the property sidecars": v4.48.0 measured 25-30 B per posting (97-117 B near-unique) and defined
no budget; reopen only if a consumer reports memory pressure; then design a per-store budget that drops a sidecar and falls back to the fold.

### 71. Valid-time hull per label / relationship type (sigma round 8, optional)

*FEATURE* — open, not built in round 8. No structure holds it exactly: a temporal index keeps per-ID envelopes that only widen (`Extend` unions, `Remove` is a no-op), so its hull is an upper bound over history and exists only where a temporal index exists. Exact needs a per-label ordered multiset of valid-from and valid-to (write cost on every node write: opt-in per label, as `TrackRelTypeDegrees`) or a walk. Design first.

## v5

v5 is Markus Nissl's work on branch `origin/v5`, a separate Go module (`v5/`); the plan (`docs/v5/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md`) lives on branch `v5`, not on main (our plan commit `b1193dc`, tag `v5-plan-20261009`). Do not push to `v5`; Markus merges v5 into main (René 2026-10-12), we do not; open questions for him are the rho-tkg GitHub issue #11. Maturity, merge analysis and the v4 to v5 door mapping: `docs/v5-analysis.md`; the Fable review of the branch: `docs/v5/HANDOVER.md` on branch `v5`. The older notes `tasks/review-v5-plan-vs-code-20261009.md` and `tasks/v4-changes-for-v5-importer-20261010.md` were removed from main (recover with `git log -- <path>`). v4 keeps taking consumer features (René 2026-10-09). **DECIDED 2026-09-25 (René): ADR-0011 S2 and S5 shipped on v4 as v4.39.0** (ADR-0011 §6 gate met: 24.0 / 24.8 / 25.7 B/HOP at 790 K / 3.15 M / 12.6 M); v5 carries the segments forward.

---

## Not tracked here (cross-team)

Consumer builds these; rho-tkg already exposes the local primitives:

- START→END foreign-stub-delete fan-out (BACKLOG 2 Inc 4c)
- Consumer-gated constraint dry-run (HP2.5)

When consumer pins a shape that needs a **new** rho-tkg primitive, it re-enters
**Open** as a concrete item.

---

## Closed (pointers only — detail in CHANGELOG)

| Epic / item | Where it landed |
|------|-----------------|
| BACKLOG 1 — Retention purge (ex-ADR-0008 R2–R5) | CHANGELOG (4.18–4.24 era) |
| BACKLOG 2 — Cross-machine Model A (ADR-0010 §3.3) | CHANGELOG |
| BACKLOG 3 — Columnar / streaming whole-node fetch | CHANGELOG |
| BACKLOG 4 — Review adaptations (4b–4e; **4a DO NOT BUILD**) | CHANGELOG |
| BACKLOG 5 — Rel ordering-soundness (`RelRangeCardinality`, type-class) | CHANGELOG |
| BACKLOG 6–21 (old numbering) — full-library hardening (~196 findings) | closed 2026-07-18…22, `[4.24.0]` |
| Last HIGH (10b cascade/resumption) + 10c perf follow-up | `[4.24.0]` / next-day fix |
| Consumer-gated ask batch (5 items, 2026-07-29) | `[4.25.0]` same day |
| CI bench-gate (blocking) | 2026-07-29, `bench.yml` |
| Item 3 (HIGH) — entity wire widened nested values; DECIDED 2026-09-24: kind envelope, no write-time normalization | `[4.37.0]` Fixed |
| HIGH — badger reads dropped or replaced rows when a flush + eviction landed mid-read (flush epoch, `scanSnapshot`, `LoadCleanAt`) | `[4.37.1]` Fixed |
| Review 2026-09-24: delete after close, correction template, endpoint masking (HIGH) | `[4.38.0]` (merge `4473705`) |
| Review 2026-09-24: TxAt-only doors "nondeterministic" (implicit valid-time now = wall clock; now `readNow()`) | `[4.38.1]` |
| Item 6 (FEATURE) — `DeleteWithTx` / `UpdateWithTx` on every write door, `ErrTxOrder` | `[4.44.0]` Added; evidence `tasks/evidence/tx-backfill-delete-update/` |
| Item 34 (FEATURE) — `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx` on every door | `[4.50.1]` Added; evidence `tasks/evidence/interval-withtx/` |
| Item 49 (HIGH, tiered) — re-import onto another shard made earlier lives unreachable | `[4.51.2]` Fixed; evidence `tasks/evidence/reimport-shards/` |
| Item 50 (MEDIUM, sigma) — pinned adjacency doors missed a re-imported rel's earlier life; AtNodes O(H) | `[4.51.2]` Added/Changed/Fixed (ex-adjacency index, store marker 3); evidence `tasks/evidence/effective-scans/50-*` |
| Item 51 (HIGH, sigma step 6) — effective scans bounded by valid end | `[4.51.2]` Added; evidence `tasks/evidence/effective-validfrom/` |
| Item 52 (HIGH, ai-soc) — durable commit under SyncWrites synced per mutation | `[4.51.3]` Changed; evidence `tasks/evidence/aisoc-commit-cost/52-*` |
| Item 53 (HIGH, ai-soc) — rel planner-stat memos one RAM entry per rel | `[4.51.3]` Fixed; evidence `tasks/evidence/planner-stats-memo/` |
| Item 54 (HIGH, ai-soc) — badger open linear in history | `[4.51.3]` Changed (open image incl. planner stats, stale-key repair); evidence `tasks/evidence/open-cost/` |
| Item 55 (MEDIUM, ai-soc + sigma) — effective scans one badger txn per rel | `[4.51.3]` Changed; evidence `tasks/evidence/effective-scans/55-*` |
| Item 21 (FEATURE, sigma) — purge relationships by age, per-type retention gate | `[4.51.5]` Added/Fixed; evidence `tasks/evidence/rel-retention/` |
| Item 28 (FEATURE perf, sigma) — state form of the as-of column doors and counts | `[4.51.5]` Added/Changed (ordinals 0 on temporal scans); evidence `tasks/evidence/state-columns/` |
| Item 41 (MEDIUM RAM) — history presence / stamps sidecars | `[4.51.5]` Changed (open-addressing columnar tables); evidence `tasks/evidence/badger-read-cost/41-0*` |
| Item 44 (MEDIUM) — commit-clock floor persisted only at Close: a crash reopened `NowTx` below stored stamps | `[4.51.4]` Fixed (commit floor persisted with the rows, `store.CommitFloorCapability`); evidence `tasks/evidence/clock-floor-crash/` |
| Item 15 (MEDIUM) — durable commit power-loss gap on a memtable switch | `[4.51.4]` docs: power loss needs `DurableCommit` + `SyncWrites` (13.5 ms vs 12.5 ms per 530-op group, `tasks/evidence/aisoc-commit-cost/52-bench.txt`) |
| Item 48 (FEATURE, sigma-tkgd) — `ColumnData.Other`: column-scan cells held outside the column (node and rel batches); typed slots aligned with `Null` | `[4.51.1]` Added; evidence `tasks/evidence/column-other/` |
| Item 35 (FEATURE, sigma-tkgd) — broader effective scans: `ForEachNodeEffective` / `ForEachRelEffective`, `ForEachRelEffectiveAtNodes`, `NodesEffectiveByIDs` / `RelsEffectiveByIDs`, `ForEachNodeEffectiveByLabels` | `[4.51.1]` Added; evidence `tasks/evidence/effective-scans/` |
| Item 11 (FEATURE) — durable-on-return commit `Config.DurableCommit` | `[4.44.0]` Added (power-loss gap: item 15) |
| HIGH — one-tick `[t, t+1)` rows skipped as the eclipse sentinel; skip removed | `[4.44.0]` Fixed "One-tick valid intervals are ordinary spans" |
| Review-v5 §6.2 — column and range scans ignored `TxAt`/`TxPin` | `[4.44.0]` Fixed "Column and range scans answer temporal options" (interval semantics: item 16) |
| Item 20 fix 1a — badger history iterators bounded to the entity prefix | `[4.44.1]` Fixed |
| Item 10 (FEATURE) — tiered composite + relationship temporal indexes, hot+warm bound | `[4.45.0]` Added (remaining work: item 10 above) |
| Item 12 (HIGH) — unique constraint bypass via `SetNodeVersionInterval` props on all four doors | `[4.46.0]` Fixed "unique constraints are enforced on `SetNodeVersionInterval` props patches" |
| Item 14 (HIGH) — a correction on a deleted entity copied the tombstone stamps; refused with `ErrEntityDeleted` | `[4.46.0]` Changed "A valid-time correction on a hard-deleted entity is refused", Fixed "Rows a write appends carry no retraction" |
| Item 18 (HIGH) — cascade row versions collided with later Updates; as-of answers at a pin changed after a later write | `[4.46.0]` Changed "The as-of doors answer the newest row recorded by the pin", Fixed "One version allocator", "The chain resolver reads chains in version order" |
| Item 19 (HIGH) — GraphTx rollback dropped the cascade rows above the current version | `[4.46.0]` Fixed "GraphTx rollback keeps the cascade rows above the current version" |
| Item 20 fix 1b — `Nodes()/Rels().HasHistory` | `[4.46.0]` Added; evidence `tasks/evidence/has-history/` |
| Item 20 fix 2a (HIGH) — a delete after a bounded cascade left the entity readable | `[4.46.0]` Fixed "A delete after a bounded cascade ends the entity in every door" |
| Item 24 (HIGH) — history compaction broke the hash chain of a cascaded entity | `[4.46.0]` Fixed "History compaction keeps every row the kept chain's hash links point to" |
| Item 20 fix 1c — `Node/RelEffectiveTimeline` | `[4.47.0]` Added; evidence `tasks/evidence/effective-timeline/` |
| Item 27 (FEATURE) — scan forms `ForEachRelEffectiveByType` / `ForEachNodeEffectiveByLabel` | `[4.47.0]` Added |
| Item 32 (HIGH) — pinned doors missed an entity while a write moved its row to history | `[4.47.0]` Fixed; evidence `tasks/evidence/point-door-race/` |
| Item 36 (FEATURE) — `types.NodeID.MintInstant()` / `types.RelID.MintInstant()` | `[4.47.0]` Added; evidence `tasks/evidence/mint-instant/` |
| Read-time supersession rule (a replaced row ends where its replacer starts) and life ordering of re-imported IDs | `[4.47.0]` Fixed "A replacing write … ends every older belief", "A re-imported ID's rows order after the earlier life's" (code `chain_supersession.go`, `chainWriteOrder`) |
| Item 8 (MEDIUM, perf) — pinned property lookups cost the history; property membership sidecar | `[4.48.0]` Added "Temporal property lookups cost the value's ever-members"; evidence `tasks/evidence/pinned-property-index/` |
| Item 38 + review 2026-09-24 "(HIGH?) Re-import of a deleted ID" (HIGH, data loss) | `[4.48.0]` Changed "A backfilled re-import of a deleted ID must be recorded after the ID's chain", Fixed "a re-import of a deleted ID no longer overwrites the earlier life's history"; evidence `tasks/evidence/reimport-life/` |
| Item 29 (MEDIUM) — a failed node write left its `UniqueForever` value owned (crash consistency between claim and row: v5 PLAN §5.2) | `[4.49.0]` Fixed; evidence `tasks/evidence/unique-claims/` |
| Item 30 (FEATURE, perf) — `Nodes()/Rels().LatestStamps(id)` | `[4.49.0]` Added; evidence `tasks/evidence/latest-stamps/` |
| Item 43 (FEATURE, ai-soc) — retraction: `Retract` / `RetractWithTx` on every write door, "this was never true" | `[4.49.1]` Added "Retraction: a way to say this was never true"; evidence `tasks/evidence/retraction/` |
| Item 33 (MEDIUM, perf) — badger effective scans read each candidate row once (`store.MemberIDsCapability`) | `[4.51.1]` Changed; evidence `tasks/evidence/badger-read-cost/` |
| Item 42 (FEATURE, perf) — `store.CurrentStampsCapability`, the current-row half of `LatestStamps` | `[4.51.1]` Added; evidence `tasks/evidence/badger-read-cost/` |
| Item 22 (MEDIUM) — badger rel temporal index create raced concurrent writes | `[4.51.6]` Fixed (3-phase mutated-set install); evidence `tasks/evidence/rel-temporal-rebuild/` |
| Item 10(a) (PERF) — sequential-scan rel temporal index rebuild, 3.9-4.5x | `[4.51.6]` Changed; evidence `tasks/evidence/rel-temporal-rebuild/` |
| Item 56 (MEDIUM) — node temporal envelope holes (`PutNodeVersion`, publish-before-fold, label add, memory re-create) | `[4.51.7]` Fixed; evidence `tasks/evidence/node-temporal-envelope/` |
| Item 57 (FEATURE, sigma R1) — `NodesAtTxByIDs` / `RelsAtTxByIDs` | `[4.51.8]` Added |
| Item 59 (PERF, sigma R3) — tiered event-shard routing by binary search | `[4.51.8]` Changed; evidence `tasks/evidence/shard-routing/` |
| Item 58 (FEATURE, sigma R2) — tiered tx-membership sidecars + valid-end acceleration | `[4.52.1]` Added; evidence `tasks/evidence/tiered-capabilities/` |
| Item 60 (LOW) — tiered ref-then-archive readers outside moveMu | `[4.52.1]` Fixed (16 scan doors + point reads under moveMu's read side); evidence `tasks/evidence/tiered-move-readers/` |
| Item 61 (FEATURE, René, ai-soc) — integrity-hash switch (default on), one binary hash path | `[4.52.2]` step 1, `[4.54.0]` step 2 (writer, marker 4); evidence `tasks/evidence/integrity-switch/` |
| Item 62 (SEMANTICS, sigma) — one valid-from source for untimed rows | `[4.52.2]` phase A (Deprecated), `[4.54.0]` phase B (`storeutil.EntityValidFrom`); evidence `tasks/evidence/valid-from-source/`; option for the rule: todo row 42 |
| Item 63 (FEATURE, sigma R6) — `PropertyIndexOptions{Elements: true}` | `[4.52.2]` Added; evidence `tasks/evidence/property-elements/` |
| Item 64 (FEATURE, sigma R7) — `ExciseNodeValidity` / `ExciseRelValidity` | `[4.54.0]` Added (void pieces, marker 6); evidence `tasks/evidence/excise/` |
| Item 65 (SEMANTICS) — `Update` with only `tkg_valid_to` closes like `CloseVersion` | `[4.52.2]` phase A, `[4.54.0]` phase B |
| Item 66 (FEATURE, sigma round 7) — property-index range walks and counts at a tx pin | `[4.55.2]` Added (Markus, round 8 request 2: `RangeCardinalityAt`, sidecar numeric view); evidence `tasks/evidence/cypher-round8/r2-*` |
| Item 67 (FEATURE, sigma) — per label/type explicit-valid-time statement | `[4.55.2]` Added (`Stats().LabelTimeStats` / `RelTypeTimeStats`) |
| Item 68 / 68b (PERF, sigma R10) — `NodeEffectiveTimeline` O(segments), write side from the head entry | `[4.55.0]` read side, `[4.55.1]` write side; evidence `tasks/evidence/r10/`, `r10-write/` |
| Item 69 (FEATURE, ai-soc) — `RelsEffectivePage` / `NodesEffectivePage` | `[4.55.0]` Added; evidence `tasks/evidence/paged-effective/` |
| Item 70 (MEDIUM) — version allocation below surviving rows after a compaction gap; rollback-after-compaction data loss | `[4.55.1]` Fixed; evidence `tasks/evidence/r10-write/` |
| Item 72 (PERF, ai-soc) — sorted postings for property/composite indexes, 1M walk 85 s to 0.5 s | `[4.55.3]` Changed |
| Item 73 (FEATURE, René, sigma-tkgd, ai-soc) — `Config.UntimedValidStart`: untimed first version from TxFrom (default) or -inf, `Admin().SetUntimedValidStart` one-way conversion, marker 7 | `[Unreleased]` Added + Deprecated (default for new stores becomes `UntimedUnbounded` next minor); evidence `tasks/evidence/untimed-mode/` |
| Item 74 (PERF, ai-soc) — first page after an open waited ~3 min: one full store scan per declared rel property index (`buildRelPropTx`, 123 of 259 s CPU); now one scan per kind builds every declared sidecar, and the open image carries the built ones | `[Unreleased]` Changed; evidence `tasks/evidence/sidecar-one-scan/` |
| Item 75 (PERF, ai-soc) — paged effective doors prune valid-filtered candidates by the envelope index (no automatic build: a full type scan plus RAM and a write cost per relationship forever; reopen if a consumer cannot declare `CreateRelTemporal`) | `[Unreleased]` Changed; evidence `tasks/evidence/page-valid-prune/` |

Recover closed investigation prose via `git log --all -- tasks/backlog.md` if needed.

### DO NOT BUILD / reopen criteria (keep here so they are not re-filed)

- **Distinct start and end counts per relationship type maintained with the writes (sigma-tkgd round 5 request 5):
  NOT BUILT 2026-10-11.** `RelTypeDegreeStats` is derived and cached per type; since round 5 request 9 the memory
  store keys it on a per-type stamp as badger does, so the cold harness's writes to another type no longer force
  the recount that motivated the request. Maintaining exact distinct counts under writes to the type itself needs a
  per-node degree counter per type (O(distinct endpoints) RAM, about 40-50 B per endpoint in a Go map) and two map
  updates per relationship write of the type, a new write and memory cost the round's design rule only allows as
  opt-in or lazy; sigma-tkgd reverted its use of the degree statistics for the one-hop estimate pending a cost-model
  calibration (C4s). Reopen when sigma-tkgd re-adopts the degree statistics and measures the recount after writes
  to the read type itself on its cold rows; then build it opt-in per type (built lazily on the first
  `RelTypeDegreeStats` of a type that opted in, maintained at the relationship write seams).
  Re-evaluated 2026-10-14 (round 6): half met. sigma-tkgd re-adopted the statistic (`a04176971`, C4u: the weighed
  expand's fanout from `RelTypeDegreeStats`) and states the recount after a write to the type itself, but has not
  measured it on a cold row; its cold harness writes another type, which round 5 request 9 already keeps from
  forcing a recount. Still not built; the reopen criterion is the measurement.
  BUILT 2026-10-14 (round 7 request 2): measured in rho-tkg (`BenchmarkRelTypeDegreeStatsRecount`, three types of
  n relationships): the recount after a write to the read type itself is 0.7 ms at n = 10,000 and 12-16 ms at
  n = 100,000 (memory and badger) against 2-6 us for a write plus a cached read; a statement that writes a type
  and plans a hop over it pays that per statement. Criterion met; built opt-in per type
  (`Stats().TrackRelTypeDegrees`), see CHANGELOG [Unreleased]. Kept here as the record of the decision.
- **Item 9 — property-keyed unique constraint on relationships (ai-soc request 4): WITHDRAWN 2026-10-09** (René,
  relayed by ai-soc: "rho-tkg already has unique snowflake ids"). ai-soc uses the snowflake ID as the global ID.
  Possible cheap follow-up, not requested: a relationship door that accepts a caller-supplied snowflake ID "if
  absent", mirroring `Nodes().AddByIDIfAbsent` (today `Rels().AddByIDIfAbsent` dedups on `(start, end, type)`).
- **DECIDED NO 2026-10-09 (René, relayed by ai-soc):** an index-provider "failure aborts the mutation" option
  (ai-soc request 5; ai-soc rebuilds from the change feed) and one instant per commit group (ai-soc request 7; the
  cut-record pin is enough). Reopen only with a consumer case the change feed or the cut-record pin cannot cover.
- **Async ingest outcome ambiguity (R4 of the temporal-replay plan, ai-soc):** NOT BUILT, conditional. R0-R3 and R5 shipped
  (R3/R5 in v4.35.1, `store.GroupCommitCapability`; `Config.DurableCommit` in v4.44.0); the ingest `AppliedSeq` counts failed
  attempts and async failures are consumed on first wait (evicted after 8,192), so a process-local token is no durable checkpoint.
  Reopen only if ai-soc moves off synchronous sessions. Benchmark and evidence: `bench/temporalreplay/`,
  `tasks/evidence/temporal-index/` (the plan file `tasks/plan-temporal-index-ingestion.md` was removed, in git history).
- **Per-version temporal-envelope prune (ex-BACKLOG 4a):** owner-decided net-negative
  for the confirmed workload. Detail in CHANGELOG.
- **Wire `fv` bump** (widen temporal tail to full envelope + eclipsed-row flag) **and
  inverted-suffix history keyspace:** DECIDED NOT BUILT 2026-07-29 after no-format-change
  paths shipped (tail-peek TX; selection-skeleton + zero-alloc token scanner VT).
  **Reopen only if** consumer re-runs depth oracles against current HEAD **and** residual
  depth-linearity still breaks a concrete consumer latency budget — then the prepared
  v3 design is the next step.
- **Zone maps before I/O (columnar phase-2 ZM):** withdrawn 2026-08-04; envelope prune +
  in-RAM `BlockCanMatch` cover the need. Reopen only if a consumer needs a block-level
  pre-I/O prune that those cannot express without breaking full-membership snapshots
  (see CHANGELOG `[4.27.0]`).


### Reviewed native-v4 comparison prerequisite

The pinned correctness tool reproduces 92 Memory +92 synchronous Badger +23 reopened current answers against shipped v4.43.0. One retained-history query is pending. All three vendor comparisons, native-v5 integration, full declared resource/performance profiles and V0–V7 acceptance remain open; see the portable comparison-harness proof and tool README.
