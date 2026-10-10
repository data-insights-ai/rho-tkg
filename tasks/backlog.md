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

**Remaining open work (verified against the code at v4.49.0, 2026-10-10):** no reproduced CRITICAL or HIGH
item; two traced, unreproduced HIGH? findings in item 4. Next step: item 34, then 33 + 42 (+ 41),
35, 28, 21. Open items, one line each (full text below):

| # | Type / severity | Item | Status |
|---|---|---|---|
| 0 | FEATURE | Column segments on NVMe (ADR-0011) | S0-S3, S5 done (S2/S5 in v4.39.0, S3 + P7 in v4.40.0); S4 merge, S6, S7 and sub-points (a), (b), (d)-(i) open |
| 1 | MEDIUM (improvement, deferred locking) | Import-under-a-scope | open; `IO().Import` still takes only `c.mu` (no `c.txMu`), stopgap in place |
| 2 | TEST-GAP | Old BACKLOG 22: six TEST-GAP research items (22a-22f) | open; prioritise only when a consumer needs the confidence |
| 3 | MEDIUM (perf, unmeasured) | Temporal adjacency scan cost (v4.35.0 follow-up) | open; measure first |
| 4 | HIGH? / MEDIUM? (traced, not reproduced) | Temporal semantics review 2026-09-24: traced findings | open; each needs a failing two-phase test first; code unchanged at the cited sites (`resolveOpenEndInstant`, `validInstantAfter` in `core/temporal.go`) |
| 5 | MEDIUM (perf) | P7 row compaction follow-ups | open; options (a)-(d) unbuilt (P7 itself shipped in v4.40.0) |
| 7 | FEATURE (limitation) | Caller-instant delete of a row with a scheduled close | open; `DeleteWithTx` refuses `ValidTo >= t` with `ErrTxOrder` (R8, v4.44.0) |
| 10 | FEATURE | Tiered store: remaining work after composite + rel temporal indexes | indexes shipped in v4.45.0; (a) sequential-scan rebuild and (b) rel property indexes / node `TemporalCandidateCapability` on tiered open |
| 13 | MEDIUM | Ingest applier attributes group errors by numeric entity id, not by kind | open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`) |
| 15 | MEDIUM | Durable commit: power-loss gap on a memtable switch | open; documented on `Config.DurableCommit` (v4.44.0); scheduling: René |
| 16 | MEDIUM (contract) | Range folds under an interval: predicate-anywhere or value at the resolved version? | open; decision pending; behaviour documented in the v4.44.0 CHANGELOG |
| 20 | LOW (TEST-GAP, perf) | Effective-read cost: RelAtTx benchmark targets unmeasured | fixes 1a (v4.44.1), 1b (v4.46.0), 1c (v4.47.0), 2a (v4.46.0) shipped; only the RelAtTx benchmarks remain |
| 21 | FEATURE | Purge relationships by age, gate retention reads per type | open; no `PurgeExpiredRels` in the tree; sigma: not blocking until `@overview` starts |
| 22 | MEDIUM (not reproduced) | Badger rel temporal index create races concurrent writes | open; `CreateRelTemporalIndex` still uses the snapshot / unlocked build / install shape |
| 23 | LOW (perf, unmeasured) | `changeFeedPage` prefetches past its page | open; `PrefetchValues = true`, no `opts.Prefix` (`badgerstore_changelog.go:648-659`) |
| 25 | MEDIUM | A future close followed by a delete changes the answer at an earlier pin | open; `stampDeleteTombstone` still clamps (`core/temporal.go:133-137`) |
| 26 | LOW (known limit) | Old chains with collided versions keep answering in the old version order | open; schedule only if a consumer holds such chains |
| 28 | FEATURE (perf) | State form of the as-of column doors and counts | open; design first (sigma) |
| 31 | LOW | Bulk and point as-of disagree with the cascade oracle on version gaps | open; fix when a consumer holds such chains |
| 33 | MEDIUM (perf) | Badger effective-timeline scan costs 13-17 µs per relationship | open; restart pending (baseline `tasks/evidence/badger-read-cost/`) |
| 34 | FEATURE | `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx` | open; agent stopped before committing anything; restart (sigma replay need) |
| 35 | FEATURE | Broader effective scans | open; signatures confirmed by sigma 2026-10-10 (HANDOVER.md §11) |
| 37 | LOW | Tiered error-rollback restore leaves a window with empty history | open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`) |
| 39 | LOW (not tested) | Per-entity compaction stubs survive `Admin().Reset` | open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`) |
| 40 | LOW (cosmetic) | A cascade resumption may end at an earlier life's own bound | open |
| 41 | MEDIUM (RAM) | Merge the history stamps sidecar into the presence set | open; after 42 |
| 42 | FEATURE (perf) | A current-row stamps capability | open; restart with 33 |
| 44 | MEDIUM (traced, not reproduced) | Commit-clock floor persisted only at Close | open; filed 2026-10-10 from `tasks/review-v5-plan-vs-code-20261009.md` §6.5 |
| 45 | MEDIUM (API) | Internal sentinels reach callers but have no exported alias; `ErrMixedNumericColumn` is outside the errors inventory test | open; found by the doc review of `docs/errors.md` 2026-10-10 |
| 46 | LOW (stale comments) | Code comments that contradict the code or cite ADRs no longer in `docs/adr/` | open; found by the doc reviews 2026-10-10 |
| 47 | LOW (unverified claims) | `NodesByLabelAt` vs K1; a RAM budget for the property sidecars | open; each needs a real check before it becomes work |
| 48 | FEATURE (small) | `Other` marker on node `ColumnData` (`ScanNodeColumns`) | open; requested by sigma-tkgd 2026-10-10; patch release |

**v5:** the next engine generation is planned on branch `v5` (`docs/v5/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md`, revised 2026-10-09 after `tasks/review-v5-plan-vs-code-20261009.md`; branch created 2026-10-09 from main `32568c4`). v4 keeps taking consumer features (decision René 2026-10-09, the earlier fixes-only rule is deleted). ADR-0011 S2+ waited for owner decision D6 in that plan. **DECIDED 2026-09-25 (René): S2 and S5 ship on v4 as v4.39.0**, because the AI-SOC cross-check needs them now (ADR-0011 §6 gate met: 24.0 / 24.8 / 25.7 B/HOP at 790 K / 3.15 M / 12.6 M); v5 carries the segments forward from this code.

---

## Open — v5 V0–V7 acceptance

Specification and evidence checklist: [PLAN.md §8](../docs/v5/PLAN.md#8-implementation-sequence).
Pinned fixtures: [consumer contracts](../docs/v5/reference/consumer-contracts.json)
and [migration obligations](../docs/v5/reference/consumer-migration.json) are
reviewed specification/inventory evidence; consumer integration acceptance stays open.
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
| V1 | Reviewed/implemented temporal primitives and codecs (exact scalar/tuple values, scopes/regions/Allen, point knowledge and opaque descriptors), plus component-state reducer primitives. Graphstate `Plan`/`Project` and the byte-identical 16-fixture/52-assertion Go path accepted (local `e5b23d9`), including bounded page/type/uniqueness/CDC-delta behavior. Pure value/state preservation, remaining revised models and phase-specific Go evidence still require V1 acceptance; durable engine/storage and certified-cut integration belong to V2/V3 and remain separate |
| V2 | Candidate Raft/Pebble log/replica adapter independently validated as an adapter only. Bounded idalloc primitive independently validated on Go 1.26.9 (99.3% coverage; eight source/config hashes match). Bounded two-group transaction correctness prototype accepted (`92312ae`; independent 24-source/config checks, scoped combined coverage 86.3%). Six-process crash-functional slice accepted (`f3a1ee7`); power-loss/multi-host durability and global cross-process cut transport stay open. Full graph transaction/cut integration and serial-history oracle, production graph identity reuse/fencing, application GC, end-to-end Driver snapshot activation and Raft application multi-voter integration, physical-host durability, comparative costs/faults and engine selection remain open |
| V3 | Coordinate timeblock candidate independently validated (local commit `fa0c982`; scoped temporal+block coverage 95.7%, no exported method at 0%). Full engine seal/merge/recovery, paged structures and all-in budget/byte-ledger measurements remain open; resident-buffer prototype timings do not close V3 |
| V4 | Production read/change APIs, historical cross-door parity, gap-free leased feed handoff and pinned sigma access-contract build and nonempty mutation-then-historical tests (the current signature/empty smoke is insufficient) |
| V5 | Distributed access across 1/2/4/8 partitions, cross-edge/stall/rebalance/feed cases and sigma workload parity |
| V6 | Provenance-aware importer and ambiguity report, consumer-repository migrations, retained-cut/CDC agreement and restore on a different topology |
| V7 | Versioned contracts/limits, full CI/race/coverage/security, distributed matrix and consumer suites; measured comparative space/time acceptance against Neo4j/TigerGraph/Memgraph plus v4, with reviewed excess-cost exceptions |

V0 specification acceptance, 2026-10-10:

| Criterion | Accepted evidence | Remaining phase |
|---|---|---|
| Contracts/ownership and IDs/cuts/formats | [PLAN §§2–5/8](../docs/v5/PLAN.md) separates preservation, native access and sigma evaluation, with explicit failure outcomes | V1 implementation; V2–V5 database/storage/access |
| Independent corpus/models | [Reference validation](../docs/v5/reference/independent/validation.json): 38 tests, unchanged 16/52, 42 revised records, 18,192 pairs, 600 mutations, 88 accepted/39 refused protocol histories; ordinary freshness check passes | V1 Go differential/attachments; V2 durable fault/cut evidence |
| Units and compatibility | PLAN §2.3: Q/ms/POSIX1970 default policy, exact integral-ms Instant codec, explicit other axes and conversion/refusal boundaries | V1 default-axis factory/binding; V2 persistence; V6 importer |
| Versioned consumer/data inventory | [Pinned doors](../docs/v5/reference/consumer-contracts.json), [migration obligations](../docs/v5/reference/consumer-migration.json), PLAN §8a current `95a1de39`/v4.50.0 delta and declared datasets | V4 actual adapters; V6 migration; V7 representative runs |
| Resource profiles and limits | [Comparison protocol](../docs/v5/reference/graph-db-comparison.json): future native test configurations, one-factor/pinned-mixed dataset controls, separate local functional lanes; initial numerical limits distinguished from proposed performance thresholds | V2–V3 fault/cost evidence; V7 measured comparative capacity |
| Import provenance | PLAN §9 and reconciled [v4 handover](v4-changes-for-v5-importer-20261010.md): verified writer/format provenance; dates/one-tick width alone are insufficient | V6 implementation/ambiguity report |

Fresh `check.py --write-validation`, ordinary `check.py` and original
`design_checks.py` pass. Original design checks retain 16/52, 20,000 domain pairs,
480 schedules/80 certified-read examples and the weak-cut counterexample.
Corpus/oracle/consumer pins are unchanged. This closes specification V0 only;
declared resources are test configurations, not deployed/proven capacity or an
owner SLO. No solver, engine, consumer-integration or performance acceptance follows.

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

**Caller-instant delete of a row with a scheduled close** (FEATURE, follows item 6): `DeleteWithTx(id, t)` refuses when the recorded `ValidTo >= t`, because one tombstone row cannot end belief at `t` and keep the close that pins before `t` believed (decision 2026-10-09, handover §6.11; the refusal shipped in v4.44.0, `core/tx_order.go`). Lifting it needs a tombstone that keeps the believed `ValidTo` beside the deletion instant (no clamp for a caller instant), with the normalizer (`normalizeTemporalVisibleAtTxTime`, `core/txtime.go:495`) and the valid-time history reads agreeing. Write the two-phase red test first.

### 10. Tiered store: remaining work after composite + rel temporal indexes

*FEATURE* — indexes shipped in v4.45.0; (a) sequential-scan rebuild and (b) rel property indexes / node `TemporalCandidateCapability` on tiered open.

Composite and relationship temporal indexes on tiered shipped in v4.45.0 (ai-soc request 3; per-shard fan-out anchored by the reference shard; rel temporal 156 B/relationship, composite 310 B/node; the rel temporal index lives on reference + hot + warm shards only, cold demotion frees it, `PromoteColdShardsAtOpen` rebuilds it; ≈ 5.8–9.2 GB per indexed type at `ColdAfter` = 1 day). Remaining: (a) a sequential-scan rebuild — today one point read plus one history prefix scan per relationship (0.018–0.021 M rels/s), 29–54 min per day shard on reopen or promotion; (b) relationship property indexes (`CreateRelPropertyIndex` declines, `store/tiered/tieredstore_write.go`) and the node-side `TemporalCandidateCapability` are still declined on tiered.

### 13. Ingest applier attributes group errors by numeric entity id, not by kind

*MEDIUM* — open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`).

**Ingest applier attributes group errors by numeric entity id, not by kind** (MEDIUM, found 2026-10-09 writing `TestSessionSetVersionInterval_FailedGroupShape`): the strong-async applier keys group-error attribution by the numeric id, so a missing node id and a missing rel id with the same number attribute errors to the wrong group. Minted snowflake ids never collide across the two generators, but caller-supplied ids (`AddByID`, `Import`) can (the rollback-snapshot rule in AGENTS.md "Data Model" already keys by kind + id for the same reason). Red test first: two groups, a node and a rel with the same numeric id, each missing; each group gets its own sentinel. Fix: key by (kind, id).

### 15. Durable commit: power-loss gap on a memtable switch

*MEDIUM* — open; documented on `Config.DurableCommit` (v4.44.0); scheduling: René.

**Durable commit: power-loss gap on a memtable switch** (MEDIUM, follows item 11, found by the item-11 review 2026-10-09): `Config.DurableCommit`'s `DurableFlush` ends with badger `db.Sync()`, which fsyncs only the active memtable's WAL and the current value-log file (badger v4.9.2 `db.go:705-709`). A flush that fills the memtable switches to a new one in `ensureRoomForWrite` (`db.go:1015-1040`) without syncing the retired WAL, and a finished value-log file is synced only under `SyncWrites` (`memtable.go:408`), so rows of a group that crossed a switch can be unsynced after `DurableFlush` returned: process-crash safe, not power-loss safe (documented on `Config.DurableCommit`, `store.DurableFlushCapability`, docs/architecture.md "Durable-on-return commit"). Options: (a) sync the retired memtable WAL (and the finished vlog file) on switch when a durable flush is in progress — needs a badger hook or a fork; (b) measure `SyncWrites` cost against `DurableCommit` on a group workload and recommend it for strict durability. Red test first: a group larger than one memtable, a power-loss-shaped check (fsync accounting or a dm-flakey/`LD_PRELOAD` drop of unsynced pages). Scheduling: René.

### 16. Range folds under an interval: predicate-anywhere or value at the resolved version?

*MEDIUM (contract)* — open; decision pending; behaviour documented in the v4.44.0 CHANGELOG.

**Range folds under an interval: predicate-anywhere or value at the resolved version?** (MEDIUM, contract inconsistency, found reviewing item C scan-temporal-opts 2026-10-09). Decide whether all range folds (ordered included) pass the range into the resolver's predicate (predicate-anywhere) like `ByLabelAndProperty`. Today, under `ValidStart`+`ValidEnd`, `Nodes().ForEachByLabelPropertyRange` / `Rels().ForEachByTypePropertyRange` and the ordered / prefix siblings (`forEachNodeInRangeTemporal`, `forEachNodeValueOrderedTemporal` and rel mirrors) test the range on the ONE version `ByLabel` / `ByType` resolve (the most recent overlapping one), while `ByLabelAndProperty` / `ByTypeAndProperty` pass the property into `findNodeVersionForOpts`'s predicate and match a value held anywhere in the interval (`graph_property_query.go:87-95`). A value in range only during an earlier part of the interval is found by the equality door and missed by the range doors. Point opts (`ValidAt`, `TxAt`, `TxPin`) agree. If predicate-anywhere is chosen: the selecting predicate must be EXACT (an over-selecting one could pick a version fn then rejects while an earlier one matched), and the ordered doors sort on the value of the matching version. Red test first: rule-16 shape (value in range on the earlier version only, interval spanning both) across all range doors.

### 20. Effective-read cost: RelAtTx benchmark targets unmeasured

*LOW (TEST-GAP, perf)* — fixes 1a (v4.44.1), 1b (v4.46.0), 1c (v4.47.0), 2a (v4.46.0) shipped; only the RelAtTx benchmarks remain.

Everything the handover `tasks/handover-effective-read-cost-20261009.md` asked for shipped (Closed table) except its `BenchmarkRelAtTx/{plain,cascaded}/{hot,cold}` rows and their targets (< 1 µs plain hot row, cold row + 1 µs, ≤ 6 µs cascaded via the skeleton path; 2.0 / 9.5 / 18.5 µs at v4.43.0): no such benchmark exists in the tree. Add it (ReportAllocs, bench-gate) and measure on v4.49.0 before deciding whether any work remains.

### 21. Purge relationships by age, gate retention reads per type

*FEATURE* — open; no `PurgeExpiredRels` in the tree; sigma: not blocking until `@overview` starts.

**Purge relationships by age, and gate retention reads per type** (FEATURE, requested by sigma-tkgd's temporal overview design 2026-10-09; first filed as item 10 in c8de314 and dropped by my backlog edit in f952015, restored here under a new number because 10 is now the tiered indexes): `Admin().PurgeExpiredRels` with a watermark per relationship type, a retention scan gate that a purge of the raw event type does not trip for the long-lived `Overview` nodes, and the tiered landing place of a long-lived node's new versions (answered from the code). Handover with verified citations, API, backend and change-feed semantics, and the red tests to write first: `tasks/handover-overview-retention-20261009.md`.

### 22. Badger rel temporal index create races concurrent writes

*MEDIUM (not reproduced)* — open; `CreateRelTemporalIndex` still uses the snapshot / unlocked build / install shape.

**Badger rel temporal index create races concurrent writes** (MEDIUM, pre-existing, NOT reproduced — read from code, review of backlog 10, 2026-10-09). `CreateRelTemporalIndex` (`store/badger/badgerstore_reltype_temporal_index.go`, the create body) snapshots the type's relationship IDs under `idxMu`, builds the envelope index unlocked and installs it; a relationship created or updated in the gap is not folded in, because `ExtendRelInTemporalIndexes` returns early while no index exists — lesson 74's lazy-build rule. A missed new relationship is merely uncovered (kept); a missed update of a covered one can leave its envelope short of a row, i.e. an unsound prune. On tiered this now runs against the hot shard while it ingests. Red test first with a write-generation guard (lesson 63): write between snapshot and install, then prune at the written row's interval; fix with the 3-phase mutated-set install `CreateTemporalIndex` uses. The header comment was corrected (it claimed the scan held `idxMu`).

### 23. `changeFeedPage` prefetches past its page

*LOW (perf, unmeasured)* — open; `PrefetchValues = true`, no `opts.Prefix` (`badgerstore_changelog.go:648-659`).

**`changeFeedPage` prefetches past its page** (LOW, perf, found by the review of the history-prefix fix 2026-10-09; not measured): `store/badger/badgerstore_changelog.go:654-659` iterates with `PrefetchValues = true`, no `opts.Prefix`, `Seek(start)` then `ValidForPrefix(ChangeLogPrefix())`: a small-limit poll near the tail prefetches up to 100 values it never uses and may read past the change-log keyspace. Fix: `opts.Prefix = prefix` before `NewIterator`, and `PrefetchValues = limit == 0 || limit > 100`. Measure with a polling benchmark first (replica watchers poll it).

### 25. A future close followed by a delete changes the answer at an earlier pin

*MEDIUM* — open; `stampDeleteTombstone` still clamps (`core/temporal.go:133-137`).

**A future close followed by a delete changes the answer at an earlier pin** (MEDIUM, pre-existing on main, found by the review of the cascade-correctness branch 2026-10-09): `CloseVersion(far-1)` → pin → `Delete`: `AsOf(pin)` changes from `[1000,far-1)` to `[1000,inf)` and `AtTx(far, pin)` from absent to present, because `stampDeleteTombstone` clamps the close to the delete instant (`core/temporal.go:136`) and the as-of rewind (`normalizeTemporalVisibleAtTxTime`, `core/txtime.go:495`) reopens it to 0 (the same "known limitation: future-scheduled close, then delete" of the 2026-09-24 review, now that the life-end cap exists the delete could keep a finite ValidTo). Red test first: two-phase on all four backends, node and rel; fix: the tombstone keeps the believed ValidTo beside the deletion instant (see item 7). (Seen again on a re-imported ID's earlier life, item L review 2026-10-09.)

### 26. Old chains with collided versions keep answering in the old version order

*LOW (known limit)* — open; schedule only if a consumer holds such chains.

**Old chains with collided versions keep answering in the old version order** (LOW, known limit, documented in the v4.46.0 CHANGELOG): chains written before the version allocator (backlog 18b) that already hold two rows with one version cannot be repaired by the read rule: e.g. old S2 followed by two old Updates answers the patch row at the pin after the first Update. A one-off repair tool (walk chains, renumber) is possible without a format change; schedule only if a consumer holds such chains.

### 28. State form of the as-of column doors and counts

*FEATURE (perf)* — open; design first (sigma).

**State form of the as-of column doors and counts** (FEATURE, requested by sigma-tkgd 2026-10-09 after it moved every pinned read to the state doors): pinned COUNT and aggregate queries lost the as-of column path because `DocValuesSnapshotAsOf` / the as-of column set / `ScanNodeColumns` as-of are record doors. Since v4.46.0 `ScanNodeColumns`/`ScanRelColumns` answer `ValidAt + TxAt` exactly (the ByLabel/ByType fold, item C), but at row-fold cost. Wanted: a cached state column set keyed by (label, valid-at, pin) (or a segment-based cut) so `CountByLabelAt`/column scans with `ValidAt + TxAt` regain the as-of column speed. Design first (cache key explosion over valid-at; reuse the effective-timeline cut logic of item J). **Measured by sigma-tkgd on v4.46.0 (2026-10-09), pinned count over 1 M nodes with every tenth updated after the pin, 3 runs:** memory store state label scan 1.81 s, state `ScanNodeColumns` 1.74 s, unpinned columns 0.61 s; badger in-memory 14.2 s (80 M allocs) and 13.0 s (74 M allocs) for the state doors vs 1.41 s unpinned columns: the v4.46 state column door is exact but only 4–8 % faster than the label scan (it folds rows) and about 9× slower than unpinned columns on badger. Target: a state column path within ~2× of the unpinned columns on badger (rows whose current row answers the state need no history read; only entities with history after the pin need the cut).

### 31. Bulk and point as-of disagree with the cascade oracle on version gaps

*LOW* — open; fix when a consumer holds such chains.

**Bulk and point as-of disagree with the cascade oracle on version gaps** (LOW, pre-existing, found by the review of the bulk as-of change 2026-10-09): on a chain with a version gap above the current row (direct `Store.TruncateNodeHistory`, or a re-import over a compacted old life stored by v4.43–v4.47 (since item 38 a re-import starts above the chain's top)) both `NodeAsOf` doors miss a row above a gap past current+1 (56 cases against `cascadeAsofWant` in the reviewer's scratch test, identical on main and the branch). The allocator (item G) never creates such a gap; the density assumption is documented in `core/version_alloc.go` and AGENTS.md. Fix when a consumer holds such chains: either close the gap at truncate/re-import time (renumber under the entity lock) or make the doors scan for the max version above current. Red test first (the reviewer's five gap chains, node and rel, with/without reopen, 17 pins).

### 33. Badger effective-timeline scan costs 13-17 µs per relationship

*MEDIUM (perf)* — open; restart pending (baseline `tasks/evidence/badger-read-cost/`).

**Badger effective-timeline scan costs 13-17 µs per relationship** (MEDIUM, perf, from the effective-timeline numbers of v4.47.0: ≈ 3 s for 200 K rels vs 1.3 µs/rel on memory): the pinned candidate gather decodes every current row and each rel's row is then read again. Reuse the rows the gather decoded, or gather candidate IDs only (key scan) and read once. Measure with `BenchmarkForEachRelEffectiveByType` before and after.

### 34. `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`

*FEATURE* — open; agent stopped before committing anything; restart (sigma replay need).

**`SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`: a caller instant for interval rewrites** (FEATURE, requested by sigma-tkgd 2026-10-09 from the burst-growth replay): a replayed burst growth `[vs,ve) → [vs,ve')` cannot be stamped at the record's own transaction instant because the append-only cascade (`Temporal().SetXVersionInterval`, `GraphTx`, `BatchBuilder`, ingest `Session`) stamps `TxFrom = now`. Mirror `DeleteWithTx`/`UpdateWithTx` (handover tx-backfill §6): gate `AllowTxBackfill`, `ErrInvalidTxFrom` for non-positive or future instants, `ErrTxOrder` against the entity's own chain (t must exceed the newest `TxFrom` and every appended piece takes the same caller instant), one instant for every appended row of the cascade, `notePastDatedWrite` for the as-of cache, whole-unit pre-flight in Batch/ingest, replica reproduces the stamps. Red tests first on all four backends, node and rel, every door (the W5 oracle gets the new door in its generator).

### 35. Broader effective scans

*FEATURE* — open; signatures confirmed by sigma 2026-10-10 (HANDOVER.md §11).

**Broader effective scans** (FEATURE, requested by sigma-tkgd 2026-10-09, exact call patterns from its builtins and push-down; N/M = nodes/rels at the pin, K up to 500 k rels from a push-down, S up to 100 k seed nodes, E entities per session cut): after `ForEachRelEffectiveByType` / `ForEachNodeEffectiveByLabel` (item J), all with the same contract as the per-entity door (one entity's segments contiguous and ascending, entities deleted before the pin included, fn false stops): (1) `ForEachNodeEffective(pin, fn func(NodeSegment) bool)` and `ForEachRelEffective(pin, fn func(RelSegment) bool)`, label- and type-free, one call each per evaluation (builtins node/2, edge/3, prop/3 today: a TxPin scan that misses entities deleted before the pin plus one timeline call per row, twice when prop is used); (2) `ForEachRelEffectiveAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin, fn func(RelSegment) bool)` batched for the src_/dst_ push-down (today `Outgoing/IncomingForNodesAtPin`, which misses rels deleted before the pin, plus K timeline calls), direction fixed per call, deleted-before-pin rels included, each rel once even if both endpoints are seeds (a single-node form would mean up to 100 k calls); (3) `NodesEffectiveByIDs(ids []types.NodeID, pin, fn)` and `RelsEffectiveByIDs(ids []types.RelID, pin, fn)`, unknown IDs skipped, one call per @source or per cut and kind; (4) optional, low priority: `ForEachNodeEffectiveByLabels(labels []string, pin, fn)`, each node once even with several matching labels. Red tests first: exact-set per pin on the oracle generators (live, closed, deleted-before-pin, deleted-after-pin, created-after-pin; rels with both endpoints seeded; duplicate IDs in a by-IDs batch), memory/badger/tiered/sharded.

### 37. Tiered error-rollback restore leaves a window with empty history

*LOW* — open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`).

**Tiered error-rollback restore leaves a window with empty history** (LOW, pre-existing, found by the review of the point-door race fix 2026-10-09): `rollbackDeletedRelationships` → `restoreRelHistorySnapshot` (`store/tiered/tieredstore_write_history.go:899`) runs `TruncateRelHistory(0)` and then `PutRelVersion`; on that error-rollback path a concurrent reader can see an empty history between the two calls. Fix by restoring in one shard call or under a lock readers respect (the publish-history-first rule of AGENTS.md "Version History"). Red test first: a hook between the truncate and the put, plus a pinned `RelAtTx`/`History` read there, on the tiered store.

### 39. Per-entity compaction stubs survive `Admin().Reset`

*LOW (not tested)* — open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`).

**Per-entity compaction stubs survive `Admin().Reset`** (LOW, found by the review of item L 2026-10-09; not tested): `reapCompactionForReset` deliberately leaves per-entity stubs (it assumes IDs are never reused); on memory an `Import` of an old ID after `Admin().Reset` can still restart at version 0 under a leftover stub, because Reset zeroes the watermark that gates the stub probe (`stubLifeStart`). Red test first: compact → Reset → Import(old ID) → `Verify*Chain` on memory/badger/tiered, node and rel. Fix: Reset reaps the stubs (it already wipes the rows), or the stub probe does not depend on the watermark.

### 40. A cascade resumption may end at an earlier life's own bound

*LOW (cosmetic)* — open.

**A cascade resumption may end at an earlier life's own bound** (LOW, cosmetic, item L review 2026-10-09): `temporal_cascade.go:334,352` (`nodeResumptionEnd` / `relResumptionEnd`) considers bounds of rows before the last tombstone, which adds an extra split row; reads are unchanged (life oracle). Fix only if row count matters: consider only rows after the last tombstone. Red first: a row-count assertion on a cascade after a re-import.

### 41. Merge the history stamps sidecar into the presence set

*MEDIUM (RAM)* — open; after 42.

**Merge the history stamps sidecar into the presence set** (MEDIUM, RAM, from the review of `LatestStamps` 2026-10-10): one map with `{from, to, top}` under one protocol saves about 30 B/ID and one of the two lazy builds (66 B/ID today vs the 16 B target; 66 MB at 1 M IDs with history). Red first: the existing differentials (HasHistory, LatestStamps) stay green on the merged structure; a memory-per-ID benchmark gate.

### 42. A current-row stamps capability

*FEATURE (perf)* — open; restart with 33.

**A current-row stamps capability** (FEATURE, perf, from the review of `LatestStamps` 2026-10-10): `LatestStamps` on a badger entity-cache miss costs 6.5–7.1 µs and 25 allocs (reads and decodes the whole current row), sharded/tiered allocate 8–12 per call (core treats sharded as an untrusted store: no Lend, a validated copy). Add an optional capability that reads only the current row's temporal block (badger: temporal-block read on a cache miss; sharded/tiered: slot routing without copying). Measure `BenchmarkLatestStampsRotating` before and after.

### 44. Commit-clock floor persisted only at Close

*MEDIUM (traced, not reproduced)* — open; filed 2026-10-10 from `tasks/review-v5-plan-vs-code-20261009.md` §6.5.

A crash after a burst whose monotonic floor outran the wall reopens with `NowTx` below committed stamps (lesson 71's reopen case on the crash path): `persistInstantFloor` is called only from `Close` (`core/core.go`, `core/instant_floor.go:129`), and `seedInstantFloor` reseeds only from that watermark. Red test first: a crash-shaped child (exit without `Close`) after writes stamped ahead of the wall, reopen, assert `NowTx()` and a fresh write's `TxFrom` exceed every stored `TxFrom`. Fix candidates: persist the floor with the durable commit / flush, or reseed from the newest stored stamp at open.

---

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

*LOW (stale comments)* — found by the doc reviews (groups C, D, 2026-10-10).

`pkg/types/node.go:30` says "88 bytes" (the struct is 96 B, `pkg/types/layout_test.go`); `pkg/graph/internal/core/ingest_lanes.go:79-80`
says the lane range is [0,127] (it is 0-15); `pkg/graph/store/tiered/tieredstore_property_stats.go:19` and `bench/ingest_pipeline_test.go`
cite ADR-0005 / ADR-0006, which are no longer in `docs/adr/` (recover with `git log --all -- docs/adr/`, or rewrite the comment to state the
rule itself); the header of `pkg/graph/internal/core/effective_timeline.go` (~lines 10-20) describes resolving each piece with the point resolver, but the code (`effectivePieces`, ~line 115) uses one max-heap sweep. Also stale lesson citations: `pkg/graph/store/badger/badgerstore_property_stats.go:38` cites lesson 62 for the write-generation guard (it is lesson 63); `pkg/graph/store/store.go:30` and the `B33`/`B7`/`B36` citations in `docs/architecture.md` use an old B-numbering that no longer exists in `tasks/lessons.md`. Comment-only change; the docs-consistency tests must stay green.

### 47. Claims that need a real check before they become work

*LOW (unverified)* — carried from HANDOVER.md §7 and left unfiled by the doc review of the tasks files (group E).

(a) "`NodesByLabelAt` still folds all history instead of using the K1 sidecar": `NodesByLabelAt` is a valid-time door and K1 is
transaction-time membership; verify whether the named door can use K1 at all (a two-phase test with a churned label, memory/badger)
before filing a fix. (b) "A RAM budget for the property sidecars": v4.48.0 measured 25-30 B per posting (97-117 B near-unique) and defined
no budget; reopen only if a consumer reports memory pressure; then design a per-store budget that drops a sidecar and falls back to the fold.

### 48. An `Other` marker on `ColumnData` (node column scans)

*FEATURE (small)* — requested by sigma-tkgd 2026-10-10.

`store.ColumnData` (`pkg/graph/store/capabilities.go:1268`, shared by `ColumnBatch` and `RelColumnBatch`) carries only `Null [][]bool`, so a cell
holding a value outside the column's scalar kind (a list, a map, a kind mismatch) cannot be told from a missing one. Rel segments already
carry `SegmentColumnValues.Other` (`store/rel_segments.go:259`: "row k may hold the property in another Go kind or shape, read it from
Row(k)"). Request: `Other [][]bool` (parallel to `Null`, per column) on `ColumnData` with the same meaning, so a reader falls back to the
row read for exactly the flagged rows; today sigma routes a whole label to the row feed whenever `PropertyTypeClassCounts` shows
`Other > 0` for one of its properties and re-checks after the scan. Open points to settle with evidence: it lives in the shared struct, so the
rel batch gets the field too (define it for `ScanRelColumns` or leave it nil and document); every backend that fills `ColumnData`
(memory, badger, tiered, sharded, segment-backed rels) must fill it identically (differential test against `Row(k)` classification); the
`Null` meaning must not change (a flagged cell stays `Null == false`? decide and test: the safest is `Other[k] == true` implies the typed
value slot is zero and `Null[k] == false`); additive field, nil when no column has an Other cell (zero extra allocation on the common
path, benchmark rows allocs-gated). Red tests first: per backend a node with a list, a map, a kind-mismatched value and a missing value in
one column, scan and compare each cell's class with the row read; stale `Other` after an update that fixes or breaks a cell; mutants:
Other never set, Other set for a missing cell, Other set on the wrong column index.

## v5

v5 is Markus Nissl's work on branch `origin/v5`, a separate Go module (`github.com/data-insights-ai/rho-tkg/v5` in
`v5/`); the plan (`docs/v5/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md`, revised 2026-10-09 after
`tasks/review-v5-plan-vs-code-20261009.md`) lives on branch `v5`, not on main (our plan commit `b1193dc`, tag
`v5-plan-20261009`). Do not push to `v5` (HANDOVER.md §4). What the v4 changes mean for the importer:
`tasks/v4-changes-for-v5-importer-20261010.md`. v4 keeps taking consumer features (decision René 2026-10-09, the
earlier fixes-only rule is deleted). **DECIDED 2026-09-25 (René): ADR-0011 S2 and S5 shipped on v4 as v4.39.0**
(ADR-0011 §6 gate met: 24.0 / 24.8 / 25.7 B/HOP at 790 K / 3.15 M / 12.6 M); v5 carries the segments forward.

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
| Item 6 (FEATURE) — `DeleteWithTx` / `UpdateWithTx` on every write door, `ErrTxOrder` | `[4.44.0]` Added; handover `tasks/handover-tx-backfill-delete-update-20261009.md` |
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

Recover closed investigation prose via `git log --all -- tasks/backlog.md` if needed.

### DO NOT BUILD / reopen criteria (keep here so they are not re-filed)

- **Item 9 — property-keyed unique constraint on relationships (ai-soc request 4): WITHDRAWN 2026-10-09** (René,
  relayed by ai-soc: "rho-tkg already has unique snowflake ids"). ai-soc uses the snowflake ID as the global ID.
  Possible cheap follow-up, not requested: a relationship door that accepts a caller-supplied snowflake ID "if
  absent", mirroring `Nodes().AddByIDIfAbsent` (today `Rels().AddByIDIfAbsent` dedups on `(start, end, type)`).
- **DECIDED NO 2026-10-09 (René, relayed by ai-soc):** an index-provider "failure aborts the mutation" option
  (ai-soc request 5; ai-soc rebuilds from the change feed) and one instant per commit group (ai-soc request 7; the
  cut-record pin is enough). Reopen only with a consumer case the change feed or the cut-record pin cannot cover.
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
