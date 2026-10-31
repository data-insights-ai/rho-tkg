# Contributing to tkg/v4

The developer guide for humans and agents. Read the **Rules digest** every session; read the rest when you gate, test,
review or meet a "lesson N" cite. How rho is built and which invariants hold: [architecture.md](architecture.md). Which file
owns which fact: [README.md § Documentation](../README.md#documentation).

## Rules digest

**Process (René's standing rules)**

- The goal (René 2026-10-26): a realtime system at about 2 TB a day, sometimes more (about 23 MB/s sustained, 4 billion
  500-byte facts a day), grown only by more RAM, NVMe and block storage, never by a redesign. Nothing may grow in RAM per
  entity, or in open, recovery, Clear or compaction time per store, without a bound; work that removes such a wall ranks
  above a constant-factor win (todo SCALE, `tasks/evidence/SCALE/`).
- Never create a new .md file; fold new facts into the owning file (README § Documentation). One fact, one place.
- Commits, MRs and PRs carry no agent attribution: no Co-Authored-By, no Claude-Session, no "Generated with".
- A fix is a PATCH, a new door or option a MINOR. A fix never rides in a minor, a door never in a patch; one round of consumer
  requests ships as one minor or one patch. A new store format marker is a door (older releases refuse the store), and so is
  any new exported name, internal packages included: `scripts/patchgate` decides (FLUSHPART, 2026-10-24).
- Scope: consumer-blocking or wrong-answer/data bugs only, plus ai-soc runtime items (standing go). A backlog item needs a
  consumer request or a failing test. Everything else goes to `tasks/todo.md` § Waiting.
- Release authority: merge, gate, tag, push and notify sigma-tkgd + ai-soc (their agents: HANDOVER § State) without asking. `git fetch` + merge origin/main
  before a gate; never force-push, never push `origin/v5`. Follow the release checklist below.
- Commit with explicit paths, never `git commit -a`: a docs commit swept unreviewed step code onto main (2026-10-23).
- Work order (René 2026-10-23): (1) the request's `tasks/todo.md` item first, before any other action; (2) the red test;
  (3) the implementation; (4) the tests green; (5) the review; (6) when finished, the item leaves `tasks/todo.md` and its
  `docs/CHANGELOG.md` entry names the same tag. `tasks/todo.md` and the HANDOVER brief get one commit on main per item end (René 2026-10-31: 298 of 545 commits in 7 days
  were todo commits), from the main worktree `../rho-tkg-main` while a branch is checked out;
  `scripts/check-ledger.sh` before every such commit.
- Peers (René 2026-10-31): one message per item each way to sigma-tkgd and ai-soc, about 1,500 characters at most, detail
  in a file; status goes in the todo. A status line is `item | state | next | waits for`, checked against the repo before
  it is stated.
- Reopen check (René 2026-10-31): before handing over a commit that changes what sigma or ai-soc read at reopen, run a
  small reopen cycle with the host's writer, on masked rows only, never a Flux file. Sample: ai-soc's
  `TestRestartModesOverBARows` (524 rows); ask ai-soc for it. On 30 Oct the About-edge blocker appeared only after the
  freeze and needed v4.98.3.
- By hand, `go test` without `-count=1` unless a flake or a timing is the question: 9.3 h across 1,009 calls (ai-soc's
  audit, 24-31 Oct). Gates keep `-count=1`.
- Who works (René 2026-10-23): the main session does the work itself, analysis and code alike, and runs every test, lint
  and gate, and reviews its own work (§ Review); the Fable reviewer was deleted (René 2026-10-24). No subagents unless
  René asks for them.
  One trial (René 2026-10-31): Fable builds the next state-restore item, from a brief naming the host's call path; report
  how it went.
- Consumer data: ask the consumer's agent for runs and stores, never browse its repo. Flux (ai-soc's run machine) needs
  René's word per run; customer data stays on Flux: copy the store into a new dir under `/srv/fast/rene/` and open only
  the copy, report counts, sizes and relation names only (no row values, hosts, keys or record ids in chat or git), and
  never touch the other `aisoc serve` there.
- Act, do not offer: open work with a clear next step is started. Before asking René a design question, trace its cause in
  the code and try to remove it; ask only the remainder.
- Behaviour changes go through the deprecation ritual (`docs/api.md` § Stability and deprecation).
- v5 work happens in v5, never as v4 API changes; v5 is paused until René restarts it.

**Code**

- Red test first: one compact break-the-code test, red output kept under `tasks/evidence/<item>/`, then the root-cause fix at the
  shared seam. No "known limitation" unless the sound fix was shown impossible. A crash-safety change also gets a mutation
  sweep: each defended property broken once in the code must fail a test; the sweep runs every test that guards the code (the
  long tier too, not only the new tests), counts a timeout as caught, and a fault hook fires once (a requeue re-runs it).
- Validate before any side effect: IDs, decoded bytes and property values at every entry, before an ID is minted, a token
  interned, a snapshot taken or an op queued. Decode untrusted bytes only through `storeutil.SafeUnmarshal`; allocate by bytes
  delivered, never by a declared count; verify imported hashes.
- A contract over a family of doors lands in every door (named, generic, batch, tx, ingest; every backend), and a test asserts
  the doors agree.
- Time: transaction time comes from the commit clock (`c.now()` / `readNow()`), never the wall clock; corrections append rows;
  valid times compare only through `types.ValidAxis`.
- Locks: follow the lock order in architecture.md § 4; take entity locks before store writes; `sync.RWMutex` is not reentrant;
  bound every retry loop; swap a shared pointer only while holding every lock class its readers use.
- Persistence: every in-memory structure needs a rebuild path at open and fails closed on bad data; flush data before advancing
  a watermark.
- Sentinel errors are precise and tested with `errors.Is`; a caller skips only the sentinel the operation tolerates.
- Reproduce before relying: profile before optimizing; reproduce an external performance diagnosis before touching the
  component it names; check a claim about a dependency's behaviour (badger) or a reviewer's statement against the source,
  and give it a test that tries to break it before code, docs or a plan builds on it (two rounds of false badger claims in
  `docs/badger.md`, 2026-10-23/24).
- Cost is part of done (lesson 89): an item that adds or changes a door, a format or a stored structure states its cost
  budget in its todo "Done when" (bytes allocated per fact, RAM per entity, open time per million rows, durable writes
  per second, whichever it touches), measured by a rho test on the consumer's shape (row size, versions, warm and cold)
  before its first release. Design from `docs/badger.md`'s measured costs, and profile before the review, not after a
  consumer's complaint.
- Trim comments by hand, never by script; never drop lock-order, crash-ordering or ownership contracts.

**Tests**

- New tests run under `-short` in seconds (one test ≲2 s, a package ≲5 s). A slow test is gated with
  `testing.Short() && !raceEnabled` so it runs in the race gate, or with a plain `testing.Short()` when its bounds hold
  only without race: then the minor gate's long tier runs it (`make test-long`, which finds every such skip). A cap or
  growth test lowers the cap through a test switch.
- History-aware code gets two-phase tests: create state X at t0, mutate after t0, query at t0 and assert X.
- Assert exact sets and negative cases ("must NOT contain Y"), not happy paths. Ask of every test: would it fail if the code
  returned the current instead of the historical version?
- An accelerator (index, cache, sidecar, pre-encode) is tested against the slow path it replaces.
- A contract change greps every test pinning the old outcome and runs those by name with `-race` before the branch is done.

## Gates

| Command | Use |
|---|---|
| `make ci-fast` | **Patch gate** (~30 s for docs, 1–2 min for a fix): fmt, vet, build, lint, `scripts/patchgate` (refuses new exported API), `-short` of changed packages, `-race` of changed tests, docs tests, check-ledger |
| `make ci-docker` | **Minor gate**: fmt, vet, lint, build, full `test-race`, `test-long` (the tests a plain `testing.Short()` skips), gosec, govulncheck, cover-gate (80 %), metakv-reap check, `fuzz-smoke` (each trust-boundary fuzz harness for `FUZZTIME`, default 20 s), check-ledger |
| `make hooks` | Installs `scripts/hooks/pre-commit`, under 30 s: gofmt, build, vet, patchgate's tests, docs tests, check-ledger; `.md` only: docs tests and check-ledger; the git-only worktree: check-ledger, and it refuses Go files |
| `make test` / `make test-race` | short tests / race detector |
| `make cover` | coverage report (`coverage.html`) |
| `make lint-docker`, `security-docker`, `vulncheck-docker` | the pinned tools in the go.mod toolchain image; Docker is always available, so the gate is never "un-runnable" |
| `go test -run TestFoo ./pkg/...` | one test |

GitHub CI runs only when a `v*` tag is pushed and only builds, vets and runs `go test -short ./...`; a push to main without a
tag runs nothing, and nothing is scheduled. Everything else runs locally in the gate above. Fuzzing and benchmarks run on
GitHub only when started by hand (`fuzz.yml`, `bench.yml`). `#nosec` is allowed only with a reason at the call site.

### Release checklist

1. Merge every branch of the batch; `go test -short` of the touched packages.
2. Self-review of the merged batch (§ Review); fixes merged.
3. Gate: `make ci-fast` for a patch, `make ci-docker` for a minor; plus the consumer build/vet (HANDOVER Appendix B). No code
   edits while a gate runs (it tests the live work tree). A minor also runs the scale probe (a scratch module from
   `tasks/evidence/SCALE/scaleprobe_main.go.txt`, replace to the release's worktree) and appends its numbers there.
4. Release commit: CHANGELOG heading, README "Current release", line 1 of `docs/architecture.md`, HANDOVER names the tag, shipped
   rows leave `tasks/todo.md`; docs tests and `scripts/check-ledger.sh` green.
5. Push main, `git tag -a vX.Y.Z`, push the tag.
6. Notify sigma-tkgd and ai-soc: tag, commit, migration notes.
7. Watch the tag's CI run (`gh run list --workflow CI --limit 1`). A red run is fixed on main and shipped as a new patch tag,
   never by re-running the old run.

## Review

The main session reviews each branch itself before its gate: it reads the full diff and answers: does each new method do what its name promises; is
the same defect in sibling doors, the other backends, batch/tx/ingest paths; would each new test fail on a plausible bug
(current instead of historical, skipped rollback, reordered write), shown by breaking the code once per defended property.
Every claim about badger cites its source or a measurement.

## Documentation

CHANGELOG entries go under `[Unreleased]` in the same change as the code. A new public error sentinel needs a row in
[errors.md](errors.md). The docs tests (`TestDocsMetadataMatchesSourceOfTruth`, `TestErrorsDocumentation`,
`TestNoMDFileOutsideTheDocumentation`) run in `make ci-fast`.

## Fuzzing

The trust-boundary fuzz harnesses (`make fuzz-smoke` runs them briefly in the minor gate; `.github/workflows/fuzz.yml` on demand) are listed in
[SECURITY.md](SECURITY.md) ("Fuzz harnesses"). Run a deep session when you change a decode or import surface and commit any new
crasher (under the package's `testdata/fuzz/`) with the fix:

```bash
go test -fuzz=FuzzImport -fuzztime=30m -run='^$' ./pkg/graph/internal/core
```

## Benchmarks

`bench/` is the cross-backend suite (harness `bench/harness_test.go`); each scenario runs on `memory` and `badger`, some also on
tiered or sharded. Read-only scenarios build their fixture once with `for b.Loop()`; write scenarios use the classic `b.N` loop
with a fresh graph per iteration.

```bash
go test -bench=. -benchtime=1x -run '^$' ./bench   # smoke: every benchmark once
make bench                                         # local suite
make bench-baseline                                # per-machine baseline -> bench/local-baseline.txt
make bench-check                                   # fail on >15 % time or >10 % allocs regression
make bench-compare                                 # compare revisions (bench/bench-compare-revisions.sh)
```

Baselines are per machine and gitignored; never compare across machines. CI (`.github/workflows/bench.yml`) benchmarks the
merge-base and HEAD of a pull request on the same runner and fails a core scenario that regresses beyond 30 % (time) or an
allocs-gated scenario beyond 10 %; the workflow file holds the scenario filters. Measurements belong in CHANGELOG entries, not
here.

## Lessons

A lesson is a rule learned from a real defect. Its number is cited from code and docs and is never reused. A lesson is kept as
prose only when no test or lint can enforce it; otherwise the test is the rule and the index names it. Add a lesson only for a
defect class that recurred or reached a release and cannot be made a test now; prefer writing the guard.

Index (`→` names where the rule lives now):

1. Validate at every boundary → digest (Code); `TestGraphReadAPIsRejectInvalidIDs`
2. Hash canonical state → architecture.md § 4 Integrity
3. Lock before store mutation → architecture.md § 4 Locking
4. Preflight then apply → invariant 19
5. Multi-shard moves need rollback → `TestRollbackArchiveNodeStateRemovesDestinationNode`
6. Store is the trust boundary → invariant 16
7. Deleted entities still matter → invariant 1
8. Iterate without materializing everything → callback `ForEach*` doors; `TestRunShardPoolBoundsConcurrency`
9. `sync.RWMutex` is not reentrant → digest (Code)
10. Persistence must rebuild in-memory state → invariants 13, 14
11. Index creation is phased → architecture.md § 6; `TestPropertyIndexBuiltUnderConcurrentWritesAgrees`
12. Sentinel errors must be precise → digest (Code)
13. Version identity is the version number, not a slice position → retired
14. Public API coverage is direct → cover-gate; retired
15. Performance tests need production shape → digest (Code); retired
16. Lazy init flags must be synchronized → race gate; retired
17. Rollback logs track pre-transaction existence → invariant 9
18. Query results come in ID order → `TestNodesAsOfReturnsSortedByID`
19. Native fast paths respect store wrappers → `TestNativePreEncodedPut_RoutesShardedButNotWrapper`
20. Transaction time is monotonic → invariant 7; `TestTxTimeMonotonicWhenClockRepeats`
21. Batch visibility → see 29
22. Peek-then-lock must revalidate identity → `TestUpdateRelationship_RetriesWhenReusedIDHasDifferentEndpoints`
23. Property index keys use equality semantics → invariant 23; `TestPropertyEqualityDoorsAgree`
24. Bound every retry loop → digest (Code)
25. Equality and hash differ for floats → `TestPropertyEqualityDoorsAgree`
26. Sub-API forwarding matches the internal contract → architecture.md § 1
27. → 78
28. Adjacency folds need only the seed's ex-adjacency → `TestOutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe`
29. `PublishBatch` ordering needs a priority floor → `TestAsyncEventBusPublishBatch_PriorityCeiling`
30. File size is a signal, not a rule → retired
31. Tx and the write lock → architecture.md § 4 "Transaction isolation"
32. Transaction time is not valid time in the resolver → invariants 1–3
33. → 42
34. As-of reads default to now → retired (`TxAtSet`)
35. → 46
36. Migration-gated resolver shims need a runtime flag → `bitemporalMigrated`; retires at v5
37. Token registries are capacity-soft → architecture.md § 2 Tokens; `TestPropertyKeyRegistryCapacityIsSoft`
38. Side indexes hook into validation → rule: maintain a side index from the validation hook, not per door
39. Custom `EncodeMsgpack` bypasses struct tags → rule until a reflection test over every wire field exists
40. Load registries before rebuilding indexes → invariant 13; `TestPropertyKeyRegistry_PersistsAcrossReopen`
41. A version bump updates every doc target → `TestDocsMetadataMatchesSourceOfTruth`
42. Every door that copies the current version clears inherited valid time → invariant 2
43. Superseded is not retracted → invariant 3
44. A trust boundary that stores hashes verifies them → invariant 16
45. A shrunken resource bound must handle state from the old bound → `TestBadgerReadOnlyOpenOversizedWALFailsClosed`
46. A correction is a new belief → invariant 1
47. Decoding is part of the trust boundary → invariant 16
48. Allocate by bytes delivered → `TestImportOversizedRecordWithLyingBody`
49. The change-log is in-backend and co-committed → invariant 17
50. A replica reproduces the primary's rows → invariant 18
51. Append-only registry sync: capture order, prefix guard, persist before use → `TestAppendNames*`
52. Delta backups reuse the change-log → `TestImportMerge_ReproducesSupersededTxTo`
53. A reset keeps the durable watermark present → `TestChangeLog_LogOffClearPreservesWatermark`
54. A commit-window buffer is cleared on success and read by every reader → invariant 12
55. The change-log is a physical redo log → persistence.md § Change-log
56. A purge-and-recreate rollback captures every side effect → retired
57. An overlay resolves set vs delete per key at every reader → invariant 12
58. A contract over a family of doors lands in every door → digest (Code)
59. A privileged override lands at the shared seam → `TestTxBackfill*`
60. Delete is a transaction-time tombstone → invariant 1
61. → 71
62. As-of never falls through a retracted newest belief → `TestAsOfCascadeThenDeleteNode`
63. An unlocked collect window needs a write-generation guard → `TestBadgerStoreNodePropertyStatsStaleRescanOverwrite`
64. Snapshot the overlay before the Badger view → `TestBitemporalOracle_BadgerCommitWindow`
65. NDV tests need well-distributed values → `TestHyperLogLogSequentialIntegerKeysAccuracy`; retired
66. Swap a pointer under every lock class → digest (Code)
67. A raw-byte saving is not a disk saving → retired
68. Reconstructed wire properties sort by key string → `TestHistoryDeltaCoreDifferentialAndIntegrity`
69. No overlap prune for non-overlapping Allen relations → `TestNodesRelating_AllThirteenRelations`
70. No `reflect` sorting or comparison on hot paths → rule until a lint exists
71. The monotonic floor is reopen-safe → invariant 7
72. Reproduce before relying (a diagnosis, a dependency claim, a reviewer's statement) → digest (Code); `tasks/evidence/BADGER/`
73. The resolver's input order is semantic → `TestSkeletonResolve_RandomizedEquivalence`
74. → 64
75. A green workflow means its gates ran → CI configuration; retired
76. A type tag covers one level; test every kind at depth → `TestWire_NestedKindsRoundTripExact`
77. → 78
78. Red break-the-code test first; mutation sweep for crash safety → digest (Code); `tasks/evidence/FLUSHPART/mutations.txt`
79. Fix = patch, door = minor → digest (Process); `scripts/patchgate`
80. One todo and HANDOVER commit on main per item end → digest (Process); `scripts/check-ledger.sh`
81. Act, do not offer → digest (Process)
82. A release is green only when the tag's GitHub CI is green → Release checklist
83. → 78
84. Trace the cause, then consolidate → digest (Process)
85. Comment trimming is editing → digest (Code)
86. v5 convergence happens in v5 → digest (Process)
87. Fast tests per commit → digest (Tests)
88. → 79
89. Cost is part of done: doors shipped correct and were made cheap only after a consumer measured them (R37, 26 Oct:
    24.7x a fact's values, cut in a second release) → digest (Code); `TestR37ListingBytesPerFact`
