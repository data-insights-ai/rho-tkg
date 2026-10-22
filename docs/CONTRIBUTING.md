# Contributing to tkg/v4

The one developer guide for humans and agents (Codex, Claude Code, others): build and gates, testing rules, review protocol,
benchmarks, and the numbered lessons ("lesson N" in code and docs = `### N.` below). Architecture, store contracts, temporal
semantics and lock order live in [architecture.md](architecture.md). tkg/v4 is a pure Go library (no binary, no server, no query
language); external orchestration (START→END stub-delete fan-out, consumer-gated constraint dry-run) is out of scope, rho-tkg
exposes the local primitives.

## Session protocol

Read order: [AGENTS.md](../AGENTS.md). HANDOVER.md holds the state, [tasks/todo.md](../tasks/todo.md) the next steps and open
decisions; the newest user message wins. Standing rules: the [Rules digest](#rules-digest).

- Before planning: read the full API of every file the change touches, not snippets near the suspected bug; ask "does this
  algorithm deliver what the method name promises?" and "which existing feature could this break?".
- After implementation: update only docs whose user-facing behaviour, API contract or architecture changed; tighten the
  owning text, no long-form notes.

## Build, test and gates

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector — required for any change touching concurrency
make test-integration  # integration tests (long-running)
make cover          # coverage report -> coverage.html; then go tool cover -func=coverage.out
make check          # pre-commit: vet + build + test
make ci             # fmt-check + vet + lint + build + test-race + security + vulncheck + cover-gate + check-metakv-reap
make fmt            # format code
go test -run TestFoo ./pkg/types/   # one test
```

`golangci-lint`, `gosec` and `govulncheck` are usually absent from the host. **Do not report the gate as un-runnable — Docker is
always available.** Run them in the go.mod-matching toolchain image (`GO_IMAGE` tracks the `go` line of `go.mod`), with cached
named volumes:

```bash
make lint-docker        # golangci-lint v2 (reads .golangci.yml)
make security-docker    # gosec
make vulncheck-docker   # govulncheck
make ci-docker          # full gate: fmt-check + vet + lint-docker + build + test-race + security-docker + vulncheck-docker + cover-gate + check-metakv-reap
# raw form, ad hoc:
docker run --rm -v "$PWD":/src -w /src -v rho-tkg-gocache:/go -v rho-tkg-buildcache:/root/.cache/go-build \
  golang:$(awk '/^go /{print $2}' go.mod) \
  sh -c 'go install golang.org/x/vuln/cmd/govulncheck@v1.7.0 && govulncheck ./...'   # the Makefile's GOVULNCHECK_VERSION
```

The tools are version-pinned by the Makefile. `gosec` and `govulncheck` are clean, blocking gates: `#nosec` annotations document
audited conversions and deterministic/randomized algorithms at the exact call site; new unannotated findings fail CI.
golangci-lint uses its pull-request new-issues mode.

**Race gate in GitHub CI (race budget).** Locally `make test-race` is one unsplit `go test -short -race ./...`. CI (4-vCPU runners)
runs it as a matrix through `scripts/test-race.sh`: `race-plan` lists the parts (`RACE_SHARD=list bash scripts/test-race.sh 4`),
one `race (<part>)` job per part runs `ROOT_RACE_CORE_SHARDS=4 RACE_SHARD=<part> make test-race`. Parts: `other` (every package
not listed next), `graph` (pkg/graph), `memory` (store/memory), `core-1..4` (internal/core, top-level tests round-robin by name).
Each part has Go's default 10 min timeout; keep each under 5 min at GOMAXPROCS=4 (`tasks/evidence/ci-race/`). A new randomized
oracle sizes its `-short` + race run for that budget (fewer seeds, same cases, full size behind `!testing.Short()`), and its
generator-coverage check must still pass at that size. Script tests: `make test-race-script`. One part:
`RACE_SHARD=core-2 ROOT_RACE_CORE_SHARDS=4 make test-race`.

**Stdlib aliasing.** `pkg/graph/hash` and `pkg/graph/io` shadow stdlib `hash` and `io`. Inside the package no alias is needed; at
a site importing both, alias the LOCAL one (`tkghash` / `tkgio`) and leave stdlib unaliased.

## Testing rules (hard requirements)

Every rule was violated at least once.

1. **Every public method gets a direct test.** Indirect coverage via delegation does NOT count. A public method at 0% is a blocker.
2. **Node and Relationship must have test parity.** They are structural mirrors.
3. **Every type-switch branch gets its own test.** A branch at 0% in `cover` needs one.
4. **Sentinel errors are tested with `errors.Is`, not just `err != nil`**, at every call layer.
5. **Fallback/reflect paths must be tested or removed.**
6. **Deep copy means deep copy.** Clone all nested reference types.
7. **Run `make cover` before marking any step complete.** A public method at 0% or new code below 80% is a blocker.
8. **Validation must be recursive/adversarial.** `[]any{&myStruct{}}` must be rejected.
9. **Test nil values in reflect-based code.** `reflect.ValueOf(nil)` returns a zero `reflect.Value`; `SetMapIndex` with it deletes the key.
9a. **Pointer-taking public helpers must test nil inputs.** If the method returns `error`, nil becomes a sentinel error, not a panic.
10. **One-time warnings use `sync.Once`.** Never `>=` for one-shot warnings.
11. **No empty stubs when the spec defines the fields.**
12. **Public return types must not leak dependencies.** `type nodeID snowflake.ID`, NOT `type nodeID int64`.
13. **Config fields must be used or removed.**
14. **No millisecond or sub-millisecond `ShardWindow` in tests.** Windows are truncated to millisecond precision, so sub-second
    windows create boundary gaps. Use the 1-week window (`newTestTieredStore`) and test cold/warm via manual rotation + `demoteToCold`.
15. **History-aware code needs two-phase tests.** Any method answering about the past, another version or a comparative state
    (`*ValidAt`, `*ValidDuring`, `*AsOf`, `Verify*Chain`, `Snapshot`, `Diff`, `*At`, any `*ByLabel`/`*ByType` with temporal
    `QueryOpts`) needs a test that (1) creates state X at t0, (2) mutates it after t0, (3) queries at t0 and asserts X. Single-mutation
    tests prove the API exists; only mutation-then-query proves it remembers (lessons 33, 42).
16. **Adversarial shape, not happy path.** Multi-entity scenarios with diverging lifecycles, exact-set assertions
    (`assertNodeSet`/`assertRelSet`), negative assertions ("must NOT contain Y", "phantom value returns empty"). For interval queries
    the "predicate held during part of the interval but not on the most-recent version" case MUST be asserted.
17. **Fixing a temporal named method, audit the generic equivalent.** A fix in `Get*ValidAt(...)` without the matching fix in
    `*By*(opts QueryOpts)` leaves the bug behind another door. Grep `g\.store\.\w+ByLabel\|g\.store\.\w+ByType` in every touched file.

Plus: fix in one Store implementation → check the others (`memory`, `badger`, `tiered`; `sharded` when slot routing is touched).
A fix that lands in one backend is incomplete.

## MR review protocol

Run the three phases in order.

**Phase 1 — correctness of the MR itself.**
1. Read the full diff: `git diff origin/main...origin/<branch>`.
2. Verify each new method/test delivers what its name promises; check edge cases the author assumed away.
3. Lessons and CHANGELOG hygiene: a new lesson uses the next number (`grep '^### [0-9]' docs/CONTRIBUTING.md | tail -1`) and
   duplicates no title or code pattern; the CHANGELOG section (`[Unreleased]` or the version) sits above the latest release.
4. Test quality per the testing rules: two-phase temporal tests (15), adversarial exact-set scenarios (16), negative and
   phantom-value assertions, the predicate-anywhere interval case, `errors.Is` at every layer (4).
5. `make test-race` on the branch. A suite that fails for one backend is not mergeable.

**Phase 2 — is the issue also present elsewhere?**
1. Grep the same pattern across the codebase (audit checklists below, lesson 58).
2. Symmetric types: Node/Relationship; `Get*ValidAt` (named) vs `*By*(opts QueryOpts)` (generic) — rule 17.
3. All Store implementations, both directions.
4. Batch paths: a standalone mutation fix also lands in `BatchBuilder` paths.

**Phase 3 — are the tests sufficient?**
1. `make cover`: every new public method covered, no new code below 80%.
2. Missing scenarios: cross-shard relationships (tiered), deleted entities (history queryable after deletion, B32), concurrent
   access (a `test-race` run), cold→warm transitions (`demoteToCold`, not a sub-second `ShardWindow`).
3. For each test: "if the implementation silently returned current instead of historical state, would the assertions fail?" If
   not, it is happy-path regardless of coverage.
4. State whether the MR is mergeable; list blocking and non-blocking issues separately.

## Code review rules and audit checklists

- **Use library APIs, never reimplement internals** (e.g. `snowflake.Node.Decompose()`): hardcoded bit shifts break silently
  when the library changes its layout.
- **Every fix needs a grep audit**; a contract over a family of doors lands in every door (58).
- **Fix descriptions include exact signatures**: "use lazy iterators" without the callback shape took 5 rounds (C4).
- **Review by feature, not by file**: the `batch.go` hash bug was invisible from `integrity.go`.
- **Repair tools complement but do not replace correctness** (5).
- **Most-recent-overlap is wrong for predicate-during-interval**: use `findNodeVersionMatchingDuring(id, start, end, pred)`,
  which scans every overlapping version.
- **API design**: config fields used or removed; opaque wrappers wrap the real type; Graph is the sole external API; doc comments
  match behavior.

Run after any change to the subsystem:

- **Hash computation**: `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/` — every call passes canonical (registry-resolved) labels/type.
- **Entity locks**: `grep -rn 'store\.Put\|store\.Delete\|store\.Replace' pkg/graph/` — every Store write is preceded by
  `LockEntity`/`LockTwo`/`LockThree`/`LockMany`.
- **Index cleanup**: every delete path handling missing entities purges label, property AND adjacency indexes (brute force is
  fine on corruption paths).
- **Index creation visibility**: `CreatePropertyIndex` installs the empty placeholder BEFORE the I/O phase; phase 3 checks
  `mutated[id]`, not `contains(id)`.
- **Untrusted msgpack**: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test` — every hit goes through `storeutil.SafeUnmarshal`.

## Documentation hygiene

- Which file owns which fact: [README.md § Documentation](../README.md#documentation).
- Update [CHANGELOG.md](CHANGELOG.md) and any affected owner file in the same PR as the code, not as a follow-up.
- CHANGELOG entries go under `[Unreleased]` (create it above the latest release heading) unless told which version.
  **Never bump a version number yourself** — that is the release step (82).
- Keep the docs-consistency test green: `pkg/graph/internal/core/docs_consistency_test.go` checks the Go version (`go.mod`) and the
  current release (CHANGELOG's topmost `## [x.y.z]`) against fixed strings in README and `docs/architecture.md`; run
  `go test -run TestDocsMetadataMatchesSourceOfTruth ./pkg/graph/internal/core/` when touching them. A new public error sentinel
  needs a row in [errors.md](errors.md) (`TestErrorsDocumentation`, `TestGraphErrorsFileInventoryComplete` in
  `pkg/graph/errors_doc_test.go`).

## Pull request checklist

- [ ] `make check` passes
- [ ] `make test-race` passes for every touched package
- [ ] `make cover`: no new code below 80%, no new public method at 0%
- [ ] CHANGELOG entry under `[Unreleased]` (or the version you were told)
- [ ] Affected docs updated in the same PR
- [ ] A new lesson only for a real, reusable-pattern defect
- [ ] The docs-consistency test passes

## Fuzzing

The trust-boundary fuzz harnesses, which of them `.github/workflows/fuzz.yml` runs weekly, and how their seed corpora run in
`make test` are listed in [SECURITY.md](SECURITY.md) ("Fuzz harnesses"). Run a deep session when you change a decode/import
surface and commit any new crasher (under the package's `testdata/fuzz/`) with the fix:

```bash
go test -fuzz=FuzzImport -fuzztime=30m -run='^$' ./pkg/graph/internal/core
```

## Benchmarks

`bench/` is the cross-backend suite (shared harness `bench/harness_test.go`). Each scenario has a `memory` and a `badger`
(`BadgerInMemory`) sub-benchmark, except `ANNSearch10k` (memory only: the vector index is store-level in RAM on every backend),
`PinnedScanScaling` and `ChangeLogTxSerialization` (badger only); `PinnedRelPropertyLookup` adds `sharded` when
`RHO_TKG_PINNED_REL_SIZES` is set. Read-only scenarios build their fixture once per (scenario, backend) with `for b.Loop()`.
Write scenarios (`Ingest*`, `BulkAddNodes10k`, the ingest-pipeline family, `ChangeLogTxSerialization`) use the classic `b.N` loop
with a fresh graph per iteration behind `b.StopTimer()`/`b.StartTimer()`: a graph reused across `b.Loop()` iterations grew with
the iteration count, and `b.StopTimer()` inside `b.Loop()` hard-fails the benchmark (`ingest_test.go` package comment).

| Benchmark | What it measures |
|---|---|
| `PointReadHit` | Warm cache-hit `g.Nodes().Get` over 2k nodes |
| `LabelScan10k` | `g.Nodes().ByLabel` over 10k nodes; `sorted` and `nosort` (`QueryOpts.NoSort`) |
| `TwoHop` | Decode-free two-hop via `g.Rels().ForEachAdjacentEndpoint`, 10k nodes / 30k rels |
| `TemporalPoint` | `g.Temporal().NodesAt` over 5-version `tkg_valid_from` chains, pinned mid-chain |
| `AsOfPin` | `g.Temporal().NodesAsOf` pinned mid-way through 5 update rounds via `NowTx()` |
| `RelHistoryPlain` / `RelHasHistory` | `g.Rels().History` on a rel without history (10k rels, 1 % with history) vs `HasHistory` (`miss`/`hit`; badger's RAM set built by a warm-up call) |
| `RelEffectiveTimeline` | `RelEffectiveTimeline` at a pin over 200 K rels (`BENCH_EFFECTIVE_RELS`), 1 % with a bounded correction; `plain`, `plain-hot`, `cascaded` |
| `NodeAtTxLongChain` / `NodeEffectiveTimelineLongChain` | one node with 300 / 1000 / 3000 updates and one bounded correction: `NodeAtTx` at the correction and the whole timeline |
| `RelEffectiveLoop` | the consumer loop the timeline replaces: `Get` + `History` + `RelAtTx` at every row bound |
| `ForEachRelEffectiveByType` / `EffectiveScans` | effective scans (`all`, `at-nodes`, `by-ids`); `ns/rel` is a custom metric, not time-gated |
| `EffectiveAtNodesOneSeed` | one seed (~50 rels) on all-history rels, a quarter deleted: the gather stays O(the seed's ever-degree) (backlog 50) |
| `OrderedTopK` | `ForEachByLabelPropertyRangeOrdered` top-10 vs collect-then-limit over 100 k values ([query-planners.md](query-planners.md)) |
| `CompositeLookupVsSingleIndexPlusFilter` | composite index vs single-key index + post-filter on 100 k nodes |
| `RelPropertyLookup10k` | `ByTypeAndProperty` with a rel property index vs type scan + filter |
| `Ingest1kSingle` / `Ingest10kBatch` / `BulkAddNodes10k` | 1,000 single `Add`; 10,000 via `BatchBuilder.AddNode` + `Execute`; 10,000 via `AddNodes` |
| `ANNSearch10k` | `SearchNearest` k=10, 10k x 128 dims, `hnsw` vs `bruteforce` (M4 Max: ~193 µs vs ~816 µs/op) |
| `PinnedScanScaling` | study: `ByLabel` plain vs `TxPin`/`TxAt` vs `NodesAsOf`, {10k,100k} x churn x selectivity |
| `PinnedRelPropertyLookup` | backlog 8: `ByTypeAndProperty` with `TxPin` (200 matches) vs no pin across churn profiles; `build-ms` reported, never gated; 20 000 rels by default, `RHO_TKG_PINNED_REL_SIZES=100000,1000000` for the matrix |
| `LatestStamps` | backlog 30/42: `LatestStamps` with 0 / 100 / 1,000 versions; `/rotating-cache-miss` on badger, tiered, sharded (7 / 19 / 7 allocs/op; a whole-row decode would be 24 / 36 / 32) |
| `IngestPipeline`, `IngestConcurrent`, `IngestPipelineChangeLog`, `IngestConcurrentChangeLog`, `IngestSingleDurable`, `IngestPipelineDurable` | ingest sessions: 10 k creates at 1 and 8 producers, concurrent mode, change-log on/off, the durable pair with `SyncWrites` (1,000 creates) |
| `ChangeLogTxSerialization` | study: ops/sec for standalone `Add` vs tx-per-batch at {1,4,16} goroutines, `ChangeLog` on/off |

```bash
go test -bench=. -benchtime=1x -run '^$' ./bench   # smoke: every benchmark once (no CI job runs this)
make bench           # local suite, 0.3s per sub-benchmark, count 1
make bench-baseline  # per-machine baseline -> bench/local-baseline.txt
make bench-check     # fail on >15% time regression (REGRESSION_THRESHOLD_PCT) or allocs-gated rows >10% (ALLOCS_THRESHOLD_PCT)
make bench-graph-baseline / bench-graph-production-small / bench-graph-production-large   # graph API suites (BENCH_COUNT / PROD_BENCH_COUNT > 1)
make bench-compare   # diagnostic: bench/bench-compare-revisions.sh over refs (default HEAD, v4.27.0, v4.26.0)
```

Baselines and `bench-check` scratch files (`bench/local-*`; `BENCH_CURRENT`, `BENCH_REPORT_CSV` override paths) are per-machine
and gitignored; never compare across machines. After a deliberate performance change re-run `make bench-baseline`.
At `-count=1` benchstat's delta column always prints "~", so `bench/bench-compare.sh` (used by `bench-check.sh` and CI) reads
`benchstat -format csv` and computes the percentage itself; sub-microsecond scenarios (`TwoHop`, `PointReadHit`) are the noisiest,
so a failure is a signal to re-run, not proof.

**CI `.github/workflows/bench.yml`.** `bench-gate` (pull_request, blocking, on `pkg/**`, `bench/**`, `go.mod` or the workflow)
benchmarks merge-base and HEAD on the same runner, `-benchtime=0.3s`, and fails when a core scenario's median regresses beyond
`REGRESSION_THRESHOLD_PCT=30`. `BENCH_FILTER` (`-count=3`): `PointReadHit`, `LabelScan10k`, `TwoHop`, `TemporalPoint`, `AsOfPin`,
the `Ingest` prefix, `BulkAddNodes10k`, `ANNSearch10k`, `OrderedTopK`, the `CompositeLookup` prefix, `RelPropertyLookup10k`,
`RelHistoryPlain`, `RelHasHistory`, `RelEffectiveTimeline`, `RelEffectiveLoop`, `ForEachRelEffectiveByType`,
`NodeAtTxLongChain`, `NodeEffectiveTimelineLongChain`. `PINNED_REL_FILTER` (`-count=5`): `PinnedRelPropertyLookup`,
`LatestStamps` — the `ALLOCS_GATE_FAMILY`, allocs-gated at `ALLOCS_THRESHOLD_PCT=10` (a 0 allocs/op baseline fails on the first
allocation); their time rows are reported only (identical code swung +42..+178 % in time while allocs did not move; the regression
it catches, a fallback to the history fold, is 1,423 → 106,555 allocs/op). `TIME_CANARY=canary` time-gates 8 of those rows again
(any other value is a regex); `ALLOCS_GATE_FAMILY=none` time-gates every row. The studies are excluded. Gate rules:
`bench/bench-gate.awk` (tests `bench_gate_test.go`, fixtures `testdata/gate/`), only the `sec/op` block is time. `bench-compare`
(workflow_dispatch) is the informational full-suite run. Local `make bench-baseline` + `make bench-check` on stable hardware stays
the high-resolution tool.

**Write-path micro-benchmarks** (`pkg/graph/internal/core`, `memory.New()`; 2026-10-22, v4.70.0, Ryzen 9 7950X3D, Go 1.26.9,
shared host, `-benchtime=0.3s`; ns/op varies ~2x on this host, allocs and bytes carry over):

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `AddNode` (1 label, 2 props) | 1,800-2,020 | ~1,100 | 12 |
| `AddRelationship` (`KNOWS`, 1 prop) | 2,150-2,390 | ~1,170 | 11 |
| `MemoryStorePutNode` | 787 | 277 | 4 |
| `NodeDeepCopy` | 100 | 368 | 4 |
| `ComputeNodeHash` | 151 | 64 | 1 |
| `NewPropertySlice` | 98 | 64 | 1 |
| `SnowflakeIDGen` | 24 | 0 | 0 |

```bash
go test -run '^$' -bench 'Benchmark(AddNode|AddRelationship|SnowflakeIDGen|NewPropertySlice|ComputeNodeHash|NodeDeepCopy|MemoryStorePutNode)$' \
  -benchmem -benchtime=0.3s ./pkg/graph/internal/core
```

`Nodes().Add` runs `addNodeInternal` (`node_add.go`): validate, property slice + label tokens, snowflake ID,
`ComputeNodeHashChecked` (SHA-256 over one pooled buffer), metadata, unique stripes, `Store.PutNode`. `Rels().Add` runs
`createRelationshipLocked` and the create kernel: `LockTwo` on the endpoints, endpoint checks and hashes before the ID is minted,
the type token after every endpoint failure path, `Store.PutRelationship`. Both puts also keep a compact frozen copy with its
ordinal, the K1 membership record, planner stats (off with `DisablePlannerStats`) and the change-log record where enabled.

**ADR-0011 S0 baseline** (a measurement test): `bench/segment_baseline_test.go` writes a synthday-shaped workload
(`internal/synthhop`) through `g.Rels().AddWithTx` into memory, badger and badger lean and reports resident B/rel, objects/rel,
on-disk B/rel, HOP scan rows/s and write time. Skipped unless `RHO_TKG_SEGMENT_BASELINE` is set (a tiny smoke always runs);
options `RHO_TKG_SEGMENT_BASELINE_{REPEAT,MODES,WORKLOADS,DIR}`; 12.6 M badger runs take ~1 min and ~4 GB heap each; numbers in
CHANGELOG and ADR-0011 §6.

```bash
RHO_TKG_SEGMENT_BASELINE=790k,3.15M,12.6M RHO_TKG_SEGMENT_BASELINE_REPEAT=2 \
  go test -run 'TestSegmentBaseline$' -v -count=1 -timeout 0 ./bench
```

## Lessons

Rules distilled from real bugs: Rule, Trigger, Detector; long evidence lives in git history and CHANGELOG. Add an entry only for
a new recurring pattern, else edit the owner (next number: `grep '^### [0-9]' docs/CONTRIBUTING.md | tail -1`; check no title or
code pattern duplicates it; no lesson for a one-off typo). Numbers are cited from code, docs and the backlog ("lesson N"): never
renumber or reuse. "merged into N" / "superseded by N" stubs keep a number alive; the newest rule wins.

### Rules digest

One current rule per bullet; the number owns the detail.

- Never create a new .md file: the documentation set and which file owns what is the table in README.md § Documentation (84).
- Commits, MRs and PRs carry no agent attribution (no Co-Authored-By, no Claude-Session, no "Generated with"); tell every agent that commits (82).
- Versioning: a fix is a PATCH, a new door/option a MINOR; a fix never rides in a minor, a door never in a patch; doors from
  one round of consumer requests batch into one minor, fixes into one patch (79).
- Scope: just essential patches (consumer-blocking or wrong-answer/data bugs) EXCEPT ai-soc runtime items, which have René's
  standing go; a go René types into a consumer session and that session relays verbatim counts (79, 80). Critical path
  first: build now only what blocks ai-soc on Flux or sigma's O1-3 (sigma's resident-state work, under the ai-soc go); everything
  else goes to tasks/todo.md § Waiting (79).
- Models: builds Opus, a build Opus cannot finish goes to Fable; Fable reviews (branch and final batch review before the tag)
  and writes code only, running no tests; the main session runs every deciding test, gate, lint and security check; doc writing
  Sonnet (row 36), a docs-against-code audit Opus (row 109); build briefs end with `make lint-docker security-docker` (79).
- Tests: new tests are `-short` and take seconds (one new test under ~2 s, a package under ~5 s); growth/cap tests lower the cap
  through a test switch and pin the real constant once; slow tests sit behind `testing.Short()` and run only in the release gate
  `make ci-docker`, which includes the full `make test-race` (87, 66). A contract change greps every test pinning the old outcome
  and runs those by name with `-race` before the branch is reported done (87).
- Release: gate + tag + push, then notify consumers at once; GitHub CI runs in the background; a red tag CI is fixed on main and
  shipped as a NEW patch tag, never by rerunning the old tag's run (82); a green workflow means its gates ran (75). The local
  gate tests the live work tree: no code edit or merge while it runs; a push to main cancels the running main CI (82).
- Ledger: every request gets its `tasks/todo.md` line before the first action; todo.md (open work only) and HANDOVER (state)
  updated in place at every event; `scripts/check-ledger.sh` after every merge and before every release commit (80); doc targets
  at every version bump (41).
- One owner per fact, consolidate in place, trace the cause before asking (84). Act, do not offer (81).
- Red break-the-code test first (one compact test, red output kept under `tasks/evidence/<item>/`), clean root-cause fix at the
  shared seam, no "known limitation" instead of work; a limit is documented only after the sound fix was shown impossible (78).
- Behaviour changes go through the deprecation ritual (`docs/api.md` § Stability and deprecation), no new named exceptions (79).
- A contract over a family of doors lands in every door; test cross-door agreement (58).
- Trust boundary: validate IDs and decoded bytes at every entry, `SafeUnmarshal`, allocate by bytes delivered, verify imported
  hashes (1, 6, 44, 47, 48).
- Time: `c.now()` never wall clock, reads `readNow()`, floor seeded on open (20, 71); corrections append (46); `TxTo` never bounds
  valid time (43); newest belief by version (62); resolver input order is semantic (73).
- Locks: entity locks before Store writes, no reentrant RWMutex, bounded retries, swap pointers under every lock class (3, 9, 22,
  24, 66); overlays: capture before the snapshot, set-vs-delete per key (54, 57, 64).
- Persistence: rebuild at open and fail closed; change-log in-backend; flush work before the watermark (10, 40, 49-53).
- v5 is the new public API: converge on the v5 side, never change v4's API for it (86).
- Test coverage: every public method, Node/Rel parity, production-shape perf, every codec kind at depth, scattered NDV inputs,
  oracles for accelerators (14, 15, 65, 68, 73, 76).

### 1. Validate At Every Boundary

Rule: zero and negative IDs are malformed (except where an API defines `0` as cursor / unset / "all types"); every
store mutation, direct and history read, graph mutation target and repair helper calls `store.ValidateNodeID` before
lookup, scan or no-op. BAD: `if id == 0 {...}; lookup(id)` (negatives fall through).
Trigger: a store/graph entry point looks up, scans or no-ops on an entity ID.

### 2. Hash Canonical State

Rule: hash the canonical state after token normalization and registry resolution (`resolvedCanonicalLabels`, never raw
labels); fixing one call site means grepping all. A relationship mutation that recomputes integrity refreshes
`FromNodeHash` / `ToNodeHash` from locked current endpoints first.
Trigger: any `ComputeNodeHash` / `ComputeRelHash` call site. Detector: `rg -n 'ComputeNodeHash|ComputeRelHash' pkg/`.

### 3. Lock Before Store Mutation

Rule: take the entity locks (`g.entityLocks.LockEntity(id)` + deferred unlock) before the Store write; transactions and
batches call internal lock-free helpers only while holding the tx/batch graph lock.
Trigger: a graph mutation that writes to the Store. Detector: `rg -n 'store\.(Put|Delete|Replace)' pkg/graph/`.

### 4. Preflight Then Apply

Rule: separate validation/read gathering (no writes) from the apply phase; if an apply step can still fail, add
rollback or return per-operation errors. BAD: delete-in-loop (partial mutation on a mid-loop error).
Trigger: a multi-row Store operation.

### 5. Multi-Shard Moves Need Rollback

Rule: a later failing step (e.g. `src.DeleteNode`) rolls back the destination writes: no duplicates, no orphaned split
indexes; repair tools are not a substitute for transactional cleanup.
Trigger: Tiered archive/restore, migration, cross-shard relationship moves.

### 6. Store Is The Trust Boundary

Rule: deep-copy entities on put/get and validate payload invariants; treat MsgPack as untrusted (reject key/value ID
mismatches, invalid tokens, temporal ranges, malformed properties, corrupt counters/metadata before building live
entities). Defensive-copy fallbacks must not panic on legacy shapes (typed zero values for nil reflect elements).
Trigger: Store put/get paths; Badger wire decode.

### 7. Deleted Entities Still Matter

Rule: temporal, tx-time, diff, snapshot and integrity queries merge current and history rows
(`mergeCurrentAndHistoryIDs()`, not `AllNodeIDs()` alone); verification tolerates current-row deletion when history exists.
Trigger: any query that must see entities deleted before the pin.

### 8. Iterate Without Materializing Everything

Rule: callback iterators (`ForEachNodeID`) or paged scans, not `AllNodeIDs` + `mergeIDSlices`; callbacks run outside backend locks and
Tiered shard checkouts. Tiered scans that dereference shard stores pin every handle `Close` can close (`checkoutStore`,
`checkoutArchive`), check out one shard batch at a time, and fan out with bounded pools.

### 9. sync.RWMutex Is Not Reentrant

Rule: never call exported graph methods from code already holding `g.mu` / `c.mu` (BAD `g.mu.RLock(); g.Nodes().Get(...)`);
use unexported `*Locked` / `*Unlocked` helpers. Still binding in rollback, batch execute, snapshot, export (see 31, 63).
Trigger: graph internals holding a lock.

### 10. Persistence Must Rebuild In-Memory State

Rule: anything affecting query correctness that outlives a process has a durable definition and is rebuilt at startup
(`CreateIndex` persists, `loadIndexes` rebuilds). Counters and registry metadata fail closed when corrupt; persistent
backends mark close-in-progress before stopping flush workers so public ops fail closed once shutdown begins.
Trigger: a new index, counter or registry.

### 11. Index Creation Is A Three-Phase Operation

Rule: install an empty placeholder first (concurrent writes see it before build I/O), build from a snapshot, install only
if not mutated, using dirty/mutated tracking, never "does the built index already contain this ID?".
Trigger: building an index while writes run.

### 12. Sentinel Errors Must Be Precise

Rule: skip only the sentinel the operation tolerates (`errors.Is(err, ErrRelNotFound)`); I/O, corruption, lifecycle and
invalid-input errors surface. Trigger: `if err != nil { continue }`.

### 13. Version Identity Is Not Slice Position

Rule: history can be truncated; use `entry.Version()` and entity IDs as identity, never array positions.
Trigger: `if i == 0` over history.

### 14. Public API Coverage Is Direct

Rule: every public method has a direct test (delegation coverage is not enough); Node/Relationship mirror types get
parity tests including nil, sentinel and malformed input ("Testing rules" 1-2 above).

### 15. Performance Tests Need Production Shape

Rule: tiny fixtures hide O(N); keep microbenchmarks plus production shapes (baseline graph ops, high-fanout
relationships, history-heavy temporal queries, export/import, batch writes, Tiered hot/warm routing).

### 16. Lazy Init Flags Must Be Synchronized

Rule: the separate initialized marker is atomic (`initialized.Load()`) or guarded by an exclusive lock; `sync.Once`
serializes the init function, not unrelated reads of a plain flag.
Trigger: zero-value lazy init that can run under a shared read lock.

### 17. Rollback Logs Track Pre-Transaction Existence

Rule: `createdIDs` holds only IDs absent at `BeginTx`; rollback restores the pre-tx row and never deletes it as
"created". Key create/delete rollback logs by pre-tx existence, not by the operation that produced the current row.
Trigger: caller-specified imports reuse an ID deleted earlier in the same tx.

### 18. Map-Backed Query Results Need Explicit Ordering

Rule: any public query built from maps/dedup sets sorts by entity ID before returning (`sortedKeys(seen)`); never expose
Go map iteration order.

### 19. Native Fast Paths Must Respect Store Wrappers

Rule: tests/consumers embed in-tree stores to inject failures or stale reads; a native shortcut must not bypass those
overrides. The generic Store contract path is the default; exact-store shortcuts only for exact in-tree stores
(`isExactNativeStore`), or prove wrappers cannot override the optimized operation. Trigger: `store.(fastPath)` assertions.

### 20. Mutation Transaction Time Must Be Monotonic

Rule: stamp `TxFrom`, `TxTo`, `UpdatedAt`, `DeletedAt` and mutation events with `c.now()` (per-Core monotonic ms), never
`time.Now().UnixMilli()`; wall-clock ms repeat or go backwards and collapse tx-time version intervals (see 71).

### 21. Batch Visibility Must Match Consumer Scheduling

Rule: a producer-side batch mutex does not make the batch atomic for consumers that observe queues directly; publish in
the order that keeps the consumer contract under partial observation (critical first), or gate consumers on the same boundary.

### 22. Peek-Then-Lock Must Revalidate Identity

Rule: after locking the IDs found by an unlocked peek, re-read and revalidate the row identity (imports can reuse
deleted IDs with a different shape); retry if it changed, within the bound of 24.

### 23. Property Index Keys Are Equality Semantics

Rule: property-index value keys are not display strings; they preserve the property equality contract incl. special
floats (exact bit keys where `strconv.FormatFloat(v,'g',-1,64)` collapses NaN payloads), so fallback scans and indexed
lookups return the same set.

### 24. Bound Every Retry Loop

Rule: every retry loop has a max-iteration ceiling (`for range maxRetries`, error "changed after N retries") so a hostile
workload cannot deadlock the caller; mirror the ceiling across symmetric methods (`deleteNodeInternal` and
`lockRelationshipCurrentEndpoints` both use 10). Trigger: peek-then-lock retries.

### 25. Equality and Hash Have Different Contracts (Float Bit-Pattern Case)

Rule: `PropertyValueEqual` is the CAS short-circuit (NaN == NaN, +0 == -0); the integrity hash is the durable identity and
preserves IEEE-754 bit patterns. The asymmetry is intentional and tested: "equal CAS" does not imply "equal hash";
callers exchanging data with NaN-canonicalizing systems canonicalize at the boundary.

### 26. Public Sub-API Forwarding Must Match Internal Contract Width

Rule: each sub-API package declares its own minimal `Ops` interface and `*core.Core` satisfies each implicitly; when
collapsing or splitting sub-APIs, shrink each `Ops` to the new shape (old methods may stay on the internal type).

### 27. (merged into 78)

### 28. Adjacency Folds Need Only The Seed's Ex-Adjacency

Rule: split a shared helper before optimizing one caller. `forEach*CandidateID` stays all-history (an entity whose CURRENT
label is Y but was X at t must be visited); `forEachRelAdjacencyCandidateID` folds only each seed's ex-adjacency (rels that
named the seed in a row its live adjacency no longer holds: deleted rels, earlier lives re-imported with other endpoints;
backlog 50), kept by the store at every delete and history-row write. Collapsing all folds to deleted-only broke label queries.

### 29. PublishBatch "Atomic" Ordering Survives Saturation Only With A Floor

Rule: `AsyncEventBus.PublishBatch` enforces a per-batch priority ceiling in the dispatcher (`batchPriorityCeiling` atomic
Int32, raised per pass, cleared at batch end; skip `priorityOrder[i]` for `i >= ceiling`) so a `BackpressureBlock`
wake-up cannot dispatch a pre-existing lower-priority event mid-batch; liveness kept.
Detector: stage a lower-priority event, saturate with QueueSize below the per-priority slice, assert all batch events
dispatch first (`TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait`).

### 30. File Size Is A Signal, Not A Rule

Rule: split by "what does this return", not by line count (900 lines doing one thing is fine, a 200-line grab-bag is not;
a shared receiver `c *Core` is not cohesion). Evidence: `store_capabilities.go` split into capabilities, validation,
copy and fetch files.

### 31. Tx Holds The Write Lock — Mirror Every Read Accessor

→ superseded by Path B (v4.1.0): a tx no longer holds `c.mu.Lock` for its lifetime; the tx-side read mirrors (`tx_consistent_reads.go`) are for clarity only and would be required again only if that lock came back (9).

### 32. Don't Conflate Transaction Time With Valid Time In The Resolver

Rule: the valid-time end of a version is `next.ValidFrom` (valid time of the next state), falling back to
`next.UpdatedAt` only when unset, never `UpdatedAt` (the TX time of the supersede) alone (`nodeVersionBounds` /
`relVersionBounds`). Failure: a caller-supplied `tkg_valid_from` did not close the prior tile and `NodesAtTx` fell back.
Trigger: resolver code deriving `vEnd`.

### 33. Update Must Not Inherit Valid-Time From The Previous Version

→ merged into 42 (Update is one of the deep-copy mutation doors; the rule, the legacy shim and the detector are there).

### 34. AsOf TX Queries Default To Now — Pass TxAt For Bitemporal Reads

Rule: `QueryOpts.TxAt == 0` means "no TX filter" (VT match across the union of all history rows, including writes not yet
happened in the caller's timeline); kept for pre-bitemporal call sites, NOT "as of now". New bitemporal point queries set
`TxAt` explicitly.

### 35. (superseded by 46)

Still true: eclipsed rows use the zero-width sentinel `ValidTo = ValidFrom + 1`, split fragments get versions `maxVersion+1+i`, temporal metadata is not in the content hash.

### 36. Migration-Gated Resolver Heuristics Need A Runtime Flag

Rule: when a data migration removes the need for a resolver heuristic, keep the heuristic for stores that cannot run the
migration (no capability, mock or injected stores), gated on a per-core flag set by the post-`New` migration runner
(`if c.bitemporalMigrated || !heuristic(...)`); removing it outright breaks mock stores whose iteration errors.

### 37. Capacity-Soft Token Registries Don't Fail Writes

Rule: at 65536 entries `GetOrCreate` returns `(0, nil)` with a sticky warn-once, never `ErrRegistryFull` (property-key
cardinality is hard to bound; failing turns degradation into an outage). Encoders MUST treat token 0 as "no token, write
the raw key string".

### 38. Validation Hooks Are The Right Place For Side Indexes

Rule: hook side indexes (property-key registry, cardinality stats) into the Core validation layer
(`validateOwnedPropertyEntryForCreate`, `validatePropertyUpdates`): it sees every key on Add and Update with Core in scope.
Not `types.PropertySlice.Set` (`pkg/types` is fundamental, circular-dependency risk), not the wire encoder (no Core context).

### 39. Custom EncodeMsgpack Bypasses Struct Tags

Rule: a new msgpack-tagged field on a struct with a hand-written `EncodeMsgpack` (`PropertyWire` in `wire_encode.go`) is
also appended to the custom encoder, replicating omitempty by hand; plain `msgpack.Marshal` round trips do not catch it.
Detector: `grep -nE "EncodeMsgpack|EncodeMapLen" pkg/graph/internal/storeutil/` lists every encoder to update.

### 40. Load Registry Before Index Rebuild

Rule: backend-internal decodes that run during init have their dependencies resolved first: load the property-key
registry from meta KV BEFORE `loadIndexes` calls `bs.decodeNodeWireForKey`, else tokenized rows fail decoding and
`GetNode` returns `ErrNodeNotFound` for a row that exists on disk.

### 41. A Version Bump Is Not Done Until Every Doc-Consistency Target Matches

Rule: a commit that adds a numbered `## [x.y.z]` CHANGELOG heading also bumps the title in `docs/architecture.md` and
README "Current release", and keeps the go.mod Go version equal to README/architecture; "the
CHANGELOG says docs were updated" is not evidence, the test is.
Detector: `go test -run TestDocsMetadataMatchesSourceOfTruth ./pkg/graph/internal/core/` before committing a release.

### 42. A Mutation Door That Deep-Copies Current Must Clear Inherited Valid Time — EVERY Door, Not Just Update

Rule (absorbs 33): clear `current.Temporal().ValidFrom` / `ValidTo` before re-applying caller values, so `ValidFrom != 0` on a
non-genesis version means caller-supplied. It holds for every door that DeepCopies current and writes a `*WithHistory` row
(`updateNodeInternal`, `add/removeNodeLabelInternal`, both property-CAS sites) except doors that close the interval on purpose
(delete tombstones, `CloseVersion`); the `nodeVersionInheritedValidFrom` heuristic is only a shim for pre-Phase-1 data.
Detector: `rg -n 'WithHistory\(' pkg/graph/internal/core/*.go` and the cross-door test `TestTemporalTwoDoorsAgreeOnLabelQueries`
(single-door tests cannot see it: all doors share the broken resolver input).

### 43. Superseded Is Not Retracted — TxTo Must Not Bound Valid-Time Answerability

Rule: `visibleAtTx := TxFrom <= txAt` (recorded by then), never `&& (TxTo == 0 || txAt < TxTo)`: `TxTo` marks supersession,
not retraction, and the row stays the authority for its valid-time slot in later belief states (32's class, in the
visibility rule). Pointers returned from frozen scan rows must be independent (`Temporal()` / `Integrity()` copy-on-frozen).
Detector: v0 VT=[1000,inf), v1 VT=[2000,inf): `NodeAtTx(1500, now)==v0`, `(2500, now)==v1`, `(2500, txBetween)==v0`,
`(*, txBeforeFirstRecord)==nothing`.

### 44. A Trust Boundary That Stores Hashes Must Verify Them On Entry

Rule: besides structural validation (6), import recomputes each row's content hash against the claimed one AND verifies the chain of
every imported entity after replay (content checks cannot see a flipped `PrevHash`); any mismatch gives `ErrCorruptExport` + rollback,
and every structural failure (truncation mid-record included) wraps that sentinel.
Detector: truncate an export at many offsets and flip single bytes: each attempt fails `errors.Is(ErrCorruptExport)` with ZERO
partial state, or yields a graph whose entities all pass `Verify*Chain`.

### 45. Shrinking A Resource Bound Must Account For State Written Under The Old Bound — And A Dependency's Assertion May Be os.Exit

Rule: (1) state written under the old bound may be unopenable under the new one (Badger replays each WAL, created at 2x the
MemTableSize that wrote it, into an arena sized by the CURRENT one): owe a migration (`MigrateOversizedWAL`). (2) A dependency
invariant violation may be `os.Exit` ("Arena too small" via `y.AssertTruef`), not a `recover()`able panic, probe opens included:
detect the unsafe condition before handing the directory over (scan `.mem` sizes), then migrate or fail closed (`ErrOversizedWAL`).
Detector: test the `os.Exit` with a subprocess re-exec doing the RAW dependency open; mutation-verify "knob is applied" tests.

### 46. A Correction Is A New Belief — Never Mutate A Stored Row To Express It

Rule: a valid-time correction recorded at `now` appends fresh rows stamped `TxFrom = UpdatedAt = now`; stored rows are immutable and
`TxFrom` is never back-dated (the old in-place rewrite made `NodeAtTx(_, oldTxAt)` reconstruct a belief that never existed and left
two TX-open rows that broke the native `NodeAsOf` early-stop). The resolver filters the chain to `TxFrom <= txAt` and tiles the rest
in effective valid-from order, overlaps broken by the newer belief (higher `TxFrom`, then version). Pairs with 43.
Detector: two-phase on all backends: create, correct at a later tx, query `*AtTx` BEFORE it: pre-correction values, no holes, no leak.

### 47. The Decode Step Is Part Of The Trust Boundary — And `recover()` Cannot Catch A Fatal Stack Overflow

Rule: decode persisted/imported bytes only through `storeutil.SafeUnmarshal`, never raw `msgpack.Unmarshal`. msgpack crashes the
process two ways: a reflect panic (repeated map key on an interface field) and a fatal stack overflow on deep `Value any`
nesting. ORDER matters: `guardMsgpackDepth` (non-recursive scan, rejects depth above 64, never under-counts) first, THEN
`defer recover()`; both return `store.ErrCorruptWire`. 64 not lower: legitimate wire with a 32-level property sits at ~35.
Sanctioned exception: `scanWireTemporalMeta` (architecture.md "Decode untrusted msgpack").
Detector: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test`; `FuzzWireToNodeChecked`; assert `ErrCorruptWire` on a 200000-deep blob.

### 48. Allocate Proportional To Bytes DELIVERED, Not To An Untrusted Size/Count Field

Rule: `make(T, n)` / `Grow(n)` / map hints from a decoded length are bounded by bytes actually delivered or a small constant, never the
declared value even under a "max" cap. GOOD: `b.Grow(min(length, cap)); io.CopyN(&b, r, int64(length))`. Found by `FuzzImport` stalling
at 0 execs/sec: `readImportStageRecord` (5-byte header, ~25M x amplification) and `reserve()` (~20 bytes forced ~312 MiB; cap now 4096).
Detector: feed a tiny lying header and assert the `TotalAlloc` delta stays far below the declared size; fuzz streaming boundaries END-TO-END.

### 49. A Change-Log Must Be Emitted In-Backend And Co-Committed — A Decorator Cannot Be Crash-Safe

Rule: a write-observer / op-log / CDC seam lives INSIDE each backend, co-committed in the same atomic write, as an OPTIONAL
capability the core type-asserts; never a Store wrapper (it cannot reach the inner `WriteBatch` and is not an exact native store,
which silently disables the zero-copy scan). Design: architecture.md "Change-log (op-log) is IN-BACKEND".
- LSN + record enqueue under ONE lock window with the entity ops (`appendOpsLogged` for doors without `idxMu`); audit every early
  return after the snapshot in `flush()` and the empty-batch guard `len(ops)==0 && len(logs)==0`.
- Shared helpers stay record-free; the public door emits one logical record. Backends share one set of body builders
  (`storeutil`) plus a byte-parity test. The op-log alone does not converge a replica (snapshot the registry, then tail).

### 50. A Replica Must REPRODUCE The Primary's Rows, Not Re-Derive Them — And One Bit Of Provenance Decides History Depth

Rule: apply writes the wire VERBATIM through foreign-ID store doors, never `NodeOps.Add`/`Update` (they re-mint IDs, re-stamp
`TxFrom`, recompute hashes); the import trust pipeline VERIFIES, never STAMPS, the hash (the convergence oracle). Design: architecture.md
"Replica apply reproduces the primary's rows VERBATIM".
- A bare new-current row is ambiguous: carry ONE provenance bit (`NodePutBody.WithHistory`); a record must disambiguate the
  TRANSITION. Put wire is untokenized. The read-only gate is core-layer (`checkWritable()` on every user mutation door, found by what
  a method DOES, not by file).
- Watermark after work needs idempotent apply (identical row -> no-op, `rec.LSN <= applied` -> skip) AND the work flushed first
  (`flushStoreLocked()` before `setAppliedLSNLocked`): ahead of the data is fatal, behind is harmless.
Detector: a disk-backed non-`SyncWrites` restart-resume test, a double-apply history-depth test.

### 51. Syncing An Append-Only Registry Across Nodes: Capture-Order, Prefix-Guard, And Persist-Before-Use

Rule: (1) capture the anchor LSN BEFORE the names (`CapturedAtLSN >= rec.LSN`). (2) Grow via prefix-guarded
`AppendNames(prefix, suffix)`: equal contents or `(false, nil)` without mutating; `ImportNames` is load-only; all three
registries. (3) Persist the grow BEFORE the row that needs it under `registryMu`, `RollbackNames` on persist failure. (4) Sync only
labels and rel types (property keys tokenize locally). (5) Lock order `c.mu -> registryMu`. (6) Failover: the ID-slot lease is a
durable hint, not consensus; a promoted node and the one it replaces hold different slots.
Design: architecture.md "Token-registry refetch" and "Bootstrap LSN ... failover lease".

### 52. Delta Backups Reuse The Op-Log, Not A Diff — And A Hash Oracle Hides Non-Hashed Divergence

Rule: `ExportSince` / `ImportMerge` key a delta off the change-log (TX-ordered), never a temporal diff (it drops backdated writes
and cannot rebuild label changes or cascade tombstones). Hash equality cannot see `TxFrom`/`TxTo`: apply reproduces superseded
`TxTo` and tests compare bytes. A feed shows only durably committed records, so `Export`/`ExportSince` `flushStoreLocked()` first
(50). A "present but disabled" capability needs a status probe (`ChangeLogEnabled()`). The hash chain is a DAG: `Verify*Chain`
checks "every non-genesis PrevHash is among the version hashes". A purge-and-recreate rollback truncates history too.

### 53. A Reset That Wipes A Durable Watermark Must Keep The Watermark Continuously Present

Rule: when a reset must preserve one durable invariant, do not wipe-then-rewrite; keep the key present and mutate in place
(`clearAndReanchorChangeLog` drops data/index/history/log keyspaces, leaves `KeyMeta` with `LastLSNKey`, overwrites it with the
marker in one atomic batch). `DropAll` then a marker write let a crash reseed LSNs from 0. Delete stale counters/registries BEFORE
data (a counter above live rows is fatal, an absent one trusts rows). Fix EVERY door: the log-DISABLED `Clear()` arm also keeps
`LastLSNKey` (`clearDataPreservingLastLSN`); grep every path that deletes the key.

### 54. A Swap-Out Commit-Window Buffer Must Be Cleared On Success AND Consulted By EVERY Reader

Rule: the badger flush swaps `pending` for a fresh map and commits lock-free, so a row is in neither `pending` nor a `View`.
Park the snapshot in `flushing` and union `flushing ++ pending` in ONE helper (`rangePending` / `lookupPending`).
- Audit EVERY reader (`grep 'range bs.pending\|bs.pending\['`); at first only 1 of ~13 was routed.
- Populated between swap and commit, EMPTY otherwise: clear on success, on requeue and in `Clear()`/reset.
- Delete-set-computing mutators (cascade delete, incoming repair) need the snapshot too, or they orphan keys; set/delete
  symmetric across both buffers.
Detector: a white-box helper doing the `pending`->`flushing` swap WITHOUT committing; every reader still sees the row.

### 55. The Change-Log Is A PHYSICAL Redo Log, Not A LOGICAL Transaction Log

Superseded for `GraphTx` by the scoped log (`store.TxChangeLogScope`, `DiscardScopedLog` in `Rollback`): a rolled-back tx emits
nothing, LSNs mint at commit; the scoped tx takes `c.mu.Lock` per MUTATION (not lifetime, see 31). Still live for `Batch.Execute`
and `IO().Import` (eager emission): with the log ENABLED registries are APPEND-ONLY across rollback, because a durable put may
reference a new token (replicas stick, the number gets reused). Gate on `c.changeLogEnabled` (not `changeFeed != nil`) and only
the two entity-failure `RollbackNames` sites; allocation/persist-failure closures fire before any record and still de-allocate.
General: audit what an aborted operation EMITS and what it RETRACTS from shared monotone state.

### 56. A Purge-And-Recreate Rollback Must Capture EVERY Door's Full Side-Effect Footprint

Rule: `ImportMerge` rollback purges each touched entity (cascade drops attached edges) and re-creates it from a snapshot; the
capture pass must record the transitive closure the purge destroys (adjacency) for EVERY record kind that can be the sole
record touching a node. `captureMergeRecord` arms for `ChangeNodeHistoryVersion` / `ChangeNodeHistoryTruncate` skipped
`captureNodeAdjacency`, so a bounded-past `SetNodeVersionInterval` lost an unchanged edge. Audit by symmetry across the `switch`.
Detector: a node whose ONLY record is the under-tested kind, asserting that precondition by decoding the feed.

### 57. A Multi-Buffer Overlay Must Resolve Set-vs-Delete PER KEY At EVERY Reader Door

Rule: two readers sharing one overlay (`flushing` older, `pending` newer) share one resolver (`pendingHistoryIDOverlay`); a
running min/max/count over a multi-op buffer is a red flag (a scalar cannot retract a deleted key): collect surviving keys,
resolve set-vs-delete, then reduce. Failure: `MaxNodeHistoryID` returned 500 for a flushing SET masked by a pending DELETE
while `AllNodeHistoryIDs` returned none.
Detector: cross-door AGREEMENT (`Max*` vs `All*` on the same overlay state), not each door alone.

### 58. A Contract That Spans A FAMILY Of Sibling Doors Must Land In EVERY Door — Audit The Family

Rule: list the siblings, grep them, confirm the same `errors.Is` classification, validation predicate and ordering. Failures:
"decode rejections wrap `ErrCorruptExport`" held for bootstrap import but the shared replica-apply path returned
`WireTo*Checked` errors bare (seven sites); `PropertyKeyRegistry.GetOrCreate` guarded `== ""` while the others used `isBlankName`.
The fix DIRECTION comes from the existing shared parametric test, not the loosest door. Tighten CREATE doors, keep the LOAD door
lenient for legacy data (pin it). Make un-expressible invariants executable (a `Config.Store` decorator injecting one `Flush()` failure).

### 59. A Privileged Override Of A System-Controlled Field Lands At The Shared Seam, Scoped Where The Invariant Binds

Rule: enumerate the field's write sites and relax the invariant only where it binds. `tkg_tx_from` backfill is CREATE-only (the six
`extractTemporal` doors); supersession doors keep the clock (20/46) except `DeleteWithTx` / `UpdateWithTx` / `Set*VersionIntervalWithTx`,
which take a caller instant as an ARGUMENT once the invariant is a check under the entity lock (`checkTxOrder`, `ErrTxOrder`).
- Land it at the ONE shared seam (`extractTemporal`; one `at` seam on the delete doors; `updateTemporal.txAt`); deferred doors
  (Batch / ingest) add a whole-unit pre-flight reusing the seam's refusal functions.
- Gate with `Config.AllowTxBackfill`; malformed input beats the disabled-gate error; bound a caller value on a monotonic axis on BOTH
  sides against the clock (`UnixMicro` passed as ms is year 58000).
- Non-hashed fields are invisible to `VerifyChain` (52): assert `TxFrom` on the re-fetched row. Reap durable metadata (`asof_tags`) in
  `Admin().Reset()`; load re-validates VALUES against the write door (`{"tag": 0}` aliased "no TX filter").

### 60. A Retroactive In-Place Stamp Must Be Un-Applied At Every Belief-Reconstruction Door — Delete Is A Transaction-Time Tombstone

Rule: hard Delete stamps `DeletedAt`/`ValidTo`/`TxTo` IN PLACE on the final version (43 fixed `TxTo` only); the stamped `ValidTo`
killed the row in valid-time coverage, so generic `TxAt` reads dropped deleted entities at every pin while the named as-of door
resurrected them. For any retroactive stamp, enumerate the fields and every "as known at T" reader; each stamp is disregarded
when recorded after T. Land it at the shared seam (`filter{Node,Rel}ChainByTxAt`, `normalizeTemporalVisibleAtTxTime`), deep-copy
before normalizing. Tests derive pins from the entities' own `TxFrom`, never a slept wall-clock pin (`Core.now()` has a 1ms floor).

### 61. A "Current Transaction Time" Reader Must Consult The Commit Clock, Not The Session High-Water Mark

→ merged into 71; still true: `Temporal().NowTx()` returns `c.now()`, never the session-local `c.lastInstant` (0 after reopen = the "no TX filter" sentinel); detector `TestNowTx_ReopenSafe`.

### 62. An As-Of Resolver Must Not Fall Through Past A Retracted Newest Belief — And "Newest" Is By Version, Not TxFrom

Rule: select the decisive belief, then judge its retraction, never filter it out during selection: the newest version recorded by
`txAt`, if superseded or deleted by the pin (`TxTo != 0 && TxTo <= txAt` or `DeletedAt != 0 && DeletedAt <= txAt`), means ABSENT.
"Newest" is the highest **version** with `TxFrom <= txAt`, not the highest `TxFrom` (`validInstantAfter` and the cascade break their
co-monotonicity). Fix in the READ path, keep the cascade append-only. Failure: Add(vf=1000) -> `SetNodeVersionInterval(2000)` ->
Delete -> `NodesAsOf(far-future)` returned the open genesis.
Detector: `asof_cascade_delete_test.go` (memory/badger/tiered, node+rel) plus an as-of clause in the bitemporal oracle harness.

### 63. An Unlocked Collect Window Before A Rebuild-Commit Needs A Write-Generation Guard — And No Rescan Under The Caller-Held Write Lock

Rule: (a) a rescan fetching cache-cold rows takes `idxMu.RLock`, so it must not run under the caller's `idxMu.Lock` (self-deadlock,
see 9); (b) releasing the lock to gather and re-taking it to COMMIT opens a lost-update window (not a data race, `-race` is silent).
Guard with a write-generation counter bumped by every mutator (`PropertyStatsAccumulator.WriteGen`): read before the collect,
re-read before the commit, redo if it moved (bounded, 24); on exhaustion return the live snapshot WITHOUT committing, leave `dirty`.
Detector: `TestBadgerStoreNodePropertyStatsStaleRescanOverwrite`; storm tests assert VALUE correctness against a sequential truth.

### 64. A Scan-Then-Overlay Reader Drops A Row Across The Commit Window — Snapshot The Overlay BEFORE The Badger View

Rule: overlay cache + snapshot-isolated store are two clocks: snapshot the MUTABLE overlay into a local map before `db.View`, merge it
over the scan, the immutable snapshot second. Structural, never timing-justified ("back-to-back so the window is negligible" is a
probability argument; absorbs 74). Audit readers AND delete-set mutators (history truncate/trim, index purges, incoming scrub); point
reads are exempt. Lazy builds make a missed row permanent: read the "built" flag and enqueue under `idxMu.RLock`. Publish order: when a
write changes two views a lock-free reader reads in sequence, publish in the REVERSE of the reader's order (`publishMoveLocked`).
Detector: grep every overlay reader (`pendingHistory*Overlay|snapshotHistoryOverlay|rangePending(|lookupPending(` under
`pkg/graph/store/badger/`) for capture before `db.View`; test the WINDOW (`historyScanTestHook`), not presence.

### 65. HyperLogLog Accuracy Tests (And Any NDV-Bound Test) Must Feed Well-Distributed Values

Rule: a test asserting NDV close to a known count feeds random strings, hashed values or prime-stepped integers (`i*97`), never
a tight run of small consecutive integers: `AddString` (FNV-1a) avalanches poorly on short low-byte-varying inputs and
undercounts ~11x. Test-authoring rule only, not a production defect. Anyone tightening the upper-bound-only concurrent tests
must scatter values. Evidence: `hyperloglog_sequential_test.go` (BACKLOG 16d).

### 66. A Pointer Swap Guarded By One Lock Class Races Every Reader Outside That Class

Rule: (1) a shared POINTER is swapped only while holding EVERY lock class its readers use (`restoreRegistries` swaps
`c.labels`/`c.relTypes` under `c.mu.Lock` AND `registryMu`; `Close`'s persist takes `registryMu`); adding a reader on a new lock
class means grepping every writer. (2) The pre-release race gate is the FULL `make test-race`, never a targeted `-run` subset
(storm tests only run in the full suite; slower CI runners open windows a fast box never does); it is part of the release gate
`make ci-docker` (Makefile:196), so it runs once per release, not per commit (87).
Detector: `grep -rn 'c\.labels = \|c\.relTypes = ' pkg/`.

### 67. A Raw-Byte Storage-Saving Estimate Is Not A Disk-Saving Estimate

Rule: state any "shrink the wire" saving as post-block-Snappy (4 KB blocks, keyspace order, history rows contiguous) over a realistic
corpus, never uncompressed or per-row. Low-entropy redundancy (timestamps) is a mirage post-Snappy (B3: 2.99% raw -> 1.13%, dropped);
whole-structure bulk survives (B6: 57.65% -> 39.10%, kept). Keep the measurement as an executable gate (`wire_b3_ondisk_gate_test.go`).

### 68. Reconstructed Wire Properties Must Be Re-Sorted By KEY STRING, Not Token

Rule: code that REBUILDS a wire property slice from parts (delta apply, merge, splice) emits key-string order
(`storeutil.SortWirePropertiesByKey` after resolving `KeyToken->Key`), because `WireToNodeChecked` rejects non-ascending key
strings and token order is not key order (it only looked alphabetical when `SetProperty` pre-sorted). Truncate re-tokenizes
before re-marshaling. Detector: a memory-vs-badger DIFFERENTIAL ORACLE for any storage-representation change (a same-backend
round trip is not enough); `history_delta_test.go` (BACKLOG 18s).

### 69. An Overlap-Based Candidate Prune Must NOT Be Applied To A Query Door Whose Predicate Set Includes NON-Overlapping Relations

Rule: before reusing a prune/short-circuit on a new door, check the door's predicate is a SUBSET of what the prune assumes.
`PruneTemporalCandidates` (valid-time envelope) is sound for `During`/point doors but wrong for `NodesRelating`, whose Allen set
includes `Before/After/Meets/MetBy`; scope the wiring and say why at the call site. Open interval ends are +inf: the Relating
classifier (`types.RelateOpen`) takes the query end RAW (0 = open). Detector: an exact-set test over `{Before, After, Meets,
MetBy}`; a happy-path `{Overlaps}` test passes with the wrong prune.

### 70. `reflect`-Based Sorting/Comparison On A Hot Path Is A Perf Bug — But Not All Reflection Is

Rule: no `reflect`-mediated operation (`sort.Slice`, `reflect.DeepEqual` as a typed-`Equal` substitute) on a per-write or
per-query path; use `slices`/`cmp`/`maps` or typed code. Reflection stays where the problem is reflection-shaped (`pkg/types`
property validation over arbitrary `any`) or the path is cold; the line is call frequency. Also audit THIRD-PARTY fallbacks:
`msgpack.Marshal` reflects unless the type implements `CustomEncoder`/`CustomDecoder` (the change-log body wrappers and
`HistoryDeltaEncoding` wrappers did not: a struct embedding an optimized field is not itself optimized).

### 71. A Wall-Dominated Monotonic Floor Is Not Reopen-Safe — Seed It On Open And Advance It On Every Foreign-Stamp Ingress

Rule (absorbs 61): `Core.now()` is `max(wall, last+1)`, so above 1 write/ms stamps run ahead of the wall and persist; after a reopen
(`lastInstant` = 0) or a foreign stamp (replica apply, bootstrap import) a new write can get a `TxFrom` below a committed one
(silent; `TxFrom` is not hashed). Persist the floor as a MetaKV watermark on `Close`, reseed on open (`seedInstantFloor`); advance
it at the central apply seam (`recordCommitStamp`) and per replayed wire on `Import`; only system TX stamps pull it
(`maxCommitStamp`), never `ValidFrom/ValidTo`. A crash loses the Close watermark: the store persists a commit floor with the rows (the newest stamp of every row it flushes, decoded from the batch, so no door can miss it) and `New` raises the clock to it (`seedCommitFloor`, backlog 44). "Every door" is a MOVING set:
re-audit on rebase, enumerate unhandled change tags with a reason. Reads use `c.readNow()` = max(wall, floor).
Detector: `TestNowTx_ReopenAfterBurst_MonotonicFloorAnachronism`, `TestCrashReopenNowTxAboveStoredStamps`, `TestNowTx_ReplicaCoversAppliedFutureTxFrom`, `TestInstantFloor_PreservedAcrossReset`.

### 72. An Externally-Filed Perf Diagnosis Names A Plausible Mechanism — Reproduce And Profile Before Touching The Named Component

Rule: treat only the SYMPTOM as data: reproduce in-repo with the reporter's fixture shape, profile, let the profile name the component
(a head-pin report blamed `RelBeliefWatermarkCapability`; the profile found `ForEachDeletedRelID` stepping every `0x08` row). Drain the
flush tick before `b.ResetTimer()`. Distinct-ID scans over `prefix/<id>/<version>` `Seek` to `prefix/<id+1>` once an id is decided.
Detector: `TestBadgerStore_AllNodeHistoryIDsFrom_PendingDeleteMasksSomeVersions`.

### 73. The Chain Resolver's Input Order Is A Semantic Input

Rule: `resolveNodeChain`/`resolveRelChain` take the chain in ASCENDING-VERSION order and `sortNodeChainForResolve` both detects
non-monotonicity and sorts IN PLACE; feeding a sorted slice back (selection-skeleton fast path) took the positional-tiling arm and lost
a cascade-reopened row. Give every invocation its own pristine copy; "an accelerator must never change answers" needs an equivalence
oracle in CI, not hand-written fixtures. Detector: an env-gated divergence cross-check (run fast AND full, panic with both chains).

### 74. "Back-To-Back, So The Window Is Negligible" Is A Probability Argument, Not A Correctness Argument

→ merged into 64 (capture the overlay before the snapshot structurally; the audit recipe and the lazy-build rule are there).

### 75. A Green Workflow Must Mean Its Gates Passed

Rule: `continue-on-error` plus a prose-only finding baseline turns a security scan into an unaudited log stream; pin analyzer
versions, document audited suppressions at the call site, make the scan exit status blocking.

### 76. A Type Tag Covers One Level — Test Every Kind At Depth, Through The Bytes

Rule: the property wire tags only the top-level value, so nested kinds (eight integer widths, typed slices,
`map[string]string`, typed nil, registered structs) came back as other kinds. A codec round-trip test enumerates EVERY allowlisted
kind at depth 0, 1 and 2 in both container kinds through the real bytes, comparing value AND Go kind (hash bytes for NaN / -0).
An envelope adds wire depth: apply the property depth limit to the reconstructed value, bound the raw walk by the decode limit.

### 77. (merged into 78)

### 78. Red Break-The-Code Test First, Root-Cause Fix — see the rules digest

### 79. Release And Review Process: Fix = Patch, Door = Minor

Rule (René 2026-10-10, -17, -21):
1. Versioning: a fix is a PATCH, a new door or option a MINOR; a fix never rides in a minor, a door never in a patch; doors from
   one round of consumer requests batch into one minor, fixes into one patch. Each release: one `make ci-docker`, one consumer
   build, one message. Behaviour changes go through the deprecation ritual (`api.md` § Stability and deprecation); no new named exceptions.
2. Review: one reviewer round per branch (Fable), fix round to the same agent, a second only for a blocker; Fable runs no tests;
   the main session runs every test that decides a merge or release, lint, security and the gate. Briefs name 5-8 files and end
   with `make lint-docker security-docker`. Doc/comment-only changes get the docs tests and a diff read.
3. Release authority: merge, gate, tag, push main and the tag, notify consumers, without asking; `git fetch` + merge origin/main
   before every gate (Markus also releases on main; keep our tag numbers above his); never push `origin/v5`, never force-push;
   commits carry no attribution lines.
4. Scope ("Just essential Patches from now on"): consumer-blocking or wrong-answer/data bugs only, plus ai-soc runtime items
   (standing go); a backlog item needs a consumer request or a failing test.
Detector: a second review round, a brief without lint + security docker, a minor tag without a new door, a fix inside a minor.

### 80. Keep todo.md And HANDOVER Current At Every Event

Rule: see the rules digest (Ledger). Detector: `scripts/check-ledger.sh`: HANDOVER.md names the latest tag; the top release
heading of docs/CHANGELOG.md is the latest tag; tasks/todo.md has the sections Next, Waiting, Decisions and Parked backlog, no
done item, and no step citing a backlog number missing from Parked backlog. Run after every merge and before every release commit;
a failure blocks it.

### 81. Open Work With A Clear Next Step Is Started, Not Offered — see the rules digest

### 82. A Release Is Not Green Until GitHub CI Is Green

Rule: the release gate is `make ci-docker` AND the GitHub CI run of the pushed commit, but consumers are notified right after
the local gate, tag and push; CI runs in the background and consumers get one line only if a red run changes anything for them.
Check `gh run list --branch main --workflow CI` before reporting a release done. A tag's red CI is fixed on main and shipped as a
NEW patch tag; never rerun the old tag's run (it re-tests the old commit).
Operations: `make ci-docker` mounts the live work tree, so code edits and merges wait until it exits (md edits are harmless). The
CI concurrency group cancels a running main run on every push (no run on the tag ref): batch note commits and watch the newest
main run. The Go build cache is shared by every session on the machine: `go clean -cache` only when no session builds.
Fast track (René 2026-10-22, todo 115): `make ci-fast` replaces step (3)'s `make ci-docker` when the batch since the last tag
changes no shipped Go code: `scripts/commentonly` refuses on a changed go.mod/go.sum, an added/removed/renamed non-test .go file
outside scripts/, bench/, tutorials/, or a non-test .go file whose comment-free tokens differ. It then runs fmt-check, vet, build,
`-race -short` of only the tests whose code changed (whole package for a changed helper or tool), the docs tests and
check-ledger; ~15 s for a docs batch. Refused → `make ci-docker`. GitHub CI still runs the full suite on the pushed commit.
Release checklist (the one order): (1) merge every branch of the batch, `go test -short` of the touched packages; (2) Fable review
of the merged batch, fixes merged; (3) `make ci-docker` + consumer build/vet (HANDOVER Appendix B), no code edits meanwhile;
(4) release commit: CHANGELOG heading, README "Current release", `docs/architecture.md` line 1, HANDOVER names the tag, the
shipped rows leave tasks/todo.md; docs tests + `scripts/check-ledger.sh`; (5) push main, `git tag -a vX.Y.Z`, push the tag;
(6) notify sigma-tkgd + ai-soc with tag, commit and migration notes; (7) watch the newest main CI run; red → fix on main, new
patch tag.
Detector: a tag whose CI run is not `success`.

### 83. (merged into 78)

### 84. Trace The Cause, Then Consolidate: No Unreflected Questions, No Layers On Top

Rule (René 2026-10-14, -16, -22): before a design question to René, trace where it comes from in the
current code (file:line, not an old note), look for a strategy that removes it (fix upstream, a standard, an existing mechanism)
and solve it if one works; ask only the remainder, naming the cause and each rejected strategy in one line.
Consolidate, don't add: when two code paths, notes or rules do the same job, merge them and delete the rest; duplication is often
the real bug (numeric equality had two rules and 4+ widening helpers). Every write into an md file consolidates into the owning
entry. Never create a new .md file: the set and which file owns what is README.md § Documentation; `tasks/evidence/` holds raw
outputs only; a removed doc is cited as `git show <hash>:<path>`. Agents report in their final message. Cost claims name their
condition and are measured.
Detector: before any verdict "intended / documented" on a disagreement between sibling doors, grep the helpers implementing the
concept (`func .*(Numeric|Widen|Equal|Key)`); two or more = consolidate (58).

### 85. Comment Trimming Is Editing, Not A Script

Rule: trim comments by hand, file by file, never inside a sentence; never drop lock-order, caller-holds, crash-ordering,
re-entrancy, error/nil/ownership/concurrency contract, format-compatibility or `lesson N` lines. Every comment pass gets an Opus
check of the diff for lost invariants before merge (2026-10-14: a scripted pass lost ~260 passages and was discarded).

### 86. v5 Convergence Happens In v5, Not In v4

Rule: when the goal is "v5 becomes the new public API", do not propose v4 API changes (deprecations, mirror doors, godoc notes)
to converge on v5; v4 stays as it is for its live consumers, and the path is built on the v5 side (public v5 packages, v4→v5
door map, compat façade, conformance suite). René 2026-10-16: "no, we do not want this in v4".
Detector: a v5-related ledger row whose deliverable names a v4 file or a v4.x release.

### 87. Fast Tests Per Commit, Long Tests Only Before The Release Push — see the rules digest

### 88. (merged into 79)
