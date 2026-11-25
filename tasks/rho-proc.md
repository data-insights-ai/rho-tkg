# rho-proc

rho's gate, test tiers, flaky tests, comments, release process.

Items of this topic; the head of [index.md](index.md) says how to use them. Fields: [index.md](index.md#fields).
Code facts are read at origin/main f837590b (v4.107.17); paths without a prefix are under `pkg/graph/`.

### RHO-12 Code without a caller is removed from rho

- area: RHO
- kind: cleanup
- provides: STABLE
- state: parked: paused
- release: later
- owner: -
- depends: -
- blocks: PROC-61
- links: -

#### Goal

About 1,100 lines of rho have no caller. AGENTS.md says code without an ai-soc caller is unfinished; dead code is
read, compiled and reviewed for nothing. The maintainer agreed to its removal.

#### Current state

- The list of those lines is not in the repository. No dead-code check runs in `make dev` or the gate.

#### Design

- Produce the list again with `deadcode` over the module's commands and tests; each function with no caller in
  rho, Sigma or ai-soc is deleted with its tests. A public door only tests call is asked of the maintainer before it
  goes.
- One commit per package.

#### Test

`make check` green after each commit; Sigma's and ai-soc's builds against the commit (`make dev RHO=…`).

#### Open points

- None.

### PROC-56 rho's warm gate takes 60 seconds or less

- area: PROC
- kind: perf
- provides: PROD
- state: parked: waits for measured reductions
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

AGENTS.md sets the budget: the gate warm at most 60 s. `make check` was 170.6 s on 19 Nov (194 s under heavy
load). Every review round and every release pays that time.

#### Current state

- `make dev` warm takes about 2 s, 28 s after an edit in core (its test package compiles in about 21 s, PROC-61).
- Which tests the 170 s are was not measured per package.

#### Design

- Measure first: `go test -json` of the gate, seconds per package and per test, the twenty slowest listed.
- Then per slow test: move it to the long tier if it guards no release property, shrink its fixture, or leave
  it with the reason. No new script (AGENTS.md: process changes remove).

#### Test

`make check` warm, wall time before and after in the commit message; 60 s or less ends the item.

#### Open points

- None.

### PROC-57 `testtier.Slow` is the only way a rho test skips

- area: PROC
- kind: cleanup
- provides: STABLE
- state: parked: paused 19 Nov, necessary work first
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A test that skips on a bare `testing.Short()` never runs in the gate, which runs `-short` with `RHO_GATE=1`;
`testtier.Slow` skips only outside the gate. A bare skip is therefore a test nobody runs before a release.

#### Current state

- `internal/testtier/testtier.go` has `Slow(t)`.
- `testing.Short()` is called at 458 places in 237 files under `pkg`, `cmd` and `internal`. How many of them are
  skips and how many shrink a fixture was not counted.

#### Design

- Each bare skip becomes `testtier.Slow(t)` or is moved to the long tier on purpose with a build tag; a
  `testing.Short()` that only sizes a fixture stays.
- A guard test fails on a new bare skip.

#### Test

`TestNoBareShortSkip` in `internal/docscheck`: it parses every test file and fails for a `t.Skip` reached only
under `testing.Short()`. Red today.

#### Open points

- Tests that turn red when they run for the first time are fixed in this item, each in its own commit.

### PROC-58 The commit-order wait test does not spin when many goroutines exist

- area: PROC
- kind: fix
- provides: STABLE
- state: idea
- release: 5.20
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A test helper decides "the writer is parked" by reading all goroutine stacks into a fixed 1 MB buffer. With
thousands of goroutines the dump is cut off, the parked goroutine may never be seen, and the test loops for ever.

#### Current state

- `coParkedInWait` (`store/tiered/commit_order_crash_test.go:279`) allocates `make([]byte, 1<<20)` and searches
  the dump; its caller loops `for !coParkedInWait()`.

#### Design

- The store exposes the parked state to the test through its existing test exports (a counter of waiters on the
  commit-order gate); the test waits on that with a deadline. No stack dump.

#### Test

The existing wait test, run with 5,000 idle goroutines started first: it finishes within its deadline.

#### Open points

- None.

### PROC-59 The block-cache statistics test does not fail on an asynchronous cache insert

- area: PROC
- kind: fix
- provides: STABLE
- state: parked: waits for a second failure
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

`TestStorageStats_CompressedReadsCountInTheBlockCache` failed once in CI (431 misses, nothing added) and not again
in 1,200 local runs. A gate test that fails one time in thousands costs a release rerun each time.

#### Current state

- The test is in `store/badger/storage_stats_test.go:65`. The suspected cause, not proven: the cache library
  applies inserts asynchronously, so a read right after the first read can miss.

#### Design

- On a second failure: the test waits for the cache to apply its pending inserts (the library's wait call)
  before it reads the counters.

#### Test

The test itself, 2,000 runs at `-cpu 1,4`.

#### Open points

- None.

### PROC-60 Comments that cite a review section or a design file say what the code does instead

- area: PROC
- kind: cleanup
- provides: STABLE
- state: parked: waits for a code item in those files
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

The code rules forbid comments that point at a review or design document: the document moves or is deleted and the
comment then explains nothing. About 50 such comments were added in one period.

#### Current state

- Not counted on this code. Comments naming `tasks/evidence/...` exist (for example
  `store/tiered/warm_memtable.go:61`), and that folder is not in the repository.

#### Design

- Each such comment is rewritten to state the rule the code keeps, or deleted. Done file by file when an item
  touches the file, or in one sweep by the maintainer's word.

#### Test

A grep in `internal/docscheck`: no comment under `pkg` names `tasks/evidence`, a review section number or a design
file.

#### Open points

- None.

### PROC-61 A core edit does not recompile one test package for 21 seconds

- area: PROC
- kind: perf
- provides: PROD
- state: parked: deferred 18 Nov, revisit if core edits dominate
- release: later
- owner: -
- depends: RHO-12
- blocks: -
- links: -

#### Goal

`internal/core` has 469 test files in one package. Any edit in core recompiles all of them: `make dev` goes from 2 s
to 28 s.

#### Current state

- 469 test files and 179 other files in `internal/core`.

#### Design

- Tests that use only the public surface move to external test packages by area (reads, writes, retention,
  budget), so an edit recompiles the area it touches.

#### Test

`make dev` after a one-line edit in a core file: wall time before and after; target 10 s.

#### Open points

- None.

### PROC-62 Whether ai-soc's tests run against a rho commit before rho's tag

- area: PROC
- kind: decision
- provides: STABLE
- state: parked: default is no
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A rho release can break ai-soc. The question is whether the release runs ai-soc's tests first.

#### Current state

- `make release` runs rho's gate only. ai-soc's own gate runs when it pins the new tag.

#### Design

- Default while the maintainer has not answered (Q-25): no. ai-soc's gate catches such a break before it pins.

#### Test

None; a decision.

#### Open points

- Q-25.

### PROC-63 Whether a patch release starts the race run detached

- area: PROC
- kind: decision
- provides: STABLE
- state: parked: default stands
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

After each patch push the release starts `make race` detached. It loads the shared machine for a long time.

#### Current state

- `scripts/release/main.go` starts `make race` detached in a temporary worktree at the tag (log `.git/race.out`).

#### Design

- Default while the maintainer has not answered (Q-25): it stays, after a release only. Options: drop it, run it at low
  priority, cap it at `-p 2`.

#### Test

None; a decision.

#### Open points

- Q-25.

### PROC-64 Releases made on origin by someone else are merged and checked against rho's rules

- area: PROC
- kind: cleanup
- provides: STABLE
- state: idea
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

rho has one released line. A release made directly on origin must be merged into `dev` and pass rho's gate, or
Sigma can pin a tag that lacks the other line's fixes.

#### Current state

- `origin/main` and `origin/dev` are both at f837590b (v4.107.17); nothing is unmerged.

#### Design

- At each foreign release: fetch, merge `origin/main` into `dev`, `make check`, and a review of the diff against
  the rules digest. This is a standing procedure; as an item it has no end.

#### Test

`make check` on the merge.

#### Open points

- Proposal of this rewrite: move the sentence to AGENTS.md's standing orders on the maintainer's word and delete this
  item.

### PROC-65 CodeQL runs from `codeql.yml` only

- area: PROC
- kind: fix
- provides: STABLE
- state: idea
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

CodeQL is configured twice: the repository's workflow and GitHub's default setup. Two analyses give two sets of
alerts and one of them cannot be changed from the repository.

#### Current state

- `.github/workflows/codeql.yml` exists. Whether the default setup is still on, and whether the workflow's first
  run was green, cannot be read from the repository. Switching the default setup off through the API returned
  HTTP 422; it is an organisation setting.

#### Design

- The organisation owner switches the default setup off; then the workflow is run once and its result read.

#### Test

The workflow's run on `main` is green and the repository's code-scanning page lists one configuration.

#### Open points

- Waits for the organisation owner.

### RHO-1001 Three statements in rho's code and rules are verified or corrected

- area: RHO
- kind: cleanup
- provides: CORRECT
- state: parked: open until each is reproduced or cleared
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A handover on 14 Nov left three things unverified: the lock comments on `BeginTx`, the endpoint hashes that
`closeRelVersionInternal` writes, and three lessons missing from the contributing rules. An unverified comment on
locking is a trap for the next change there.

#### Current state

- None of the three was read here. What the three missing lessons are was recorded only in the evidence folder,
  which is not in the repository.

#### Design

- `BeginTx`: read its lock order against `docs/architecture.md` section 4 and correct whichever is wrong.
- `closeRelVersionInternal`: a test that closes a relationship version and reads both endpoint hashes back.
- The lessons: recovered from the session that held them, or dropped by the maintainer's word.

#### Test

`TestCloseRelVersionKeepsEndpointHashes` in `internal/core`: write a relationship, close its version, assert both
endpoints' hashes equal the hashes of a fresh write of the closed state.

#### Open points

- The third point cannot be done without its text.

### RHO-1002 The layered lend test builds its levels itself

- area: RHO
- kind: fix
- provides: STABLE
- state: idea
- release: 5.20
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

`TestLendSortedLayeredAgreesWithPointReads` needs tables on levels 1 and 2. Its fixture leaves that to badger's
background compactor, so under load the precondition failed once (tables per level 2, 0, 0, 3).

#### Current state

- The test is in `store/badger/badgerstore_lend_cost_test.go:148`; the fixture `buildLayeredLendStore` is in
  `badgerstore_lend_cost_bench_test.go:61`.

#### Design

- The fixture places tables on the levels it needs by explicit flushes and level compactions, and checks the
  layout before it returns; it does not wait for the compactor.

#### Test

The test itself, 50 runs with eight parallel copies under `-cpu 1`.

#### Open points

- None.

### RHO-1007 The archived remote branches are deleted

- area: RHO
- kind: cleanup
- provides: PROC
- state: parked: waits for the maintainer to run the deletes
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

origin holds branches nobody works on. Each of them is contained in the archive branch, so deleting them loses
nothing and leaves a branch list that shows only what is live.

#### Current state

- origin has 17 remote refs. Contained in `origin/archive/unmerged-2026-10-27`: the five `codex/...` branches,
  `round10` and `v5`. Not checked against it here: `adjdoor`, `ci-mem`, `memacct`, `read-only-open`, `v2-rho` and
  the dependabot branch.
- `v5` is never pushed to and v5 work is paused (AGENTS.md); whether it is one of the branches to delete is the
  maintainer's
  word.

#### Design

- The maintainer runs `git push origin --delete <branch>` for each archived branch; an agent does not (pushes are the
  maintainer's).
- Before that, each branch outside the list above is checked with `git branch -r --merged origin/main` and added
  when it is contained.

#### Test

`git branch -r --no-merged origin/archive/unmerged-2026-10-27` lists only `main`, `dev` and branches in work.

#### Open points

- None.
