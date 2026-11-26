# Open work: index

Start here. This file lists every open item of this repository, one row each, sorted by topic, then release.

## How to use it

- Find the item here, then open only the topic file the row names; the item's design, test and open points are there.
- Keys (`RHO-12`, `PROC-56`, ...) are stable. A key is never reused and never renamed; there are no aliases.
- An agent never starts an item by itself. A human assigns it, and the assignee's name goes into the owner field
  (here and in the item) when the work starts. An empty owner (`-`) means the item is open to take.
- A finished item is deleted from this index and from its topic file in the finishing commit, whose message names the
  key. Git keeps the history; nothing is marked done.
- Binding decisions and open owner questions are in [decisions.md](decisions.md). An item that touches another
  repository lives where the code changes and is cited there by key.

## Area prefixes

- `RHO` work on the rho-tkg store, `PROC` process work in this repository: tests, gates, releases, docs.

## Fields

- kind: `feature`, `fix`, `perf`, `measure`, `cleanup`, `design`, `decision`.
- provides: `CORRECT`, `PERF`, `MEM`, `STABLE`, `PROD`, `PROC` (one or more).
- state: `idea` (no design yet), `designed`, `building`, `review`, `merged-unreleased`, `parked: <reason>`;
  ai-soc's `open-work.sh` refuses any other value and an item whose release is already tagged.
- release: the consumers' release that wants it (`5.13` to `5.22`), `next` (wanted by the consumers' next release),
  `later`, `after 5.20`.
- owner: the person who took the item, `-` until taken.
- depends, blocks: keys or named preconditions; links: branch, commit or test once work exists.

## Items

| key | title | state | release | owner | topic |
| --- | --- | --- | --- | --- | --- |
| RHO-2 | The share of rho's memory store in a session's live heap is measured again | idea | later | - | [rho-memory](rho-memory.md) |
| RHO-3 | `docs/architecture.md` lists everything in rho that grows with the total volume, each bounded or queued | designed | 5.18 | - | [rho-memory](rho-memory.md) |
| RHO-4 | rho's RAM per entity is capped or moved to keys on disk | parked: paused 19 Nov, necessary work first | later | - | [rho-memory](rho-memory.md) |
| RHO-5 | Per-entity answers leave the Go heap: the heap is fixed and RAM is page cache | parked: waits for RHO-4 | later | - | [rho-memory](rho-memory.md) |
| RHO-6 | The RAM of a writable and of an inactive shard is measured on three production-sized examples | parked: after the small production-sized example | later | - | [rho-memory](rho-memory.md) |
| RHO-8 | What is never cold is bounded: rho's heap grows over days in parts no tier frees | parked: waits for ai-soc's view of what holds the memory | later | - | [rho-memory](rho-memory.md) |
| RHO-9 | A clean reopen of a store takes time that does not grow with the store | parked: waits for the maintainer's word to read profiles on the test host | later | - | [rho-memory](rho-memory.md) |
| RHO-10 | Where the the extra storage goes: the split by layer | parked: waits for the maintainer's word and a free test host | later | - | [rho-memory](rho-memory.md) |
| RHO-11 | ai-soc's store layout is agreed: encoding per label and the tier of each kind of node | designed | later | - | [rho-memory](rho-memory.md) |
| RHO-12 | Code without a caller is removed from rho | parked: paused | later | - | [rho-proc](rho-proc.md) |
| PROC-56 | rho's warm gate takes 60 seconds or less | parked: waits for measured reductions | later | - | [rho-proc](rho-proc.md) |
| PROC-57 | `testtier.Slow` is the only way a rho test skips | parked: paused 19 Nov, necessary work first | later | - | [rho-proc](rho-proc.md) |
| PROC-58 | The commit-order wait test does not spin when many goroutines exist | idea | 5.20 | - | [rho-proc](rho-proc.md) |
| PROC-59 | The block-cache statistics test does not fail on an asynchronous cache insert | parked: waits for a second failure | later | - | [rho-proc](rho-proc.md) |
| PROC-60 | Comments that cite a review section or a design file say what the code does instead | parked: waits for a code item in those files | later | - | [rho-proc](rho-proc.md) |
| PROC-61 | A core edit does not recompile one test package for 21 seconds | parked: deferred 18 Nov, revisit if core edits dominate | later | - | [rho-proc](rho-proc.md) |
| PROC-62 | Whether ai-soc's tests run against a rho commit before rho's tag | parked: default is no | later | - | [rho-proc](rho-proc.md) |
| PROC-63 | Whether a patch release starts the race run detached | parked: default stands | later | - | [rho-proc](rho-proc.md) |
| PROC-64 | Releases made on origin by someone else are merged and checked against rho's rules | idea | later | - | [rho-proc](rho-proc.md) |
| PROC-65 | CodeQL runs from `codeql.yml` only | idea | later | - | [rho-proc](rho-proc.md) |
| RHO-1001 | Three statements in rho's code and rules are verified or corrected | parked: open until each is reproduced or cleared | later | - | [rho-proc](rho-proc.md) |
| RHO-1002 | The layered lend test builds its levels itself | idea | 5.20 | - | [rho-proc](rho-proc.md) |
| RHO-1007 | The archived remote branches are deleted | parked: waits for the maintainer to run the deletes | later | - | [rho-proc](rho-proc.md) |
| RHO-13 | The cost of a backfill read in rho is measured: per shard open, per row, rows read against rows kept | idea | 5.18 | - | [rho-read](rho-read.md) |
| RHO-14 | Open shards are capped by bytes, not by count | idea | 5.18 | - | [rho-read](rho-read.md) |
| RHO-15 | A deleted relationship's history read opens only the shard that holds it | parked: waits for a cut refused at shard opens on ai-soc's path | later | - | [rho-read](rho-read.md) |
| RHO-16 | A test can wait for a cold shard's close instead of timing it | designed | 5.18 | - | [rho-read](rho-read.md) |
| RHO-17 | The member-time statistics answer at a pin and for a filter value | idea | 5.20 | - | [rho-read](rho-read.md) |
| RHO-18 | Every rho read that ai-soc's cycle reaches is charged to the cut's read meter | idea | later | - | [rho-read](rho-read.md) |
| RHO-20 | Every native iterator and shard open goes through one mechanism that requires a tally | parked: stopped 19 Nov mid-build; nothing of the build is on a branch | later | - | [rho-read](rho-read.md) |
| RHO-21 | A read that pinned its view before Close returns `ErrStoreClosed` | idea | 5.18 | - | [rho-read](rho-read.md) |
| RHO-22 | A bounded correction in a cold process plans without listing the whole chain | designed | 5.13.4 | - | [rho-read](rho-read.md) |
| RHO-1008 | A relationship property absent or nil is read through the index by `types.Null` | review | next | Claude | [rho-read](rho-read.md) |
| RHO-23 | A chain longer than the head entry's cap is still read in part | parked: paused 19 Nov, necessary work first | later | - | [rho-read](rho-read.md) |
| RHO-24 | An untyped adjacency read of a reference node lists only the shards that hold its edges | parked: paused 19 Nov, necessary work first | later | - | [rho-read](rho-read.md) |
| RHO-25 | The reads a write door makes run under a meter | parked: waits for a design after RHO-4 | later | - | [rho-read](rho-read.md) |
| RHO-26 | Six read costs from ai-soc's read-back inventory are measured, then fixed or dropped | parked: waits for ai-soc's measuring run | later | - | [rho-read](rho-read.md) |
| RHO-27 | A relationship property read on an on-disk index without a valid window counts the entries it rejects | parked: waits for a caller | later | - | [rho-read](rho-read.md) |
| RHO-29 | History reads take less than 10 % of allocated bytes on ai-soc's workload | parked: after the memory and profiling work on the small production-sized example | later | - | [rho-read](rho-read.md) |
| RHO-30 | `HasHistory` answers for a batch of IDs in one scan | designed | 5.20 | - | [rho-read](rho-read.md) |
| RHO-31 | A transaction-time walk by IDs merges across shards | building | 5.20 | - | [rho-read](rho-read.md) |
| RHO-32 | A page of the as-of door with a valid window does not read one row per member of a cold shard | parked: waits for the maintainer's choice | later | - | [rho-read](rho-read.md) |
| RHO-1003 | A candidate door bounded by valid start, if the backfill measurement shows the need | idea | later | - | [rho-read](rho-read.md) |
| RHO-1004 | A pinned timeline stream lends the current relationship instead of copying it | idea | later | - | [rho-read](rho-read.md) |
| RHO-1006 | The composite, temporal and high-frequency index DDL take the caller's context and meter | idea | 5.18 | - | [rho-read](rho-read.md) |
| RHO-33 | rho's bytes and commit cost per committed line are measured over a whole run | designed | 5.18 | - | [rho-write](rho-write.md) |
| RHO-34 | A hot shard takes more than one writer's rate | idea | 5.20 | - | [rho-write](rho-write.md) |
| RHO-35 | Raw rows older than the installation's retention are purged, so rho's disk stays flat | building | 5.13.4 | - | [rho-write](rho-write.md) |
| RHO-36 | The flush pass over every open writable shard has a bounded cost | idea | 5.20 | - | [rho-write](rho-write.md) |
| RHO-37 | badger's Clear invalidates every cached relationship column | parked: not on ai-soc's path, not reproduced | later | - | [rho-write](rho-write.md) |
| RHO-38 | A catalog rollback restores only what its own operation changed | parked: waits for a failing test or a consumer's request | later | - | [rho-write](rho-write.md) |
| RHO-39 | A second `WaitApplied` on a failed asynchronous token returns the failure | parked: no consumer uses asynchronous sessions | later | - | [rho-write](rho-write.md) |
| RHO-40 | A node property that is an append-only list is appended without rewriting the list | parked: needs ai-soc's specification and the maintainer's word | later | - | [rho-write](rho-write.md) |
| RHO-41 | The transaction-time seek index's cost on the write path is shown by a profile or left | parked: waits for a write-path profile of ai-soc's that shows it | later | - | [rho-write](rho-write.md) |
| RHO-42 | The value-log threshold for large values on the tiered store is measured on a current run | designed | later | - | [rho-write](rho-write.md) |
| RHO-43 | Compaction settings per tier follow a measured write amplification | parked: waits for a quiet machine and an ai-soc run on the tiered store | later | - | [rho-write](rho-write.md) |
| RHO-44 | Sigma's page purge reaches rho's purge by ID on ai-soc's path | parked: Sigma's page purge finds only pages an earlier version wrote; it waits for Sigma units of its own to purge | later | - | [rho-write](rho-write.md) |
| RHO-45 | Whether a backfill with valid times before 2026-01-01 is accepted | parked: default stands (refused) | later | - | [rho-write](rho-write.md) |
| RHO-1005 | A purge by ID that reaches an edge on another shard charges that open and can be resumed | parked: waits for a caller whose purged nodes have such edges | later | - | [rho-write](rho-write.md) |
