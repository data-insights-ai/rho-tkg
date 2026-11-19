# AGENTS.md

## Standing rules (René 19 Nov 2026). They override every other rule.

1. arch-v6 is a new architecture, for realtime and for batch jobs over large data.
2. arch-v6 is ONE SYSTEM: rho-tkg and sigma-tkgd are separate libraries, and ai-soc MUST use them fully integrated;
   no memory or resources wasted, no duplicated path (below).
3. Reuse code where appropriate; no rule or guard forbids it.

## One system (René 18 Nov), standing rule 2 in detail
ai-soc is one program. rho-tkg and sigma-tkgd are separate libraries that ai-soc uses fully integrated; their repositories decide where code is edited and released. At run time there is exactly one of each:
- ONE ai-soc: one process, the only caller. No service, protocol, host layer or boundary between the parts. Code in rho or Sigma without an ai-soc caller is unfinished.
- ONE memory: one allowance per cut (Sigma's lease tree) and one native read meter (rho's ReadMeter), from the first read to the analyst's answer. No part opens its own budget, estimate or cache outside them.
- ONE tkg: the single rho graph holds every event and every derived fact. No copy, mirror store or side table.
- ONE Sigma: one Tyla engine whose sessions run over that graph. No second evaluator or test-only route.
What follows:
1. A concept has one type, defined in the lowest part that needs it and used unchanged above. A mirror type, adapter, wrapper or conversion is a defect, removed in the item that finds it.
2. An item has one owner, who changes every repo its path needs.
3. An item is done only when ai-soc's production path runs it. Its first test goes from ai-soc's input to the analyst's answer; tests of single parts come after. There is no "library done" or "host integrated" state.
4. A memory or read limit is checked where that path reaches it.
Check for every design and review: does it add a second process, memory, graph, engine, type or acceptance state? If yes, it is wrong.

Never create a new `.md` file — [README.md § Documentation](README.md#documentation).

Entry point for agents on **rho-tkg** (`tkg/v4`). Code and test rules: `docs/CONTRIBUTING.md` § Rules digest (every session).
Work order: `tasks/todo.md`. Design:
`docs/architecture.md`; API: `docs/api.md`. Read `docs/badger.md` before changing code that reads or writes badger.

## Agent procedure (every task)

1. Run **`make`** first (the menu). Use Make targets, not raw `go` or Docker, wherever one exists.
2. **Todo first:** the item's row in `tasks/todo.md`, before any other action.
3. **Red test, then the fix:** the first red test runs ai-soc's path, from its input to the analyst's answer; then
   the part test, `make test-one PKG=./pkg/… RUN=Test…`, then `make old`. After every edit and before every
   commit: **`make dev`** (the development check).
4. **Review** the full diff before the check (`docs/CONTRIBUTING.md` § Review).
5. **Finished or handing off:** **`make dev`**, then **`make check`** (the minor gate) green. On a red leg, fix
   against the narrowest command: a red test with `make test-one PKG=<failing package> RUN=<failing test>`, a red
   build or lint with `make build` or `make lint`. Then `make check` once more.
6. **Item end** (ai-soc's production path runs it): the item leaves `tasks/todo.md`; its entry is in
   `docs/CHANGELOG.md` under `[Unreleased]`. The todo is committed once per item end, with the item's commits (alone
   only at a session handover), after `make test-one PKG=./internal/docscheck/` (`TestLedger`; `TestDocSizes` caps
   the todo).
7. **Push `dev`:** only when RH asks, after a green check: `git fetch origin`, then `git push origin dev`. No tag;
   `main` does not move. A green check never pushes by itself.
8. **Release:** only when RH asks: `make release` (a major: `make release-major`). It gates, tags and pushes `main`,
   `dev` and the tag atomically, then starts `make race` detached. Before it: CHANGELOG, README and todo committed,
   clean tree (`docs/CONTRIBUTING.md` § Release checklist).

## Who works

**003-aisoc** (ai-soc's session) designs and orchestrates for rho-tkg and sigma-tkgd: it writes each item's design
(**Fable**, read-only, before any code), assigns its owner, briefs the builders and accepts the result. **This session
(001-rho-tkg)** carries out and reviews rho's part on those briefs. Every item and every fix round, no step skipped:
design → **Opus build** (steps 3 to 5, from the brief; spec frozen at launch) → **Fable review** (read-only, before any
merge) → a second round only for a medium or above → merge. The main session checks every finding against source, runs every test, lint
and gate, and commits; it does not design or write item code. Sonnet does docs and mechanical work (bulk scans,
sweeps). Other agents only on RH's word. One session per checkout. At an item end after about three compactions:
update local `HANDOVER.md` if used, tell RH, fresh session.
Every Fable brief opens with René's rule (19 Nov): "You are the owner of one System and long term maintainer of this
codebase. Design is yours."
**One review round (René, 19 Nov).** A fix for a limit, refusal, lifetime or recovery defect starts with its class
table, red: rows are the cases around the defect, columns the stages and every state the fix adds, released on ok,
refused, panic, close and reopen; each hunk turns a named cell green. "Not on the path" is shown by a trace of the real
path, never asserted. The builder hands over, on one tree hash (tracked and untracked files): the touched packages run
without -short on a green base (a test a commit stales is fixed in that commit), and a mutation ledger (per hunk, the
test that goes red when it is reverted; a hunk without one is tested or deleted). A sentence that states a property
names its test. The review checks these and reruns three ledger hunks.

## Standing orders

The only place for them: no standing order lives in `tasks/todo.md` or `docs/CONTRIBUTING.md`.

- **Goal:** realtime ingest at about 2 TB a day (≈23 MB/s, 4 billion 500-byte facts) and batch jobs over large
  data, grown only by more RAM, NVMe and block storage. Nothing may grow in RAM per entity, or in open, recovery,
  Clear or compaction time per store, without a bound. Removing such a wall ranks above a constant-factor win
  (todo SCALE).
- **Scope:** wrong-answer/data bugs and items on ai-soc's production path; an item is done only when that path runs
  it. Anything off that path goes to `tasks/todo.md` § Waiting.
- **`tasks/todo.md` is always up to date:** it is the one authority for what is open. Update it in the same step
  as the work: a new item gets its row before the work starts, a changed state is written when it changes, a
  finished item leaves. A row is one sentence, then when it is done and what it waits for; **rho/sigma** says what
  the item brings to the rho + sigma stack (memory, CPU, storage, restart time) or "none". An open decision of
  RH's carries a default.
- **Releases:** the kinds are **patch**, **minor** and **major**, the same in all three repos. A minor runs the full
  gate: all short tests plus the Slow tests (`RHO_GATE=1`). Development works at the patch level only: fast checks of
  a few seconds, Slow tests skipped. Budget: gate warm ≤ 60 s, cold after
  a Go or dependency bump ≤ 5 min, whole release warm ≤ 2 min; over budget is a todo row the same day. Race, long
  and fuzz never run on a patch tag's path.
- **No attribution:** commits, MRs and PRs carry no Co-Authored-By, Claude-Session or "Generated with" line.
- **Peers:** 003-aisoc briefs; rho reports each item to it in one short message (≲1,500 characters), detail in a
  file; status in the todo (`item | state | next | waits for`), checked against the repo before it is stated.
- **Reopen check:** before a commit that changes what ai-soc reads at reopen, run a small reopen cycle with
  ai-soc's writer, on masked rows only (never a Flux file). Sample: ai-soc `TestRestartModesOverBARows`.
- **Process changes remove:** a change answering "too slow" or "too hard" deletes a step or script, or turns on a
  Go facility, with wall time before and after. No new hook, script or workflow without RH's word. A correction
  from RH changes the rule it corrects; it never adds a second one.
- **Questions and orders:** a question from RH gets an answer from the evidence, then a stop; only an order starts
  work. Act, do not offer. Before a design question, trace the cause in the code and try to remove it; ask only
  the remainder.
- **v5:** v5 work happens in v5, never as v4 API changes; it is paused until RH restarts it.
- **`HANDOVER.md`:** only the latest work items, as a reference for the next session; local, gitignored, never
  committed. It holds no rule and no status the todo lacks; where the two differ, the todo is right.

## Commands

Nothing here runs unless you or RH invoke it; no hook runs a check.

| Stage | Command | What it runs |
|---|---|---|
| Edit, before every commit | `make dev` | Development check: gofmt check, `go vet ./...`, `go test -short ./...`; Slow tests skip. |
| Edit | `make test-one PKG=…` [ `RUN=…` ] | One package or test, `-short`; `PKG` required. |
| After new tests | `make old` | Two stable integration checks on the store surface. |
| Before sharing | `make check` (= `make gate`) | Minor gate: `build` + Docker `lint` + `RHO_GATE=1 go test -short ./...` (Slow included); lists every red leg. |
| One gate leg | `make build`, `make lint`, `make test` | The three legs of the check, each alone. |
| Release | `make release`, `make release-major` | Gate (a major: `gate-major`), annotated tag, atomic push. |
| Optional | `make race`; `make bench` [ `BENCH=smoke\|baseline\|check` ] | Race run and benchmarks; not in the gate. |
| Off the menu | `go run ./scripts/release kind`, `… patch -dry-run`; `go test ./...` | Release kind and dry run; the long test tier. |

`scripts/release/` is the only code under `scripts/`. The gates rely on Go's test cache: no routine `-count=1`.

## Repository and branch

**Item owner:** one per item (003-aisoc assigns it); the owner edits every repo the path needs, each in that repo's
worktree with its gates. Consumers pin **`origin/main`**; local integration uses `go.dev.mod` (sigma: `make dev RHO=…`;
ai-soc: `make dev-pin RHO=… SIGMA=…`).

**Branches:** all day-to-day work on **`dev`**; **`main`** is the released line and moves only with a release.
Commit with explicit paths, never `git commit -a`; never force-push, never push `origin/v5`; the main session
commits; tag and push only when RH asks. Unfinished code is normal until the next release, but `dev` keeps building
(sigma and ai-soc can pin this checkout) and stays green: a red test lands on it only with its fix.

## Where to edit

`pkg/graph` (public) → `pkg/graph/internal/core` → `pkg/graph/store` (+ backends). `pkg/types`. Layers and lock order:
`docs/architecture.md` §§ 1, 4; API: `docs/api.md`. A change at core or store holds on every door and backend the
contract covers (Rules digest, Code).

## Resource limits

- **Allowance:** one per cut (Sigma's lease tree), drawn through rho's one ReadMeter by every read, across keys,
  shards, versions and pending writes; no read opens its own.
- **Charging:** before the work, in the helper that does it (walk, decode, clone, allocate, open); pass the tally
  down; examined and rejected entries count. Each cost has one owner; no mode flag moves the count to another layer.
  Work that cannot be charged is refused with a typed error before it starts.
- **Limit tests:** an exact, non-empty unlimited control plus a counter at the native boundary (iterator steps,
  decodes, clones, shard opens) that the meter under test does not keep; several keys and several shards that each
  fit the limit but exceed it together. These part tests come after the first test, which runs ai-soc's path and
  checks the limit where that path reaches it. A found failure gets a test at the helper that owns it and at the
  public caller.
- **Doors:** public variants of one query run through the same admitted walk; the `docs/api.md` table says which
  entry point bounds which dimension, backed by those tests. A streaming door may hand out a prefix before a
  refusal; a slice door returns all or the refusal.
- **Shard lifecycle:** handle states, who moves them and the lock order stay together in the sweep code; extend that
  one path, never add a second cleanup path; keep the pin, scope, late-write, failed-replacement, close-fault and
  reopen tests.
- **Reporting:** `BufferBytes` / `DecodedBytes` are logical counters, never live heap or RSS. Bytes divided by
  1 << 20 are MiB. Todo and evidence say what the code does **now**.