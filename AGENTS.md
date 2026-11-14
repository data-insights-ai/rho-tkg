# AGENTS.md

Never create a new `.md` file — [README.md § Documentation](README.md#documentation).

Entry point for agents on **rho-tkg** (`tkg/v4`). Code and test rules: `docs/CONTRIBUTING.md` § Rules digest (every session).
Work order: `tasks/todo.md`. Design:
`docs/architecture.md`; API: `docs/api.md`. Read `docs/badger.md` before changing code that reads or writes badger.

## Agent procedure (every task)

1. Run **`make`** first (the menu). Use Make targets, not raw `go` or Docker, wherever one exists.
2. **Todo first:** the item's row in `tasks/todo.md`, before any other action.
3. **Red test, then the fix:** `make test-one PKG=./pkg/… RUN=Test…`, then `make old`.
4. **Review** the full diff before the check (`docs/CONTRIBUTING.md` § Review).
5. **Finished or handing off:** **`make check`** green. On a red leg, fix against the narrowest command: a red test
   with `make test-one PKG=<failing package> RUN=<failing test>`, a red build or lint with `make build` or
   `make lint`. Then `make check` once more.
6. **Item end:** the item leaves `tasks/todo.md`; its entry is in `docs/CHANGELOG.md` under `[Unreleased]`. The todo
   is committed once per item end, with the item's commits (alone only at a session handover), after
   `make test-one PKG=./internal/docscheck/` (`TestLedger`; `TestDocSizes` caps the todo).
7. **Push `dev`:** only when RH asks, after a green check: `git fetch origin`, then `git push origin dev`. No tag;
   `main` does not move. A green check never pushes by itself.
8. **Release:** only when RH asks: `make release` (a major: `make release-major`). It gates, tags and pushes `main`,
   `dev` and the tag atomically, then starts `make race` detached. Before it: CHANGELOG, README and todo committed,
   clean tree (`docs/CONTRIBUTING.md` § Release checklist).

## Who works

**Codex:** the main session designs, implements, documents, reviews and validates the work directly. Do not
delegate or start Claude processes unless RH explicitly asks. The procedure and code rules still apply.

**Claude sessions only:** every item and fix round follows **Fable design** (read-only, before code) → **Opus build**
(steps 3 to 5, from the frozen design) → **Fable review** (read-only, before merge) → another round for findings →
merge. Claude's main session briefs the agents, verifies findings against source, runs tests, lint and gates, and
merges; it does not design or write item code. Sonnet handles docs and mechanical work. Other agents only on RH's word.

One session per checkout. At an item end after about three compactions: update local `HANDOVER.md` if used,
tell RH, fresh session.

## Standing orders

The only place for them: no standing order lives in `tasks/todo.md` or `docs/CONTRIBUTING.md`.

- **Goal:** realtime ingest at about 2 TB a day (≈23 MB/s, 4 billion 500-byte facts), grown only by
  more RAM, NVMe and block storage. Nothing may grow in RAM per entity, or in open, recovery, Clear or compaction
  time per store, without a bound. Removing such a wall ranks above a constant-factor win (todo SCALE).
- **Scope:** consumer-blocking or wrong-answer/data bugs, plus ai-soc runtime items. Anything else needs a consumer
  request or a failing test, or goes to `tasks/todo.md` § Waiting.
- **`tasks/todo.md` is always up to date:** it is the one authority for what is open. Update it in the same step
  as the work: a new item gets its row before the work starts, a changed state is written when it changes, a
  finished item leaves. A row is one sentence, then when it is done and what it waits for; **rho/sigma** says what
  the item brings to the rho + sigma stack (memory, CPU, storage, restart time) or "none". An open decision of
  RH's carries a default.
- **Releases:** every release is a **PATCH** (a new door, option, store format marker, badger minor bump or raised
  go directive included); only a removed or changed public name is a **MAJOR**. Budget: gate warm ≤ 60 s, cold after
  a Go or dependency bump ≤ 5 min, whole release warm ≤ 2 min; over budget is a todo row the same day. Race, long
  and fuzz never run on a patch tag's path.
- **No attribution:** commits, MRs and PRs carry no Co-Authored-By, Claude-Session or "Generated with" line.
- **Peers:** one message per item each way to sigma-tkgd and ai-soc, ≲1,500 characters, detail in a file; status in
  the todo (`item | state | next | waits for`), checked against the repo before it is stated.
- **Reopen check:** before a commit that changes what sigma or ai-soc read at reopen, run a small reopen cycle with
  the host's writer, on masked rows only (never a Flux file). Sample: ai-soc `TestRestartModesOverBARows`.
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
| Edit | `make test-one PKG=…` [ `RUN=…` ] | One package or test, `-short`; `PKG` required. |
| After new tests | `make old` | Two stable integration checks on the store surface. |
| Before sharing | `make check` (= `make gate`) | `build` + Docker `lint` + `go test -short ./...`; lists every red leg. |
| One gate leg | `make build`, `make lint`, `make test` | The three legs of the check, each alone. |
| Release | `make release`, `make release-major` | Gate (a major: `gate-major`), annotated tag, atomic push. |
| Optional | `make race`; `make bench` [ `BENCH=smoke\|baseline\|check` ] | Race run and benchmarks; not in the gate. |
| Off the menu | `go run ./scripts/release kind`, `… patch -dry-run`; `go test ./...` | Release kind and dry run; the long test tier. |

`scripts/release/` is the only code under `scripts/`. The gates rely on Go's test cache: no routine `-count=1`.

## Repository and branch

**Fix order:** rho → sigma → ai-soc. Cross-repo edits only if `tasks/todo.md` names them. Consumers pin
**`origin/main`**; local integration uses `go.dev.mod` (sigma: `make dev RHO=…`; ai-soc: `make dev-pin RHO=… SIGMA=…`).

**Branches:** all day-to-day work on **`dev`**; **`main`** is the released line and moves only with a release.
Commit with explicit paths, never `git commit -a`; never force-push, never push `origin/v5`; commit and tag, like
push, only when RH asks. Unfinished code is normal until the next release, but `dev` keeps building (sigma and
ai-soc can pin this checkout) and stays green: a red test lands on it only with its fix.

## Where to edit

`pkg/graph` (public) → `pkg/graph/internal/core` → `pkg/graph/store` (+ backends). `pkg/types`. Layers and lock order:
`docs/architecture.md` §§ 1, 4; API: `docs/api.md`. A change at core or store holds on every door and backend the
contract covers (Rules digest, Code).

## Resource limits

- **Allowance:** one per read, across keys, shards, versions and pending writes.
- **Charging:** before the work, in the helper that does it (walk, decode, clone, allocate, open); pass the tally
  down; examined and rejected entries count. Each cost has one owner; no mode flag moves the count to another layer.
  Work that cannot be charged is refused with a typed error before it starts.
- **Limit tests:** an exact, non-empty unlimited control plus a counter at the native boundary (iterator steps,
  decodes, clones, shard opens) that the meter under test does not keep; several keys and several shards that each
  fit the limit but exceed it together. An item is "developed" only when such a test covers the whole contract it
  claims. A found failure gets a test at the helper that owns it and at the public caller.
- **Doors:** public variants of one query run through the same admitted walk; the `docs/api.md` table says which
  entry point bounds which dimension, backed by those tests. A streaming door may hand out a prefix before a
  refusal; a slice door returns all or the refusal.
- **Shard lifecycle:** handle states, who moves them and the lock order stay together in the sweep code; extend that
  one path, never add a second cleanup path; keep the pin, scope, late-write, failed-replacement, close-fault and
  reopen tests.
- **Reporting:** `BufferBytes` / `DecodedBytes` are logical counters, never live heap or RSS. Bytes divided by
  1 << 20 are MiB. Todo and evidence say what the code does **now**.
