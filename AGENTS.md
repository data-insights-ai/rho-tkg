# AGENTS.md

Never create a new .md file: the documentation set and which file owns what is the table in README.md § Documentation.

rho-tkg (`tkg/v4`) is a pure Go temporal knowledge-graph library. This file is the entry point for Codex, Claude Code and other
agents; it holds the documentation rule, the hierarchies below and the development loop.

**Scripts.** Makefile + `go test` only. **`scripts/release/`** only: kind, tag, push, and optional **`presubmit`**
(`make presubmit` → `go run ./scripts/release presubmit`). Race, gate-major, fuzz and measure are **Makefile** targets;
**`bench/bench-compare.sh`** is the bench comparator. No shell under `scripts/`.

Read before any work: run **`make`** (default goal prints the Makefile menu) → `docs/CONTRIBUTING.md` § Rules digest →
`tasks/todo.md` (Now, Next, Waiting). Optional local
`HANDOVER.md` at the repo root is gitignored operator state (not in git). Read `docs/architecture.md`, a lesson or another
doc section when the task or the code you touch needs it, and
`docs/badger.md` before changing code that reads or writes badger.

**Repository hierarchy (fix order).** rho-tkg is the embeddable graph library. sigma-tkgd builds query and Tyla on rho.
ai-soc embeds rho and sigma at released pins in its own module. A defect or contract gap is fixed in the repo that
owns it: rho first, then sigma, then ai-soc; consumers test local rho with `go.dev.mod` (sigma: `make dev RHO=…`;
ai-soc: `make dev-pin RHO=… SIGMA=…`). Do not change sigma or ai-soc from a rho session unless the todo names it.

**Branch hierarchy.** Work on `dev` (the working branch). `main` / `origin/main` is the released line after a tag;
unreleased work stays on `dev`. Do not commit or push to `main` by hand for day-to-day work.

**Git: DEV vs PROD (rho).** Branch for all work: `dev`. `main` / `origin/main` is what consumers pin after a release.
No hook runs gate or presubmit. Agents **commit**, **tag** or **push** only when René asks; never `git commit -a`
(explicit paths; `docs/CONTRIBUTING.md` § Rules digest).

**DEV** — share unreleased work on the working branch (not a version release).

| Step | What |
|---|---|
| **Commit** | On `dev` (or item branch → `dev`): code, tests, docs for the item; `go test -run …` / `make old` in-session. Todo-only commits follow Rules digest. |
| **Tag** | **No** version tag. Dev pushes do not create `v*` tags and do not move `main`. |
| **Push** | After green **`make gate`**: `git fetch origin`, then `git push origin dev` by hand. Gate green does not push. |

**PROD** — version release (René calls one only).

| Step | What |
|---|---|
| **Commit** | On `dev`: finished CHANGELOG / README / `tasks/todo.md` for the release batch; clean tree (`git status`); optional `make dry-patch` preflight. |
| **Tag** | **`make release`** / `release-minor` / `release-major`: `scripts/release` runs gate (or major gate), then `git tag -a` for the next `v*`. |
| **Push** | Same command: atomic **`git push`** of **`main`**, **`dev`**, and the **tag** (`scripts/release`). Detached **`make race`** after patch/minor; one line to sigma and ai-soc. GitHub **ci.yml** on **`v*` tag push**. Checklist: `docs/CONTRIBUTING.md` § Release checklist. |

**Check hierarchy (rho).** Narrow checks while editing; wide checks before push or release. Nothing in this list runs
presubmit or gate automatically except when you or René invoke it.

| Stage | Command | Role |
|---|---|---|
| Edit | `make test-one PKG=./pkg/… RUN=Test…` or `go test -run '…' …` | One package or test; PKG required on the Make target (fails fast if missing). |
| Wider dev | `make old` | Two stable integration tests on the store surface (seconds cached); default after new tests. |
| Optional | `make presubmit` | `go run ./scripts/release presubmit` — vet + short on packages since `origin/main`; minutes on fat `dev` |
| Pre-push (DEV) | `make gate` | Before **DEV push** — `build`, Docker `lint`, full `go test -short ./...` |
| Release (PROD) | `make dry-patch` / `make release` | Before **PROD tag+push** (René only); not daily dev |

`make test` is the same short tier as gate's test step; use by hand when you want the whole tree without lint or build.
By-hand only, never before a tag: `go test ./...` (long tier), `make race`. Dev and gate use the Makefile and `go test`
only. Go's test cache reruns only what changed, so never `-count=1`; only a major release runs the cold comprehensive gate.

**Code hierarchy (where to edit).** Public callers use `pkg/graph` (`graph.Graph` and its sub-APIs). Shared behaviour and
temporal semantics live in `pkg/graph/internal/core` (not imported outside rho). Persistence is `pkg/graph/store` and
backends (`memory`, `badger`, `tiered`, `sharded`). Cross-cutting types: `pkg/types`; wire and helpers: `pkg/graph/io`, `internal/`.
A change at core or store must stay consistent across every door and backend the contract covers; the layer diagram and
lock order are `docs/architecture.md` §§ 1 and 4. API names and stability: `docs/api.md`.

Development loop (René, 13 Nov): fix the gap in the owning repo. Each step: a few new tests for the new code, then
`make old` or one or two named old tests (`go test -run 'TestA|TestB' ./pkg/x`), and `make presubmit` only at item
handoff when the diff since `origin/main` is still small. Unfinished code is normal until the next release, but keep
`dev` building: sigma and ai-soc can pin this checkout (`make dev-pin RHO=..`).

Resource limits (René's review of 14 Nov, ai-soc `doc/REVIEW-rh-rho.md` §9). A read limit is one allowance for the whole
read: across keys, shards, versions and pending writes. Charge work before doing it, in the helper that does it (walk,
decode, clone, allocate, open): pass the tally down; examined and rejected entries count. Each cost has one owner; no mode
flag that moves the count to another layer. Work that cannot be charged is refused with a typed error before it starts.
Every limit test pairs an exact, non-empty unlimited control with a counter at the native boundary (iterator steps,
decodes, clones, shard opens) that the meter under test does not keep, and covers several keys and several shards that each
fit the limit but exceed it together. An item is "developed" only when such a test covers the whole contract it claims.
Public variants of one query run through the same admitted walk; docs/api.md keeps a short table of which entry points
bound which dimensions, backed by those tests. A streaming door may hand out a prefix before a refusal; a door that returns
a slice returns all or the refusal. A found failure gets a test at the helper that owns it and at the public caller.
Logical counters (BufferBytes, DecodedBytes) are not live heap or RSS; never report one as the other.
Shard lifecycle: keep the handle states, who moves them and the lock order together in the sweep code; extend that one path,
never add a second cleanup path; keep the pin, scope, late-write, failed-replacement, close-fault and reopen tests.
Byte figures divided by 1 << 20 are MiB. Status text (todo, evidence) says what the code does now, not what it did.
