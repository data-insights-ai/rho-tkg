# AGENTS.md

Never create a new .md file: the documentation set and which file owns what is the table in README.md § Documentation.

rho-tkg (`tkg/v4`) is a pure Go temporal knowledge-graph library. This file is the entry point for Codex, Claude Code and other
agents; it holds two rules: this one and the development loop below.

Read before any work: `HANDOVER.md` (state) → `docs/CONTRIBUTING.md` § Rules digest → `tasks/todo.md` (Now, Next,
Waiting). Read `docs/architecture.md`, a lesson or another doc section when the task or the code you touch needs it, and
`docs/badger.md` before changing code that reads or writes badger.

Development loop (René, 13 Nov, the same in rho, sigma and ai-soc): fix a gap in the repo that owns it (rho, then
sigma, then ai-soc). Each step: a few new tests for the new code, then 1-2 old tests (`go test -run 'TestA|TestB' ./pkg/x`,
`make old`). Go's test cache reruns only what changed, so never `-count=1`; only a release runs everything. Unfinished
code is normal until the next release, but keep main building: sigma and ai-soc build on this checkout
(`make dev-pin RHO=..`). This checkout stays on `dev`, the working branch; `main` is the released state and
equals `origin/main`. After a green `make gate`, push `dev` by hand (`git fetch origin`, then `git push origin dev`; no tag, `main`
untouched); `make release`, only when René calls one, merges `dev` into `main` and tags it.

Resource limits (René's review of 14 Nov, ai-soc `doc/REVIEW-rh-rho.md` §9). A read limit is one allowance for the whole
read: across keys, shards, versions and pending writes. Charge work before doing it, in the helper that does it (walk,
decode, clone, allocate, open): pass the tally down; examined and rejected entries count. Each cost has one owner; no mode
flag that moves the count to another layer. Work that cannot be charged is refused with a typed error before it starts.
Every limit test pairs an exact, non-empty unlimited control with a counter at the native boundary (iterator steps,
decodes, clones, shard opens) that the meter under test does not keep, and covers several keys and several shards that each
fit the limit but exceed it together. An item is "developed" only when such a test covers the whole contract it claims.
Byte figures divided by 1 << 20 are MiB.
