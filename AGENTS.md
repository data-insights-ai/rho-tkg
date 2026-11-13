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
(`make dev-pin RHO=..`). Finished work goes to local main and, once `make gate` is green, to GitHub's `dev`
(`git push origin main:dev`, no tag). `origin/main` and a tag only when René calls a release.
