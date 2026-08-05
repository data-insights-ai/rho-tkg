# Contributing to tkg/v4

Thanks for looking at contributing. This is a pure Go library — no main
binary, no server, no query language — so most changes are either a fix in
`pkg/types` / `pkg/graph`, a new capability on a `Store` backend, or docs.

## Before you start

- Read `CLAUDE.md` (agent-facing instructions, but the architecture, testing,
  and invariant rules apply to every contributor, human or AI). If you use
  Codex, `AGENTS.md` mirrors the same session protocol.
- Read `CHANGELOG.md`'s `[Unreleased]` section and the last few dated
  releases — it is the source of truth for version history, not a changelog
  written after the fact.
- Read `tasks/lessons.md` — actionable rules distilled from real bugs found in
  this codebase. Most recurring mistakes are already documented there.

## Building and testing

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector — required for any change touching concurrency
make test-integration  # integration tests (long-running)
make cover          # coverage report -> coverage.html
make check          # pre-commit: vet + build + test
```

Run a single test:

```bash
go test -run TestFoo ./pkg/types/
```

### Lint, security, and vulnerability scans

`golangci-lint`, `gosec`, and `govulncheck` are usually **not installed
locally**. Don't skip these gates because the binary is missing — run them in
the go.mod-matching Docker toolchain image instead (cached named volumes, fast
after the first run):

```bash
make lint-docker        # golangci-lint v2 (reads .golangci.yml)
make security-docker    # gosec
make vulncheck-docker   # govulncheck
make ci-docker          # full gate: fmt-check + vet + lint-docker + build + test-race + security-docker + vulncheck-docker + cover-gate
```

The repo carries a small pre-existing baseline (a stdlib-only `govulncheck`
finding fixed in a later Go patch, a handful of `#nosec`-worthy `gosec` G115s,
and some existing `golangci-lint` findings). A non-empty gate is not
automatically a blocked PR — filter findings to the files your change actually
touched (`git diff --name-only`) before treating something as new.

## Testing Rules (hard requirements)

`CLAUDE.md`'s "Testing Rules" section lists 17 rules that exist because every
one of them was violated at least once in this codebase's history. The ones
that come up most often when reviewing a PR:

- **80%+ coverage, no exceptions.** Every new public method needs a direct
  test; indirect coverage through delegation does not count. Run `make cover`
  before calling a PR ready.
- **Two-phase tests for temporal/history-aware methods.** Any method that
  answers a question about the past (`*ValidAt`, `*AsOf`, `Verify*Chain`,
  `Snapshot`, `Diff`, `*At`, or any `*ByLabel`/`*ByType` accepting temporal
  `QueryOpts`) needs a test that creates state X at t0, mutates it, then
  queries at t0 and asserts the result is still X — not the post-mutation
  state.
- **Adversarial shape, not happy-path.** Multi-entity scenarios, exact-set
  assertions, explicit negative assertions ("must NOT contain Y").
- **Sentinel errors asserted with `errors.Is`**, at every call layer that
  wraps or forwards the error.
- **Node and Relationship test parity.** They are structural mirrors; a test
  added for one usually needs its twin for the other.
- **Fix in one Store implementation → check the others.** `memory.Store`,
  `badger.Store`, and `tiered.Store` must agree on the shared contract; when
  the change touches slot routing, also check EXPERIMENTAL `sharded.Store`.
  A fix that only lands in one backend is an incomplete fix, not a defect
  found and closed.

Follow the TDD workflow for any behavior change: write the test first, watch
it fail for the right reason, then implement the minimal change to pass.

## Lessons protocol

`tasks/lessons.md` is a short, curated list of patterns distilled from real
bugs — not a bug log. When you fix a defect that exposes a genuinely new,
reusable pattern (not already covered):

1. Find the next sequential number: `grep '^## B' tasks/lessons.md` (or
   `grep '^## [0-9]'`, depending on the file's current numbering scheme) and
   use the next integer.
2. Check there is no duplicate — same title or same underlying code pattern
   — before adding a new entry.
3. Keep the entry short: the failure shape, the fix, and the rule that
   prevents recurrence.

Do not add a lesson for a straightforward typo or a one-off mistake with no
reusable pattern, and do not touch `tasks/lessons.md` at all unless you
actually fixed a real defect.

## Documentation hygiene

- Update `CHANGELOG.md`, and any of `README.md` / `docs/architecture.md` /
  `docs/api.md` / `docs/SPEC.md` that your change affects, in the same PR as
  the code change — not as a follow-up.
- Add `CHANGELOG.md` entries under `[Unreleased]` (create that section above
  the latest release heading if it doesn't exist yet) unless you have been
  told which released version the change belongs to. **Never bump a version
  number yourself** — that is a separate release step.
- **PRs must keep the docs-consistency test green.**
  `pkg/graph/internal/core/docs_consistency_test.go` cross-checks the Go
  version (`go.mod`) and the current release version (`CHANGELOG.md`'s
  topmost `## [x.y.z]` heading) against fixed strings in `README.md`,
  `AGENTS.md`, and `docs/architecture.md`. If your change touches any of
  those files, run
  `go test -run TestDocsMetadataMatchesSourceOfTruth ./pkg/graph/internal/core/`
  before opening the PR.

## Pull request checklist

- [ ] `make check` passes (vet + build + test)
- [ ] `make test-race` passes for every package you touched
- [ ] `make cover` shows no new code below 80%, and no new public method at 0%
- [ ] `CHANGELOG.md` has an entry under `[Unreleased]` (or the version you
      were told to target)
- [ ] Docs affected by the change are updated in the same PR
- [ ] `tasks/lessons.md` has a new entry only if you fixed a real, reusable-pattern defect
- [ ] The docs-consistency test still passes

## Fuzzing (local only — not in CI)

The trust-boundary fuzz harnesses (`FuzzWireToNodeChecked`, `FuzzWireToRelChecked`,
`FuzzUnmarshalNodeWireWithKeys` in `pkg/graph/internal/storeutil`; `FuzzImport` in
`pkg/graph/internal/core`) run their seed corpora as ordinary tests in `make test`.
Deep fuzz sessions are a local, deliberate activity — run one when you change a
decode/import surface:

```bash
go test -fuzz=FuzzImport -fuzztime=30m -run='^$' ./pkg/graph/internal/core
```

Commit any new crasher the run minted (it lands under the package's
`testdata/fuzz/` directory) together with the fix.
