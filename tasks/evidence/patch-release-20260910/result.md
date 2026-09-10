# rho-tkg v4.35.1 qualification — 2026-09-10

Reviewed the complete working delta from fdddc3d, including group-commit and
vocabulary durability code, regression fixtures, security/toolchain updates and
retained replay evidence. The gzip CPU profile is measurement evidence, not a
build artifact. No remote push or corpus mutation is part of this release.

## Repairs found during review

* Backend BeginGroupCommit panics leaked batch/graph locks. Red-first tests now
  cover begin, end and double-panic cleanup and verify every lock is released.
* An invalid vocabulary suffix left earlier allocations without a checkpoint.
  Full name/capacity preflight rejects the declaration before any allocation;
  duplicate existing names consume no capacity. The regression failed first.
* Added direct persistence-panic cases for declared labels and relationship types.
* Corrected the durability contract: final grouped flush, configured SyncWrites,
  backend transaction-size limits and possible asynchronous/registry writes.
* Fixed two lint findings: temporary string allocation and an unused test helper.

## Fresh validation

| Gate | Result |
|---|---|
| make cover | PASS, full repository coverage 83.9% |
| make cover-gate | PASS, pkg coverage 85.4%, floor 80% |
| make test-race | PASS; final changed core additionally rerun with race/coverage |
| Final complete core package with race/coverage | PASS, 80.2% |
| fmt-check, vet, lint-docker, security-docker | PASS on final source |
| vulncheck-docker | PASS, no called vulnerabilities reported |
| check-metakv-reap | PASS |
| New BeginGroupCommit / EndGroupCommit | 100% / 100% |
| New persistence-panic helper / vocabulary preflight | 100% / 100% |
| predeclareVocabulary | 83.3% |

All process trees used explicit watchdog ceilings. Attached reports retain command,
limits, duration and result; full local logs were written under
`/tmp/library-patch-audit/`. Full race ran before the final vocabulary preflight;
the final complete core race/coverage and full pkg coverage include that change.

## Scope limits

The opt-in historical TKG_REPLAY_R4 probe asks for repeatable asynchronous failure
reads beyond the current consume-on-read API and remains a documented failing
hazard probe. It is not a passing regression claim. Sync submitters receive their
failure directly. This patch does not change that API or establish crash atomicity
for arbitrary Badger WriteBatch sizes. Prior throughput numbers remain historical
measurements, not remeasured release performance claims.
