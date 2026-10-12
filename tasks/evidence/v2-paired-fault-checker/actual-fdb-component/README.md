# Actual FDB checker component qualification

The supplied actual FDB trace is2,984,168bytes, SHA256
`cde5dfee209c99c78de3cc43b31c78b7471dedc61f38b2ddeb98e7033cf8c14c`:
`/var/tmp/rho-v5-fdb-paired-fault-20261012/tools/v5-substrate/fdb/artifacts/rho-v5-fdb-1791784452-98317/paired-graph-fault-trace.json`.
Its artifact root is the same directory. The validator passes on the original
artifact and independently binds26 artifact files plus221 sealed command events.
`validation.json` explicitly reports `fullPairAccepted: false`.

The paired loader now reads at most8MiB plus one refusal byte. Exact empty,
8MiB and8MiB+1 boundaries are tested. The imported legacy lifecycle loader
retains its2MiB bound; its source hash is unchanged.

Twelve tests passed: seven representation/parser/OS-schema guards and five actual
FDB artifact/tampering tests. The latter check actual component observations,
pre-kill receipt provenance, hashes and path bounds, frozen backend content,
copied process/marker fields, exact sealed-ledger prefix, physical current/history,
retained ABA revisions, lifecycle/census/postings, retries and CLI refusal paths.
No positive native fault trace was synthesized.

Stdlib trace records503/561 unique AST statement-bearing lines (89.66%).
Subprocess CLI hits are excluded. Branch coverage is unmeasured because no branch
coverage tool is installed. `coverage.json` lists all58 uncovered lines grouped
by function. The 80% statement-line criterion passes for this affected tooling.

Native OS bindings follow the actual exporter source: member/PID/incarnation and
directory, SIGKILL wait status/reap, clean-exit status, and startup/recovery bytes.
Their direct tests are explicitly inert schema guards. Full native process
evidence has not yet been qualified against a generated native paired trace.
Native transparent entity/life/posting checks, opaque record/cut boundary,
Pending branch and positive pair comparison remain pending that actual trace.

This is a qualified partial checker component. Pair acceptance, native actual
artifact tampering and full cross-engine CLI acceptance are outstanding. No
Go/build/Docker/heavy runtime, production changes, external archive patch or push
was performed. The worktree remains on the original delegated base.

Run the actual component lane with:

```sh
python3 -B tools/v5-substrate/compare_graph_fault_test.py \
  --fdb-only /path/to/paired-graph-fault-trace.json \
  --fdb-artifact-root /path/to/artifacts
```
