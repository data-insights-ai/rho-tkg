# Paired fault checker source checkpoint

Base: `ded845311ae9c1dfd3fd90d7e3ad7d8ec96cd62b`. Contract commits:
`11210cc4`, `13b7f510`, `9a98cda3`. Only common contract/checker/tests/evidence
are owned here. No FDB/native Go source, external archive patch or heavy runtime
was changed or executed.

Five Python representation guard tests passed. `guards-and-coverage.json` records
stdlib trace coverage of unique AST statement-bearing lines. These inert metadata
guards are not a valid runtime trace, paired acceptance, or the new-code80% gate.
Subprocess CLI hits are excluded from that line count; branch coverage is unmeasured.

The checker implements six actual logical operations, seven exact component
changes, both endpoint sets, immutable lives, typed definition/reuse/provenance,
retained seed/ABA reads, original inputs/results, raw groups/history/current
inventory, explicit Pending versus old-complete, process event order/cohort,
pre-kill receipt binding and six post-reopen original retries. It reuses accepted
low-level lifecycle validators, never the exact-seven-operation analyze method.
Native opaque cut/outcome codecs remain qualified by the actual native exporter.

Required before acceptance:

- Load the actual native and FDB `graph-fault-local-v1` exports, qualify all positive
  paths and run the finite artifact tampering tests; correct DTO integration errors
  from those real inputs rather than synthesizing a positive trace.
- Bind native transparent process/wait/startup evidence to its exported event
  fields once the native DTO is available; currently native raw OS blobs are
  retained and exporter-qualified while generic event assertions are checked.
- Verify `controllerArtifacts`, `backendTraceArtifact` and `controllerCommandLedger`
  against supplied artifact roots. The sealed controller ledger must be an exact
  byte prefix of final `commands.jsonl`. This artifact-root lane is not implemented
  or run in this source checkpoint and is required for actual qualification.
- Reach at least80% new Python coverage with actual artifacts, including CLI error
  paths; report branch coverage if available. Source-only guards remain below80%.

The held-old checkpoint now explicitly precedes the earlier quorum/source-owner
kill as well as full-cohort stops; a direct negative guard covers this boundary.
No read owner is assumed to survive its actual process death.

Scope remains local functional comparison only: no multi-host/power-loss/full
fault matrix, cost/performance, generic backend or engine decision acceptance.
