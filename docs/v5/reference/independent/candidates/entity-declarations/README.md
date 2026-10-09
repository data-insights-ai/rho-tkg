# Entity declaration preservation candidate

**REVIEW DATA; NOT APPLIED GO SOURCE.** The unchanged
[production patch](candidate-production.patch) and
[test patch](candidate-tests.patch) describe an isolated candidate based on
`c01279c46fff1dc0773fa3b72be572de2c9d725b`. Do not apply these patches blindly:
the full 42-row test adapter is intentionally red for 13 missing integrations on
that historical slice, and current owner tests/APIs now differ.

The candidate adds optional typed `Interpretation` and `TemporalRole` declaration
tokens to immutable `EntityRecord`. The role describes only its declared native
axis association, not every property or other axis. Create supplies metadata;
lifecycle correction/close/reopen preserves it. Non-create operations cannot
restate/replace it. Empty means undeclared; unknown well-formed tokens decline.
Known tokens are independent of Node/Relationship kind, Point/Span shape,
profile, labels, property names and equal support. No semantic equality,
evaluator, opaque extension or general metadata-correction API is claimed.

Accepted short tokens use `strings.Clone` at ownership boundaries. Each token
uses MaxNameBytes; provisional MaxInputMetadataBytes defaults to 1 MiB with a
64 MiB ceiling. This ledger covers these two input metadata fields only. Whenever
either is declared, conservative accounting charges both 4-byte length frames
and every token byte in input occurrences, actual entity reads, created Delta
records and primary projections. Reference dereferences charge complete entity
reads; dependency structs carry only IDs/version tokens. Two undeclared fields
have zero declaration framing, preserving the legacy accounting baseline.
This is logical record accounting, not measured Go struct/heap/RSS usage or a
selected stored encoding.

[Validation](entity-declarations-validation.json) keeps distinct evidence:

- The canonical absent-field baseline fails the three complete metadata goldens.
  A separate DTO-only stage fails 44 negative sensitivity leaf checks before
  validation/accounting is implemented. These are different red stages.
- The candidate passes the three unchanged full golden answers in
  [full-goldens.json](full-goldens.json), original 16 cases/52 assertions, all known
  token combinations, node/relationship parity, wrong-field/malformed/provider
  refusals and exact-fit/one-byte budget boundaries with no partial outputs.
- The unchanged independent model supplies
  [32 complete lifecycle answers](declaration-lifecycle-v1.json) over creation,
  close, correction, reopen, boundaries and retained old views.
- Worker component normal/race/vet pass. Graphstate coverage is 86.6%; the four
  declaration helpers are 100%; changed-line overlapping coverage blocks are
  96.72% (an approximation). All 102 archived baseline files match Git blobs.
- The parent separately reviewed the patches/plumbing and passed focused
  `go test -race -count=1 -run '^TestDeclaration' ./internal/graphstate` in the
  external candidate, 1.908 s. Its current production-patch apply-check also
  passes; this is not canonical application.

The historical full-corpus accounting **25 supported / 13 missing / 4 model**
belongs to the old candidate slice. The owner subsequently committed unit APIs
at `22f80a4`; those counts do not describe the newest APIs. Source drift is
recorded separately, including later owner changes and uncommitted graphstore work.

**Graphstore codec and assembly integration remains required.** The two DTO fields
alone do not establish encoding/decoding, validation/budgeting, retained durable
history, correction/reopen or crash-recovery preservation. The uncommitted
canonical graphstore under construction was not verified by this candidate.
The prototype metadata fields do not select a persistent compact layout or
complete V1/production graph acceptance.

Run the portable extraction/hash check from any working directory:

```sh
python3 -B docs/v5/reference/independent/candidates/entity-declarations/check.py
```

This reads package data and the unchanged canonical corpus; it writes nothing.
The Go patches carry module-relative testdata and need no Python runtime. They
remain review artifacts: preserve the explicit 13-row failing historical ledger,
and adapt reviewed tests deliberately when integrating with current owner code.
