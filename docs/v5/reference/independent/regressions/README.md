# Reviewed regression patches

The ID receipt-floor, Host read-identity and graph scalar-axis metadata
conditions were resolved by committed implementations at
`e5712e1651ee22ca142b5978d400649d8e0f5744`. Parent verification of its 90-file
git archive passed package race tests and vet for idalloc, graphstate, txnproto
and replica with Go 1.26.9. See [integration evidence](canonical-integration.json).
This establishes those package checks, not full CI or phase completion. Broader
graph integration remains pending.

The three candidate patches below are historical reviewed alternatives. The
committed implementations differ; do not apply them again to integrated source.
These files are data only and no patch is applied by this directory.
The [index](regression-index.json) gives synthetic failure conditions, baseline
commits, SHA-256s, intended behavior and proof boundaries. Current phase status
remains exclusively in the repository backlog.

`idalloc-receipt-floor-fix.patch` changes three files: allocator state validation,
the reachable final-sequence test, and direct checkpoint regressions. Nonzero
receipts require first ID at least request sequence. Every earlier successful
request consumed at least one ID. Marshal/decode refuse impossible receipts;
valid transfers, final reservation, replay and no-wrap assertions remain.
The supplied isolated validation reports tests/race/vet passed and 99.3% coverage.
That coverage includes an additional independent suite not bundled in this patch;
it is not a prediction of canonical patch-only coverage. Baseline source hashes
were independently matched to commit `6ef233c19acb38d11a6cf82350f833208b345eaf`.
Remote grant delivery, quorum/applied-position binding, graph qualification and
multiple-owner issuance/session activation remain separate integration work.

`host-read-identity.patch` changes Host and adds self-contained regression tests.
It distinguishes repeated reads by session/sequence identity, with admission and
completion handles. A valid old read completes under its own ID; a newer read
cannot consume its barrier. Tests cover delayed actual follower responses,
leadership changes, repeated questions, bounded admission, sequence overflow,
restart, invalid calls and private-envelope guards. The supplied clean-patch
validation used an 85-file pinned module copy at commit
`42d00f47b26c5c95d2c91a0f8e14d6ef5817362c`, with original txnproto/replica suites
and the six-process harness. Tests/race/vet passed; coverage was 88.4%/87.8%.
The existing serial process helper still correlates by query; concurrent consumers
must route using admission/completion IDs. This is not full V2/V5 acceptance.

`graph-scalar-axis-footprint.patch` is the stable reviewed graph accounting
patch against source-tree SHA-256 pin `118b1a6513bdc646`, captured at HEAD
`6ef233c19acb38d11a6cf82350f833208b345eaf`. The pin is not a Git commit.
A scalar scope retains exactly 32,000 bytes in its AxisDefinition Reference;
each separately tested read/delta budget is 8,192 bytes. Refusal returns no
entities, lives, values, patches or dependencies and leaves the source view
unchanged. Clean stock plus bundled self-contained tests, race and vet passed
with Go 1.26.9; coverage was 84.0%. The prototype ValueRef declared size now
includes complete retained scalar metadata while equality-key bytes remain
unchanged. Persistent payload-handle compatibility needs canonical-owner review.
These conservative byte ledgers are not measured heap/RSS or capacity results.
Of the 41 revised model cases, 20 are supported, 17 lack required integration
and 4 are model-only/declarative. This patch does not complete that integration
gate. The original 16/52 corpus remains unchanged.

`replica-input-guards.patch` is a fourth frozen reviewed candidate, with canonical
integration pending. It adds semantic admission guards before malformed
append/proposal/snapshot or configuration input reaches RawNode. Legal nonzero
protobuf booleans and follower forwarding remain supported. Original pinned
tests, race and vet passed; replica/raftlog coverage was 89.5%/82.3%. The original
Driver hash was `afda86c69dc55e4191b6db5ebb36f02c25a72ba2668df1d266ed9fb181cc3aef`.
Current stock has an ApplicationMachine producer path and Driver hash
`9a01a9ac4e08d1ea447f3330fdc65ac6517814f3360358bef477cd23f9d4697a`;
read-only apply checking and scoped patched-stock tests passed with unchanged
stock source files. This artifact does not claim a full race/vet run against
that newer stock. Import-only cleanup preserved assertions and code bodies.
The proof is input-admission robustness only, not Byzantine safety, divergent
replica protection, V2 certification or production resource admission.

Each validation JSON retains source and patch hashes, exact reported outcomes,
coverage and limitations without machine paths, raw logs or source snapshots.
Host commands retain their package scope with a relative coverage destination;
ID commands are labelled reproduction commands because its source validation did
not preserve exact shell invocations. For historical reproduction only, use a separate matching baseline checkout.
The following read-only applicability commands describe those old baselines;
they are not instructions to reapply patches to current integrated source:

```sh
git apply --check docs/v5/reference/independent/regressions/idalloc-receipt-floor-fix.patch
git apply --check docs/v5/reference/independent/regressions/host-read-identity.patch
git apply --check docs/v5/reference/independent/regressions/graph-scalar-axis-footprint.patch
```

These commands depend on matching live source and do not apply changes. Refresh
and check the portable reference manifest through `../check.py`; that validates
artifact hashes and reference models, not the Go patch behavior.

Follow-up context, 2026-10-09: the linked integration record, patch-only coverage
figures and 41-case mapping above retain their original evidence scope. The
canonical owner separately reported passing full v5 race, build, vet, coverage
(90.0%), lint, security and vulnerability gates after independently reproducing
and reimplementing the three fixes at `e5712e1651ee22ca142b5978d400649d8e0f5744`.
The supplied external patches were not applied. The current independent model
corpus is revision 3 with 42 records; its larger count does not revise that
historical mapping or establish broader consumer/distributed phase acceptance.
