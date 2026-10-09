# Independent acceptance references

These reviewed Python standard-library models import neither production Go code
nor the original oracle. The unchanged historical fixture remains
[`../temporal-fixtures.json`](../temporal-fixtures.json). The current contract is
[`../../PLAN.md`](../../PLAN.md); phase status is exclusively
[`tasks/backlog.md`](../../../../tasks/backlog.md).

| Package | Representation and executable coverage | Boundary |
|---|---|---|
| `temporal/` | Exact Fraction/integer/lexicographic Q×N coordinates; closed integer endpoints; immutable region-per-full-Cell snapshots. 20 tests, 42 revised records, 18,192 interval pairs, 600 component mutations, historical 16 cases/52 assertions. | Mathematical snapshots are not certified database cuts. Solver, calendar, join, importer and transaction-dependency obligations remain explicitly declarative or preservation-only. |
| `protocol/` | Independent serial-order search over complete transaction read footprints plus separate declared protocol-evidence audit. 18 tests; 18 named positive histories and 70 interleavings; 39 expected refusals. | Bounded declared histories trust complete capture and replica receipts. No consensus, fsync, real message delivery, process-kill or progress proof. Active-intent transfer explicitly declines. |

Revised corpus revision 2 isolates the missing-target atomic-failure case with
a fully live source B, retaining expected `NOT_FOUND` and testing that tentative
relationship edits change neither current nor earlier snapshots. Revision 3
adds strict life-bound presence corrections checked against the originally bound
endpoint LifeIDs; declared property edits while endpoints are masked remain valid.
The original 16/52 fixture and historical parity bytes remain unchanged;
`provenance.json` retains original records and adds exact correction lineage.
Original source authorship remains unattributed; content hashes do not establish it.

The temporal model preserves null/absence/retraction, revisions, provenance,
independent lives, declared/effective projection, endpoint binding and exact
units. Historical integer UTC microsecond test coordinates remain unchanged;
explicit conversion to milliseconds divides by 1,000 and may refuse fractional
IntegerZ/int64-codec results. Dense membership witnesses cover all endpoint cells
in this finite corpus; this is not a proof over unbounded inputs.

Protocol histories cover durable decisions versus application, unresolved older
prepares, private abort effects, absent-key/range conflicts, revision ABA,
recovery inventories for every durable COMMIT/ABORT regardless of client ack,
ID reservation, request replay, quiescent ownership moves and retained old cuts.
Local commits check the complete key/range read and write routing, logical-bucket
epochs, historical revalidation and outstanding prepared read/mutation locks.
Protocol keys use the declared `bucket/...` namespaces. Lexical ranges `[lo,hi)`
route through every intersected declared prefix interval `[bucket+"/",bucket+"0")`,
including buckets without matching stored keys; the lower endpoint alone is
insufficient. Adapters must supply the complete declared namespace inventory.
Fresh collection includes unacknowledged durable decisions
and coordinator authorities outside read scope, then requires a new barrier.
Transport labels imply no durable transition. Missing events do not prove absent
prepares; completeness declarations require authoritative capture from adapters.
Old cuts use retained original-owner lineage here; serving them through a new
owner needs an unimplemented restoration mapping. These capability limits do not
change the production contract.

`Limits` and JSON byte ledgers describe an oracle envelope, including names,
axes, payloads, revision/provenance and fragment counts. They are separate from
production wire bytes, Go heap/RSS/page-cache budgets, accepted deployment
capacity and performance policy. All JSON schema/version fields are provisional
reference artifacts, not production serialization formats.

Run from any working directory (the command path below assumes repository root):

```sh
python3 -B docs/v5/reference/independent/check.py
```

This runs both unit suites, checks temporal freshness, generates each corpus twice
in isolated directories and compares bytes, exercises all 127 protocol CLI
outcomes (88 exit 0; 39 expected exit 1), scrubs developer absolute paths and
checks hashes against `validation.json`. It leaves checked-in files untouched.
The recorded Python version identifies the validation interpreter; another
Python version may validate without rewriting that historical fact.

For an intentional reviewed regeneration, run in the respective directory:

```sh
# temporal/
python3 -B -m unittest -v
python3 -B generate_corpus.py
python3 -B generate_corpus.py --check
# Optional: --fixture <explicit-fixture-path>; default resolves relative to this file.
# protocol/
python3 -B -m unittest -v
python3 -B corpus.py
python3 -B oracle.py histories/E17-stable-bucket-A-moves-to-owner-B.json
python3 -B oracle.py histories/unsupported-active-intent-transfer.json
```

The last command deliberately exits 1 with
`unsupported_active_intent_transfer`. After reviewing source/corpus changes,
refresh evidence with `python3 -B check.py --write-validation` from this directory.
Returned protocol violations are sorted by canonical JSON content for deterministic
artifacts; checks and expected outcomes are unchanged.
`provenance.json` records pre-integration source SHA-256s; `validation.json`
records current sources/artifacts, PLAN/fixture/substrate hashes, counts and
actual expected CLI exits without raw machine logs.

Graph differential mapping does not yet cover whole-set Unset, edge-label
operations, arbitrary Go property kinds or typed assertion interpretation/role
fields. Opaque JSON descriptors establish this model's preservation only.
Graph steps record unqualified change counts, not complete component-qualified
CDC envelopes; region and atom groupings can differ without semantic divergence.
Adapters must declare these limits and refuse unsupported operation/property/
metadata mappings explicitly; success must not silently omit them or drop fields
to manufacture parity.

[`regressions/`](regressions/README.md) contains reviewed portable ID-checkpoint
Host read-identity and graph scalar-axis accounting patch data for the canonical
owner, plus compact source validation. These three conditions are resolved in
committed implementations at `e5712e1651ee22ca142b5978d400649d8e0f5744`,
with parent package-race/vet verification. Candidate implementations differ and
must not be applied again. Broader integration and full CI/phase gates remain
pending; see the compact regression integration evidence. A fourth reviewed
replica-input admission patch remains pending canonical integration, with
original-baseline and drifted-stock evidence recorded separately.

The linked integration record retains its earlier package-only verification.
Separately, the canonical owner independently reproduced the three resolved
conditions, reimplemented their Go fixes at the same commit and reported passing
full v5 race, build, vet, coverage (90.0%), lint, security and vulnerability gates.
The supplied external patches were not applied. Their patch data, original
validation and 41-case mapping remain historical evidence; these checks do not
complete broader repository CI, consumer or distributed phase acceptance.

Canonical Go differential, graph/API, durable fault and consumer integration
remain pending. These model results alone do not complete V0, V1 or V2. Keep
current acceptance and remaining work in the backlog only.
