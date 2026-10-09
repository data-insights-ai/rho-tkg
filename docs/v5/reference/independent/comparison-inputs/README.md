# Portable basic graph comparison inputs

This external prerequisite supplies deterministic synthetic inputs, workload
changes, exact reference answers and strict export comparison. It implements no
vendor adapter, benchmark runner, license setup or V7 acceptance. The reviewed
PLAN/comparison/module hashes are in `contract-pin.json`; native per-vendor
integer ranges, label mappings and durability remain unverified.

Run with Python's standard library and `-B`:

```sh
python3 -B generate.py --out data --nodes 32 --edges 64 --seed 0
python3 -B -m unittest -v
python3 -B check.py
```

Larger node/edge counts use the same streaming format and disk-backed oracle;
no fit-in-RAM or capacity meaning is inferred from a size. `--max-depth` is 1–4
and defaults to 2. `--max-output-rows` bounds each complete answer and defaults to
5,000,000. A refusal removes the unpublished temporary dataset; it never labels a
truncated answer successful. An existing output directory is never overwritten.
Source signal rows default to **unavailable** for this graph-only fixture.
`--source-signal-rows N` records an explicitly declared inventory annotation,
without any graph conversion or synthday/consumer-data equivalence.

## Mapping and equal inputs

`basic-graph-v1` external node/edge IDs are distinct text namespaces with fixed
16-digit hexadecimal ordinals. Counts and a uint64 seed determine the rows;
SplitMix64 constants and counter positions are fixed by the generator source
hash. No clock, random runtime hash or machine path enters canonical files.
Nodes preserve complete label sets; edges preserve direction, type, endpoints
and separate external identities. With at least two edges, the first two have
identical endpoints/type/common properties and differ only by ID. With at least
three, an explicit self-edge is present. The source does not assume a vendor
natively supports the required multiplicity or label mapping. Discriminators,
reverse edges or reification must be counted separately by later adapters.

The common scalar envelope supports `bool`, `i64`, `f64`, and UTF-8 `text`:

```json
{"type":"i64","value":"9007199254740993"}
{"type":"bool","value":false}
{"type":"f64","bits":"3ff8000000000000"}
{"type":"text","value":"café"}
```

Signed integers use canonical decimal text. Exact tagged native JSON integers
can be normalized, but floats, booleans, wrapped unsigned values, leading zeros
and out-of-int64 values cannot impersonate integers. Fixtures include both int64
ends, int32 boundaries, and adjacent values around ±2^53. SQLite binds identifiers
and keys as TEXT and uses biased fixed-width TEXT integer-order keys; it never
parses integer comparisons through REAL. Float bit tags are a lossless fixture
encoding of finite binary64 values. The **basic lane canonicalizes numeric ±0**;
signed-zero source-bit preservation is a separate feature. NaN/Inf are refused.
Present-null is not a basic common scalar. False, numeric zero, empty text and
an absent property retain their distinct meanings.

`nodes.jsonl`/`edges.jsonl` are revision-0 offline inputs. `changes.jsonl` supplies
ordered append, update, upsert and explicit edge/node deletions in stages 1–3.
Incident-edge deletions precede node deletion. External identity recreation and
edge endpoint/type rewrites are outside this workload. Basic queries at stage
r0/r1/r2/r3 run **before/after the corresponding mutation stage**, not as historical
queries against a current-only engine. Their complete outputs cover ID lookup,
label/type, typed equality and closed range, in/out adjacency, directed bounded
walks distinguished by complete edge-ID sequences, projected scan and full-row scan. Both node and edge IDs may repeat within a walk, including repeated self-edges,
through the declared maximum depth.
Every expected answer is explicit JSONL plus exact row count and SHA256.

`retained-revisions.jsonl` and its query belong to a separate retained-history
feature lane. Earlier full rows, property removals and entity tombstones remain
observable there. Current projection uses explicit present/absent columns;
never-present versus removed reasons require the retained change record.
`feature-cases.json` additionally distinguishes present-null/absent/removed and
source negative-zero bits. Those feature cases declare their mapping requirement;
they do not establish native support or a complete temporal/lives/provenance lane.
No current-only vendor result is counted equivalent to retained history.

## Answer validation and costs

The independent `oracle.py` imports no generator. It loads emitted rows and
applies events through a SQLite reducer, with per-event rollback, explicit
identity checks and exact query selection. It retains rows/revisions on disk,
uses a 4 MiB SQLite cache target and file-backed sorting, and streams answers.
The generator also streams rows/events; it keeps no per-record Python map for
the complete large graph. Small literal test fixtures independently assert the
oracle's values, exact ledgers and ten parallel/self-edge walks.

For a later adapter export, write metadata containing only mapping version,
canonical dataset checksum, query ID and lane, then compare the complete output:

```sh
python3 -B normalize.py --dataset data --query r3.edge.full \
  --actual adapter-output.jsonl --metadata adapter-output.meta.json
```

```json
{"mapping_version":"basic-graph-v1","dataset_sha256":"COPY canonical_input_sha256 FROM manifest.json","query_id":"r3.edge.full","lane":"basic-graph-current"}
```

The adapter must supply the declared row shape and truthful scalar type tags.
Normalization uses a disk-backed multiset sort; order differences are accepted,
but duplicate/omitted rows, edge collapse, precision/type loss, dropped false
values, extra/deleted phantom rows, wrong versions and missing presence markers
fail. All declared reference files, their complete row/byte ledgers, the canonical
input identity and query/answer metadata are checked first. Missing, changed or
extra reference artifacts refuse; the disclosed runtime oracle-cost record is
excluded. The supplied specification and manifest still require a trusted
reference pin; input identity alone does not authenticate a rewritten oracle. A normalization
pass establishes output equality only, not native support, index completeness,
transaction acknowledgement, fault durability or benchmark suitability.

`manifest.json` records exact current nodes/edges/assertions, label/type
memberships, parallel/self edges, current and retained property-entry/value-byte
counts by node/edge and scalar type, retained revisions/tombstones and deleted
identities at every stage. Property-value payload bytes are bool=1, i64=8,
f64=8, and exact UTF-8 text bytes. Keys/label/type UTF-8 bytes and serialized
artifact bytes are separate; this is not graph-engine storage accounting.
Vendor reified/reverse/auxiliary records and per-copy/replica totals are unknown.
`oracle-costs.json` records the observed final SQLite workspace bytes and version,
with peak/temporary costs explicitly unmeasured. This runtime cost record is
excluded from canonical input identity and attributed to generator/oracle work,
not to future engines. It is removed from engine denominators, never concealed.

`check.py` runs independent tests, regenerates the small canonical fixture twice,
checks bytes/checksums, and records compact validation without machine paths.
Code-mutant checks reject parallel-adjacency collapse, float-rounded int64 range
keys and omission of a present false value. Export fault tests additionally reject
wrong signed integers/types, omissions and deleted phantoms. This is bounded
reference evidence; no vendors, provider systems or private consumer inputs are
used, and no performance or full V0/V7 result is claimed.
