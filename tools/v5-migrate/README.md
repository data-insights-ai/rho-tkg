# v4 retained-state migration preflight

This separate Go1.26.9 module provides usable `PreflightNode` and `PreflightRel`
readers over an already-open v4 graph. It maps retained effective state at an
explicit source knowledge-time pin to existing v5 Q/ms scopes. The local module
replacements select this repository's current v4 reader and v5 public values;
they do not select an old consumer fixture or an unavailable destination engine.

The original local qualification is pinned to repository base
`f414733828f1e62fe211f0ff2900aa680c0a62b7` and the v4.50.1 reader baseline
`91684e54d25fd8c1b136f5f80bd8f33f5d0ab3bd` (release code `efbedc28`). This is
a local-source qualification, rather than a shipped-binary reproduction; the
selected module dependency graph includes protobuf v1.36.11 from v5. Source/input
fingerprints, actual tool builds, coverage and narrow gate logs are recorded in
[qualification.json](evidence/qualification.json). The subsequent
[hosted qualification at `3ede096d`](evidence/hosted-main451-3ede096d.json)
passes all narrow gates against the integrated v4.51.0 reader, including race,
96.2% coverage and pinned scanners on Go 1.26.9 Linux/amd64. Neither record
completes V6; the earlier source pins and local resource measurements remain
historical. The integrated v4.51.1 reader at `dbd74cf45c65c1086798012e92453315cab75143`
is locally requalified by the new
[ten-gate receipt at `993e89a6`](evidence/main4511-993e89a6.json) and its compact
[raw evidence archive](evidence/main4511-993e89a6.tar.gz). All ten gates passed
serially with Go 1.26.9 darwin/arm64 and independently checked pinned scanner
builds, each bounded to 180 seconds and sampled process-tree RSS of 4096 MiB.
Observed coverage is 96.2% overall and 100% for both preflight functions. That qualification
required v4.51.1; its local replacements select the source-equivalent
integrated root recorded in that receipt. Archived qualification pins remain
unchanged. This narrow local result does not qualify whole-root/hosted gates,
original history completeness, destination import or full V6.

The historical v4.51.4/main `a1248ffc` reader is locally requalified at integrated
root `9e805ddc` by the [ten-gate receipt](evidence/main4514-9e805ddc.json) and
[raw archive](evidence/main4514-9e805ddc.tar.gz). All ten existing module gates
passed serially with Go 1.26.9 darwin/arm64, owned pinned scanner builds, and the
same 180-second/4096-MiB sampled process-tree bounds. Existing nonempty node/rel
preflight cases ran; coverage remains 96.2% overall and 100% for both public
preflight functions. This qualifies the local reader mapping only, without
whole-root or V6 acceptance. Dated source checkpoints and historical observations
remain unchanged. Live read snapshots/open images are source accelerators, never
an import cut or proof of missing history.

The historical v4.51.5/main `c23c8b04` reader is narrowly requalified at integrated
root `da5d32e6` by the [ten-gate receipt](evidence/main4515-da5d32e6.json) and
[raw archive](evidence/main4515-da5d32e6.tar.gz). All ten existing module gates
passed with Go 1.26.9 darwin/arm64 and verified reused pinned scanners, bounded
serially to 180 seconds/4096 MiB per command. Existing nonempty node/relationship
cases ran; coverage is 96.2% overall and 100% for both preflight functions.
An initial cache-only tools attempt refused Go's metadata lookup; its failure is
retained, then only the four remaining targets ran in the original environment.
All frozen source and scanner hashes stayed unchanged. The historical v4.51.4
receipt above and [finite reader checks](../../tasks/evidence/current-reader-9e805ddc-20261014/receipt.json)
remain pinned to `9e805ddc`. This qualifies the existing per-entity mapping cases,
without whole-root or V6 acceptance; current-source consumer obligations remain open.
Relationship retention now refuses affected old pins; preflight does not capture
global source watermark families or perform import. Per-type retention, state
column cuts and zero temporal ordinals have explicit consumer obligations in
[PLAN §8a](../../docs/v5/PLAN.md); full V6 remains open.

The selected v4.51.7/main `9e9f0e0a` reader is narrowly qualified at integrated
root `0eb0a0ac` by the [new ten-target receipt](evidence/main4517-0eb0a0ac.json)
and [raw archive](evidence/main4517-0eb0a0ac.tar.gz). All ten targets passed once
with absolute Go1.26.9, pinned scanner versions/builds and unchanged cached
binary bytes. All 2250 frozen Go/module/Makefile/lint inputs remained unchanged;
coverage is 96.2% overall and 100% for both preflight functions. A startup
`/var/tmp` versus `/private/var/tmp` provenance check refused before any target
began; the retained correction compares canonical file identity. Each target
stayed within 180 seconds and sampled process-tree RSS of 4096 MiB; every observed
owned identity is absent at terminal cleanup. The heavy lease is released.

v4.51.6/main `a77b1301` had source-only reconciliation, and historical v4.51.5
qualification remains immutable. PLAN §8a records relationship rebuild/re-create
and node-envelope/derived disk-row obligations. These unchanged preflight readers
use per-entity effective timelines, raw history/current rows and chain verification;
they neither create nor prune temporal indexes. Indexed selector/build, retention
refusal, consumer and destination-import proofs remain separate. Newer main
`bf0050b2`/v4.51.8 was observed while this qualification ran; it is outside this
exact v4.51.7 source cutoff and remains unqualified by this receipt.

The caller quiesces source writes, imports, retention and DDL across a call,
supplies capture/reader identity and evidence, and designates an explicit v5
graph/default-axis binding. Each report owns frozen raw source rows, resolved
registry names, original IDs/endpoints, concrete property kinds, raw timestamps,
audit/signature/hash fields and retraction markers. Raw evidence includes retained
rows after the requested pin. Separate reader-normalized segment rows and bounds
come from v4 `NodeEffectiveTimeline` / `RelEffectiveTimeline`, preserving the state
door rather than current Get, the newest-record AsOf door or sorted history.
Relationship state is the declared view; endpoint-life masking remains explicit.

Integral Unix milliseconds map exactly to `t/1` on the supplied Q/ms/POSIX axis.
Lower bounds stay inclusive, finite upper bounds exclusive, and source open
`ValidTo == 0` becomes explicit positive infinity. Original raw zero valid-from
and the source ID's actual `MintInstant()` remain distinct evidence; the timeline
reader applies source fallback and later-version tiling before scope mapping.
No global transaction order, certified cut, event or point is inferred.

A genuine source one-tick row is admitted only with a caller-verified
`SpanAttestation` binding source ID, node/relationship namespace, ID, version,
hash/previous hash and complete raw temporal metadata, plus writer/format/evidence
identity and explicit exclusion of sentinel encoding. This package checks that
binding; external evidence authentication and audit are the caller's duty.
Release numbers, export dates and recent history starts do not suffice.

Unknown one-tick provenance, collided versions, missing genesis/version gaps,
inverted transaction stamps or failed source-chain verification return the raw
report and `ErrAmbiguousHistory`. All scopes in a refused report are invalid;
raw evidence and the oracle's segment bounds remain reviewable. Other admission
and source errors return a zero report and preserve sentinel matching with
`errors.Is`. Retractions stay belief retractions, separately from validity closes.

A clean result establishes the requested retained-state mapping only. v4 can
erase history through in-place edits, old re-imports or retention; even its clean
hash verifier tolerates a missing lowest predecessor. This component neither
certifies original history completeness nor repairs missing lives. It materializes
one entity's source history; it does not bound upstream source memory/work. It
defines no archive/storage format or destination writer and performs no import,
restore, cut installation, durable commit or consumer adaptation. Full V6 and
later-phase acceptance remain governed by `tasks/backlog.md` and PLAN §9.

Run this module's normal, race, vet and coverage gates with the exact compiler:

```sh
make -C tools/v5-migrate ci GO=/path/to/go1.26.9
```

The module Makefile applies `GOWORK=off`, `GOTOOLCHAIN=local`, `GOMAXPROCS=2`,
`GOMEMLIMIT=2GiB` and `GOFLAGS=-p=1`. The coordinating caller must also apply the
shared process-tree limiter and sole heavy-runner lease. Root aggregate gates
dispatch this module independently; CI uses a separate `migration-preflight`
job. `MIGRATION_DISPATCH=0` disables aggregate dispatch when a coordinating
runner checks modules separately. Scanner versions match the root Makefile;
`make tools` installs those explicit versions, and compiler-matched Docker
targets are available when host scanners are absent.
