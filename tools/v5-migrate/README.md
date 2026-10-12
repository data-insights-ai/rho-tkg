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
requires fresh qualification; its complete narrow gates and source/input/scanner
record remain pending. The local replacement selects that current root reader
and the module now requires v4.51.1. This dependency pin prepares requalification;
it does not establish a passing gate. Archived qualification pins are unchanged.

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
