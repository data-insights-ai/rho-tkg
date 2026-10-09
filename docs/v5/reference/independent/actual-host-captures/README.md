# Reviewed actual Host capture bridge

Five synthetic-key histories captured through actual `txnproto.Host` APIs pass
unchanged `../protocol/oracle.py` mathematical `serial_witness` search in both
normal and race runs. Each run records 14 decisions (including aborts), 9 complete
certified snapshot observations and 5 complete final maps. No divergence was
reproduced in this bounded scalar slice. Whole V2 and production graph acceptance
remain open.

The cases exercise concurrent conflicting and independent transactions, absent
keys and empty-range phantom footprints, partial cross-partition installation,
old retained cuts after correction/deletion and simulated reopen, and coordinator
B authority obtained after an A-only collection. Every supplied generation and
named key version binds to a completed request-bound Host Current view. Export
validation independently checks submitted inputs, decisions and complete maps
against raw owned Host responses before mathematical serial-order search.
Ranges are selected by the review client from a complete Current map; the native
footprint is conservative partition Generation, not a native named range selector.
Native revisions remain raw evidence and are omitted from mathematical search.

## Run the packaged captures

The checker resolves its source, captures and canonical oracle by its own file
location. It verifies the oracle's pinned SHA-256 and runs from any working
directory. From repository root:

```sh
python3 -B docs/v5/reference/independent/actual-host-captures/check_captures.py
python3 -B docs/v5/reference/independent/actual-host-captures/check_captures.py \
  docs/v5/reference/independent/actual-host-captures/captures-normal
```

The default input is the packaged race capture set. Results default to a new
system temporary file; the command prints its location. `--output <external-file>`
selects an explicit destination. The checker refuses output inside this reviewed
package. It does not mutate packaged captures or validation records. No oracle
fork, copied source archive, services or network deployment is included.

Five explicitly synthetic mathematical sensitivity controls must also reject:
two stale key/range read cycles, a missing cross-partition result, a current map
substituted at an old cut and an extra phantom key. These are altered oracle
inputs, not faulty actual Host captures or production red-before-fix findings.
Full `Evidence.check` is intentionally unused. No per-event quorum receipts or
durable-event completeness declarations are manufactured.

## Reproduce actual Go capture

`independent-host-capture.patch` is unchanged reviewed **test patch data**, not an
applied canonical Go test. It relies on existing `host_test.go` transport/election
helpers and `protocol_test.go` fixture constructors. Apply it only to an isolated
copy matching the source hashes in `source-validation.json`.

The Go test intentionally requires `RHO_SERIAL_CAPTURE_DIR` and fails when it is
absent. Applying the patch as an ordinary CI test would therefore add an explicit
environment requirement; that requirement has not been hidden or rewritten.
From the isolated repository copy, choose external writable output directories:

```sh
export RHO_SERIAL_CAPTURE_DIR="<external-normal-capture-directory>"
cd v5
GOWORK=off GOTOOLCHAIN=go1.26.9 go test -count=1 -json \
  ./internal/txnproto -run '^TestIndependentActualHostSerialCapture$'
export RHO_SERIAL_CAPTURE_DIR="<external-race-capture-directory>"
GOWORK=off GOTOOLCHAIN=go1.26.9 go test -race -count=1 -json \
  ./internal/txnproto -run '^TestIndependentActualHostSerialCapture$'
GOWORK=off GOTOOLCHAIN=go1.26.9 go vet ./internal/txnproto
```

The patch exports raw evidence without inserting historical packaged provenance.
Pass `--source-root <isolated-repository-root>` when checking new raw captures.
The checker verifies all 101 tracked source files against the pinned manifest
before checking them; changed source requires a new review and pin. It leaves
raw input bytes untouched and makes no new commit-origin assertion. For example,
from the canonical repository root:

```sh
python3 -B docs/v5/reference/independent/actual-host-captures/check_captures.py \
  "<external-race-capture-directory>" --source-root "<isolated-repository-root>"
```

Do not copy old metadata onto changed source. No canonical Go build depends on
Python.

## Distinct evidence boundaries

The capture behavior was tested on a stable copy of schema-owner HEAD
`844e06589eeeab1adb2b9c4a605b8acc42083228` plus a SHA-pinned uncommitted replica
transport overlay. It is a copy-based test, not an archived-commit test. Normal,
race, both mathematical searches and `go vet ./internal/txnproto` passed.

Separately, the parent coordinator archived actual commit
`1a446a88c518a2cb7972c9d8e1149435e962a7c6`, independently matched all 101 tracked
v5 files against Git blobs, and ran full v5 race (8 packages) and vet successfully.
The copied tracked bytes match that archive, but the capture patch was not in it.
This stock archive evidence does not turn copy-based captures into an archive run.
`source-validation.json` separates both checks, transaction/transport source,
reused fixture and independent adapter hashes. Ignored `coverage.html`,
`coverage.out` and `coverage.pkg.out` are excluded from tracked-source claims.

The actual transport uses established Raft drivers and CrashableMemFS stores on
two groups of three replicas. Reopen checks a simulated synced-byte crash
boundary, not OS process-kill/disk durability. Fresh collection, fencing and
certification are manually orchestrated through primitives, not a production
Fresh service. Ownership moves/active-intent handoff remain unsupported. Capture
completeness, all fault schedules, graph installation, capacity/recipient-fence
findings and whole V2 acceptance remain separate obligations.

`capture-validation.json` records packaged mathematical results, source/patch
checks and portability validation. `source-validation.json` pins the canonical
oracle SHA-256:
`a132b4e4981f3dcea485b7ee5dea0df48a906cdfcda4a75bec5ff9f1a5589d3a`.
