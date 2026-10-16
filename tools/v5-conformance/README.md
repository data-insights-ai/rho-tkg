# v5-conformance

Door-level, mutation-then-historical conformance suite for the rho-tkg graph
doors that sigma-tkgd and ai-soc call. It imports no rho-tkg package: it is
written against the neutral `Adapter`/`Graph` interface in `adapter.go`
(int64 IDs as in the v5 public convention, int64 millisecond valid-time
coordinates, plain property maps). `internal/v4adapter` serves rho-tkg v4.56.1
today; a v5 adapter implements the same interface and calls `conformance.Run`.

## Profiles

| Profile | Meaning | Who serves it |
|---|---|---|
| `native` | semantics v5 keeps: graph-clock writes, historical cuts from `CommittedTx`, valid time as explicit coordinates, cursors followed to `next == 0`, history compared by content and order | any adapter (v5-native and compat) |
| `compat` | the v4 contract v5 drops on purpose (PLAN 8a): caller stamp = pin, `ErrTxOrder`, ms clock pins, keyset `next == last ID`, dense `Version == i`, `*WithTx` twins, void rows by stamp | a v4-shaped facade only |

Every case is tagged `native`, `compat` or `both` (`suite_*.go`). `both` cases
are written once and use `ts()` (caller stamp under compat, graph clock under
native), `open()` and `pinNow()`. An adapter declares what it serves by
implementing `ProfileAdapter.Profiles()`; without it both are required. Exit
criteria: a native v5 adapter passes `native`; the compat facade passes both.
`TestNativeProfileNeedsNoCallerStamps` runs `native` on an engine that refuses
caller instants, proving the profile needs none.

## Run

    GOFLAGS=-mod=mod GOPROXY=off GOWORK=off go test -short ./...

The module pins `rho-tkg/v4 v4.56.1` without a replace; `GOPROXY=off` takes it
from the local module cache. The whole suite runs in about two seconds.
`run-mutants.sh` (one run per deliberately broken door in `internal/mutant`) is
slow and is not part of the normal run.
