# rho-tkg v5

Unreleased development module: `github.com/data-insights-ai/rho-tkg/v5`.
The [v5 plan](../docs/v5/PLAN.md) defines the intended temporal graph database
contract; [the backlog](../tasks/backlog.md#open--v5-v0v7-acceptance) records
implementation status and phase acceptance. Public graph operations and consumer
migration remain under development. Development commits carry no released API,
storage-format or operational compatibility promise.

Rho owns graph storage, temporal values, transactions, snapshots and changes.
Sigma owns reasoning, query planning and execution. The embedding application
supplies transport and process orchestration, including authentication of unique
live replica incarnations.

The current importable packages are:

| Package | Purpose |
|---|---|
| [`pkg/temporal`](pkg/temporal) | Explicit axes and domains, exact positions, bounds, regions, scopes, descriptors and point knowledge |
| [`pkg/types`](pkg/types) | Graph identity, integral-millisecond compatibility values and an explicit default-axis binding |

The engine in `internal/` is an implementation boundary. Consumers cannot use it
as a substitute for the planned public graph API. Existing v4 consumers should
follow the [migration contract](../docs/v5/PLAN.md#8a-consumer-compatibility-revision-2026-10-09)
and [v4-to-v5 mapping](../docs/v5-analysis.md#5-v4--v5-door-mapping).

The distributed engine (`graphapply`, `txnproto`, `replica`) lives under
`internal/dist/`; the single-partition v5.0 path (`pkg/graph` -> `graphstore` ->
`raftlog`) does not import it. `make check` covers the v5.0 set; `make check-dist`
covers `internal/dist/...` at the release gate.

Use the toolchain specified in [`go.mod`](go.mod). From a repository checkout,
`make -C v5 check` runs vet, build and short tests; `make -C v5 test-race` and
`make -C v5 cover-gate` provide the race and coverage gates. These commands do
not replace the distributed, consumer and measured-capacity acceptance gates.

Future releases use repository tags `v5.X.Y`, beginning with `v5.0.0`, for this
module path and directory layout. Go excludes the major-version directory from
the tag prefix; `v5/v5.0.0` is not the release tag convention for this module.
See [Go's version-to-commit rules](https://go.dev/ref/mod#vcs-version) and the
[repository release policy](../docs/stability.md#unreleased-v5-module).

Security reports follow [SECURITY.md](../SECURITY.md). License: [Apache-2.0](../LICENSE).
