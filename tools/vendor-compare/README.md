# TigerGraph functional adapter (unfinished)

This standalone Go 1.26.9 module implements the basic-current graph mapping for
TigerGraph 4.2.5. Support is unfinished: build, REST/schema compatibility and
functional validation remain pending. The existing native-v4 tool and its
114-file comparison reference stay unchanged.

The functional contract requires all 92 staged current answers and all 23 r3
answers after service/container restart to equal complete reference multisets.
Errors, skipped mutations, incomplete exports and mismatches prevent a completion
receipt. Retained-history, other vendor transports, native-v5 and distributed
mappings are not implemented.

The image is pinned to TigerGraph 4.2.5 at
`sha256:78a3d62604527ba8686930465554fc3a419bf4b4de5c3d2d50202825a5308d49`,
linux/amd64. ARM hosts require emulation. Emulated timings do not substitute for
native-x86 performance comparisons.

The bundled Compose configuration isolates the deployment using project,
container and volume names `rho-vendor-tg-20261010`,
`rho-vendor-tg-20261010-db` and `rho-vendor-tg-20261010-home`.
The REST proxy binds to 127.0.0.1:19240 at `/restpp`. The default container
limits are 4 CPUs and 12 GiB, without additional swap allowance. The controller
verifies project, image, volume and endpoint identity before operations. It
starts services, checks the version/schema hash, creates the named schema and
restarts the same container. It does not create containers or download images.

The `--out` option selects local artifacts. External-path enforcement and
automatic temporary-directory selection are unfinished; execution requires
completing the outside-Git guard. Outputs have no upload or hosting integration.
A partially loaded database remains available when an operation fails.

```sh
GOTOOLCHAIN=local go run ./cmd/vendor-compare capabilities
GOTOOLCHAIN=local go run ./cmd/vendor-compare run --reference /path/to/frozen/comparison-inputs --out /outside/git/results
```

Native INT values use exact signed-int64 encoding/decoding. Native DOUBLE values
must match fixture bits on export; general binary64 REST round-trip support is
not established. Explicit presence flags distinguish absent properties from
false, numeric zero and empty text. Labels use a SET<STRING> mapping. Edge
discriminators preserve multiplicity; an ordinary external-ID attribute supports
complete exports. Reverse edges support incoming traversal and are counted as
physical rows rather than additional logical graph edges.

The adapter reads native rows rather than a mirrored entity-value map. Predicates
and projections filter bounded REST scans in the client. Directed walks recurse
through native adjacency, preserving complete edge-ID sequences and allowing
repeated nodes and edges. These mappings establish no index/planner equivalence.

Upserts use `ack=all`, `gsql-atomic-level:atomic` and, for edges,
`vertex_must_exist=true`. Accepted/skipped counts and complete native readback
are checked. Mutation events execute separately. GPE acknowledgement and
service/container restart do not establish fsync, power-loss, quorum durability
or concurrent transaction isolation.

Resource schemas separate declared topology, forward/reverse rows, schema helpers,
HTTP traffic, cgroup counters and home-volume file inventories. Missing metrics
use explicit unavailable status and null values. Regular-file allocation is
counted once per device/inode; directory/symlink metadata and filesystem overhead
remain excluded and the volume totals are marked partial. Client/host/VM costs,
allocator categories, Docker stdout logs, temporary/peak disk and complete replica
totals remain unavailable. Complete cost and performance acceptance stay false.
Overlapping heap/RSS/cache diagnostics are not added to cgroup memory.current.

Tests use isolated HTTP/command fakes and temporary files. They do not start a
Docker runtime. The unchanged comparison reference is supplied explicitly:

```sh
VENDOR_COMPARE_REFERENCE=/path/to/frozen/comparison-inputs GOTOOLCHAIN=local go test -race -count=1 ./...
```

Technical references:

- https://www.tigergraph.com/docs/tigergraph-server/4.2/api/upsert-rest
- https://www.tigergraph.com/docs/tigergraph-server/4.2/api/built-in-endpoints
- https://www.tigergraph.com/docs/gsql-ref/4.2/ddl-and-loading/defining-a-graph-schema
- https://www.tigergraph.com/docs/tigergraph-server/4.2/system-management/manage-services
