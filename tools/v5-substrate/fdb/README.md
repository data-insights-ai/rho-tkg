# FoundationDB functional comparator

Optional, separate test-support module for the bounded V2 substrate comparison.
It does not provide a public rho backend, allocator, graph planner or server.
The implementation is independent of `v5/internal/txnproto`; its scalar wire
shapes and whole-effect expectations match that provisional protocol's scope.

Run on an arm64 Docker host with bash and Python 3:

```sh
cd tools/v5-substrate/fdb
./run-local.sh smoke       # native boot + ordinary two-prefix read/write
make cover                # build, vet, race/coverage, oracle and process faults
make static               # pinned lint, gosec and govulncheck
make ci
```

The scripts create an internal Docker network, four ephemeral volumes and four
FDB containers. No host ports or native packages are installed. All created
containers, network and data volumes are cleaned; failed-run artifacts survive
under ignored `artifacts/`. Owned client image tags are removed; downloaded base images, shared build cache
and named Go tool caches remain.
The image supports native **linux/arm64**, without emulation. Other architectures
need separately verified pins and are outside this launcher.

Pins: FoundationDB **7.3.77**, commit
`3ea44ce1d9003ad095e408039e1f755c319c4dfb`, Go binding API **730**,
Go **1.26.9**, exact native image/client/header hashes in `Dockerfile` and
[the evidence manifest](evidence/functional-20261009.json). FDB is Apache-2.0;
its Rocky base has separate licenses. The release header archive omits
`fdb_c_types.h`; the build verifies that file from the same immutable commit.
The published repository advisory API returned no advisories on 2026-10-09;
that does not establish absence of native vulnerabilities. Go vulnerability
scanning does not audit the native runtime.

One ordinary FDB transaction validates complete partition generations, declared
revision reads, absent request/ID bindings and every affected prior revision.
Read conflict ranges cover the full participating current-state prefixes **before
writes**. Its terminal outcome, all scalar effects, tombstones, immutable history
and logical rounds commit atomically. There is no rho 2PC over FDB. The two
logical prefixes `g0/` and `g1/` are not two physical FDB shards. Request/TxID
bindings are **coordinator scoped**, as in the prototype; no graph-wide routing
or allocator integration is claimed. Generation checks and whole-prefix ranges
are conservative conflict overhead, not a tuned concurrency implementation.

Fresh/After commits retained certificates and round fences across the exact scope.
Later writes must choose rounds above the fence or fail native validation. At
pages immutable application history in **fresh** small native transactions; the
test crosses six seconds and installs a correction between pages. The native
five-second MVCC window is not used as an indefinitely retained rho cut. This
adds an application history layer and its cost; FDB still depends on its native
global commit-version ordering/services. Full columns, long-scan production
budgets, CDC, retention/GC, ownership transfer and real host/network faults remain
later comparison work. Nothing here selects an engine or establishes V2/V3/V7.

Defaults retain all metadata: 4 MiB encoded admission bytes, 256 coordinator
transactions, 4,096 history revisions and 256 certificates **per logical prefix**;
128 reads/effects per participant, 64 KiB encoded value/command cap, 128-byte
UTF-8 names and at most 128 rows per history page. Metadata bytes reserve the
exact immutable config/keys plus the worst-width mutable metadata value; user
KV deltas are exact. These finite **logical admission** quotas do not bound FDB
disk, native/server pools, OS cache or Go materialization overhead. No metadata
is reclaimed to hide exhaustion. FDB transactions have a 10-second total retry
timeout and 20 retries; clients/test processes have separate outer deadlines.

The oracle checks exact whole states, serial write-skew rejection, absent/empty
predicates, revision ABA, dedup mismatch, corrections/deletion and retained cuts.
Real native errors observed: conflict **1020** and cancelled future **1101**.
An absent request remains UNKNOWN; same-key/digest retry atomically resolves it.
The spike does not manufacture `commit_unknown_result` **1021** or claim it was
observed. Child SIGKILL before native commit and after native commit/before reply
checks zero-or-complete effects and no duplicate revisions. Four real server
processes are killed sequentially, all down before any restart, then reopened
against their prior volumes; one-zone loss permits
a new whole transaction with three remaining logical zones.

Triple SSD replication uses three coordinators and four observed machine/zone
localities. Startup and final reopen must reach healthy/full replication and
healthy storage teams before fault/cost reporting. Server roles/localities,
effective commands and status are captured. Each server has a 2 GiB Docker cap,
1,536 MiB resident setting and 128 MiB storage/cache settings. All zones share
one Docker Desktop VM on one M4 Max host: this is functional crash evidence.

Resource records include every server and client: allocated data/trace bytes,
raw cgroup current/stat/peak and native/container provenance. Per-container peaks
can occur at different times; their sum is only an upper bound. Final parallel
cgroup samples and Docker stats are snapshots, not deployment peak; Docker stats
may subtract inactive file cache. Test-client peak includes Go compilation and
race instrumentation. No large-data/vendor performance or space verdict follows
from this small functional workload.
