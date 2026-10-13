# FoundationDB functional comparator

Optional, separate test-support module for the bounded V2 substrate comparison.
It does not provide a public rho backend, allocator, graph planner or server.
The implementation is independent of `v5/internal/txnproto`. Its bounded scalar
spike is extended with a fixed two-node graph fixture and complete transaction
change groups. The fixed graph/CDC slice has native unit, race and static validation.
The evidence manifest keeps the earlier scalar and graph process-fault snapshots
separate from the current source validation; no general fault acceptance follows.

The launcher runs on an arm64 Docker host with bash and Python 3:

```sh
cd tools/v5-substrate/fdb
./run-local.sh smoke       # native boot + ordinary two-prefix read/write
./run-local.sh graph-fault # vet/race/coverage, graph client kills, zone loss/reopen
./run-local.sh paired-graph-fault # same bounded controller, common six-operation trace
./run-local.sh paired-tests # normal/race/coverage in fresh triple; no external kills
./run-local.sh lifecycle-trace # vet/race/coverage + actual common lifecycle export
make cover                # vet/race/coverage, oracle and scalar process faults
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

The default named `small` profile admits four 512 MiB/0.375 CPU servers and one
sequential client capped at 1 GiB/0.5 CPU: **3 GiB/2 CPU** together. Native server
settings are 384 MiB resident and 64 MiB storage/cache each. Prebuild/header setup
runs separately in one 1 GiB/2 CPU container before servers start. Each Go
invocation is capped at 180 seconds; prebuild orchestration is bounded to 600
seconds, the cluster lifecycle to 900 seconds, ordinary fault clients to 45
seconds, and cleanup to 180 seconds. Healthy, fully replicated triple SSD with
four logical machines and three coordinators remains mandatory. No fallback
changes replication. `large` preserves the earlier 2 GiB server, 1,536 MiB native
resident and 128 MiB storage/cache settings, with a 2 GiB client. Its 10 GiB
aggregate admission is refused by default; an explicitly sufficient
`FDB_ADMISSION_MIB` and `FDB_ADMISSION_CPUS` allowance is required. These are test
profiles, not production capacity recommendations or guarantees about Docker's
shared VM/daemon memory.

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
and logical rounds commit atomically. Format 2 also retains every committed
request's immutable complete change group in that same commit; aborted requests
produce no group. There is no migration from the prior prototype format: a
different stored configuration is refused. There is no rho 2PC over FDB. The two
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
global commit-version ordering/services. Closed-cut CDC pages whole immutable
groups and checks both coordinators' retained sequence watermarks, even for a
single-owner scope. Missing groups or unavailable certificates fail closed.
Full columns, long-scan production budgets, retention/GC, ownership transfer and
real host/network faults remain later comparison work. Nothing here selects an
engine or establishes V2/V3/V7.

The graph fixture declares two graph-qualified node IDs, LifeIDs and one
cross-owner edge ID; it does not allocate IDs. Create/update validates both live
endpoint incarnations and writes the edge identity plus both postings atomically.
An endpoint closure masks the old edge from visible/out/in results. A new life
does not rebind that identity; cleanup can close its three revisions after an
endpoint closes. Identity and visible graph reads require both owners' cut scope.
The independent literal oracle checks actual retained endpoint/edge/posting values,
correction, deletion and complete CDC; local observation sequence and dependency
records describe a small sequential fixture, not general concurrent serializability.

The fresh optional lifecycle comparison profile additionally persists the exact
AllScope Q/ms/POSIX descriptor, own edge LifeID, caller provenance and typed value
definitions/references in immutable config and admitted transaction groups.
Config equality is structural after decoding. The declared input definitions
do not create observed dictionary membership; only committed definitions and
matching history introduce values in the export. Zero-profile scalar and graph
fault encodings remain unchanged; a different profile on an existing namespace
is refused. This is fixed comparison support, not a generic schema, allocator
or dictionary backend.

`lifecycle-trace` runs the same seven graph operations and exports all raw history,
config, original groups/outcomes, cuts and validated final current rows. It checks
every physical edge/out/in revision and tombstone before deriving logical changes.
Immutable declarations and properties remain observable after presence closes.
Empty labels follow the complete stored-row census; unexpected rows refuse.
The held cut after operation 3 is edge-only and includes its immutable life;
reopened cuts after operations 3/4/5 are
complete within the declared fixture domain. This differs from native v5's
allocator-issued range completeness; both proofs remain visible. Native binary
scope and FDB JSON/Go byte-array encodings remain raw evidence; canonical AllScope,
typed references and operation bindings provide the explicit correspondence.

Compare supplied actual traces, preserving their raw bytes:

```sh
python3 -B compare-lifecycle.py native-trace.json artifacts/RUN/lifecycle-trace-2.json
python3 -B compare_lifecycle_test.py native-trace.json artifacts/RUN/lifecycle-trace-2.json
```

The comparator validates identity/operation bijections, physical key ownership,
stored metadata/digests/original records, exact old-cut identities and completeness
before comparing full answers and logical CDC. It normalizes null/empty lists
and documented representation differences. It does not compare physical-write
counts, numeric rounds or native codecs as though they were equivalent. The
[lifecycle receipt](evidence/lifecycle-20261012.json) records the local qualification.
This lane executes no external kill schedule; earlier graph-fault evidence remains
separate. Non-All temporal access, nonempty labels, allocation, retention/GC,
multi-host durability, broad cost evaluation and engine selection are unsupported.

`paired-graph-fault` uses a separate fresh lifecycle-profile namespace and the
`graph-fault-local-v1` fixture. Its three seed transactions create both nodes and
the life-bound edge/property 5; the faulted updates are 6, 5, 6 with original
caller provenance 4, 5, 6. Both typed value definitions come from actual admitted
commands; reusing them introduces no duplicate declaration. The legacy
`graph-fault` sequence remains 6, 7, 8.

The paired lane preserves original input bytes, observation-time cuts and the
three seed cuts. A private transaction killed before native Commit leaves the
caller unknown; a later validated cut can observe absent request/ID rows and the
old complete graph. Identical retry resolves the original and a separate repeat
retry checks unchanged complete CDC. After the committed-reply kill, recovery and
retry recover the same result; a differently valued, otherwise valid payload
under the original key must reach immutable-binding mismatch without changing
graph or complete group inventory. One-zone progress uses the frozen original
request while that server is down. The edge-only old observation is captured
before zone loss and closed; full old-cut reads are repeated after every server
is stopped and all original volumes reopen. All six original requests are
recovered and replayed after that reopen, preserving original results and the
unchanged six-group/current-row ledger. The postcommit child writes its actual
outcome, digest and raw durable records before the controller observes its marker
and kills it. The paired marker includes the SHA256 of those exact receipt bytes;
later recovery supplies a separate observation.

`paired-graph-fault-trace.json` contains the backend's actual history/config,
complete groups, original receipts, typed logical changes and independently
checked physical edge/out/in revisions. The controller attaches raw markers,
actual kill/reap/ready inspections, per-instance volume identities and ordered
command events. Referenced artifacts are hash-bound in the trace and terminal
launcher receipt. Compare it only with the separately qualified native paired
trace using the shared six-operation checker; the seven-operation
`compare-lifecycle.py` deliberately does not accept this fixture. Unit seam tests
do not claim real kills, and this local lane does not establish independent-host
durability, power-loss recovery, the full fault/cost matrix or engine selection.
The [source checkpoint](evidence/paired-20261012-source.json) records only
lightweight controller guards; Go and live qualification remain unrun there.
The subsequent [runtime checkpoint](evidence/paired-20261012.json) records 33
passing normal and race parents and the actual local paired fault schedule.
The first race attempt reused normal-test namespaces; the launcher now clears
only the owned `[g0/, g2/)` fixture range between completed test processes.
No reset occurs during the fault/reopen schedule. The
[final source qualification](evidence/paired-final-20261012.json) passed 33
normal and race parents without skips, pinned statics and 84.38% new-file
statement coverage. `paired-tests` qualifies the same fixture and refusal
matrix without repeating the external kill schedule. Its test container uses
the final 310-second bound; the earlier fault receipt preserves its actual
490-second configuration and observed duration. The actual native paired trace
is still required for full comparison.

With a fresh namespace and an already bounded native deployment, the executable
supports `graph-smoke` (`FDB_GRAPH_FIXTURE`) and `graph-fault-seed`,
`graph-fault-before-child`, `graph-fault-recover-before`, `graph-fault-after-child`,
`graph-fault-recover-after`, `graph-fault-verify`, `graph-fault-prepare-progress`,
`graph-fault-progress` and `graph-fault-terminal` (`FDB_GRAPH_FAULT_GRAPH`). Child
fault modes deliberately wait for an external controller to kill their exact
process after the printed native commit seam. Fault records use the external
`/evidence` mount. The `graph-fault` launcher checks the exact phase marker and
non-OOM exit 137, recovers both killed clients, captures a fixed graph update
before losing one zone, and replays that identical request after uncertainty.
It verifies all three retained seed cuts and complete CDC, kills all four exact
servers before starting any of them on their original volumes, then verifies
again. A timed-out ordinary client must be observed stopped before another
client is admitted; uncertain cleanup fails the run. Terminal receipts hash
the complete artifact set and include cleanup status and source/resource pins.

The 2026-10-12 local `small` run passed 29 native race test parents without skips
(84.2% package coverage), both actual client kills, one-zone-loss graph progress
and same-volume all-server reopen with six complete transaction groups. The
earlier 30-poll readiness attempt refused while data distribution was still
initializing; the successful run restores the original 90-poll window. The
[launcher receipt](evidence/launcher-20261012.json) separates that failed attempt,
static validation and successful source snapshot. Subsequent exact-one-inspect
and immediate inspect-failure guards passed their existing contract tests and
rechecked the recorded inspections; the Go body is unchanged from the live run.

Defaults retain all metadata: 4 MiB encoded admission bytes, 256 coordinator
transactions, 4,096 history revisions and 256 certificates **per logical prefix**;
128 reads/effects per participant, 64 KiB encoded value/command cap, 128-byte
UTF-8 names and at most 128 rows per history page. Metadata bytes reserve the
exact immutable config/keys plus the worst-width mutable metadata value; user
KV deltas are exact. These finite **logical admission** quotas do not bound FDB
disk, native/server pools, OS cache or Go materialization overhead. No metadata
is reclaimed to hide exhaustion. FDB transactions have a 10-second total retry
timeout and 20 retries; clients/test processes have separate outer deadlines.

The historical scalar oracle checks exact whole states, serial write-skew rejection, absent/empty
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
effective commands and status are captured. Resource settings follow the selected
named profile above. All zones share
one Docker Desktop VM on one M4 Max host: this is functional crash evidence.

Resource records include every server and client: allocated data/trace bytes,
raw cgroup current/stat/peak and native/container provenance. Per-container peaks
can occur at different times; their sum is only an upper bound. Final parallel
cgroup samples and Docker stats are snapshots, not deployment peak; Docker stats
may subtract inactive file cache. Test-client peak includes Go compilation and
race instrumentation. No large-data/vendor performance or space verdict follows
from this small functional workload.

The selected `cost-cell` mode uses three fixed endpoint nodes and the declared
relationship/life interval from `RHO_GRAPH_COST_SPEC`. It creates N relationships
with typed `p=5`, updates all N to `p=6`, retains the complete history and original
CDC, and checks old/current views after the updates. Its dependency capture uses
bounded real point reads; history and CDC use the existing 128-row paging doors.
`run-local.sh cost-cell small` reuses the four actual FDB containers, triple SSD
configuration and fresh volumes, recording all server/client process RSS and
all four data/log directories. The FDB fixture durably admits externally supplied
fixed IDs; it does not implement rho allocator/recipient issuance. The native
runner includes actual issuance and setup coordination, so those setup costs are
reported separately without claiming identical allocation contracts.

`../cost-cell.py --plan` preserves the reviewed 10k/100k/1M × 0/10/50/100% cross ×
1/8/32-writer matrix and five fresh runs. The current source supports writer1;
concurrent writers and the native one-million-fact multi-grant setup remain
pending. `--probe` explicitly records one feasibility cell, and `--compare`
checks the completed native/FDB retained-output witnesses. Execution requires
the genuine qualified paired-fault inputs; local cost cells remain
single-machine distributed tests. The pinned SDK profile tests and two-fact local/cross functional paths are
qualified separately from the incomplete native10k measurement. `FDB_COST_QUALIFY=1`
runs only the two cost test parents in normal/race mode and instruments the actual
client for coverage, including a read-only stale-configuration refusal. These
instrumented results make no performance claim. The controller preserves
functional success but refuses cost qualification when no actual client PID was
sampled, instrumentation is enabled, or its mode is unknown. Full10k matched
measurements and the remaining matrix remain pending; no engine is selected.
