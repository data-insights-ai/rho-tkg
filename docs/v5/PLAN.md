# rho-tkg v5 architecture and implementation plan

Design proposal, 2026-10-09; revised the same day after the code review
[tasks/review-v5-plan-vs-code-20261009.md](../../tasks/review-v5-plan-vs-code-20261009.md)
(main `32568c4`; revised passages are marked "Revision 2026-10-09").
Build a compact, efficient, distributed temporal
graph database that supports the data needs of broad temporal reasoning.
**rho-tkg owns the database; sigma-tkgd owns reasoning.** The same storage and
read contracts apply on one machine and across partitions.
[DISCUSSION.md](DISCUSSION.md) is the short introduction;
[RESEARCH-REVIEW.md](RESEARCH-REVIEW.md) records evidence and alternatives.
Implementation status belongs only in [tasks/backlog.md](../../tasks/backlog.md).

## 1 Design decision

Use **typed temporal graph data, explicit value and access capabilities,
specialized column storage, and partitioned transactions**.

An assertion states something about an object, occurrence or relationship.
Its temporal meaning is independent of its encoding and placement on machines.
Common point and state workloads have concrete, allocation-light implementations;
additional temporal forms enter through versioned value descriptors and declared
database predicates. The graph model remains nodes, relationships and properties,
with independent component histories and temporal values.

Generality means that adding a temporal model need not replace the identity,
history, storage or distribution model. It does not mean one algorithm decides
every temporal logic efficiently. Three claims must be assessed separately:
lossless preservation, native database access, and reasoning in sigma-tkgd.
An extension can have preservation support before a native predicate exists;
that status must be visible to callers. Broad reasoning support is an integration
property, never inferred merely from storing a descriptor.

### Ownership contract

| Concern | Owner and boundary |
|---|---|
| Temporal value formats and semantics | rho-tkg's public types define domain/axis identity, exact encodings, bounds and declared value predicates; sigma imports or explicitly adapts them |
| Graph persistence and access | rho-tkg: nodes, relationships, properties, revisions, exact selections, adjacency, indexes, typed batches and statistics |
| Database consistency | rho-tkg: transactions, replication state machines, retained snapshots, changes, recovery and ownership fencing |
| Query and reasoning execution | sigma-tkgd: languages/IR, planning, joins, temporal windows, aggregation, rules, recursion, constraint/uncertainty solvers and computational progress |
| Derived knowledge | sigma computes and maintains it; rho stores supplied results, provenance and covered-input-cut metadata transactionally |
| Deployment | The embedding application supplies processes and transport; rho's distributed database state machines remain distinct from sigma's distributed reasoning workers |

Pure value operations such as comparison, intersection, overlap and containment
belong in rho when needed for its public value API, native selection or state
correction. Evaluating a rule or solving a network of constraints belongs in
sigma. Sharing a scalar contract does not introduce a rule-plan executor, solver
registry or reverse dependency on sigma's internal IR into rho.

### Decisions and alternatives

| Question | Recommendation | Why |
|---|---|---|
| Universal logical model | Assertions plus typed temporal scope and knowledge | Represents occurrences, persistence, relations and incomplete information without converting them all into states |
| Generality versus fast paths | Explicit capabilities; concrete storage/predicate paths chosen once per batch | The semantic contract is general; per-row dispatch and boxed universal values are avoidable |
| Temporal coverage | Linear and structured values; causal graph facts; typed uncertainty, constraint and recurrence descriptors | Integers and rationals are useful instances, not the boundary of what can be represented |
| Reasoning integration | Typed reads, complete temporal histories and transaction-grouped changes for sigma | Efficient reasoning inputs without duplicating sigma's execution layer |
| Storage | One arena/memtable/column-segment engine per logical partition | Memory mode, disk mode and distributed mode share the reducer and visibility rules |
| Replication | Replicated partition logs, consensus fencing and recoverable cross-partition commit | A sorted timestamp or a replicated log alone does not provide distributed transactions |
| Database snapshots | Opaque certified cuts over logical commit rounds | Local log offsets, source clocks and reasoning frontiers are different coordinates |
| Coordination | Partition-local commit fast path; coordinated cross-partition transactions and fresh cuts | Avoid a global data sequencer without pretending global consistency is free |
| IDs | Graph-qualified opaque 64-bit IDs from durable allocation blocks | No clock dependence; current ownership is a separate, movable directory entry |
| Full V0–V7 completion | Distributed and local database correctness, sigma compatibility and measured cost together | Embedded-first release eligibility (§5.2) does not discharge distributed gates |

## 2 What is represented

### 2.1 Semantic layers

| Concept | Definition and contract | Does not determine |
|---|---|---|
| Identity | Stable identity of an object, assertion, event or revision | Temporal equality, ordering or physical location |
| Domain | Set of semantic positions and declared mathematical structure | Codec precision or observed event density |
| Order | A specified partial or total order; equality and incomparability are meaningful | Seconds, causality unless specifically asserted, or storage order |
| Axis | An instance of a domain with reference system and identity | Its role as validity, occurrence, source receipt or another dimension |
| Temporal role | Meaning of an axis association | Numeric representation |
| Scope | Region or constraint specifying where an assertion applies | Whether it describes persistence, an occurrence or uncertain knowledge |
| Duration | A specified elapsed-amount or shift operation with units and laws | A metric, symmetric distance, or invertibility |
| Metric or distance | A named operation with stated laws and units | Ordering, identity, or a default distance between intervals |
| Knowledge | What positions/scopes are possible, measured or merely nominal | Occupied duration or statistical independence |
| Interpretation | Occurrence, state, observation, constraint, derived assertion | A physical row shape |
| Granularity | A versioned grouping/abstraction and interpretation policy | Exact unit conversion or source accuracy |
| Representation | Exact codec, column layout and compression | Any change in the preceding meanings |
| Access structure | Database index or adjacency directory; sigma may build its own computation arrangements | The meaning of predicates or the truth of unindexed facts |

These are related, not freely interchangeable knobs. A metric induces a topology;
a conventional interval algebra needs an appropriate ordered domain; a calendar
shift needs a reference calendar. A profile declares its valid combinations.
Graph topology, order topology, a topological sort and a search index remain
different concepts.

Always qualify density: **order-dense domain**, **sparsely observed history**,
or **dense/sparse storage layout**. Unequal gaps between events change neither
the domain nor its metric by themselves.

### 2.2 Assertions and views

Conceptual records, not a mandatory per-row struct:

    AssertionID, RevisionID
    Graph entity/component + typed value or relationship endpoints
    Interpretation
    TemporalScope
    Knowledge + source/provenance references
    Database revision metadata

Node identity, node life, labels, relationship identity/endpoints, relationship
life and properties are distinct assertions/components. Wide stable event
attributes may be stored together; the logical separation does not require a
physical row per property.

A stable event represented in the graph can have several corrected revisions.
Two events with identical coordinates remain two events. Multiple sources can
assert conflicting values; separate graph records preserve them. Sigma or the
application selects an authority/conflict policy and can persist the resulting
state. Rho's revision selection is a database operation; it does not adjudicate
conflicting claims about the world by arrival order.

State component operations such as Set/Unset replace exactly a requested region,
preserve the remaining support and distinguish absent from present-null.
Corrections and retractions are new database revisions. A late-arriving assertion
can refer to the distant past without retroactively inserting a database commit.

Source truth and database contents are distinct: “no stored match at cut C” is
an exact database answer. “The event did not happen” additionally depends on the
reasoner's closed/open-world and source-completeness assumptions.

### 2.3 Domains, capabilities and exact codecs

A domain descriptor defines semantic positions P. A codec can encode P_enc,
a subset of P, exactly. The encoder must not silently replace P with P_enc.
On Z, (0,1) is empty; on Q it is nonempty, even when both endpoints use identical
integer bytes. The index orders stored keys; it need not enumerate P.
[PostgreSQL range canonicalization](https://www.postgresql.org/docs/18/rangetypes.html#RANGETYPES-DISCRETE)
is a useful engineering precedent.

Profile descriptors and support levels separate:

- Preservation: versioned type/axis identity, exact encoding, validation,
  round-trip and byte-integrity guarantees.
- Native value/access operations: equality/canonical form, declared comparison,
  membership, overlap and region operations needed by database state updates.
- Optional native capabilities: successor/predecessor only where valid, and
  conservative index bounds with an exact database residual where available.
- Consumer semantics: duration/metric laws, clock/calendar mappings and symbolic
  descriptors are preserved; sigma owns their use in rules and inference.

A named scalar helper can be shared through the public value API, but this does
not register a solver or relational execution plan. Native implementations are
deterministic and typed; dispatch occurs at batch boundaries. Cluster members
must support the database operations assigned to them. Unknown versions cannot
be compared or interpreted silently; an explicit opaque-preservation mode may
retain bounded tagged bytes. It promises byte round-trip, not semantic equality
or temporal predicate support. Stored data never loads executable code.

Initial native value/access profiles cover integer/step domains, dense rational
linear time and lexicographic positions (including model time plus microstep).
Causal partial orders are represented by identified events and asserted graph
relations; rho enumerates those records, while sigma derives their consequences.
Rational endpoints have a scaled-integer fast path when exact, a normalized
numerator/denominator path otherwise, and checked arbitrary-precision comparison
within configured resource limits. A sigma-derived 1/3 must round-trip, index and
compare exactly; an int64-only codec cannot satisfy that storage contract.

The domain interface permits dense-real, mixed, branching and additional symbolic
profiles. A dense-real profile with rational endpoints can exactly implement
finite interval operations, but cannot claim arbitrary exact real arithmetic.
Every further native capability needs a tested implementation and declared
limits; broader evaluation remains in sigma. Mixed or branching models use
domain-specific regions or graph relations. They do not inherit a linear
interval implementation merely because values can be sorted for serialization.

Semantic comparison may return incomparable. A deterministic storage tie-break
must not be exposed as temporal precedence. Incomplete evidence about a relation
is “unknown,” distinct from proven incomparability within a declared partial order.

An exact database selection either rechecks all candidates or explicitly declines
the predicate. A separate candidate-access API can promise a complete superset
with residual evaluation required in sigma. It must never silently omit possible
matches. Sigma tracks approximation and solver completeness independently; a
complete database scan does not certify a complete inference.

### 2.4 Scopes, constraints and uncertainty

Fast scope forms are Unplaced, Point, Span and finite RegionSet. Multi-axis
scopes use products when dimensions are independent; dependent coordinates use
a typed relation/constraint descriptor. Periodic regions and recurrences retain
symbolic descriptors and exceptions; sigma performs bounded-window enumeration
or symbolic evaluation. Rho stores these losslessly and advertises which native
predicates, if any, it implements. It does not expand infinite scopes into rows.

On a linear axis, bounds explicitly carry finite/infinite and inclusive/exclusive
status. Zero is an ordinary coordinate. Empty is a set-operation result;
Unplaced means no placement was asserted; All(axis) is a positive universal-scope
claim. These are three different things.

Singletons normalize to points for set algebra. On Z, Point(0) and [0,1) have
equal support; their event/state interpretations can still differ. On dense
time they do not have equal support. Point events never become small duration
intervals merely to fit storage. State subtraction can leave singleton tails:
[0,2] minus (0,2) yields {0,2}.

Allen classification of two located proper intervals and solving a network of
unknown interval relations are different operations. Boundary membership and
Allen endpoint ordering are also distinct: meeting half-open intervals do not
overlap. Rho may classify supplied endpoints as a pure value predicate. Sigma
owns relation-network propagation/search and its completeness status.

Knowledge is a constraint on possible placements. For an uncertain point,
support U is a set of possible coordinates. For a window W:

    PossibleIn(U,W) iff U intersects W
    DefiniteIn(U,W) iff U is a subset of W

For uncertain spans, knowledge constrains possible pairs (start,end), including
start <= end and any correlation; two unrelated endpoint error bars are not a
complete semantics. Rho preserves that relation; sigma's solver evaluates the
richer case. It is never coerced to one occupied envelope. Nominal coordinates,
hard bounds, confidence intervals and unspecified quality are separately tagged.

For shared-clock uncertainty, preserve a shared latent constraint/reference.
Use Cartesian products only as an explicitly conservative relaxation.
A probability distribution is optional and never inferred from a support.
PossibleIn/DefiniteIn over an explicit finite support can be native set predicates.
Inferring a support or combining correlated evidence belongs in sigma. Its answers
distinguish exact, definite, possible, unknown and inconsistent evidence;
evaluation completeness/approximation is a separate status dimension.

Exact equality, same-support equality, source-record identity and approximate
matching have separate names. Closeness is not transitive. Canonical semantic
hashes agree across exact unit/codec conversion on one axis; source-byte integrity
hashes may intentionally differ. Neither hash replaces a stable assertion ID.
Rounding creates an explicit derived value and policy reference, retaining the
raw observation when required.

### 2.5 Lifecycles, calendars and granularity

A state relationship may bind to endpoint LifeIDs. Effective visibility
intersects its declared scope with the same endpoint lives at the same database
cut; reopening a node allocates a new life and does not rebind an old edge.
An identity-reference observation instead refers to stable identities, even
after their active lives end. Projections expose that lifecycle status.
Creation validates references transactionally; late-bound references use explicit
stubs. A strict life-bound placement must use a profile whose coverage rho can
verify transactionally. Unsupported symbolic/uncertain constraints remain stored
assertions, not silently enforced database invariants. A sigma solver result is
derived evidence; accepting it as a database constraint proof would require a
separately specified verifier, not an unchecked caller assertion.

Calendar periods and fixed durations are separate types. Calendar/timezone rules,
ambiguity policy, time scale and leap/smear conversion versions accompany their
use. Rho preserves those references; sigma evaluates the calendar or clock model.
Historical replay cannot depend silently on today's timezone database.
Order-only axes have no implicit seconds. A stopped business clock can produce a
pseudometric and a nonunique inverse; it cannot implement a metric capability
without satisfying that capability's laws.

Granularity changes declare Any, All or an aggregate interpretation, coverage and
information loss. Weekly counts cannot establish every 20 ms deadline. Summaries
are substitutable only for specified predicates, source cuts and rule versions.
Sigma decides substitution and maintains derivations after corrections. Rho
stores the supplied summary and coverage metadata atomically and reports retained
data/cuts; it cannot relabel an old summary as fresh or infer lost resolution.

## 3 Database access and the sigma boundary

### 3.1 Coverage matrix

Every row tests a representation/access requirement against a reasoning use case.
Rho's release proves the database column; sigma's suites prove the evaluation
column for supported reasoning features. Research examples beyond sigma's current
features are contract fixtures, not a promise to implement those solvers in rho.

| Workload | rho-tkg stores and exposes | sigma-tkgd evaluates |
|---|---|---|
| Event windows and sequences | Points, stable event IDs, source-order facts, exact indexed selection | Window joins, ordered patterns, source-completeness conditions |
| Evolving properties and topology | Component regions, lives, revisions, slices, adjacency and state corrections at a cut | Temporal graph queries combining multiple inputs and derived state |
| Qualitative intervals | Located spans and asserted relation constraints; native endpoint classification where supported | Unknown-interval constraint propagation/search and completeness |
| Metric rules including DatalogMTL | Exact coordinates and finite regions; typed symbolic results; full histories and changes | Shifts, joins, universal/existential windows, recursion and symbolic/automata evaluation |
| Source causality | Identified events and asserted causal edges without invented total precedence | Transitive consequences and reasoning under incomplete evidence |
| Same-time reactions | Lexicographic coordinates with separate microstep and model coordinate | Model-specific duration and reaction semantics |
| Calendars and recurrences | Versioned schedules, exceptions, timezone/clock references | Calendar arithmetic, expansion, recurrence reasoning |
| Uncertainty and constraints | Supports, correlations and constraint descriptors; exact finite-support predicates where implemented | Support inference, STN/general constraint solving and probabilistic reasoning |
| Multiple clocks or scenarios | Typed axes, mapping descriptors and context/branch relations | Cross-axis mapping and propagated uncertainty |
| Incremental recursive reasoning | Atomic changes, retained input cuts, stable revision IDs and stored provenance | Delta algebra, joins, arrangements, recursion, retractions and computation frontiers |

Continuous/hybrid trajectories and richer temporal models can preserve equations,
automata and parameters as typed values or graph structures. Rho must not flatten
them into endpoint snapshots and imply equivalence. Sigma chooses evaluators and
their guarantees. MeTeoR illustrates why multiple evaluation strategies may be
needed even within one language; it does not prescribe a database runtime.
[Practical Reasoning in DatalogMTL, §§3–5](https://arxiv.org/html/2401.02869v1)

### 3.2 Native read contract

Provide typed, projected batches for identity lookups, label/type/property scans,
temporal selections, histories and adjacency expansion. Rho may choose indexes
and push down its declared exact scalar predicates. Sigma owns query planning,
relational joins, temporal windows, aggregation and rules. No query/rule IR,
general Evaluate(plan), solver callback or query-subscription executor is part
of rho's v5 API.

Each access capability states its accepted profiles, predicate semantics, result
order, projection, history coverage and exactness. A declined predicate remains
explicitly available for sigma to evaluate over complete candidates. A missing
native evaluator never means false. Cost/cardinality estimates are separate from
correctness guarantees. Rho's own state replacement uses exact region operations;
that does not make it responsible for a join across reasoning relations.

Current sigma-tkgd's internal/query/ir/time.go and program_time.go already define
structural metric operators, Since/Until and endpoint binding. They describe
dense-real semantics with integer endpoints, projection before box evaluation,
and an empty-window convention in which even box derives nothing. Its adapter
must preserve those rules and annotation behavior. In particular, a native
selection must not discard history required by sigma's CompleteSnapshot reads.
These are sigma semantics; rho exposes the complete data and exact value profile
needed to implement them, without importing sigma's internal packages.

Revision 2026-10-09: two v4 behaviours the typed selector must not inherit.
`QueryOpts.TxAt` without a valid-time coordinate adds an implicit valid-at-now
filter (`store/opts.go:48-61`, `core/temporal.go:1119-1123`); in v5 a snapshot
mode never implies a valid-time coordinate. v4 serves two relationship views by
door: the declared view (scan, during, relating doors) and the effective view
masked by endpoint validity (Snapshot, NeighborsAt, `OutgoingRelsAt`). A v5
selector names the view explicitly.

Reference access surface, with names still subject to API review:

    OpenSnapshot(Fresh | Stable | At(Cut) | After(Receipt), scope)
    Lookup / Scan / Expand(snapshot, typed selector, projection)
    Timeline / Occurrences(snapshot, typed selector)
    Changes(cursor, certified boundary) -> transaction envelopes
    WatchChanges(selector, starting cut) -> resumable database changes
    Capabilities / Statistics / ExplainAccess

Snapshot bootstrap and change-feed continuation must form one gap-free contract:
pin cut C, scan at C, then consume all changes after C using a cursor allocated
under the same retention lease. Reconnect may replay records; stable transaction
and revision IDs permit deduplication. No exactly-once external processing is
implied. A filtered feed includes transitions into and out of its selector using
before/after state; filtering only the new value would lose removals.

Resume cursors pin the cut, selector/profile versions, partition lineage and
order. Distributed pagination is applied after the specified merge order.
Cancellation, backpressure and bounded buffered bytes apply throughout access.
Batches have both row and byte limits (initial row ceiling 4,096, byte limit
configured by the operation). Borrowed buffer lifetimes are explicit; retention
across calls requires ownership transfer or copying. Variable-sized coordinates
and symbolic payloads cannot evade the byte budget through references.

### 3.3 Changes, provenance and progress

Rho emits transaction-grouped inserts, replacements and removals of graph
components, with before/after images or retrievable revision references under
the feed lease. State corrections expose the complete changed regions. A consumer
must not reconstruct the previous value by reading the latest state later.
Cross-partition groups have a completeness boundary before the feed advances
its covered cut; one row appearing is not evidence that the group is complete.

Sigma translates these changes into its computational deltas. Signed weights,
annotation algebras, duplicate consolidation, dependency tracking and recursive
deletion algorithms live there. Database multiplicity preserves distinct IDs;
it does not count proofs or decide which derived fact survives a retraction.
DBSP informs that consumer execution design and the need for complete changes,
not a second relational runtime inside rho.
[DBSP, §§2–6](https://mihaibudiu.github.io/work/budiu-vldb23.pdf)

Keep three types and authorities distinct:

- Source completeness: an ingestion/source contract interpreted by sigma.
- Database cut: rho's certificate of visible complete transactions.
- Computation frontier: sigma's accounting of input, iteration and outstanding work.

Rho can retain source/computation markers as data without asserting that they
prove completeness. Its feed completion means the selected database changes
through a cut are complete, not that sigma reached a fixpoint. Conversely, an
idle reasoning worker cannot certify a database snapshot. Naiad provides
precedent for the separate computation-progress model.
[Naiad, §§2–3](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/11/naiad_sosp2013.pdf)

Sigma writes derived records together with input cut, program/profile version
and derivation identity. Rho commits records and coverage metadata atomically;
sigma decides whether those results answer a requested input cut and whether
to wait, update or recompute. Rho supports retention pins and reports expiration;
sigma owns proof dependencies and inference-related retention requirements.

## 4 Storage and cost

### 4.1 Specialized layouts

Group segments by schema and compatible temporal profile. Within them use typed
blocks for exact points, spans, region fragments, uncertain placements and
symbolic payloads. Profile IDs, common axis, units, boundary conventions and
commit references can be block metadata. Do not split every small variation into
a tiny segment: block occupancy and metadata amplification are measured.

| Physical case | Coordinate payload before compression | Additional costs |
|---|---|---|
| Exact point with int64 codec | One 8-byte coordinate | Identity, values and revision/index costs still exist |
| Finite span with int64 codec | Two 8-byte bounds | Boundary bits can be shared or packed |
| Uncertain point | Nominal coordinate if present plus support/constraint representation | Shared calibration/correlation dictionary; sparse overrides |
| Rational point | Inline scaled integer when exact, otherwise fraction reference/payload | Variable precision is paid only by blocks/values using it |
| Repeated or symbolic scope | Descriptor/reference plus exceptions | Rho pays storage/access costs; sigma pays expansion/solver costs |

These are payload arithmetic, not a B/fact benchmark. v4 already omits all-zero
segment sections; “remove one endpoint” does not guarantee eight compressed
bytes saved. Compare homogeneous blocks, sparse extension columns and tagged
mixed blocks on identical facts. Representation density is a physical choice.

Use byte arenas, typed numeric slices, segment-local dense ordinals and dictionary
codes. No resident pointer-bearing Go object per sealed fact. Variable values
use offsets into bounded pages. A common profile is statically specialized;
reflection and per-row interface calls are excluded from measured hot paths.
The public row adapter remains available at its documented materialization cost.

The partition's replicated mutation log is authoritative. It feeds an MVCC
memtable and immutable column segments with a durable manifest. Avoid an
independent graph WAL plus a second independently committed metadata store.
Consensus hard state and log records may have their own required storage format;
the ownership of each durability boundary must be explicit.

Catalog/location/index structures are paged, not unbounded Go maps. A Pebble
catalog is a candidate to compare with Badger under the same workload, not a
second public engine. Its checkpoint is derived from the applied partition log
and manifest; it cannot independently acknowledge graph truth.

State histories use normalized regions plus bounded patches/checkpoints.
Compaction preserves retained cuts, assertion multiplicity, knowledge and
provenance. Long-lived spans get a proper interval access path; partitioning
solely by start time must not duplicate or miss every long interval.

### 4.2 Access paths

Identity, adjacency, commit/revision and temporal access structures have separate
contracts. Points use ordered coordinate postings; linear spans use a paged
interval structure with sound bound pruning; causal facts use ordinary graph
adjacency indexes. Symbolic regions may expose validated conservative bounds;
their residual evaluation stays in sigma unless a native predicate is declared.
Use the selected relation's predicate: an overlap index cannot prune “before.”
Uncertain placement indexes cover the possible support, not only the nominal
coordinate.

Maintain partitioned in/out adjacency postings independent of time. The canonical
relationship has one owner; endpoint postings are transactionally maintained
in the reference design. A derived asynchronous posting is allowed only with
explicit coverage and a complete delta/fallback path. It cannot silently omit
remote relationships.

Current-state and historical-state accelerators are separate. The current-state
gate uses 10 active relationships and 10,000 then 100,000 ended ones: no ended-row
payload decoding, with page visits and correction overlays measured. Arbitrary
historical reads need their own access structures; current-state measurements
do not prove their complexity.

### 4.3 Bounded resources and integrity

Budget memtables, caches, dictionaries, indexes, lock tables, active transactions,
read/change buffers and retained database metadata. Access paths spill or
backpressure before exceeding budgets. Database-wide accounting prevents one
“bounded” pool per partition/scan from exceeding rho's assigned allowance.
Sigma separately budgets joins, arrangements, proof dependencies and solvers.
The embedding application allocates their combined process allowance; rho does
not set the application's process-wide GOMEMLIMIT.

Open verifies paged roots and replays a bounded retained tail; page integrity is
checked before use, with separate full scrub. Manifest growth, disk catalog size
and OS page cache remain measured costs. “Bounded resident engine memory” does
not mean constant total storage or unlimited reasoning inside fixed RAM.

Per-partition log hash chains and segment integrity roots bind transaction IDs,
profile versions and effects. Cross-partition decision records bind participant
effect digests. There is no fictitious single global hash-chain predecessor.
Canonical hashes use stable semantic names/IDs rather than local dictionary
numbers. Replicas agree on logical effects; local compaction layouts may differ.

## 5 Distributed transactions and snapshots

### 5.1 Ownership and failure model

Logical partitions own disjoint primary records and have replicated logs.
Default distributed durability is three replicas with majority durable
acknowledgement; deployment may choose another declared replication policy.
Fault model: crash/recovery, delayed/duplicated/reordered messages and partitions,
not Byzantine agreement. A quorum loss can make affected work unavailable;
it cannot authorize stale data labelled fresh.

Routing uses versioned range/bucket ownership. IDs do not encode the current
server or timestamp. Allocate nonoverlapping ID blocks durably, then mint locally;
external references include GraphID. Leases/epochs fence stale allocators and
owners. No global counter request per object.

Default placement favors owner/component and source adjacency locality, with
split buckets for hot owners and explicit secondary postings. Node existence,
global uniqueness and remote endpoint lives add participants when needed.
Graph cuts and high-degree vertices have unavoidable communication/skew costs;
the benchmark includes them rather than assuming perfect locality.

Consensus, transactions and read-cut certification are separate mechanisms.
Use an established Raft implementation behind explicit log/VFS/transport seams;
do not implement a new consensus protocol. Raft by itself does not make a
transaction spanning groups atomic.
[Raft, §§5–6](https://raft.github.io/raft.pdf)

### 5.2 Transaction reference protocol

Use private writes, serializable locking/validation and recoverable two-phase
commit across replicated groups. Start with conservative locking: shared locks
for read keys/predicates and exclusive locks for mutations/constraints, retained
through durable decision and local installation. Predicate locks include empty
ranges, temporal overlap predicates, uniqueness keys and endpoint-life
dependencies. A coarse lock is an acceptable initial exact implementation;
silent omission of a dependency is not.

A transaction that begins from an immutable historical read cannot simply write
against it. It must reacquire/validate its complete read footprint against current
state or restart. Read-only snapshots use MVCC without retaining these locks.
Use a globally ordered transaction priority for deadlock prevention and bounded
retry; prepared locks survive failover until the decision is recovered.

1. Register TxID, request key, participating partitions and ownership epochs in
   a replicated coordinator record. Freeze the participant set before prepare.
2. Participants validate, durably replicate intents/read locks and a prepare
   vote containing their logical clock floor and effect digest.
3. If all vote yes, choose a commit round above every voted floor and dependency
   round, and persist COMMIT plus the round and all digests in the coordinator
   group. Otherwise persist ABORT. The decision is immutable.
4. Participants resolve from that durable decision, install the complete local
   effects and dependent postings, advance their logical floor, then release
   locks. Readers never expose undecided intents.
5. A durable decision permits acknowledgement when all prepares and the decision
   have quorum durability. Installation can finish afterward; readers wait or
   resolve it, never observe a half transaction. Retry by request key returns the
   same decision while its declared deduplication lease is retained.

One-partition transactions combine validation, decision and effects in one
replicated entry. No other data partition or global sequencer participates.
Revision 2026-10-09: no consumer runs partitions today (ai-soc embeds one
process on badger; sigma-tkgd uses single-primary change-log replication for
read replicas). The one-partition path is therefore a shippable product on its
own: if V2 does not pass, an embedded-first v5 release remains eligible after
its applicable local, migration, consumer and release gates pass. It keeps the
same value, access and change contracts with one-partition capability limits
explicitly advertised. This fallback does not complete V2, V5 or the distributed
parts of V6/V7: full V0–V7 completion still requires their distributed evidence.
A local simulator or prototype cannot certify those gates.
Cross-partition 2PC can still wait for a failed/unreachable participant or
coordinator quorum. Timeouts initiate decision recovery; they cannot unilaterally
abort a possibly committed transaction. Consensus replication improves recovery
but does not abolish these availability limits.
[Gray and Lamport, transaction commit](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2003-96.pdf)

### 5.3 Logical rounds and certified cuts

A commit has (logical round, TxID). Rounds advance from persisted partition
floors, dependencies and read fences, not wall clocks. Conflicting transactions
are serialized by the transaction protocol and receive increasing rounds.
Independent transactions can share a round; TxID is a deterministic enumeration
tie-break, not source causality or physical time.

A commit receipt is not a snapshot. In particular, a larger observed stamp
does not prove all smaller transactions are resolved. A database cut is a
certificate over a graph scope and topology version:

    Cut = graph identity + scope + closed round
          + retained partition coverage/certificates + descriptor versions

The public token is opaque; a compact token may refer to a durable certificate.
Partition-local log offsets remain internal cursor coordinates, never globally
comparable times. Historical system validity is revision visibility at cuts;
there is no mandatory explicit end timestamp on every stored event.

Reference cut protocol:

1. Pin a topology version covering the read scope. Gather authoritative logical
   high-water marks using leader/quorum reads. This includes durable committed
   decisions referenced by prepared intents, even before local application and
   even when the coordinator is outside the data scope. Choose round B at least
   as high as those marks and any After(receipt) requirement. An unavailable
   decision cannot be ignored to manufacture a fresh result.
2. Each covered partition logs a fence at B. New prepares and local commits
   after that fence must choose rounds above B. The fence is durable across
   leader changes; wall-clock leases are not the safety mechanism.
3. Transactions prepared before the fence may still decide at or below B.
   Resolve all such potentially relevant decisions; install every committed
   local effect <= B and remove aborted intents. An undecided old prepare blocks
   certification, even if its eventual commit round is not known yet.
4. Publish the partition's resolved certificate with its applied log position.
   A cut is usable only when every partition in scope has such coverage.
   Followers must apply through the certified positions before serving it.

A fence is a promise about future allocation; a resolved certificate additionally
accounts for older transactions. They are deliberately different states.
This is a proposed conservative protocol to model-check and fault-test, not a
claim that composing these mechanisms is already proven here. Closed timestamp
and resolved-intent distinctions in existing systems are useful evidence for
this obligation, not a correctness proof for rho-tkg.
[CockroachDB closed and resolved timestamps](https://www.cockroachlabs.com/blog/follower-reads-stale-data/)

Fresh takes this barrier after the request begins, covering writes acknowledged
before its authoritative collection. Stable reuses a certified older cut and
reports it. After(receipt) waits for a cut including that transaction.
At(cut) reproduces retained history or returns expired/unavailable explicitly.
Scope-limited cuts cannot silently expand during traversal; extend coverage at
the same B or restart. A graph-wide cut is the default when scope is unknown.

The baseline promises serializable read/write transactions and atomic snapshot
reads under these explicit modes. It does not equate commit-round order with
global wall-clock order or claim external consistency solely from timestamps.
If strict external ordering of all transactions is required, include a fresh
start barrier for them or a verified timestamp/ordering service and measure
its coordination cost. No HLC/TrueTime guarantee is assumed.

Background batched certification amortizes barriers and supports follower reads
and lets sigma choose certified input epochs. Lagging partitions or unresolved prepares can delay
a graph-wide fresh cut. Measure that tradeoff. A later dependency-vector cut
optimization must preserve atomicity and closure; it is not required in every row.

### 5.4 Rebalance, change feeds and operations

For a move/split: copy a retained certified base and log tail, fence the old
owner's writes/prepares, resolve its prepared work, replicate the handoff, then
activate the new owner through an epoch-checked catalog transition. Transfer
clock floors, decisions, deduplication state and pins. Old owners reject stale
epochs. Snapshot lineage locates retained data across the move; IDs never change.
Use joint-consensus membership changes within a group.

Change feeds use per-partition log cursors plus transaction envelopes and a
certified boundary. Cross-partition effects are grouped/deduplicated by TxID;
delivery of half a transaction cannot advance the consumer's completed cut.
Deterministic total enumeration is available after closure; source-causal order
is a separate relation interpreted by sigma.

The log is the recovery/change source, but consumer retention leases can
outlive recovery needs. Archive or retain required effects before reclamation.
Backups capture a certified cut, participant/catalog lineage, descriptors,
decisions and durable files. Restore must reproduce that cut across a different
topology. Erasure and retention explicitly delimit reproducibility; no promise
to retain erased proofs secretly. Object tiering changes placement only.

Embedded mode is one partition and optionally one replica, using the same record
and reducer contracts. A buffered submission is not a durable commit receipt;
distributed success requires the stated quorum durability. Crash-loss behavior
cannot be hidden behind the same acknowledgement type.

## 6 Worked acceptance examples

“Integration” cases exercise rho's data contract with a sigma evaluator or an
independent mathematical fixture. They do not assign that evaluator to rho.

| ID | Example and distinguishing answer | Ownership of the check |
|---|---|---|
| E01 | Two events at 12 yield two identities; neither implies a state after 12 | Rho identity/scope preservation; sigma chooses persistence rules |
| E02 | State A on [0,10), replace [3,7) with B: A on [0,3) and [7,10) remains; the old cut still returns A throughout | Rho state reducer and history |
| E03 | (0,1) is empty on Z, nonempty on Q; a derived 1/3 round-trips and compares exactly | Rho value algebra/codec/index; sigma produces rule-derived fractions |
| E04 | 11.9999999 and 12.0000001 differ by 200 ns; nearest µs yields the same derived coordinate, not the same event | Rho preserves exact values and IDs; explicit conversion is caller-selected |
| E05 | U=[11.9,12.1] is possible-only in [12,13), definite in [11,13); a 95% interval is not substituted for hard U | Rho typed support and native finite-support predicates |
| E06 | A=10+x, B=11+x share x in [-1,1]: B-A is exactly 1; independent envelopes lose that fact | Integration: rho retains shared constraint identity; sigma infers difference |
| E07 | A precedes C and B precedes C does not order A versus B | Rho preserves asserted edges and labels enumeration order; sigma evaluates causal consequences |
| E08 | (12,0) precedes (12,1), elapsed model time is 0; no fabricated nanosecond | Rho tuple order and coordinate preservation; sigma duration semantics |
| E09 | P=[0,4), Q=[2,6) join on [2,4); shift by 1/3 retains exact boundaries | Integration: sigma join/shift; rho exact read/write/index round-trip |
| E10 | 2<=b-a<=4 and 3<=c-b<=5 imply 5<=c-a<=9; adding c-a<=4 is inconsistent | Integration: rho constraint round-trip; sigma/independent STN evaluator |
| E11 | Removing one of two reachability paths preserves the result; removing the last retracts it; cycles also checked | Rho complete change feed; sigma recursive maintenance |
| E12 | Source A is complete through t, B is not: absence across both is not final despite idle workers | Rho preserves markers; sigma interprets source/computation completeness |
| E13 | A calendar day across DST differs from 24 elapsed hours; replay retains its rule version | Rho retains references; sigma calendar evaluation |
| E14 | Property TimeSpan does not set its owner's validity; uncertain occurrence is not state throughout its support | Rho property/scope/knowledge separation |
| E15 | A transaction changes partitions A/B; decision is durable, only A applied: certified read waits/resolves, never mixes new A with old B | Rho distributed transactions/cuts |
| E16 | Round 100 observed, older prepared transaction can commit at 90: max observed is not a cut certificate | Rho cut certification |
| E17 | Delayed old-owner write is rejected after transfer; an earlier cut yields the same IDs and values | Rho fencing, routing and history |
| E18 | Correction moves a current index entry; an old-cut read still finds the old position locally/remotely | Rho index/MVCC parity |
| E19 | Closed life masks life-bound edges without deleting evidence; reopening does not rebind; identity-reference event remains | Rho explicit lifecycle contract |
| E20 | A summary establishes Any(P,day), not All(P,day); corrections update affected derivations | Rho atomic summary/coverage storage and changes; sigma derivation/substitution policy |
| E21 | Projection-before-box, empty-window and annotation rules survive adaptation | Rho complete source histories; sigma IR/evaluator compatibility |

Also require all endpoint inclusivity combinations, infinities, singleton tails,
overflow/precision limits, disjoint scopes, malformed descriptors, mixed profiles,
null/absent, inconsistent knowledge, query cancellation and resource exhaustion.
Exact-set assertions include negative cases and historical predicates true only
during an earlier part of the queried interval.

## 7 Invariants and proof obligations

1. Physical representation, indexing, partitioning and tiering preserve semantic answers.
2. Identity, support equality, approximate matching and provenance equality are distinct.
3. Every native value predicate/read checks the domain/axis capabilities it requires.
4. Event observations do not imply persistence or source completeness.
5. Uncertainty, metric extent, codec error and quantization have explicit meanings.
6. Retained cuts reproduce prior assertions, descriptors and derivation versions.
7. A transaction is visible everywhere required by a read scope or nowhere there.
8. Published cuts contain no unresolved eligible transactions and admit no later commit below the fence.
9. Ownership and allocation fencing survive restart and delayed messages.
10. Database index/feed coverage never exceeds complete applied effects; stored sigma coverage remains explicitly caller-supplied metadata.
11. Changes identify complete replacements/removals and preserve multiplicity by identity; rho never infers proof counts from revisions.
12. Every data-growing database structure has a disk/spill/budget policy, including distributed metadata.
13. Fast-path queries equal the independent model; “current” never substitutes for requested history.
14. Unsupported, unavailable, expired, partial and exact-empty reads are distinguishable; stored uncertainty is not a read failure.

Integration obligations belong to sigma: incremental and batch reasoning agree;
surviving justifications persist after retractions; approximate, inconsistent,
unknown and incomplete inference results remain distinguishable. Rho's checks
establish that the required data and changes arrive without semantic loss.

Prove conflict serialization separately from atomic commit and cut closure.
Model the interaction of prepare/fence/decision/apply, not a guard that merely
restates the desired invariant. Include leader loss, replay, duplicated messages,
topology changes and predicate conflicts. Safety must not depend on two actions
happening “almost immediately” after one another.

## 8 Implementation sequence

Proposed Go ownership boundaries under github.com/data-insights-ai/rho-tkg/v5:
public temporal values/profiles, graph components, transactions and access;
internal codecs, value-predicate helpers, storage, indexes and replication.
The public consumer contract exposes typed batches, access capabilities,
statistics, cuts and changes without leaking storage/consensus dependencies.
The embedding application supplies transport/process orchestration; rho owns
database state machines and invariants. Sigma owns its adapter, planner, joins,
temporal/rule execution, solvers and distributed computation runtime. No reverse
dependency, reasoning-IR executor or solver package is introduced into rho.

Revision 2026-10-09: v5 code starts on branch `v5`, created from main `32568c4`
(v4.43.0 plus the `DeleteWithTx` / `UpdateWithTx` doors). The September draft,
its 16 fixtures/52 assertions, `reference/design_checks.py` and
`reference/README.md` have been recovered locally in `docs/v5/reference/`
(five files, including the historical draft). Recovery supplies reference
evidence; V0 requires a reviewed, tracked corpus and additional independent
models for the revised contracts. Carry forward current column codecs, integrity
checks, batch interfaces and trust-boundary tests where contracts match (the
list is in the review, §7); do not copy old transaction semantics (its must-not-
copy list, same section). v4 keeps taking consumer features (owner decision
2026-10-09); each v4 door present when V4 starts is part of the compatibility
baseline in §8a.

| Phase | Deliverable | Required exit evidence |
|---|---|---|
| V0 Specification | Specify temporal value/access contracts, rho/sigma ownership and deployment consistency; inventory consumer data; independent database/reference models | Every coverage row identifies preservation, native access and sigma evaluation separately; failure cases have expected outcomes; IDs/cuts and provisional formats reviewed together |
| V1 Values and state | Typed profiles, exact rational codec/comparison, regions, state reducer, tuple ordering, finite-support predicates and descriptor preservation | Recovered 16 cases/52 assertions after corpus review (V0); rho-owned portions of E01–E10/E13–E14/E19–E20; independent boundary/differential tests, no solver/runtime implementation |
| V2 Distributed correctness slice | Two logical partitions, three replicas each in simulation; log/VFS, durable IDs, locks, decisions, cuts and ownership transfer; external transactional-KV comparison | E15–E18; independent serial-history/cut oracle; process-kill and message-fault schedules; no acknowledged durable loss or mixed transaction; engine selection recorded |
| V3 Compact engine | Arena memtables, typed blocks, paged catalog/indexes, seal/merge/recovery and budgets | Full byte ledger, no per-fact resident object growth, bounded replay; single-partition fast path compared with v4 |
| V4 Read and change API | Typed projected scans, temporal selections, complete histories, access negotiation/statistics, atomic changes and snapshot-to-feed handoff | Named/generic/row/column parity; corrections leaving a selector; replay and retention boundaries; sigma adapter contract fixtures preserve E09–E13/E21 inputs; sigma's pinned `access` contract compiles and mutation-then-historical adapter tests pass against v5 (§8a); signature/empty smoke tests are insufficient |
| V5 Integrated distributed access | Locality-aware routing, synchronous endpoint postings, temporal indexes, distributed scans/expansion, resumable cuts/feeds | All read doors agree across 1/2/4/8 partitions; cross-edge, stalled participant, rebalance and feed bootstrap cases; existing sigma workloads consume identical inputs/results |
| V6 Migration and operations | Importer, consumer migration contract (§8a), backup/restore, retention/erase and segment tiering | Canonical parity for defined v4 mapping, ambiguity report, restore on different topology, retained-cut/CDC agreement; sigma/ai-soc adapters validated in their own repos; the named semantic changes in §8a each have a consumer-side change recorded |
| V7 Release | Versioned public API/wire/descriptor contracts and operational limits | Full repository CI/coverage/race/security gates; distributed database matrix; existing consumer suites and agreed integration fixtures; measured capacity accepted |

V2 precedes format freeze and performance tuning. A runnable local prototype is
an implementation step, not a reason to postpone distributed architecture.
Sigma's new reasoning features have their own roadmap. This rho plan requires
the representation/access contract and agreed compatibility tests; it does not
claim that every research evaluator exists or schedule those implementations
as rho deliverables. Integration failures must be attributed to the owning layer.

### Acceptance evidence checklist

Each phase exit attaches reproducible commands, commit IDs, inputs and output
artifacts to its review. This checklist specifies acceptance; current completion
and missing evidence are recorded only in [the backlog](../../tasks/backlog.md).
Reference checks establish their stated subset, never production completion.

| Phase | Concrete acceptance record |
|---|---|
| V0 | Reviewed tracked reference corpus; profile/axis unit and exact ms import mappings; versioned consumer door and capacity inventory; independent model inputs/expected failures; agreed limits distinguished from proposed thresholds |
| V1 | Go differential runs against the unchanged 16/52 corpus and revised E-case models; node/relationship two-phase exact-set tests, boundary/fuzz/codec failures and direct public API coverage |
| V2 | Two-partition/three-replica fault schedules plus real durable process-kill/restart runs; serial-history/cut oracle results for E15–E18; stale-owner/allocator rejection, decision recovery and predicate-conflict tests; comparable transactional-KV evaluation and engine decision |
| V3 | Byte ledger at 790 K/3.15 M/12.6 M source signal rows (107,113/408,282/1,584,150 HOP relationships); seal/merge/crash/reopen oracle results; heap/RSS/page-cache and replay measurements proving budgets and no per-fact resident growth; equivalent v4 fast-path comparison |
| V4 | Named/generic/row/column exact-set historical parity; byte-limit/cancellation/borrow-lifetime tests; selector-exit before/after groups, lease expiry, replay and concurrent snapshot/feed handoff; pinned sigma access-contract build and mutation-then-historical tests, including nonempty `NodeByIDAt` tests that fail if options are ignored |
| V5 | 1/2/4/8-partition read/expand/feed result parity, three replicas and specified cross-partition mixes; stalled participant, cross-edge, pagination, leader loss and rebalance schedules; sigma workload parity with pinned versions |
| V6 | Writer/format-provenance import corpus, canonical parity and explicit ambiguity report; retained-cut/CDC agreement after backup/restore to a different topology, retention/erase/tiering checks; sigma/ai-soc repository commits and adapter tests for every semantic change |
| V7 | Versioned API/wire/descriptors and documented limits; full version-matched CI, coverage/race/security output; distributed matrix and pinned consumer suite results; reproducible space/time comparisons against all three vendors and v4 at declared V0 resource profiles, excess-cost exceptions reviewed, proposed thresholds resolved explicitly |

### 8a Consumer compatibility (Revision 2026-10-09)

The compatibility baseline is pinned to sigma-tkgd commit
`6aadc2b3651f68ad43bc2704e7820602651d4403` (rho v4.43.0) and ai-soc commit
`9a26e689aa3cc8576e7235210a23d85bc8ca6416` (engine rho v4.40.0,
sigma v0.10.1). The earlier 258-file/14-package and 27-file counts describe the
review snapshot; they are not a fresh inventory measurement at these pins.

Sigma's Cypher adapter boundary includes `internal/cypher/core/access/contract.go`,
`contract_optional.go` and `contract_runtime.go`: Catalog, Scan, Lookup,
EqualityIndex, OrderedIndex, Adjacency, Statistics, Snapshot, Columns, Epoch,
Lend, Ordinals, Degrees, CountsAt, HistoryCounter, CommittedPin, RelColumns,
EndpointOrdinals, ScanOrder and IndexMaintenance, plus writer/transaction/batch,
CostStater/ColumnStatement, Epochs/EpochFold, Snapshots, RelWalk, OrderedScan and
RuntimeSource. The Tyla adapter also needs `ScanRelSegments`, `ScanNodeColumns`,
`ByLabel`/`ByType`, `*ForNodesAtPin` and `Resolve()`. ai-soc embeds badger with
`AllowTxBackfill`, `RelSegments` and `SegmentDir`.

Pinned inventory: [consumer-contracts.json](reference/consumer-contracts.json);
migration obligations: [consumer-migration.json](reference/consumer-migration.json).

The existing `contract_test.go` checks signatures and empty smoke behavior;
its `NodeByIDAt` probe ignores options. V4 requires nonempty, mutation-then-query
historical tests with exact sets and negative assertions at the pinned adapters.
A build or that smoke suite alone cannot establish semantic compatibility.

1. Keep the names: `types.NodeID` / `RelID` stay opaque 64-bit IDs,
   `types.Instant` is the int64 millisecond codec of the default axis,
   `Node` / `Relationship` accessor names and `graph.QueryOpts` field names stay.
   Most consumer sites then compile with an import-path change.
2. sigma's pinned `access` contract and adversarial historical adapter tests
   are v5 acceptance fixtures (V4 exit evidence).
3. Optional `compat` façade over the v5 engine for the v4 sub-API doors
   (`g.Nodes()`, `g.Rels()`, `g.Temporal()`, `g.Replication()`, …), shipped with
   v5.0, retained (and optionally deprecated) throughout v5. Removing a shipped
   public façade at v5.1 would break the major-version API contract; removal
   belongs in the next major. An earlier retirement requires an explicitly
   agreed consumer-only contract that never promises a public v5 API. It cannot
   bridge semantics v5 changes on purpose: automatic event inference from
   one-tick spans is prohibited and genuine spans retain their interpretation;
   `TxAt` no longer implies valid-at-now, other readers cannot observe a
   `GraphTx`'s private writes, and IDs no longer encode placement. Those changes are listed per consumer in V6.

Evolving v4 into v5 in place is not planned: the ID, time-type and transaction
changes break the `pkg/types` shapes; the isolated branch with the three levers
above keeps the edge thin.

### Performance gates

Owner acceptance, 2026-10-09: handle large datasets efficiently in both space
and time. Space should be at least as efficient as comparable supported
Neo4j, TigerGraph and Memgraph configurations, preferably better. Any excess
requires a quantified, causally isolated justification and explicit review;
extra temporal features do not excuse unexplained base-graph overhead. There is
no fixed deployment or absolute SLO. Use declared resource profiles and measured
space/time curves rather than inventing an owner-approved machine or latency cap.
The [comparison protocol](reference/graph-db-comparison.json) records official
vendor deployment constraints and the required experiments; it contains no results.

Compare identical basic directed property graphs first: stable logical node/edge
IDs, parallel-edge multiplicity, labels, exact common property types, values and
index/query obligations. Report logical identities, property entries/payload bytes,
source rows and physical records separately. History, exact temporal values,
uncertainty/provenance and distributed consistency are additional lanes with
lossless mappings and correctness oracles; native support and explicit reification
are identified separately. Count every retained revision and auxiliary record.
Do not compare a latest-only graph with rho's retained history as if they stored
the same information, or use unsupported vendor settings to claim a win.

Pin versions, editions, licenses, image digests, storage formats, queries, index
sets and acknowledgement/fsync/replication policies. Measure online durable ingest
separately from offline bulk load and memory-only modes; include checkpoint,
compaction, restart and cold/warm reads. Repeat each comparable configuration at
least five times with randomized order, report dispersion and p50/p95/p99 ingest,
commit and read latencies, useful completed throughput, failures and exact outputs.
Include all services, clients and replicas with separately attributed costs.
The byte ledger includes heap/allocator usage, RSS/PSS, mapped pages, OS page cache,
disk data/index/WAL/catalog/provenance and replica totals, at steady state and peak.
These views overlap: report a non-double-counted cgroup/host memory total alongside
the breakdown, never add heap and mapped pages to RSS as independent consumption.

An excess-cost ledger identifies the comparator, workload, bytes/latency delta,
required semantic benefit, feature-on/off or equivalent component evidence,
alternatives and review disposition. Report base-graph inefficiency separately
from unavoidable retained-information costs; no exception is accepted merely by
naming a feature. Full acceptance requires actual reproducible comparisons against
all three vendors and reviewed exceptions; unavailable licenses/data remain an
explicit missing comparison, never a marketing-number substitute.

Use the existing 790 K, 3.15 M and 12.6 M synthday **source signal rows**,
which produce 107,113, 408,282 and 1,584,150 HOP relationships respectively;
keep source rows and graph facts separate in every byte/throughput denominator.
For representative consumer data, ai-soc reports one BO day of 58.8 M records
and one BA day of 36.9 M rows (Revision 2026-10-09; the earlier 22.8 M-row
figure is superseded). Its reported 400–700 rows/s with bursts is an unverified
arrival shape, not an accepted throughput target or v5 measurement. Add non-SOC data:
rational intervals, graphs with deletions, schedules, uncertain/correlated events
and fragmented state corrections. Measure their storage/access costs directly;
run supported reasoning workloads through sigma as a separate integration suite.

Test point/span mixes 100/0, 95/5, 50/50 and 0/100; uncertainty 0/1/10/100%;
ordered and late ingest; hot owners; long spans; rational denominators. Sigma's
join workloads include an output-size stress for the access interface.
Distributed database runs use 1/2/4/8 data
partitions, three replicas, 0/10/50/100% cross-partition transactions and measured
LAN/WAN delays. Report useful throughput, latency, output and completeness together.

Measure raw/compressed bytes, all indexes/logs/provenance/replicas, resident heap,
mmap/RSS/page cache, allocations/GC, locks, write/read amplification, network
bytes, coordination rounds and p50/p95/p99 latency. Attribute sigma's solver and
arrangement memory separately while also reporting combined process-tree cost.
Distinguish column scan, full graph read and complete sigma reasoning execution.

Hard gates: zero semantic divergence; every configured engine pool respects its
budget; no mandatory unused endpoint/uncertainty/microstep payload in exact-point
blocks; no full-data decode at open; no unbounded per-history RAM index;
no full database rescan to produce a retained local transaction's change group.
Sigma's incremental evaluation costs are separate integration measurements;
result/dependency work cannot be charged to rho's scan throughput.

Proposed review thresholds, neither measured results nor owner-accepted gates:
no >10% throughput/p99
regression against the semantically equivalent baseline without an explicit
tradeoff; >=70% scaling efficiency from 1 to 4 partitions for balanced independent
ingest and partition-local scans at equal per-partition resources. Global joins,
hot keys and cross-partition commit are measured separately, not forced to meet
an impossible locality assumption. Use at least five comparable runs and report
dispersion, hardware, replication and durability settings.

The owner set no fixed absolute capacity, latency SLO or deployment. V0 records
reproducible resource/dataset profiles for the comparative space/time goal; later
workload-specific SLOs require their own agreement. The historical 1 TB raw/day
figure is a stress target, not an achieved facts/s rate. Missing representative
data or vendor comparisons leaves acceptance pending, never an inferred success.

## 9 Migration, open choices and evidence

v4 has whole-entity millisecond metadata with zero sentinels and typed ISO
temporal property values; it has no native general interval property kind.
The importer explicitly maps these to assertion interpretation and axis/profile.
Never infer an event from a one-millisecond interval. Preserve raw source times,
exact property kinds, source IDs and original hashes as legacy evidence.

Revision 2026-10-09: a v4 row with `ValidTo == ValidFrom + 1` cannot be
classified at import from its width or dates alone. Commit 994df82
(2026-06-12, append-only cascade) removed the known eclipse writer; it does not
prove which binaries, imports or copied histories wrote a particular store.
Import one-tick rows as genuine spans only with verified writer/format provenance
that excludes sentinel encoding for those rows. Otherwise preserve legacy
evidence and report ambiguity; never infer an event. A history starting after
2026-06-12 alone is insufficient provenance. v4's
default unit is the millisecond; V0 declares the default axis unit per profile
and the importer's millisecond-to-unit mapping, preserving raw source times
(ai-soc carries 100 ns source times as a property).

v4 entity history does not establish a complete global atomic transaction history.
Sorting TxFrom cannot recover missing transaction boundaries: one `GraphTx`
spans one instant per mutation, batch creates share two instants, the ingest
applier merges groups into one execute, `validInstantAfter` bumps stamps above
the clock, and `TxFrom` is outside the integrity hash (review §2). Preserve known
per-entity order and source provenance; expose synthetic import cuts as synthetic.
Ambiguous/in-place/erased history is reported, with a latest-state import option.
New canonical hashes are versioned; old segment roots cannot become v5 roots.

| Remaining choice | Recommendation and closure condition |
|---|---|
| Fine-grained concurrency | Conservative serializable locks first; OCC/read-refresh only after equivalent model/fault tests and contention measurements |
| Fresh-cut cost | Batched logical fences and resolved certificates; compare timestamp-service and dependency-vector alternatives at V2/V5 before API freeze |
| Numerical and symbolic limits | Initial implementation limits specified: 64 KiB input/value/descriptor bytes, 4,096 magnitude bits and 4,096 region pieces; pending V1 adversarial validation before V2 format freeze, not performance acceptance; sigma sets solver limits separately |
| Physical block thresholds | Homogeneous fast paths with measured mixed/sparse alternatives; V3 determines thresholds |
| Catalog and consensus libraries | Benchmark/license/version review and fault-injection seams at V2/V3; public contracts hide implementation types |
| Build versus transactional substrate | [Substrate evaluation](reference/substrate-evaluation.json) is research and an execution plan only; no engine selected. Execute the comparable V2 spike before choosing against embedding, column access, failure semantics and cost requirements |
| Native predicate coverage | Select useful exact database predicates per profile; expose residuals explicitly; sigma owns broader evaluation and solver-fragment guarantees |
| External consistency | Baseline modes as §5.3; stronger real-time ordering must select and measure an explicit protocol |

Current code evidence: rho's AGENTS.md and docs/architecture.md define the pure
storage/type library; sigma's internal/query/ir/time.go and program_time.go own
temporal rule semantics. Also inspected: main CHANGELOG v4.43.0, pkg/types/temporal.go,
temporal_value.go, pkg/graph/internal/segment/segment.go, sharded store/log
contracts and docs/architecture.md. v4's sharded batch contract is atomic per
shard, not cross-shard; v5 cannot inherit a stronger guarantee by renaming it.
Current segment work supersedes the old plan's proposal to stop v4 at S1.

The recovered original oracle and design checks
([design_checks.py](reference/design_checks.py), scope in
[reference/README.md](reference/README.md), Revision 2026-10-09) are local
reference evidence, not an engine benchmark or distributed implementation proof.
Tracked-corpus acceptance and production gate status are recorded only in the backlog.
No engine code or production configuration changes in this design revision.
Implementation must follow direct public API coverage, node/relationship parity,
two-phase historical tests, exact-set adversarial assertions, errors.Is checks,
>=80% new-code coverage, make cover, race detection and version-matched full CI.
