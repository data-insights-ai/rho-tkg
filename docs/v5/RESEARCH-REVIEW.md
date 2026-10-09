# Research basis and architecture alternatives for rho-tkg v5

2026-10-09. The [architecture](PLAN.md) defines rho-tkg's temporal graph storage,
access and distributed consistency contracts. Reasoning papers are examined to
identify what sigma-tkgd needs from that database. **They do not move reasoning
execution into rho.** The recommendations are a synthesis, not a claim that one
cited system meets the combined representation, embedding and cost requirements.

## Temporal representation

**Hayes provides a catalogue of assumptions, not a universal database model.**
His report separates alternative temporal theories and concepts such as order,
duration, clocks and calendars. It supports making these choices explicit.
It does not justify choosing intervals, physical seconds or a single ontology
as mandatory for every fact.
[A Catalog of Temporal Theories, §§1, 3, 6](https://www.ihmc.us/users/phayes/docs/TimeCat96.pdf)

**Domain, axis and numeric encoding are distinct.** OWL-Time supplies vocabulary
for instants, intervals and temporal reference systems. PostgreSQL explicitly
distinguishes discrete range canonicalization from continuous treatment,
including finite-precision timestamps. These are useful precedents for separating
semantic positions from encodable endpoints; they do not prove a finite codec
represents all reals.
[OWL-Time §3](https://www.w3.org/TR/owl-time/#overview),
[PostgreSQL §8.17.7](https://www.postgresql.org/docs/18/rangetypes.html#RANGETYPES-DISCRETE)

**Order-dense does not mean unindexable.** Q is countable and has no immediate
successor in its usual order; R is uncountable and Dedekind-complete. A rational
grid can nevertheless be discrete. A database index orders represented values,
not every semantic position. Dense and discrete are not exhaustive alternatives:
mixed orders exist. The architecture therefore declares capabilities and domain
laws instead of inferring semantics from an integer field or dense array.

**Granularity includes meaning, not only units.** Bettini/Wang/Jajodia study
relations between granularities; Euzenat/Montanari discuss changing the
interpretation of statements across layers. This supports separate exact unit
conversion, quantization, aggregation and truth quantification. It does not
establish that a coarse summary preserves arbitrary fine-grained queries.
[Bettini et al., author repository abstract](https://air.unimi.it/handle/2434/242543),
[Euzenat and Montanari, §§3.1–3.3](https://exmo.inria.fr/files/publications/euzenat2005a.pdf)

**Alternative clock structures are legitimate representation requirements.**
The Tagged Signal Model allows events with structured tags. Ptolemy's superdense
time distinguishes model time from microsteps. MARTE distinguishes time bases,
clocks, units and resolution; CCSL studies relationships among clocks. These
motivate typed axes and product/order capabilities. They do not require UML,
mandatory microstep columns or a clock solver in every query.
[Tagged Signal Model](https://ptolemy.berkeley.edu/publications/papers/98/framework/),
[Ptolemy §1.7.2](https://ptolemy.eecs.berkeley.edu/books/Systems/chapters/HeterogeneousModeling.pdf),
[MARTE §9.2.3](https://www.omg.org/spec/MARTE/1.0/PDF),
[CCSL research](https://rex-radar.inria.fr/report/2016/aoste/uid55.html)

**Time-scale calculus and hybrid systems describe additional structure.**
A time scale is a nonempty closed subset of R; Q is not one in this sense.
Hybrid automata model continuous evolution and discrete transitions.
Neither follows merely from irregular observation gaps. If such models are used,
rho must preserve their dynamics/constraints as typed data or graph structures.
Sigma supplies the evaluator; flattening to endpoint snapshots is not generally
equivalent.
[Time-scale definitions §2](https://link.springer.com/article/10.1155/2009/209707),
[Henzinger, hybrid automata](https://www2.eecs.berkeley.edu/Pubs/TechRpts/1996/ERL-96-28.pdf)

**Uncertainty is knowledge about a placement, not its duration.**
Event-pattern research explicitly distinguishes imprecise timestamps from
events with duration. JCGM distinguishes measurement uncertainty, coverage and
resolution. These support bounded-support and probability annotations with
different contracts. A confidence interval is not automatically a hard bound;
correlated clock errors require joint information.
[Zhang, Diao, Immerman, §§1–3](https://vldb.org/pvldb/vol3/R21.pdf),
[JCGM uncertainty](https://jcgm.bipm.org/vim/en/2.26.html),
[coverage](https://jcgm.bipm.org/vim/en/2.36.html)

**Numerical proximity is not equality.** Floating-point rounding depends on the
computation and representation. An approximate comparison is a predicate, not
an equivalence relation suitable for hashing. The design retains exact
coordinates and explicit quantization/error policies; it does not install a
universal epsilon.
[Goldberg](https://docs.oracle.com/cd/E19957-01/806-3568/ncg_goldberg.html),
[PEP 485](https://peps.python.org/pep-0485/)

## Reasoning research and its implications for the database

**Interval relations and metric constraints need more than set membership.**
Allen's representation describes qualitative interval relations.
Meiri's framework combines qualitative and quantitative constraints;
tractable-subclass research demonstrates why one propagation algorithm must not
claim completeness for every relation network. Rho may classify located intervals
using endpoint comparisons; sigma owns solving networks of unknown positions.
Constraint identity and structure must survive database round-trip.
[Allen, original paper](https://cse.unl.edu/~choueiry/Documents/Allen-CACM1983.pdf),
[Meiri, qualitative and quantitative constraints](https://ics.uci.edu/~dechter/publications/r19-comb-qualit-quantit-constr-temp-rea.pdf),
[Nebel and Bürckert, ORD-Horn](https://www.dfki.de/en/web/research/projects-and-publications/publication/2523)

**Temporal graphs benefit from compositional operations.** Portal models evolving
topology and attributes and relates its graph algebra to temporal relational
operations. This informs sigma's temporal joins and rho's region values and
graph-specific access primitives. Its state-oriented graph model is not a full
account of uncertain events, provenance or database transaction visibility.
[Portal, §§2–4](https://arxiv.org/pdf/1602.00773)

**DatalogMTL needs exact domains and multiple evaluation strategies.**
The MeTeoR paper uses rational time and combines scalable materialisation with
automata techniques to obtain termination/completeness for its language.
Materialisation alone can require infinitely many rounds. Bounded-interval
research also exploits periodic canonical models. These findings justify exact
rational representation in rho and exact arithmetic/symbolic execution in sigma.
They do not require rho to implement MTL operators or a materialisation engine.
[MeTeoR, §§3–5](https://arxiv.org/html/2401.02869v1),
[Periodic materialisation research](https://ojs.aaai.org/index.php/AAAI/article/view/25807)

**Incremental execution can be modular.** DBSP models changes algebraically and
composes incremental operators for rich relational languages. Signed differences
are computational multiplicities, not uncertainty probabilities. The important
database consequence is to expose complete changes and stable identities.
Sigma owns delta operators and tests recursive maintenance separately from
simple counting; proof multiplicity is not a database revision count.
[DBSP, §§2–6](https://mihaibudiu.github.io/work/budiu-vldb23.pdf)

**Distributed computation has its own progress semantics.** Naiad combines
timestamped dataflow, iteration and progress tracking. Differential dataflow
uses structured logical time for incremental computations. Neither supplies
database cross-partition atomicity by itself. Sigma tracks computation frontiers;
rho certifies database cuts. Neither certificate can substitute for the other or
for a source-completeness contract.
[Naiad](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/11/naiad_sosp2013.pdf),
[Differential computation](https://www.microsoft.com/en-us/research/publication/composable-incremental-and-iterative-data-parallel-computation-with-naiad/)

Calendar recurrence and streaming completeness supply further independent
requirements: timezone rules affect calendar instances, and a watermark is a
policy/estimate or guarantee with a stated source contract, not proof obtained
from the absence of incoming messages.
[RFC 5545](https://www.rfc-editor.org/rfc/rfc5545),
[Dataflow model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)

## Distributed consistency and implementation

**Clocks, consensus and atomic transactions solve different problems.**
Lamport establishes the distinction between causal ordering and physical clocks.
Raft replicates a state machine, including membership changes. Gray/Lamport
separate transaction commit from consensus and explain two-phase commit's
blocking behavior. Together they motivate per-partition consensus, explicit
transaction decisions and separate visibility certification.
[Lamport](https://www.microsoft.com/en-us/research/publication/time-clocks-ordering-events-distributed-system/),
[Raft](https://raft.github.io/raft.pdf),
[Consensus on Transaction Commit](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2003-96.pdf)

**A timestamp alone is not a safe read cut.** CockroachDB's documented distinction
between closed timestamps and unresolved transaction intents is particularly
relevant. The rho-tkg proposal uses durable fences plus resolution/application
certificates, with logical rather than physical rounds. This is our proposed
composition and requires its own proof/testing. Spanner's external consistency
depends on explicit clock guarantees and protocol; those guarantees cannot be
imported simply by selecting a timestamp type.
[Closed and resolved timestamps](https://www.cockroachlabs.com/blog/follower-reads-stale-data/),
[Spanner](https://www.usenix.org/conference/osdi12/technical-sessions/presentation/corbett)

**Reusing a consensus library still leaves substantial engineering.**
The etcd Raft library exposes a state machine while storage and transport remain
application responsibilities. FoundationDB supplies a contrasting architecture:
an existing distributed transactional KV foundation, with deterministic
simulation integral to its engineering. Both deserve comparison; neither
automatically provides rho-tkg's native column storage and temporal access contract.
[etcd Raft](https://github.com/etcd-io/raft),
[FoundationDB, §§2–3 and simulation](https://www.foundationdb.org/files/fdb-paper.pdf)

**Compactness is measured across the full system.** Arrow describes different
union layouts; Parquet specifies useful column encodings. Neither establishes
our best layout. Indexes, duplicated endpoint postings, replication, decoder
buffers and pending transactions can exceed the timestamp payload. Sigma's
incremental arrangements add separate computation costs. v4's omitted sections and typed segment decoding
are a stronger local baseline than a naive two-int64 struct calculation.
[Arrow format](https://arrow.apache.org/docs/format/Columnar.html#union-layout),
[Parquet encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/)

## Architecture alternatives

| Alternative | Advantage | Material tradeoff | Decision |
|---|---|---|---|
| Universal two-endpoint rows | Simple uniform state path | Points, uncertain placements and symbolic scopes still need distinct interpretation; unused payload and dispatch | Keep as a measured baseline, not the logical foundation |
| Event log only | Efficient append and replay | State/window access and corrections require explicit database structures | Log is durability/change source; add native graph/history access and expose complete changes to sigma |
| Fully generic per-row object/plugin model | Easy apparent extensibility | Boxing, indirect dispatch, weak predicate guarantees and cluster compatibility | Use validated value/access contracts with typed storage and predicate paths |
| One global sequencer | Straightforward total visibility order | Global write coordination and failure/performance bottleneck | Comparison baseline; no global data sequencer in the recommendation |
| Only causal/eventual database convergence | Low coordination for independent updates | Cannot promise the selected cross-partition graph invariants and atomic cuts | Source causality stays supported; database integrity needs explicit transactions |
| External transactional KV under column segments | Reuses mature distributed transactions | Additional deployment, client/transaction limits, segment-publication coordination and possible duplicate storage | Required V2 buy-versus-build spike, not excluded on taste |
| Embedded partition engine with replicated commit protocol | Direct columns, local mode and controlled resource use | We own a difficult transaction/cut/rebalance integration | Recommended candidate, conditional on V2 correctness and comparative cost |

Revision 2026-10-09: the Pebble-versus-Badger catalog comparison must first
remove v4's per-row badger RAM maps (`relIDs`, `relRevs`, `typeIdx`, adjacency,
stats memos; `badgerstore.go:434-454`), or it measures those maps rather than
the catalog.

The external-KV comparison must use the same workload and durability. If it
meets embedding/deployment requirements and materially reduces implementation
risk without violating the cost gates, select it before format freeze. This is
one engine selection decision, not a commitment to maintain multiple divergent
public backends.

## Evidence limits

The central mathematical examples and old state fixtures have executable checks
in `reference/` (pending check-in under `docs/v5/reference/`, Revision
2026-10-09). Their ownership is explicit: value/state and
database-cut checks inform rho; STN, joins and recursive examples illustrate sigma
consumer requirements. They validate neither production implementations nor
workload performance or the full distributed protocol.

The references support specific distinctions and mechanisms. The Bettini entry
is an author-repository abstract; the Euzenat/Montanari chapter was inspected.
The MARTE clock passage checked is the indexed v1.0 §9.2.3 excerpt; the
[v1.3 record](https://www.omg.org/spec/MARTE/1.3/About-MARTE) was checked separately,
not its full specification. No broad taxonomy or complexity result is treated
as proof of this architecture. The numerical tradeoffs and implementation
selection remain the explicit V2–V5 gates in the plan.
