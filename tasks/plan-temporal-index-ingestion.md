# TKG support for indexed temporal replay

Date: 2026-09-09. Status: planned; no implementation or performance claim.
Repository inspected: main at fdddc3d, v4.35.0 plus local modifications.
Consumer plan: [AI-SOC](../../ai-soc/project-2026-09-01-BO/doc-20260905/temporal-index-plan.md).

Amendment 2026-09-09 (René): R1 sweeps 1 and 4 producers only; the 8 and 16
producer runs are dropped because AI-SOC P4 already measured that more writers
do not help the durable write path (0.58x-1.96x over 1->8). The saved time goes
to batch-size sweeps and exact physical write/flush counting under SyncWrites.

## Scope and existing capability

AI-SOC owns parsing, temporal partitions, fact equality, noise routing and source
references. TKG stores converged nodes/edges and temporal evidence; do not put BO
CSV logic or a source-file index in this library.

The current public ingest API already exposes NewSession, Submit, AppliedSeq and
WaitApplied. Strong mode prepares in parallel and applies commit groups through
the batch path. Concurrent mode applies on caller threads with per-entity atomicity,
not whole-group visibility. Snowflake ingest lanes already exist; reuse their API.

Important limits visible in pkg/graph/internal/core/ingest.go:
AppliedSeq includes failed attempts. Async failures are consumed on first wait and
can be evicted after 8,192 retained failures. Concurrent Submit returns its outcome
directly. None of these process-local tokens is automatically a durable AI-SOC
checkpoint. Badger flushIfNeeded still flushes each mutation in SyncWrites mode;
a high-level batch name is not evidence of one physical durability operation.

## Smallest useful work order

- [x] R0 (evidence: tasks/evidence/temporal-index/20260909-stream2/result.md): Build a synthetic consumer-shaped benchmark using the current API:
  first-seen identities, repeated-fact timestamp updates, occurrence/inbox writes,
  hot shared endpoints and disjoint endpoints. Read complete involved APIs before
  proposing changes. Record backend options and exact source revision.
- [x] R1 (same evidence; 8/16 dropped per amendment): Compare existing transaction path, synchronous strong ingest sessions and
  synchronous concurrent sessions. Sweep 1, 4, 8 and 16 producers, bounded batch
  sizes, same durable workload and output. Count physical writes/flushes and measure
  commit latency, RSS and scaling with retained history. Separate creates from
  read/modify/write convergence; insert sessions are not an atomic MERGE operation.
- [x] R2 (decision in the same evidence: one-writer strong Sync session, batch 128, tx door for atomic RMW; R3 condition demonstrated): Prefer the existing mode that passes correctness and improves the consumer.
  Initial AI-SOC integration may use one writer. Do not implement more lanes or a
  new batch API unless R1 demonstrates a missing contract or material bottleneck.
- [ ] R3, conditional: If durability cost dominates, specify the narrow group-commit
  contract first: accepted mutations and associated metadata durable before success;
  failure cannot advance the consumer cut. Distinguish atomic visibility, partial
  batch errors and physical durability. Inspect registry/index/history/change-log
  writes and concurrent flushes together. Reuse existing transaction/store machinery.
  Never achieve a speed claim merely by turning off SyncWrites.
- [ ] R4, conditional: If async ingestion is required, fix outcome ambiguity before
  relying on it. Repeated waits and expired outcomes must never turn failure into
  success; return an explicit unknown/expired outcome or retain a bounded durable
  result contract. Today's consumer can avoid this extension with synchronous calls.
- [ ] R5: Qualify only the chosen storage mode for the AI-SOC replay adapter.
  Publish tested options, success/failure semantics and benchmark evidence.
  Update shipped API documentation/changelog only for implemented changes.

## Tests that must break incorrect implementations

| Case | Required outcome |
|---|---|
| Two workers create the same logical identity | One identity; deterministic conflict/retry, no lost occurrences |
| Repeated updates on one fact | Exact times, gaps and multiplicity retained |
| Invalid operation among valid operations | Documented per-operation outcome; consumer checkpoint cannot skip failure |
| Concurrent reads during group apply | Never represented as a complete analysis cut until publication barrier |
| Kill before/after durable acknowledgment | All acknowledged work survives; replay is idempotent |
| ENOSPC, flush/registry/index failure | Error reaches caller; no falsely committed cut |
| Backpressure, close, cancellation | Bounded buffers; blocked callers terminate; no accepted work disappears |
| Snowflake lanes and restart | Unique IDs using library generators; event time stays separate |
| Async repeated waits and eviction, if used | A previous failure never becomes reported success |

Use an actual killed child process, not only clean Close/reopen. Process-kill tests
do not establish power-loss guarantees; verify the required sync contract separately.
Keep node/relationship, memory/Badger and temporal-history parity. Concurrent changes
need race tests; public API changes need direct tests and repository coverage gates.

## Exit and deferrals

No mandatory TKG code change is assumed. Exit with either an evidence-backed existing
API integration or a narrowly tested fix to the demonstrated gap. Do not redesign
sharding, replication, retention or the library's temporal model for this BO scan.
Time partitions describe source events; they must not be inferred from Snowflake
creation times. AI-SOC owns a durable replay cursor and completed analysis cut.

