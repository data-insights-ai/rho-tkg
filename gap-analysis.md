Gap Analysis: tkg-2025-v2 vs rho/tkg-v3

  I've compared every feature in v2's pkg/graph and pkg/storage against what exists in v3. Here's what's missing,
  prioritized by severity.

  ---
  CRITICAL — Core Graph Operations Missing

  ┌─────┬──────────────────────────────┬───────────────────────────────────────────────┬───────────────────────────────┐
  │  #  │           Feature            │                      v2                       │              v3               │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 1   │ UpdateNode(id, updates)      │ Full copy-on-write with version bump          │ Missing entirely              │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 2   │ UpdateRelationship(id,       │ Same COW pattern                              │ Missing entirely              │
  │     │ updates)                     │                                               │                               │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 3   │ RemoveNodeProperty /         │ Idempotent remove ops                         │ Missing                       │
  │     │ RemoveNodeLabel              │                                               │                               │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 4   │ Version History              │ appendNodeHistory, GetNodeHistory,            │ Missing — no history tracking │
  │     │                              │ HistoryStore interface, TruncateHistory       │  at all                       │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 5   │ Version Chains               │ VersionChain, AddNodeVersion, linked list of  │ Missing — version uint32      │
  │     │                              │ versions                                      │ exists but never incremented  │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 6   │ Batch Operations             │ ExecuteBatch, BatchBuilder fluent API,        │ Missing — no batch API        │
  │     │                              │ BatchConfig/BatchResult                       │                               │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 7   │ GetAllNodes /                │ On Store interface                            │ Missing from Store interface  │
  │     │ GetAllRelationships          │                                               │                               │
  ├─────┼──────────────────────────────┼───────────────────────────────────────────────┼───────────────────────────────┤
  │ 8   │ GetByIDs([]string)           │ Bulk fetch by IDs                             │ Missing                       │
  └─────┴──────────────────────────────┴───────────────────────────────────────────────┴───────────────────────────────┘

  v3 can create and delete but cannot update. This is the biggest gap — updates are the most common graph operation.

  ---
  IMPORTANT — Expected for a Temporal Graph Engine

  ┌─────┬───────────────────┬─────────────────────────────────────────────────────────┬────────────────────────────────┐
  │  #  │      Feature      │                           v2                            │               v3               │
  ├─────┼───────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────┤
  │     │                   │ GetNodesValidAt(t), GetNodesValidDuring(start,end),     │ Missing — temporal metadata    │
  │ 9   │ Temporal Queries  │ GetNodesByLabelValidAt, GetNeighborsValidAt,            │ exists but is never queried    │
  │     │                   │ GraphSnapshot                                           │                                │
  ├─────┼───────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────┤
  │     │ Hash Chain        │ VerifyNodeHashChain, VerifyRelationshipHashChain with   │ Missing — Hash/PrevHash fields │
  │ 10  │ Verification      │ SHA-256 content hash computation                        │  exist but are never computed  │
  │     │                   │                                                         │ or verified                    │
  ├─────┼───────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────┤
  │ 11  │ Hash Chain        │ ComputeNodeContentHash, ComputeRelationshipContentHash  │ Missing — integrity struct is  │
  │     │ Computation       │                                                         │ a dead field                   │
  ├─────┼───────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────┤
  │ 12  │ Property Indexing │ PropertyIndexManager, CreateIndex/DropIndex,            │ Missing — only label/type      │
  │     │                   │ GetByLabelAndProperty                                   │ indexes exist                  │
  ├─────┼───────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────┤
  │ 13  │ Context-Aware     │ AddNodeWithContext, UpdateNodeWithContext, etc. with    │ Missing                        │
  │     │ Operations        │ timeout/cancel                                          │                                │
  ├─────┼───────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────┤
  │ 14  │ Relationship      │ FromNodeHash, ToNodeHash on RelationshipIntegrity for   │ Missing — RelIntegrity only    │
  │     │ endpoint hashes   │ cross-validation                                        │ has Hash/PrevHash              │
  └─────┴───────────────────┴─────────────────────────────────────────────────────────┴────────────────────────────────┘

  ---
  MODERATE — Supporting Infrastructure

  ┌─────┬────────────────────┬──────────────────────────────────────────────────┬──────────────────────────────────────┐
  │  #  │      Feature       │                        v2                        │                  v3                  │
  ├─────┼────────────────────┼──────────────────────────────────────────────────┼──────────────────────────────────────┤
  │     │                    │ TemporalIndex (interval tree),                   │                                      │
  │ 15  │ Temporal Indexing  │ AdvancedTemporalIndex (Allen's algebra:          │ Missing                              │
  │     │                    │ overlapping, containing, before, after, during)  │                                      │
  ├─────┼────────────────────┼──────────────────────────────────────────────────┼──────────────────────────────────────┤
  │     │ Statistics         │ LabelStats with atomic LabelCount, RelTypeCount, │ Partial — has                        │
  │ 16  │ Subsystem          │  AllLabelCounts, AllRelTypeCounts, Rebuild       │ NodeCount/RelationshipCount but no   │
  │     │                    │                                                  │ per-label/per-type stats             │
  ├─────┼────────────────────┼──────────────────────────────────────────────────┼──────────────────────────────────────┤
  │ 17  │ Memory Management  │ MemoryManager background cleanup, MemoryConfig   │ Missing — no memory pressure         │
  │     │                    │ (MaxMB, cleanup interval, retention days)        │ handling                             │
  ├─────┼────────────────────┼──────────────────────────────────────────────────┼──────────────────────────────────────┤
  │ 18  │ Query Engine /     │ QueryEngine with queryCache for repeated queries │ Missing                              │
  │     │ Caching            │                                                  │                                      │
  ├─────┼────────────────────┼──────────────────────────────────────────────────┼──────────────────────────────────────┤
  │     │                    │ CRUDDiffOutput, Neo4j export,                    │                                      │
  │ 19  │ Export             │ Snapshot/Restore/Import/Export on Store          │ Missing                              │
  │     │                    │ interface                                        │                                      │
  ├─────┼────────────────────┼──────────────────────────────────────────────────┼──────────────────────────────────────┤
  │ 20  │ UpdateNodeInPlace  │ Mutable update without history (for counters)    │ Missing                              │
  └─────┴────────────────────┴──────────────────────────────────────────────────┴──────────────────────────────────────┘

  ---
  NICE-TO-HAVE — Advanced Features

  ┌─────┬───────────────────────────┬───────────────────────────────────────────────────────┬──────────────────────────┐
  │  #  │          Feature          │                          v2                           │            v3            │
  ├─────┼───────────────────────────┼───────────────────────────────────────────────────────┼──────────────────────────┤
  │ 21  │ Vector Embeddings         │ VectorField on Node/Relationship                      │ Missing                  │
  ├─────┼───────────────────────────┼───────────────────────────────────────────────────────┼──────────────────────────┤
  │ 22  │ Time Granularity          │ TimeGranularity enum (ms→year), normalization,        │ Missing — uses raw       │
  │     │                           │ TimePoint struct                                      │ Instant int64            │
  ├─────┼───────────────────────────┼───────────────────────────────────────────────────────┼──────────────────────────┤
  │ 23  │ Allen's Interval Algebra  │ TemporalConstraint with 13 relations (before, meets,  │ Missing                  │
  │     │                           │ overlaps, during, etc.)                               │                          │
  ├─────┼───────────────────────────┼───────────────────────────────────────────────────────┼──────────────────────────┤
  │ 24  │ Recurrence Patterns       │ RecurrencePattern (iCalendar-style)                   │ Missing                  │
  ├─────┼───────────────────────────┼───────────────────────────────────────────────────────┼──────────────────────────┤
  │ 25  │ Signature/Author on       │ Signature []byte, AuthorID, Timestamp                 │ Missing — only           │
  │     │ Integrity                 │                                                       │ Hash/PrevHash            │
  ├─────┼───────────────────────────┼───────────────────────────────────────────────────────┼──────────────────────────┤
  │ 26  │ Systems/Authorization     │ SystemsTable for RBAC, signature verification         │ Missing                  │
  │     │ Table                     │                                                       │                          │
  ├─────┼───────────────────────────┼───────────────────────────────────────────────────────┼──────────────────────────┤
  │ 27  │ Snapshot/Restore          │ Full graph state save/load                            │ Missing                  │
  └─────┴───────────────────────────┴───────────────────────────────────────────────────────┴──────────────────────────┘

  ---
  What v3 Does BETTER Than v2

  Not all gaps are regressions. v3 makes genuine improvements:

  ┌─────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────┐
  │        Area         │                                        v3 Improvement                                         │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Memory efficiency   │ 80B Node vs v2's much larger struct (string IDs, exported fields, sync.RWMutex per entity)    │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Token interning     │ Labels/types compressed to uint16 — v2 stores full strings everywhere                         │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Binary key encoding │ Fixed-width, byte-comparable Badger keys with temporal clustering — v2 uses string            │
  │                     │ concatenation ("n:" + id)                                                                     │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Serialization       │ Msgpack with type-faithful reconstruction — v2 uses GOB (slower, Go-only)                     │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Write-skew          │ 256-shard entity locks with deadlock-free LockTwo — v2 has per-ID locks but no relationship   │
  │ prevention          │ endpoint locking                                                                              │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Cascade delete      │ Atomic DeleteNodeCascade under single lock — v2 has separate delete calls                     │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Property validation │ Recursive allowlist checking to depth 32 — v2 accepts anything                                │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ LRU dirty tracking  │ Version-aware flush with re-queue on conflict — v2 writes through synchronously (slower)      │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Async batched       │ 100ms flush loop with dedup buffer — v2 writes every op to Badger immediately                 │
  │ writes              │                                                                                               │
  ├─────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Snowflake IDs       │ Distributed, sortable, embeds creation timestamp — v2 uses caller-supplied string IDs         │
  └─────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────┘

  ---
  Recommended Priority for v3 Roadmap

  Phase 1 — Make it a usable graph (must-have):
  1. UpdateNode / UpdateRelationship with copy-on-write versioning
  2. PutBatch / bulk operations on Store interface
  3. GetAllNodes / GetAllRelationships on Store interface
  4. Hash chain computation on Add/Update (make integrity fields real)

  Phase 2 — Make it temporal (the whole point of TKG):
  5. Version history store (append old versions, query history)
  6. Temporal queries (ValidAt, ValidDuring, point-in-time lookups)
  7. Hash chain verification methods

  Phase 3 — Make it production-grade:
  8. Property indexes (CreateIndex, GetByLabelAndProperty)
  9. Per-label/per-type statistics
  10. Context-aware operations with timeout/cancel
  11. Export/Snapshot/Restore
  12. Memory management & history retention policies

  Want me to write this analysis to tasks/todo.md and start planning the implementation for any of these phases?


