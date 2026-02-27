# tkg-v3 — Current Tasks

## Completed: Graph Layer Phase 2A — Store, MemoryStore, AddNode/AddRel, Shadow Resolution

- [x] `NewPropertySlice(map[string]any)` — O(N log N) bulk loader, single allocation + sort
- [x] `SetProperties(ps PropertySlice)` on Node and Relationship — bypass per-property Set loop
- [x] SnowflakeID bridge methods on `nodeID`, `relID`, `entityID` — cross-package persistence keys
- [x] `Store` interface — pure persistence contract (PutNode/GetNode/DeleteNode, PutRel/GetRel/DeleteRel, index queries, adjacency, counts)
- [x] Sentinel errors: `ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`
- [x] `MemoryStore` — thread-safe in-memory store with hash-set adjacency indexes (O(1) insert/delete)
- [x] `Graph.AddNode(labels, props)` — auto-ID, bulk property loader, label resolution
- [x] `Graph.AddRelationship(type, start, end, props)` — auto-ID, bulk properties, endpoint validation
- [x] `Graph.DeleteNode(id)` — cascade-deletes all connected relationships (outgoing + incoming)
- [x] `Graph.DeleteRelationship(id)` — simple passthrough
- [x] Passthrough queries: `GetNode`, `GetRelationship`, `NodesByLabel`, `RelationshipsByType`, `NodeCount`, `RelationshipCount`
- [x] Shadow resolution: `ResolveNodeProperty` / `ResolveRelProperty` — all 15 `tkg_*` keys with nil-guards
- [x] Verification: `make ci` green, 96.7% coverage, race-clean, gosec clean, 256 total tests

## Next up

- Phase 2B: msgpack wire formats (nodeWire/relWire), Badger persistence
- Registry persistence to Badger (meta/label_tokens, meta/reltype_tokens)
- Update README.md and CLAUDE.md with Phase 2A changes
