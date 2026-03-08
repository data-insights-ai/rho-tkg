# Test Store Cleanup — Explicit MemoryStore + Organization

## Goal
1. Make MemoryStore usage explicit (not hidden behind `Config{}` default)
2. Clean up Badger test data properly (ensure t.Cleanup/defer Close)
3. Move misplaced BadgerStore tests to badgerstore_test.go
4. Convert tests that don't need Badger to MemoryStore

## Phase 1: Make MemoryStore explicit in helpers + graph-layer tests
- [x] Update `newTestGraph()` in batch_test.go
- [x] All `New(Config{})` → `New(Config{Store: NewMemoryStore()})` in graph-layer tests
- [x] All `graph.New(graph.Config{})` → explicit MemoryStore in export_test.go
- [x] v3061_fixes_test.go, v3062_fixes_test.go updated
- [x] pagination_test.go graph-layer tests updated

## Phase 2: Migrate unnecessary Badger tests to MemoryStore
- [x] graph_test.go: TestGraphWithBadgerInMemory → deleted (redundant)
- [x] graph_test.go: TestGraphBadgerUpdateNode → MemoryStore (renamed)
- [x] graph_test.go: TestGraphBadgerUpdateRelationship → MemoryStore (renamed)
- [x] graph_test.go: TestGraphBadgerDeleteNodeCascade → MemoryStore (renamed)
- [x] graph_test.go: TestGraphCloseIdempotent → deleted (duplicate)
- [x] graph_test.go: TestGraphCloseConcurrent → MemoryStore
- [x] graph_test.go: Concurrency stress tests → MemoryStore
- [x] export_test.go: TestExportImport_RoundTrip_BadgerStore → deleted (redundant)
- [x] bench_ingest_test.go: benchGraph → MemoryStore

## Phase 3: Move BadgerStore tests to badgerstore_test.go
- [x] foreach_test.go → badgerstore_test.go: 6 ForEach tests
- [x] pagination_test.go → badgerstore_test.go: 8 pagination tests + seedBadgerStore

## Phase 4: Verify
- [x] `make test` passes (7.9s, down from 9.2s)
- [x] `make test-race` passes (21.9s, down from 25.1s)
