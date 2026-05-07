package graph

// All store backend types and their constructors have moved out of pkg/graph
// into dedicated sub-packages:
//
//	memory.New()  / *memory.Store    pkg/graph/store/memory
//	badger.New(badger.Config{...}) / *badger.Store / badger.Config    pkg/graph/store/badger
//	tiered.New(tiered.Config{...}) / *tiered.Store / tiered.Config    pkg/graph/store/tiered
//
// Sentinel errors and admin types (tiered.ShardInfo, tiered.VerifyResult,
// tiered.RepairResult, tiered.MigrateFromBadger) live in their respective
// sub-packages. External callers must import the sub-package directly.
