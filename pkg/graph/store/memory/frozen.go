package memory

// The rows the store publishes into ms.nodes / ms.rels are frozen copies
// (storedNode / storedRel in memorystore_ordinal.go, which also carry the
// dense ordinal). Frozen entries let query scan paths return the shared
// pointer instead of a per-row deep copy; any caller that mutates one fails
// fast (types.ErrFrozenNode or a panic from void mutators) instead of
// silently corrupting the store. Point reads (GetNode) still return mutable
// deep copies because graph-core write flows mutate what they fetch.
//
// The copy is in the compact frozen form (P7, types.CompactFrozenCopy): a
// first version's temporal and integrity metadata in one small object.
