package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.CreatedRollbackCapability = (*Store)(nil)

// Created-entity rollback scope (CUTCOST H1, store.CreatedRollbackCapability).
// A transaction's rollback deletes the nodes it created with their legs and
// history. Those doors read every event shard: the untyped mint fan-out
// (connectedRelIDs) and the history walk of an entity that no longer lives
// anywhere (TruncateNodeHistory), each opening every cold shard, on no meter.
// But what the transaction wrote lies only on shards written since it began.
// A shard is sealed before it closes and loses its seal before any write, and
// each seal after a writable handle takes the next sequence, so a shard that
// is closed with its counts and a sequence at or below the transaction's
// stamp was not written since: the rollback skips it. A node created under
// an ID an earlier, deleted node used keeps that earlier life's history on
// such shards.

// WriteStamp returns the stamp a transaction takes before its first write.
func (ts *Store) WriteStamp() uint64 { return ts.sealSeq.Load() }

// ScopeCreatedSince scopes the store's deletes and history walks to what was
// written after stamp, until end is called. The caller holds the graph's
// exclusive lock for that time (a transaction's Rollback: every read door and
// every mutation of the graph waits on it), so nothing else reads or writes
// the store in the scope; a caller using the store beside the graph must not
// read it meanwhile.
func (ts *Store) ScopeCreatedSince(stamp uint64) (end func()) {
	ts.createdSince.Store(stamp + 1)
	return func() { ts.createdSince.Store(0) }
}

// sealedBeforeCreated reports whether es is closed, sealed at or before the
// stamp of the rollback in progress: it holds nothing that rollback removes.
func (ts *Store) sealedBeforeCreated(es *EventShard) bool {
	since := ts.createdSince.Load()
	return since != 0 && es.cachedCounts() != nil && es.sealedSeq.Load() < since
}
