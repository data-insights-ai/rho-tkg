package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ReimportedRelCapability = (*Store)(nil)

// NoteReimportedRel implements storecontract.ReimportedRelCapability
// (backlog 50) on the reference shard, which is always open and persistent,
// like the graph-layer meta keys: a relationship's lives may sit in different
// event shards, so no single event shard owns the note. The reference shard
// syncs the note before returning, so it is durable before the row the caller
// writes next to an event shard.
func (ts *Store) NoteReimportedRel(id types.RelID) error {
	if ts == nil {
		return ErrNilStore
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}
	return ts.refShard.NoteReimportedRel(id)
}

// ForEachReimportedRelID visits the reference shard's set.
func (ts *Store) ForEachReimportedRelID(fn func(types.RelID) bool) error {
	if ts == nil {
		return ErrNilStore
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}
	return ts.refShard.ForEachReimportedRelID(fn)
}
