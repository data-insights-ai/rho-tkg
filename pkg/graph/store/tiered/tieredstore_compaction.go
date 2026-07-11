package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CompactNodeHistory implements store.HistoryCompactionCapability on tiered.
//
// A node's whole version chain lives on ONE shard (history snapshots route to the
// same shard as the live entity; the primary-label class is immutable, so a chain
// never splits across shards — CLAUDE.md B33). The two writes go to two different
// places:
//
//   - The per-entity STUB (metaWrites) is written to the GLOBAL reference shard
//     via the store-level MetaSet. It MUST live there because the graph layer
//     reads it back through the store-level MetaGet, which tiered delegates to the
//     reference shard; a stub written into an event shard's meta would be
//     invisible to the point-door compaction gate.
//   - The TRIM is routed to the entity's owning shard through TruncateNodeHistory,
//     which already resolves the owner by snowflake-timestamp (checkout/checkin,
//     writable cold-shard open) and tolerates archived / deleted / multi-source
//     chains.
//
// The two writes are NOT a single atomic batch (they land on different shards),
// so ordering is chosen for the fail-closed direction: the stub is written BEFORE
// the trim. A crash between them leaves the stub present while the chain is still
// intact — the point-door gate over-rejects (ErrHistoryCompacted for a read that
// could still be answered) and Verify*Chain reports a transient boundary mismatch;
// both are conservative, and an idempotent re-run rewrites the identical stub and
// completes the trim. The reverse order could leave a trimmed chain with no stub —
// a silently-incomplete read — which this ordering makes impossible. The global
// watermark is advanced separately, once, by the core layer (also on the reference
// shard, and also over-stated ahead of the trims — see
// core/compaction.go advanceCompactionWatermark).
func (ts *Store) CompactNodeHistory(nid types.NodeID, keepVersions int, metaWrites []storecontract.MetaWrite) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}
	// Stub → reference shard FIRST (see the ordering note above).
	if err := ts.applyCompactionMetaWrites(metaWrites); err != nil {
		return err
	}
	// Trim → owning shard, via the robust multi-source router.
	return ts.TruncateNodeHistory(nid, keepVersions)
}

// CompactRelHistory is the relationship mirror of CompactNodeHistory.
func (ts *Store) CompactRelHistory(rid types.RelID, keepVersions int, metaWrites []storecontract.MetaWrite) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}
	if err := ts.applyCompactionMetaWrites(metaWrites); err != nil {
		return err
	}
	return ts.TruncateRelHistory(rid, keepVersions)
}

// applyCompactionMetaWrites routes the compaction meta writes (the per-entity
// stub) to the reference shard via the store-level MetaSet, so they read back
// through the store-level MetaGet. A nil value clears the key (the reference
// shard stores an empty value, which the stub/watermark readers treat as absent).
func (ts *Store) applyCompactionMetaWrites(writes []storecontract.MetaWrite) error {
	for _, w := range writes {
		if err := ts.MetaSet(w.Key, w.Value); err != nil {
			return err
		}
	}
	return nil
}
