package tiered

import (
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// This file exposes a curated set of unexported Store and EventShard
// internals so tests in pkg/graph can keep poking at them after the
// restructure. Production code must not call these helpers — they exist
// solely to keep the moves-only restructure reviewable without rewriting
// dozens of integration tests.

// --- Store field accessors ---

// RefShardForTest returns the reference shard's BadgerStore.
func (ts *Store) RefShardForTest() *BadgerStore { return ts.refShard }

// RefActiveReqsForTest returns the refcount on the reference shard.
func (ts *Store) RefActiveReqsForTest() *atomic.Int64 { return &ts.refActiveReqs }

// RefArchiveForTest returns the atomic pointer to the reference archive.
func (ts *Store) RefArchiveForTest() *atomic.Pointer[BadgerStore] { return &ts.refArchive }

// ArchiveActiveReqsForTest returns the refcount on the reference archive.
func (ts *Store) ArchiveActiveReqsForTest() *atomic.Int64 { return &ts.archiveActiveReqs }

// HotShardForTest returns the current hot event shard.
func (ts *Store) HotShardForTest() *EventShard { return ts.hotShard }

// EventShardsForTest returns the event-shard map. Caller must hold MuForTest.
func (ts *Store) EventShardsForTest() map[string]*EventShard { return ts.eventShards }

// CatalogForTest returns the shard catalog.
func (ts *Store) CatalogForTest() *ShardCatalog { return ts.catalog }

// ClosedForTest returns the closed flag (atomic.Bool).
func (ts *Store) ClosedForTest() *atomic.Bool { return &ts.closed }

// MuForTest returns the Store-level RW mutex.
func (ts *Store) MuForTest() *sync.RWMutex { return &ts.mu }

// CompressionForTest returns the configured compression algorithm.
func (ts *Store) CompressionForTest() options.CompressionType { return ts.compression }

// ZSTDLevelForTest returns the configured ZSTD compression level.
func (ts *Store) ZSTDLevelForTest() int { return ts.zstdLevel }

// SetIdleTimeoutForTest overrides the idle-timeout for cold shards. Tests use
// this to drive closeIdleShards quickly.
func (ts *Store) SetIdleTimeoutForTest(d time.Duration) { ts.idleTimeout = d }

// TempIdxLabelsForTest returns the tracked temporal-index labels under lock.
// Returns a fresh copy.
func (ts *Store) TempIdxLabelsForTest() []uint16 {
	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()
	out := make([]uint16, len(ts.tempIdxLabels))
	copy(out, ts.tempIdxLabels)
	return out
}

// HFBucketsForTest returns the tracked high-frequency index labels under lock.
// Returns a fresh copy.
func (ts *Store) HFBucketsForTest() map[uint16]time.Duration {
	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()
	out := make(map[uint16]time.Duration, len(ts.hfIdxBuckets))
	for tok, bucket := range ts.hfIdxBuckets {
		out[tok] = bucket
	}
	return out
}

// HasArchiveShardForTest reports whether the catalog has an archive entry.
func (ts *Store) HasArchiveShardForTest() bool { return ts.hasArchiveShard() }

// EnsureRefArchiveForTest forces lazy-open of the reference archive.
func (ts *Store) EnsureRefArchiveForTest() error { return ts.ensureRefArchive() }

// CloseIdleShardsForTest invokes the closeIdleShards loop body once.
func (ts *Store) CloseIdleShardsForTest() { ts.closeIdleShards() }

// CheckRotationForTest invokes the rotation check.
func (ts *Store) CheckRotationForTest() error { return ts.checkRotation() }

// ResolveShardStoreForTest exposes resolveShardStore.
func (ts *Store) ResolveShardStoreForTest(name string) (*BadgerStore, func(), error) {
	return ts.resolveShardStore(name)
}

// ShardForNodeForTest exposes shardForNode (reference vs event routing).
func (ts *Store) ShardForNodeForTest(primaryLabel uint16) *BadgerStore {
	return ts.shardForNode(primaryLabel)
}

// ShardForNodeIDForTest exposes shardForNodeID (the unpinned variant).
func (ts *Store) ShardForNodeIDForTest(nid types.NodeID) (*BadgerStore, error) {
	return ts.shardForNodeID(nid)
}

// ShardForRelIDForTest resolves the shard for a rel ID (checked variant; handles checkin internally).
func (ts *Store) ShardForRelIDForTest(rid types.RelID) (*BadgerStore, error) {
	shard, checkin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return nil, err
	}
	checkin()
	return shard, nil
}

// --- EventShard field accessors ---

// Name returns the event-shard name (e.g., "2026-W10").
func (es *EventShard) Name() string { return es.name }

// Path returns the on-disk relative path for the event-shard.
func (es *EventShard) Path() string { return es.path }

// Tier returns the shard's tier (hot, warm, cold).
func (es *EventShard) Tier() ShardTier { return es.currentTier() }

// SetTierForTest mutates the tier without rotating. Tests use this to
// simulate demote-to-cold without shipping bytes between shards.
func (es *EventShard) SetTierForTest(t ShardTier) { es.setTier(t) }

// ReadOnlyForTest reports whether the shard is marked read-only.
func (es *EventShard) ReadOnlyForTest() bool { return es.readOnly }

// Store returns the underlying BadgerStore (or nil for cold + closed shards).
func (es *EventShard) Store() *BadgerStore { return es.store }

// SetStoreForTest replaces the underlying store. Used by close-idle tests.
func (es *EventShard) SetStoreForTest(bs *BadgerStore) {
	es.store = bs
	if bs == nil {
		es.readTransientOpen = false
	}
}

// LockShardMuForTest / UnlockShardMuForTest expose the lazy-open mutex.
func (es *EventShard) LockShardMuForTest()   { es.shardMu.Lock() }
func (es *EventShard) UnlockShardMuForTest() { es.shardMu.Unlock() }

// CheckoutStoreForTest exposes the activeReqs-pinned checkout path.
func (es *EventShard) CheckoutStoreForTest(ts *Store) (*BadgerStore, error) {
	return es.checkoutStore(ts)
}

// CheckinStoreForTest decrements the activeReqs counter.
func (es *EventShard) CheckinStoreForTest() { es.checkinStore() }

// ActiveReqsForTest returns the activeReqs atomic counter.
func (es *EventShard) ActiveReqsForTest() *atomic.Int64 { return &es.activeReqs }

// SetLastAccessForTest mutates the lastAccess atomic counter (Unix ms).
func (es *EventShard) SetLastAccessForTest(t int64) { es.lastAccess.Store(t) }

// SetTimeEndForTest mutates the shard's window end. Tests use this to drive
// rotation paths without waiting for the actual ShardWindow to elapse.
func (es *EventShard) SetTimeEndForTest(t time.Time) { es.timeEnd = t }

// --- More Store field accessors ---

// ShardForRelIDCheckedForTest exposes the pinned rel-id router.
func (ts *Store) ShardForRelIDCheckedForTest(rid types.RelID) (*BadgerStore, func(), error) {
	return ts.shardForRelIDChecked(rid)
}

// ShardForNodeIDCheckedForTest exposes the pinned node-id router.
func (ts *Store) ShardForNodeIDCheckedForTest(nid types.NodeID) (*BadgerStore, func(), error) {
	return ts.shardForNodeIDChecked(nid)
}

// FindRelInAnyShardStoreForTest exposes the cross-shard rel probe.
func (ts *Store) FindRelInAnyShardStoreForTest(relID snowflake.ID, stores []NamedStore) *BadgerStore {
	return ts.findRelInAnyShardStore(relID, stores)
}

// AllShardStoresWithLazyOpenForTest exposes the shard-snapshot helper.
func (ts *Store) AllShardStoresWithLazyOpenForTest() ([]NamedStore, func(), error) {
	return ts.allShardStoresWithLazyOpen()
}

// ForEachHistoryShardForTest exposes the history-shard fan-out helper.
func (ts *Store) ForEachHistoryShardForTest(skip *BadgerStore, fn func(*BadgerStore) (bool, error)) error {
	return ts.forEachHistoryShard(skip, fn)
}

// NamedStore is the (name, store) pair returned by allShardStoresWithLazyOpen.
type NamedStore = namedStore

// StoreForTest returns the BadgerStore in the named-store pair.
func (ns NamedStore) StoreForTest() *BadgerStore { return ns.store }

// OntologyForTest returns the ontology mapping.
func (ts *Store) OntologyForTest() *OntologyMapping { return ts.ontology }

// HasIncomingEntryForTest exposes the package-private hasIncomingEntry helper
// (used by the repair tests).
func HasIncomingEntryForTest(store *BadgerStore, nodeID, relID snowflake.ID) bool {
	return hasIncomingEntry(store, nodeID, relID)
}
