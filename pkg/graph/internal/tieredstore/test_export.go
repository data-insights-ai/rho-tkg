package tieredstore

import (
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// This file exposes a curated set of unexported TieredStore and EventShard
// internals so tests in pkg/graph can keep poking at them after the
// restructure. Production code must not call these helpers — they exist
// solely to keep the moves-only restructure reviewable without rewriting
// dozens of integration tests.

// --- TieredStore field accessors ---

// RefShardForTest returns the reference shard's BadgerStore.
func (ts *TieredStore) RefShardForTest() *BadgerStore { return ts.refShard }

// RefArchiveForTest returns the atomic pointer to the reference archive.
func (ts *TieredStore) RefArchiveForTest() *atomic.Pointer[BadgerStore] { return &ts.refArchive }

// ArchiveActiveReqsForTest returns the refcount on the reference archive.
func (ts *TieredStore) ArchiveActiveReqsForTest() *atomic.Int64 { return &ts.archiveActiveReqs }

// HotShardForTest returns the current hot event shard.
func (ts *TieredStore) HotShardForTest() *EventShard { return ts.hotShard }

// EventShardsForTest returns the event-shard map. Caller must hold MuForTest.
func (ts *TieredStore) EventShardsForTest() map[string]*EventShard { return ts.eventShards }

// CatalogForTest returns the shard catalog.
func (ts *TieredStore) CatalogForTest() *ShardCatalog { return ts.catalog }

// ClosedForTest returns the closed flag (atomic.Bool).
func (ts *TieredStore) ClosedForTest() *atomic.Bool { return &ts.closed }

// MuForTest returns the TieredStore-level RW mutex.
func (ts *TieredStore) MuForTest() *sync.RWMutex { return &ts.mu }

// CompressionForTest returns the configured compression algorithm.
func (ts *TieredStore) CompressionForTest() options.CompressionType { return ts.compression }

// ZSTDLevelForTest returns the configured ZSTD compression level.
func (ts *TieredStore) ZSTDLevelForTest() int { return ts.zstdLevel }

// SetIdleTimeoutForTest overrides the idle-timeout for cold shards. Tests use
// this to drive closeIdleShards quickly.
func (ts *TieredStore) SetIdleTimeoutForTest(d time.Duration) { ts.idleTimeout = d }

// TempIdxLabelsForTest returns the tracked temporal-index labels under lock.
// Returns a fresh copy.
func (ts *TieredStore) TempIdxLabelsForTest() []uint16 {
	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()
	out := make([]uint16, len(ts.tempIdxLabels))
	copy(out, ts.tempIdxLabels)
	return out
}

// HasArchiveShardForTest reports whether the catalog has an archive entry.
func (ts *TieredStore) HasArchiveShardForTest() bool { return ts.hasArchiveShard() }

// EnsureRefArchiveForTest forces lazy-open of the reference archive.
func (ts *TieredStore) EnsureRefArchiveForTest() error { return ts.ensureRefArchive() }

// CloseIdleShardsForTest invokes the closeIdleShards loop body once.
func (ts *TieredStore) CloseIdleShardsForTest() { ts.closeIdleShards() }

// CheckRotationForTest invokes the rotation check.
func (ts *TieredStore) CheckRotationForTest() error { return ts.checkRotation() }

// CheckoutArchiveForTest exposes checkoutArchive.
func (ts *TieredStore) CheckoutArchiveForTest() (*BadgerStore, func(), error) {
	return ts.checkoutArchive()
}

// ResolveShardStoreForTest exposes resolveShardStore.
func (ts *TieredStore) ResolveShardStoreForTest(name string) (*BadgerStore, func(), error) {
	return ts.resolveShardStore(name)
}

// ShardForNodeForTest exposes shardForNode (reference vs event routing).
func (ts *TieredStore) ShardForNodeForTest(primaryLabel uint16) *BadgerStore {
	return ts.shardForNode(primaryLabel)
}

// ShardForNodeIDForTest exposes shardForNodeID (the unpinned variant).
func (ts *TieredStore) ShardForNodeIDForTest(nid types.NodeID) (*BadgerStore, error) {
	return ts.shardForNodeID(nid)
}

// ShardForRelIDForTest resolves the shard for a rel ID (checked variant; handles checkin internally).
func (ts *TieredStore) ShardForRelIDForTest(rid types.RelID) (*BadgerStore, error) {
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
func (es *EventShard) Tier() ShardTier { return es.tier }

// SetTierForTest mutates the tier without rotating. Tests use this to
// simulate demote-to-cold without shipping bytes between shards.
func (es *EventShard) SetTierForTest(t ShardTier) { es.tier = t }

// ReadOnlyForTest reports whether the shard is marked read-only.
func (es *EventShard) ReadOnlyForTest() bool { return es.readOnly }

// Store returns the underlying BadgerStore (or nil for cold + closed shards).
func (es *EventShard) Store() *BadgerStore { return es.store }

// SetStoreForTest replaces the underlying store. Used by close-idle tests.
func (es *EventShard) SetStoreForTest(bs *BadgerStore) { es.store = bs }

// LockShardMuForTest / UnlockShardMuForTest expose the lazy-open mutex.
func (es *EventShard) LockShardMuForTest()   { es.shardMu.Lock() }
func (es *EventShard) UnlockShardMuForTest() { es.shardMu.Unlock() }

// GetStoreForTest exposes the lazy-open path.
func (es *EventShard) GetStoreForTest(ts *TieredStore) (*BadgerStore, error) {
	return es.getStore(ts)
}

// CheckoutStoreForTest exposes the activeReqs-pinned checkout path.
func (es *EventShard) CheckoutStoreForTest(ts *TieredStore) (*BadgerStore, error) {
	return es.checkoutStore(ts)
}

// CheckinStoreForTest decrements the activeReqs counter.
func (es *EventShard) CheckinStoreForTest() { es.checkinStore() }

// ActiveReqsForTest returns the activeReqs atomic counter.
func (es *EventShard) ActiveReqsForTest() *atomic.Int64 { return &es.activeReqs }

// SetLastAccessForTest mutates the lastAccess atomic counter (Unix ms).
func (es *EventShard) SetLastAccessForTest(t int64) { es.lastAccess.Store(t) }

// LastAccessForTest reads the lastAccess atomic counter.
func (es *EventShard) LastAccessForTest() int64 { return es.lastAccess.Load() }

// TimeStartForTest returns the shard's window start.
func (es *EventShard) TimeStartForTest() time.Time { return es.timeStart }

// TimeEndForTest returns the shard's window end.
func (es *EventShard) TimeEndForTest() time.Time { return es.timeEnd }

// SetTimeEndForTest mutates the shard's window end. Tests use this to drive
// rotation paths without waiting for the actual ShardWindow to elapse.
func (es *EventShard) SetTimeEndForTest(t time.Time) { es.timeEnd = t }

// --- More TieredStore field accessors ---

// RegFileForTest returns the registry file path.
func (ts *TieredStore) RegFileForTest() string { return ts.regFile }

// DataDirForTest returns the configured data directory.
func (ts *TieredStore) DataDirForTest() string { return ts.dataDir }

// InMemoryForTest reports whether the TieredStore was created in-memory.
func (ts *TieredStore) InMemoryForTest() bool { return ts.inMemory }

// ShardForRelIDCheckedForTest exposes the pinned rel-id router.
func (ts *TieredStore) ShardForRelIDCheckedForTest(rid types.RelID) (*BadgerStore, func(), error) {
	return ts.shardForRelIDChecked(rid)
}

// ShardForNodeIDCheckedForTest exposes the pinned node-id router.
func (ts *TieredStore) ShardForNodeIDCheckedForTest(nid types.NodeID) (*BadgerStore, func(), error) {
	return ts.shardForNodeIDChecked(nid)
}

// FindRelInAnyShardStoreForTest exposes the cross-shard rel probe.
func (ts *TieredStore) FindRelInAnyShardStoreForTest(relID snowflake.ID, stores []NamedStore) *BadgerStore {
	return ts.findRelInAnyShardStore(relID, stores)
}

// AllShardStoresWithLazyOpenForTest exposes the shard-snapshot helper.
func (ts *TieredStore) AllShardStoresWithLazyOpenForTest() ([]NamedStore, func(), error) {
	return ts.allShardStoresWithLazyOpen()
}

// ForEachHistoryShardForTest exposes the history-shard fan-out helper.
func (ts *TieredStore) ForEachHistoryShardForTest(skip *BadgerStore, fn func(*BadgerStore) (bool, error)) error {
	return ts.forEachHistoryShard(skip, fn)
}

// NamedStore is the (name, store) pair returned by allShardStoresWithLazyOpen.
type NamedStore = namedStore

// StoreForTest returns the BadgerStore in the named-store pair.
func (ns NamedStore) StoreForTest() *BadgerStore { return ns.store }

// NameForTest returns the shard name in the named-store pair.
func (ns NamedStore) NameForTest() string { return ns.name }

// OntologyForTest returns the ontology mapping.
func (ts *TieredStore) OntologyForTest() *OntologyMapping { return ts.ontology }

// HasIncomingEntryForTest exposes the package-private hasIncomingEntry helper
// (used by the repair tests).
func HasIncomingEntryForTest(store *BadgerStore, nodeID, relID snowflake.ID) bool {
	return hasIncomingEntry(store, nodeID, relID)
}
