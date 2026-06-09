// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// getCounter reads a big-endian int64 counter from the given key within txn.
// Returns 0 if the key does not exist.
func getCounter(txn *badgerv4.Txn, key []byte) (int64, error) {
	item, err := txn.Get(key)
	if err == badgerv4.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var val int64
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("graph: counter value size %d, want 8", len(v))
		}
		val = int64(binary.BigEndian.Uint64(v)) // #nosec G115 — inverse of counter encoding
		if val < 0 {
			return fmt.Errorf("%w: counter value must be non-negative, got %d", ErrInvalidStoreMutation, val)
		}
		return nil
	})
	return val, err
}

// NodeCount returns the number of stored nodes. O(1).
func (bs *Store) NodeCount() (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	return int(bs.nodeCount.Load()), nil // #nosec G115 — count is always non-negative and within int range
}

// RelationshipCount returns the number of stored relationships. O(1).
func (bs *Store) RelationshipCount() (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	return int(bs.relCount.Load()), nil // #nosec G115 — count is always non-negative and within int range
}

// NodeCountByLabel returns the number of nodes with the given label token. O(1).
func (bs *Store) NodeCountByLabel(token uint16) (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return 0, err
	}
	if v, ok := bs.labelCounts.Load(token); ok {
		return int(v.(*atomic.Int64).Load()), nil // #nosec G115 — count is always non-negative and within int range
	}
	return 0, nil
}

// RelCountByType returns the number of relationships with the given type token. O(1).
func (bs *Store) RelCountByType(token uint16) (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return 0, err
	}
	if v, ok := bs.typeCounts.Load(token); ok {
		return int(v.(*atomic.Int64).Load()), nil // #nosec G115 — count is always non-negative and within int range
	}
	return 0, nil
}

// getOrCreateLabelCounter returns the atomic counter for the given label token,
// creating it if it doesn't exist.
func (bs *Store) getOrCreateLabelCounter(token uint16) *atomic.Int64 {
	if v, ok := bs.labelCounts.Load(token); ok {
		return v.(*atomic.Int64)
	}
	v, _ := bs.labelCounts.LoadOrStore(token, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// getOrCreateTypeCounter returns the atomic counter for the given reltype token,
// creating it if it doesn't exist.
func (bs *Store) getOrCreateTypeCounter(token uint16) *atomic.Int64 {
	if v, ok := bs.typeCounts.Load(token); ok {
		return v.(*atomic.Int64)
	}
	v, _ := bs.typeCounts.LoadOrStore(token, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// NodeCacheHits returns the total number of node cache hits since store creation.
// Implements StoreStats. Both indexpkg.CacheHit and indexpkg.CacheDeleted (tombstone) results count
// as hits, because both avoid a Badger read.
func (bs *Store) NodeCacheHits() int64 {
	if bs == nil || bs.nodeCache == nil || bs.closing.Load() || bs.dbClosed.Load() {
		return 0
	}
	return bs.nodeCache.Hits()
}

// NodeCacheMisses returns the total number of node cache misses since store creation.
// Implements StoreStats.
func (bs *Store) NodeCacheMisses() int64 {
	if bs == nil || bs.nodeCache == nil || bs.closing.Load() || bs.dbClosed.Load() {
		return 0
	}
	return bs.nodeCache.Misses()
}

// RelCacheHits returns the total number of relationship cache hits since store creation.
// Implements StoreStats.
func (bs *Store) RelCacheHits() int64 {
	if bs == nil || bs.relCache == nil || bs.closing.Load() || bs.dbClosed.Load() {
		return 0
	}
	return bs.relCache.Hits()
}

// RelCacheMisses returns the total number of relationship cache misses since store creation.
// Implements StoreStats.
func (bs *Store) RelCacheMisses() int64 {
	if bs == nil || bs.relCache == nil || bs.closing.Load() || bs.dbClosed.Load() {
		return 0
	}
	return bs.relCache.Misses()
}

// SaveRegistries persists both the label and relationship-type registries
// atomically in a single Badger transaction. Either both writes commit or
// neither does — callers do not need to reason about a partially-applied
// registry on crash. This mirrors the contract of TieredStore.SaveRegistries
// so that lifecycle code can persist registries through a single uniform
// interface regardless of the underlying Store implementation.
func (bs *Store) SaveRegistries(labelReg *registrypkg.LabelRegistry, relTypeReg *registrypkg.RelTypeRegistry) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if labelReg == nil {
		return errNilLabelRegistry()
	}
	if relTypeReg == nil {
		return errNilRelTypeRegistry()
	}
	labelNames := labelReg.ExportNames()
	labelData, err := msgpack.Marshal(labelNames)
	if err != nil {
		return fmt.Errorf("graph: marshal label registry: %w", err)
	}
	relTypeNames := relTypeReg.ExportNames()
	relTypeData, err := msgpack.Marshal(relTypeNames)
	if err != nil {
		return fmt.Errorf("graph: marshal reltype registry: %w", err)
	}
	return bs.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Set(storepkg.MetaKey("label_tokens"), labelData); err != nil {
			return err
		}
		return txn.Set(storepkg.MetaKey("reltype_tokens"), relTypeData)
	})
}

// SaveLabelRegistry persists the label registry to the Badger store.
func (bs *Store) SaveLabelRegistry(reg *registrypkg.LabelRegistry) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if reg == nil {
		return errNilLabelRegistry()
	}
	names := reg.ExportNames()
	data, err := msgpack.Marshal(names)
	if err != nil {
		return fmt.Errorf("graph: marshal label registry: %w", err)
	}
	return bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.MetaKey("label_tokens"), data)
	})
}

// LoadLabelRegistry loads the label registry from the Badger store.
// Returns false if no saved data exists (fresh database).
func (bs *Store) LoadLabelRegistry(reg *registrypkg.LabelRegistry) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if reg == nil {
		return false, errNilLabelRegistry()
	}
	var names []string
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.MetaKey("label_tokens"))
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &names)
		})
	})
	if err != nil {
		return false, fmt.Errorf("graph: load label registry: %w", err)
	}
	if names == nil {
		return false, nil
	}
	return true, reg.ImportNames(names)
}

// SaveRelTypeRegistry persists the relationship type registry to the Badger store.
func (bs *Store) SaveRelTypeRegistry(reg *registrypkg.RelTypeRegistry) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if reg == nil {
		return errNilRelTypeRegistry()
	}
	names := reg.ExportNames()
	data, err := msgpack.Marshal(names)
	if err != nil {
		return fmt.Errorf("graph: marshal reltype registry: %w", err)
	}
	return bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.MetaKey("reltype_tokens"), data)
	})
}

// LoadRelTypeRegistry loads the relationship type registry from the Badger store.
// Returns false if no saved data exists (fresh database).
func (bs *Store) LoadRelTypeRegistry(reg *registrypkg.RelTypeRegistry) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if reg == nil {
		return false, errNilRelTypeRegistry()
	}
	var names []string
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.MetaKey("reltype_tokens"))
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &names)
		})
	})
	if err != nil {
		return false, fmt.Errorf("graph: load reltype registry: %w", err)
	}
	if names == nil {
		return false, nil
	}
	return true, reg.ImportNames(names)
}

// MetaGet returns the bytes stored under storepkg.MetaKey(key), or (nil, nil) if absent.
func (bs *Store) MetaGet(key string) ([]byte, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	var out []byte
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.MetaKey(key))
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out = make([]byte, len(val))
			copy(out, val)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("graph: meta get %q: %w", key, err)
	}
	return out, nil
}

// MetaSet stores value under storepkg.MetaKey(key).
func (bs *Store) MetaSet(key string, value []byte) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	return bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.MetaKey(key), value)
	})
}

// SavePropertyKeyRegistry persists the property-key registry to the Badger store.
func (bs *Store) SavePropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if reg == nil {
		return fmt.Errorf("graph: nil property-key registry")
	}
	names := reg.ExportNames()
	data, err := msgpack.Marshal(names)
	if err != nil {
		return fmt.Errorf("graph: marshal property-key registry: %w", err)
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.MetaKey("property_keys"), data)
	}); err != nil {
		return fmt.Errorf("graph: persist property-key registry: %w", err)
	}
	// Write-ahead durability: the registry MUST reach stable storage before any
	// row that references its newly-assigned tokens. Entity rows are buffered
	// (bs.pending) and flushed asynchronously to a DIFFERENT Badger DB (the
	// event shard) under SyncWrites=false, so plain db.Update ordering is not
	// enough — without an fsync here a crash could leave a tokenized row durable
	// while its token is still in an unsynced refShard buffer, making the row
	// undecodable on reload (counter mismatch → fatal). New keys are rare
	// (bounded by the schema), so this fsync amortizes to near-zero.
	//
	// Skip when SyncWrites is on (db.Update already fsynced) or in InMemory mode
	// (no WAL — mt.wal is nil, so db.Sync would nil-panic, and there is nothing
	// to make durable).
	if !bs.syncWrites && !bs.inMemory {
		if err := bs.db.Sync(); err != nil {
			return fmt.Errorf("graph: sync property-key registry: %w", err)
		}
	}
	return nil
}

// LoadPropertyKeyRegistry loads the property-key registry from the Badger store.
// Returns false if no saved data exists (fresh database).
func (bs *Store) LoadPropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if reg == nil {
		return false, fmt.Errorf("graph: nil property-key registry")
	}
	var names []string
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.MetaKey("property_keys"))
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &names)
		})
	})
	if err != nil {
		return false, fmt.Errorf("graph: load property-key registry: %w", err)
	}
	if names == nil {
		return false, nil
	}
	return true, reg.ImportNames(names)
}

// readPropertyKeyRegistryFromMeta loads the persisted property-key registry
// directly from the meta KV. Called by New BEFORE loadIndexes so the index
// rebuild can resolve tokenized property keys. Returns nil if no registry
// has been persisted yet (fresh store).
func readPropertyKeyRegistryFromMeta(bs *Store) *registrypkg.PropertyKeyRegistry {
	reg := registrypkg.NewPropertyKeyRegistry()
	found, err := bs.LoadPropertyKeyRegistry(reg)
	if err != nil || !found {
		return nil
	}
	return reg
}
