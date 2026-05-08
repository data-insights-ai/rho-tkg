// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
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
		return nil
	})
	return val, err
}

// NodeCount returns the number of stored nodes. O(1).
func (bs *Store) NodeCount() (int, error) {
	return int(bs.nodeCount.Load()), nil // #nosec G115 — count is always non-negative and within int range
}

// RelationshipCount returns the number of stored relationships. O(1).
func (bs *Store) RelationshipCount() (int, error) {
	return int(bs.relCount.Load()), nil // #nosec G115 — count is always non-negative and within int range
}

// NodeCountByLabel returns the number of nodes with the given label token. O(1).
func (bs *Store) NodeCountByLabel(token uint16) (int, error) {
	if v, ok := bs.labelCounts.Load(token); ok {
		return int(v.(*atomic.Int64).Load()), nil // #nosec G115 — count is always non-negative and within int range
	}
	return 0, nil
}

// RelCountByType returns the number of relationships with the given type token. O(1).
func (bs *Store) RelCountByType(token uint16) (int, error) {
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
func (bs *Store) NodeCacheHits() int64 { return bs.nodeCache.Hits() }

// NodeCacheMisses returns the total number of node cache misses since store creation.
// Implements StoreStats.
func (bs *Store) NodeCacheMisses() int64 { return bs.nodeCache.Misses() }

// RelCacheHits returns the total number of relationship cache hits since store creation.
// Implements StoreStats.
func (bs *Store) RelCacheHits() int64 { return bs.relCache.Hits() }

// RelCacheMisses returns the total number of relationship cache misses since store creation.
// Implements StoreStats.
func (bs *Store) RelCacheMisses() int64 { return bs.relCache.Misses() }

// SaveRegistries persists both the label and relationship-type registries
// atomically in a single Badger transaction. Either both writes commit or
// neither does — callers do not need to reason about a partially-applied
// registry on crash. This mirrors the contract of TieredStore.SaveRegistries
// so that lifecycle code can persist registries through a single uniform
// interface regardless of the underlying Store implementation.
func (bs *Store) SaveRegistries(labelReg *registrypkg.LabelRegistry, relTypeReg *registrypkg.RelTypeRegistry) error {
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
