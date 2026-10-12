package badger

import (
	"encoding/binary"
	"fmt"
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.ReimportedRelCapability = (*Store)(nil)

// reimportedRelMetaPrefix names the meta keys of the re-imported set: the
// prefix followed by the relationship ID (8 bytes, big-endian), empty value.
const reimportedRelMetaPrefix = "rel_reimported/"

// reimportedRels is the RAM copy of the store.ReimportedRelCapability set
// (backlog 50): the relationship IDs whose lives name different endpoints.
// The meta keys are the truth; the copy is loaded by one key-only prefix scan
// on first use (after open or Clear) and kept in step by NoteReimportedRel.
// RAM: one map entry per noted ID (re-imports that move an endpoint only).
type reimportedRels struct {
	mu     sync.Mutex
	loaded bool
	ids    map[types.RelID]struct{}
}

// reset drops the copy (Clear): the next use reloads it from the keys left.
func (r *reimportedRels) reset() {
	r.mu.Lock()
	r.loaded, r.ids = false, nil
	r.mu.Unlock()
}

func reimportedRelKey(id types.RelID) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(id.SnowflakeID())) // #nosec G115 -- a validated positive snowflake ID
	return storepkg.MetaKey(reimportedRelMetaPrefix + string(b[:]))
}

// loadReimportedLocked fills the RAM copy from the meta keys. Caller holds
// bs.reimported.mu.
func (bs *Store) loadReimportedLocked() error {
	if bs.reimported.loaded {
		return nil
	}
	prefix := storepkg.MetaKey(reimportedRelMetaPrefix)
	ids := make(map[types.RelID]struct{})
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) != len(prefix)+8 {
				return fmt.Errorf("graph: re-imported relationship key of %d bytes", len(k))
			}
			id := types.RelID(binary.BigEndian.Uint64(k[len(prefix):])) // #nosec G115 -- written from a positive ID
			if err := storecontract.ValidateRelID(id); err != nil {
				return err
			}
			ids[id] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: load re-imported relationships: %w", err)
	}
	bs.reimported.ids, bs.reimported.loaded = ids, true
	return nil
}

// NoteReimportedRel records id (store.ReimportedRelCapability). The key is
// committed and, without SyncWrites, synced before the call returns, so it is
// durable before the row the caller writes next (that row is buffered and
// flushed later, possibly to another database of a partitioned store).
func (bs *Store) NoteReimportedRel(id types.RelID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return err
	}
	bs.reimported.mu.Lock()
	defer bs.reimported.mu.Unlock()
	if err := bs.loadReimportedLocked(); err != nil {
		return err
	}
	if _, ok := bs.reimported.ids[id]; ok {
		return nil
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(reimportedRelKey(id), nil)
	}); err != nil {
		return fmt.Errorf("graph: note re-imported relationship: %w", err)
	}
	if !bs.syncWrites && !bs.inMemory {
		if err := bs.db.Sync(); err != nil {
			return fmt.Errorf("graph: sync re-imported relationship: %w", err)
		}
	}
	bs.reimported.ids[id] = struct{}{}
	return nil
}

// ForEachReimportedRelID visits every noted ID (store.ReimportedRelCapability):
// a snapshot of the RAM copy, fn called outside the lock. fn returning false
// stops.
func (bs *Store) ForEachReimportedRelID(fn func(types.RelID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	bs.reimported.mu.Lock()
	if err := bs.loadReimportedLocked(); err != nil {
		bs.reimported.mu.Unlock()
		return err
	}
	ids := make([]types.RelID, 0, len(bs.reimported.ids))
	for id := range bs.reimported.ids {
		ids = append(ids, id)
	}
	bs.reimported.mu.Unlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}
