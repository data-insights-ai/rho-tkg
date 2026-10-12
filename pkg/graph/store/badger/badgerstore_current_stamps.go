package badger

import (
	"fmt"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.CurrentStampsCapability = (*Store)(nil)

// NodeCurrentStamps returns the stamps and version of nid's current row
// (store.CurrentStampsCapability) in the order prefetchNode reads the row: the
// entity cache first (a hit answers from the cached row; a tombstone of an
// unflushed delete is not found — dirty entries are never evicted, so the
// cache holds every write the flush has not committed), then the ID index,
// then the committed row, of which only the temporal block is decoded
// (storeutil.DecodeWireRowStamps: no properties, labels or hashes). A cache
// hit allocates nothing; a miss allocates only the read transaction's
// objects, and fills no cache entry (nothing is decoded to fill it with).
// The partial decode does not verify the row's ID against its key nor the
// body's fields: a row whose temporal block decodes but whose body is corrupt
// is answered where GetNode fails (HasNodeHistory / NodeHistoryStamps have the
// same asymmetry); a newer format version or an undecodable temporal block
// fails here too.
func (bs *Store) NodeCurrentStamps(nid types.NodeID) (storecontract.RowStamps, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return storecontract.RowStamps{}, err
	}
	id := nid.SnowflakeID()
	switch n, status := bs.nodeCache.GetNoPromote(id); status {
	case indexpkg.CacheHit:
		return storecontract.NodeRowStamps(n), nil
	case indexpkg.CacheDeleted:
		return storecontract.RowStamps{}, ErrNodeNotFound
	}
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	if !exists {
		return storecontract.RowStamps{}, ErrNodeNotFound
	}
	return bs.committedRowStamps(storepkg.KeyNode, id, ErrNodeNotFound)
}

// RelCurrentStamps is NodeCurrentStamps for relationships (the prefetchRel
// order: relationship cache, relIDs, the committed row's temporal block).
func (bs *Store) RelCurrentStamps(rid types.RelID) (storecontract.RowStamps, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return storecontract.RowStamps{}, err
	}
	id := rid.SnowflakeID()
	switch r, status := bs.relCache.GetNoPromote(id); status {
	case indexpkg.CacheHit:
		return storecontract.RelRowStamps(r), nil
	case indexpkg.CacheDeleted:
		return storecontract.RowStamps{}, ErrRelNotFound
	}
	bs.idxMu.RLock()
	_, exists := bs.relIDs[rid]
	bs.idxMu.RUnlock()
	if !exists {
		return storecontract.RowStamps{}, ErrRelNotFound
	}
	return bs.committedRowStamps(storepkg.KeyRel, id, ErrRelNotFound)
}

// rowKeyPool lends the 9-byte entity-key buffers of committedRowStamps: the
// key escapes into badger's Item, so a per-call key would cost one allocation
// beside badger's own eight read objects.
var rowKeyPool = sync.Pool{New: func() any { return new([storepkg.SizeNodeKey]byte) }}

// committedRowStamps reads the committed current row of id (prefix KeyNode or
// KeyRel: the NodeKey / RelKey layout) and decodes its temporal block; missing
// is returned when the key holds no row. The key buffer returns to the pool
// after the read transaction ends (nothing of the transaction outlives it).
func (bs *Store) committedRowStamps(prefix byte, id snowflake.ID, missing error) (storecontract.RowStamps, error) {
	kb := rowKeyPool.Get().(*[storepkg.SizeNodeKey]byte)
	defer rowKeyPool.Put(kb)
	key := kb[:]
	key[0] = prefix
	storepkg.PutUint64(key, 1, int64(id))
	var s storecontract.RowStamps
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err == badgerv4.ErrKeyNotFound {
			return missing
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var derr error
			if s, derr = storepkg.DecodeWireRowStamps(val); derr != nil {
				return fmt.Errorf("graph: decode current row stamps: %w", derr)
			}
			return nil
		})
	})
	if err != nil {
		return storecontract.RowStamps{}, err
	}
	return s, nil
}
