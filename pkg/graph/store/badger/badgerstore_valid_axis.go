package badger

import (
	"fmt"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// openValidAxis fixes the store's valid axis at open (round 11): from the
// persisted untimed mode in its own MetaKV when it holds one, else fallback
// (Config.ValidAxis: a composite's shard). Called before the open-time index
// rebuild reads a row.
func (bs *Store) openValidAxis(fallback types.ValidAxis) error {
	axis := fallback
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.MetaKey(storecontract.UntimedValidStartMetaKey))
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			axis = storecontract.ValidAxisOfModeByte(val)
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("graph: read the untimed valid-start mode: %w", err)
	}
	bs.validAxis.Set(axis)
	return nil
}

// ValidAxis implements store.ValidAxisCapability.
func (bs *Store) ValidAxis() types.ValidAxis { return bs.validAxis.Get() }

// SetValidAxis implements store.ValidAxisCapability: the graph sets the axis
// of a store it opens empty (a new store, or one an import or a replica feed
// is about to fill) before the first row is written.
func (bs *Store) SetValidAxis(a types.ValidAxis) { bs.validAxis.Set(a) }
