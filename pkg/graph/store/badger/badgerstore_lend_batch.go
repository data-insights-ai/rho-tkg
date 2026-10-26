package badger

import (
	"errors"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.EntityLendBatchCapability = (*Store)(nil)

// LendNodesSorted implements store.EntityLendBatchCapability: the cache
// answers its hits (no promotion), and the misses the store still lists are
// read through one forward-seeking iterator of a scanSnapshot, which re-pins
// after a flush (see its doc comment), and are not filled into the cache.
func (bs *Store) LendNodesSorted(ids []types.NodeID, out []*types.Node) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	snap := newScanSnapshot(bs, bs.nodeCache.FlushEpoch)
	defer snap.close()
	for i, nid := range ids {
		out[i] = nil
		id := nid.SnowflakeID()
		v, status := bs.nodeCache.GetNoPromote(id)
		switch status {
		case indexpkg.CacheHit:
			out[i] = v
			continue
		case indexpkg.CacheDeleted:
			continue
		}
		rev := bs.liveNodeRev(nid)
		if rev == 0 {
			continue
		}
		item := snap.seekAfterMiss(storepkg.NodeKey(id))
		if item == nil {
			if err := bs.nodeRowMissing(nid, rev); !errors.Is(err, ErrNodeNotFound) {
				return err // a damaged table, not a delete
			}
			continue
		}
		if err := item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n, err := bs.decodeNodeWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode node: %w", err)
			}
			n.Freeze()
			out[i] = n
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// LendRelationshipsSorted is LendNodesSorted for relationships.
func (bs *Store) LendRelationshipsSorted(ids []types.RelID, out []*types.Relationship) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	snap := newScanSnapshot(bs, bs.relCache.FlushEpoch)
	defer snap.close()
	for i, rid := range ids {
		out[i] = nil
		id := rid.SnowflakeID()
		v, status := bs.relCache.GetNoPromote(id)
		switch status {
		case indexpkg.CacheHit:
			out[i] = v
			continue
		case indexpkg.CacheDeleted:
			continue
		}
		rev := bs.liveRelRev(rid)
		if rev == 0 {
			continue
		}
		item := snap.seekAfterMiss(storepkg.RelKey(id))
		if item == nil {
			if err := bs.relRowMissing(rid, rev); !errors.Is(err, ErrRelNotFound) {
				return err // a damaged table, not a delete
			}
			continue
		}
		if err := item.Value(func(val []byte) error {
			var w storepkg.RelWire
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal relationship: %w", err)
			}
			r, err := bs.decodeRelWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode relationship: %w", err)
			}
			r.Freeze()
			out[i] = r
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
