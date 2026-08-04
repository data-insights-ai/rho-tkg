package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship columns, memory-store side. Closes an asymmetry: the node columns
// exist on both backends, but rel columns shipped on badger only.
//
// Everything is simpler here because the entities are already in RAM: there is no
// bulk-read to optimise and no persistence, so this is membership from typeIdx plus
// the same generic column builder.
//
// The epoch is the store-wide relEpoch, NOT striped per type. Badger stripes because
// its rebuild is expensive; here a rebuild walks an in-memory map, so the coarser
// counter is the honest trade rather than a second invalidation scheme to keep in
// step with the first.

// RelStartColumn / RelEndColumn are the reserved keys holding a relationship's
// endpoints, matching the badger backend so a consumer reads one name on both.
const (
	RelStartColumn = "tkg_rel_start"
	RelEndColumn   = "tkg_rel_end"
)

// RelMutationEpochForType is the freshness stamp for a rel-type column snapshot.
// Store-wide, so any edge write invalidates every type's columns.
func (ms *Store) RelMutationEpochForType(uint16) uint64 { return ms.relEpoch.Load() }

// RelColumnSnapshot returns the columnar snapshot for a relationship type, building
// it if stale. ok=false means the columnar path is unusable — an empty or over-cap
// type, or a requested property that is not a uniformly numeric/string column — and
// the caller must fall back to the row path.
func (ms *Store) RelColumnSnapshot(typeToken uint16, propKeys []string) (snap *indexpkg.DocValues[types.RelID], gen uint64, ok bool, err error) {
	if ms == nil {
		return nil, 0, false, ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if cerr := ms.checkOpenLocked(); cerr != nil {
		return nil, 0, false, cerr
	}

	cur := ms.relEpoch.Load()
	keys := indexpkg.UnionKeys([]string{RelStartColumn, RelEndColumn}, propKeys)

	col := ms.relColumns[typeToken]
	if col == nil || col.Epoch() != cur || !col.HasAll(keys) {
		built, declined := ms.buildRelColumnsLocked(typeToken, keys, col, cur)
		if declined {
			return nil, 0, false, nil
		}
		col = built
	}
	if !col.HasAll(keys) {
		return nil, col.Epoch(), false, nil
	}
	return col, col.Epoch(), true, nil
}

// buildRelColumnsLocked builds a fresh snapshot over one relationship type. Must
// hold ms.mu (write lock).
func (ms *Store) buildRelColumnsLocked(typeToken uint16, keys []string,
	old *indexpkg.DocValues[types.RelID], cur uint64) (*indexpkg.DocValues[types.RelID], bool) {

	set := ms.typeIdx[typeToken]
	if len(set) == 0 || len(set) > indexpkg.MaxDocValuesNodes {
		return nil, true
	}
	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	if old != nil {
		keys = indexpkg.UnionKeys(old.Keys(), keys)
	}

	getProp := func(id types.RelID, key string) (any, bool) {
		r := ms.rels[id]
		if r == nil {
			return nil, false
		}
		// Endpoints are structure, not properties, so they are answered from the
		// relationship itself rather than its property slice.
		switch key {
		case RelStartColumn:
			return int64(r.StartNodeID()), true
		case RelEndColumn:
			return int64(r.EndNodeID()), true
		}
		return r.GetProperty(key)
	}
	getTemporal := func(id types.RelID) (int64, int64, bool) {
		r := ms.rels[id]
		if r == nil {
			return 0, 0, false
		}
		f, t, has := r.ValidRange()
		// An unset ValidFrom resolves to the entity's MINT time, not the epoch.
		// Storing the raw 0 would make a columnar reader disagree with every
		// row-path valid-time filter on exactly those edges (Pattern 38).
		if !has || f == 0 {
			f = storeutil.SnowflakeInstant(id.SnowflakeID())
		}
		return int64(f), int64(t), true
	}

	col := indexpkg.BuildDocValues(cur, ids, keys, getProp, getTemporal)
	ms.columnRebuilds.Add(1)
	if ms.relColumns == nil {
		ms.relColumns = make(map[uint16]*indexpkg.DocValues[types.RelID])
	}
	ms.relColumns[typeToken] = col
	return col, false
}
