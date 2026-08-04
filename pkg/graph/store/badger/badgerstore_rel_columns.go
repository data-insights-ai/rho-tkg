package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship columns (RC). The node side caches a DocValues snapshot per label;
// this is the same structure keyed by REL-TYPE token, which the generic
// DocValues[T] made possible without a second implementation.
//
// Almost none of this is new machinery. Membership comes from typeIdx (the exact
// mirror of labelIdx), invalidation from relEpoch (already bumped on every edge
// write), and the columns from the same builder the node side uses. What IS new is
// the endpoint columns.
//
// ENDPOINT COLUMNS ARE THE POINT. StartNodeID/EndNodeID are already int64, so as
// columns they are free — and they are what makes a traversal aggregation readable
// as typed arrays. A consumer computing, say, weight-by-target over a rel type reads
// three aligned arrays and never materialises a *types.Relationship. They are
// therefore ALWAYS built, unlike property columns which are requested.
//
// Endpoints are stored under reserved property keys rather than as separate fields
// so they ride the existing column machinery — typed storage, presence bitset,
// append-extend and the zone map all apply unchanged.
const (
	// RelStartColumn / RelEndColumn are the reserved column keys holding a
	// relationship's endpoints. They cannot collide with a user property: the
	// store rejects the reserved prefix on writes.
	RelStartColumn = "tkg_rel_start"
	RelEndColumn   = "tkg_rel_end"
)

// relColumnKeys returns requested plus the always-present endpoint columns.
func relColumnKeys(requested []string) []string {
	return indexpkg.UnionKeys([]string{RelStartColumn, RelEndColumn}, requested)
}

// RelMutationEpochForType is the freshness stamp for a rel-type column snapshot.
//
// Unlike the node side there is no per-TYPE stripe: relEpoch is global across all
// relationship types, so a write to any type invalidates every type's columns. That
// is a real limitation and it is deliberate — the node side's per-label stripes were
// added by a dedicated change (BACKLOG 4b) after the global counter proved too
// coarse, and inventing a rel-side equivalent here would be a second, unmeasured
// invalidation scheme landing in the same release as the columns themselves.
func (bs *Store) RelMutationEpochForType(uint16) uint64 { return bs.relEpoch.Load() }

// buildRelColumns builds a fresh immutable snapshot over every relationship of one
// type, mirroring buildLabelColumns exactly (lock-free, epoch-stamped, cached only
// if the epoch held). declined=true means an empty or over-cap type.
func (bs *Store) buildRelColumns(typeToken uint16, requested []string) (col *indexpkg.DocValues[types.RelID], declined bool) {
	gen := bs.relEpoch.Load()

	bs.idxMu.RLock()
	set := bs.typeIdx[typeToken]
	n := len(set)
	if n == 0 || n > indexpkg.MaxDocValuesNodes {
		bs.idxMu.RUnlock()
		return nil, true
	}
	ids := make([]types.RelID, 0, n)
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	keys := relColumnKeys(requested)
	bs.docMu.Lock()
	if old := bs.relColumns[typeToken]; old != nil {
		keys = indexpkg.UnionKeys(old.Keys(), keys)
	}
	bs.docMu.Unlock()

	getProp, getTemporal := bs.bulkRelGetters(ids)
	col = indexpkg.BuildDocValues(gen, ids, keys, getProp, getTemporal)
	bs.columnRebuilds.Add(1)

	bs.docMu.Lock()
	if bs.relEpoch.Load() == gen { // build saw a consistent snapshot — safe to cache
		if bs.relColumns == nil {
			bs.relColumns = make(map[uint16]*indexpkg.DocValues[types.RelID])
		}
		bs.relColumns[typeToken] = col
	}
	bs.docMu.Unlock()
	return col, false
}

// bulkRelGetters returns the property and temporal accessors for a rel column
// build, decoding each relationship EXACTLY ONCE into a shared map so the value
// columns and the validity columns cannot come from different reads.
//
// The endpoint keys are answered from the relationship itself rather than its
// property slice, which is why this cannot just be the node getter with a different
// type: a relationship's endpoints are structure, not properties.
func (bs *Store) bulkRelGetters(ids []types.RelID) (
	func(types.RelID, string) (any, bool),
	func(types.RelID) (int64, int64, bool),
) {
	mat := make(map[types.RelID]*types.Relationship, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(id)
		if err != nil || r == nil {
			continue // deleted between the membership snapshot and the scan
		}
		mat[id] = r
	}

	getProp := func(id types.RelID, key string) (any, bool) {
		r := mat[id]
		if r == nil {
			return nil, false
		}
		switch key {
		case RelStartColumn:
			return int64(r.StartNodeID()), true
		case RelEndColumn:
			return int64(r.EndNodeID()), true
		}
		return r.GetProperty(key)
	}
	getTemporal := func(id types.RelID) (int64, int64, bool) {
		r := mat[id]
		if r == nil {
			return 0, 0, false
		}
		f, t, ok := r.ValidRange()
		// Same rule as the node side: an unset ValidFrom resolves to the entity's
		// MINT time, not to the epoch. Storing the raw 0 would make a columnar
		// reader disagree with every row-path valid-time filter on exactly those
		// relationships (Pattern 38).
		if !ok || f == 0 {
			f = storepkg.SnowflakeInstant(id.SnowflakeID())
		}
		return int64(f), int64(t), true
	}
	return getProp, getTemporal
}

// RelColumnSnapshot returns the columnar snapshot for a relationship type, building
// it if stale. ok=false means the columnar path is unusable for this type and the
// caller must fall back to the row path — an empty or over-cap type, or a requested
// property that is not a uniformly numeric/string column.
func (bs *Store) RelColumnSnapshot(typeToken uint16, propKeys []string) (snap *indexpkg.DocValues[types.RelID], gen uint64, ok bool, err error) {
	if bs == nil {
		return nil, 0, false, ErrStoreClosed
	}
	if err := bs.checkOpen(); err != nil {
		return nil, 0, false, err
	}

	cur := bs.relEpoch.Load()
	keys := relColumnKeys(propKeys)

	bs.docMu.Lock()
	col := bs.relColumns[typeToken]
	bs.docMu.Unlock()

	if col == nil || col.Epoch() != cur || !col.HasAll(keys) {
		built, declined := bs.buildRelColumns(typeToken, propKeys)
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
