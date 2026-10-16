// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"
	"iter"
	"log/slog"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Property indexes ---

// NodeRangeCardinality counts the label's nodes whose numeric propertyKey
// value lies in [min, max], summed from the property index's sorted buckets
// (no node scan). exact=false declines (no usable index, or poisoned by an
// integer past 2^53) and the caller scans. Mirrors the badger method.
func (ms *Store) NodeRangeCardinality(labelToken uint16, propertyKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if ms == nil {
		return 0, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, false, err
	}
	idx, ok := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	if !ok {
		return 0, false, nil
	}
	count, exact := idx.RangeCardinality(min, max, inclMin, inclMax)
	return count, exact, nil
}

// CreatePropertyIndex creates a property index for the label token and key;
// ErrIndexExists if it exists. Three phases so a large label scan does not
// hold the store mutex (backlog 17h; same shape as badger):
//
//	1 (Lock): install an empty live index capturing concurrent writes via
//	  Mutated; snapshot the label's node IDs.
//	2 (brief per-row RLock): read each node's current value. Safe because
//	  stored nodes are frozen and writes REPLACE the map entry, so a stale
//	  pointer never tears.
//	3 (Lock): merge the backfill, skipping IDs Mutated or deleted meanwhile.
func (ms *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	return ms.CreatePropertyIndexWithOptions(labelToken, propertyKey, storecontract.PropertyIndexOptions{})
}

// CreatePropertyIndexWithOptions is CreatePropertyIndex with
// storecontract.PropertyIndexOptions (round 4 R2: RangeCounts).
func (ms *Store) CreatePropertyIndexWithOptions(labelToken uint16, propertyKey string, opts storecontract.PropertyIndexOptions) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1. checkOpenLocked before argument validation: a closed store
	// reports ErrStoreClosed even for invalid arguments (pinned by
	// TestMemoryStoreIndexAPIsCheckLifecycleBeforeValidation).
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidatePropertyIndexOptions(opts); err != nil {
		ms.mu.Unlock()
		return err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.propertyIndexes[key]; exists {
		ms.mu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewPropertyIndexWith(opts.RangeCounts, opts.Elements)
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	ms.propertyIndexes[key] = liveIdx
	nids := ms.labelNodeIDsSnapshotLocked(labelToken)
	ms.mu.Unlock()

	// Phase 2.
	backfill := indexpkg.NewPostingsBuild(opts.Elements)
	for _, nid := range nids {
		ms.mu.RLock()
		n, ok := ms.nodes[nid]
		ms.mu.RUnlock()
		phase2Yield() // test seam: ms.mu is provably unheld here
		if !ok {
			continue // deleted between snapshot and fetch
		}
		backfill.AddEntityKeys(nid.SnowflakeID(), propertyKey, n.ForEachPropertyIndexKey)
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := requirePropertyIndexCurrentForCreate(ms.propertyIndexes, key, liveIdx); err != nil {
		return err
	}
	return liveIdx.FinishBuild(backfill, func(id snowflake.ID) (bool, error) {
		_, alive := ms.nodes[types.NodeID(id)] // a node deleted during Phase 2 is skipped
		return alive, nil
	})
}

// labelNodeIDsSnapshotLocked returns a slice copy of the label's current node
// ID set. The caller MUST hold at least ms.mu's read lock.
func (ms *Store) labelNodeIDsSnapshotLocked(labelToken uint16) []types.NodeID {
	idSet, ok := ms.labelIdx[labelToken]
	if !ok {
		return nil
	}
	nids := make([]types.NodeID, 0, len(idSet))
	for nid := range idSet {
		nids = append(nids, nid)
	}
	return nids
}

// requirePropertyIndexCurrentForCreate: Phase 3 check that the placeholder is
// still the live index (mirrors badger's helper). No fatal-error companion:
// a map lookup cannot fail.
func requirePropertyIndexCurrentForCreate(idxs map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex, key indexpkg.PropertyIndexKey, expected *indexpkg.PropertyIndex) error {
	current := idxs[key]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create property index: index dropped during creation: %w", ErrIndexNotFound)
	}
	return fmt.Errorf("graph: create property index: index replaced during creation: %w", ErrIndexExists)
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (ms *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.propertyIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	delete(ms.propertyIndexes, key)
	delete(ms.nodePropTxMembers, key) // its membership sidecar goes with it
	return nil
}

// --- Composite property indexes ---

// CreateCompositePropertyIndex creates a composite index over 2..4
// ORDER-PRESERVING keys for the label token. ErrIndexExists for the same
// (labelToken, ordered keys); a different order of the same keys is a
// distinct definition. Three phases as in CreatePropertyIndex (backlog 17h).
func (ms *Store) CreateCompositePropertyIndex(labelToken uint16, keys []string) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1.
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		ms.mu.Unlock()
		return err
	}
	key := indexpkg.CompositeIndexKey{LabelToken: labelToken, Keys: indexpkg.EncodeCompositeKeyTuple(keys)}
	if _, exists := ms.compositeIndexes[key]; exists {
		ms.mu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewCompositePropertyIndex(keys)
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	indexpkg.RegisterCompositeIndex(ms.compositeIndexes, ms.compositeIndexesByLabel, key, liveIdx)
	nids := ms.labelNodeIDsSnapshotLocked(labelToken)
	ms.mu.Unlock()

	// Phase 2.
	backfill := indexpkg.NewPostingsBuild(false)
	for _, nid := range nids {
		ms.mu.RLock()
		n, ok := ms.nodes[nid]
		ms.mu.RUnlock()
		phase2Yield() // test seam: ms.mu is provably unheld here
		if !ok {
			continue // deleted between snapshot and fetch
		}
		if vk, found := indexpkg.NodeCompositeValueKey(liveIdx.Keys, n); found {
			backfill.AddKey(nid.SnowflakeID(), vk)
		}
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := requireCompositeIndexCurrentForCreate(ms.compositeIndexes, key, liveIdx); err != nil {
		return err
	}
	backfill.Range(func(vk string, ids []snowflake.ID) bool {
		for _, id := range ids {
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := ms.nodes[types.NodeID(id)]; !alive {
				continue // node deleted during Phase 2
			}
			liveIdx.AddKey(id, vk)
		}
		return true
	})
	liveIdx.Mutated = nil // stop tracking — index creation complete
	return nil
}

// requireCompositeIndexCurrentForCreate mirrors requirePropertyIndexCurrentForCreate
// (and badger's identically-named helper) for the composite-index door.
func requireCompositeIndexCurrentForCreate(idxs map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex, key indexpkg.CompositeIndexKey, expected *indexpkg.CompositePropertyIndex) error {
	current := idxs[key]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create composite property index: index dropped during creation: %w", ErrIndexNotFound)
	}
	return fmt.Errorf("graph: create composite property index: index replaced during creation: %w", ErrIndexExists)
}

// DropCompositePropertyIndex removes a composite property index declared
// over the exact ordered keys. Returns ErrIndexNotFound if no such
// definition exists.
func (ms *Store) DropCompositePropertyIndex(labelToken uint16, keys []string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}

	key := indexpkg.CompositeIndexKey{LabelToken: labelToken, Keys: indexpkg.EncodeCompositeKeyTuple(keys)}
	if _, exists := ms.compositeIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	indexpkg.UnregisterCompositeIndex(ms.compositeIndexes, ms.compositeIndexesByLabel, key)
	return nil
}

// NodesByLabelAndProperties returns nodes of labelToken whose current row
// matches EVERY (key, value) pair (AND, equality only). Uses a composite
// index whose key SET equals values' keys, else label scan + filter.
func (ms *Store) NodesByLabelAndProperties(labelToken uint16, values map[string]any, opts QueryOpts) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := validateCompositeQueryValues(values); err != nil {
		return nil, err
	}

	if idx, found := indexpkg.FindCompositeIndexForQuery(ms.compositeIndexes, ms.compositeIndexesByLabel, labelToken, values); found {
		vk, ok := indexpkg.QueryCompositeValueKey(idx.Keys, values)
		if !ok {
			return nil, nil
		}
		// The posting list is sorted (backlog 72): the page seeks past
		// opts.After and reads only its rows, no copy or sort of the key.
		ids := idx.Postings(vk)
		if ids.Len() == 0 {
			return nil, nil
		}
		return ms.nodesByLabelPropertiesFrom(labelToken, values, ids.After(opts.After.SnowflakeID()), ids.Len(), opts), nil
	}

	// Fallback: label scan + post-filter.
	slog.Debug("graph: NodesByLabelAndProperties using full label scan (no matching composite index)",
		"labelToken", labelToken, "keys", len(values))
	labelIDs := ms.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		return nil, nil
	}
	matchIDs := make([]types.NodeID, 0, len(labelIDs))
	for id := range labelIDs {
		matchIDs = append(matchIDs, id)
	}
	storepkg.SortNodeIDs(matchIDs)
	return ms.nodesByLabelPropertiesFrom(labelToken, values, idsAfter(matchIDs, opts.After), len(matchIDs), opts), nil
}

// nodesByLabelPropertiesFrom filters the ascending candidates ids (already
// past opts.After; n of them at most) to the label's current matches, then
// opts' temporal filter, then at most opts.Limit.
func (ms *Store) nodesByLabelPropertiesFrom(labelToken uint16, values map[string]any, ids iter.Seq[snowflake.ID], n int, opts QueryOpts) []*types.Node {
	hasTemporal := storepkg.HasTemporalFilter(opts)
	result := make([]*types.Node, 0, resultCap(opts.Limit, n))
	for raw := range ids {
		id := types.NodeID(raw)
		if n, ok := ms.nodes[id]; ok {
			if !n.HasLabelTokenRaw(labelToken) {
				continue
			}
			if !indexpkg.NodeMatchesAllProperties(n, values) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
				continue
			}
			result = append(result, n) // the frozen stored row, as every plural read
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// addNodeToCompositeIndexesLocked / removeNodeFromCompositeIndexesLocked
// are called at the same sites as indexpkg.AddNodeToPropertyIndexes /
// RemoveNodeFromPropertyIndexes; keep both together.
func (ms *Store) addNodeToCompositeIndexesLocked(n *types.Node, rawID snowflake.ID) {
	indexpkg.AddNodeToCompositeIndexes(ms.compositeIndexes, ms.compositeIndexesByLabel, n, rawID)
}

func (ms *Store) removeNodeFromCompositeIndexesLocked(n *types.Node, rawID snowflake.ID) {
	indexpkg.RemoveNodeFromCompositeIndexes(ms.compositeIndexes, ms.compositeIndexesByLabel, n, rawID)
}

// validateCompositeQueryValues validates the query-side (key,value) map: 2..4
// keys, no shadow keys, and every value indexable per the property allowlist.
func validateCompositeQueryValues(values map[string]any) error {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}
	for _, v := range values {
		if err := types.ValidatePropertyValue(v); err != nil {
			return fmt.Errorf("graph: nodes by label and properties value: %w", err)
		}
	}
	return nil
}

// --- Temporal indexes ---

// CreateTemporalIndex creates a temporal interval index on nodes with the
// label token; ErrTemporalIndexExists if the label already has a temporal or
// high-frequency index. Three phases as in CreatePropertyIndex (backlog 17h)
// with two differences:
//
//	2: each node's current row AND history rows are read under the SAME RLock
//	   (no tearing) and folded into the per-ID ENVELOPE, so a past version with
//	   a different valid interval is still covered and the resolver's
//	   predicate-anywhere narrowing never misses it.
//	3: Extend the live index with every interval collected for each node still
//	   alive and not Mutated; Extend unions into the existing entry.
func (ms *Store) CreateTemporalIndex(labelToken uint16) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1.
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.Unlock()
		return err
	}
	if _, exists := ms.temporalIndexes[labelToken]; exists {
		ms.mu.Unlock()
		return ErrTemporalIndexExists
	}
	if _, exists := ms.hfIndexes[labelToken]; exists {
		ms.mu.Unlock()
		return ErrTemporalIndexExists
	}
	liveTI := indexpkg.NewTemporalIndex()
	liveTI.Building = true
	liveTI.Mutated = make(map[snowflake.ID]struct{})
	ms.temporalIndexes[labelToken] = liveTI
	nids := ms.labelNodeIDsSnapshotLocked(labelToken)
	ms.mu.Unlock()

	// Phase 2.
	type interval struct{ from, to types.Instant }
	type nodeEnvelope struct {
		id        snowflake.ID
		intervals []interval
	}
	backfill := make([]nodeEnvelope, 0, len(nids))
	for _, nid := range nids {
		rawID := nid.SnowflakeID()
		var entry nodeEnvelope
		// The history map is ranged under the RLock: a concurrent
		// PutNodeVersion writes the same inner map.
		ms.mu.RLock()
		n, ok := ms.nodes[nid]
		if ok {
			from, to := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
			entry = nodeEnvelope{id: rawID, intervals: []interval{{from, to}}}
			for _, hv := range ms.nodeHistory[nid] {
				if hv == nil {
					continue
				}
				hf, ht := indexpkg.NodeTemporalBounds(rawID, hv.Temporal())
				entry.intervals = append(entry.intervals, interval{hf, ht})
			}
		}
		ms.mu.RUnlock()
		phase2Yield() // test seam: ms.mu is provably unheld here
		if !ok {
			continue // deleted between snapshot and fetch
		}
		backfill = append(backfill, entry)
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := requireTemporalIndexCurrentForCreate(ms.temporalIndexes, labelToken, liveTI); err != nil {
		return err
	}
	// An ID written during Phase 2 folded only the row that write made: its
	// current row and whole history are read here, under the lock, instead of
	// the stale Phase 2 copy (backlog 56).
	written := liveTI.Mutated
	liveTI.ClearMutationTracking()
	for _, entry := range backfill {
		if _, alive := ms.nodes[types.NodeID(entry.id)]; !alive {
			continue // node deleted during Phase 2
		}
		if _, w := written[entry.id]; w {
			continue // folded from the live rows below
		}
		for _, iv := range entry.intervals {
			liveTI.Extend(entry.id, iv.from, iv.to)
		}
	}
	for id := range written {
		if cur, alive := ms.nodes[types.NodeID(id)]; alive {
			from, to := indexpkg.NodeTemporalBounds(id, cur.Temporal())
			liveTI.Extend(id, from, to)
		}
		ms.foldNodeHistoryLocked(liveTI, id)
	}
	liveTI.Building = false
	return nil
}

// foldNodeHistoryLocked extends ti by every history row of id. Caller holds
// ms.mu (write).
func (ms *Store) foldNodeHistoryLocked(ti *indexpkg.TemporalIndex, id snowflake.ID) {
	for _, hv := range ms.nodeHistory[types.NodeID(id)] {
		if hv != nil {
			from, to := indexpkg.NodeTemporalBounds(id, hv.Temporal())
			ti.Extend(id, from, to)
		}
	}
}

// addNodeToTemporalIndexesLocked extends the envelope of every label of n by
// its row. A published index that does not cover id yet first gets id's
// history, else the envelope would vouch for less than the ID holds
// (backlog 56). Caller holds ms.mu (write).
func (ms *Store) addNodeToTemporalIndexesLocked(n *types.Node, id snowflake.ID) {
	for i := 0; i < n.LabelTokenCount(); i++ {
		ti := ms.temporalIndexes[n.LabelTokenRawAt(i)]
		if ti == nil || ti.Building {
			continue
		}
		if _, _, covered := ti.EnvelopeOf(id); !covered {
			ms.foldNodeHistoryLocked(ti, id)
		}
	}
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, id)
}

// extendNodeVersionTemporalLocked folds a PutNodeVersion row into the index of
// each of its labels that covers id or is still Building (the create folds
// the history of every ID written meanwhile). An uncovered ID stays uncovered
// and is never pruned. Caller holds ms.mu (write).
func (ms *Store) extendNodeVersionTemporalLocked(n *types.Node, id snowflake.ID) {
	from, to := indexpkg.NodeTemporalBounds(id, n.Temporal())
	for i := 0; i < n.LabelTokenCount(); i++ {
		ti := ms.temporalIndexes[n.LabelTokenRawAt(i)]
		if ti == nil {
			continue
		}
		if _, _, covered := ti.EnvelopeOf(id); covered || ti.Building {
			ti.Extend(id, from, to)
		}
	}
}

// addRelToTemporalIndexesLocked extends the envelope of r's type by r. An index
// that does not cover id yet first gets id's history (a deleted relationship
// re-created or re-imported under the same ID keeps its old rows), else the
// envelope would vouch for less than the ID holds and a valid-time prune would
// drop a row a read at an earlier pin returns (backlog 75 review; badger's twin
// is foldRecreatedRelHistoryLocked, the node side addNodeToTemporalIndexesLocked).
// Caller holds ms.mu (write).
func (ms *Store) addRelToTemporalIndexesLocked(r *types.Relationship, id snowflake.ID) {
	if ti := ms.relTypeTemporalIndexes[r.TypeToken().Value()]; ti != nil {
		if _, _, covered := ti.EnvelopeOf(id); !covered {
			for _, hv := range ms.relHistory[types.RelID(id)] {
				if hv != nil {
					from, to := indexpkg.RelTemporalBounds(id, hv.Temporal())
					ti.Extend(id, from, to)
				}
			}
		}
	}
	indexpkg.AddRelToTemporalIndexes(ms.relTypeTemporalIndexes, r, id)
}

// requireTemporalIndexCurrentForCreate mirrors requirePropertyIndexCurrentForCreate
// (and badger's identically-named helper) for the temporal-index door.
func requireTemporalIndexCurrentForCreate(idxs map[uint16]*indexpkg.TemporalIndex, labelToken uint16, expected *indexpkg.TemporalIndex) error {
	current := idxs[labelToken]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create temporal index: index dropped during creation: %w", ErrTemporalIndexNotFound)
	}
	return fmt.Errorf("graph: create temporal index: index replaced during creation: %w", ErrTemporalIndexExists)
}

// DropTemporalIndex removes a temporal index for the given label token.
// Returns ErrTemporalIndexNotFound if no index exists.
func (ms *Store) DropTemporalIndex(labelToken uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}

	if _, exists := ms.temporalIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(ms.temporalIndexes, labelToken)
	return nil
}

// --- Relationship-type temporal indexes (BACKLOG 21c) ---

// CreateRelTemporalIndex creates a temporal interval index on relationships
// of the rel-type token, scanning existing ones; ErrTemporalIndexExists if
// present. Rel-side mirror of CreateTemporalIndex (envelope fold).
func (ms *Store) CreateRelTemporalIndex(relType uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}

	if _, exists := ms.relTypeTemporalIndexes[relType]; exists {
		return ErrTemporalIndexExists
	}

	ti := indexpkg.NewTemporalIndex()
	// ADR-0011: every current row of the type — memtable and sealed.
	if err := ms.forEachTypeRowLocked(relType, func(r *types.Relationship) bool {
		relID := r.ID()
		rawID := relID.SnowflakeID()
		from, to := indexpkg.RelTemporalBounds(rawID, r.Temporal())
		ti.Extend(rawID, from, to)
		for _, hv := range ms.relHistory[relID] {
			if hv == nil {
				continue
			}
			hf, ht := indexpkg.RelTemporalBounds(rawID, hv.Temporal())
			ti.Extend(rawID, hf, ht)
		}
		return true
	}); err != nil {
		return err
	}

	ms.relTypeTemporalIndexes[relType] = ti
	return nil
}

// DropRelTemporalIndex removes a rel-type temporal index.
// Returns ErrTemporalIndexNotFound if no index exists.
func (ms *Store) DropRelTemporalIndex(relType uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}

	if _, exists := ms.relTypeTemporalIndexes[relType]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(ms.relTypeTemporalIndexes, relType)
	return nil
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed index on nodes with the
// label token. ErrInvalidTemporalIndexConfig if bucketSize is not a positive
// whole millisecond; ErrTemporalIndexExists if the label already has a
// temporal or high-frequency index. Three phases as in CreatePropertyIndex
// (backlog 17h).
func (ms *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1.
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
		ms.mu.Unlock()
		return err
	}
	if _, exists := ms.temporalIndexes[labelToken]; exists {
		ms.mu.Unlock()
		return ErrTemporalIndexExists
	}
	if _, exists := ms.hfIndexes[labelToken]; exists {
		ms.mu.Unlock()
		return ErrTemporalIndexExists
	}
	liveHFI := indexpkg.NewHighFrequencyIndex(bucketSize, 0)
	liveHFI.Mutated = make(map[snowflake.ID]struct{})
	ms.hfIndexes[labelToken] = liveHFI
	nids := ms.labelNodeIDsSnapshotLocked(labelToken)
	ms.mu.Unlock()

	// Phase 2.
	type nodeEntry struct {
		id   snowflake.ID
		from types.Instant
	}
	backfill := make([]nodeEntry, 0, len(nids))
	for _, nid := range nids {
		ms.mu.RLock()
		n, ok := ms.nodes[nid]
		ms.mu.RUnlock()
		phase2Yield() // test seam: ms.mu is provably unheld here
		if !ok {
			continue // deleted between snapshot and fetch
		}
		rawID := nid.SnowflakeID()
		from, _ := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
		backfill = append(backfill, nodeEntry{id: rawID, from: from})
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := requireHighFrequencyIndexCurrentForCreate(ms.hfIndexes, labelToken, liveHFI); err != nil {
		return err
	}
	for _, entry := range backfill {
		if _, alive := ms.nodes[types.NodeID(entry.id)]; !alive {
			continue // node deleted during Phase 2
		}
		if liveHFI.WasMutated(entry.id) {
			continue // concurrent write handled this ID during Phase 2
		}
		liveHFI.Add(types.NodeID(entry.id), entry.from)
	}
	liveHFI.ClearMutationTracking()
	return nil
}

// requireHighFrequencyIndexCurrentForCreate mirrors requirePropertyIndexCurrentForCreate
// (and badger's identically-named helper) for the high-frequency-index door.
func requireHighFrequencyIndexCurrentForCreate(idxs map[uint16]*indexpkg.HighFrequencyIndex, labelToken uint16, expected *indexpkg.HighFrequencyIndex) error {
	current := idxs[labelToken]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create high-frequency index: index dropped during creation: %w", ErrTemporalIndexNotFound)
	}
	return fmt.Errorf("graph: create high-frequency index: index replaced during creation: %w", ErrTemporalIndexExists)
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (ms *Store) DropHighFrequencyIndex(labelToken uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}

	if _, exists := ms.hfIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(ms.hfIndexes, labelToken)
	return nil
}

// CreateVectorIndex creates a vector similarity index for the label token and
// property key, expecting vectors of length dims, and populates it from
// existing nodes. Defaults to approximate HNSW (CreateVectorIndexWithOptions
// for brute force or tuning). ErrVectorIndexExists on duplicate.
func (ms *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	return ms.CreateVectorIndexWithOptions(labelToken, propertyKey, dims, metric, storecontract.VectorIndexOptions{})
}

// CreateVectorIndexWithOptions is CreateVectorIndex with control over the
// engine (opts.UseBruteForce) and HNSW tuning (M / EfConstruction /
// EfSearch); zero opts equals CreateVectorIndex. Three phases as in
// CreatePropertyIndex (backlog 17h). While backfilling, the index is
// "Building" and searches decline it as ErrVectorIndexNotFound until Phase 3.
func (ms *Store) CreateVectorIndexWithOptions(labelToken uint16, propertyKey string, dims int, metric DistanceMetric, opts storecontract.VectorIndexOptions) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1.
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := indexpkg.ValidateVectorIndexConfig(dims, metric); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := indexpkg.ValidateVectorIndexOptions(opts); err != nil {
		ms.mu.Unlock()
		return err
	}
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; exists {
		ms.mu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric, Mutated: make(map[snowflake.ID]struct{})}
	indexpkg.ApplyVectorIndexOptions(vi, opts)
	ms.vectorIndexes[key] = vi
	nids := ms.labelNodeIDsSnapshotLocked(labelToken)
	ms.mu.Unlock()

	// Phase 2. Populate from nodes carrying this label only — keep this
	// backfill shape aligned with the other index builders and avoid
	// scanning unrelated node rows.
	type vectorBackfillEntry struct {
		id  snowflake.ID
		vec []float32
	}
	backfill := make([]vectorBackfillEntry, 0, len(nids))
	for _, nid := range nids {
		ms.mu.RLock()
		n, ok := ms.nodes[nid]
		ms.mu.RUnlock()
		phase2Yield() // test seam: ms.mu is provably unheld here
		if !ok {
			continue // deleted between snapshot and fetch
		}
		vec, ok := n.Float32SlicePropertyCopy(propertyKey)
		if !ok {
			continue
		}
		backfill = append(backfill, vectorBackfillEntry{id: nid.SnowflakeID(), vec: vec})
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := indexpkg.RequireVectorIndexCurrentForCreate(ms.vectorIndexes, key, vi); err != nil {
		return err
	}
	for _, entry := range backfill {
		if _, alive := ms.nodes[types.NodeID(entry.id)]; !alive {
			continue // node deleted during Phase 2
		}
		if vi.WasMutated(entry.id) {
			continue // concurrent write handled this ID during Phase 2
		}
		if err := vi.AddOwned(entry.id, entry.vec); err != nil {
			indexpkg.DeleteVectorIndexIfCurrent(ms.vectorIndexes, key, vi)
			return fmt.Errorf("graph: create vector index: node %d: %w", entry.id, err)
		}
	}
	vi.ClearMutationTracking()
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (ms *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	delete(ms.vectorIndexes, key)
	return nil
}

// SearchNearestNodes returns the k nodes closest to query under the index for
// labelToken+propertyKey, ascending distance. Errors: ErrVectorIndexNotFound
// (no index), ErrDimensionMismatch (query length), ErrInvalidVectorValue
// (NaN/Inf). nil, nil if the index is empty. Temporal filters and After/Limit
// apply to the distance-ordered result; Depth is only validated.
func (ms *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ms.vectorIndexes[key]
	ms.mu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	// A live-but-still-backfilling index (BACKLOG 17h's 3-phase
	// CreateVectorIndexWithOptions) is not yet trustworthy for search — mirrors
	// badger's identical IsBuilding gate in its SearchNearestNodes.
	if vi.IsBuilding() {
		return nil, ErrVectorIndexNotFound
	}
	if k <= 0 {
		if _, err := vi.SearchNearest(query, k, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}

	dims := vi.Dims
	filter, err := ms.vectorTemporalFilter(vi, key, dims, opts)
	if err != nil {
		return nil, err
	}
	ids, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	hasTemporal := storepkg.HasTemporalFilter(opts)
	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[types.NodeID(id)]; ok {
			if !indexpkg.NodeMatchesVectorIndex(n, key, dims) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id, n.Temporal(), opts) {
				continue
			}
			result = append(result, n) // the frozen stored row, as every plural read
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return storepkg.PaginateNodesInOrder(result, opts.After, opts.Limit), nil
}

func (ms *Store) vectorTemporalFilter(vi *indexpkg.VectorIndex, key indexpkg.VectorIndexKey, dims int, opts QueryOpts) (func(snowflake.ID) bool, error) {
	if !storepkg.HasTemporalFilter(opts) {
		return nil, nil
	}
	ids := vi.IDs()
	eligible := make(map[snowflake.ID]struct{})
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		n := ms.nodes[types.NodeID(id)]
		if n == nil {
			continue
		}
		if !indexpkg.NodeMatchesVectorIndex(n, key, dims) {
			continue
		}
		if storepkg.MatchesTemporalFilter(id, n.Temporal(), opts) {
			eligible[id] = struct{}{}
		}
	}
	return func(id snowflake.ID) bool {
		_, ok := eligible[id]
		return ok
	}, nil
}

// SearchNearestFiltered is the Graph layer's vector search with an
// eligibility filter applied BEFORE the k-cut (combined with current-row
// shape validation). Returns raw snowflake.IDs by ascending distance; the
// caller resolves entities.
func (ms *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ms.vectorIndexes[key]
	ms.mu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	// See SearchNearestNodes's identical gate.
	if vi.IsBuilding() {
		return nil, ErrVectorIndexNotFound
	}
	dims := vi.Dims
	var filterErr error
	shapeFilter := func(id snowflake.ID) bool {
		ms.mu.RLock()
		if err := ms.checkOpenLocked(); err != nil {
			ms.mu.RUnlock()
			if filterErr == nil {
				filterErr = err
			}
			return false
		}
		n := ms.nodes[types.NodeID(id)]
		if n == nil {
			ms.mu.RUnlock()
			return false
		}
		matches := indexpkg.NodeMatchesVectorIndex(n, key, dims)
		ms.mu.RUnlock()
		if !matches {
			return false
		}
		return filter == nil || filter(id)
	}
	ids, err := vi.SearchNearest(query, k, shapeFilter)
	if err != nil {
		return nil, err
	}
	if filterErr != nil {
		return nil, filterErr
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	return ids, nil
}

// NodesByLabelAndProperty returns nodes matching label and property value,
// with pagination and temporal filtering. Uses the property index if present,
// else label scan + filter. Sorted by snowflake.ID. Rows are the store's
// shared FROZEN entries, as for every plural read.
func (ms *Store) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: nodes by label and property value: %w", err)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}
	if idx, ok := ms.propertyIndexes[key]; ok && idx.Mutated == nil {
		// Indexed path: the value's sorted posting list from opts.After on
		// (backlog 72), each row verified before the limit.
		ids := idx.Postings(value)
		if ids.Len() == 0 {
			return nil, nil
		}
		return ms.nodesByLabelPropertyFrom(labelToken, propKey, targetKey, idx.Elements(), ids.After(opts.After.SnowflakeID()), ids.Len(), opts), nil
	}

	// Fallback: label scan + property filter, also while the index is still
	// building (its postings are partial), with the declared index's match.
	slog.Debug("graph: NodesByLabelAndProperty using full label scan (no property index)",
		"labelToken", labelToken, "propertyKey", propKey)
	scanElements := ms.propertyIndexes[key].Elements()
	labelIDs := ms.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		return nil, nil
	}

	// Collect candidate IDs from label scan.
	matchIDs := make([]types.NodeID, 0, len(labelIDs))
	for id := range labelIDs {
		matchIDs = append(matchIDs, id)
	}

	if len(matchIDs) == 0 {
		return nil, nil
	}
	storepkg.SortNodeIDs(matchIDs)
	return ms.nodesByLabelPropertyFrom(labelToken, propKey, targetKey, scanElements, idsAfter(matchIDs, opts.After), len(matchIDs), opts), nil
}

// nodesByLabelPropertyFrom is nodesByLabelPropertiesFrom for one key.
func (ms *Store) nodesByLabelPropertyFrom(labelToken uint16, propKey, targetKey string, elements bool, ids iter.Seq[snowflake.ID], n int, opts QueryOpts) []*types.Node {
	hasTemporal := storepkg.HasTemporalFilter(opts)
	result := make([]*types.Node, 0, resultCap(opts.Limit, n))
	for raw := range ids {
		id := types.NodeID(raw)
		if n, ok := ms.nodes[id]; ok {
			if !n.HasLabelTokenRaw(labelToken) {
				continue
			}
			if !n.HasPropertyIndexKey(propKey, elements, targetKey) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
				continue
			}
			result = append(result, n) // the frozen stored row, as every plural read
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
