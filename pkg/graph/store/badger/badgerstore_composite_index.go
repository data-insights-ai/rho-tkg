// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"errors"
	"fmt"
	"log/slog"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// Composite property indexes (K3c) — RAM-only in v1 (no on-disk mode, unlike
// PropertyIndexOnDisk; see docs/query-planners.md "Composite property
// indexes" for the documented follow-up and planner guidance). Definitions
// are persisted (compositeIdxDef below) so a reopen rebuilds the SAME
// definitions and re-scans current node state; entries themselves never
// touch Badger.
//
// Maintenance shares the single-key property index's maintenance seam
// (maintainPropertyIndexesAdd/Remove/Purge in badgerstore_property_disk.go)
// rather than duplicating a call at all 18+ node-mutation door call sites —
// every door already funnels through those three functions.

// compositeIdxDef is the serialization format for composite property index
// definitions.
type compositeIdxDef struct {
	LabelToken uint16   `msgpack:"l"`
	Keys       []string `msgpack:"k"`
}

// CreateCompositePropertyIndex creates a composite property index over the
// declared, ORDER-PRESERVING keys (2..4) for the given label token.
// Three-phase approach to prevent blocking concurrent reads/writes during
// slow I/O — mirrors CreatePropertyIndex:
//
//	Phase 1 (write Lock): Install an empty live index so concurrent PutNode/ReplaceNode
//	writes are captured immediately. Snapshot existing node IDs.
//	Phase 2 (no lock): Prefetch node data to build a backfill set.
//	Phase 3 (write Lock): Merge backfill entries into the live index, skipping IDs
//	that were already handled by concurrent writes during Phase 2.
//
// Returns ErrIndexExists if an index for the exact same (labelToken, ordered
// keys) already exists — a different key ORDER for the same key SET is a
// distinct definition (no implicit dedup across orderings).
func (bs *Store) CreateCompositePropertyIndex(labelToken uint16, keys []string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}

	// Phase 1: Install empty live index + snapshot IDs under write Lock.
	bs.idxMu.Lock()
	key := indexpkg.CompositeIndexKey{LabelToken: labelToken, Keys: indexpkg.EncodeCompositeKeyTuple(keys)}
	if _, exists := bs.compositeIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewCompositePropertyIndex(keys)
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	indexpkg.RegisterCompositeIndex(bs.compositeIndexes, bs.compositeIndexesByLabel, key, liveIdx)
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	if idErr != nil {
		indexpkg.UnregisterCompositeIndex(bs.compositeIndexes, bs.compositeIndexesByLabel, key)
		bs.idxMu.Unlock()
		return idErr
	}
	bs.idxMu.Unlock()

	// Phase 2: Fetch node data OUTSIDE any lock.
	backfill := indexpkg.NewCompositePropertyIndex(keys)
	for _, nid := range nids {
		n, err := bs.prefetchNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			bs.idxMu.Lock()
			deleteCompositeIndexIfCurrent(bs.compositeIndexes, bs.compositeIndexesByLabel, key, liveIdx)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create composite property index: %w", err)
		}
		vk, found := indexpkg.NodeCompositeValueKey(keys, n)
		if !found {
			continue
		}
		backfill.AddKey(nid.SnowflakeID(), vk)
	}

	// Phase 3: Merge backfill into live index under write Lock. Skip
	// entries for IDs already handled by concurrent writes during Phase 2,
	// and entries for nodes deleted during Phase 2.
	bs.idxMu.Lock()
	if err := requireCompositeIndexCurrentForCreate(bs.compositeIndexes, key, liveIdx); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	for vk, idSet := range backfill.Entries {
		for id := range idSet {
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := bs.nodeIDs[types.NodeID(id)]; !alive {
				continue // node deleted during Phase 2
			}
			liveIdx.AddKey(id, vk)
		}
	}
	liveIdx.Mutated = nil // stop tracking — index creation complete
	bs.persistCompositeIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// DropCompositePropertyIndex removes a composite property index declared
// over the exact ordered keys. Returns ErrIndexNotFound if no such
// definition exists.
func (bs *Store) DropCompositePropertyIndex(labelToken uint16, keys []string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}

	bs.idxMu.Lock()
	key := indexpkg.CompositeIndexKey{LabelToken: labelToken, Keys: indexpkg.EncodeCompositeKeyTuple(keys)}
	if _, exists := bs.compositeIndexes[key]; !exists {
		bs.idxMu.Unlock()
		return ErrIndexNotFound
	}
	indexpkg.UnregisterCompositeIndex(bs.compositeIndexes, bs.compositeIndexesByLabel, key)
	bs.persistCompositeIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

func deleteCompositeIndexIfCurrent(idxs map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex, defsByLabel map[uint16][]indexpkg.CompositeIndexKey, key indexpkg.CompositeIndexKey, expected *indexpkg.CompositePropertyIndex) {
	if idxs[key] == expected {
		indexpkg.UnregisterCompositeIndex(idxs, defsByLabel, key)
	}
}

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

// persistCompositeIndexDefs serializes the current composite index
// definitions to Badger. Caller must hold bs.idxMu write lock.
func (bs *Store) persistCompositeIndexDefs() {
	var defs []compositeIdxDef
	for key, idx := range bs.compositeIndexes {
		if idx == nil || idx.Mutated != nil {
			continue
		}
		defs = append(defs, compositeIdxDef{LabelToken: key.LabelToken, Keys: idx.Keys})
	}
	if len(defs) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.CompositeIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		slog.Error("graph: persist composite index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.CompositeIndexDefsKey, value: data})
}

// NodesByLabelAndProperties returns nodes matching labelToken whose current
// row matches EVERY (key, value) pair in values (AND-conjunction, equality
// only — v1 has no partial-prefix or range semantics). Uses a matching
// composite index if one exists whose declared key SET equals values' keys
// and is not itself mid-creation (idx.Mutated == nil, same convention
// NodesByLabelAndProperty uses); falls back to a label scan + post-filter
// otherwise.
func (bs *Store) NodesByLabelAndProperties(labelToken uint16, values map[string]any, opts QueryOpts) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := validateCompositeQueryValuesBadger(values); err != nil {
		return nil, err
	}

	// Snapshot matching IDs under RLock, then release before entity I/O.
	bs.idxMu.RLock()
	idx, found := indexpkg.FindCompositeIndexForQuery(bs.compositeIndexes, bs.compositeIndexesByLabel, labelToken, values)
	if found && idx.Mutated == nil {
		vk, ok := indexpkg.QueryCompositeValueKey(idx.Keys, values)
		if !ok {
			bs.idxMu.RUnlock()
			return nil, nil
		}
		nids := idx.NodeIDs(vk)
		if len(nids) == 0 {
			bs.idxMu.RUnlock()
			return nil, nil
		}
		bs.idxMu.RUnlock()

		storepkg.SortNodeIDs(nids)
		nids = bs.filterNodeIDsByTemporalPeek(nids, opts)
		return bs.fetchNodesByLabelPropertiesIDs(labelToken, values, nids, opts)
	}

	// Fallback: snapshot label IDs, release lock, then scan properties.
	slog.Debug("graph: NodesByLabelAndProperties using full label scan (no matching composite index)",
		"labelToken", labelToken, "keys", len(values))
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return nil, idErr
	}
	if len(nids) == 0 {
		return nil, nil
	}
	storepkg.SortNodeIDs(nids)
	return bs.fetchNodesByLabelPropertiesIDs(labelToken, values, nids, opts)
}

func (bs *Store) fetchNodesByLabelPropertiesIDs(labelToken uint16, values map[string]any, nids []types.NodeID, opts QueryOpts) ([]*types.Node, error) {
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0)
	if len(nids) == 0 {
		return nil, nil
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	var result []*types.Node
	for _, nid := range nids {
		n, err := bs.prefetchNodeScan(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // orphaned index entry
			}
			return nil, err
		}
		if !n.HasLabelTokenRaw(labelToken) {
			continue
		}
		if !indexpkg.NodeMatchesAllProperties(n, values) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(nid.SnowflakeID(), n.Temporal(), opts) {
			continue
		}
		result = append(result, n)
		if opts.Limit > 0 && len(result) >= opts.Limit {
			break
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// validateCompositeQueryValuesBadger validates the query-side (key,value)
// map: 2..4 keys, no shadow keys, and every value indexable per the
// property allowlist.
func validateCompositeQueryValuesBadger(values map[string]any) error {
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
