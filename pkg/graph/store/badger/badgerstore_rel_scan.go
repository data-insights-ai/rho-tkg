package badger

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Streaming relationship scans — the rel-side mirror of
// badgerstore_node_scan.go (enterprise-scale ceiling 2: slice-materializing
// query APIs). Same contract throughout:
//
// Isolation: the ID set is snapshotted under the index lock, then rows are
// fetched and fn is called WITHOUT any store lock held — fn may freely call
// back into the store. Rows deleted between snapshot and fetch are skipped
// (same orphan tolerance as the materializing siblings); rows created after
// the snapshot are not seen.
//
// Rows are fetched through the scan (no-cache-fill) path and are FROZEN
// shared pointers — fn must not mutate them and must not retain them past
// its own return unless it accounts for the sharing.

// ForEachRelByType streams the type's relationships to fn in snowflake-ID
// order without materializing a result slice — the streaming sibling of
// RelationshipsByType for scan consumers (count/filter/aggregate pipelines)
// whose peak memory must stay O(1) in the type's cardinality. fn returning
// false stops the scan early.
//
// Temporal-index fast paths are intentionally NOT consulted beyond the Peek
// pre-filter: with a temporal filter present the per-row
// MatchesTemporalFilter check below is authoritative, just not pre-pruned.
func (bs *Store) ForEachRelByType(token uint16, opts QueryOpts, fn func(*types.Relationship) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}

	bs.idxMu.RLock()
	set := bs.typeIdx[token]
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil
	}
	storepkg.SortRelIDs(rids)
	rids = bs.filterRelIDsByTemporalPeek(rids, opts)
	rids = storepkg.PaginateRelIDs(rids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted since snapshot, or orphaned index entry
			}
			return fmt.Errorf("graph: scan relationship %d: %w", rid.SnowflakeID(), err)
		}
		if !r.HasTypeTokenRaw(token) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(rid.SnowflakeID(), r.Temporal(), opts) {
			continue
		}
		if !fn(r) {
			return nil
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return nil
		}
	}
	return nil
}

// ForEachOutgoingRel streams the node's outgoing relationships (optionally
// type-filtered; typeToken 0 means all types) to fn in snowflake-ID order —
// the streaming sibling of OutgoingRelationships for hub-degree adjacency
// consumers. fn returning false stops the scan early. Returns
// ErrNodeNotFound when the node does not exist, matching the materializing
// sibling.
func (bs *Store) ForEachOutgoingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error {
	return bs.forEachAdjacentRel(nid, typeToken, false, fn)
}

// ForEachIncomingRel is ForEachOutgoingRel for the incoming direction.
func (bs *Store) ForEachIncomingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error {
	return bs.forEachAdjacentRel(nid, typeToken, true, fn)
}

func (bs *Store) forEachAdjacentRel(nid types.NodeID, typeToken uint16, incoming bool, fn func(*types.Relationship) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := bs.ensureNodeRowLive(nid); err != nil {
		return err
	}

	bs.idxMu.RLock()
	if _, ok := bs.nodeIDs[nid]; !ok {
		bs.idxMu.RUnlock()
		return ErrNodeNotFound
	}
	rids, idErr := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, incoming)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return idErr
	}

	if len(rids) == 0 {
		return nil
	}
	storepkg.SortRelIDs(rids)

	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted since snapshot, or orphaned index entry
			}
			return fmt.Errorf("graph: scan relationship %d: %w", rid.SnowflakeID(), err)
		}
		match := relationshipMatchesOutgoing(r, nid, typeToken)
		if incoming {
			match = relationshipMatchesIncoming(r, nid, typeToken)
		}
		if !match {
			continue
		}
		if !fn(r) {
			return nil
		}
	}
	return nil
}
