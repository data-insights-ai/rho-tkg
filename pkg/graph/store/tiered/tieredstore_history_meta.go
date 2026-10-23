package tiered

import (
	"sort"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// History temporal metadata (store.TemporalMetaHistoryCapability) across the
// shards, so the graph's historical-pin resolution decodes only the winning
// rows on tiered too (row 120). The row set is exactly GetNodeHistory's /
// GetRelHistory's: the same routing (owner shard, archive or reference shard,
// the other shards holding earlier lives), merged by version.

var _ storecontract.TemporalMetaHistoryCapability = (*Store)(nil)

// NodeHistoryTemporalMeta returns the temporal skeleton of every history
// version of nid, ascending by version.
func (ts *Store) NodeHistoryTemporalMeta(nid types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
	return ts.nodeHistoryTemporalMetaVia(nid, directReads)
}

func (ts *Store) nodeHistoryTemporalMetaVia(nid types.NodeID, via shardVia) ([]storecontract.VersionTemporalMeta, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	store, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	liveHere, err := nodeRowLive(store, nid)
	if err != nil {
		return nil, err
	}
	liveHere, isArchive = ts.nodeLivesRoute(nid, liveHere, isArchive)
	return historyAcrossShards(ts, store, liveHere, isArchive, func(s *BadgerStore) ([]storecontract.VersionTemporalMeta, error) {
		return via(s).NodeHistoryTemporalMeta(nid)
	}, mergeHistoryMeta)
}

// RelHistoryTemporalMeta mirrors NodeHistoryTemporalMeta for relationships.
func (ts *Store) RelHistoryTemporalMeta(rid types.RelID) ([]storecontract.VersionTemporalMeta, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	shard, checkin, isArchive, err := ts.shardForRelIDCheckedWithArchive(rid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	liveHere, err := relationshipRowLive(shard, rid)
	if err != nil {
		return nil, err
	}
	liveHere, isArchive = ts.relLivesRoute(rid, liveHere, isArchive)
	return historyAcrossShards(ts, shard, liveHere, isArchive, func(s *BadgerStore) ([]storecontract.VersionTemporalMeta, error) {
		return s.RelHistoryTemporalMeta(rid)
	}, mergeHistoryMeta)
}

// historyAcrossShards follows GetNodeHistory's routing: a row live on an
// event shard answers alone; live on the reference shard adds the archive;
// archived adds the reference shard; otherwise every history shard is asked.
// merge folds the per-shard answers, the owner's first.
func historyAcrossShards[T any](ts *Store, owner *BadgerStore, liveHere, isArchive bool,
	fetch func(*BadgerStore) ([]T, error), merge func([][]T) []T) ([]T, error) {
	own, err := fetch(owner)
	if err != nil {
		return nil, err
	}
	parts := [][]T{own}
	add := func(s *BadgerStore, release func()) error {
		got, err := fetch(s)
		release()
		if err != nil {
			return err
		}
		parts = append(parts, got)
		return nil
	}
	switch {
	case liveHere && !isArchive:
		if owner != ts.refShard {
			return own, nil
		}
		ts.refArchiveGapHook()
		archive, release, err := ts.checkoutArchive()
		if err != nil {
			return nil, err
		}
		if archive == nil || archive == owner {
			if archive != nil {
				release()
			}
			return own, nil
		}
		if err := add(archive, release); err != nil {
			return nil, err
		}
	case isArchive:
		ref, release, err := ts.checkoutRefShard()
		if err != nil {
			return nil, err
		}
		if ref == owner {
			release()
			return own, nil
		}
		if err := add(ref, release); err != nil {
			return nil, err
		}
	default:
		if err := ts.forEachHistoryShard(owner, func(candidate *BadgerStore) (bool, error) {
			got, err := fetch(candidate)
			if err != nil {
				return false, err
			}
			parts = append(parts, got)
			return false, nil
		}); err != nil {
			return nil, err
		}
	}
	return merge(parts), nil
}

// mergeHistoryMeta merges per-shard skeletons ascending by version, keeping the
// first source's entry for a version (mergeNodeHistorySources' rule).
func mergeHistoryMeta(parts [][]storecontract.VersionTemporalMeta) []storecontract.VersionTemporalMeta {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if total == 0 {
		return nil
	}
	out := make([]storecontract.VersionTemporalMeta, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	deduped := out[:0]
	for i, m := range out {
		if i > 0 && m.Version == deduped[len(deduped)-1].Version {
			continue
		}
		deduped = append(deduped, m)
	}
	return deduped
}
