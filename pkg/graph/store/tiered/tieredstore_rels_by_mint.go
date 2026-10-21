package tiered

import (
	"cmp"
	"fmt"
	"slices"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.AdjacencyDepthCapability = (*Store)(nil)

// Rels by mint (Config.RelsByMint, row 94). A relationship whose endpoints
// both live on the reference shard is written, entity and both adjacency
// legs, to the event shard of its ID's mint instant
// (BadgerStore.PutRelationshipCoLocated: the same keys as a same-shard rel,
// one ChangeRelPut). routeRelShard already probes that shard, so every by-ID
// door is unchanged; a cold shard's close frees the legs with its Badger.
//
// Such a rel's row and legs never sit on its start node's shard, which is
// how the delete and repair doors tell it from a split-written one
// (relLegsColocated). Adjacency reads of a reference or archive node fan out
// over the event shards QueryOpts.Depth selects once the store is marked
// (relsByMintMarkerKey), and unite them with the node shard's legacy legs.

// relsByMintMarkerKey is the reference-shard meta entry set by the first open
// with Config.RelsByMint and never cleared: from then on reference and
// archive adjacency reads fan out, whatever later opens configure.
const relsByMintMarkerKey = "rels_by_mint"

type relsByMintState struct {
	enabled   bool          // Config.RelsByMint of this open: steers placement
	fanOut    atomic.Bool   // the store's marker is set: reference-side reads fan out
	fallbacks atomic.Uint64 // candidates placed on the reference shard because their mint shard was cold
}

// loadRelsByMint adopts the marker at open and sets it when this open places
// by mint, durably, before any rel can be placed.
func (ts *Store) loadRelsByMint(enabled bool) error {
	ts.relsByMint.enabled = enabled
	raw, err := ts.refShard.MetaGet(relsByMintMarkerKey)
	if err != nil {
		return fmt.Errorf("graph: read rels-by-mint marker: %w", err)
	}
	if raw == nil && enabled {
		if err := ts.refShard.MetaSetDurable(relsByMintMarkerKey, []byte{1}); err != nil {
			return fmt.Errorf("graph: write rels-by-mint marker: %w", err)
		}
	}
	ts.relsByMint.fanOut.Store(raw != nil || enabled)
	return nil
}

// resetRelsByMint follows a Clear of the reference shard, which wiped the
// marker: an empty store holds no mint rels, so only this open's option
// counts.
func (ts *Store) resetRelsByMint(ref *BadgerStore) error {
	if ts.relsByMint.enabled {
		if err := ref.MetaSetDurable(relsByMintMarkerKey, []byte{1}); err != nil {
			return fmt.Errorf("graph: write rels-by-mint marker: %w", err)
		}
	}
	ts.relsByMint.fanOut.Store(ts.relsByMint.enabled)
	return nil
}

// RelsByMintFallbacks returns how many relationships this open placed on the
// reference shard although Config.RelsByMint applied to them, because the
// event shard of their mint instant was cold. Process-local; 0 without the
// option.
func (ts *Store) RelsByMintFallbacks() uint64 {
	if ts == nil {
		return 0
	}
	return ts.relsByMint.fallbacks.Load()
}

// relsByMintCandidate reports whether a new rel with endpoints on start and
// end goes by mint under this open's option.
func (ts *Store) relsByMintCandidate(start, end *BadgerStore) bool {
	return ts.relsByMint.enabled && start == ts.refShard && end == ts.refShard
}

// relMintPlacement returns the event shard a new rel r with endpoints on
// start and end is written to, or ok=false for the legacy placement. A cold
// mint shard is ok=false, counted when count is set (the write, not its
// preflight).
func (ts *Store) relMintPlacement(r *types.Relationship, start, end *BadgerStore, count bool) (es *EventShard, ok bool) {
	if !ts.relsByMintCandidate(start, end) {
		return nil, false
	}
	es = ts.timestampToEventShardEntry(r.ID().SnowflakeID())
	if es.currentTier() == TierCold {
		if count {
			ts.relsByMint.fallbacks.Add(1)
		}
		return nil, false
	}
	return es, true
}

// relCreateOwner is the shard a create of r writes its row to, for
// relCreateCheck (which compares the pointer only): the mint shard when r goes
// by mint, else start.
func (ts *Store) relCreateOwner(r *types.Relationship, start, end *BadgerStore) *BadgerStore {
	es, ok := ts.relMintPlacement(r, start, end, false)
	if !ok {
		return start
	}
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store == nil {
		return start
	}
	return es.store
}

// relLegsColocated reports whether the rel whose row is on entity keeps both
// legs there: a same-shard rel, or a mint-placed one (its row is never on its
// start node's shard, a split-written one's always is).
func relLegsColocated(entity, start, end *BadgerStore) bool {
	return start == end || entity != start
}

// mintFanOut reports whether adjacency reads of a node on shard also read the
// mint-placed rels: the store is marked and the node is on the reference side.
func (ts *Store) mintFanOut(shard *BadgerStore) bool {
	if !ts.relsByMint.fanOut.Load() || shard == nil {
		return false
	}
	return shard == ts.refShard || shard == ts.refArchive.Load()
}

// forEachMintShard calls fn with every event shard depth selects, pinned. A
// cold one is opened for the read (Config.MaxOpenColdShards keeps it).
func (ts *Store) forEachMintShard(depth ShardDepth, fn func(*BadgerStore) error) error {
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(depth)
	ts.mu.RUnlock()
	for _, es := range shards {
		s, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return err
		}
		err = fn(s)
		release()
		if err != nil {
			return err
		}
	}
	return nil
}

// mintLegIDs is nid's typed adjacency on an event shard: the legs of its
// mint-placed rels (a split write keys none of a reference node's legs there).
func mintLegIDs(s *BadgerStore, nid types.NodeID, typeToken uint16, incoming bool) []snowflake.ID {
	if incoming {
		return s.IncomingRelIDs(nid.SnowflakeID(), typeToken)
	}
	return s.OutgoingRelIDsOfType(nid.SnowflakeID(), typeToken)
}

// mintRels returns the mint-placed rels of nids on the event shards depth
// selects, resolved on the shard holding their legs.
func (ts *Store) mintRels(nids []types.NodeID, typeToken uint16, incoming bool, depth ShardDepth) (map[types.NodeID][]*types.Relationship, error) {
	out := make(map[types.NodeID][]*types.Relationship)
	err := ts.forEachMintShard(depth, func(s *BadgerStore) error {
		for _, nid := range nids {
			for _, id := range mintLegIDs(s, nid, typeToken, incoming) {
				r, ok, err := relationshipRow(s, types.RelID(id))
				if err != nil {
					return fmt.Errorf("graph: read relationship %d: %w", id, err)
				}
				if !ok {
					continue // orphan leg: the repair door's business
				}
				if incoming && !relationshipMatchesIncoming(r, nid, typeToken) || !incoming && !relationshipMatchesOutgoing(r, nid, typeToken) {
					continue
				}
				r.Freeze() // as the cross-shard resolver's rows
				out[nid] = append(out[nid], r)
			}
		}
		return nil
	})
	return out, err
}

func relationshipMatchesOutgoing(r *types.Relationship, nid types.NodeID, typeToken uint16) bool {
	if r == nil || r.StartNodeID() != nid {
		return false
	}
	return typeToken == 0 || r.HasTypeTokenRaw(typeToken)
}

// mintDegree counts nid's mint-placed legs on depth's event shards (index
// entries, as BadgerStore's degree).
func (ts *Store) mintDegree(nid types.NodeID, typeToken uint16, incoming bool, depth ShardDepth) (int, error) {
	n := 0
	err := ts.forEachMintShard(depth, func(s *BadgerStore) error {
		n += len(mintLegIDs(s, nid, typeToken, incoming))
		return nil
	})
	return n, err
}

// unionRels merges a and b into one ID-sorted slice without duplicates; nil
// when both are empty, as the shard doors answer.
func unionRels(a, b []*types.Relationship) []*types.Relationship {
	if len(b) == 0 {
		return a
	}
	out := append(slices.Clip(a), b...)
	slices.SortFunc(out, func(x, y *types.Relationship) int { return cmp.Compare(x.ID(), y.ID()) })
	return slices.CompactFunc(out, func(x, y *types.Relationship) bool { return x.ID() == y.ID() })
}

// withMintRels adds nids' mint-placed rels (depth) to the per-node rows m of
// the nids that are on the reference side (fan).
func (ts *Store) withMintRels(m map[types.NodeID][]*types.Relationship, fan []types.NodeID, typeToken uint16, incoming bool, depth ShardDepth) (map[types.NodeID][]*types.Relationship, error) {
	if len(fan) == 0 {
		return m, nil
	}
	extra, err := ts.mintRels(fan, typeToken, incoming, depth)
	if err != nil {
		return nil, err
	}
	if len(extra) == 0 {
		return m, nil
	}
	if m == nil {
		m = make(map[types.NodeID][]*types.Relationship, len(extra))
	}
	for nid, rels := range extra {
		m[nid] = unionRels(m[nid], rels)
	}
	return m, nil
}

// connectedRelIDs is every rel ID adjacent to nid, which lives on shard: its
// legs there plus, for a reference-side node, its mint-placed legs on every
// tier (a node delete must see a cold shard's edges, not only the window's).
func (ts *Store) connectedRelIDs(shard *BadgerStore, nid types.NodeID) (out, in []snowflake.ID, err error) {
	raw := nid.SnowflakeID()
	out, in = shard.OutgoingRelIDs(raw), shard.IncomingRelIDs(raw, 0)
	if !ts.mintFanOut(shard) {
		return out, in, nil
	}
	err = ts.forEachMintShard(DepthAll, func(s *BadgerStore) error {
		out = append(out, mintLegIDs(s, nid, 0, false)...)
		in = append(in, mintLegIDs(s, nid, 0, true)...)
		return nil
	})
	return out, in, err
}

// refSideNode reports whether nid lives or lived on the reference side (the
// reference shard or the archive): the nodes whose ex-adjacency a mint shard
// can hold.
func (ts *Store) refSideNode(nid types.NodeID) (bool, error) {
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return false, err
	}
	defer refCheckin()
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return false, err
	}
	defer archiveCheckin()
	for _, s := range []*BadgerStore{ref, archive} {
		if s == nil {
			continue
		}
		if s.HasNodeID(nid.SnowflakeID()) {
			return true, nil
		}
		if has, err := s.HasNodeHistory(nid); err != nil || has {
			return has, err
		}
	}
	return false, nil
}

// ForEachExAdjacentRelIDByDepth implements store.AdjacencyDepthCapability:
// ForEachExAdjacentRelID with a reference-side node's mint shards limited to
// the tiers depth selects. An ID may repeat.
func (ts *Store) ForEachExAdjacentRelIDByDepth(nid types.NodeID, incoming bool, depth ShardDepth, fn func(types.RelID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := validateDepth(depth); err != nil {
		return err
	}
	refSide := false
	if ts.relsByMint.fanOut.Load() {
		var err error
		if refSide, err = ts.refSideNode(nid); err != nil {
			return err
		}
	}
	// A reference-side node's legs on an event shard are rels by mint, its
	// own mint shard's included: those shards are read by depth.
	ids, err := ts.exAdjRelIDs(nid, incoming, !refSide)
	if err != nil {
		return err
	}
	if refSide {
		if err := ts.forEachMintShard(depth, func(s *BadgerStore) error {
			return s.ForEachExAdjacentRelID(nid, incoming, func(id types.RelID) bool { ids = append(ids, id); return true })
		}); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}
