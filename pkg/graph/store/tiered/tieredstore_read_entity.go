package tiered

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Entity reads ---
// O(1) shard resolution: ref probe + timestamp extraction.

func (ts *Store) GetNode(nid types.NodeID) (*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return store.GetNode(nid)
}

// NodeIntegrityHash returns a live node's integrity hash via the owning shard
// without fetching a defensive copy of the full node.
func (ts *Store) NodeIntegrityHash(nid types.NodeID) (string, error) {
	if err := ts.checkOpen(); err != nil {
		return "", err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return "", err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return "", err
	}
	defer checkin()
	return store.NodeIntegrityHash(nid)
}

// EndpointIntegrityHashes returns both endpoint integrity hashes while each
// owning shard is pinned, without materializing node copies. Self-loop: one
// NodeIntegrityHash read returned twice (ErrNodeNotFound if missing). Same
// shard: the backend's atomic EndpointIntegrityHashes under one pin. Cross
// shard: two reads under their own pins (a concurrent Archive blocks on the
// entity lock held higher up the stack).
func (ts *Store) EndpointIntegrityHashes(startID, endID types.NodeID) (string, string, error) {
	if err := ts.checkOpen(); err != nil {
		return "", "", err
	}
	if err := storecontract.ValidateNodeID(startID); err != nil {
		return "", "", err
	}
	if err := storecontract.ValidateNodeID(endID); err != nil {
		return "", "", err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	startShard, startCheckin, err := ts.shardForNodeIDChecked(startID)
	if err != nil {
		return "", "", err
	}
	defer startCheckin()
	if startID == endID {
		hash, err := startShard.NodeIntegrityHash(startID)
		if err != nil {
			return "", "", err
		}
		return hash, hash, nil
	}

	endShard, endCheckin, err := ts.shardForNodeIDChecked(endID)
	if err != nil {
		return "", "", err
	}
	defer endCheckin()
	if startShard == endShard {
		return startShard.EndpointIntegrityHashes(startID, endID)
	}

	fromHash, err := startShard.NodeIntegrityHash(startID)
	if err != nil {
		return "", "", err
	}
	toHash, err := endShard.NodeIntegrityHash(endID)
	if err != nil {
		return "", "", err
	}
	return fromHash, toHash, nil
}

func (ts *Store) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	return ts.getRelationshipChecked(rid)
}

var _ storecontract.EntityLendCapability = (*Store)(nil)

// LendNode returns nid's current row frozen and without a copy from its owning
// shard (store.EntityLendCapability); it stays valid after checkin or
// idle-close.
func (ts *Store) LendNode(nid types.NodeID) (*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return store.LendNode(nid)
}

// LendRelationship is LendNode for relationships, routed as GetRelationship
// routes (reference shard, archive, the ID's event shard, then every other).
func (ts *Store) LendRelationship(rid types.RelID) (*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	return routeRelRow(ts, rid, lendRelationshipRow)
}

// getRelationshipChecked returns a defensive relationship copy from the shard
// owning rid, keeping shardForRelIDChecked's stale-index guard and reading the
// row only once.
func (ts *Store) getRelationshipChecked(rid types.RelID) (*types.Relationship, error) {
	return routeRelRow(ts, rid, relationshipRow)
}

// routeRelRow finds the shard holding rid's row and reads it with row (copy,
// lent row or stamps): reference, archive, the ID's event shard, then every
// other event shard; the first hit answers.
func routeRelRow[T any](ts *Store, rid types.RelID, row func(*BadgerStore, types.RelID) (T, bool, error)) (T, error) {
	var zero T
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return zero, err
	}
	rel, found, err := row(ref, rid)
	refCheckin()
	if err != nil {
		return zero, err
	}
	if found {
		return rel, nil
	}

	ts.refArchiveGapHook()
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return zero, err
	}
	if archive != nil {
		rel, found, err = row(archive, rid)
		archiveCheckin()
		if err != nil {
			return zero, err
		}
		if found {
			return rel, nil
		}
	}

	candidateEntry := ts.timestampToEventShardEntry(rid.SnowflakeID())
	candidate, candidateRelease, err := candidateEntry.checkoutStoreForRead(ts)
	if err != nil {
		return zero, err
	}
	rel, found, err = row(candidate, rid)
	candidateRelease()
	if err != nil {
		return zero, err
	}
	if found {
		return rel, nil
	}

	ts.mu.RLock()
	probe := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if es == candidateEntry {
			continue
		}
		probe = append(probe, es)
	}
	ts.mu.RUnlock()

	for _, es := range probe {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return zero, err
		}
		rel, found, err = row(store, rid)
		release()
		if err != nil {
			return zero, err
		}
		if found {
			return rel, nil
		}
	}
	return zero, ErrRelNotFound
}

func (ts *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	if len(ids) == 1 {
		n, err := ts.GetNode(ids[0])
		if err != nil {
			return nil, fmt.Errorf("graph: get nodes by IDs %d: %w", ids[0], err)
		}
		return []*types.Node{n}, nil
	}

	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	if unique, ok := uniqueNodeIDsPreserveOrderIfDuplicate(ids); ok {
		return ts.getNodesByIDsWithDuplicates(ids, unique)
	}

	batches, err := ts.groupNodeIDsByShard(ids)
	if err != nil {
		return nil, err
	}

	result := make([]*types.Node, 0, len(ids))
	for _, batch := range batches {
		store, release, err := batch.checkout()
		if err != nil {
			return nil, err
		}
		nodes, err := store.GetNodesByIDs(batch.ids)
		release()
		if err != nil {
			return nil, fmt.Errorf("graph: get nodes by IDs: %w", err)
		}
		result = append(result, nodes...)
	}
	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(result)
	return result, nil
}

func (ts *Store) getNodesByIDsWithDuplicates(ids, unique []types.NodeID) ([]*types.Node, error) {
	batches, err := ts.groupNodeIDsByShard(unique)
	if err != nil {
		return nil, err
	}

	found := make(map[types.NodeID]*types.Node, len(unique))
	for _, batch := range batches {
		store, release, err := batch.checkout()
		if err != nil {
			return nil, err
		}
		nodes, err := store.GetNodesByIDs(batch.ids)
		release()
		if err != nil {
			return nil, fmt.Errorf("graph: get nodes by IDs: %w", err)
		}
		for _, node := range nodes {
			found[node.ID()] = node
		}
	}

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		node, ok := found[id]
		if !ok {
			return nil, fmt.Errorf("graph: get nodes by IDs %d: %w", id, ErrNodeNotFound)
		}
		result = append(result, node)
	}
	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(result)
	return result, nil
}

func (ts *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storecontract.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	if len(ids) == 1 {
		r, err := ts.GetRelationship(ids[0])
		if err != nil {
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", ids[0], err)
		}
		return []*types.Relationship{r}, nil
	}

	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	unique := uniqueRelIDsPreserveOrder(ids)
	found, err := ts.getUniqueRelationshipsByIDs(unique)
	if err != nil {
		return nil, err
	}

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		rel, ok := found[id]
		if !ok {
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", id, ErrRelNotFound)
		}
		result = append(result, rel)
	}
	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(result)
	return result, nil
}

func uniqueNodeIDsPreserveOrder(ids []types.NodeID) []types.NodeID {
	unique := make([]types.NodeID, 0, len(ids))
	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

func uniqueNodeIDsPreserveOrderIfDuplicate(ids []types.NodeID) ([]types.NodeID, bool) {
	if len(ids) < 2 {
		return nil, false
	}
	if len(ids) <= 32 {
		for i, id := range ids {
			for _, prev := range ids[:i] {
				if id == prev {
					return uniqueNodeIDsPreserveOrder(ids), true
				}
			}
		}
		return nil, false
	}

	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return uniqueNodeIDsPreserveOrder(ids), true
		}
		seen[id] = struct{}{}
	}
	return nil, false
}

func uniqueRelIDsPreserveOrder(ids []types.RelID) []types.RelID {
	unique := make([]types.RelID, 0, len(ids))
	seen := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

func (ts *Store) getUniqueRelationshipsByIDs(ids []types.RelID) (map[types.RelID]*types.Relationship, error) {
	found := make(map[types.RelID]*types.Relationship, len(ids))
	pending := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		pending[id] = struct{}{}
	}

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	err = collectRelationshipsFromStore(ref, ids, found, pending)
	refCheckin()
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return found, nil
	}

	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, err
	}
	if archive != nil {
		err = collectRelationshipsFromStore(archive, ids, found, pending)
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		if len(pending) == 0 {
			return found, nil
		}
	}

	candidateBuckets := make(map[*EventShard][]types.RelID)
	for id := range pending {
		es := ts.timestampToEventShardEntry(id.SnowflakeID())
		candidateBuckets[es] = append(candidateBuckets[es], id)
	}
	checkedCandidates := make(map[*EventShard]struct{}, len(candidateBuckets))
	for es, bucket := range candidateBuckets {
		checkedCandidates[es] = struct{}{}
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return nil, err
		}
		err = collectRelationshipsFromStore(store, bucket, found, pending)
		release()
		if err != nil {
			return nil, err
		}
		if len(pending) == 0 {
			return found, nil
		}
	}

	ts.mu.RLock()
	probe := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if _, checked := checkedCandidates[es]; checked {
			continue
		}
		probe = append(probe, es)
	}
	ts.mu.RUnlock()

	for _, es := range probe {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return nil, err
		}
		err = collectPendingRelationshipsFromStore(store, found, pending)
		release()
		if err != nil {
			return nil, err
		}
		if len(pending) == 0 {
			return found, nil
		}
	}
	return found, nil
}

func collectRelationshipsFromStore(store *BadgerStore, ids []types.RelID, found map[types.RelID]*types.Relationship, pending map[types.RelID]struct{}) error {
	for _, id := range ids {
		if _, want := pending[id]; !want {
			continue
		}
		rel, ok, err := relationshipRow(store, id)
		if err != nil {
			return fmt.Errorf("graph: get relationships by IDs %d: %w", id, err)
		}
		if !ok {
			continue
		}
		// The row fans out to every duplicate slot of the caller's result —
		// freeze it so the aliased rows cannot corrupt one another.
		rel.Freeze()
		found[id] = rel
		delete(pending, id)
	}
	return nil
}

func collectPendingRelationshipsFromStore(store *BadgerStore, found map[types.RelID]*types.Relationship, pending map[types.RelID]struct{}) error {
	for id := range pending {
		rel, ok, err := relationshipRow(store, id)
		if err != nil {
			return fmt.Errorf("graph: get relationships by IDs %d: %w", id, err)
		}
		if !ok {
			continue
		}
		// See collectRelationshipsFromStore — frozen because rows fan out.
		rel.Freeze()
		found[id] = rel
		delete(pending, id)
	}
	return nil
}

type nodeShardIDBatch struct {
	ids      []types.NodeID
	checkout func() (*BadgerStore, func(), error)
}

func (ts *Store) groupNodeIDsByShard(ids []types.NodeID) ([]nodeShardIDBatch, error) {
	batches := make([]nodeShardIDBatch, 0, 2)
	byKey := make(map[any]int, 2)

	add := func(key any, checkout func() (*BadgerStore, func(), error), id types.NodeID) {
		if idx, ok := byKey[key]; ok {
			batches[idx].ids = append(batches[idx].ids, id)
			return
		}
		byKey[key] = len(batches)
		batches = append(batches, nodeShardIDBatch{
			ids:      []types.NodeID{id},
			checkout: checkout,
		})
	}

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	defer refCheckin()

	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, err
	}
	defer archiveCheckin()

	for _, id := range ids {
		raw := id.SnowflakeID()
		if ref.HasNodeID(raw) {
			add(ref, ts.checkoutRefShard, id)
			continue
		}
		if archive != nil && archive.HasNodeID(raw) {
			add(archive, ts.checkoutArchive, id)
			continue
		}

		es := ts.timestampToEventShardEntry(raw)
		add(es, func() (*BadgerStore, func(), error) {
			return es.checkoutStoreForRead(ts)
		}, id)
	}
	return batches, nil
}

// --- Adjacency queries ---

func (ts *Store) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	return ts.OutgoingRelationshipsByDepth(nid, typeToken, DepthAll)
}

// OutgoingRelationshipsByDepth implements store.AdjacencyDepthCapability:
// OutgoingRelationships with a reference-side node's mint-placed rels
// (Config.RelsByMint) read from the event shards depth selects.
func (ts *Store) OutgoingRelationshipsByDepth(nid types.NodeID, typeToken uint16, depth ShardDepth) ([]*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	if err := validateDepth(depth); err != nil {
		return nil, err
	}
	// Entity + out/ are co-located in the node's shard. Use the checked
	// resolver so a cold owner stays pinned for the duration of the read.
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	if !shard.HasNodeID(nid.SnowflakeID()) {
		return nil, ErrNodeNotFound
	}
	rels, err := shard.OutgoingRelationships(nid, typeToken)
	if err != nil || !ts.mintFanOut(shard) {
		return rels, err
	}
	extra, err := ts.mintRels([]types.NodeID{nid}, typeToken, false, depth)
	if err != nil {
		return nil, err
	}
	return unionRels(rels, extra[nid]), nil
}

// OutgoingRelationshipsForNodes batches outgoing queries across shards; each
// owner shard is pinned once so cold shards cannot close mid-query.
func (ts *Store) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	return ts.OutgoingRelationshipsForNodesByDepth(nodeIDs, typeToken, DepthAll)
}

// OutgoingRelationshipsForNodesByDepth is OutgoingRelationshipsForNodes with
// reference-side nodes' mint-placed rels read from the event shards depth
// selects (OutgoingRelationshipsByDepth).
func (ts *Store) OutgoingRelationshipsForNodesByDepth(nodeIDs []types.NodeID, typeToken uint16, depth ShardDepth) (map[types.NodeID][]*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	if err := validateDepth(depth); err != nil {
		return nil, err
	}

	queryNodeIDs := nodeIDs
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	if unique, ok := uniqueNodeIDsPreserveOrderIfDuplicate(nodeIDs); ok {
		queryNodeIDs = unique
	}

	batches, err := ts.groupNodeIDsByShard(queryNodeIDs)
	if err != nil {
		return nil, err
	}

	result := make(map[types.NodeID][]*types.Relationship, len(queryNodeIDs))
	var fan []types.NodeID // reference-side nodes: their mint-placed rels join below
	for _, batch := range batches {
		store, release, err := batch.checkout()
		if err != nil {
			return nil, err
		}
		m, err := store.OutgoingRelationshipsForNodes(batch.ids, typeToken)
		if err == nil && ts.mintFanOut(store) {
			fan = append(fan, batch.ids...)
		}
		release()
		if err != nil {
			return nil, err
		}
		for nid, rels := range m {
			result[nid] = rels
		}
	}
	result, err = ts.withMintRels(result, fan, typeToken, false, depth)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// OutgoingDegree counts a node's outgoing relationships (type-filtered) from
// the owning shard's adjacency index without resolving entities. See
// store.DegreeCapability.
func (ts *Store) OutgoingDegree(nid types.NodeID, typeToken uint16) (int, error) {
	return ts.adjacencyDegree(nid, typeToken, false, DepthAll)
}

// OutgoingDegreeByDepth is OutgoingDegree with a reference-side node's
// mint-placed legs counted on the event shards depth selects.
func (ts *Store) OutgoingDegreeByDepth(nid types.NodeID, typeToken uint16, depth ShardDepth) (int, error) {
	return ts.adjacencyDegree(nid, typeToken, false, depth)
}

// IncomingDegreeByDepth is IncomingDegree with a reference-side node's
// mint-placed legs counted on the event shards depth selects.
func (ts *Store) IncomingDegreeByDepth(nid types.NodeID, typeToken uint16, depth ShardDepth) (int, error) {
	return ts.adjacencyDegree(nid, typeToken, true, depth)
}

// adjacencyDegree is the body of the degree doors: the owning shard's index
// count plus, for a reference-side node, its mint-placed legs.
func (ts *Store) adjacencyDegree(nid types.NodeID, typeToken uint16, incoming bool, depth ShardDepth) (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, err
	}
	if err := validateDepth(depth); err != nil {
		return 0, err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return 0, err
	}
	defer checkin()
	live, liveErr := nodeRowLive(shard, nid)
	if liveErr != nil {
		return 0, liveErr
	}
	if !live {
		return 0, ErrNodeNotFound
	}
	degree := shard.OutgoingDegree
	if incoming {
		degree = shard.IncomingDegree
	}
	n, err := degree(nid, typeToken)
	if err != nil || !ts.mintFanOut(shard) {
		return n, err
	}
	m, err := ts.mintDegree(nid, typeToken, incoming, depth)
	return n + m, err
}

// IncomingDegree counts a node's incoming relationships (type-filtered) from
// the owning shard's co-located adjacency index (single-shard lookup), plus a
// reference-side node's mint-placed legs. See store.DegreeCapability.
func (ts *Store) IncomingDegree(nid types.NodeID, typeToken uint16) (int, error) {
	return ts.adjacencyDegree(nid, typeToken, true, DepthAll)
}

func (ts *Store) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	return ts.IncomingRelationshipsByDepth(nid, typeToken, DepthAll)
}

// IncomingRelationshipsByDepth implements store.AdjacencyDepthCapability:
// IncomingRelationships with a reference-side node's mint-placed rels read
// from the event shards depth selects.
func (ts *Store) IncomingRelationshipsByDepth(nid types.NodeID, typeToken uint16, depth ShardDepth) ([]*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	if err := validateDepth(depth); err != nil {
		return nil, err
	}
	// Get relIDs from the node's shard inIdx. The owner is checked out so a
	// cold node shard cannot be closed mid-read.
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	live, liveErr := nodeRowLive(shard, nid)
	if liveErr != nil {
		checkin()
		return nil, liveErr
	}
	if !live {
		checkin()
		return nil, ErrNodeNotFound
	}
	relIDs := shard.IncomingRelIDs(nid.SnowflakeID(), typeToken)
	var mint []*types.Relationship
	if ts.mintFanOut(shard) {
		extra, err := ts.mintRels([]types.NodeID{nid}, typeToken, true, depth)
		if err != nil {
			checkin()
			return nil, err
		}
		mint = extra[nid]
	}
	checkin()

	if len(relIDs) == 0 {
		return unionRels(nil, mint), nil
	}

	// Fetch rel entities via the batched resolver so rels on shards aged to cold
	// stay reachable without the full probe sequence per relID.
	result := make([]*types.Relationship, 0, len(relIDs))
	typedRelIDs := make([]types.RelID, 0, len(relIDs))
	for _, relID := range relIDs {
		typedRelIDs = append(typedRelIDs, types.RelID(relID))
	}
	found, err := ts.getUniqueRelationshipsByIDs(typedRelIDs)
	if err != nil {
		return nil, err
	}
	for _, relID := range typedRelIDs {
		if r, ok := found[relID]; ok && relationshipMatchesIncoming(r, nid, typeToken) {
			result = append(result, r)
		}
	}
	if len(result) == 0 {
		return unionRels(nil, mint), nil
	}

	storepkg.SortRelsByID(result)
	return unionRels(result, mint), nil
}

// IncomingRelationshipsForNodes batches incoming queries. relIDs come from the
// node's shard inIdx; entities are resolved cross-shard because the owner may
// be an older start-node shard, not the relID timestamp shard.
func (ts *Store) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	return ts.IncomingRelationshipsForNodesByDepth(typedNodeIDs, typeToken, DepthAll)
}

// IncomingRelationshipsForNodesByDepth is IncomingRelationshipsForNodes with
// reference-side nodes' mint-placed rels read from the event shards depth
// selects (IncomingRelationshipsByDepth).
func (ts *Store) IncomingRelationshipsForNodesByDepth(typedNodeIDs []types.NodeID, typeToken uint16, depth ShardDepth) (map[types.NodeID][]*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range typedNodeIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	if err := validateDepth(depth); err != nil {
		return nil, err
	}

	// Phase 1: collect relIDs per node from each node's shard inIdx.
	type relRef struct {
		nodeID types.NodeID
		relID  types.RelID
	}
	var refs []relRef
	var fan []types.NodeID // reference-side nodes: their mint-placed rels join below
	seen := make(map[types.NodeID]struct{}, len(typedNodeIDs))
	uniqueNodeIDs := make([]types.NodeID, 0, len(typedNodeIDs))

	for _, tnid := range typedNodeIDs {
		if _, dup := seen[tnid]; dup {
			continue
		}
		seen[tnid] = struct{}{}
		uniqueNodeIDs = append(uniqueNodeIDs, tnid)
	}

	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	batches, err := ts.groupNodeIDsByShard(uniqueNodeIDs)
	if err != nil {
		return nil, err
	}

	for _, batch := range batches {
		store, release, err := batch.checkout()
		if err != nil {
			return nil, err
		}
		for _, tnid := range batch.ids {
			live, liveErr := nodeRowLive(store, tnid)
			if liveErr != nil {
				release()
				return nil, liveErr
			}
			if !live {
				release()
				return nil, ErrNodeNotFound
			}
			relIDs := store.IncomingRelIDs(tnid.SnowflakeID(), typeToken)
			for _, rid := range relIDs {
				refs = append(refs, relRef{nodeID: tnid, relID: types.RelID(rid)})
			}
		}
		if ts.mintFanOut(store) {
			fan = append(fan, batch.ids...)
		}
		release()
	}

	if len(refs) == 0 {
		mint, err := ts.withMintRels(nil, fan, typeToken, true, depth)
		if err != nil || len(mint) == 0 {
			return nil, err
		}
		return mint, nil
	}

	// Phase 2: fetch rel entities via the batched resolver (see above).
	result := make(map[types.NodeID][]*types.Relationship, len(seen))
	relIDs := make([]types.RelID, 0, len(refs))
	for _, ref := range refs {
		relIDs = append(relIDs, ref.relID)
	}
	found, err := ts.getUniqueRelationshipsByIDs(relIDs)
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if r, ok := found[ref.relID]; ok && relationshipMatchesIncoming(r, ref.nodeID, typeToken) {
			result[ref.nodeID] = append(result[ref.nodeID], r)
		}
	}

	// Sort per-node slices for deterministic output.
	for nid := range result {
		storepkg.SortRelsByID(result[nid])
	}
	result, err = ts.withMintRels(result, fan, typeToken, true, depth)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func relationshipMatchesIncoming(r *types.Relationship, nid types.NodeID, typeToken uint16) bool {
	if r == nil || r.EndNodeID() != nid {
		return false
	}
	return typeToken == 0 || r.HasTypeTokenRaw(typeToken)
}
