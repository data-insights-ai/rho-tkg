package sharded

import (
	"fmt"
	"sort"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Batch mutations (ADR-0007 S2) ---
//
// SEMANTICS (documented + tested): a sharded batch is atomic PER SHARD GROUP,
// NOT across shards — there is no cross-shard WriteBatch. To keep partial
// failure impossible for anything BUT a genuine mid-sequence I/O error, every
// batch door VALIDATES THE WHOLE INPUT FIRST (all rows structurally valid, all
// slots local, no duplicate IDs anywhere, node creates not already present, rel
// creates' endpoints live, node deletes unconnected across all shards) BEFORE it
// touches any shard. It then applies the per-shard groups in ASCENDING
// shard-index order. If a shard door still fails (only an I/O error survives the
// pre-validation), the batch returns a typed *PartialBatchError naming exactly
// which shard groups committed and which one failed — it FAILS LOUDLY and NEVER
// silently rolls back a cross-shard partial (a rollback delete can itself fail,
// and pretending atomicity we do not have is worse than reporting the truth;
// the verify door diagnoses the residue).

// PartialBatchError reports a cross-shard batch that could not be applied
// atomically: some shard groups committed, then a later shard's door returned an
// I/O error. It is the fail-loud signal the ADR promises — never a silent
// partial. CommittedShards are the shard indices whose group fully committed
// (ascending); FailedShard is the shard whose door returned Err (its group may
// be partially applied).
type PartialBatchError struct {
	Op              string // "PutNodesBatch" / "PutRelationshipsBatch" / ...
	CommittedShards []int
	FailedShard     int
	Err             error
}

func (e *PartialBatchError) Error() string {
	return fmt.Sprintf("graph: sharded: %s partially committed: shards %v committed, shard %d failed: %v",
		e.Op, e.CommittedShards, e.FailedShard, e.Err)
}

func (e *PartialBatchError) Unwrap() error { return e.Err }

// ascendingKeys returns the map keys sorted ascending — the deterministic
// per-shard-group commit order.
func ascendingKeys[V any](m map[int]V) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// PutNodesBatch partitions nodes by shard and applies one batch per shard in
// ascending shard order. See the package-level SEMANTICS note: validate all
// first (structure, slot-locality, no duplicate IDs, none already present), then
// commit ascending; a surviving I/O error returns *PartialBatchError.
func (s *Store) PutNodesBatch(nodes []*types.Node) error {
	return s.putNodesBatchInternal(nodes, nil, nil, false)
}

// PutNodesBatchPreEncoded satisfies store.PreEncodedPutCapability (ADR-0006 §4.5).
// wireBodies[i] is the producer-thread pre-encoded, applier-patched v2 entity-row
// for nodes[i] (nil ⇒ re-encode row i). The parallel wireBodies array is sliced
// per shard group WITH INDEX ALIGNMENT PRESERVED — wireBodiesSub[j] always
// travels with nodesSub[j] — so a row's pre-encoded bytes reach the same shard as
// the row (an off-by-one here is the silent-wrong-answer class, tested for
// byte-identity per shard against unsharded badger).
func (s *Store) PutNodesBatchPreEncoded(nodes []*types.Node, wireBodies [][]byte) error {
	return s.putNodesBatchInternal(nodes, wireBodies, nil, false)
}

// PutNodesBatchOwnedPreEncoded satisfies store.OwnedPreEncodedPutCapability: the
// ownership-transfer variant used by the ingest bulk apply path. It partitions
// per shard exactly like PutNodesBatchPreEncodedLog, then hands each shard group
// to the shard's OWN owned door so the shard freezes the nodes in place instead
// of deep-copying them. Same ownership contract as the badger door: the caller
// must never touch a node again.
func (s *Store) PutNodesBatchOwnedPreEncoded(nodes []*types.Node, wireBodies, logBodies [][]byte) error {
	return s.putNodesBatchInternal(nodes, wireBodies, logBodies, true)
}

// PutNodesBatchPreEncodedLog satisfies store.PreEncodedPutLogCapability: like
// PutNodesBatchPreEncoded, plus logBodies[i] as node i's pre-encoded ChangeNodePut
// payload. Both parallel arrays are sliced per shard group with the SAME index
// alignment as the nodes.
func (s *Store) PutNodesBatchPreEncodedLog(nodes []*types.Node, wireBodies, logBodies [][]byte) error {
	return s.putNodesBatchInternal(nodes, wireBodies, logBodies, false)
}

// putNodesBatchInternal is the shared body of the three node-batch doors. When
// wireBodies/logBodies are non-nil, they are the pre-encoded parallel arrays
// (ADR-0006 §4.5) and are partitioned per shard group in index alignment with
// their nodes; when nil the plain PutNodesBatch path runs on every shard.
func (s *Store) putNodesBatchInternal(nodes []*types.Node, wireBodies, logBodies [][]byte, owned bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
	preEncoded := wireBodies != nil || logBodies != nil

	// Phase 1 — validate the WHOLE input before any shard is touched. Partition
	// nodes (and their aligned pre-encoded buffers) into per-shard buckets.
	type nodeBucket struct {
		nodes []*types.Node
		wire  [][]byte
		log   [][]byte
	}
	buckets := make(map[int]*nodeBucket)
	seen := make(map[types.NodeID]struct{}, len(nodes))
	for i, n := range nodes {
		if err := storecontract.ValidateNodeWrite(n); err != nil {
			return err
		}
		nid := n.InternalID()
		if _, dup := seen[nid]; dup {
			return fmt.Errorf("graph: sharded: duplicate node ID %d in batch", nid.SnowflakeID())
		}
		seen[nid] = struct{}{}
		idx, err := s.shardIndexForNode(nid)
		if err != nil {
			return err
		}
		if s.shards[idx].HasNodeID(nid.SnowflakeID()) {
			return storecontract.ErrNodeExists
		}
		b := buckets[idx]
		if b == nil {
			b = &nodeBucket{}
			if preEncoded {
				b.wire = [][]byte{}
				b.log = [][]byte{}
			}
			buckets[idx] = b
		}
		b.nodes = append(b.nodes, n)
		if preEncoded {
			b.wire = append(b.wire, elemAt(wireBodies, i))
			b.log = append(b.log, elemAt(logBodies, i))
		}
	}

	// Phase 2 — apply per shard group in ascending order.
	var committed []int
	for _, idx := range ascendingKeys(buckets) {
		b := buckets[idx]
		var err error
		switch {
		case owned:
			err = s.shards[idx].PutNodesBatchOwnedPreEncoded(b.nodes, b.wire, b.log)
		case logBodies != nil:
			err = s.shards[idx].PutNodesBatchPreEncodedLog(b.nodes, b.wire, b.log)
		case wireBodies != nil:
			err = s.shards[idx].PutNodesBatchPreEncoded(b.nodes, b.wire)
		default:
			err = s.shards[idx].PutNodesBatch(b.nodes)
		}
		if err != nil {
			return &PartialBatchError{Op: "PutNodesBatch", CommittedShards: committed, FailedShard: idx, Err: err}
		}
		committed = append(committed, idx)
	}
	return nil
}

// elemAt returns arr[i] or nil when i is out of range (a shorter parallel array
// means "re-encode this row" for every trailing element).
func elemAt(arr [][]byte, i int) []byte {
	if i < len(arr) {
		return arr[i]
	}
	return nil
}

// PutRelationshipsBatch validates every relationship up front (structure, no
// duplicate IDs, slots local, not already present, BOTH endpoints live —
// endpoints may be cross-shard) and then writes each relationship on its own
// shard, one shard group at a time in ascending shard order. Each relationship
// row plus both adjacency legs co-commit on its shard, so a group is atomic per
// relationship; a surviving I/O error returns *PartialBatchError.
func (s *Store) PutRelationshipsBatch(rels []*types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(rels) == 0 {
		return nil
	}
	buckets := make(map[int][]*types.Relationship)
	seen := make(map[types.RelID]struct{}, len(rels))
	for _, r := range rels {
		if err := storecontract.ValidateRelationshipWrite(r); err != nil {
			return err
		}
		if _, dup := seen[r.ID()]; dup {
			return ErrRelExists
		}
		seen[r.ID()] = struct{}{}
		idx, err := s.shardIndexForRel(r.InternalID())
		if err != nil {
			return err
		}
		if s.shards[idx].HasRelID(r.ID().SnowflakeID()) {
			return ErrRelExists
		}
		if err := s.requireNodeLive(r.StartNodeID()); err != nil {
			return err
		}
		if err := s.requireNodeLive(r.EndNodeID()); err != nil {
			return err
		}
		buckets[idx] = append(buckets[idx], r)
	}

	var committed []int
	for _, idx := range ascendingKeys(buckets) {
		for _, r := range buckets[idx] {
			if err := s.putRelationshipToShard(idx, r); err != nil {
				return &PartialBatchError{Op: "PutRelationshipsBatch", CommittedShards: committed, FailedShard: idx, Err: err}
			}
		}
		committed = append(committed, idx)
	}
	return nil
}

// putRelationshipToShard writes a fully pre-validated relationship (row + both
// adjacency legs) to the given shard index via the co-located door — one atomic
// WriteBatch with a co-committed ChangeRelPut record. The batch already validated
// endpoints (cross-shard) and non-existence, so the door's own checks are redundant
// but harmless.
func (s *Store) putRelationshipToShard(idx int, r *types.Relationship) error {
	return s.shards[idx].PutRelationshipCoLocated(r)
}

// DeleteNodesBatch removes unconnected node rows. It validates the whole input
// first — every ID structurally valid, slot local, present, and unconnected
// across ALL shards — then deletes per shard group in ascending order. A
// surviving I/O error returns *PartialBatchError.
func (s *Store) DeleteNodesBatch(ids []types.NodeID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	buckets := make(map[int][]types.NodeID)
	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
		if _, dup := seen[id]; dup {
			continue // coalesce duplicate requested deletes
		}
		seen[id] = struct{}{}
		idx, err := s.shardIndexForNode(id)
		if err != nil {
			return err
		}
		if !s.shards[idx].HasNodeID(id.SnowflakeID()) {
			return ErrNodeNotFound
		}
		connected, cerr := s.nodeConnectedAnyShard(id)
		if cerr != nil {
			return cerr
		}
		if connected {
			return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, id)
		}
		buckets[idx] = append(buckets[idx], id)
	}
	var committed []int
	for _, idx := range ascendingKeys(buckets) {
		if err := s.shards[idx].DeleteNodesBatch(buckets[idx]); err != nil {
			return &PartialBatchError{Op: "DeleteNodesBatch", CommittedShards: committed, FailedShard: idx, Err: err}
		}
		committed = append(committed, idx)
	}
	return nil
}

// DeleteRelationshipsBatch partitions rel IDs by their (co-located) shard,
// validates the whole input first (structure, slot local, present), and deletes
// per shard group in ascending order. A surviving I/O error returns
// *PartialBatchError.
func (s *Store) DeleteRelationshipsBatch(ids []types.RelID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	buckets := make(map[int][]types.RelID)
	seen := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		idx, err := s.shardIndexForRel(id)
		if err != nil {
			return err
		}
		if !s.shards[idx].HasRelID(id.SnowflakeID()) {
			return ErrRelNotFound
		}
		buckets[idx] = append(buckets[idx], id)
	}
	var committed []int
	for _, idx := range ascendingKeys(buckets) {
		if err := s.shards[idx].DeleteRelationshipsBatch(buckets[idx]); err != nil {
			return &PartialBatchError{Op: "DeleteRelationshipsBatch", CommittedShards: committed, FailedShard: idx, Err: err}
		}
		committed = append(committed, idx)
	}
	return nil
}

// Compile-time assertions: sharded.Store satisfies the pre-encoded put
// capabilities (ADR-0006 §4.5). Wired for direct/S4 use; core's current router
// (nativePreEncodedPut) keeps them badger-only, so a sharded deployment falls
// back to encode-at-flush PutNodesBatch until S4 lane wiring routes them.
var (
	_ storecontract.PreEncodedPutCapability      = (*Store)(nil)
	_ storecontract.PreEncodedPutLogCapability   = (*Store)(nil)
	_ storecontract.OwnedPreEncodedPutCapability = (*Store)(nil)
)
