package sharded

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Relationship CRUD ---
//
// ADR-0007 v1: a relationship row AND both its adjacency index entries (out +
// in) live on the REL ID's shard. Endpoint existence is validated on the
// endpoint's OWN shard (which may differ from the rel's shard for foreign-ID
// puts). Adjacency reads fold across all shards' local indexes.

func (s *Store) PutRelationship(r *types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	relShard, err := s.shardForRelID(r.InternalID())
	if err != nil {
		return err
	}
	// Validate endpoints on their own shards (cross-shard reads).
	if err := s.requireNodeLive(r.StartNodeID()); err != nil {
		return err
	}
	if err := s.requireNodeLive(r.EndNodeID()); err != nil {
		return err
	}
	if relShard.HasRelID(r.ID().SnowflakeID()) {
		return ErrRelExists
	}
	// The rel entity + BOTH adjacency legs live on the rel's shard (ADR-0007), so
	// this is a single-shard atomic write via the co-located door — one WriteBatch,
	// one co-committed ChangeRelPut record (endpoints already validated cross-shard
	// above). The record emission is why this must NOT use the record-free partial
	// doors (PutRelEntityAndOut/PutRelIncoming) — a sharded rel create would
	// otherwise be invisible to a tailing replica.
	return relShard.PutRelationshipCoLocated(r)
}

// PutRelationshipForeignEnd persists a generated-ID relationship whose END node
// lives on a FOREIGN partition not present in this store (ADR-0010). Unlike
// PutRelationship it does NOT validate the end node's existence — that node is
// on another machine and the caller has attested it out-of-band. The START node
// IS validated locally; the rel + BOTH adjacency legs write on the rel's shard
// exactly as the co-located in-process path does (the local incoming leg is
// inert here — a query for the foreign end's incoming edges runs on the end's
// own machine, fed by the Model-A half-edge stub). Emits one co-committed
// ChangeRelPut so a replica of this machine reproduces the edge verbatim.
//
// Fails closed with ErrForeignEndpointLocal if the END slot is in fact local
// (that is a misuse of this door — the normal PutRelationship must be used so
// the local end is properly validated).
func (s *Store) PutRelationshipForeignEnd(r *types.Relationship, proof generatedcreate.Proof) error {
	if !proof.Valid() {
		// Foreign-endpoint creation is a graph-generated fresh-ID fast path only;
		// a caller outside pkg/graph cannot name Proof, so it cannot reach here.
		return storecontract.ErrInvalidStoreMutation
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	relShard, err := s.shardForRelID(r.InternalID())
	if err != nil {
		return err
	}
	// The START must be local and live; the END must be genuinely foreign.
	if err := s.requireNodeLive(r.StartNodeID()); err != nil {
		return err
	}
	if _, err := s.shardForNodeID(r.EndNodeID()); err == nil {
		return ErrForeignEndpointLocal
	} else if !errors.Is(err, ErrSlotNotLocal) {
		return err
	}
	if relShard.HasRelID(r.ID().SnowflakeID()) {
		return ErrRelExists
	}
	// Single-shard atomic co-located write (one WriteBatch, one co-committed
	// ChangeRelPut record) — see PutRelationship for why the record-free partial
	// doors must not be used.
	return relShard.PutRelationshipCoLocated(r)
}

// RecordForeignIncoming stores a cross-machine incoming half-edge STUB (ADR-0010
// Model A) on the END node's shard so IncomingRelationships(END) is locally
// complete on this (the end's) machine. It is the mirror of PutRelationshipForeignEnd,
// executed on the OTHER machine: here the END node is LOCAL and the START node is
// FOREIGN (the edge's authoritative row lives on the start's machine). The stub's
// rel-ID belongs to a foreign slot, so it is written co-located on the END's shard
// (reachable only via that shard's adjacency fold, never a slot-routed GetRelationship)
// and co-commits a ChangeForeignIncoming record so a replica routes apply by the
// END-node slot. Fails closed with ErrForeignEndpointLocal if the START is in fact
// local (then it is an ordinary local edge, not a cross-machine one).
func (s *Store) RecordForeignIncoming(r *types.Relationship, proof generatedcreate.Proof) error {
	if !proof.Valid() {
		return storecontract.ErrInvalidStoreMutation
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	// The END must be LOCAL and live (it hosts the stub); the START must be foreign.
	endShard, err := s.shardForNodeID(r.EndNodeID())
	if err != nil {
		return err // END slot not local → ErrSlotNotLocal (routed to the wrong machine)
	}
	if !endShard.HasNodeID(r.EndNodeID().SnowflakeID()) {
		return ErrNodeNotFound
	}
	if _, serr := s.shardForNodeID(r.StartNodeID()); serr == nil {
		return ErrForeignEndpointLocal // START local → not a cross-machine incoming edge
	} else if !errors.Is(serr, ErrSlotNotLocal) {
		return serr
	}
	if endShard.HasRelID(r.ID().SnowflakeID()) {
		return ErrRelExists // idempotency guard — apply tolerates this
	}
	return endShard.PutRelationshipForeignIncoming(r)
}

// DeleteForeignIncoming removes a Model-A incoming half-edge stub (ADR-0010 §3.3
// cascade). The stub is physically co-located on endID's shard even though its
// rel-ID slot is foreign, so the delete routes by endID (never the rel slot) and
// co-commits a ChangeForeignIncomingDelete record there. Idempotent: a stub that
// is already gone (torn cascade / replica re-apply) returns nil, not an error.
// Satisfies generatedcreate.ForeignIncomingRelCapability.
func (s *Store) DeleteForeignIncoming(relID types.RelID, endID types.NodeID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(relID); err != nil {
		return err
	}
	endShard, err := s.shardForNodeID(endID)
	if err != nil {
		return err // END slot not local → routed to the wrong machine
	}
	if derr := endShard.DeleteRelationshipForeignIncoming(relID); derr != nil && !isRelNotFound(derr) {
		return derr
	}
	return nil
}

func (s *Store) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return nil, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return nil, err
	}
	return shard.GetRelationship(id)
}

func (s *Store) ReplaceRelationship(r *types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	shard, err := s.shardForRelID(r.InternalID())
	if err != nil {
		return err
	}
	return shard.ReplaceRelationship(r)
}

// DeleteRelationship removes the rel row + both adjacency entries — all on the
// rel's shard, so this is a single-shard delegation.
func (s *Store) DeleteRelationship(id types.RelID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return err
	}
	return shard.DeleteRelationship(id)
}

// requireNodeLive returns ErrNodeNotFound if the node is absent from its shard.
func (s *Store) requireNodeLive(id types.NodeID) error {
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	if !shard.HasNodeID(id.SnowflakeID()) {
		return ErrNodeNotFound
	}
	return nil
}

// --- Adjacency (parallel folds over all shards) ---

func (s *Store) OutgoingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	if err := s.requireNodeLive(nodeID); err != nil {
		return nil, err
	}
	return s.foldAdjacency(nodeID, typeToken, true)
}

func (s *Store) IncomingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	if err := s.requireNodeLive(nodeID); err != nil {
		return nil, err
	}
	return s.foldAdjacency(nodeID, typeToken, false)
}

// foldAdjacency collects a node's outgoing (or incoming) relationships across
// every shard. Each shard holds the entity co-located with its adjacency entry,
// so per shard we resolve the adjacency rel IDs against that same shard.
//
// PERF CAVEAT (BACKLOG 20g): this ALWAYS fans out to every claimed shard
// (forEachShardErr below), never just nodeID's own shard or the shards of its
// actual neighbors — a full-fan-out cost proportional to Config.SlotCount for
// EVERY adjacency read, regardless of how localized the node's real
// connectivity is. This is architecturally required by the current sharding
// strategy, not a missed optimization: a relationship's entity AND BOTH its
// adjacency legs are co-located on the shard the REL's OWN ID routes to
// (ADR-0007), which has NO guaranteed relationship to either endpoint's home
// shard — nodeID's own outgoing/incoming adjacency entries could legitimately
// live on any of the store's shards, so there is no cheaper subset to query
// without changing the fundamental placement rule. A future re-architecture
// (e.g. co-locating a rel's adjacency legs with the START node's shard
// instead of the rel's own ID) could bound this to the neighbor shards
// actually involved, but that is a fundamental sharding-strategy change for
// this WIP backend, not a hardening-pass fix — deliberately not attempted
// here. Operators choosing SlotCount for an adjacency-traversal-heavy
// workload should weigh this cost explicitly.
func (s *Store) foldAdjacency(nodeID types.NodeID, typeToken uint16, outgoing bool) ([]*types.Relationship, error) {
	sid := nodeID.SnowflakeID()
	per := make([][]*types.Relationship, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		var ids []types.RelID
		if outgoing {
			for _, id := range shard.OutgoingRelIDs(sid) {
				ids = append(ids, types.RelID(id))
			}
		} else {
			for _, id := range shard.IncomingRelIDs(sid, typeToken) {
				ids = append(ids, types.RelID(id))
			}
		}
		if len(ids) == 0 {
			return nil
		}
		rels, gerr := shard.GetRelationshipsByIDs(ids)
		if gerr != nil {
			// Tolerate index orphans (an adjacency entry whose entity was
			// concurrently removed): skip missing rather than failing the fold.
			if errors.Is(gerr, ErrRelNotFound) {
				rels = resolveRelsTolerant(shard, ids)
			} else {
				return gerr
			}
		}
		var matched []*types.Relationship
		for _, rr := range rels {
			if relationshipMatches(rr, nodeID, typeToken, outgoing) {
				matched = append(matched, rr)
			}
		}
		per[idx] = matched
		return nil
	})
	if err != nil {
		return nil, err
	}
	merged := mergeSortRels(per)
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

// resolveRelsTolerant resolves rel IDs one at a time, skipping any that are
// missing (index orphan / tombstone).
func resolveRelsTolerant(shard *badgerShard, ids []types.RelID) []*types.Relationship {
	var out []*types.Relationship
	for _, id := range ids {
		r, err := shard.GetRelationship(id)
		if err != nil {
			continue
		}
		out = append(out, r)
	}
	return out
}

func relationshipMatches(r *types.Relationship, nodeID types.NodeID, typeToken uint16, outgoing bool) bool {
	if r == nil {
		return false
	}
	if outgoing {
		if r.StartNodeID() != nodeID {
			return false
		}
	} else {
		if r.EndNodeID() != nodeID {
			return false
		}
	}
	return typeToken == 0 || r.HasTypeTokenRaw(typeToken)
}

func (s *Store) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	return s.foldAdjacencyForNodes(nodeIDs, typeToken, true)
}

func (s *Store) IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	return s.foldAdjacencyForNodes(nodeIDs, typeToken, false)
}

func (s *Store) foldAdjacencyForNodes(nodeIDs []types.NodeID, typeToken uint16, outgoing bool) (map[types.NodeID][]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
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
	// All requested nodes must exist (all-or-error contract).
	seen := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		if _, done := seen[id]; done {
			continue
		}
		seen[id] = struct{}{}
		if err := s.requireNodeLive(id); err != nil {
			return nil, err
		}
	}
	result := make(map[types.NodeID][]*types.Relationship)
	for id := range seen {
		rels, err := s.foldAdjacency(id, typeToken, outgoing)
		if err != nil {
			return nil, err
		}
		if len(rels) > 0 {
			result[id] = rels
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}
