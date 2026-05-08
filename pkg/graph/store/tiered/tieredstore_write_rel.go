package tiered

import (
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Relationship write operations ---
// Relationships may be cross-shard when start and end nodes live in different shards.
// After rotation, two event entities may live in different shards (warm vs hot).
// We use shard-based routing (shardForNodeIDChecked) instead of class-based routing
// to correctly handle E→E cross-shard relationships, and pin cold owners for the
// duration of the write.

func (ts *Store) PutRelationship(r *types.Relationship) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}

	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()
	relID := r.ID().SnowflakeID()

	// Resolve actual shards — not class. Two event entities may be in different shards.
	// Use the checked variants so cold owner shards stay pinned during the
	// split-write below.
	startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
	if err != nil {
		return err
	}
	defer startCheckin()
	endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
	if err != nil {
		return err
	}
	defer endCheckin()

	if startShard == endShard {
		// Same shard: delegate entirely. Includes the both-on-archive
		// case (e.g., a self-loop on an archived node) which is the
		// only archive write the M2 invariant currently permits.
		return startShard.PutRelationship(r)
	}

	// Cross-shard with an archived endpoint is the runtime counterpart of
	// the M2 / ErrCrossShardArchiveRel invariant. Without this check
	// AddRelationship can sneak past the ArchiveNode pre-scan: after
	// archiving A, a fresh AddRelationship(A, B) where B lives on
	// refShard or an event shard would cross the archive boundary and
	// re-introduce the silent-loss surface RestoreNode would later hit.
	//
	// We probe via archive.HasNodeID rather than identity comparison
	// against the resolved shard pointers because HasNodeID is the
	// single-source-of-truth for archive residency — independent of any
	// momentary refArchive pointer state.
	//
	// Close-race note: when refArchive.Load() returns nil, ts.closed
	// is already true (Close stores nil under archiveMu only AFTER
	// setting closed). shardForNodeIDChecked / checkoutArchive return
	// ErrStoreClosed in that state, so we never reach this point with
	// a still-live archive whose Load() yields nil — the guard's
	// nil-skip branch is unreachable while the archive holds rels.
	if archive := ts.refArchive.Load(); archive != nil {
		startOnArchive := archive.HasNodeID(startID)
		endOnArchive := archive.HasNodeID(endID)
		if startOnArchive != endOnArchive {
			return fmt.Errorf("graph: cross-shard relationship endpoint is archived: %w", ErrCrossShardArchiveRel)
		}
	}

	// Cross-shard: verify endpoints exist.
	entityShard := startShard // entity + out/
	inShard := endShard       // in/

	if !entityShard.HasNodeID(startID) {
		return ErrNodeNotFound
	}
	if !inShard.HasNodeID(endID) {
		return ErrNodeNotFound
	}

	// Split-write ordering per spec §12.
	if endShard == ts.refShard {
		// E→R: reference shard (in/) first — critical path for SOC queries.
		if err := inShard.PutRelIncoming(endID, startID, relType, relID); err != nil {
			return err
		}
		if err := entityShard.PutRelEntityAndOut(r); err != nil {
			// Roll back the in/ write so the partial state isn't left visible.
			info := RelDeleteInfo{ID: relID, RelType: relType, StartID: startID, EndID: endID}
			if rbErr := inShard.DeleteRelIncoming(info); rbErr != nil {
				return fmt.Errorf("tiered: put cross-shard relationship entity failed after in/ write: %w (rollback in/ failed: %v)", err, rbErr)
			}
			return err
		}
		return nil
	}
	// R→E or E→E(cross-shard): entity shard first.
	if err := entityShard.PutRelEntityAndOut(r); err != nil {
		return err
	}
	if err := inShard.PutRelIncoming(endID, startID, relType, relID); err != nil {
		// Roll back the entity/out write so the partial state isn't left visible.
		if _, rbErr := entityShard.DeleteRelEntityAndOut(relID); rbErr != nil {
			return fmt.Errorf("tiered: put cross-shard relationship in/ failed after entity write: %w (rollback entity/out failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

func (ts *Store) ReplaceRelationship(r *types.Relationship) error {
	shard, checkin, err := ts.shardForRelIDChecked(r.ID())
	if err != nil {
		return err
	}
	defer checkin()
	return shard.ReplaceRelationship(r)
}

func (ts *Store) DeleteRelationship(rid types.RelID) error {
	id := rid.SnowflakeID()

	// Find which shard owns the entity. Use the checked variant so a cold
	// owner stays pinned until the writes below complete.
	entityShard, entityCheckin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return err
	}
	defer entityCheckin()

	// Check if this is a cross-shard relationship by reading the rel metadata.
	r, err := entityShard.GetRelationship(types.RelID(id))
	if err != nil {
		return err
	}

	startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
	if err != nil {
		return err
	}
	defer startCheckin()
	endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
	if err != nil {
		return err
	}
	defer endCheckin()

	if startShard == endShard {
		// Same shard: delegate entirely.
		return entityShard.DeleteRelationship(types.RelID(id))
	}

	// Cross-shard: delete entity+out from entity shard, in/ from in shard.
	// On failure of the second leg, restore the entity+out write so a re-read
	// still observes the rel rather than leaving a phantom in/ entry on the
	// end-node shard.
	info, err := entityShard.DeleteRelEntityAndOut(id)
	if err != nil {
		return err
	}

	inShard := endShard
	if err := inShard.DeleteRelIncoming(info); err != nil {
		if rbErr := entityShard.PutRelEntityAndOut(r); rbErr != nil {
			return fmt.Errorf("tiered: delete cross-shard relationship in/ failed after entity/out delete: %w (rollback entity/out failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

func (ts *Store) PutRelationshipsBatch(rels []*types.Relationship) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}

	// Partition: same-shard rels can use batch, cross-shard use individual put.
	// Group by *BadgerStore pointer for batching. Owner shards are checked
	// out so cold owners stay pinned for the per-shard batch write below.
	shardBuckets := make(map[*BadgerStore][]*types.Relationship)
	var checkins []func()
	releaseAll := func() {
		for _, fn := range checkins {
			fn()
		}
	}

	for _, r := range rels {
		startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
		if err != nil {
			releaseAll()
			return err
		}
		endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
		if err != nil {
			startCheckin()
			releaseAll()
			return err
		}

		if startShard != endShard {
			// Cross-shard: individual put — release these checkins immediately
			// since PutRelationship pins again internally. There is a brief
			// unpinned window between checkin here and PutRelationship's
			// internal re-pin, but closeIdleShards cannot fire inside it:
			// each owner shard's lastAccess was just bumped by the prior
			// checkout (line above), and IdleTimeout >= 5min, so the window
			// is bounded by IdleTimeout and the activeReqs gap is microseconds.
			startCheckin()
			endCheckin()
			if err := ts.PutRelationship(r); err != nil {
				releaseAll()
				return err
			}
			continue
		}

		// Same-shard: hold the start-shard pin (covers both endpoints) until
		// the per-shard batch write below. The endpoint pin is redundant but
		// cheap; release it now so we don't leak duplicate references.
		endCheckin()
		shardBuckets[startShard] = append(shardBuckets[startShard], r)
		checkins = append(checkins, startCheckin)
	}
	defer releaseAll()

	// Write per-shard buckets, tracking committed batches for best-effort rollback.
	// If a later shard fails, previously committed shards are rolled back to prevent
	// silent partial state (e.g., rels in refShard with no corresponding hot-shard rels).
	type committedBatch struct {
		shard *BadgerStore
		rels  []*types.Relationship
	}
	var committed []committedBatch

	for shard, bucket := range shardBuckets {
		if err := shard.PutRelationshipsBatch(bucket); err != nil {
			// Best-effort rollback: delete rels written to previously committed shards.
			for _, cb := range committed {
				for _, r := range cb.rels {
					_ = cb.shard.DeleteRelationship(r.ID())
				}
			}
			return fmt.Errorf("tiered: put rels batch (prior shard writes rolled back best-effort): %w", err)
		}
		committed = append(committed, committedBatch{shard: shard, rels: bucket})
	}
	return nil
}

// DeleteRelationshipsBatch partitions same-shard relationships into per-shard
// buckets and submits each bucket as a single BadgerStore.DeleteRelationshipsBatch
// call (mirroring the partitioning used by PutRelationshipsBatch). Cross-shard
// relationships continue down the per-ID DeleteRelationship path so the
// split-delete + rollback ordering remains intact.
//
// Behavioural compatibility with the previous per-ID loop:
//   - Empty / nil input is a no-op (returns nil).
//   - Failure semantics are unchanged: this is NOT atomic across shards. A
//     mid-batch failure leaves earlier per-shard batches and earlier cross-shard
//     deletes committed. Callers that need all-or-nothing semantics must wrap
//     the call in a higher-level transaction.
//
// Throughput: same-shard rels collapse from N shard lookups + N WriteBatches
// down to one shard lookup per rel + one WriteBatch per shard, mirroring the
// PutRelationshipsBatch optimisation.
func (ts *Store) DeleteRelationshipsBatch(ids []types.RelID) error {
	if len(ids) == 0 {
		return nil
	}

	// Group same-shard rel IDs by their owning *BadgerStore. Owner shards are
	// pinned via checkout so cold owners cannot be torn down between resolution
	// here and the per-shard batch delete below. Cross-shard rels are released
	// immediately and replayed via the per-ID path which re-pins internally.
	sameShard := make(map[*BadgerStore][]types.RelID)
	var crossShard []types.RelID
	var checkins []func()
	releaseAll := func() {
		for _, fn := range checkins {
			fn()
		}
	}

	for _, id := range ids {
		entityShard, entityCheckin, err := ts.shardForRelIDChecked(id)
		if err != nil {
			releaseAll()
			return err
		}

		// Read rel metadata to determine endpoint shards. If the entity is
		// missing, surface ErrRelNotFound — matches per-ID DeleteRelationship.
		r, err := entityShard.GetRelationship(id)
		if err != nil {
			entityCheckin()
			releaseAll()
			return err
		}

		startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
		if err != nil {
			entityCheckin()
			releaseAll()
			return err
		}
		endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
		if err != nil {
			entityCheckin()
			startCheckin()
			releaseAll()
			return err
		}

		if startShard == endShard && entityShard == startShard {
			// Same shard: hold the entity-shard pin until the per-shard batch
			// delete below. Endpoint pins are redundant once we know the rel
			// is fully co-located, so release them now.
			startCheckin()
			endCheckin()
			sameShard[entityShard] = append(sameShard[entityShard], id)
			checkins = append(checkins, entityCheckin)
			continue
		}

		// Cross-shard: release all pins now and let DeleteRelationship re-pin
		// internally. There is a brief unpinned window before the re-pin, but
		// closeIdleShards cannot fire inside it: each owner shard's lastAccess
		// was just bumped by checkout above, and IdleTimeout >= 5min.
		entityCheckin()
		startCheckin()
		endCheckin()
		crossShard = append(crossShard, id)
	}
	defer releaseAll()

	// Per-shard batch delete for same-shard rels.
	for shard, bucket := range sameShard {
		if err := shard.DeleteRelationshipsBatch(bucket); err != nil {
			return fmt.Errorf("tiered: delete same-shard rels batch: %w", err)
		}
	}

	// Cross-shard rels: per-ID via existing split-delete path.
	for _, id := range crossShard {
		if err := ts.DeleteRelationship(id); err != nil {
			return err
		}
	}
	return nil
}
