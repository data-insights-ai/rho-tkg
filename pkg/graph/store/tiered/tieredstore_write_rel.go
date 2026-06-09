package tiered

import (
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Relationship write operations ---
// Relationships may be cross-shard when start and end nodes live in different shards.
// After rotation, two event entities may live in different shards (warm vs hot).
// We use shard-based routing (shardForNodeIDChecked) instead of class-based routing
// to correctly handle E→E cross-shard relationships, and pin cold owners for the
// duration of the write.

func (ts *Store) relIDExists(id types.RelID) (bool, error) {
	shard, checkin, err := ts.shardForRelIDChecked(id)
	if err != nil {
		return false, err
	}
	defer checkin()
	return relationshipRowExists(shard, id), nil
}

func (ts *Store) PutRelationship(r *types.Relationship) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	if err := ts.checkRotation(); err != nil {
		return err
	}
	ts.relCreateMu.Lock()
	defer ts.relCreateMu.Unlock()

	return ts.putRelationshipLocked(r, true)
}

func (ts *Store) PutRelationshipGeneratedID(r *types.Relationship, proof generatedcreate.Proof) error {
	if !proof.Valid() {
		return ts.PutRelationship(r)
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	if err := ts.checkRotation(); err != nil {
		return err
	}
	ts.relCreateMu.Lock()
	defer ts.relCreateMu.Unlock()

	return ts.putRelationshipLocked(r, false)
}

func (ts *Store) PutRelationshipGeneratedIDWithEndpointHashes(r *types.Relationship, proof generatedcreate.Proof) (string, string, error) {
	if !proof.Valid() {
		err := ts.PutRelationship(r)
		if err != nil {
			return "", "", err
		}
		if ig := r.Integrity(); ig != nil {
			return ig.FromNodeHash, ig.ToNodeHash, nil
		}
		return "", "", nil
	}
	if err := ts.checkOpen(); err != nil {
		return "", "", err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return "", "", err
	}
	if err := ts.checkRotation(); err != nil {
		return "", "", err
	}
	ts.relCreateMu.Lock()
	defer ts.relCreateMu.Unlock()

	return ts.putRelationshipWithEndpointHashesLocked(r, false)
}

func (ts *Store) putRelationshipLocked(r *types.Relationship, checkDuplicate bool) error {
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
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

	// Verify endpoints exist before duplicate checks, matching the
	// single-shard store contract.
	entityShard := startShard // entity + out/
	inShard := endShard       // in/

	startLive, err := nodeRowLive(entityShard, r.StartNodeID())
	if err != nil {
		return err
	}
	if !startLive {
		return ErrNodeNotFound
	}
	endLive, err := nodeRowLive(inShard, r.EndNodeID())
	if err != nil {
		return err
	}
	if !endLive {
		return ErrNodeNotFound
	}
	if checkDuplicate {
		exists, err := ts.relIDExists(r.ID())
		if err != nil {
			return err
		}
		if exists {
			return ErrRelExists
		}
	}

	if startShard == endShard {
		// Same shard: delegate entirely, including relationships whose
		// endpoints both live on refArchive.
		return startShard.PutRelationship(r)
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

func (ts *Store) putRelationshipWithEndpointHashesLocked(r *types.Relationship, checkDuplicate bool) (string, string, error) {
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return "", "", err
	}
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()
	relID := r.ID().SnowflakeID()

	startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
	if err != nil {
		return "", "", err
	}
	defer startCheckin()
	endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
	if err != nil {
		return "", "", err
	}
	defer endCheckin()

	entityShard := startShard
	inShard := endShard

	fromHash, err := entityShard.NodeIntegrityHash(r.StartNodeID())
	if err != nil {
		return "", "", err
	}
	toHash := fromHash
	if r.StartNodeID() != r.EndNodeID() {
		toHash, err = inShard.NodeIntegrityHash(r.EndNodeID())
		if err != nil {
			return "", "", err
		}
	}
	if checkDuplicate {
		exists, err := ts.relIDExists(r.ID())
		if err != nil {
			return "", "", err
		}
		if exists {
			return "", "", ErrRelExists
		}
	}

	ig := r.Integrity()
	if ig == nil {
		ig = &types.RelIntegrity{}
		r.SetIntegrity(ig)
	}
	prevFrom, prevTo := ig.FromNodeHash, ig.ToNodeHash
	ig.FromNodeHash = fromHash
	ig.ToNodeHash = toHash
	restoreHashes := func(err error) (string, string, error) {
		ig.FromNodeHash = prevFrom
		ig.ToNodeHash = prevTo
		return "", "", err
	}

	if startShard == endShard {
		if err := startShard.PutRelationship(r); err != nil {
			return restoreHashes(err)
		}
		return fromHash, toHash, nil
	}

	if endShard == ts.refShard {
		if err := inShard.PutRelIncoming(endID, startID, relType, relID); err != nil {
			return restoreHashes(err)
		}
		if err := entityShard.PutRelEntityAndOut(r); err != nil {
			info := RelDeleteInfo{ID: relID, RelType: relType, StartID: startID, EndID: endID}
			if rbErr := inShard.DeleteRelIncoming(info); rbErr != nil {
				return restoreHashes(fmt.Errorf("tiered: put cross-shard relationship entity failed after in/ write: %w (rollback in/ failed: %v)", err, rbErr))
			}
			return restoreHashes(err)
		}
		return fromHash, toHash, nil
	}
	if err := entityShard.PutRelEntityAndOut(r); err != nil {
		return restoreHashes(err)
	}
	if err := inShard.PutRelIncoming(endID, startID, relType, relID); err != nil {
		if _, rbErr := entityShard.DeleteRelEntityAndOut(relID); rbErr != nil {
			return restoreHashes(fmt.Errorf("tiered: put cross-shard relationship in/ failed after entity write: %w (rollback entity/out failed: %v)", err, rbErr))
		}
		return restoreHashes(err)
	}
	return fromHash, toHash, nil
}

func (ts *Store) ReplaceRelationship(r *types.Relationship) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	shard, checkin, err := ts.shardForRelIDChecked(r.ID())
	if err != nil {
		return err
	}
	defer checkin()
	return shard.ReplaceRelationship(r)
}

func (ts *Store) DeleteRelationship(rid types.RelID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
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
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if len(rels) == 0 {
		return nil
	}
	if err := ts.checkRotation(); err != nil {
		return err
	}
	ts.relCreateMu.Lock()
	defer ts.relCreateMu.Unlock()

	seen := make(map[types.RelID]struct{}, len(rels))
	for _, r := range rels {
		if err := storecontract.ValidateRelationshipWrite(r); err != nil {
			return err
		}
		rid := r.ID()
		if _, ok := seen[rid]; ok {
			return ErrRelExists
		}
		seen[rid] = struct{}{}

		startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
		if err != nil {
			return err
		}
		startExists, startErr := nodeRowLive(startShard, r.StartNodeID())
		startCheckin()
		if startErr != nil {
			return startErr
		}
		if !startExists {
			return ErrNodeNotFound
		}

		endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
		if err != nil {
			return err
		}
		endExists, endErr := nodeRowLive(endShard, r.EndNodeID())
		endCheckin()
		if endErr != nil {
			return endErr
		}
		if !endExists {
			return ErrNodeNotFound
		}

		exists, err := ts.relIDExists(rid)
		if err != nil {
			return err
		}
		if exists {
			return ErrRelExists
		}
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
			// Cross-shard writes run after partitioning through
			// putRelationshipLocked. Release these classification pins now;
			// the split-write helper pins endpoints again for the actual write.
			startCheckin()
			endCheckin()
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

	var committedCross []types.RelID
	rollbackCross := func() error {
		var rollbackErrs []error
		for i := len(committedCross) - 1; i >= 0; i-- {
			if err := ts.DeleteRelationship(committedCross[i]); err != nil && !errors.Is(err, ErrRelNotFound) {
				rollbackErrs = append(rollbackErrs, err)
			}
		}
		return errors.Join(rollbackErrs...)
	}

	type committedBatch struct {
		shard *BadgerStore
		rels  []*types.Relationship
	}
	var committed []committedBatch
	rollbackBatches := func() error {
		var rollbackErrs []error
		for i := len(committed) - 1; i >= 0; i-- {
			cb := committed[i]
			for _, r := range cb.rels {
				if err := cb.shard.DeleteRelationship(r.ID()); err != nil && !errors.Is(err, ErrRelNotFound) {
					rollbackErrs = append(rollbackErrs, err)
				}
			}
		}
		return errors.Join(rollbackErrs...)
	}

	// Cross-shard rels: write through the same split-write helper used by
	// PutRelationship. The batch preflight above already checked duplicate IDs,
	// so the per-rel helper can skip duplicate probes and avoid deadlocking on
	// relCreateMu.
	for _, r := range rels {
		startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
		if err != nil {
			if rbErr := rollbackCross(); rbErr != nil {
				return fmt.Errorf("%w (cross-shard relationship rollback failed: %v)", err, rbErr)
			}
			return err
		}
		endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
		if err != nil {
			startCheckin()
			if rbErr := rollbackCross(); rbErr != nil {
				return fmt.Errorf("%w (cross-shard relationship rollback failed: %v)", err, rbErr)
			}
			return err
		}
		startCheckin()
		endCheckin()

		if startShard == endShard {
			continue
		}
		if err := ts.putRelationshipLocked(r, false); err != nil {
			if rbErr := rollbackCross(); rbErr != nil {
				return fmt.Errorf("%w (cross-shard relationship rollback failed: %v)", err, rbErr)
			}
			return err
		}
		committedCross = append(committedCross, r.ID())
	}

	for shard, bucket := range shardBuckets {
		if err := shard.PutRelationshipsBatch(bucket); err != nil {
			rbErr := errors.Join(rollbackBatches(), rollbackCross())
			if rbErr != nil {
				return fmt.Errorf("tiered: put rels batch: %w (rollback failed: %v)", err, rbErr)
			}
			return fmt.Errorf("tiered: put rels batch (prior shard writes rolled back): %w", err)
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
//   - Duplicate IDs are coalesced before shard routing, so counters and indexes
//     are updated once per relationship.
//   - Missing-ID and routing validation errors are detected before any delete.
//   - Backend I/O failures after one shard accepts a delete roll back prior
//     accepted relationship deletes before returning.
//
// Throughput: same-shard rels collapse from N shard lookups + N WriteBatches
// down to one shard lookup per rel + one WriteBatch per shard, mirroring the
// PutRelationshipsBatch optimisation.
func (ts *Store) DeleteRelationshipsBatch(ids []types.RelID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	for _, id := range ids {
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
	}
	ids = uniqueRelIDs(ids)

	// Group same-shard rel IDs by their owning *BadgerStore. Owner shards are
	// pinned via checkout so cold owners cannot be torn down between resolution
	// here and the per-shard batch delete below. Cross-shard rels are released
	// immediately and replayed via the per-ID path which re-pins internally.
	sameShard := make(map[*BadgerStore][]types.RelID)
	var crossShard []types.RelID
	relByID := make(map[types.RelID]*types.Relationship, len(ids))
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
		relByID[id] = r

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

	ts.relCreateMu.Lock()
	defer ts.relCreateMu.Unlock()

	committed := make([]*types.Relationship, 0, len(ids))
	rollback := func(err error) error {
		if rbErr := ts.rollbackDeletedRelationshipsBatchLocked(committed); rbErr != nil {
			return fmt.Errorf("%w (relationship batch rollback failed: %v)", err, rbErr)
		}
		return fmt.Errorf("%w (prior relationship deletes rolled back)", err)
	}

	// Per-shard batch delete for same-shard rels.
	for shard, bucket := range sameShard {
		if err := shard.DeleteRelationshipsBatch(bucket); err != nil {
			if !errors.Is(err, ErrRelNotFound) {
				committed = appendMissingRelationships(committed, bucket, relByID, shard)
			}
			return rollback(fmt.Errorf("tiered: delete same-shard rels batch: %w", err))
		}
		committed = appendRelationshipsForIDs(committed, bucket, relByID)
	}

	// Cross-shard rels: per-ID via existing split-delete path.
	for _, id := range crossShard {
		if err := ts.DeleteRelationship(id); err != nil {
			return rollback(err)
		}
		committed = append(committed, relByID[id])
	}
	return nil
}

func appendRelationshipsForIDs(dst []*types.Relationship, ids []types.RelID, relByID map[types.RelID]*types.Relationship) []*types.Relationship {
	for _, id := range ids {
		if r := relByID[id]; r != nil {
			dst = append(dst, r)
		}
	}
	return dst
}

func appendMissingRelationships(dst []*types.Relationship, ids []types.RelID, relByID map[types.RelID]*types.Relationship, shard *BadgerStore) []*types.Relationship {
	for _, id := range ids {
		if relationshipRowExists(shard, id) {
			continue
		}
		if r := relByID[id]; r != nil {
			dst = append(dst, r)
		}
	}
	return dst
}

func (ts *Store) rollbackDeletedRelationshipsBatchLocked(rels []*types.Relationship) error {
	var rollbackErrs []error
	for i := len(rels) - 1; i >= 0; i-- {
		if err := ts.putRelationshipLocked(rels[i], false); err != nil {
			rollbackErrs = append(rollbackErrs, fmt.Errorf("restore relationship %d: %w", rels[i].ID(), err))
		}
	}
	return errors.Join(rollbackErrs...)
}

func uniqueRelIDs(ids []types.RelID) []types.RelID {
	seen := make(map[types.RelID]struct{}, len(ids))
	out := make([]types.RelID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
