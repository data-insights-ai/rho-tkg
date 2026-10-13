package badger

import (
	"fmt"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelRetentionPurgeCapability = (*Store)(nil)
	_ storecontract.RelRangePurgeLogCapability  = (*Store)(nil)
)

// relHistoryIDPage is how many history IDs one page of the ended-candidate
// walk reads.
const relHistoryIDPage = 1024

// LogRelRangePurge appends ONE ChangeRelRangePurge record (backlog 21) to the
// change-log so a replica re-executes the relationship predicate. No-op when
// the log is disabled. See store.RelRangePurgeLogCapability.
func (bs *Store) LogRelRangePurge(typeToken uint16, before types.Instant, mode uint8) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if !bs.logEnabled.Load() {
		return nil
	}
	payload, err := bs.buildChangePayload(storepkg.RelRangePurgeBody{
		TypeToken: typeToken,
		Before:    int64(before),
		Mode:      mode,
	})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	bs.idxMu.Lock()
	bs.logChangeRaw(storecontract.ChangeRelRangePurge, payload)
	bs.idxMu.Unlock()
	return bs.flush()
}

// relPurgeVictim is one relationship a chunk removes, with the endpoints of
// every row read before the write lock (for the ex-adjacency legs and the
// partitioned stores' residue sweep).
type relPurgeVictim struct {
	id   types.RelID
	legs []storecontract.PurgedRel
}

// PurgeRelsByTypeBefore hard-removes up to `chunk` relationships of typeToken
// minted before `before` (backlog 21, store.RelRetentionPurgeCapability):
// current rows of the type first (ascending ID), then IDs with history rows of
// the type and no current row (an ascending walk of the history keyspace that
// stops at the first ID minted at or after `before`). Phase A selects and
// reads the victims' rows outside the write lock; Phase B removes, under
// idxMu, each live row (deleteRelByInfo: row, type key, both legs, counters,
// statistics), the whole history, the ex-adjacency legs of every row and the
// RAM sidecars, all in ONE flush. No change-log record. Nodes are untouched.
func (bs *Store) PurgeRelsByTypeBefore(typeToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if err := bs.checkWritable(); err != nil {
		return zero, err
	}
	if before <= 0 || typeToken == 0 {
		return zero, nil
	}
	defer bs.bumpRelEpoch()
	if chunk <= 0 {
		chunk = defaultRetentionPurgeChunk
	}
	// Commit the write buffer first: every history read below folds the
	// pending overlay, which would otherwise be walked once per row read
	// (half the purge's time with an unflushed backlog).
	if err := bs.flush(); err != nil {
		return zero, err
	}
	ids, more, err := bs.relPurgeCandidates(typeToken, before, chunk)
	if err != nil {
		return zero, err
	}
	if len(ids) == 0 {
		return zero, nil
	}
	victims := make([]relPurgeVictim, 0, len(ids))
	for _, id := range ids {
		legs, lerr := bs.relPurgeLegs(id)
		if lerr != nil {
			return zero, lerr
		}
		victims = append(victims, relPurgeVictim{id: id, legs: legs})
	}

	bs.idxMu.Lock()
	var ops []writeOp
	var purged []storecontract.PurgedRel
	for _, v := range victims {
		if _, live := bs.relIDs[v.id]; live {
			old, gerr := bs.getRelLocked(v.id)
			if gerr != nil {
				bs.idxMu.Unlock()
				return zero, gerr
			}
			info := relDeleteInfoFromRelationship(old)
			bs.deleteRelByInfo(info, old)
			v.legs = appendLeg(v.legs, storecontract.PurgedRel{ID: v.id, TypeToken: info.RelType,
				StartID: types.NodeID(info.StartID), EndID: types.NodeID(info.EndID)})
		}
		histKeys, _, herr := bs.historyTruncateDeleteKeys(storepkg.HistRelPrefix(v.id.SnowflakeID()), 0)
		if herr != nil {
			bs.idxMu.Unlock()
			return zero, herr
		}
		for _, k := range histKeys {
			ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(k)})
		}
		for _, l := range v.legs {
			ops = append(ops, exAdjLegOps(writeOpDelete, v.id.SnowflakeID(), l.StartID.SnowflakeID(), l.EndID.SnowflakeID())...)
		}
		bs.purgeRelSidecarsLocked(v.id)
		purged = append(purged, v.legs...)
	}
	bs.appendOps(ops...)
	bs.dropPropertyTxMembersLocked() // purged values must not outlive their rows (backlog 8)
	bs.idxMu.Unlock()

	if err := bs.flush(); err != nil {
		return zero, err
	}
	return storecontract.RetentionPurgeResult{RelsPurged: len(victims), More: more, PurgedRels: purged}, nil
}

// relPurgeCandidates selects up to chunk relationship IDs of typeToken minted
// before `before`: current rows first, then history-only IDs of the type.
// more reports that at least one further candidate exists.
func (bs *Store) relPurgeCandidates(typeToken uint16, before types.Instant, chunk int) (ids []types.RelID, more bool, err error) {
	minted := func(id types.RelID) bool { return storepkg.SnowflakeInstant(id.SnowflakeID()) < before }
	bs.idxMu.RLock()
	live := bs.relTypeRelIDsSnapshotLocked(typeToken)
	bs.idxMu.RUnlock()
	slices.Sort(live)
	for _, id := range live {
		if !minted(id) {
			continue
		}
		if len(ids) >= chunk {
			return ids, true, nil
		}
		ids = append(ids, id)
	}
	var after types.RelID
	for {
		page, perr := bs.AllRelHistoryIDsFrom(after, relHistoryIDPage)
		if perr != nil {
			return nil, false, perr
		}
		for _, id := range page {
			if !minted(id) {
				return ids, false, nil // ascending: every later ID is minted later
			}
			if bs.HasRelID(id.SnowflakeID()) {
				continue // current here: selected above when of the type
			}
			tok, ok, terr := bs.relHistoryTypeToken(id)
			if terr != nil {
				return nil, false, terr
			}
			if !ok || tok != typeToken {
				continue
			}
			if len(ids) >= chunk {
				return ids, true, nil
			}
			ids = append(ids, id)
		}
		if len(page) < relHistoryIDPage {
			return ids, false, nil
		}
		after = page[len(page)-1]
	}
}

// relHistoryTypeToken reads the type of a relationship from its oldest
// history row (a relationship's type is immutable). ok is false when the
// history is gone.
func (bs *Store) relHistoryTypeToken(id types.RelID) (uint16, bool, error) {
	rows, err := bs.relHistoryVersionsFromPrefix(storepkg.HistRelPrefix(id.SnowflakeID()), 0, 1)
	if err != nil {
		return 0, false, err
	}
	if len(rows) == 0 {
		return 0, false, nil
	}
	return rows[0].TypeToken().Value(), true, nil
}

// relPurgeLegs returns one routing descriptor per distinct endpoint pair of
// the relationship's history rows (a re-imported life may name other
// endpoints). Read outside idxMu; the live row's pair is added under it.
func (bs *Store) relPurgeLegs(id types.RelID) ([]storecontract.PurgedRel, error) {
	rows, err := bs.GetRelHistory(id)
	if err != nil {
		return nil, err
	}
	var legs []storecontract.PurgedRel
	for _, r := range rows {
		legs = appendLeg(legs, storecontract.PurgedRel{ID: id, TypeToken: r.TypeToken().Value(),
			StartID: r.StartNodeID(), EndID: r.EndNodeID()})
	}
	return legs, nil
}

// appendLeg appends l unless the same endpoint pair is already listed.
func appendLeg(legs []storecontract.PurgedRel, l storecontract.PurgedRel) []storecontract.PurgedRel {
	for _, have := range legs {
		if have.StartID == l.StartID && have.EndID == l.EndID {
			return legs
		}
	}
	return append(legs, l)
}

// purgeRelSidecarsLocked drops the RAM sidecars that still name a purged
// relationship (the live-row ones went with deleteRelByInfo). Caller holds
// idxMu (write).
func (bs *Store) purgeRelSidecarsLocked(id types.RelID) {
	bs.maintainRelTypeTemporalIndexesPurge(id.SnowflakeID()) // history goes too: nothing left to cover
	delete(bs.relValidIdx, id)
	delete(bs.relBeliefWatermark, id)
	for tok, members := range bs.relTypeTxMembers {
		delete(members, id)
		if len(members) == 0 {
			delete(bs.relTypeTxMembers, tok)
		}
	}
}

// PurgeRelLegResidue removes, on THIS store, the adjacency and ex-adjacency
// legs a purged relationship left where its row never lived: the in-leg on its
// END node's shard of a split-write partitioned store (tiered), and the
// ex-adjacency legs a delete wrote there. A store that still holds the
// relationship's current row is left alone (its own PurgeRelsByTypeBefore
// removes it whole). Idempotent and recordless.
func (bs *Store) PurgeRelLegResidue(rel storecontract.PurgedRel) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipIndexEntry(rel.StartID, rel.EndID, rel.TypeToken, rel.ID); err != nil {
		return err
	}
	defer bs.bumpRelEpoch()
	bs.idxMu.Lock()
	if _, live := bs.relIDs[rel.ID]; live {
		bs.idxMu.Unlock()
		return nil
	}
	if !bs.adjOnDisk {
		if set := bs.outIdx[rel.StartID]; set != nil {
			delete(set, rel.ID)
			if len(set) == 0 {
				delete(bs.outIdx, rel.StartID)
			}
		}
		if set := bs.inIdx[rel.EndID]; set != nil {
			delete(set, rel.ID)
			if len(set) == 0 {
				delete(bs.inIdx, rel.EndID)
			}
		}
	}
	rid, start, end := rel.ID.SnowflakeID(), rel.StartID.SnowflakeID(), rel.EndID.SnowflakeID()
	ops := []writeOp{
		{opType: writeOpDelete, key: storepkg.OutKey(start, rel.TypeToken, end, rid)},
		{opType: writeOpDelete, key: storepkg.InKey(end, rel.TypeToken, start, rid)},
	}
	bs.appendOps(append(ops, exAdjLegOps(writeOpDelete, rid, start, end)...)...)
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}
