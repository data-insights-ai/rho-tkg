package badger

import (
	"encoding/binary"
	"errors"
	"fmt"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// LogRangePurge appends ONE ChangeRangePurge record (ADR-0008 R3) to the change-
// log so a replica re-executes the predicate. No-op when the log is disabled.
// See store.RangePurgeLogCapability.
func (bs *Store) LogRangePurge(labelToken uint16, before types.Instant, mode uint8) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if !bs.logEnabled {
		return nil
	}
	payload, err := bs.buildChangePayload(storepkg.RangePurgeBody{
		LabelToken: labelToken,
		Before:     int64(before),
		Mode:       mode,
	})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	bs.idxMu.Lock()
	bs.logChangeRaw(storecontract.ChangeRangePurge, payload)
	bs.idxMu.Unlock()
	return bs.flush()
}

// defaultRetentionPurgeChunk bounds how many nodes one PurgeNodesByLabelBefore
// call removes in a single atomic batch, so the lock span and WriteBatch stay
// bounded regardless of how far behind the boundary the store is.
const defaultRetentionPurgeChunk = 256

// PurgeNodesByLabelBefore hard-removes aged-out nodes of a label (ADR-0008 R2).
// See store.RetentionPurgeCapability for the contract. Each call removes up to
// `chunk` nodes below the boundary plus all their rels/indexes/history in ONE
// atomic batch (mirroring DeleteNodeWithHistory's single-lock-span assembly), and
// reports More so the caller loops. It emits NO change-log record — the graph
// layer owns the single ChangeRangePurge + the watermark advance.
func (bs *Store) PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if err := bs.checkWritable(); err != nil {
		return zero, err
	}
	// A purge removes node AND rel rows + adjacency, so both mutation epochs must
	// advance for consumer cache invalidation (same contract as delete doors).
	defer bs.bumpNodeEpoch()
	defer bs.bumpRelEpoch()
	if before <= 0 {
		return zero, nil // nothing is older than a non-positive boundary
	}
	if chunk <= 0 {
		chunk = defaultRetentionPurgeChunk
	}

	// Phase A (idxMu.RLock): snapshot the label's node IDs, then select up to
	// `chunk` whose IMMUTABLE snowflake mint-time is below the boundary. The
	// selection is on the ID's own time — never ValidFrom / a backfilled TxFrom.
	bs.idxMu.RLock()
	ids, err := bs.labelNodeIDsSnapshotLocked(labelToken)
	bs.idxMu.RUnlock()
	if err != nil {
		return zero, err
	}
	victims := make([]types.NodeID, 0, chunk)
	more := false
	for _, id := range ids {
		if storepkg.SnowflakeInstant(id.SnowflakeID()) >= before {
			continue
		}
		if len(victims) >= chunk {
			more = true
			break
		}
		victims = append(victims, id)
	}
	if len(victims) == 0 {
		return storecontract.RetentionPurgeResult{More: false}, nil
	}

	// Prefetch each victim's cascade rows OUTSIDE the write lock (prefetch takes
	// idxMu.RLock internally). cascadeDeleteInner re-validates every prefetched row
	// under the write lock, so a stale prefetch is safe.
	prefetch := make(map[types.NodeID]cascadeDeletePrefetch, len(victims))
	for _, v := range victims {
		pf, perr := bs.prefetchCascadeDeleteRows(v)
		if errors.Is(perr, ErrNodeNotFound) {
			continue // concurrently gone — skipped under the lock too
		}
		if perr != nil {
			return zero, perr
		}
		prefetch[v] = pf
	}

	// Phase B (idxMu.Lock): cascade-delete each victim (recordless — appends its
	// own node/rel/index delete ops) and accumulate history-row deletes for the
	// node and every removed rel. All ops land in ONE flush → one atomic batch.
	bs.idxMu.Lock()
	nodesPurged := 0
	relsPurged := 0
	purgedIDs := make([]types.NodeID, 0, len(victims))
	var purgedRels []storecontract.PurgedRel
	var histOps []writeOp
	for _, v := range victims {
		pf, ok := prefetch[v]
		if !ok {
			continue // skipped above (already gone)
		}
		// Capture every relationship touching v from its adjacency KEYS BEFORE the
		// cascade removes them — this is the only way to surface a CROSS-SHARD rel
		// whose entity lives on another shard (an entity read here would miss it),
		// which the tiered residue sweep needs.
		purgedRels = append(purgedRels, bs.purgedRelsForNodeLocked(v)...)
		deleted, corruptErr, fatalErr := bs.cascadeDeleteInner(v, pf)
		if fatalErr != nil {
			if errors.Is(fatalErr, ErrNodeNotFound) {
				continue // concurrently purged between prefetch and lock
			}
			bs.idxMu.Unlock()
			return zero, fatalErr
		}
		// corruptErr: the node row was unreadable but its indexes were scrubbed and
		// the connected rels are in `deleted` — the entity IS gone, which is exactly
		// the purge goal, so proceed (removing corrupt aged-out data is desirable).
		_ = corruptErr
		nodesPurged++
		relsPurged += len(deleted)
		purgedIDs = append(purgedIDs, v)

		nodeHistKeys, _, herr := bs.historyTruncateDeleteKeys(storepkg.HistNodePrefix(v.SnowflakeID()), 0)
		if herr != nil {
			bs.idxMu.Unlock()
			return zero, herr
		}
		for _, k := range nodeHistKeys {
			histOps = append(histOps, writeOp{opType: writeOpDelete, key: []byte(k)})
		}
		for _, rd := range deleted {
			relHistKeys, _, herr := bs.historyTruncateDeleteKeys(storepkg.HistRelPrefix(rd.ID), 0)
			if herr != nil {
				bs.idxMu.Unlock()
				return zero, herr
			}
			for _, k := range relHistKeys {
				histOps = append(histOps, writeOp{opType: writeOpDelete, key: []byte(k)})
			}
		}
	}
	if len(histOps) > 0 {
		bs.appendOps(histOps...)
	}
	bs.idxMu.Unlock()

	if err := bs.flush(); err != nil {
		return zero, err
	}
	return storecontract.RetentionPurgeResult{
		PurgedRels:    purgedRels,
		NodesPurged:   nodesPurged,
		RelsPurged:    relsPurged,
		More:          more,
		PurgedNodeIDs: purgedIDs,
	}, nil
}

// purgedRelsForNodeLocked decodes every relationship touching nid straight from
// its adjacency KEYS (persisted + pending overlay), not from entity reads — the
// key encodes BOTH endpoints (out: 0x05|start|type|end|relID; in:
// 0x06|end|type|start|relID), so a CROSS-SHARD rel whose entity lives on another
// shard (invisible to a local entity read) is still captured with its endpoints.
// The tiered cross-shard residue sweep needs those endpoints. Caller holds
// idxMu.Lock. Deduplicated by rel ID (a self-loop appears in both directions).
func (bs *Store) purgedRelsForNodeLocked(nid types.NodeID) []storecontract.PurgedRel {
	sid := nid.SnowflakeID()
	seen := make(map[types.RelID]struct{})
	var out []storecontract.PurgedRel

	collect := func(keyTag byte, incoming bool) {
		prefix := make([]byte, 9)
		prefix[0] = keyTag
		binary.BigEndian.PutUint64(prefix[1:], uint64(sid)) // #nosec G115 — key round-trips int64 bits
		emit := func(kb []byte) {
			if len(kb) != storepkg.SizeAdjacency || kb[0] != keyTag || !hasPrefix(kb, prefix) {
				return
			}
			rid := types.RelID(storepkg.ParseIDFromKey(kb, 19))
			if _, ok := seen[rid]; ok {
				return
			}
			seen[rid] = struct{}{}
			pr := storecontract.PurgedRel{
				ID:        rid,
				TypeToken: binary.BigEndian.Uint16(kb[9:11]),
			}
			other := types.NodeID(storepkg.ParseIDFromKey(kb, 11))
			if incoming {
				pr.StartID, pr.EndID = other, nid // 0x06|end=nid|type|start=other
			} else {
				pr.StartID, pr.EndID = nid, other // 0x05|start=nid|type|end=other
			}
			out = append(out, pr)
		}
		// Pending overlay: a set adds the key, a delete removes it (authoritative
		// over the committed keyspace).
		dels := make(map[string]struct{})
		var pendingSets [][]byte
		bs.rangePending(func(k string, op writeOp) {
			kb := []byte(k)
			if len(kb) != storepkg.SizeAdjacency || kb[0] != keyTag || !hasPrefix(kb, prefix) {
				return
			}
			if op.opType == writeOpDelete {
				dels[k] = struct{}{}
			} else {
				pendingSets = append(pendingSets, kb)
			}
		})
		_ = bs.db.View(func(txn *badgerv4.Txn) error {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				k := it.Item().KeyCopy(nil)
				if _, dropped := dels[string(k)]; dropped {
					continue
				}
				emit(k)
			}
			return nil
		})
		for _, kb := range pendingSets {
			if _, dropped := dels[string(kb)]; dropped {
				continue
			}
			emit(kb)
		}
	}

	collect(storepkg.KeyOut, false)
	collect(storepkg.KeyIn, true)
	return out
}

// PurgeRelationshipByInfo recordlessly cleans a relationship's residue on THIS
// store, given only its routing descriptor (the row may already be gone). It is
// the cross-shard cleanup primitive for a SPLIT-WRITE partitioned backend
// (tiered), where a rel's entity+out-leg live on the start node's shard and its
// in-leg on the end node's shard, so a per-shard label purge leaves a residue on
// the OTHER endpoint's shard. It dispatches on where the residue is:
//   - entity present here (a survivor→purged rel whose entity is on this shard):
//     full delete of the entity + both legs' local keys + its version history;
//   - entity absent here (only a dangling in-leg for a purged→survivor rel):
//     an orphan-index purge of the leftover adjacency/type keys.
//
// Idempotent: a rel with no residue on this shard is a no-op. Recordless — the
// graph layer owns the single ChangeRangePurge.
func (bs *Store) PurgeRelationshipByInfo(rel storecontract.PurgedRel) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch()
	rid := rel.ID

	bs.idxMu.Lock()
	if _, entityHere := bs.relIDs[rid]; entityHere {
		// The rel entity lives on this shard — a full recordless delete (both legs'
		// local keys via the info; the remote leg was already purged with the node)
		// plus its version history (0x08, co-located with the entity).
		bs.deleteRelByInfo(RelDeleteInfo{
			ID:      rid.SnowflakeID(),
			RelType: rel.TypeToken,
			StartID: rel.StartID.SnowflakeID(),
			EndID:   rel.EndID.SnowflakeID(),
		})
		histKeys, _, herr := bs.historyTruncateDeleteKeys(storepkg.HistRelPrefix(rid.SnowflakeID()), 0)
		if herr != nil {
			bs.idxMu.Unlock()
			return herr
		}
		if len(histKeys) > 0 {
			ops := make([]writeOp, 0, len(histKeys))
			for _, k := range histKeys {
				ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(k)})
			}
			bs.appendOps(ops...)
		}
		bs.idxMu.Unlock()
		return bs.flushIfNeeded()
	}
	// The entity is elsewhere — only a dangling adjacency leg (or nothing) is here.
	err := bs.purgeOrphanRelIDLocked(rid)
	bs.idxMu.Unlock()
	if err != nil {
		return err
	}
	return bs.flushIfNeeded()
}

// PurgeAdjacentRelsForNode recordlessly removes every relationship on THIS store
// whose start OR end is nodeID, together with each rel's version history. It is
// the partitioned-store cross-shard cleanup primitive (ADR-0008 R4): when a
// sharded purge removes a node on one shard, an edge MINTED IN ANOTHER node's
// slot that points at it lives on a different shard and must be swept there too,
// or it dangles. nodeID itself need NOT exist on this store (it was purged on its
// own shard). Returns the number of relationships removed. Recordless — the
// caller owns the single ChangeRangePurge record.
func (bs *Store) PurgeAdjacentRelsForNode(nodeID types.NodeID) (int, error) {
	if err := bs.checkWritable(); err != nil {
		return 0, err
	}
	defer bs.bumpRelEpoch()

	bs.idxMu.Lock()
	// Dedup: a self-loop (start == end == nodeID) appears in both directions, and a
	// rel could be listed once per direction.
	out, oerr := bs.adjacentRelIDsSnapshotLocked(nodeID, 0, false)
	if oerr != nil {
		bs.idxMu.Unlock()
		return 0, oerr
	}
	in, ierr := bs.adjacentRelIDsSnapshotLocked(nodeID, 0, true)
	if ierr != nil {
		bs.idxMu.Unlock()
		return 0, ierr
	}
	relIDs := make(map[types.RelID]struct{}, len(out)+len(in))
	for _, rid := range out {
		relIDs[rid] = struct{}{}
	}
	for _, rid := range in {
		relIDs[rid] = struct{}{}
	}
	removed := 0
	var histOps []writeOp
	for rid := range relIDs {
		r, err := bs.getRelLocked(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				// Stale adjacency with no row — purge the orphan index entries.
				if perr := bs.purgeOrphanRelIDLocked(rid); perr != nil {
					bs.idxMu.Unlock()
					return removed, perr
				}
				continue
			}
			bs.idxMu.Unlock()
			return removed, err
		}
		bs.deleteRelByInfo(relDeleteInfoFromRelationship(r))
		removed++
		relHistKeys, _, herr := bs.historyTruncateDeleteKeys(storepkg.HistRelPrefix(rid.SnowflakeID()), 0)
		if herr != nil {
			bs.idxMu.Unlock()
			return removed, herr
		}
		for _, k := range relHistKeys {
			histOps = append(histOps, writeOp{opType: writeOpDelete, key: []byte(k)})
		}
	}
	if len(histOps) > 0 {
		bs.appendOps(histOps...)
	}
	bs.idxMu.Unlock()

	if err := bs.flush(); err != nil {
		return removed, err
	}
	return removed, nil
}
