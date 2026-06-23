package badger

import (
	"encoding/binary"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// This file implements the in-backend change-log (op-log): the write-side
// record producer (logChangeRaw / writeClearMarker), the restart LSN seed
// (seedLogLSN / maxChangeLogLSN), and the read side (the
// store.ChangeFeedCapability methods). Records are persisted by flush() in the
// same WriteBatch as the data; this file only buffers them and reads them back.

// changeFeedPageSize bounds how many records changeFeedPage materializes per
// Badger read transaction. ForEachChange pages through the log so the callback
// runs OUTSIDE the read transaction (it may re-enter Store methods) and peak
// memory stays bounded regardless of log length.
const changeFeedPageSize = 256

// logChangeRaw buffers one change-log record for the next flush, assigning it a
// monotonic LSN. It is a no-op when the change-log is disabled (zero overhead).
//
// The LSN is allocated and the record appended under wbMu in a single critical
// section, so the LSN order equals the buffer order — a total commit order.
// Callers MUST invoke logChangeRaw while holding idxMu.Lock (the door's mutation
// lock), so the record and the entity ops it describes are both buffered before
// any flush can snapshot them (flush snapshots under idxMu.RLock).
func (bs *Store) logChangeRaw(tag storecontract.ChangeTag, payload []byte) {
	if !bs.logEnabled {
		return
	}
	value := storepkg.EncodeChangeValue(tag, payload)
	bs.wbMu.Lock()
	lsn := bs.logSeq.Add(1)
	bs.pendingLog = append(bs.pendingLog, pendingLogRecord{lsn: lsn, value: value})
	bs.wbMu.Unlock()
}

// buildChangePayload msgpack-encodes a composite change-log body BEFORE the
// mutation enqueues its entity ops, so a (practically impossible) marshal error
// aborts the door without leaving an entity op without its record. Returns nil
// when the change-log is disabled — the matching logChangeRaw is then a no-op.
// Put-style records pass the already-marshaled entity wire bytes straight to
// logChangeRaw and never need this.
func (bs *Store) buildChangePayload(body any) ([]byte, error) {
	if !bs.logEnabled {
		return nil, nil
	}
	return storepkg.MarshalChangeBody(body)
}

// appendOpsLogged enqueues entity ops AND one change-log record under ONE wbMu
// critical section, so flush (which swaps pending and pendingLog together under
// wbMu) sees both or neither — atomic even for doors that do NOT hold idxMu.Lock
// across the enqueue (PutNodeVersion / PutRelVersion / history truncation).
// payload is the pre-marshaled body; when the log is disabled only the ops are
// enqueued (equivalent to appendOps).
func (bs *Store) appendOpsLogged(tag storecontract.ChangeTag, payload []byte, ops ...writeOp) {
	bs.wbMu.Lock()
	for _, op := range ops {
		bs.pending[string(op.key)] = op
	}
	if bs.logEnabled {
		value := storepkg.EncodeChangeValue(tag, payload)
		lsn := bs.logSeq.Add(1)
		bs.pendingLog = append(bs.pendingLog, pendingLogRecord{lsn: lsn, value: value})
	}
	bs.wbMu.Unlock()
}

// historyVersionNodePayload builds a ChangeNodeHistoryVersion body for an
// explicit-version history write (PutNodeVersion). nil when the log is disabled.
func (bs *Store) historyVersionNodePayload(version uint32, n *types.Node) ([]byte, error) {
	if !bs.logEnabled {
		return nil, nil
	}
	return storepkg.NodeHistoryVersionPayload(version, n)
}

// historyVersionRelPayload is the relationship counterpart of historyVersionNodePayload.
func (bs *Store) historyVersionRelPayload(version uint32, r *types.Relationship) ([]byte, error) {
	if !bs.logEnabled {
		return nil, nil
	}
	return storepkg.RelHistoryVersionPayload(version, r)
}

// nodeDeleteWithHistoryPayload builds a with-history ChangeNodeDelete body: the
// node tombstone, every connected-relationship tombstone, and the cascaded rel
// IDs. nil when the log is disabled.
func (bs *Store) nodeDeleteWithHistoryPayload(id snowflake.ID, nodeTombstone *types.Node, relTombstones []RelTombstone) ([]byte, error) {
	if !bs.logEnabled {
		return nil, nil
	}
	return storepkg.NodeDeleteWithHistoryPayload(id, nodeTombstone, relTombstones)
}

// logCascadeNodeDelete emits the hard-cascade ChangeNodeDelete record (no
// tombstone/history) carrying the IDs of the relationships the cascade removed,
// so a replica deletes the same node and edges. Called under idxMu.Lock right
// after the cascade's ops are enqueued; a marshal error is surfaced to the
// caller (it never silently drops a record).
func (bs *Store) logCascadeNodeDelete(id snowflake.ID, deleted []RelDeleteInfo) error {
	if !bs.logEnabled {
		return nil
	}
	body := storepkg.NodeDeleteBody{ID: int64(id)}
	for _, d := range deleted {
		body.CascadedRelIDs = append(body.CascadedRelIDs, int64(d.ID))
	}
	// Sort ascending so the record is DETERMINISTIC: `deleted` is built by
	// ranging a Go map (randomized iteration order), so without this the same
	// cascade emits different durable bytes run-to-run and diverges from the
	// memory backend, which also sorts. CascadedRelIDs is defined as the rel rows
	// actually removed by the cascade (orphan-only adjacency entries are purged
	// separately and excluded), sorted ascending.
	sort.Slice(body.CascadedRelIDs, func(i, j int) bool { return body.CascadedRelIDs[i] < body.CascadedRelIDs[j] })
	payload, err := storepkg.MarshalChangeBody(body)
	if err != nil {
		return err
	}
	bs.logChangeRaw(storecontract.ChangeNodeDelete, payload)
	return nil
}

// relDeleteWithHistoryPayload builds a with-history ChangeRelDelete body.
func (bs *Store) relDeleteWithHistoryPayload(id snowflake.ID, tombstone *types.Relationship) ([]byte, error) {
	if !bs.logEnabled {
		return nil, nil
	}
	return storepkg.RelDeleteWithHistoryPayload(id, tombstone)
}

// writeClearMarker re-anchors the change-log after a Clear() DropAll, which
// removed every KeyChangeLog record and LastLSNKey. A fresh ChangeClear record
// at a new (still strictly monotonic) LSN tells a tailing consumer that
// everything before it is gone and it must reset its state. Called from Clear()
// while holding idxMu.Lock and flushMu, so no concurrent writer/flush can race.
func (bs *Store) writeClearMarker() error {
	lsn := bs.logSeq.Add(1)
	value := storepkg.EncodeChangeValue(storecontract.ChangeClear, nil)
	lsnBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBuf, lsn)

	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	if err := wb.SetEntry(badgerv4.NewEntry(storepkg.ChangeLogKey(lsn), value)); err != nil {
		return fmt.Errorf("graph: clear marker record: %w", err)
	}
	if err := wb.SetEntry(badgerv4.NewEntry(storepkg.LastLSNKey, lsnBuf)); err != nil {
		return fmt.Errorf("graph: clear marker watermark: %w", err)
	}
	if bs.dbClosed.Load() {
		wb.Cancel()
		return fmt.Errorf("graph: clear marker flush: %w", badgerv4.ErrDBClosed)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("graph: clear marker flush: %w", err)
	}
	return nil
}

// seedLogLSN reads the durable change-log watermark within the open-time
// transaction so the LSN allocator resumes strictly above every persisted
// record. LastLSNKey commits in the same WriteBatch as its records, so it is
// crash-consistent with the maximum KeyChangeLog key; the max-key scan is a
// defensive fallback for an absent marker (a fresh store has neither and
// seeds 0).
func seedLogLSN(txn *badgerv4.Txn) (uint64, error) {
	item, err := txn.Get(storepkg.LastLSNKey)
	switch {
	case err == badgerv4.ErrKeyNotFound:
		return maxChangeLogLSN(txn), nil
	case err != nil:
		return 0, err
	}
	var marker uint64
	if e := item.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("graph: last_lsn value size %d, want 8", len(v))
		}
		marker = binary.BigEndian.Uint64(v)
		return nil
	}); e != nil {
		return 0, e
	}
	if scanned := maxChangeLogLSN(txn); scanned > marker {
		marker = scanned
	}
	return marker, nil
}

// maxChangeLogLSN returns the highest LSN present in the KeyChangeLog keyspace,
// or 0 when it is empty. It seeks the upper bound of the prefix with a reverse
// iterator so it touches only the single largest key.
func maxChangeLogLSN(txn *badgerv4.Txn) uint64 {
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	prefix := storepkg.ChangeLogPrefix()
	// Largest possible key in the prefix: prefix byte followed by 8 x 0xFF. The
	// reverse iterator lands on the greatest key <= seek, i.e. the max LSN.
	seek := append(storepkg.ChangeLogPrefix(), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
	it.Seek(seek)
	if it.ValidForPrefix(prefix) {
		if lsn, ok := storepkg.ChangeLogLSNFromKey(it.Item().KeyCopy(nil)); ok {
			return lsn
		}
	}
	return 0
}

// LastCommittedLSN returns the highest durably-committed change-log LSN, or 0
// when the log is empty. It reads the durable watermark (committed records
// only) — a record buffered but not yet flushed is intentionally not counted.
func (bs *Store) LastCommittedLSN() (uint64, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	var lsn uint64
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.LastLSNKey)
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("graph: last_lsn value size %d, want 8", len(v))
			}
			lsn = binary.BigEndian.Uint64(v)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return lsn, nil
}

// ChangeFeed returns up to limit committed records with LSN > afterLSN, in
// ascending LSN order. limit <= 0 returns all available records. Payloads are
// owned copies, safe to retain.
func (bs *Store) ChangeFeed(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	var out []storecontract.ChangeRecord
	err := bs.ForEachChange(afterLSN, func(rec storecontract.ChangeRecord) bool {
		payload := make([]byte, len(rec.Payload))
		copy(payload, rec.Payload)
		out = append(out, storecontract.ChangeRecord{LSN: rec.LSN, Tag: rec.Tag, Payload: payload})
		return limit <= 0 || len(out) < limit
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ForEachChange streams committed records with LSN > afterLSN in ascending LSN
// order, invoking fn for each until fn returns false or the log is exhausted.
// fn is called OUTSIDE the Badger read transaction (records are paged), so it
// may re-enter Store methods; the ChangeRecord.Payload it receives is valid only
// for the duration of the call (copy it to retain).
func (bs *Store) ForEachChange(afterLSN uint64, fn func(storecontract.ChangeRecord) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return ErrInvalidStoreMutation
	}
	cursor := afterLSN
	for {
		recs, err := bs.changeFeedPage(cursor, changeFeedPageSize)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		for i := range recs {
			cursor = recs[i].LSN
			if !fn(recs[i]) {
				return nil
			}
		}
		if len(recs) < changeFeedPageSize {
			return nil
		}
	}
}

// changeFeedPage reads up to limit committed records with LSN > afterLSN within
// one Badger read transaction, copying each value so the records outlive the
// transaction. A corrupt or hostile record value fails closed with
// store.ErrCorruptWire via SplitChangeValue.
func (bs *Store) changeFeedPage(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error) {
	if afterLSN == ^uint64(0) {
		return nil, nil // no LSN can exceed MaxUint64
	}
	recs := make([]storecontract.ChangeRecord, 0, limit)
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := storepkg.ChangeLogPrefix()
		start := storepkg.ChangeLogKey(afterLSN + 1) // exclusive of afterLSN
		for it.Seek(start); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			lsn, ok := storepkg.ChangeLogLSNFromKey(item.KeyCopy(nil))
			if !ok {
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			tag, payload, err := storepkg.SplitChangeValue(val)
			if err != nil {
				return err // ErrCorruptWire — a corrupt record fails the read closed
			}
			recs = append(recs, storecontract.ChangeRecord{LSN: lsn, Tag: tag, Payload: payload})
			if limit > 0 && len(recs) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recs, nil
}
