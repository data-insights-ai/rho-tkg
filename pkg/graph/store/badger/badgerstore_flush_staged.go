package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// A flush commits atomically (FLUSHPART, docs/badger.md rule 1). Badger splits
// a WriteBatch that outgrows one transaction (MaxBatchCount entries or
// MaxBatchSize bytes) into independent transactions, so a crash between them
// kept a random part of a flush: rows without their change-log records, or,
// after a removal flush, counters above the live rows and a store that refused
// to open. Now:
//
//   - A flush that fits one badger transaction commits as one Txn (one write
//     request: all or nothing).
//   - A larger flush is staged. 1: every entry is written under KeyFlushStage
//     with a fresh sequence (invisible to every reader). 2: one small
//     transaction sets FlushStageMarkerKey (sequence, entry count) and raises
//     the store format marker to StagedFlushStoreFormatVersion: the commit
//     point. 3: the entries are applied (badger may split this; the marker
//     makes it redoable). 4: the marker is deleted. 5: the staging keys are
//     deleted (a failure here only leaves garbage the next staged flush or
//     open deletes, so the flush still reports success). Badger commits transactions in WAL order and a crash keeps a
//     prefix of them (docs/badger.md section 2), so the order holds with or
//     without SyncWrites.
//   - New recovers before anything reads a row (recoverStagedFlush): with a
//     marker it re-applies that sequence's staging (the values are final, so a
//     repeat is harmless) and cleans up; without one it deletes any staging.
//   - After step 2 the flush is committed: it is never requeued. A failure in
//     step 3 or 4 fails the store closed (every door answers ErrStoreClosed)
//     and the next open applies the flush; a requeue, or a write while the
//     marker stands, could commit a newer value that the reopen's re-apply of
//     the older staged value then overwrites.
//   - The format marker a flush writes is the tracker's (bs.formatMarker), not
//     the value of the raise that queued the write: a staged flush raises the
//     tracker to 9 while a lower raise may already sit in pending.

// stagedFlushStep names the steps of a staged flush (the stagedFlushFault seam).
type stagedFlushStep int

const (
	stagedStepBegin        stagedFlushStep = iota + 1 // the flush outgrew one transaction, nothing staged yet
	stagedStepStaged                                  // staging written
	stagedStepMarked                                  // marker committed
	stagedStepApplyWritten                            // apply entries added, before its Flush
	stagedStepApplied                                 // applied, before the unmark
	stagedStepUnmarked                                // marker deleted
	stagedStepCleanWritten                            // clean-up deletes added, before its Flush
)

func (s stagedFlushStep) String() string {
	switch s {
	case stagedStepBegin:
		return "begin"
	case stagedStepStaged:
		return "staged"
	case stagedStepMarked:
		return "marked"
	case stagedStepApplyWritten:
		return "apply-written"
	case stagedStepApplied:
		return "applied"
	case stagedStepUnmarked:
		return "unmarked"
	case stagedStepCleanWritten:
		return "clean-written"
	}
	return fmt.Sprintf("step%d", int(s))
}

// flushState is how far a failed flush got.
type flushState int

const (
	flushNotCommitted        flushState = iota // nothing committed: requeue
	flushCommittedNotApplied                   // the marker committed and stands: fail closed
	flushApplied                               // committed and applied
)

// flushEntry is one key a flush writes or deletes.
type flushEntry struct {
	key   []byte
	value []byte
	del   bool
}

// stagedApplyRetries bounds the in-place retries of a staged apply.
const stagedApplyRetries = 3

// nextStageSeq returns a staging sequence never used before in this store, so
// recovery never mixes two attempts: increasing within a session (bs.stageSeq),
// seeded at open from the wall clock. An open deletes every staging key before
// the first flush (recoverStagedFlush), so across sessions no older staging
// survives to collide with.
func (bs *Store) nextStageSeq() uint64 {
	bs.stageSeqOnce.Do(func() { bs.stageSeq.Store(uint64(time.Now().UnixNano())) }) // #nosec G115 -- a positive wall clock
	return bs.stageSeq.Add(1)
}

// recoverStagedFlushFault (tests only) fails the open's recovery right after
// the step it names (stagedStepApplyWritten, stagedStepApplied,
// stagedStepUnmarked, stagedStepCleanWritten).
var recoverStagedFlushFault func(stagedFlushStep) error

func recoveryFault(step stagedFlushStep) error {
	if recoverStagedFlushFault != nil {
		return recoverStagedFlushFault(step)
	}
	return nil
}

// writtenFormatMarkers rewrites a format-marker entry to the tracker's value
// (never lower than the entry's own).
func (bs *Store) writtenFormatMarkers(entries []flushEntry) {
	for i := range entries {
		e := &entries[i]
		if e.del || len(e.value) != 2 || string(e.key) != string(storepkg.WireFormatVersionKey) {
			continue
		}
		if v := bs.raisedFormatMarker(binary.BigEndian.Uint16(e.value)); v != binary.BigEndian.Uint16(e.value) {
			e.value = formatMarkerValue(v)
		}
	}
}

func stagePrefix(seq uint64) []byte {
	p := make([]byte, 9)
	p[0] = storepkg.KeyFlushStage
	binary.BigEndian.PutUint64(p[1:], seq)
	return p
}

// commitFlushEntries commits entries atomically: one transaction when they fit,
// else a staged flush. On error, state says how far it got.
func (bs *Store) commitFlushEntries(entries []flushEntry) (flushState, error) {
	bs.writtenFormatMarkers(entries)
	txn := bs.db.NewTransaction(true)
	defer txn.Discard()
	for _, e := range entries {
		var err error
		if e.del {
			err = txn.Delete(e.key)
		} else {
			err = txn.SetEntry(badgerv4.NewEntry(e.key, e.value))
		}
		if errors.Is(err, badgerv4.ErrTxnTooBig) {
			txn.Discard()
			return bs.commitStaged(entries)
		}
		if err != nil {
			return flushNotCommitted, err
		}
	}
	if err := txn.Commit(); err != nil {
		return flushNotCommitted, err
	}
	return flushApplied, nil
}

// commitStaged runs steps 1 to 5 of a staged flush.
func (bs *Store) commitStaged(entries []flushEntry) (flushState, error) {
	fault := func(step stagedFlushStep) error {
		if bs.stagedFlushFault != nil {
			return bs.stagedFlushFault(step)
		}
		return nil
	}
	if err := fault(stagedStepBegin); err != nil {
		return flushNotCommitted, err
	}
	// The marker counts staged keys, and staging keeps one value per key: a
	// duplicate would make recovery refuse the store. None occurs today (ops
	// is a map; counters, last_lsn and the floor are written only here), so
	// this keeps the invariant for a future entry source: the last write of a
	// key wins, as in badger.
	entries = dedupFlushEntries(entries)
	// The staged values carry the format marker the marker transaction stores
	// (at least 9), so a re-apply never lowers it. The tracker rises only when
	// the marker commits: it never leads its cause.
	staged := max(bs.currentFormatMarker(), storepkg.StagedFlushStoreFormatVersion)
	for i := range entries {
		e := &entries[i]
		if !e.del && len(e.value) == 2 && string(e.key) == string(storepkg.WireFormatVersionKey) && binary.BigEndian.Uint16(e.value) < staged {
			e.value = formatMarkerValue(staged)
		}
	}

	// 1. Stage.
	seq := bs.nextStageSeq()
	prefix := stagePrefix(seq)
	wb := bs.db.NewWriteBatch()
	for _, e := range entries {
		val := make([]byte, 1+len(e.value))
		if e.del {
			val[0] = byte(writeOpDelete)
		} else {
			val[0] = byte(writeOpSet)
			copy(val[1:], e.value)
		}
		if err := wb.Set(append(append([]byte(nil), prefix...), e.key...), val); err != nil {
			wb.Cancel()
			return flushNotCommitted, fmt.Errorf("stage: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return flushNotCommitted, fmt.Errorf("stage: %w", err)
	}
	if err := fault(stagedStepStaged); err != nil {
		return flushNotCommitted, err
	}

	// 2. Mark: the commit point, with the format marker.
	marker := make([]byte, stageMarkerLen)
	binary.BigEndian.PutUint64(marker, seq)
	binary.BigEndian.PutUint64(marker[8:], uint64(len(entries)))
	if bs.syncWrites {
		marker[16] = stageMarkerSynced
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Set(storepkg.FlushStageMarkerKey, marker); err != nil {
			return err
		}
		return txn.Set(storepkg.WireFormatVersionKey, formatMarkerValue(staged))
	}); err != nil {
		return flushNotCommitted, fmt.Errorf("mark: %w", err)
	}
	bs.raisedFormatMarker(staged)
	if err := fault(stagedStepMarked); err != nil {
		return flushCommittedNotApplied, err
	}

	// 3. Apply, retried in place: after the marker nothing is requeued.
	var applyErr error
	for attempt := 0; attempt < stagedApplyRetries; attempt++ {
		if applyErr = bs.applyFlushEntries(entries, func() error { return fault(stagedStepApplyWritten) }); applyErr == nil {
			break
		}
		if bs.stagedFlushFault != nil {
			break // an injected failure is final: the test decides what follows
		}
	}
	if applyErr != nil {
		return flushCommittedNotApplied, fmt.Errorf("apply: %w", applyErr)
	}
	if err := fault(stagedStepApplied); err != nil {
		return flushCommittedNotApplied, err
	}

	// 4. Unmark. While the marker stands, the next open re-applies the staged
	// values, so a failure here fails closed like a failed apply.
	if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Delete(storepkg.FlushStageMarkerKey) }); err != nil {
		return flushCommittedNotApplied, fmt.Errorf("unmark: %w", err)
	}

	// 5. Clean every staging key (this attempt's and any aborted one's). A
	// failure leaves unmarked staging, which nothing reads and the next staged
	// flush or open deletes: the flush itself succeeded.
	err := fault(stagedStepUnmarked)
	if err == nil {
		err = deleteAllStaging(bs.db, func() error { return fault(stagedStepCleanWritten) })
	}
	if err != nil && bs.logger != nil {
		bs.logger.Warningf("graph: clean up a staged flush (the next open deletes the leftovers): %v", err)
	}
	return flushApplied, nil
}

// stageMarkerLen is the marker value: 8B sequence, 8B entry count, 1B flags.
const stageMarkerLen = 17

// stageMarkerSynced flags a staged flush written under SyncWrites: every part
// was durable before the next began, so only corruption can tear its staging.
// Without it a power loss can lose earlier staging pages while the marker
// survives (badger syncs neither a rotated memtable's WAL nor, without
// SyncWrites, the active one), which loses that flush like any unsynced one.
const stageMarkerSynced byte = 1

// dedupFlushEntries keeps the last entry of each key, in order.
func dedupFlushEntries(entries []flushEntry) []flushEntry {
	last := make(map[string]int, len(entries))
	for i, e := range entries {
		last[string(e.key)] = i
	}
	if len(last) == len(entries) {
		return entries
	}
	out := make([]flushEntry, 0, len(last))
	for i, e := range entries {
		if last[string(e.key)] == i {
			out = append(out, e)
		}
	}
	return out
}

// currentFormatMarker is the format-marker tracker.
func (bs *Store) currentFormatMarker() uint16 {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	return bs.formatMarker
}

// raisedFormatMarker raises bs.formatMarker to at least v and returns it.
func (bs *Store) raisedFormatMarker(v uint16) uint16 {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	if bs.formatMarker < v {
		bs.formatMarker = v
	}
	return bs.formatMarker
}

// applyFlushEntries writes entries through a WriteBatch, the commit floor
// (when present) first. beforeFlush is the test seam between adding and
// flushing.
func (bs *Store) applyFlushEntries(entries []flushEntry, beforeFlush func() error) error {
	wb := bs.db.NewWriteBatch()
	for _, e := range entries {
		var err error
		if e.del {
			err = wb.Delete(e.key)
		} else {
			err = wb.Set(e.key, e.value)
		}
		if err != nil {
			wb.Cancel()
			return err
		}
	}
	if err := beforeFlush(); err != nil {
		wb.Cancel()
		return err
	}
	return wb.Flush()
}

// deleteAllStaging deletes every key under KeyFlushStage.
func deleteAllStaging(db *badgerv4.DB, beforeFlush func() error) error {
	var keys [][]byte
	prefix := []byte{storepkg.KeyFlushStage}
	if err := db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	wb := db.NewWriteBatch()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			wb.Cancel()
			return err
		}
	}
	if beforeFlush != nil {
		if err := beforeFlush(); err != nil {
			wb.Cancel()
			return err
		}
	}
	return wb.Flush()
}

// failStagedFlush fails the store closed after a staged flush committed but
// could not be applied: every door answers ErrStoreClosed and the next open
// applies the flush (recoverStagedFlush).
func (bs *Store) failStagedFlush(err error) {
	wrapped := fmt.Errorf("%w: a staged flush committed but was not applied (the next open applies it): %w", ErrStoreClosed, err)
	bs.stagedFailure.CompareAndSwap(nil, &wrapped)
}

// stagedFailureErr is the fail-closed error, or nil.
func (bs *Store) stagedFailureErr() error {
	if p := bs.stagedFailure.Load(); p != nil {
		return *p
	}
	return nil
}

// recoverStagedFlush runs at open, before any row is read: a committed staged
// flush (marker present) is applied and cleaned up; unmarked staging is
// deleted. A read-only open cannot apply and fails with an error matching
// badger's ErrTruncateNeeded, so the tiered store's recovery probe falls
// through to a writable open. A torn staging (fewer entries than the marker
// counts) of a flush written without SyncWrites is a power loss that flush was
// never promised to survive: it is discarded, logged, like any lost unsynced
// flush. Any other damage refuses the open (refuseStagedFlush).
func recoverStagedFlush(db *badgerv4.DB, readOnly bool, logger badgerv4.Logger) error {
	marker, err := readStageMarker(db)
	if err != nil {
		return err
	}
	if marker == nil {
		if readOnly {
			return nil // staging without a marker is invisible; a writable open deletes it
		}
		if err := deleteAllStaging(db, nil); err != nil {
			return fmt.Errorf("graph: delete uncommitted staged flush: %w", err)
		}
		return nil
	}
	if readOnly {
		return fmt.Errorf("graph: a committed staged flush awaits its apply; open the store writable once: %w", badgerv4.ErrTruncateNeeded)
	}
	if len(marker) != stageMarkerLen {
		return refuseStagedFlush(fmt.Sprintf("its marker is %d bytes, want %d", len(marker), stageMarkerLen))
	}
	seq := binary.BigEndian.Uint64(marker)
	want := binary.BigEndian.Uint64(marker[8:])
	synced := marker[16]&stageMarkerSynced != 0
	prefix := stagePrefix(seq)
	var entries []flushEntry
	var floor *flushEntry
	if err := db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)[len(prefix):]
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(val) == 0 {
				return errEmptyStagedEntry
			}
			e := flushEntry{key: key, value: val[1:], del: writeOpType(val[0]) == writeOpDelete}
			if string(key) == string(commitFloorKey) {
				floor = &e
				continue
			}
			entries = append(entries, e)
		}
		return nil
	}); err != nil {
		if errors.Is(err, errEmptyStagedEntry) {
			return refuseStagedFlush("a staged entry is empty")
		}
		return fmt.Errorf("graph: read staged flush: %w", err)
	}
	got := uint64(len(entries))
	if floor != nil {
		got++
	}
	if got < want && !synced {
		msg := fmt.Sprintf("graph: discarded a staged flush torn by a power loss (written without SyncWrites: %d of %d entries survived); its writes are lost", got, want)
		if logger != nil {
			logger.Warningf("%s", msg)
		} else {
			slog.Warn(msg)
		}
		return discardStagedFlush(db)
	}
	if got != want {
		return refuseStagedFlush(fmt.Sprintf("it has %d entries, its marker says %d", got, want))
	}
	if floor != nil { // the commit floor first, as the flush wrote it
		if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set(floor.key, floor.value) }); err != nil {
			return fmt.Errorf("graph: apply staged commit floor: %w", err)
		}
	}
	wb := db.NewWriteBatch()
	for _, e := range entries {
		var err error
		if e.del {
			err = wb.Delete(e.key)
		} else {
			err = wb.Set(e.key, e.value)
		}
		if err != nil {
			wb.Cancel()
			return fmt.Errorf("graph: apply staged flush: %w", err)
		}
	}
	if err := recoveryFault(stagedStepApplyWritten); err != nil {
		wb.Cancel()
		return err
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("graph: apply staged flush: %w", err)
	}
	if err := recoveryFault(stagedStepApplied); err != nil {
		return err
	}
	if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Delete(storepkg.FlushStageMarkerKey) }); err != nil {
		return fmt.Errorf("graph: unmark staged flush: %w", err)
	}
	if err := recoveryFault(stagedStepUnmarked); err != nil {
		return err
	}
	if err := deleteAllStaging(db, func() error { return recoveryFault(stagedStepCleanWritten) }); err != nil {
		return fmt.Errorf("graph: clean staged flush: %w", err)
	}
	return nil
}

var errEmptyStagedEntry = errors.New("empty staged entry")

// refuseStagedFlush is the open's refusal of a damaged committed staged flush.
func refuseStagedFlush(detail string) error {
	return fmt.Errorf("%w: a committed staged flush is damaged (%s); restore a backup, or open without it after badger.DiscardStagedFlush (its writes are lost)", ErrInvalidStoreMutation, detail)
}

func readStageMarker(db *badgerv4.DB) ([]byte, error) {
	var marker []byte
	if err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.FlushStageMarkerKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		marker, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		return nil, fmt.Errorf("graph: staged flush marker: %w", err)
	}
	return marker, nil
}

// discardStagedFlush drops a staged flush: the marker first, so a crash
// leaves unmarked staging, which the next open deletes.
func discardStagedFlush(db *badgerv4.DB) error {
	if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Delete(storepkg.FlushStageMarkerKey) }); err != nil {
		return fmt.Errorf("graph: discard staged flush marker: %w", err)
	}
	if err := deleteAllStaging(db, nil); err != nil {
		return fmt.Errorf("graph: discard staged flush: %w", err)
	}
	return nil
}

// DiscardStagedFlush drops a committed staged flush from the store directory
// cfg names (a badger store, a tiered shard or a sharded slot), so a store
// whose open refuses a damaged one (ErrInvalidStoreMutation, "a committed
// staged flush is damaged") opens without it. The flush's writes are lost,
// and a part of it that was already applied stays applied. The store must be
// closed; cfg carries its options (encryption key, memtable size). It reports
// whether there was a staged flush to drop. Without one it changes nothing.
func DiscardStagedFlush(cfg Config) (bool, error) {
	if cfg.InMemory || cfg.Dir == "" {
		return false, fmt.Errorf("graph: discard staged flush: needs an on-disk store directory")
	}
	cfg.ReadOnly = false
	if err := MigrateOversizedWAL(cfg); err != nil {
		return false, err
	}
	db, err := badgerv4.Open(buildBadgerOptions(cfg))
	if err != nil {
		return false, fmt.Errorf("graph: badger open: %w", err)
	}
	marker, err := readStageMarker(db)
	if err == nil && marker != nil {
		err = discardStagedFlush(db)
	}
	return marker != nil && err == nil, errors.Join(err, db.Close())
}
