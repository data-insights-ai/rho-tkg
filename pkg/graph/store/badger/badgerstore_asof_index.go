package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// An as-of proof certifies every history row of an entity: versions and
// TxFrom strictly increase, closed intervals join without a gap or overlap,
// and none is a tombstone, void or retraction. Then the predecessor is the
// highest eligible version; neither a same-write run nor a lower tombstone
// can change the answer. Unsupported mutations invalidate the proof. There
// is no RAM mirror and no read-side build of legacy chains.
type asOfProof struct {
	version, txFrom, generation, firstTxFrom, lastTxTo uint64
}

const asOfProofSize = 41

func decodeAsOfProof(raw []byte) (asOfProof, bool, error) {
	if len(raw) == 0 || (len(raw) == 1 && raw[0] == 0) {
		return asOfProof{}, false, nil
	}
	if len(raw) != asOfProofSize || raw[0] != 1 {
		return asOfProof{}, false, fmt.Errorf("%w: malformed as-of history proof", storecontract.ErrCorruptWire)
	}
	p := asOfProof{binary.BigEndian.Uint64(raw[1:]), binary.BigEndian.Uint64(raw[9:]), binary.BigEndian.Uint64(raw[17:]), binary.BigEndian.Uint64(raw[25:]), binary.BigEndian.Uint64(raw[33:])}
	if p.version > math.MaxUint32 || p.firstTxFrom == 0 || p.firstTxFrom > p.txFrom || p.txFrom >= p.lastTxTo || p.lastTxTo > math.MaxInt64 {
		return asOfProof{}, false, fmt.Errorf("%w: invalid as-of history proof bounds", storecontract.ErrCorruptWire)
	}
	return p, true, nil
}

func (p asOfProof) encode() []byte {
	raw := make([]byte, asOfProofSize)
	raw[0] = 1
	binary.BigEndian.PutUint64(raw[1:], p.version)
	binary.BigEndian.PutUint64(raw[9:], p.txFrom)
	binary.BigEndian.PutUint64(raw[17:], p.generation)
	binary.BigEndian.PutUint64(raw[25:], p.firstTxFrom)
	binary.BigEndian.PutUint64(raw[33:], p.lastTxTo)
	return raw
}

// asOfStoredValueLocked reads write-side metadata, newest buffered operation
// first. Caller holds wbMu; its returned bytes are immutable for that lock.
func (bs *Store) asOfStoredValueLocked(txn *badgerv4.Txn, key []byte) ([]byte, error) {
	for _, ops := range []map[string]writeOp{bs.pending, bs.flushing} {
		if op, ok := ops[string(key)]; ok {
			if op.opType == writeOpDelete {
				return nil, nil
			}
			return op.value, nil
		}
	}
	noteAsOfHookGet()
	return asOfStoredValue(txn, key)
}

func asOfStoredValue(txn *badgerv4.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badgerv4.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func asOfGeneration(raw []byte) (uint64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("%w: malformed as-of recovery generation", storecontract.ErrCorruptWire)
	}
	return binary.BigEndian.Uint64(raw), nil
}

// noteAsOfHistoryLocked runs in the history mutation's wbMu section, after
// its row was queued. Proof, alias, seek key and format marker therefore
// share the row's atomic flush, including staged flush recovery. Requeue
// restores those operations verbatim and does not run this hook again.
func (bs *Store) noteAsOfHistoryLocked(op writeOp) {
	if asOfIndexOff {
		return
	}
	if len(op.key) != storepkg.SizeHistKey || (op.key[0] != storepkg.KeyHistNode && op.key[0] != storepkg.KeyHistRel) {
		return
	}
	kind, id, version := op.key[0], storepkg.ParseIDFromKey(op.key, 1), historyVersionFromKey(op.key)
	proofKey := storepkg.HistoryAsOfProofKey(kind, id)
	aliasKey := storepkg.HistoryAsOfAliasKey(kind, id, version)
	invalidate := func() {
		bs.putPendingLocked(writeOp{opType: writeOpSet, key: proofKey, value: []byte{0}})
		bs.raiseFormatMarkerPendingLocked(storepkg.HistoryAsOfStoreFormatVersion)
	}
	err := bs.view(func(txn *badgerv4.Txn) error {
		noteAsOfHookTxn()
		alias, err := bs.asOfStoredValueLocked(txn, aliasKey)
		if err != nil {
			return err
		}
		if len(alias) != 0 {
			if len(alias) != 8 {
				return fmt.Errorf("%w: malformed as-of history alias", storecontract.ErrCorruptWire)
			}
			oldKey := storepkg.HistoryAsOfSeekKey(kind, id, binary.BigEndian.Uint64(alias), version)
			bs.putPendingLocked(writeOp{opType: writeOpDelete, key: oldKey})
			bs.putPendingLocked(writeOp{opType: writeOpDelete, key: aliasKey})
		}
		raw, err := bs.asOfStoredValueLocked(txn, proofKey)
		if err != nil {
			return err
		}
		proof, valid, err := decodeAsOfProof(raw)
		if err != nil {
			return err
		}
		if len(raw) != 0 && !valid {
			return nil // a known invalid chain never grows new seek entries
		}
		if op.opType == writeOpDelete {
			if valid {
				invalidate()
			}
			return nil
		}
		tm, err := storepkg.DecodeValidEndValue(op.value, kind == storepkg.KeyHistNode)
		if err != nil {
			return err
		}
		ordinary := tm != nil && tm.TxFrom > 0 && tm.TxTo > tm.TxFrom && tm.DeletedAt == 0 && !tm.Void && !tm.Retracted && version <= math.MaxUint32
		if !ordinary {
			if valid {
				invalidate()
			}
			return nil
		}
		genRaw, err := bs.asOfStoredValueLocked(txn, storepkg.HistoryAsOfGenerationKey)
		if err != nil {
			return err
		}
		generation, err := asOfGeneration(genRaw)
		if err != nil {
			return err
		}
		if valid {
			if proof.generation != generation || version <= proof.version || uint64(tm.TxFrom) != proof.lastTxTo {
				invalidate()
				return nil
			}
		} else if !bs.asOfFirstHistoryLocked(txn, op.key) {
			return nil // unknown legacy/imported history is never certified by a read
		}
		first := proof.firstTxFrom
		if !valid {
			first = uint64(tm.TxFrom)
		}
		proof = asOfProof{version, uint64(tm.TxFrom), generation, first, uint64(tm.TxTo)}
		seekKey := storepkg.HistoryAsOfSeekKey(kind, id, proof.txFrom, version)
		alias = make([]byte, 8)
		binary.BigEndian.PutUint64(alias, proof.txFrom)
		bs.putPendingLocked(writeOp{opType: writeOpSet, key: seekKey, value: nil})
		bs.putPendingLocked(writeOp{opType: writeOpSet, key: aliasKey, value: alias})
		bs.putPendingLocked(writeOp{opType: writeOpSet, key: proofKey, value: proof.encode()})
		bs.raiseFormatMarkerPendingLocked(storepkg.HistoryAsOfStoreFormatVersion)
		return nil
	})
	if err != nil {
		// History mutation already owns its validated row. Failure to prove the
		// accelerator must disable it, never leave an older proof trusted.
		invalidate()
	}
}

// The new row is already pending. Any other buffered key or any committed
// history key makes this a legacy/unknown chain. Deleted keys conservatively
// prevent certification too; there is no cleanup scan under wbMu.
func (bs *Store) asOfFirstHistoryLocked(txn *badgerv4.Txn, key []byte) bool {
	prefix := key[:9]
	for _, keys := range [][]string{bs.pendingByPrefix[string(prefix)], bs.flushingByPrefix[string(prefix)]} {
		if len(keys) > 1 || (len(keys) == 1 && keys[0] != string(key)) {
			return false
		}
	}
	if _, ok := bs.flushing[string(key)]; ok {
		return false
	}
	noteAsOfHookSeek()
	it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: prefix})
	defer it.Close()
	it.Seek(prefix)
	return !it.ValidForPrefix(prefix)
}

// invalidateAsOfGeneration disables proofs after a staged flush is discarded:
// some applied rows may remain, so their older proof must not survive. One
// metadata update invalidates every proof without scanning entities.
func invalidateAsOfGeneration(txn *badgerv4.Txn) error {
	raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfGenerationKey)
	if err != nil {
		return err
	}
	generation, err := asOfGeneration(raw)
	if err != nil {
		return err
	}
	if generation == math.MaxUint64 {
		return fmt.Errorf("%w: as-of recovery generation exhausted", storecontract.ErrCorruptWire)
	}
	raw = make([]byte, 8)
	binary.BigEndian.PutUint64(raw, generation+1)
	return txn.Set(storepkg.HistoryAsOfGenerationKey, raw)
}

// indexedAsOf uses only committed proofs. It opens the transaction while
// holding wbMu after ruling out this entity's pending/flushing history, then
// releases the lock: its proof, predecessor, row and anchor share that pinned
// transaction even if a writer or flush proceeds. A pending chain uses the
// ordinary admitted resolver. decode must read a delta anchor from txn.
func (bs *Store) indexedAsOf(kind byte, id snowflake.ID, current *types.TemporalMetadata, pin types.Instant, t *storecontract.ReadTally, decode func(*badgerv4.Txn, uint64, []byte) error) (fromCurrent, handled bool, err error) {
	defer recoverBadgerPanic(&err)
	prefix := storepkg.HistNodePrefix(id)
	prefix[0] = kind
	txn, vlog, err := bs.asOfIndexTxn(prefix)
	if err != nil || txn == nil {
		return false, false, err
	}
	defer func() { bs.endReadTxn(txn, vlog, !bs.dbClosed.Load() && !bs.closing.Load()) }()

	proof, valid, err := admittedAsOfProof(txn, kind, id, t)
	if err != nil || !valid {
		return false, false, err
	}
	if current != nil && current.TxFrom > 0 && current.TxFrom <= pin && current.TxTo == 0 {
		return true, true, nil // every upper history row is closed: canonical current arm
	}
	if pin <= 0 {
		return false, true, ErrVersionNotFound
	}
	if err := t.Buffer(storepkg.HistoryAsOfSeekSize); err != nil {
		return false, false, err
	}
	seek := storepkg.HistoryAsOfSeekKey(kind, id, uint64(pin), math.MaxUint64)
	opts := badgerv4.IteratorOptions{Prefix: seek[:10], Reverse: true}
	it := txn.NewIterator(opts)
	defer it.Close()
	if err := t.Candidates(1); err != nil {
		return false, false, err
	}
	it.Seek(seek)
	if !it.ValidForPrefix(seek[:10]) {
		if uint64(pin) >= proof.firstTxFrom && uint64(pin) < proof.lastTxTo {
			return false, false, fmt.Errorf("%w: as-of history seek entry missing", storecontract.ErrCorruptWire)
		}
		return false, true, ErrVersionNotFound
	}
	noteIndexEntryVisited()
	key := it.Item().Key()
	if len(key) != storepkg.HistoryAsOfSeekSize {
		return false, false, fmt.Errorf("%w: malformed as-of history seek key", storecontract.ErrCorruptWire)
	}
	if err := t.Decoded(16); err != nil {
		return false, false, err
	}
	txFrom, version := binary.BigEndian.Uint64(key[10:]), binary.BigEndian.Uint64(key[18:])
	noteAsOfIndexDecode()
	if txFrom < proof.firstTxFrom || txFrom > uint64(pin) || txFrom > proof.txFrom || version > proof.version || (version == proof.version) != (txFrom == proof.txFrom) {
		return false, false, fmt.Errorf("%w: as-of history seek exceeds proof", storecontract.ErrCorruptWire)
	}
	rowKey := storepkg.HistNodeKey(id, version)
	rowKey[0] = kind
	row, err := asOfIndexedHistoryRaw(txn, rowKey, t)
	if err != nil {
		return false, false, err
	}
	// Validate the pointed row before publishing a winner. A malformed index
	// must not turn an unchecked version or transaction stamp into an answer.
	if err := t.DecodedHistory(len(row)); err != nil {
		return false, false, err
	}
	tm, err := storepkg.DecodeValidEndValue(row, kind == storepkg.KeyHistNode)
	if err != nil {
		return false, false, err
	}
	bs.historyMetaDecodes.Add(1)
	if tm == nil || tm.TxFrom <= 0 || uint64(tm.TxFrom) != txFrom || tm.TxTo <= tm.TxFrom || uint64(tm.TxTo) > proof.lastTxTo || tm.DeletedAt != 0 || tm.Void || tm.Retracted {
		return false, false, fmt.Errorf("%w: as-of history row disagrees with proof", storecontract.ErrCorruptWire)
	}
	if (version == proof.version) != (uint64(tm.TxTo) == proof.lastTxTo) {
		return false, false, fmt.Errorf("%w: as-of history row disagrees with proof end", storecontract.ErrCorruptWire)
	}
	if tm.TxTo <= pin {
		if uint64(pin) < proof.lastTxTo {
			return false, false, fmt.Errorf("%w: as-of history seek omitted the covering row", storecontract.ErrCorruptWire)
		}
		return false, true, ErrVersionNotFound
	}
	if asOfIndexedBeforeDecodeTestHook != nil {
		asOfIndexedBeforeDecodeTestHook()
	}
	return false, true, decode(txn, version, row)
}

// admittedAsOfProof reads the proof and recovery generation from one view.
// The caller opens txn through asOfIndexTxn to exclude buffered history. Each
// native probe, retained value and scalar decode is admitted before its work.
func admittedAsOfProof(txn *badgerv4.Txn, kind byte, id snowflake.ID, t *storecontract.ReadTally) (asOfProof, bool, error) {
	meta := func(key []byte) ([]byte, error) {
		if err := t.Candidates(1); err != nil {
			return nil, err
		}
		noteAsOfIndexProbe()
		item, err := txn.Get(key)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if err := t.Buffer(int(item.ValueSize())); err != nil {
			return nil, err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		noteAsOfIndexCopy(len(raw))
		if err := t.Decoded(len(raw)); err != nil {
			return nil, err
		}
		return raw, nil
	}
	raw, err := meta(storepkg.HistoryAsOfProofKey(kind, id))
	if err != nil {
		return asOfProof{}, false, err
	}
	proof, valid, err := decodeAsOfProof(raw)
	if len(raw) != 0 {
		noteAsOfIndexDecode()
	}
	if err != nil || !valid {
		return asOfProof{}, false, err
	}
	raw, err = meta(storepkg.HistoryAsOfGenerationKey)
	if err != nil {
		return asOfProof{}, false, err
	}
	generation, err := asOfGeneration(raw)
	if len(raw) != 0 {
		noteAsOfIndexDecode()
	}
	if err != nil || generation != proof.generation {
		return asOfProof{}, false, err
	}
	return proof, true, nil
}

func (bs *Store) asOfIndexTxn(prefix []byte) (*badgerv4.Txn, vlogPin, error) {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	// Close's final flush needs wbMu. Checking here prevents creating a new
	// Badger transaction after Close stopped its timestamp oracle.
	if err := bs.checkOpen(); err != nil {
		return nil, 0, err
	}
	if bs.asOfUntrusted || len(bs.pendingByPrefix[string(prefix)]) != 0 || len(bs.flushingByPrefix[string(prefix)]) != 0 {
		return nil, 0, nil
	}
	txn, pin := bs.newReadTxn()
	return txn, pin, nil
}

func asOfIndexedHistoryRaw(txn *badgerv4.Txn, key []byte, t *storecontract.ReadTally) ([]byte, error) {
	if err := t.History(1); err != nil {
		return nil, err
	}
	item, err := txn.Get(key)
	if errors.Is(err, badgerv4.ErrKeyNotFound) {
		return nil, fmt.Errorf("%w: as-of indexed history row missing", storecontract.ErrCorruptWire)
	}
	if err != nil {
		return nil, err
	}
	return copyHistoryItem(item, t)
}

func (bs *Store) indexedNodeAsOf(id snowflake.ID, current *types.Node, pin types.Instant, t *storecontract.ReadTally) (*types.Node, bool, error) {
	var tm *types.TemporalMetadata
	if current != nil {
		tm = current.Temporal()
	}
	var row *types.Node
	fromCurrent, handled, err := bs.indexedAsOf(storepkg.KeyHistNode, id, tm, pin, t, func(txn *badgerv4.Txn, version uint64, raw []byte) error {
		w, err := bs.reconstructNodeHistoryWireWithAnchor(version, raw, func(av uint64) ([]byte, error) {
			anchor, err := asOfIndexedHistoryRaw(txn, storepkg.HistNodeKey(id, av), t)
			if err == nil {
				noteHistoryAnchorBytesCopied(len(anchor))
			}
			return anchor, err
		}, t)
		if err != nil {
			return err
		}
		row, err = bs.decodeNodeHistoryWireForKey(w, id, version)
		return err
	})
	if fromCurrent {
		row = current
	}
	return row, handled, err
}

func (bs *Store) indexedRelAsOf(id snowflake.ID, current *types.Relationship, pin types.Instant, t *storecontract.ReadTally) (*types.Relationship, bool, error) {
	var tm *types.TemporalMetadata
	if current != nil {
		tm = current.Temporal()
	}
	var row *types.Relationship
	fromCurrent, handled, err := bs.indexedAsOf(storepkg.KeyHistRel, id, tm, pin, t, func(txn *badgerv4.Txn, version uint64, raw []byte) error {
		w, err := bs.reconstructRelHistoryWireWithAnchor(version, raw, func(av uint64) ([]byte, error) {
			anchor, err := asOfIndexedHistoryRaw(txn, storepkg.HistRelKey(id, av), t)
			if err == nil {
				noteHistoryAnchorBytesCopied(len(anchor))
			}
			return anchor, err
		}, t)
		if err != nil {
			return err
		}
		row, err = bs.decodeRelHistoryWireForKey(w, id, version)
		return err
	})
	if fromCurrent {
		row = current
	}
	return row, handled, err
}
