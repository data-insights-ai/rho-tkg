package graphapply

import (
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func rangeMoveTerminal(s rangeMoveStatus) bool {
	return s.phase == rangeMoveFinish || s.phase == rangeMoveAbort && s.sequence == 0 || s.result == rangeMoveMismatch || s.result == rangeMoveLimit
}

func rangeMoveUndoControlKey(n namespace, move [16]byte, sequence uint64) []byte {
	return binary.BigEndian.AppendUint64(rangeMoveControlKey(n, move, 'U'), sequence)
}

// The undo record captures the original value of every current-page write.
// It is co-committed with that page, so restart never guesses pre-stage state.
func encodeRangeMoveUndo(q *reader, writes []raftlog.KV) ([]byte, error) {
	if len(writes) > 128 {
		return nil, errLimit
	}
	if err := reserveComposer(q, 64*len(writes)+4096); err != nil {
		return nil, err
	}
	previous := make([]raftlog.KV, len(writes))
	for i, row := range writes {
		value, present, err := rangeMoveUndoOriginal(q, row.Key)
		if err != nil {
			return nil, err
		}
		previous[i] = raftlog.KV{Key: row.Key, Value: value, Deleted: !present}
	}
	return boundedEncoding((1<<20)-2048, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'U', 1})
		w.u32(len(previous))
		for _, row := range previous {
			emitSourceKV(w, row)
		}
	})
}

// Undo interprets native tombstones as logical absence. Administrative reducer
// getBounded retains its stricter invariant; a previously aborted staging row
// is expected to be tombstoned and must remain usable by a new handoff.
func rangeMoveUndoOriginal(q *reader, key []byte) ([]byte, bool, error) {
	if err := q.ctx.Err(); err != nil {
		return nil, false, err
	}
	if q.rows >= q.limits.readRows || len(key)+64 > q.limits.readBytes-q.bytes {
		return nil, false, errLimit
	}
	if err := reserveComposer(q, 128+4*len(key)); err != nil {
		return nil, false, err
	}
	maximum := min(len(key)+(1<<20), q.limits.readBytes-q.bytes-64, q.view.ReadLimits().Bytes)
	if q.arena != nil {
		maximum = min(maximum, q.arena.Remaining()-64)
	}
	if maximum < len(key) {
		return nil, false, errLimit
	}
	q.rows++
	row, present, err := q.view.Get(q.ctx, key, maximum)
	if err != nil {
		if errors.Is(err, raftlog.ErrLimit) {
			err = errors.Join(errLimit, err)
		}
		return nil, false, err
	}
	cost := max(len(key), cap(row.Key)) + cap(row.Value) + 64
	if cost > q.limits.readBytes-q.bytes {
		return nil, false, errLimit
	}
	if err := reserveComposer(q, 64+cap(row.Key)+cap(row.Value)); err != nil {
		return nil, false, err
	}
	q.bytes += cost
	if !present || row.Deleted {
		return nil, false, nil
	}
	return row.Value, true, nil
}

func decodeRangeMoveUndo(wire []byte) ([]raftlog.KV, error) {
	body, err := graphBody(wire, "GMU\x01", (1<<20)-2048)
	if err != nil {
		return nil, err
	}
	c := graphCursor{b: body}
	n := c.count(128, 9)
	if c.err != nil {
		return nil, c.err
	}
	rows := make([]raftlog.KV, n)
	for i := range rows {
		rows[i] = readSourceKV(&c, 1<<20)
		if len(rows[i].Key) == 0 {
			return nil, errCorrupt
		}
	}
	if c.err != nil || len(c.b) != 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	return rows, nil
}

func (m *declaredMaterializer) stageRangeMoveAbort(q *reader, c rangeMoveCommand, intent rangeMoveIntent, current, status rangeMoveStatus, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if len(c.data) != 0 || current.phase >= rangeMoveDecide && current.phase != rangeMoveAbort {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	decision, present, err := crossControl(q, rangeMoveControlKey(m.ns, current.move, 'D'), 256)
	if err != nil || present {
		_ = decision
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	status.floor, status.digest = current.floor, current.digest
	status.rows, status.size = current.rows, current.size
	if m.ns.partition == intent.source.partition {
		if c.sequence != 0 || len(c.proofs) != 0 {
			return raftlog.ApplicationBatch{}, errInvalid
		}
	} else {
		proof, found := rangeMoveFindProof(c.proofs, intent.source.partition)
		if !found || proof.phase != rangeMoveAbort || proof.sequence != 0 || proof.intent != current.intent || proof.result != rangeMoveApplied {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		var progress rangeMoveProgress
		if current.phase == rangeMoveImport || current.phase == rangeMoveSeal || current.phase == rangeMoveAbort {
			wire, found, err := q.getBounded(rangeMoveProgressKey(m.ns, current.move), 1<<20)
			if err != nil || !found {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
			}
			progress, err = decodeRangeMoveProgress(wire, q.arena)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			if current.phase != rangeMoveAbort {
				progress.undoTotal = current.sequence
			}
		}
		if c.sequence == 0 {
			if progress.undoNext != progress.undoTotal {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			original, found, err := crossControl(q, rangeMoveUndoControlKey(m.ns, current.move, 0), (1<<20)-2048)
			if err != nil || !found {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
			}
			rows, err := decodeRangeMoveUndo(original.Value)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			for _, row := range rows {
				if err := q.put(row.Key, row.Value); err != nil {
					return raftlog.ApplicationBatch{}, err
				}
				q.writes[len(q.writes)-1].Deleted = row.Deleted
			}
		} else {
			if c.sequence != progress.undoNext+1 || progress.undoNext >= progress.undoTotal {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			original := progress.undoTotal - progress.undoNext
			record, found, err := crossControl(q, rangeMoveUndoControlKey(m.ns, current.move, original), (1<<20)-2048)
			if err != nil || !found {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
			}
			if err := reserveComposer(q, 4096+4*len(record.Value)); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			rows, err := decodeRangeMoveUndo(record.Value)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			for _, row := range rows {
				if err := q.put(row.Key, row.Value); err != nil {
					return raftlog.ApplicationBatch{}, err
				}
				q.writes[len(q.writes)-1].Deleted = row.Deleted
			}
			progress.undoNext++
			wire, err := encodeRangeMoveProgress(progress, q.arena)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			if err := q.put(rangeMoveProgressKey(m.ns, current.move), wire); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}
	}
	if c.sequence == 0 {
		if err := q.put(rangeMoveActiveKey(m.ns), nil); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	return m.finishMovePage(q, status, nil, budget)
}
