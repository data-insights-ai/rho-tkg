package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func rangeMoveFindProof(proofs []rangeMoveStatus, partition uint64) (rangeMoveStatus, bool) {
	for _, proof := range proofs {
		if proof.ns.partition == partition {
			return proof, true
		}
	}
	return rangeMoveStatus{}, false
}

func (m *declaredMaterializer) stageRangeMoveTarget(q *reader, c rangeMoveCommand, intent rangeMoveIntent, current, status rangeMoveStatus, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if m.ns.partition != intent.target.partition {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	headroom, err := m.moveBudget(q)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	var progress rangeMoveProgress
	if c.sequence == 0 && c.phase == rangeMoveImport {
		if current.phase != rangeMoveCapture {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		manifest, err := decodeRangeMoveProgress(c.data, q.arena)
		descriptor := manifest.descriptor
		if err != nil || !manifest.transferComplete() || manifest.imported != nil || descriptor.Spec.Publication != intent.publication || descriptor.Spec.From.Partition != intent.source.partition || descriptor.Spec.From.Epoch != intent.epoch || descriptor.Spec.To.Partition != intent.target.partition || descriptor.Spec.To.Epoch != intent.epoch+1 || descriptor.Declaration != m.declaration.Digest() || descriptor.Target.Binding != m.binding {
			return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
		}
		rows, size, digest := manifest.transferSummary()
		source, found := rangeMoveFindProof(c.proofs, intent.source.partition)
		if !found || source.phase != rangeMoveDescribe || source.intent != current.intent || source.rows != rows || source.size != size || source.digest != digest {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		capture, found, err := crossControl(q, rangeMoveControlKey(m.ns, current.move, 'C'), 1024)
		if err != nil || !found {
			return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
		}
		root, floor, err := moveCaptureRoot(capture.Value, intent.target)
		if err != nil || descriptor.Target.Index != root.Index || descriptor.Target.ImageHash != root.ImageHash || !bytes.Equal(descriptor.Target.Image[:], root.Image) {
			return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
		}
		state, work, err := graphstore.BeginRangeMoveImport(q.ctx, q.view, descriptor, m.limits.catalog, m.limits.graph, headroom, q.arena)
		if charged := chargeGraphWork(q, work); charged != nil {
			return raftlog.ApplicationBatch{}, errors.Join(charged, err)
		}
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		progress = rangeMoveProgress{descriptor: descriptor, imported: new(state), graphPages: manifest.graphPages, replay: rangeMoveReplayStart(), replayEnd: manifest.replay}
		status.floor, status.digest = max(source.floor, floor), digest
	} else {
		wire, present, err := q.getBounded(rangeMoveProgressKey(m.ns, current.move), 1<<20)
		if err != nil || !present {
			return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
		}
		progress, err = decodeRangeMoveProgress(wire, q.arena)
		if err != nil || progress.imported == nil {
			return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
		}
		status.floor, status.digest, status.sequence = current.floor, current.digest, current.sequence
		switch c.phase {
		case rangeMoveImport:
			if current.phase != rangeMoveImport || c.sequence != current.sequence+1 || c.sequence > uint64(intent.maxPages) || len(c.data) == 0 { // #nosec G115 -- decoded valid intent admits maxPages1..1024.
				return raftlog.ApplicationBatch{}, errInvalid
			}
			var effects graphstore.RangeMovePageEffects
			var work graphstore.PageWork
			var replayWrites []raftlog.KV
			if c.data[0] == 0 && len(c.data) == 1 {
				effects, work, err = graphstore.AdvanceRangeMoveImportPreparation(q.ctx, q.view, progress.descriptor, *progress.imported, m.limits.catalog, m.limits.graph, headroom, q.arena)
			} else if c.data[0] == 1 {
				page, e := graphstore.DecodeRangeMovePage(c.data[1:], q.arena)
				if e != nil {
					return raftlog.ApplicationBatch{}, e
				}
				effects, work, err = graphstore.StageRangeMovePage(q.ctx, q.view, progress.descriptor, *progress.imported, page, m.limits.catalog, m.limits.graph, headroom, q.arena)
			} else if c.data[0] == 2 {
				if !progress.imported.Complete || progress.replay.complete {
					return raftlog.ApplicationBatch{}, errInvalid
				}
				page, e := decodeRangeMoveReplayPage(c.data[1:], q.arena)
				if e != nil {
					return raftlog.ApplicationBatch{}, e
				}
				if !rangeMoveReplaySame(page.before, progress.replay) {
					return raftlog.ApplicationBatch{}, errInvalid
				}
				start := len(q.writes)
				err = stageRangeMoveReplayPage(q, namespace{intent.source.graph, intent.source.partition}, m.ns, current.move, page)
				replayWrites = q.writes[start:]
				if err == nil {
					progress.replay = page.next
				}
				effects.Base, effects.Next = q.base, *progress.imported
			} else {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			if charged := chargeGraphWork(q, work); charged != nil {
				return raftlog.ApplicationBatch{}, errors.Join(charged, err)
			}
			if err != nil {
				return raftlog.ApplicationBatch{}, fmt.Errorf("range move import step %d: %w", c.sequence, err)
			}
			if !sameBase(q.base, effects.Base) {
				return raftlog.ApplicationBatch{}, fmt.Errorf("range move import base mismatch current=%d/%d page=%d/%d: %w", q.base.Generation, q.base.Index, effects.Base.Generation, effects.Base.Index, errInvalid)
			}
			for _, row := range effects.Writes {
				if row.Deleted || len(row.Key) == 0 {
					return raftlog.ApplicationBatch{}, errCorrupt
				}
				if err := q.put(row.Key, row.Value); err != nil {
					return raftlog.ApplicationBatch{}, err
				}
			}
			undoWrites := effects.Writes
			if c.data[0] == 2 {
				undoWrites = replayWrites
			}
			undo, err := encodeRangeMoveUndo(q, undoWrites)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			progress.imported = new(effects.Next)
			status.sequence, status.rows, status.size = c.sequence, effects.Next.Rows+progress.replay.rows, effects.Next.Bytes+progress.replay.bytes
			encoded, err := encodeRangeMoveProgress(progress, q.arena)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			if err := q.put(rangeMoveProgressKey(m.ns, current.move), encoded); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			return m.finishMovePage(q, status, []raftlog.ApplicationControlPut{{Key: rangeMoveUndoControlKey(m.ns, current.move, c.sequence), Value: undo}}, budget)
		case rangeMoveSeal:
			if current.phase != rangeMoveImport || c.sequence != 0 || len(c.data) != 0 || !rangeMoveReplaySame(progress.replay, progress.replayEnd) || !progress.replay.complete {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			state, work, err := graphstore.FinishRangeMoveImport(q.ctx, q.view, progress.descriptor, *progress.imported, m.limits.catalog, m.limits.graph, headroom, q.arena)
			if charged := chargeGraphWork(q, work); charged != nil {
				return raftlog.ApplicationBatch{}, errors.Join(charged, err)
			}
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			progress.imported = new(state)
			status.rows, status.size = state.Rows+progress.replay.rows, state.Bytes+progress.replay.bytes
		default:
			return raftlog.ApplicationBatch{}, errInvalid
		}
	}
	encoded, err := encodeRangeMoveProgress(progress, q.arena)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := q.put(rangeMoveProgressKey(m.ns, current.move), encoded); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return m.finishMovePage(q, status, nil, budget)
}

func rangeMoveSourceDecision(c rangeMoveCommand, intent rangeMoveIntent, digest [32]byte) (rangeMoveStatus, error) {
	decision, present := rangeMoveFindProof(c.proofs, intent.source.partition)
	if !present || decision.intent != sha256.Sum256(c.intent) || decision.phase < rangeMoveDecide || decision.phase == rangeMoveAbort || decision.round == 0 || decision.digest != digest {
		return rangeMoveStatus{}, errInvalid
	}
	return decision, nil
}
