package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func rangeMovePageControlKey(n namespace, move [16]byte, sequence uint64) []byte {
	return binary.BigEndian.AppendUint64(rangeMoveControlKey(n, move, 'P'), sequence)
}

func (m *declaredMaterializer) moveBudget(q *reader) (graphstore.OwnershipBudget, error) {
	b := graphstore.OwnershipBudget{SourceRows: m.limits.sourceRows - q.rows, SourceBytes: m.limits.sourceBytes - q.bytes, OutputBytes: q.arena.Remaining()}
	if b.SourceRows < 1 || b.SourceBytes < 1 || b.OutputBytes < 1 {
		return b, errLimit
	}
	return b, nil
}

func (m *declaredMaterializer) finishMovePage(q *reader, status rangeMoveStatus, controls []raftlog.ApplicationControlPut, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	wire, err := encodeRangeMoveStatus(status)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := q.put(rangeMoveStatusKey(m.ns, status.move), wire); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	batch := batchAt(q.base)
	batch.Writes, batch.ControlPuts, batch.Outcome = q.writes, controls, wire
	if err := sortPartitionWrites(batch.Writes); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return batch, m.preflight(batch, budget)
}

func (m *declaredMaterializer) stageRangeMoveContinuation(q *reader, command rangeMoveCommand, intent rangeMoveIntent, current, status rangeMoveStatus, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	move := status.move
	if command.phase == rangeMoveAbort {
		return m.stageRangeMoveAbort(q, command, intent, current, status, budget)
	}
	if command.phase == rangeMoveImport || command.phase == rangeMoveSeal {
		return m.stageRangeMoveTarget(q, command, intent, current, status, budget)
	}
	if command.phase == rangeMoveDecide {
		return m.stageRangeMoveDecision(q, command, intent, current, status, budget)
	}
	if command.phase == rangeMoveActivate {
		return m.stageRangeMoveActivation(q, command, intent, current, status, budget)
	}
	if command.phase == rangeMoveFinish {
		return m.stageRangeMoveFinish(q, command, intent, current, status, budget)
	}
	if command.phase == rangeMoveDescribe {
		if m.ns.partition != intent.source.partition {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		var progress rangeMoveProgress
		if command.sequence == 0 {
			if current.phase != rangeMoveCapture || len(command.data) == 0 || len(command.proofs) != 1 || command.proofs[0].phase != rangeMoveCapture || command.proofs[0].ns != (namespace{intent.target.graph, intent.target.partition}) || command.proofs[0].intent != current.intent {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			descriptor, err := graphstore.DecodeRangeMoveDescriptor(command.data, q.arena)
			if err != nil || descriptor.Sealed || descriptor.Spec.Publication != intent.publication || descriptor.Spec.From.Partition != intent.source.partition || descriptor.Spec.From.Epoch != intent.epoch || descriptor.Spec.To.Partition != intent.target.partition || descriptor.Spec.To.Epoch != intent.epoch+1 || descriptor.Declaration != m.declaration.Digest() || descriptor.Source.Binding != m.binding {
				return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
			}
			capture, found, err := crossControl(q, rangeMoveControlKey(m.ns, move, 'C'), 1024)
			if err != nil || !found {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
			}
			root, floor, err := moveCaptureRoot(capture.Value, intent.source)
			if err != nil || descriptor.Source.Index != root.Index || descriptor.Source.ImageHash != root.ImageHash || !bytes.Equal(descriptor.Source.Image[:], root.Image) {
				return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
			}
			targetCapture, err := boundedEncoding(1024, func(w *boundedWriter) {
				w.add([]byte{'G', 'M', 'C', 1})
				w.add(appendAllocationScope(nil, intent.target))
				w.u64(command.proofs[0].floor)
				emitSourceRoot(w, raftlog.ApplicationRoot{Index: descriptor.Target.Index, Image: descriptor.Target.Image[:], ImageHash: descriptor.Target.ImageHash})
			})
			if err != nil || sha256.Sum256(targetCapture) != command.proofs[0].digest {
				return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
			}
			progress.descriptor = descriptor
			if m.collectsHistory() {
				packet, present, e := readMoveRetainedLineage(q, move, false)
				if e != nil || !present {
					return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, e)
				}
				if e := checkMoveRetainedBinding(packet, intent, move, current.intent, root, floor, true); e != nil {
					return raftlog.ApplicationBatch{}, e
				}
				progress.retention, e = encodeMoveRetainedLineage(packet, q.arena)
				if e != nil {
					return raftlog.ApplicationBatch{}, e
				}
				progress.retentionRows = uint64(len(packet.owners) + 1)
			}
			status.floor = max(floor, command.proofs[0].floor)
		} else {
			if current.phase != rangeMoveDescribe || command.sequence != current.sequence+1 || len(command.data) != 0 {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			wire, present, err := q.getBounded(rangeMoveProgressKey(m.ns, move), 1<<20)
			if err != nil || !present {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
			}
			progress, err = decodeRangeMoveProgress(wire, q.arena)
			if err != nil || progress.transferComplete() || progress.imported != nil {
				return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
			}

			var before []byte
			if !progress.descriptor.Sealed {
				page, err := m.exportMoveGraphPage(q, progress, progress.cursor, intent)
				if err != nil {
					return raftlog.ApplicationBatch{}, err
				}
				cursor, err := graphstore.EncodeRangeMoveCursor(page.Before, q.arena)
				if err != nil {
					return raftlog.ApplicationBatch{}, err
				}
				before = append([]byte{1}, cursor...)
				progress.cursor = page.Next
				progress.graphPages = page.Next.Sequence
				if page.Complete {
					progress.descriptor, err = graphstore.SealRangeMoveDescriptor(progress.descriptor, page.Next, q.arena)
					if err != nil {
						return raftlog.ApplicationBatch{}, err
					}
					progress.replay = rangeMoveReplayStart()
				}
			} else {
				if rangeMoveSpans(m.SemanticContractID()) {
					batch, e := m.exportMoveBatch(q, progress, intent)
					if e != nil {
						return raftlog.ApplicationBatch{}, e
					}
					cursor, e := encodeRangeMoveReplayCursor(batch.pages[0].before, q.arena)
					if e != nil {
						return raftlog.ApplicationBatch{}, e
					}
					before = append([]byte{3}, cursor...)
					progress.replay = batch.pages[len(batch.pages)-1].next
					progress.replayPages++
				} else {
					page, err := m.exportMoveReplay(q, progress, progress.replay, intent)
					if err != nil {
						return raftlog.ApplicationBatch{}, err
					}
					cursor, err := encodeRangeMoveReplayCursor(page.before, q.arena)
					if err != nil {
						return raftlog.ApplicationBatch{}, err
					}
					before = append([]byte{2}, cursor...)
					progress.replay = page.next
				}
			}
			status.floor = current.floor
			status.rows, status.size, status.digest = progress.transferSummary()

			encoded, err := encodeRangeMoveProgress(progress, q.arena)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			if err := q.put(rangeMoveProgressKey(m.ns, move), encoded); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			controls := []raftlog.ApplicationControlPut{{Key: rangeMovePageControlKey(m.ns, move, command.sequence), Value: before}}
			return m.finishMovePage(q, status, controls, budget)
		}
		encoded, err := encodeRangeMoveProgress(progress, q.arena)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := q.put(rangeMoveProgressKey(m.ns, move), encoded); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return m.finishMovePage(q, status, nil, budget)
	}
	return raftlog.ApplicationBatch{}, errInvalid
}

func (m *declaredMaterializer) exportMoveGraphPage(q *reader, progress rangeMoveProgress, before graphstore.RangeMoveCursor, intent rangeMoveIntent) (page graphstore.RangeMovePage, err error) {
	limits := m.limits.graph
	limits.MaxSourceRows = min(limits.MaxSourceRows, q.limits.readRows-q.rows)
	limits.MaxSourceBytes = min(limits.MaxSourceBytes, q.limits.readBytes-q.bytes)
	if limits.MaxSourceRows < 1 || limits.MaxSourceBytes < 1 {
		return page, errLimit
	}
	if err := reserveComposer(q, 2048); err != nil {
		return page, err
	}
	view, err := m.store.ApplicationView(progress.descriptor.Source.Index)
	if err != nil {
		return page, err
	}
	page, work, err := graphstore.ExportRangeMovePage(q.ctx, view, progress.descriptor, before, raftlog.ReadBudget{Rows: intent.pageRows, Bytes: intent.pageBytes}, m.limits.catalog, limits, q.arena)
	err = errors.Join(err, view.Close())
	return page, errors.Join(err, chargeGraphWork(q, work))
}
func (m *declaredMaterializer) exportMoveReplay(q *reader, progress rangeMoveProgress, before rangeMoveReplayCursor, intent rangeMoveIntent) (page rangeMoveReplayPage, err error) {
	if err := reserveComposer(q, 2048); err != nil {
		return page, err
	}
	view, err := m.store.ApplicationView(progress.descriptor.Source.Index)
	if err != nil {
		return page, err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	root, err := view.RootBounded(q.ctx, 172)
	if err != nil {
		return page, err
	}
	if root.Index != progress.descriptor.Source.Index || root.ImageHash != progress.descriptor.Source.ImageHash || !bytes.Equal(root.Image, progress.descriptor.Source.Image[:]) {
		return page, errCorrupt
	}
	frozen := *q
	frozen.view, frozen.base = view, root
	frozen.rows++
	frozen.bytes += cap(root.Image) + 1024
	defer func() { q.rows, q.bytes = frozen.rows, frozen.bytes }()
	return exportRangeMoveReplayPage(&frozen, m.ns, before, raftlog.ReadBudget{Rows: intent.pageRows, Bytes: intent.pageBytes})
}
