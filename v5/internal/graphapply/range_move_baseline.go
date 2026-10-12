package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// A fenced target may have future-tree nodes in current MVCC storage while its
// published image still describes the complete pre-handoff catalog. Validate
// that complete baseline at its real retained coordinate, and separately check
// the current progress/recovery state. This is never a fallback from corruption
// or a source for a public fresh certificate/current graph read.
func (m *declaredMaterializer) rangeMoveCatalogBaseline(q *reader, cfg genesisAllocationConfig) (view *raftlog.ApplicationView, err error) {
	if !m.rangeMoves() {
		return nil, nil
	}
	move, err := readRangeMoveActive(q)
	if err != nil || move == ([16]byte{}) {
		return nil, err
	}
	status, present, err := readRangeMoveStatus(q, move)
	if err != nil || !present || status.result != rangeMoveApplied || rangeMoveTerminal(status) {
		return nil, errors.Join(errCorrupt, err)
	}
	intentRecord, present, err := crossControl(q, rangeMoveControlKey(q.ns, move, 'I'), 1024)
	if err != nil || !present || sha256.Sum256(intentRecord.Value) != status.intent {
		return nil, errors.Join(errCorrupt, err)
	}
	intent, err := decodeRangeMoveIntent(intentRecord.Value)
	if err != nil || intent.source.check(m.declaration, cfg) != nil || intent.target.check(m.declaration, cfg) != nil {
		return nil, errors.Join(errCorrupt, err)
	}
	if m.scope() != intent.target {
		return nil, nil
	}
	if status.phase != rangeMoveImport && status.phase != rangeMoveSeal && (status.phase != rangeMoveAbort || status.sequence == 0) {
		return nil, nil
	}
	configuration, err := cfg.digest()
	if err != nil {
		return nil, err
	}
	if q.arena == nil {
		// Intent/status/capture framing is fixed. Admit the shared parent before
		// copying variable-size durable import progress or opening another view.
		if err := joinGraphOutputBudget(q, graphRequest{}, 8192, m.limits); err != nil {
			return nil, err
		}
	}
	progressWire, present, err := q.getBounded(rangeMoveProgressKey(q.ns, move), 1<<20)
	if err != nil || !present {
		return nil, errors.Join(errCorrupt, err)
	}
	progress, err := decodeRangeMoveProgress(progressWire, q.arena)
	if err != nil || progress.imported == nil || !progress.descriptor.Sealed {
		return nil, errors.Join(errCorrupt, err)
	}
	d, s := progress.descriptor, progress.imported
	_, _, manifestDigest := progress.transferSummary()
	if d.Spec.Publication != intent.publication || d.Spec.From.Partition != intent.source.partition || d.Spec.From.Epoch != intent.epoch || d.Spec.To.Partition != intent.target.partition || d.Spec.To.Epoch != intent.epoch+1 || d.Configuration != configuration || d.Declaration != m.declaration.Digest() || d.DefaultAxis != cfg.defaultAxis || d.Target.Binding != m.binding || s.FutureRoot.Namespace() != (graphstore.Namespace{Graph: intent.publication.Graph, Partition: m.ns.partition}) || s.Rows+progress.replay.rows != status.rows || s.Bytes+progress.replay.bytes != status.size || status.digest != manifestDigest {
		return nil, errCorrupt
	}
	completed := s.Sequence + s.PreparationSteps + progress.replay.sequence
	if status.phase == rangeMoveAbort {
		if progress.undoTotal != completed || progress.undoNext != status.sequence || progress.undoNext > progress.undoTotal {
			return nil, errCorrupt
		}
	} else if completed != status.sequence || progress.undoNext != 0 || progress.undoTotal != 0 {
		return nil, errCorrupt
	}
	if status.phase == rangeMoveSeal && (!s.Complete || !s.Sealed || !rangeMoveReplaySame(progress.replay, progress.replayEnd)) {
		return nil, errCorrupt
	}
	capture, present, err := crossControl(q, rangeMoveControlKey(q.ns, move, 'C'), 1024)
	if err != nil || !present {
		return nil, errors.Join(errCorrupt, err)
	}
	root, _, err := moveCaptureRoot(capture.Value, intent.target)
	if err != nil || d.Target.Index != root.Index || d.Target.ImageHash != root.ImageHash || !bytes.Equal(d.Target.Image[:], root.Image) || q.base.ImageHash != root.ImageHash || !bytes.Equal(q.base.Image, root.Image) {
		return nil, errors.Join(errCorrupt, err)
	}
	if err := m.checkRangeMoveState(q); err != nil {
		return nil, err
	}
	if err := chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 1024}); err != nil {
		return nil, err
	}
	if err := reserveComposer(q, 2048); err != nil {
		return nil, err
	}
	view, err = m.store.ApplicationView(root.Index)
	if err != nil {
		return nil, err
	}
	actual, e := view.RootBounded(q.ctx, 172)
	if e == nil && (actual.Index != root.Index || actual.ImageHash != root.ImageHash || !bytes.Equal(actual.Image, root.Image)) {
		e = errCorrupt
	}
	if e == nil {
		binding, be := view.ApplicationBinding(q.ctx)
		e = be
		if e == nil && binding != m.binding {
			e = errCorrupt
		}
	}
	if e != nil {
		return nil, errors.Join(e, view.Close())
	}
	return view, nil
}
