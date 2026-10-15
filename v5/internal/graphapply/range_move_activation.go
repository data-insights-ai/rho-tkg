package graphapply

import (
	"bytes"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func (m *declaredMaterializer) stageRangeMoveDecision(q *reader, c rangeMoveCommand, intent rangeMoveIntent, current, status rangeMoveStatus, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if m.ns.partition != intent.source.partition || current.phase != rangeMoveDescribe || c.sequence != 0 || len(c.data) != 0 {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	wire, present, err := q.getBounded(rangeMoveProgressKey(m.ns, current.move), 1<<20)
	if err != nil || !present {
		return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
	}
	progress, err := decodeRangeMoveProgress(wire, q.arena)
	if err != nil || !progress.transferComplete() {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	rows, size, digest := progress.transferSummary()
	target, found := rangeMoveFindProof(c.proofs, intent.target.partition)
	if !found || target.phase != rangeMoveSeal || target.intent != current.intent || target.digest != digest || target.rows != rows || target.size != size {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	installed, err := readPartitionRoundFloor(q, intent.publication.Configuration)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	floor, err := m.allocationFloor(q, intent.publication.Configuration, installed)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	status.floor = max(current.floor, target.floor, floor)
	if status.floor >= math.MaxUint64-1 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	status.round = status.floor + 1
	status.rows, status.size, status.digest = rows, size, digest
	decision, err := encodeRangeMoveStatus(status)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return m.finishMovePage(q, status, []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, current.move, 'D'), Value: decision}}, budget)
}

func (m *declaredMaterializer) stageRangeMoveActivation(q *reader, c rangeMoveCommand, intent rangeMoveIntent, current, status rangeMoveStatus, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	wire, present, err := q.getBounded(rangeMoveProgressKey(m.ns, current.move), 1<<20)
	if err != nil || !present {
		return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
	}
	progress, err := decodeRangeMoveProgress(wire, q.arena)
	if err != nil || !progress.transferComplete() {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	_, _, digest := progress.transferSummary()
	if m.collectsHistory() {
		if e := m.hydrateMoveRetention(q, current.move, &progress); e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		packet, e := decodeMoveRetainedLineage(progress.retention, q.arena)
		if e != nil || packet.scope != intent.source || packet.move != current.move || packet.intent != current.intent {
			return raftlog.ApplicationBatch{}, errors.Join(errInvalid, e)
		}
		if m.scope() == intent.target {
			shadow, present, e := readMoveRetainedLineage(q, current.move, true)
			if e != nil || !present {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, e)
			}
			wire, e := encodeMoveRetainedLineage(shadow, q.arena)
			if e != nil || !bytes.Equal(wire, progress.retention) {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, e)
			}
		}
	}
	decision, err := rangeMoveSourceDecision(c, intent, digest)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if m.ns.partition == intent.source.partition {
		if current.phase != rangeMoveDecide || current.round != decision.round {
			return raftlog.ApplicationBatch{}, errInvalid
		}
	} else if current.phase != rangeMoveSeal || progress.imported == nil || !progress.imported.Sealed {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	status.round, status.floor, status.digest = decision.round, max(current.floor, decision.floor), decision.digest
	status.rows, status.size, status.sequence = current.rows, current.size, current.sequence
	var root graphstore.Root
	if m.ns.partition == intent.target.partition {
		headroom, err := m.moveBudget(q)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		effects, work, err := graphstore.ActivateRangeMoveImport(q.ctx, q.view, progress.descriptor, *progress.imported, m.limits.catalog, m.limits.graph, headroom, q.arena)
		if charged := chargeGraphWork(q, work); charged != nil {
			return raftlog.ApplicationBatch{}, errors.Join(charged, err)
		}
		if err != nil || !sameBase(q.base, effects.Base) {
			return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
		}
		for _, row := range effects.Writes {
			if err := q.put(row.Key, row.Value); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}
		root = effects.Root
	} else {
		root, err = graphstore.DecodeRoot(q.base.Image)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		placement, err := graphstore.EncodeRangeOwnership(progress.descriptor.Spec.To)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := q.put(graphstore.RangeOwnershipKey(root.Namespace(), intent.publication.RangeID), placement); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	// Placement changes belong to the logical round prefix. The root is published
	// only with complete staged content and the immutable handoff COMMIT.
	logical, err := boundedEncoding(2048, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'L', 1})
		w.add(current.move[:])
		w.add(current.intent[:])
		w.u64(intent.publication.RangeID)
		w.u64(intent.source.partition)
		w.u64(intent.target.partition)
		w.u64(intent.epoch)
		w.u64(intent.epoch + 1)
		w.u64(decision.round)
		w.add(decision.digest[:])
	})
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	root, err = root.AdvanceEffects(graphEffectDigest(root.EffectDigest(), logical))
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	image, err := graphstore.EncodeRoot(root)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := writePartitionRoundFloor(q, intent.publication.Configuration, decision.round); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	outcome, err := encodeRangeMoveStatus(status)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := q.put(rangeMoveStatusKey(m.ns, current.move), outcome); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	batch := batchAt(q.base)
	batch.Writes, batch.Image, batch.Outcome, batch.Changes = q.writes, image, outcome, logical
	if m.ns.partition == intent.target.partition {
		original, err := encodeRangeMoveStatus(decision)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		batch.ControlPuts = []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, current.move, 'D'), Value: original}}
	}
	return m.finishPartitionRoundMap(q, batch, intent.publication.Configuration, decision.round, status.index, budget, 4096)
}

func (m *declaredMaterializer) stageRangeMoveFinish(q *reader, c rangeMoveCommand, intent rangeMoveIntent, current, status rangeMoveStatus, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if current.phase != rangeMoveActivate || c.sequence != 0 || len(c.data) != 0 || len(c.proofs) != 2 {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	for _, partition := range []uint64{intent.source.partition, intent.target.partition} {
		proof, found := rangeMoveFindProof(c.proofs, partition)
		if !found || proof.intent != current.intent || proof.round != current.round || proof.digest != current.digest || proof.phase != rangeMoveActivate && proof.phase != rangeMoveFinish {
			return raftlog.ApplicationBatch{}, errInvalid
		}
	}
	status.floor, status.round, status.digest = current.floor, current.round, current.digest
	status.rows, status.size = current.rows, current.size
	if err := q.put(rangeMoveActiveKey(m.ns), nil); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return m.finishMovePage(q, status, nil, budget)
}
