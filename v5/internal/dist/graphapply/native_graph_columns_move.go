package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// A pooled phase's immutable GM/V survives a later current Finish and raw
// Outcome/CDC collection. The key is derived from the canonical result; no
// caller tuple, current-status substitution or completed absence is authority.
func pooledMoveOriginalMatches(row raftlog.ApplicationControlRecord, status rangeMoveStatus, wire []byte) bool {
	return row.Index == status.index && len(row.Value) == 32+len(wire) && !bytes.Equal(row.Value[:32], make([]byte, 32)) && bytes.Equal(row.Value[32:], wire) && (status.result != rangeMoveBudgetLimit || bytes.Equal(row.Value[:32], status.failedCommand[:]))
}

func readPooledMoveOriginal(q *reader, scope allocationScope, status rangeMoveStatus) (raftlog.ApplicationControlRecord, error) {
	if q == nil || scope.semantic != rangeMovePooledSemanticContractID() || status.format != 3 || status.ns != q.ns || status.index == 0 || (namespace{scope.graph, scope.partition}) != q.ns {
		return raftlog.ApplicationControlRecord{}, errInvalid
	}
	i, found, err := crossControl(q, rangeMoveControlKey(q.ns, status.move, 'I'), 1024)
	if err != nil || !found {
		return raftlog.ApplicationControlRecord{}, errors.Join(errCorrupt, err)
	}
	intent, err := decodeRangeMoveIntent(i.Value)
	if err != nil || intent.source.semantic != scope.semantic || intent.source != scope && intent.target != scope || rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id) != status.move || sha256.Sum256(i.Value) != status.intent || i.Index > status.index {
		return raftlog.ApplicationControlRecord{}, errors.Join(errCorrupt, err)
	}
	phase, sequence := status.phase, status.sequence
	if status.result == rangeMoveBudgetLimit {
		phase, sequence = status.failedPhase, status.failedSequence
	}
	credit := rangeMoveCredit(rangeMoveCommand{scope: scope, phase: phase, sequence: sequence})
	if credit != 0 {
		v, present, err := crossControl(q, movePoolPhaseKey(q.ns, status.move, credit), 288)
		if err != nil {
			return v, err
		}
		if present {
			wire, err := encodeRangeMoveStatus(status)
			if err != nil || !pooledMoveOriginalMatches(v, status, wire) {
				return v, errors.Join(errCorrupt, err)
			}
			return v, nil
		}
	}
	if i.Index != status.index || status.phase != rangeMoveFence && status.phase != rangeMoveAbort {
		return raftlog.ApplicationControlRecord{}, errCorrupt
	}
	return i, nil
}

// Only the pooled agreement interprets GMO3. Movement changes native physical
// ownership, not business cells: its genuine GML is preserved as lineage and
// never decoded as a GCD or silently replaced by empty CDC.
func (s *nativeColumnSource) moveMetadataFrame(ctx context.Context, group int, out nativeColumnFrame) (nativeColumnFrame, error) {
	h, view := s.hosts[group], s.views[group]
	if h == nil || h.machine == nil || view == nil || s.binding.Version != 2 || h.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || out.Index < 2 || len(out.Rows) != 0 {
		return out, errInvalid
	}
	status, err := decodeRangeMoveStatus(out.Outcome)
	if err != nil || status.format != 3 || status.ns != h.machine.ns || status.index > out.Index {
		return out, errors.Join(errCorrupt, err)
	}
	arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if err != nil {
		return out, err
	}
	q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: s.roots[group], limits: h.machine.limits.allocation, arena: arena}
	intentRow, found, err := crossControl(&q, rangeMoveControlKey(q.ns, status.move, 'I'), 1024)
	if err != nil {
		return out, err
	}
	if !found {
		// An actual blocked Fence observation has no installed intent or owner.
		// This local nonterminal observation supplies no original-result proof.
		if status.index != out.Index || status.phase != rangeMoveFence || status.result != rangeMoveBlocked && status.result != rangeMoveMismatch || len(out.CDC) != 0 {
			return out, errCorrupt
		}
		return s.moveUnchangedFrame(ctx, group, out)
	}
	intent, err := decodeRangeMoveIntent(intentRow.Value)
	if err != nil || intent.source.semantic != rangeMovePooledSemanticContractID() || sha256.Sum256(intentRow.Value) != status.intent || rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id) != status.move || intent.source != h.machine.scope() && intent.target != h.machine.scope() || intentRow.Index > status.index {
		return out, errors.Join(errCorrupt, err)
	}
	phase, sequence := status.phase, status.sequence
	if status.result == rangeMoveBudgetLimit {
		phase, sequence = status.failedPhase, status.failedSequence
	}
	command := rangeMoveCommand{scope: h.machine.scope(), phase: phase, sequence: sequence}
	credit := rangeMoveCredit(command)
	var original raftlog.ApplicationControlRecord
	if credit != 0 {
		original, found, err = crossControl(&q, movePoolPhaseKey(q.ns, status.move, credit), 288)
		if err != nil {
			return out, err
		}
	}
	if found && credit != 0 {
		if !pooledMoveOriginalMatches(original, status, out.Outcome) {
			return out, errCorrupt
		}
	} else {
		// Fence and an unadmitted terminal refusal use their actual immutable I
		// control rather than a credit that was never consumed.
		if intentRow.Index != status.index || status.phase != rangeMoveFence && status.phase != rangeMoveAbort {
			return out, errCorrupt
		}
		original = intentRow
	}
	var decision raftlog.ApplicationControlRecord
	if status.index < out.Index {
		if len(out.CDC) != 0 {
			return out, errCorrupt
		}
		if err := s.replay(ctx, group, &out, status.index); err != nil {
			return out, err
		}
		if !bytes.Equal(out.OriginalOutcome, out.Outcome) {
			return out, errCorrupt
		}
	}
	changes := out.CDC
	if status.index < out.Index {
		changes = out.OriginalCDC
	}
	if len(changes) > 0 {
		if status.phase != rangeMoveActivate || status.result != rangeMoveApplied || !bytes.HasPrefix(changes, []byte("GML\x01")) {
			return out, errCorrupt
		}
		decision, found, err = crossControl(&q, rangeMoveControlKey(q.ns, status.move, 'D'), 256)
		if err != nil || !found || decision.Index > status.index {
			return out, errors.Join(errCorrupt, err)
		}
		root, err := view.RootAt(ctx, status.index, 172)
		if err != nil {
			return out, err
		}
		frame := retainedFeedFrame{partition: out.Partition, index: status.index, outcome: out.Outcome, changes: changes, intent: intentRow.Value, decision: decision.Value, round: status.round}
		copy(frame.root[:], root.Image)
		if _, err := h.machine.feedEnvelope(frame, arena); err != nil {
			return out, err
		}
		mapped, present, err := q.getBounded(partitionRoundMapKey(q.ns, status.round), 256)
		if err != nil || !present {
			return out, errors.Join(errCorrupt, err)
		}
		body, err := graphBody(mapped, "PLM\x01", 256)
		if err != nil || len(body) != 220 || binary.BigEndian.Uint64(body[32:40]) != status.round || binary.BigEndian.Uint64(body[40:48]) != status.index || !bytes.Equal(body[48:], root.Image) {
			return out, errors.Join(errCorrupt, err)
		}
	} else if status.phase == rangeMoveActivate && status.result == rangeMoveApplied {
		return out, errCorrupt
	}
	if status.index == out.Index && len(out.CDC) == 0 || status.index < out.Index {
		out, err = s.moveUnchangedFrame(ctx, group, out)
		if err != nil {
			return out, err
		}
	}
	// NCM1 is fresh evidence inside the existing bounded Registration field.
	// It is descriptive; only the genuine view above authenticated the controls.
	if err := reserveComposer(&q, 4096+4*(len(intentRow.Value)+len(original.Value)+len(decision.Value))); err != nil {
		return out, err
	}
	out.Registration, err = boundedEncoding(s.binding.Policy.FrameBytes, func(w *boundedWriter) {
		w.add([]byte("NCM1"))
		for _, row := range []raftlog.ApplicationControlRecord{intentRow, original, decision} {
			w.field(row.Key)
			w.u64(row.Index)
			w.field(row.Value)
		}
	})
	return out, err
}

func (s *nativeColumnSource) moveUnchangedFrame(ctx context.Context, group int, out nativeColumnFrame) (nativeColumnFrame, error) {
	previous, err := s.views[group].RootAt(ctx, out.Index-1, 172)
	if err != nil || !bytes.Equal(previous.Image, out.Root[:]) {
		return out, errors.Join(errCorrupt, err)
	}
	return out, nil
}
