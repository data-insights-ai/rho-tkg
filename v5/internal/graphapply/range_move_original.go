package graphapply

import (
	"bytes"
	"context"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// Frozen shadows are original data, not placement or quorum capabilities. A
// shadow miss is resolved only by the immutable original logical group's real
// completed ReadIndex. Range movement does not retire that logical group.
func (m *declaredMaterializer) readMoveOriginal(q *reader, question rangeMoveQuery) ([]byte, bool, uint64, error) {
	origin := namespace{m.ns.graph, question.origin}
	if _, found := m.declaration.Partition(origin.partition); !found {
		return nil, false, 0, errInvalid
	}
	key := rangeMoveReplayNativeKey(origin, question.family, question.record)
	control := rangeMoveReplayControlTag(question.family)
	var value []byte
	var found bool
	var index uint64
	var err error
	if origin == m.ns {
		if control {
			row, present, e := crossControl(q, key, rangeMoveReplayRecordBytes)
			value, found, index, err = row.Value, present, row.Index, e
		} else {
			value, found, err = rangeMoveReplayPoint(q, key)
		}
	} else {
		value, found, err = lookupRangeMoveReplay(q, origin, control, key)
	}
	if err != nil || !found {
		return nil, false, 0, err
	}
	if !control && question.family == 4 {
		original, e := decodePartitionMappedOutcome(value, origin)
		if e != nil {
			return nil, false, 0, e
		}
		index = original.index
	}
	return value, true, index, nil
}

type rangeMoveOriginal struct {
	origin                         namespace
	key, value                     []byte
	originalIndex, evaluationIndex uint64
	found                          bool
}

// LookupOriginal preserves the caller's original full namespace/key. The
// destination is authenticated by its durable handoff COMMIT, while a missing
// shadow must be checked at its genuine original keeper. No transport refusal
// is interpreted as completed absence.
func (c *rangeMoveCoordinator) LookupOriginal(ctx context.Context, resume rangeMoveResume, origin namespace, control bool, key []byte) (result rangeMoveOriginal, err error) {
	if !c.valid() || ctx == nil || resume.scope != c.base.host.machine.scope() || resume.request == (requestID{}) || resume.intent == ([32]byte{}) || resume.move != rangeMoveID(namespace{resume.scope.graph, resume.scope.partition}, resume.request) {
		return result, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	n, family, id, valid := rangeMoveReplayOriginal(key, control)
	if !valid || n != origin || n.graph != resume.scope.graph {
		return result, errInvalid
	}
	if _, found := c.base.host.machine.declaration.Partition(n.partition); !found {
		return result, errInvalid
	}
	output, err := graphstate.NewOutputBudget(c.limits.outputBytes)
	if err != nil {
		return result, err
	}
	if err := output.Reserve(4096 + 4*len(key)); err != nil {
		return result, outputBudgetFailure(err)
	}
	budget := newGraphRemoteBudget(c.limits, output)
	endpoints, err := c.base.endpoints(budget)
	if err != nil {
		return result, err
	}

	// Locate the native destination record without requiring the original issuer
	// to remain live. Every candidate observation is a genuine completed read;
	// caller resume data only selects the original identity.
	var target *graphAuthenticatedEndpoint
	var intent rangeMoveIntent
	for i := range c.base.host.machine.declaration.Len() {
		part, _ := c.base.host.machine.declaration.PartitionAt(i)
		if part.Partition == resume.scope.partition {
			continue
		}
		endpoint := endpoints[part.Partition]
		if endpoint == nil {
			continue
		}
		candidate, e := endpoint.move(ctx, resume.move)
		if e != nil {
			return result, errors.Join(errRangeMovePending, e)
		}
		status, decision := candidate.observation.status, candidate.observation.decision
		if status.phase == 0 {
			continue
		}
		if !candidate.validSeal() || status.intent != resume.intent || status.phase < rangeMoveActivate || status.phase > rangeMoveFinish || decision.phase != rangeMoveDecide || decision.result != rangeMoveApplied || decision.intent != resume.intent || decision.ns != (namespace{resume.scope.graph, resume.scope.partition}) {
			return result, errRangeMovePending
		}
		actual, e := decodeRangeMoveIntent(candidate.observation.intent)
		if e != nil || actual.id != resume.request || actual.source != resume.scope || actual.target != endpoint.peer.scope {
			return result, errors.Join(errInvalid, e)
		}
		if target != nil {
			return result, errCorrupt
		}
		target, intent = endpoint, actual
	}
	if target == nil {
		return result, errRangeMovePending
	}
	question := rangeMoveQuery{move: resume.move, origin: origin.partition, family: family, record: id}
	observation, err := target.moveQuestion(ctx, question)
	if err != nil {
		return result, errors.Join(errRangeMovePending, err)
	}
	if !observation.validSeal() || observation.scope != intent.target || observation.observation.status.intent != resume.intent || observation.observation.decision.intent != resume.intent {
		return result, errInvalid
	}
	answer := observation
	if !answer.observation.originalFound {
		keeper := endpoints[origin.partition]
		if keeper == nil {
			return result, errRangeMovePending
		}
		answer, err = keeper.moveQuestion(ctx, question)
		if err != nil {
			return result, errors.Join(errRangeMovePending, err)
		}
		if !answer.validSeal() || answer.scope.partition != origin.partition || answer.scope.graph != origin.graph {
			return result, errInvalid
		}
	}
	if answer.question.origin != origin.partition || answer.question.family != family || answer.question.record != id || answer.question.move != resume.move {
		return result, errInvalid
	}
	if !answer.observation.originalFound {
		return rangeMoveOriginal{origin: origin, key: bytes.Clone(key), evaluationIndex: answer.observation.index}, nil
	}
	return rangeMoveOriginal{origin: origin, key: bytes.Clone(key), value: bytes.Clone(answer.observation.original), originalIndex: answer.observation.originalIndex, evaluationIndex: answer.observation.index, found: true}, nil
}
