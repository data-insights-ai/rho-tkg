package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type rangeMoveQuery struct {
	spans                                bool
	lineage                              bool
	move                                 [16]byte
	nonce                                [16]byte
	page                                 uint64
	origin                               uint64
	family                               byte
	record                               [16]byte
	sourceRows, sourceBytes, outputBytes int
}

func (q rangeMoveQuery) valid() bool {
	lookupValid := q.origin == 0 && q.family == 0 && q.record == ([16]byte{}) || q.origin != 0 && (rangeMoveReplayDataTag(q.family) || rangeMoveReplayControlTag(q.family)) && q.record != ([16]byte{}) && q.page == 0
	return lookupValid && q.move != ([16]byte{}) && q.nonce != ([16]byte{}) && q.page <= uint64(rangeMoveQueryPageLimit(q.spans)) && (!q.lineage || q.spans && q.page == 0 && q.origin == 0) && q.sourceRows > 0 && q.sourceRows <= 1<<20 && q.sourceBytes > 0 && q.sourceBytes <= 64<<20 && q.outputBytes > 0 && q.outputBytes <= 64<<20
}

type pendingRangeMoveRead struct {
	question rangeMoveQuery
	ctx      context.Context
}

type rangeMoveObservation struct {
	scope                               allocationScope
	configuration                       [32]byte
	index, floor                        uint64
	root                                raftlog.ApplicationRoot
	status                              rangeMoveStatus
	decision                            rangeMoveStatus
	intent, progress                    []byte
	original                            []byte
	originalFound                       bool
	originalIndex                       uint64
	sourceRows, sourceBytes, ownedBytes int
}

type rangeMoveProof struct {
	owner       *allocationHost
	id          allocationReadID
	question    rangeMoveQuery
	observation rangeMoveObservation
}

type rangeMoveReply struct {
	id       allocationReadID
	question rangeMoveQuery
	proof    rangeMoveProof
	err      error
}

// The unique local pending ID retains the COMPLETE question, including fresh
// profile/hydration flags; GMR1 never grants authority independently of that slot.
func encodeRangeMoveReadContext(id allocationReadID, q rangeMoveQuery) []byte {
	w := boundedWriter{max: 160, b: make([]byte, 0, 128)}
	w.add([]byte{'G', 'M', 'R', 1})
	w.add(id.session[:])
	w.u64(id.sequence)
	w.add(q.move[:])
	w.add(q.nonce[:])
	w.u64(q.page)
	w.u32(q.sourceRows)
	w.u32(q.sourceBytes)
	w.u32(q.outputBytes)
	w.u64(q.origin)
	w.tag(q.family)
	w.add(q.record[:])
	return w.b
}

// ReadRangeMove returns a completed applied quorum observation of the actual
// native transition. An absent record is meaningful only in that completed
// reply; missing transport or consensus responses never prove absence.
func (h *allocationHost) ReadRangeMove(ctx context.Context, q rangeMoveQuery) (allocationEvent, error) {
	if h == nil || ctx == nil || !q.valid() || q.page != 0 {
		return allocationEvent{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return allocationEvent{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return allocationEvent{}, replica.ErrStopped
	}
	if h.machine == nil || !h.machine.rangeMoves() || h.driver == nil || q.spans != rangeMoveSpans(h.machine.SemanticContractID()) {
		return allocationEvent{}, errInvalid
	}
	if h.sequence == math.MaxUint64 || h.pendingReadCount() >= 16 {
		return allocationEvent{}, errLimit
	}
	if h.movePending == nil {
		h.movePending = make(map[allocationReadID]pendingRangeMoveRead)
	}
	h.sequence++
	h.issuedReads.Store(h.sequence)
	id := allocationReadID{session: h.session, sequence: h.sequence}
	h.movePending[id] = pendingRangeMoveRead{q, ctx}
	out, err := h.driver.ReadIndex(encodeRangeMoveReadContext(id, q))
	if err != nil {
		delete(h.movePending, id)
		return allocationEvent{}, err
	}
	event, err := h.event(out, nil)
	event.readID = id
	return event, err
}

func (h *allocationHost) completeRangeMoveRead(wire []byte, index uint64) (rangeMoveReply, error) {
	if len(wire) != 105 || !bytes.Equal(wire[:4], []byte{'G', 'M', 'R', 1}) {
		return rangeMoveReply{}, errCorrupt
	}
	c := graphCursor{b: wire[4:]}
	id := allocationReadID{session: c.array(), sequence: c.u64()}
	pending, present := h.movePending[id]
	if c.err != nil || !present || id.session != h.session || !bytes.Equal(wire, encodeRangeMoveReadContext(id, pending.question)) {
		return rangeMoveReply{}, errInvalid
	}
	delete(h.movePending, id)
	observation, err := h.machine.observeRangeMove(pending.ctx, pending.question, index)
	reply := rangeMoveReply{id: id, question: pending.question, err: err}
	if err == nil {
		reply.proof = rangeMoveProof{h, id, pending.question, observation}
	}
	return reply, nil
}

func (m *declaredMaterializer) observeRangeMove(ctx context.Context, question rangeMoveQuery, index uint64) (observation rangeMoveObservation, err error) {
	if ctx == nil || !question.valid() || question.page != 0 || !m.rangeMoves() || question.spans != rangeMoveSpans(m.SemanticContractID()) {
		return observation, errInvalid
	}
	if question.sourceBytes < 172 || question.outputBytes < 172 {
		return observation, errLimit
	}
	view, err := m.store.ApplicationView(index)
	if err != nil {
		return observation, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			observation = rangeMoveObservation{}
		}
	}()
	base, err := view.RootBounded(ctx, 172)
	if err != nil {
		return observation, err
	}
	l := m.limits.allocation
	l.readRows = min(l.readRows, m.limits.sourceRows, question.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes, question.sourceBytes)
	effective := m.limits
	effective.outputBytes = min(effective.outputBytes, question.outputBytes)
	q := reader{ctx: ctx, view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	if err := joinGraphOutputBudget(&q, graphRequest{}, 2048, effective); err != nil {
		return observation, err
	}
	cfg, err := m.initialized(&q)
	if err != nil {
		return observation, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return observation, err
	}
	installed, err := readPartitionRoundFloor(&q, configuration)
	if err != nil {
		return observation, err
	}
	floor, err := m.allocationFloor(&q, configuration, installed)
	if err != nil {
		return observation, err
	}
	observation = rangeMoveObservation{scope: m.scope(), configuration: configuration, index: index, floor: floor, root: base}
	statusWire, present, err := q.getBounded(rangeMoveStatusKey(m.ns, question.move), 256)
	if err != nil {
		return observation, err
	}
	if present {
		observation.status, err = decodeRangeMoveStatus(statusWire)
		if err != nil || observation.status.ns != m.ns || observation.status.move != question.move || observation.status.index > index || observation.status.format != rangeMoveStatusFormat(m.SemanticContractID()) {
			return observation, errors.Join(errCorrupt, err)
		}
		intent, found, err := crossControl(&q, rangeMoveControlKey(m.ns, question.move, 'I'), 1024)
		if err != nil || !found || sha256.Sum256(intent.Value) != observation.status.intent {
			return observation, errors.Join(errCorrupt, err)
		}
		if _, err := decodeRangeMoveIntent(intent.Value); err != nil {
			return observation, err
		}
		observation.intent = intent.Value
		decision, found, err := crossControl(&q, rangeMoveControlKey(m.ns, question.move, 'D'), 256)
		if err != nil {
			return observation, err
		}
		if found {
			observation.decision, err = decodeRangeMoveStatus(decision.Value)
			if err != nil || observation.decision.move != question.move || observation.decision.intent != observation.status.intent || observation.decision.phase != rangeMoveDecide || observation.decision.round == 0 || observation.decision.format != rangeMoveStatusFormat(m.SemanticContractID()) {
				return observation, errors.Join(errCorrupt, err)
			}
		}
		observation.progress, _, err = q.getBounded(rangeMoveProgressKey(m.ns, question.move), 1<<20)
		if err != nil {
			return observation, err
		}

	}
	if question.lineage {
		progress, e := decodeRangeMoveProgress(observation.progress, q.arena)
		if e != nil || !progress.transferComplete() || progress.imported != nil || m.ns.partition != progress.descriptor.Spec.From.Partition {
			return observation, errors.Join(errInvalid, e)
		}
		if e = m.hydrateMoveRetention(&q, question.move, &progress); e != nil {
			return observation, e
		}
		progress.inlineRetention = true
		observation.progress, e = encodeRangeMoveProgress(progress, q.arena)
		if e != nil {
			return observation, e
		}
	}
	if question.origin != 0 {
		if question.origin != m.ns.partition {
			if !present || observation.status.phase < rangeMoveActivate || observation.status.phase > rangeMoveFinish || observation.decision.phase != rangeMoveDecide {
				return observation, errRangeMovePending
			}
		}
		observation.original, observation.originalFound, observation.originalIndex, err = m.readMoveOriginal(&q, question)
		if err != nil {
			return observation, err
		}
	}
	observation.sourceRows, observation.sourceBytes = q.rows, q.bytes
	observation.ownedBytes = effective.outputBytes - q.arena.Remaining()
	return observation, nil
}

func checkRangeMoveProof(p rangeMoveProof) error {
	if p.owner == nil || p.owner.machine == nil || !p.owner.machine.rangeMoves() || !p.question.valid() || p.question.spans != rangeMoveSpans(p.observation.scope.semantic) || p.question.page != 0 || p.id.session != p.owner.session || p.id.sequence == 0 || p.id.sequence > p.owner.issuedReads.Load() || p.observation.scope != p.owner.machine.scope() || p.observation.index == 0 || p.observation.root.Index != p.observation.index || len(p.observation.root.Image) != 172 || sha256.Sum256(p.observation.root.Image) != p.observation.root.ImageHash || p.observation.configuration == ([32]byte{}) {
		return errInvalid
	}
	if p.observation.status.phase != 0 && (p.observation.status.move != p.question.move || p.observation.status.index > p.observation.index || p.observation.status.format != rangeMoveStatusFormat(p.observation.scope.semantic) || sha256.Sum256(p.observation.intent) != p.observation.status.intent) {
		return errInvalid
	}
	if p.observation.sourceRows < 0 || p.observation.sourceRows > p.question.sourceRows || p.observation.sourceBytes < 172 || p.observation.sourceBytes > p.question.sourceBytes || p.observation.ownedBytes < 0 || p.observation.ownedBytes > p.question.outputBytes || p.observation.status.phase == 0 && (len(p.observation.intent) != 0 || len(p.observation.progress) != 0) {
		return errInvalid
	}
	return nil
}

func (h *allocationHost) pendingReadCount() int {
	return len(h.pending) + len(h.crossPending) + len(h.requestPending) + len(h.capacityPending) + len(h.cutPending) + len(h.movePending)
}

func rangeMoveReadBudget(l materializerLimits) rangeMoveQuery {
	return rangeMoveQuery{sourceRows: l.sourceRows, sourceBytes: l.sourceBytes, outputBytes: l.outputBytes}
}
