package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
)

type graphHostRangeMovePump interface {
	Move(context.Context, rangeMoveQuery) (rangeMoveReply, error)
}

func encodeRangeMoveQuery(q rangeMoveQuery) ([]byte, error) {
	if !q.valid() {
		return nil, errInvalid
	}
	return boundedEncoding(160, func(w *boundedWriter) {
		version := byte(2)
		if q.spans {
			version = 3
		}
		w.add([]byte{'G', 'M', 'Y', version})
		w.add(q.move[:])
		w.add(q.nonce[:])
		w.u64(q.page)
		w.u32(q.sourceRows)
		w.u32(q.sourceBytes)
		w.u32(q.outputBytes)
		w.u64(q.origin)
		w.tag(q.family)
		w.add(q.record[:])
		if q.spans {
			if q.lineage {
				w.tag(1)
			} else {
				w.tag(0)
			}
		}
	})
}

func decodeRangeMoveQuery(wire []byte) (rangeMoveQuery, error) {
	magic := "GMY\x02"
	if bytes.HasPrefix(wire, []byte("GMY\x03")) {
		magic = "GMY\x03"
	}
	body, err := graphBody(wire, magic, 160)
	if err != nil {
		return rangeMoveQuery{}, err
	}
	c := graphCursor{b: body}
	q := rangeMoveQuery{move: c.array(), nonce: c.array(), page: c.u64(), sourceRows: c.count(1<<20, 0), sourceBytes: c.count(64<<20, 0), outputBytes: c.count(64<<20, 0)}
	q.origin, q.family, q.record = c.u64(), c.tag(), c.array()
	if magic == "GMY\x03" {
		q.spans = true
		flag := c.tag()
		if flag > 1 {
			c.err = errCorrupt
		}
		q.lineage = flag == 1
	}
	if c.err != nil || len(c.b) != 0 || !q.valid() {
		return rangeMoveQuery{}, errors.Join(errCorrupt, c.err)
	}
	return q, nil
}

func encodeRangeMoveAuthorityBody(p rangeMoveAuthority) ([]byte, error) {
	if !p.validSeal() || !p.question.valid() || p.question.page != 0 || p.scope != p.observation.scope || p.id.session == ([16]byte{}) || p.id.sequence == 0 {
		return nil, errInvalid
	}
	query, err := encodeRangeMoveQuery(p.question)
	if err != nil {
		return nil, err
	}
	var status []byte
	var decision []byte
	if p.observation.status.phase != 0 {
		status, err = encodeRangeMoveStatus(p.observation.status)
		if err != nil {
			return nil, err
		}
	}
	if p.observation.decision.phase != 0 {
		decision, err = encodeRangeMoveStatus(p.observation.decision)
		if err != nil {
			return nil, err
		}
	}
	return boundedEncoding(graphSourceFrameBytes-512, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'B', 3})
		w.add(appendAllocationScope(nil, p.scope))
		w.add(p.id.session[:])
		w.u64(p.id.sequence)
		w.field(query)
		w.add(p.observation.configuration[:])
		w.u64(p.observation.index)
		w.u64(p.observation.floor)
		emitSourceRoot(w, p.observation.root)
		w.u32(p.observation.sourceRows)
		w.u32(p.observation.sourceBytes)
		w.u32(p.observation.ownedBytes)
		w.field(status)
		w.field(decision)
		w.field(p.observation.intent)
		w.field(p.observation.progress)
		if p.observation.originalFound {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.u64(p.observation.originalIndex)
		w.field(p.observation.original)
	})
}

// Decode returns inert data. Only matched authenticated endpoint completion may
// seal it; echoing this body or a request nonce alone constructs no authority.
func decodeRangeMoveAuthorityBody(wire []byte) (rangeMoveAuthority, error) {
	body, err := graphBody(wire, "GMB\x03", graphSourceFrameBytes-512)
	if err != nil {
		return rangeMoveAuthority{}, err
	}
	c := graphCursor{b: body}
	scope := c.take(120)
	p := rangeMoveAuthority{id: allocationReadID{session: c.array(), sequence: c.u64()}}
	query := c.field(160)
	copy(p.observation.configuration[:], c.take(32))
	p.observation.index, p.observation.floor = c.u64(), c.u64()
	p.observation.root = readSourceRoot(&c)
	p.observation.sourceRows = c.count(1<<20, 0)
	p.observation.sourceBytes = c.count(64<<20, 0)
	p.observation.ownedBytes = c.count(64<<20, 0)
	status, decision, intent, progress := c.field(256), c.field(256), c.field(1024), c.field(1<<20)
	originalFound, originalIndex, original := c.tag(), c.u64(), c.field(1<<20)
	if c.err != nil || len(c.b) != 0 {
		return rangeMoveAuthority{}, errors.Join(errCorrupt, c.err)
	}
	if originalFound > 1 || originalFound == 0 && (originalIndex != 0 || len(original) != 0) {
		return rangeMoveAuthority{}, errCorrupt
	}
	p.observation.originalFound, p.observation.originalIndex = originalFound == 1, originalIndex
	if originalFound == 1 {
		p.observation.original = owned(original)
	}
	p.scope = readAllocationScope(&decoder{b: scope})
	p.observation.scope = p.scope
	p.question, err = decodeRangeMoveQuery(query)
	if err != nil || p.question.spans != rangeMoveSpans(p.scope.semantic) || p.question.page != 0 || !p.scope.valid() || !rangeMoveGraphAgreement(p.scope.semantic) || p.id.session == ([16]byte{}) || p.id.sequence == 0 || p.observation.root.Index != p.observation.index || p.observation.configuration == ([32]byte{}) || p.observation.sourceRows > p.question.sourceRows || p.observation.sourceBytes < 172 || p.observation.sourceBytes > p.question.sourceBytes || p.observation.ownedBytes > p.question.outputBytes {
		return rangeMoveAuthority{}, errors.Join(errCorrupt, err)
	}
	if len(status) == 0 {
		if len(intent)+len(progress)+len(decision) != 0 {
			return rangeMoveAuthority{}, errCorrupt
		}
	} else {
		p.observation.status, err = decodeRangeMoveStatus(status)
		if err != nil || p.observation.status.ns != (namespace{p.scope.graph, p.scope.partition}) || p.observation.status.move != p.question.move || p.observation.status.index > p.observation.index || p.observation.status.format != rangeMoveStatusFormat(p.scope.semantic) || sha256.Sum256(intent) != p.observation.status.intent {
			return rangeMoveAuthority{}, errors.Join(errCorrupt, err)
		}
		p.observation.intent, p.observation.progress = owned(intent), owned(progress)
		if len(decision) != 0 {
			p.observation.decision, err = decodeRangeMoveStatus(decision)
			if err != nil || p.observation.decision.move != p.question.move || p.observation.decision.intent != p.observation.status.intent || p.observation.decision.phase != rangeMoveDecide || p.observation.decision.round == 0 || p.observation.decision.format != rangeMoveStatusFormat(p.scope.semantic) {
				return rangeMoveAuthority{}, errors.Join(errCorrupt, err)
			}
		}
	}
	return p, nil
}

func (s *graphPeerService) readRangeMove(ctx context.Context, request graphSourceRequest) ([]byte, error) {
	q, err := decodeRangeMoveQuery(request.command)
	if err != nil || q.page != 0 {
		return nil, errors.Join(errInvalid, err)
	}
	q.nonce = request.nonce
	pump, available := s.pump.(graphHostRangeMovePump)
	if !available || !s.host.machine.rangeMoves() {
		return nil, errInvalid
	}
	reply, err := pump.Move(ctx, q)
	if err != nil || reply.err != nil {
		return nil, errors.Join(err, reply.err)
	}
	if reply.question != q || reply.proof.owner != s.host || reply.id != reply.proof.id {
		return nil, errInvalid
	}
	authority, err := s.host.exportRangeMoveAuthority(reply.proof)
	if err != nil {
		return nil, err
	}
	return encodeRangeMoveAuthorityBody(authority)
}

func (e *graphAuthenticatedEndpoint) move(ctx context.Context, id [16]byte) (authority rangeMoveAuthority, err error) {
	return e.moveQuestion(ctx, rangeMoveQuery{move: id})
}

func (e *graphAuthenticatedEndpoint) moveQuestion(ctx context.Context, q rangeMoveQuery) (authority rangeMoveAuthority, err error) {
	if e == nil || e.budget == nil || !rangeMoveGraphAgreement(e.peer.scope.semantic) || q.page != 0 {
		return authority, errInvalid
	}
	nonce, err := graphReadNonce()
	if err != nil {
		return authority, err
	}
	// Progress contains at most one native bounded record. The full response
	// ceiling is reserved in admission before granting remaining physical work.
	responseCeiling := (1 << 20) + 8192
	if q.origin != 0 {
		responseCeiling = graphSourceFrameBytes
	}
	q.spans = rangeMoveSpans(e.peer.scope.semantic)
	q.nonce = nonce
	e.budget.mu.Lock()
	q.sourceRows = min(1<<20, e.budget.maxRows-e.budget.rows)
	q.sourceBytes = min(64<<20, e.budget.maxBytes-e.budget.bytes-responseCeiling-2048)
	q.outputBytes = min(64<<20, e.budget.output.Remaining()-4*(responseCeiling+2048)-1024)
	e.budget.mu.Unlock()
	if !q.valid() {
		return authority, errLimit
	}
	wire, err := encodeRangeMoveQuery(q)
	if err != nil {
		return authority, err
	}
	defer func() {
		if err != nil {
			e.budget.mu.Lock()
			e.budget.rows += q.sourceRows
			e.budget.bytes += q.sourceBytes
			ownership := e.budget.output.Reserve(q.outputBytes)
			e.budget.mu.Unlock()
			err = errors.Join(err, outputBudgetFailure(ownership))
			authority = rangeMoveAuthority{}
		}
	}()
	reply, err := e.call(ctx, graphSourceRequest{kind: sourceMoveRead, command: wire}, responseCeiling)
	if err != nil {
		return authority, err
	}
	p, err := decodeRangeMoveAuthorityBody(reply.body)
	if err != nil {
		return authority, err
	}
	q.nonce = reply.nonce
	if p.scope != e.peer.scope || p.id.session != reply.issuer || p.question != q {
		return authority, errInvalid
	}
	e.budget.mu.Lock()
	if p.observation.sourceRows > e.budget.maxRows-e.budget.rows || p.observation.sourceBytes > e.budget.maxBytes-e.budget.bytes {
		e.budget.mu.Unlock()
		return authority, errLimit
	}
	if err := e.budget.output.Reserve(p.observation.ownedBytes); err != nil {
		e.budget.mu.Unlock()
		return authority, outputBudgetFailure(err)
	}
	e.budget.rows += p.observation.sourceRows
	e.budget.bytes += p.observation.sourceBytes
	e.budget.mu.Unlock()
	return sealRangeMoveAuthority(p), nil
}

func (e *graphAuthenticatedEndpoint) submitMove(ctx context.Context, proposal allocationProposal) (rangeMoveStatus, error) {
	if proposal.target != e.peer.scope {
		return rangeMoveStatus{}, errInvalid
	}
	command, err := decodeRangeMoveCommand(proposal.wire)
	if err != nil {
		return rangeMoveStatus{}, err
	}
	intent, err := decodeRangeMoveIntent(command.intent)
	if err != nil {
		return rangeMoveStatus{}, err
	}
	wire, err := encodeGraphSourceProposal(proposal)
	if err != nil {
		return rangeMoveStatus{}, err
	}
	r, err := e.call(ctx, graphSourceRequest{kind: sourceSubmit, command: wire}, 1024)
	if err != nil {
		return rangeMoveStatus{}, err
	}
	status, err := decodeRangeMoveStatus(r.body)
	if err != nil || status.ns != (namespace{e.peer.scope.graph, e.peer.scope.partition}) || status.move != rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id) || status.intent != sha256.Sum256(command.intent) || status.format != rangeMoveStatusFormat(e.peer.scope.semantic) {
		return rangeMoveStatus{}, errors.Join(errInvalid, err)
	}
	switch status.result {
	case rangeMoveBlocked:
		return status, errRangeMovePending
	case rangeMoveMismatch:
		return status, errInvalid
	case rangeMoveLimit, rangeMoveBudgetLimit:
		return status, errLimit
	}
	return status, nil
}
