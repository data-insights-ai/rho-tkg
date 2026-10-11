package graphapply

import (
	"bytes"
	"errors"
	"math"
)

const graphCutWireBytes = 4096
const graphCutQueryBytes = 121

// graphCutAuthorityBody carries inert, correlated observation data. Neither its
// construction nor wire decoding grants a completed read or source authority.
type graphCutAuthorityBody struct {
	scope       allocationScope
	id          allocationReadID
	question    graphCutQuery
	observation graphCutObservation
}

func encodeGraphCutQuery(q graphCutQuery) ([]byte, error) {
	if !q.valid() {
		return nil, errInvalid
	}
	return boundedEncoding(graphCutQueryBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'Q', 2})
		w.tag(q.kind)
		w.u64(q.round)
		w.add(q.nonce[:])
		w.add(q.request[:])
		w.add(q.digest[:])
		w.u32(q.sourceRows)
		w.u32(q.sourceBytes)
		w.u32(q.outputBytes)
	})
}

func decodeGraphCutQuery(wire []byte) (graphCutQuery, error) {
	body, err := graphBody(wire, "GCQ\x02", graphCutWireBytes)
	if err != nil || len(body) != graphCutQueryBytes-36 {
		return graphCutQuery{}, errors.Join(errCorrupt, err)
	}
	c := graphCursor{b: body}
	q := graphCutQuery{kind: c.tag(), round: c.u64(), nonce: c.array(), request: c.array()}
	copy(q.digest[:], c.take(32))
	q.sourceRows, q.sourceBytes, q.outputBytes = c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	if c.err != nil || len(c.b) != 0 || !q.valid() {
		return graphCutQuery{}, errors.Join(errCorrupt, c.err)
	}
	return q, nil
}

func validGraphCutAuthorityBody(p graphCutAuthorityBody) bool {
	o, q := p.observation, p.question
	if !p.scope.valid() || p.scope.semantic != certifiedGraphSemanticContractID() || p.scope != o.scope || p.id.session == ([16]byte{}) || p.id.sequence == 0 || !q.valid() || o.configuration == ([32]byte{}) || o.index == 0 {
		return false
	}
	if o.sourceRows < 0 || o.sourceRows > 1<<20 || o.sourceBytes < 0 || o.sourceBytes > 64<<20 || o.outputBytes < 0 || o.outputBytes > 64<<20 || q.sourceRows > 0 && (o.sourceRows > q.sourceRows || o.sourceBytes > q.sourceBytes || o.outputBytes > q.outputBytes) {
		return false
	}
	if q.kind != 1 && (o.floor != 0 || o.pending != ([16]byte{}) || o.coordinator != 0) || q.kind != 2 && o.certificate != (graphCertificate{}) || q.kind != 3 && o.local != (graphLocalReceipt{}) || q.kind != 4 && o.participant != (graphParticipantObservation{}) {
		return false
	}
	switch q.kind {
	case 1:
		return o.floor < math.MaxUint64 && (o.pending == ([16]byte{})) == (o.coordinator == 0)
	case 2:
		c := o.certificate
		return c.scope == o.scope && c.configuration == o.configuration && c.round == q.round && c.index <= o.index
	case 3:
		r := o.local
		return r.scope == o.scope && r.identity == q.request && r.index > 0 && r.index <= o.index
	case 4:
		r := o.participant
		return r.transaction == q.request && r.digest == q.digest && r.phase <= crossResolve
	}
	return false
}

func encodeGraphCutAuthorityBody(p graphCutAuthorityBody) ([]byte, error) {
	if !validGraphCutAuthorityBody(p) {
		return nil, errInvalid
	}
	scope, err := encodeAllocationScope(p.scope)
	if err != nil {
		return nil, err
	}
	query, err := encodeGraphCutQuery(p.question)
	if err != nil {
		return nil, err
	}
	var payload []byte
	switch p.question.kind {
	case 2:
		payload, err = encodeGraphCertificate(p.observation.certificate)
		if err != nil {
			return nil, err
		}
	case 3:
		payload = encodeGraphLocalReceipt(p.observation.local)
	}
	return boundedEncoding(graphCutWireBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'B', 2})
		w.field(scope)
		w.add(p.id.session[:])
		w.u64(p.id.sequence)
		w.field(query)
		w.add(p.observation.configuration[:])
		w.u64(p.observation.floor)
		w.u64(p.observation.index)
		w.add(p.observation.pending[:])
		w.u64(p.observation.coordinator)
		w.u32(p.observation.sourceRows)
		w.u32(p.observation.sourceBytes)
		w.u32(p.observation.outputBytes)
		switch p.question.kind {
		case 2, 3:
			w.field(payload)
		case 4:
			w.add(p.observation.participant.transaction[:])
			w.add(p.observation.participant.digest[:])
			w.tag(byte(p.observation.participant.phase))
		}
	})
}

// decodeGraphCutAuthorityBody returns values only. Authenticated endpoint,
// request/nonce and native completion checks must precede any authority seal.
func decodeGraphCutAuthorityBody(wire []byte) (graphCutAuthorityBody, error) {
	body, err := graphBody(wire, "GCB\x02", graphCutWireBytes)
	if err != nil {
		return graphCutAuthorityBody{}, err
	}
	c := graphCursor{b: body}
	scope := c.field(allocationScopeBytes)
	p := graphCutAuthorityBody{id: allocationReadID{session: c.array(), sequence: c.u64()}}
	query := c.field(graphCutQueryBytes)
	copy(p.observation.configuration[:], c.take(32))
	p.observation.floor, p.observation.index = c.u64(), c.u64()
	p.observation.pending, p.observation.coordinator = c.array(), c.u64()
	p.observation.sourceRows, p.observation.sourceBytes, p.observation.outputBytes = c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	if c.err != nil {
		return graphCutAuthorityBody{}, errors.Join(errCorrupt, c.err)
	}
	p.scope, err = decodeAllocationScope(scope)
	if err != nil {
		return graphCutAuthorityBody{}, err
	}
	p.observation.scope = p.scope
	p.question, err = decodeGraphCutQuery(query)
	if err != nil {
		return graphCutAuthorityBody{}, err
	}
	switch p.question.kind {
	case 2:
		payload := c.field(512)
		if c.err == nil {
			p.observation.certificate, err = decodeGraphCertificate(payload)
		}
	case 3:
		payload := c.field(256)
		if c.err == nil {
			p.observation.local, err = decodeGraphLocalReceipt(payload)
		}
	case 4:
		p.observation.participant.transaction = c.array()
		copy(p.observation.participant.digest[:], c.take(32))
		p.observation.participant.phase = crossPhase(c.tag())
	}
	if c.err != nil || err != nil || len(c.b) != 0 {
		return graphCutAuthorityBody{}, errors.Join(errCorrupt, c.err, err)
	}
	canonical, err := encodeGraphCutAuthorityBody(p)
	if err != nil || !bytes.Equal(canonical, wire) {
		return graphCutAuthorityBody{}, errors.Join(errCorrupt, err)
	}
	return p, nil
}

// Only a single refusal cause may become an explicit cut status. Wrapping and
// repeated joins preserve that cause; unrelated joined failures are rejected.
func graphCutOnlyRefusal(err, cause error, depth int) bool {
	if err == nil || depth > 32 {
		return false
	}
	if err == cause {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !graphCutOnlyRefusal(child, cause, depth+1) {
				return false
			}
		}
		return true
	}
	return graphCutOnlyRefusal(errors.Unwrap(err), cause, depth+1)
}

func encodeGraphCutReplyBody(p graphCutAuthorityBody, refusal error) ([]byte, error) {
	status := byte(0)
	var payload []byte
	if refusal != nil {
		if p != (graphCutAuthorityBody{}) {
			return nil, errInvalid
		}
		switch {
		case graphCutOnlyRefusal(refusal, errGraphCutPending, 0):
			status = 1
		case graphCutOnlyRefusal(refusal, errGraphCutScope, 0):
			status = 2
		default:
			return nil, errInvalid
		}
	} else {
		var err error
		payload, err = encodeGraphCutAuthorityBody(p)
		if err != nil {
			return nil, err
		}
	}
	return boundedEncoding(graphCutWireBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'P', 2})
		w.tag(status)
		w.field(payload)
	})
}

func decodeGraphCutReplyBody(wire []byte) (graphCutAuthorityBody, error) {
	body, err := graphBody(wire, "GCP\x02", graphCutWireBytes)
	if err != nil {
		return graphCutAuthorityBody{}, err
	}
	c := graphCursor{b: body}
	status, payload := c.tag(), c.field(graphCutWireBytes)
	if c.err != nil || len(c.b) != 0 || status > 2 || status != 0 && len(payload) != 0 {
		return graphCutAuthorityBody{}, errors.Join(errCorrupt, c.err)
	}
	switch status {
	case 1:
		return graphCutAuthorityBody{}, errGraphCutPending
	case 2:
		return graphCutAuthorityBody{}, errGraphCutScope
	default:
		return decodeGraphCutAuthorityBody(payload)
	}
}
