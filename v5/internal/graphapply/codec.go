package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

const (
	requestHeaderBytes = 45
	maxRequestBytes    = 173
	outcomeBytes       = 273
	recipientBytes     = 125
	grantBytes         = 212
)

// Every decoder checks a fixed wire size before accessing fields. Scratch is
// bounded by these fixed encodings (including validation re-encoding and its
// owned copy), independent of delivered counts. Output accounting covers retained
// representations; neither ledger measures Go allocator rounding, heap or RSS.

func owned(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
func appendNamespace(b []byte, n namespace) []byte {
	b = append(b, n.graph[:]...)
	return binary.BigEndian.AppendUint64(b, n.partition)
}
func appendAuthority(b []byte, a idalloc.Authority) []byte {
	owner := a.Owner()
	b = append(b, owner[:]...)
	return binary.BigEndian.AppendUint64(b, a.Epoch())
}
func appendSession(b []byte, s idalloc.RecipientSession) []byte {
	b = append(b, s.ID[:]...)
	b = append(b, s.Incarnation[:]...)
	return binary.BigEndian.AppendUint64(b, s.Epoch)
}
func seal(b []byte) []byte { hash := sha256.Sum256(b); return owned(append(b, hash[:]...)) }
func wireBody(b []byte, magic string, size int) ([]byte, error) {
	if len(b) != size || !bytes.Equal(b[:4], []byte(magic)) {
		return nil, errCorrupt
	}
	hash := sha256.Sum256(b[:len(b)-32])
	if !bytes.Equal(hash[:], b[len(b)-32:]) {
		return nil, errCorrupt
	}
	return b[4 : len(b)-32], nil
}

type decoder struct{ b []byte }

func (d *decoder) array() [16]byte { var x [16]byte; copy(x[:], d.b[:16]); d.b = d.b[16:]; return x }
func (d *decoder) number() uint64  { n := binary.BigEndian.Uint64(d.b[:8]); d.b = d.b[8:]; return n }
func (d *decoder) ns() namespace   { return namespace{idalloc.GraphID(d.array()), d.number()} }
func (d *decoder) session() idalloc.RecipientSession {
	return idalloc.RecipientSession{ID: d.array(), Incarnation: d.array(), Epoch: d.number()}
}
func (d *decoder) authority() (idalloc.Authority, error) {
	return idalloc.NewAuthority(d.array(), d.number())
}
func requestSize(kind commandKind) int {
	switch kind {
	case initAllocator:
		return requestHeaderBytes + 32 + 32
	case transferAllocator:
		return requestHeaderBytes + 48 + 32
	case activateRecipient:
		return requestHeaderBytes + 56 + 32
	case reserveGrant:
		return requestHeaderBytes + 96 + 32
	}
	return 0
}
func encodeRequest(r request, l limits) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	if requestSize(r.kind) > l.inputBytes {
		return nil, errLimit
	}
	b := appendNamespace([]byte{'A', 'R', 'Q', 1, byte(r.kind)}, r.ns)
	identity := r.identity()
	b = append(b, identity[:]...)
	switch r.kind {
	case initAllocator:
		b = appendAuthority(b, r.authority)
		b = binary.BigEndian.AppendUint64(b, r.maxBlock)
	case transferAllocator:
		b = appendAuthority(b, r.authority)
		b = appendAuthority(b, r.replacement)
	case activateRecipient:
		b = appendSession(b, r.session)
		b = binary.BigEndian.AppendUint64(b, r.expectedEpoch)
		b = binary.BigEndian.AppendUint64(b, r.home)
	case reserveGrant:
		b = appendAuthority(b, r.authority)
		g := r.grantRequest()
		b = append(b, g.Graph[:]...)
		b = appendSession(b, g.Session)
		b = binary.BigEndian.AppendUint64(b, g.Sequence)
		b = binary.BigEndian.AppendUint64(b, g.Count)
	}
	return seal(b), nil
}
func decodeRequest(b []byte, l limits) (request, error) {
	if err := l.validate(); err != nil {
		return request{}, err
	}
	if len(b) > l.inputBytes {
		return request{}, errLimit
	}
	if len(b) < 5 {
		return request{}, errCorrupt
	}
	kind := commandKind(b[4])
	size := requestSize(kind)
	if size == 0 || len(b) != size || !bytes.Equal(b[:4], []byte{'A', 'R', 'Q', 1}) {
		return request{}, errCorrupt
	}
	hash := sha256.Sum256(b[:size-32])
	if !bytes.Equal(hash[:], b[size-32:]) {
		return request{}, errCorrupt
	}
	d := decoder{b: b[5 : size-32]}
	r := request{kind: kind, ns: d.ns()}
	identity := d.array()
	if kind == initAllocator {
		r.attempt = bootstrapAttemptID(identity)
	} else {
		r.id = requestID(identity)
	}
	var err error
	switch kind {
	case initAllocator:
		r.authority, err = d.authority()
		r.maxBlock = d.number()
	case transferAllocator:
		r.authority, err = d.authority()
		if err == nil {
			r.replacement, err = d.authority()
		}
	case activateRecipient:
		r.session = d.session()
		r.expectedEpoch = d.number()
		r.home = d.number()
	case reserveGrant:
		r.authority, err = d.authority()
		graph := idalloc.GraphID(d.array())
		r.session = d.session()
		r.sequence = d.number()
		r.count = d.number()
		if graph != r.ns.graph {
			return request{}, errCorrupt
		}
	}
	if err != nil || len(d.b) != 0 || r.validate() != nil {
		return request{}, errCorrupt
	}
	return r, nil
}

// Only known, bounded bootstrap identity headers can receive zero-KV malformed
// rejections. Unknown framing/identity remains an operational corruption error.
func recognizeBootstrap(b []byte) (request, bool) {
	if len(b) < requestHeaderBytes || len(b) > maxRequestBytes || !bytes.Equal(b[:5], []byte{'A', 'R', 'Q', 1, byte(initAllocator)}) {
		return request{}, false
	}
	d := decoder{b: b[5:]}
	r := request{kind: initAllocator, ns: d.ns(), attempt: bootstrapAttemptID(d.array())}
	return r, r.ns.valid() && r.attempt != (bootstrapAttemptID{})
}
func appendGrant(b []byte, g idalloc.Grant) []byte {
	r := g.Request
	b = append(b, r.Graph[:]...)
	b = appendSession(b, r.Session)
	b = binary.BigEndian.AppendUint64(b, r.Sequence)
	b = binary.BigEndian.AppendUint64(b, r.Count)
	a := g.Reservation
	b = append(b, a.Request.Graph[:]...)
	b = appendAuthority(b, a.Request.Authority)
	for _, n := range []uint64{a.Request.Sequence, a.Request.Count, a.First, a.Last} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	return b
}
func (d *decoder) grant() (idalloc.Grant, error) {
	r := idalloc.GrantRequest{Graph: idalloc.GraphID(d.array()), Session: d.session(), Sequence: d.number(), Count: d.number()}
	graph := idalloc.GraphID(d.array())
	authority, err := d.authority()
	a := idalloc.Reservation{Request: idalloc.Request{Graph: graph, Authority: authority, Sequence: d.number(), Count: d.number()}, First: d.number(), Last: d.number()}
	g := idalloc.Grant{Request: r, Reservation: a}
	if err != nil {
		return idalloc.Grant{}, errors.Join(errCorrupt, err)
	}
	if err := idalloc.ValidateGrant(g); err != nil {
		return idalloc.Grant{}, errors.Join(errCorrupt, err)
	}
	return g, nil
}
func encodeOutcome(o outcome) ([]byte, error) {
	if !o.ns.valid() || requestSize(o.kind) == 0 || o.identity == ([16]byte{}) || o.hash == ([32]byte{}) || o.index == 0 || o.disposition < applied || o.disposition > controlRecovery || o.reason > reasonLimit {
		return nil, errInvalid
	}
	hasGrant := o.grant != (idalloc.Grant{})
	if hasGrant && (o.kind != reserveGrant || o.grantIndex > o.index || o.disposition == applied && o.grantIndex != o.index) || o.disposition == grantRecovery && (!hasGrant || o.grantIndex >= o.index) || o.disposition == controlRecovery && (o.reason != reasonNone || hasGrant || o.kind != transferAllocator && o.kind != activateRecipient) || o.kind == reserveGrant && o.reason == reasonNone && !hasGrant {
		return nil, errInvalid
	}
	if hasGrant && (idalloc.ValidateGrant(o.grant) != nil || o.grant.Request.Graph != o.ns.graph || o.grantIndex == 0) || !hasGrant && o.grantIndex != 0 || o.reason != reasonNone && hasGrant {
		return nil, errInvalid
	}
	b := appendNamespace([]byte{'A', 'R', 'O', 1}, o.ns)
	b = append(b, byte(o.kind))
	b = append(b, o.identity[:]...)
	b = append(b, o.hash[:]...)
	b = binary.BigEndian.AppendUint64(b, o.index)
	b = append(b, byte(o.disposition))
	b = binary.BigEndian.AppendUint16(b, uint16(o.reason))
	b = binary.BigEndian.AppendUint64(b, o.grantIndex)
	if hasGrant {
		b = append(b, 1)
		b = appendGrant(b, o.grant)
	} else {
		b = append(b, make([]byte, 145)...)
	}
	return seal(b), nil
}
func decodeOutcome(b []byte, n namespace) (outcome, error) {
	body, err := wireBody(b, "ARO\x01", outcomeBytes)
	if err != nil {
		return outcome{}, err
	}
	d := decoder{b: body}
	o := outcome{ns: d.ns()}
	o.kind = commandKind(d.b[0])
	d.b = d.b[1:]
	o.identity = d.array()
	copy(o.hash[:], d.b[:32])
	d.b = d.b[32:]
	o.index = d.number()
	o.disposition = disposition(d.b[0])
	o.reason = reason(binary.BigEndian.Uint16(d.b[1:3]))
	d.b = d.b[3:]
	o.grantIndex = d.number()
	hasGrant := d.b[0]
	d.b = d.b[1:]
	if hasGrant == 1 {
		o.grant, err = d.grant()
	} else if hasGrant != 0 || !bytes.Equal(d.b, make([]byte, 144)) {
		return outcome{}, errCorrupt
	} else {
		d.b = d.b[144:]
	}
	if err != nil || o.ns != n || len(d.b) != 0 {
		return outcome{}, errCorrupt
	}
	if _, err := encodeOutcome(o); err != nil {
		return outcome{}, errCorrupt
	}
	return o, nil
}
func encodeRecipient(n namespace, r recipientRecord) ([]byte, error) {
	if !n.valid() || idalloc.ValidateRecipientSession(r.session) != nil || r.home != n.partition || r.index == 0 {
		return nil, errInvalid
	}
	b := appendNamespace([]byte{'A', 'R', 'R', 1}, n)
	b = appendSession(b, r.session)
	for _, v := range []uint64{r.home, r.index, r.sequence} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	b = append(b, 1) // active; this same-partition transition has no remote pending phase
	return seal(b), nil
}
func decodeRecipient(b []byte, n namespace) (recipientRecord, error) {
	body, err := wireBody(b, "ARR\x01", recipientBytes)
	if err != nil {
		return recipientRecord{}, err
	}
	d := decoder{b: body}
	stored := d.ns()
	r := recipientRecord{session: d.session(), home: d.number(), index: d.number(), sequence: d.number()}
	if stored != n || len(d.b) != 1 || d.b[0] != 1 {
		return recipientRecord{}, errCorrupt
	}
	if _, err := encodeRecipient(n, r); err != nil {
		return recipientRecord{}, errCorrupt
	}
	return r, nil
}
func encodeGrant(n namespace, g grantRecord) ([]byte, error) {
	if !n.valid() || g.index == 0 || g.grant.Request.Graph != n.graph {
		return nil, errInvalid
	}
	if err := idalloc.ValidateGrant(g.grant); err != nil {
		return nil, errors.Join(errInvalid, err)
	}
	b := appendNamespace([]byte{'A', 'R', 'G', 1}, n)
	b = binary.BigEndian.AppendUint64(b, g.index)
	return seal(appendGrant(b, g.grant)), nil
}
func decodeGrant(b []byte, n namespace) (grantRecord, error) {
	body, err := wireBody(b, "ARG\x01", grantBytes)
	if err != nil {
		return grantRecord{}, err
	}
	d := decoder{b: body}
	stored := d.ns()
	index := d.number()
	g, err := d.grant()
	if err != nil {
		return grantRecord{}, err
	}
	if stored != n || len(d.b) != 0 || index == 0 || g.Request.Graph != n.graph {
		return grantRecord{}, errCorrupt
	}
	return grantRecord{g, index}, nil
}
