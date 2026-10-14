package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Zero allowances preserve the native compatibility path. Remote requests
// require explicit positive limits carried by the actual ReadIndex context.
type graphCutQuery struct {
	sourceRows, sourceBytes, outputBytes int
	kind                                 byte
	round                                uint64
	nonce                                [16]byte
	request                              [16]byte
	digest                               [32]byte
}
type graphCutObservation struct {
	sourceRows, sourceBytes, outputBytes int
	scope                                allocationScope
	configuration                        [32]byte
	floor, index                         uint64
	pending                              [16]byte
	coordinator                          uint64
	certificate                          graphCertificate
	local                                graphLocalReceipt
	participant                          graphParticipantObservation
}
type graphCutProof struct {
	owner       *allocationHost
	id          allocationReadID
	question    graphCutQuery
	observation graphCutObservation
}
type graphCutReply struct {
	id       allocationReadID
	question graphCutQuery
	proof    graphCutProof
	err      error
}

// Phase zero is verified absence ONLY in a completed source-owned proof.
type graphParticipantObservation struct {
	transaction [16]byte
	digest      [32]byte
	phase       crossPhase
}

func (q graphCutQuery) valid() bool {
	if (q.sourceRows != 0 || q.sourceBytes != 0 || q.outputBytes != 0) && (q.sourceRows < 1 || q.sourceRows > 1<<20 || q.sourceBytes < 1 || q.sourceBytes > 64<<20 || q.outputBytes < 1 || q.outputBytes > 64<<20) {
		return false
	}
	return q.nonce != ([16]byte{}) && (q.kind == 1 && q.round == 0 && q.request == ([16]byte{}) && q.digest == ([32]byte{}) || q.kind == 2 && q.round > 0 && q.round < math.MaxUint64 && q.request == ([16]byte{}) && q.digest == ([32]byte{}) || q.kind == 3 && q.round == 0 && q.request != ([16]byte{}) && q.digest == ([32]byte{}) || q.kind == 4 && q.round == 0 && q.request != ([16]byte{}) && q.digest != ([32]byte{}))
}
func encodeGraphCutContext(id allocationReadID, q graphCutQuery) []byte {
	b := append([]byte{'G', 'S', 'R', 1}, id.session[:]...)
	b = binary.BigEndian.AppendUint64(b, id.sequence)
	b = append(b, q.kind)
	b = binary.BigEndian.AppendUint64(b, q.round)
	b = append(b, q.nonce[:]...)
	b = append(b, q.request[:]...)
	b = append(b, q.digest[:]...)
	if q.sourceRows > 0 {
		b[3] = 2
		b = binary.BigEndian.AppendUint32(b, uint32(q.sourceRows))  // #nosec G115 -- ReadGraphCut admits positive rows <= 1<<20; cancellation reuses that immutable query.
		b = binary.BigEndian.AppendUint32(b, uint32(q.sourceBytes)) // #nosec G115 -- admitted immutable source bytes are positive and <= 64 MiB.
		b = binary.BigEndian.AppendUint32(b, uint32(q.outputBytes)) // #nosec G115 -- admitted immutable output bytes are positive and <= 64 MiB.
	}
	return b
}

// ReadGraphCut issues a uniquely correlated, applied quorum barrier.
func (h *allocationHost) ReadGraphCut(q graphCutQuery) (allocationEvent, error) {
	if h == nil || !q.valid() {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return allocationEvent{}, replica.ErrStopped
	}
	if !h.machine.certifiedReads() {
		return allocationEvent{}, errInvalid
	}
	if h.sequence == math.MaxUint64 || h.pendingReadCount() >= 16 {
		return allocationEvent{}, errLimit
	}
	if h.cutPending == nil {
		h.cutPending = make(map[allocationReadID]graphCutQuery)
	}
	h.sequence++
	h.issuedReads.Store(h.sequence)
	id := allocationReadID{h.session, h.sequence}
	h.cutPending[id] = q
	out, e := h.driver.ReadIndex(encodeGraphCutContext(id, q))
	if e != nil {
		delete(h.cutPending, id)
		return allocationEvent{}, e
	}
	event, e := h.event(out, nil)
	event.readID = id
	return event, e
}

// Cancellation releases Host request metadata. Driver keeps its real outstanding
// quota until the suppressed response or leader change; no quorum-loss reset.
func (h *allocationHost) CancelGraphCutRead(id allocationReadID) error {
	if h == nil {
		return errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return replica.ErrStopped
	}
	q, ok := h.cutPending[id]
	if !ok {
		return errInvalid
	}
	e := h.driver.CancelReadIndex(encodeGraphCutContext(id, q))
	if e == nil {
		delete(h.cutPending, id)
	}
	return e
}
func (h *allocationHost) completeGraphCutRead(w []byte, index uint64) (graphCutReply, error) {
	if (len(w) != 101 || !bytes.Equal(w[:4], []byte{'G', 'S', 'R', 1})) && (len(w) != 113 || !bytes.Equal(w[:4], []byte{'G', 'S', 'R', 2})) {
		return graphCutReply{}, errCorrupt
	}
	var id allocationReadID
	copy(id.session[:], w[4:20])
	id.sequence = binary.BigEndian.Uint64(w[20:28])
	q, ok := h.cutPending[id]
	if !ok || id.session != h.session || !bytes.Equal(w, encodeGraphCutContext(id, q)) {
		return graphCutReply{}, errInvalid
	}
	delete(h.cutPending, id)
	o, e := h.machine.observeGraphCut(context.Background(), q, index)
	r := graphCutReply{id: id, question: q, err: e}
	if e == nil {
		r.proof = graphCutProof{h, id, q, o}
	}
	return r, nil
}
func (m *declaredMaterializer) observeGraphCut(ctx context.Context, query graphCutQuery, index uint64) (out graphCutObservation, err error) {
	if ctx == nil || !query.valid() || !m.certifiedReads() {
		return out, errInvalid
	}
	if query.sourceRows > 0 && (query.sourceBytes < 172 || query.outputBytes < 172) {
		return out, errLimit
	}
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return out, err
	}
	defer func() {
		err = errors.Join(err, v.Close())
		if err != nil {
			out = graphCutObservation{}
		}
	}()
	base, err := v.RootBounded(ctx, 172)
	if err != nil {
		return out, err
	}
	l := m.limits.allocation
	effective := m.limits
	if query.sourceRows > 0 {
		l.readRows = min(l.readRows, query.sourceRows)
		l.readBytes = min(l.readBytes, query.sourceBytes)
		effective.outputBytes = min(effective.outputBytes, query.outputBytes)
	}
	l.readRows = min(l.readRows, m.limits.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: ctx, view: v, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	defer func() {
		if err == nil {
			out.sourceRows = q.rows
			out.sourceBytes = q.bytes
			if q.arena != nil {
				out.outputBytes = effective.outputBytes - q.arena.Remaining()
			}
		}
	}()
	if err = joinGraphOutputBudget(&q, graphRequest{}, 4096, effective); err != nil {
		return out, err
	}
	if m.rangeMoves() && query.kind == 1 {
		move, e := readRangeMoveActive(&q)
		if e != nil {
			return out, e
		}
		if move != ([16]byte{}) {
			return out, errGraphCutPending
		}
	}
	cfg, err := m.initialized(&q)
	if err != nil {
		return out, err
	}
	c, err := cfg.digest()
	if err != nil {
		return out, err
	}
	out.scope, out.configuration, out.index = m.scope(), c, index
	if query.kind == 4 {
		status, present, e := crossCurrent(&q, query.request)
		if e != nil {
			return out, e
		}
		if present && status.digest != query.digest {
			return out, errInvalid
		}
		if !present {
			owner, held, e := m.readCrossResourceOwner(&q)
			if e != nil {
				return out, e
			}
			if held && owner.tx == query.request {
				return out, errCorrupt
			}
		}
		out.participant = graphParticipantObservation{transaction: query.request, digest: query.digest}
		if present {
			out.participant.phase = status.phase
		}
		return out, nil
	}
	if query.kind == 3 {
		wire, found, e := q.getBounded(graphLocalReceiptKey(q.ns, query.request), 256)
		if e != nil {
			return out, e
		}
		if !found {
			return out, replica.ErrUnavailable
		}
		receipt, e := decodeGraphLocalReceipt(wire)
		if e != nil || receipt.scope != m.scope() || receipt.identity != query.request || receipt.index > index {
			return out, errors.Join(errCorrupt, e)
		}
		out.local = receipt
		return out, nil
	}
	if query.kind == 2 {
		w, found, e := q.getBounded(graphCertificateKey(q.ns, query.round), 512)
		if e != nil {
			return out, e
		}
		if !found {
			return out, errGraphCutPending
		}
		cert, e := decodeGraphCertificate(w)
		if e != nil || cert.scope != m.scope() || cert.configuration != c || cert.round != query.round || cert.index > index {
			return out, errors.Join(errCorrupt, e)
		}
		out.certificate = cert
		return out, nil
	}
	installed, err := readPartitionRoundFloor(&q, c)
	if err != nil {
		return out, err
	}
	out.floor, err = m.allocationFloor(&q, c, installed)
	if err != nil {
		return out, err
	}
	pending, present, err := m.readCrossResourceOwner(&q)
	if err != nil {
		return out, err
	}
	if present {
		out.pending, out.coordinator = pending.tx, pending.coordinator
	}
	return out, nil
}
func checkGraphCutProof(p graphCutProof, q graphCutQuery) error {
	if p.owner == nil || p.owner.machine == nil || p.question != q || !q.valid() || p.id.session != p.owner.session || p.id.sequence == 0 || p.id.sequence > p.owner.issuedReads.Load() || !p.owner.machine.certifiedReads() || p.observation.scope != p.owner.machine.scope() || p.observation.configuration == ([32]byte{}) || p.observation.index == 0 {
		return errInvalid
	}
	return nil
}

// AtGraphCertificate checks the persisted certificate on this actual voter,
// then owns the exact historical MVCC source. A closed issuer is immaterial.
func (h *allocationHost) AtGraphCertificate(ctx context.Context, cert graphCertificate) (source *raftlog.ApplicationView, err error) {
	if h == nil || ctx == nil {
		return nil, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return nil, replica.ErrStopped
	}
	if !h.machine.certifiedReads() || cert.scope != h.machine.scope() {
		return nil, errInvalid
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if h.driver.Applied() < cert.index {
		return nil, replica.ErrUnavailable
	}
	current, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, current.Close())
		if err != nil && source != nil {
			err = errors.Join(err, source.Close())
			source = nil
		}
	}()
	wire, found, e := current.Get(ctx, graphCertificateKey(h.machine.ns, cert.round), 1024)
	if e != nil {
		return nil, e
	}
	if !found || wire.Deleted {
		return nil, replica.ErrUnavailable
	}
	actual, e := decodeGraphCertificate(wire.Value)
	if e != nil || actual != cert {
		return nil, errors.Join(errInvalid, e)
	}
	if h.machine.collectsHistory() {
		l := h.machine.limits.allocation
		l.readRows = min(l.readRows, h.machine.limits.sourceRows)
		l.readBytes = min(l.readBytes, h.machine.limits.sourceBytes)
		q := reader{ctx: ctx, view: current, ns: h.machine.ns, limits: l, rows: 1, bytes: 2048}
		q.base, err = current.RootBounded(ctx, 172)
		if err != nil {
			return nil, err
		}
		if err := joinGraphOutputBudget(&q, graphRequest{}, 4096, h.machine.limits); err != nil {
			return nil, err
		}
		if err := checkGraphCertificateRetention(&q, actual); err != nil {
			return nil, err
		}
	}
	source, err = h.machine.store.ApplicationView(cert.sourceIndex)
	if err != nil {
		return nil, err
	}
	base, e := source.RootBounded(ctx, 172)
	if e != nil || base.ImageHash != sha256.Sum256(cert.image[:]) || !bytes.Equal(base.Image, cert.image[:]) {
		return source, errors.Join(errCorrupt, e)
	}
	root, e := graphstore.DecodeRoot(base.Image)
	if e != nil || root.Namespace().Partition != cert.scope.partition || [16]byte(root.Namespace().Graph) != [16]byte(cert.scope.graph) || root.OwnershipEpoch() != cert.scope.ownership {
		return source, errors.Join(errCorrupt, e)
	}
	return source, nil
}
