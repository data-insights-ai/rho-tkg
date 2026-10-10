package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type applyPosition struct{ index, generation uint64 }
type reader struct {
	ctx            context.Context
	view           *raftlog.ApplicationView
	ns             namespace
	limits         limits
	base           raftlog.ApplicationRoot
	rows, bytes    int
	writes         []raftlog.KV
	stageBytes     int
	allocatorFound bool
	allocatorInfo  idalloc.View
}

func recordKey(n namespace, tag byte) []byte       { return appendNamespace([]byte{'g', 'a', tag}, n) }
func allocatorKey(n namespace) []byte              { return recordKey(n, 1) }
func recipientKey(n namespace, id [16]byte) []byte { return append(recordKey(n, 2), id[:]...) }
func grantKey(n namespace, s idalloc.RecipientSession, sequence uint64) []byte {
	return binary.BigEndian.AppendUint64(appendSession(recordKey(n, 3), s), sequence)
}
func outcomeKey(r request) []byte {
	tag := byte(4)
	if r.kind == initAllocator || r.kind == initGraph {
		tag = 5
	}
	id := r.identity()
	return append(recordKey(r.ns, tag), id[:]...)
}
func (q *reader) get(key []byte) ([]byte, bool, error) {
	return q.getBounded(key, max(idalloc.CheckpointSize, outcomeBytes, grantBytes, recipientBytes))
}
func (q *reader) getBounded(key []byte, valueBytes int) ([]byte, bool, error) {
	if valueBytes < 0 || valueBytes > 16<<20 {
		return nil, false, errInvalid
	}
	if err := q.ctx.Err(); err != nil {
		return nil, false, err
	}
	if q.rows >= q.limits.readRows || len(key)+64 > q.limits.readBytes-q.bytes {
		return nil, false, errLimit
	}
	q.rows++
	maxBytes := min(len(key)+valueBytes, q.limits.readBytes-q.bytes-64, q.view.ReadLimits().Bytes)
	row, found, err := q.view.Get(q.ctx, key, maxBytes)
	if err != nil {
		if errors.Is(err, raftlog.ErrLimit) {
			return nil, false, errors.Join(errLimit, err)
		}
		return nil, false, err
	}
	cost := max(len(key), cap(row.Key)) + cap(row.Value) + 64
	if cost > q.limits.readBytes-q.bytes {
		return nil, false, errLimit
	}
	q.bytes += cost
	if found && row.Deleted {
		return nil, false, errCorrupt
	}
	return row.Value, found, nil
}
func (q *reader) put(key, value []byte) error {
	cost := 2*len(key) + len(value) + 64
	if len(q.writes) == q.limits.stageRows || cost > q.limits.stageBytes-q.stageBytes {
		return errLimit
	}
	q.stageBytes += cost
	q.writes = append(q.writes, raftlog.KV{Key: owned(key), Value: owned(value)})
	return nil
}
func (q *reader) allocator() (idalloc.State, bool, error) {
	b, found, err := q.get(allocatorKey(q.ns))
	if err != nil || !found {
		return idalloc.State{}, found, err
	}
	s, err := idalloc.DecodeCheckpoint(b)
	if err != nil {
		return idalloc.State{}, false, errors.Join(errCorrupt, err)
	}
	v, err := idalloc.Inspect(&s)
	if err != nil || v.Graph != q.ns.graph {
		return idalloc.State{}, false, errors.Join(errCorrupt, err)
	}
	q.allocatorFound = true
	q.allocatorInfo = v
	return s, true, nil
}
func (q *reader) recipient(id [16]byte) (recipientRecord, bool, error) {
	b, found, err := q.get(recipientKey(q.ns, id))
	if err != nil || !found {
		return recipientRecord{}, found, err
	}
	r, err := decodeRecipient(b, q.ns)
	if err == nil && (r.session.ID != id || r.index > q.base.Index) {
		err = errCorrupt
	}
	return r, err == nil, err
}
func (q *reader) grant(r request) (grantRecord, bool, error) {
	b, found, err := q.get(grantKey(q.ns, r.session, r.sequence))
	if err != nil || !found {
		return grantRecord{}, found, err
	}
	g, err := decodeGrant(b, q.ns)
	if err == nil && (g.grant.Request.Session != r.session || g.grant.Request.Sequence != r.sequence || g.index > q.base.Index) {
		err = errCorrupt
	}
	return g, err == nil, err
}
func (q *reader) emptyProof() error {
	root, err := q.view.ProveNoApplicationData(q.ctx)
	if err != nil {
		return err
	}
	if root.Generation != q.base.Generation || root.Index != q.base.Index || root.ImageHash != q.base.ImageHash {
		return errInvalid
	}
	if cap(root.Image) > q.limits.readBytes-q.bytes {
		return errLimit
	}
	q.bytes += cap(root.Image)
	return nil
}
func rejectReason(err error) (reason, bool) {
	switch {
	case errors.Is(err, idalloc.ErrExhausted):
		return reasonExhausted, true
	case errors.Is(err, idalloc.ErrStaleAuthority):
		return reasonStale, true
	case errors.Is(err, idalloc.ErrInvalid):
		return reasonInvalid, true
	}
	return reasonNone, false
}
func (q *reader) transition(r request, index uint64) (outcome, bool, error) {
	o := outcome{ns: r.ns, kind: r.kind, identity: r.identity(), index: index, disposition: applied}
	if r.kind == reserveGrant {
		// A persisted grant is inspection/recovery even after service/recipient
		// rotation. It never establishes a fresh delivery or cursor authority.
		g, found, err := q.grant(r)
		if err != nil {
			return o, false, err
		}
		if found {
			if g.grant.Request != r.grantRequest() {
				o.reason = reasonMismatch
				return o, false, nil
			}
			o.disposition, o.grant, o.grantIndex = grantRecovery, g.grant, g.index
			return o, false, nil
		}
	}
	state, found, err := q.allocator()
	if err != nil {
		return o, false, err
	}
	if r.kind == initAllocator {
		if found {
			o.reason = reasonAlreadyInitialized
			return o, false, nil
		}
		if err := q.emptyProof(); err != nil {
			return o, false, err
		}
		state, err = idalloc.NewState(r.ns.graph, r.authority, r.maxBlock)
		if err != nil {
			o.reason, _ = rejectReason(err)
			return o, true, nil
		}
		wire, err := idalloc.MarshalCheckpoint(&state)
		if err != nil {
			return o, false, err
		}
		return o, true, q.put(allocatorKey(q.ns), wire)
	}
	if !found {
		if err := q.emptyProof(); err != nil {
			// The backend uses ErrInvalid for both stale and nonempty views.
			// Insufficient proof refuses; it never authorizes missing-state init.
			return o, false, err
		}
		return o, false, errNotInitialized
	}
	switch r.kind {
	case transferAllocator:
		current, err := idalloc.Inspect(&state)
		if err != nil {
			return o, false, err
		}
		if current.Authority == r.replacement {
			o.disposition = controlRecovery
			return o, false, nil
		}
		next, err := idalloc.Transfer(&state, r.authority, r.replacement)
		if err != nil {
			if why, ok := rejectReason(err); ok {
				o.reason = why
				return o, false, nil
			}
			return o, false, err
		}
		wire, err := idalloc.MarshalCheckpoint(&next)
		if err != nil {
			return o, false, err
		}
		return o, false, q.put(allocatorKey(q.ns), wire)
	case activateRecipient:
		if r.home != q.ns.partition {
			o.reason = reasonInvalid
			return o, false, nil
		}
		current, found, err := q.recipient(r.session.ID)
		if err != nil {
			return o, false, err
		}
		if found && current.session == r.session && r.expectedEpoch != math.MaxUint64 && r.session.Epoch == r.expectedEpoch+1 {
			o.disposition = controlRecovery
			return o, false, nil // do not reset sequence
		}
		if r.expectedEpoch == math.MaxUint64 {
			o.reason = reasonExhausted
			return o, false, nil
		}
		if found && current.session.Epoch != r.expectedEpoch || !found && r.expectedEpoch != 0 {
			o.reason = reasonStale
			return o, false, nil
		}
		if r.session.Epoch != r.expectedEpoch+1 {
			o.reason = reasonInvalid
			return o, false, nil
		}
		next := recipientRecord{session: r.session, home: r.home, index: index}
		wire, err := encodeRecipient(q.ns, next)
		if err != nil {
			return o, false, err
		}
		return o, false, q.put(recipientKey(q.ns, r.session.ID), wire)
	case reserveGrant:
		recipient, found, err := q.recipient(r.session.ID)
		if err != nil {
			return o, false, err
		}
		if !found || recipient.session != r.session {
			o.reason = reasonStale
			return o, false, nil
		}
		if r.sequence <= recipient.sequence {
			o.reason = reasonMismatch
			return o, false, nil
		}
		current, err := idalloc.Inspect(&state)
		if err != nil {
			return o, false, err
		}
		if current.Authority != r.authority {
			o.reason = reasonStale
			return o, false, nil
		}
		if current.LastSequence == math.MaxUint64 {
			o.reason = reasonExhausted
			return o, false, nil
		}
		next, block, replay, err := idalloc.Reserve(&state, idalloc.Request{Graph: r.ns.graph, Authority: r.authority, Sequence: current.LastSequence + 1, Count: r.count})
		if err != nil {
			if why, ok := rejectReason(err); ok {
				o.reason = why
				return o, false, nil
			}
			return o, false, err
		}
		if replay {
			return o, false, errCorrupt
		}
		g := idalloc.Grant{Request: r.grantRequest(), Reservation: block}
		wire, err := idalloc.MarshalCheckpoint(&next)
		if err != nil {
			return o, false, err
		}
		if err := q.put(allocatorKey(q.ns), wire); err != nil {
			return o, false, err
		}
		recipient.sequence = r.sequence
		wire, err = encodeRecipient(q.ns, recipient)
		if err != nil {
			return o, false, err
		}
		if err := q.put(recipientKey(q.ns, r.session.ID), wire); err != nil {
			return o, false, err
		}
		wire, err = encodeGrant(q.ns, grantRecord{grant: g, index: index})
		if err != nil {
			return o, false, err
		}
		if err := q.put(grantKey(q.ns, r.session, r.sequence), wire); err != nil {
			return o, false, err
		}
		o.grant, o.grantIndex = g, index
		return o, false, nil
	}
	return o, false, errCorrupt
}

// effectsMetadataBytes conservatively covers the fixed returned allocationEffects
// value, including its decoded outcome/grant and slice/root metadata. Codec
// scratch is separately fixed by the wire sizes (no input-sized collections);
// this owned-representation ledger is not Go allocator/heap/RSS accounting.
const effectsMetadataBytes = 512

func outputCost(e allocationEffects) int {
	total := effectsMetadataBytes + cap(e.base.Image) + cap(e.envelope) + cap(e.changes) + 64*cap(e.writes)
	for _, w := range e.writes {
		total += cap(w.Key) + cap(w.Value)
	}
	return total
}
func checkEffects(e allocationEffects, l limits, p raftlog.ApplicationPolicy, rl raftlog.Limits) error {
	stageBytes := 128
	for _, w := range e.writes {
		stageBytes += 2*len(w.Key) + len(w.Value) + 64
	}
	if len(e.writes) > l.stageRows || stageBytes > l.stageBytes {
		return errLimit
	}
	if outputCost(e) > l.outputBytes {
		return errLimit
	}
	// Pure framing/headroom validation is not reservation or admission. The
	// serialized driver's AdmitApplication and InstallApplication are authoritative.
	b := raftlog.ApplicationBatch{BaseGeneration: e.base.Generation, BaseIndex: e.base.Index, BaseImageHash: e.base.ImageHash, Image: e.base.Image, Writes: e.writes, Changes: e.changes, Outcome: e.envelope}
	if _, err := p.Preflight(b, rl); err != nil {
		if errors.Is(err, raftlog.ErrLimit) {
			return errors.Join(errLimit, err)
		}
		return errors.Join(errInvalid, err)
	}
	return nil
}
func collectEffects(q *reader, o outcome, hash [32]byte, mapRecord bool) (allocationEffects, error) {
	o.hash = hash
	envelope, err := encodeAnyOutcome(o)
	if err != nil {
		return allocationEffects{}, err
	}
	if mapRecord {
		wire := envelope // store full original outcome; charged retained duplication
		if err := q.put(outcomeKey(request{ns: o.ns, kind: o.kind, id: requestID(o.identity), attempt: bootstrapAttemptID(o.identity)}), wire); err != nil {
			return allocationEffects{}, err
		}
	}
	// Transfer the private touched slice; no reader/view-backed alias escapes.
	writes := q.writes
	slices.SortFunc(writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	e := allocationEffects{base: q.base, writes: writes, outcome: o, envelope: envelope}
	if len(q.writes) > 0 && (!mapRecord || len(q.writes) > 1) {
		e.changes = owned(envelope)
	}
	return e, nil
}

func finishEffects(q *reader, o outcome, hash [32]byte, mapRecord bool, p raftlog.ApplicationPolicy, rl raftlog.Limits) (allocationEffects, error) {
	e, err := collectEffects(q, o, hash, mapRecord)
	if err != nil {
		return allocationEffects{}, err
	}
	if err := checkEffects(e, q.limits, p, rl); err != nil {
		return allocationEffects{}, err
	}
	return e, nil
}

// stageAllocation returns private effects for one concrete immutable view. It
// never installs, changes/defines the graph root, builds a cursor or accepts KV
// mutations. Its namespace comes from the outer validated graph root. That
// outer assembler alone combines these effects with graphstore and publishes.
func stageAllocation(ctx context.Context, view *raftlog.ApplicationView, n namespace, pos applyPosition, data []byte, l limits, p raftlog.ApplicationPolicy, rl raftlog.Limits) (allocationEffects, error) {
	if ctx == nil || view == nil || !n.valid() {
		return allocationEffects{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return allocationEffects{}, err
	}
	if err := l.validate(); err != nil {
		return allocationEffects{}, err
	}
	if err := p.Validate(rl); err != nil || !p.Enabled() {
		return allocationEffects{}, errors.Join(errInvalid, err)
	}
	base, err := view.Root()
	if err != nil {
		return allocationEffects{}, err
	}
	if base.Index == math.MaxUint64 || pos.index != base.Index+1 || pos.generation != base.Generation {
		return allocationEffects{}, errInvalid
	}
	if cap(base.Image) > l.readBytes {
		return allocationEffects{}, errLimit
	}
	q := reader{ctx: ctx, view: view, ns: n, limits: l, base: base, bytes: cap(base.Image), stageBytes: 128}
	// Bound delivered work before hashing; only a fixed-size recognized
	// bootstrap header can receive a limit/malformed zero-KV rejection.
	if len(data) > maxRequestBytes {
		return allocationEffects{}, errLimit
	}
	r, decodeErr := decodeRequest(data, l)
	if decodeErr != nil {
		var known bool
		r, known = recognizeBootstrap(data)
		if !known || r.ns != n {
			return allocationEffects{}, decodeErr
		}
	} else if r.ns != n {
		return allocationEffects{}, errInvalid
	}
	hash := sha256.Sum256(data)
	// Request identity/hash lookup is FIRST, before state/epoch/grant checks.
	wire, found, err := q.get(outcomeKey(r))
	if err != nil {
		return allocationEffects{}, err
	}
	if found {
		old, err := decodeAnyOutcome(wire, n)
		if err != nil {
			return allocationEffects{}, err
		}
		if old.identity != r.identity() || old.index > base.Index {
			return allocationEffects{}, errCorrupt
		}
		if old.hash != hash {
			o := outcome{ns: n, kind: r.kind, identity: r.identity(), index: pos.index, disposition: applied, reason: reasonMismatch}
			return finishEffects(&q, o, hash, false, p, rl)
		}
		if old.kind != r.kind {
			return allocationEffects{}, errCorrupt
		}
		old.disposition = requestReplay
		return finishEffects(&q, old, hash, false, p, rl)
	}
	// Exact minimum rejection headroom is checked before transition work. No
	// quota is reserved here; callers must do real serialized driver admission.
	rejection := outcome{ns: n, kind: r.kind, identity: r.identity(), index: pos.index, disposition: applied, reason: reasonInvalid, hash: hash}
	rejectEnvelope, err := encodeOutcome(rejection)
	if err != nil {
		return allocationEffects{}, err
	}
	rejectBatch := allocationEffects{base: base, envelope: rejectEnvelope, writes: []raftlog.KV{{Key: outcomeKey(r), Value: rejectEnvelope}}}
	if r.kind == initAllocator {
		rejectBatch.writes = nil
	}
	if err := checkEffects(rejectBatch, l, p, rl); err != nil {
		return allocationEffects{}, err
	}
	if decodeErr != nil {
		state, present, err := q.allocator()
		_ = state
		if err != nil {
			return allocationEffects{}, err
		}
		if present {
			return finishEffects(&q, rejection, hash, true, p, rl)
		}
		if err := q.emptyProof(); err != nil {
			return allocationEffects{}, err
		}
		o := rejection
		if errors.Is(decodeErr, errLimit) {
			o.reason = reasonLimit
		}
		e, err := finishEffects(&q, o, hash, false, p, rl)
		e.bootstrapRejection = true
		return e, err
	}
	o, freshBootstrap, err := q.transition(r, pos.index)
	if err != nil && (!errors.Is(err, errLimit) || !freshBootstrap && !q.allocatorFound) {
		return allocationEffects{}, err
	}
	if err == nil && freshBootstrap && o.reason != reasonNone {
		q.writes = nil
		q.stageBytes = 128
		e, err := finishEffects(&q, o, hash, false, p, rl)
		e.bootstrapRejection = true
		return e, err
	}
	if err == nil {
		e, finishErr := finishEffects(&q, o, hash, true, p, rl)
		if finishErr == nil {
			return e, nil
		}
		err = finishErr
	}
	if !errors.Is(err, errLimit) {
		return allocationEffects{}, err
	}
	// A computed transition may be too large even though the reserved minimum
	// rejection fits. Discard ALL touched effects before recording that rejection.
	q.writes = nil
	q.stageBytes = 128
	o = rejection
	o.reason = reasonLimit
	e, err := finishEffects(&q, o, hash, !freshBootstrap, p, rl)
	e.bootstrapRejection = freshBootstrap
	return e, err
}
