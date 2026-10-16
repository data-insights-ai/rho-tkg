package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const (
	feedReadCursor byte = iota + 1
	feedReadFrame
	feedReadPayload
	feedReadOriginal
)

type retainedFeedQuestion struct {
	kind                byte
	definition          []byte
	boundary            graphCertificate
	tip                 retainedFeedTip
	index, round        uint64
	request             [16]byte
	phase               byte
	lookup              retainedFeedLookup
	rows, bytes, output int
}
type retainedFeedObservation struct {
	scope               allocationScope
	read                allocationReadID
	nonce               [16]byte
	definition          [32]byte
	index, generation   uint64
	root                [172]byte
	publication         uint64
	status              byte
	rows, bytes, output int
	cursor, page        []byte
	frame               *retainedFeedFrame
	payload             *retainedFeedPayload
}

func emitFeedLookupQuery(w *boundedWriter, q retainedFeedLookup) {
	w.tag(q.kind)
	w.field(q.key)
	w.field(q.upper)
	w.field(q.after)
	w.u32(q.rows)
	w.u32(q.bytes)
}
func readFeedLookupQuery(c *graphCursor) retainedFeedLookup {
	return retainedFeedLookup{kind: c.tag(), key: c.field(64 << 10), upper: c.field(64 << 10), after: c.field(64 << 10), rows: c.count(4096, 0), bytes: c.count(retainedFeedMaxBytes, 0)}
}
func encodeRetainedFeedQuestion(q retainedFeedQuestion) ([]byte, error) {
	if q.kind < feedReadCursor || q.kind > feedReadOriginal || q.rows < 16 || q.rows > 1<<20 || q.bytes < 32768 || q.bytes > 64<<20 || q.output < 32768 || q.output > 64<<20 {
		return nil, errInvalid
	}
	if _, err := decodeRetainedFeedDefinition(q.definition); err != nil {
		return nil, err
	}
	var cert []byte
	var err error
	if q.kind == feedReadFrame || q.kind == feedReadPayload {
		cert, err = encodeGraphCertificate(q.boundary)
		if err != nil {
			return nil, err
		}
	}
	if q.kind == feedReadFrame && q.index == 0 || q.kind == feedReadPayload && q.round == 0 {
		return nil, errInvalid
	}
	if (q.kind == feedReadFrame || q.kind == feedReadPayload) && (q.tip.index < q.boundary.index || q.tip.index == math.MaxUint64) {
		return nil, errInvalid
	}
	if q.kind == feedReadOriginal && (q.request == ([16]byte{}) || q.phase != feedSealPage && q.phase != feedAcknowledge) {
		return nil, errInvalid
	}
	if q.kind != feedReadOriginal && (q.request != ([16]byte{}) || q.phase != 0) {
		return nil, errInvalid
	}
	return boundedEncoding(retainedFeedCommandBytes, func(w *boundedWriter) {
		w.add([]byte("GFQ\x01"))
		w.tag(q.kind)
		w.field(q.definition)
		w.field(cert)
		w.u64(q.index)
		w.u64(q.round)
		w.add(q.request[:])
		w.tag(q.phase)
		w.u64(q.tip.index)
		w.add(q.tip.root[:])
		emitFeedLookupQuery(w, q.lookup)
		w.u32(q.rows)
		w.u32(q.bytes)
		w.u32(q.output)
	})
}
func decodeRetainedFeedQuestion(wire []byte) (retainedFeedQuestion, error) {
	body, err := graphBody(wire, "GFQ\x01", retainedFeedCommandBytes)
	if err != nil {
		return retainedFeedQuestion{}, err
	}
	c := graphCursor{b: body}
	q := retainedFeedQuestion{kind: c.tag(), definition: c.field(graphPinManifestBytes + 128)}
	cert := c.field(512)
	q.index, q.round = c.u64(), c.u64()
	q.request = c.array()
	q.phase = c.tag()
	q.tip.index = c.u64()
	copy(q.tip.root[:], c.take(172))
	q.lookup = readFeedLookupQuery(&c)
	q.rows, q.bytes, q.output = c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	// Primitive framing is allocation-bounded by delivered bytes. Admit the
	// variable canonical GFD/GPM decode before it can allocate its owned roster.
	if c.err != nil || q.rows < 16 || q.bytes < 32768 || q.output < 32768 {
		return retainedFeedQuestion{}, errInvalid
	}
	if q.output < 32768+4096+8*len(wire) {
		return retainedFeedQuestion{}, errLimit
	}
	if len(cert) != 0 {
		q.boundary, err = decodeGraphCertificate(cert)
		if err != nil {
			return retainedFeedQuestion{}, err
		}
	}
	canonical, err := encodeRetainedFeedQuestion(q)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedQuestion{}, errors.Join(errInvalid, c.err, err)
	}
	return q, nil
}
func encodeRetainedFeedObservation(o retainedFeedObservation) ([]byte, error) {
	if !retainedFeedAgreement(o.scope.semantic) || !o.scope.valid() || o.read.session == ([16]byte{}) || o.read.sequence == 0 || o.nonce == ([16]byte{}) || o.index == 0 || o.status < feedApplied || o.status > feedLimited || o.rows < 0 || o.bytes < 0 || o.output < 0 || len(o.cursor) > graphPinManifestBytes+2048 || len(o.page) > retainedFeedMaxBytes {
		return nil, errInvalid
	}
	return boundedEncoding(2*retainedFeedMaxBytes+graphPinManifestBytes+8192, func(w *boundedWriter) { emitRetainedFeedObservation(w, o) })
}
func emitRetainedFeedObservation(w *boundedWriter, o retainedFeedObservation) {
	w.add([]byte("GFS\x01"))
	w.add(appendAllocationScope(nil, o.scope))
	w.add(o.read.session[:])
	w.u64(o.read.sequence)
	w.add(o.nonce[:])
	w.add(o.definition[:])
	w.u64(o.index)
	w.u64(o.generation)
	w.add(o.root[:])
	w.u64(o.publication)
	w.tag(o.status)
	w.u32(o.rows)
	w.u32(o.bytes)
	w.u32(o.output)
	w.field(o.cursor)
	w.field(o.page)
	if o.frame == nil {
		w.tag(0)
	} else {
		w.tag(1)
		emitFeedFrame(w, *o.frame)
	}
	if o.payload == nil {
		w.tag(0)
	} else {
		w.tag(1)
		emitFeedPayload(w, *o.payload)
	}

}
func decodeRetainedFeedObservation(wire []byte) (retainedFeedObservation, error) {
	body, err := graphBody(wire, "GFS\x01", 2*retainedFeedMaxBytes+graphPinManifestBytes+8192)
	if err != nil {
		return retainedFeedObservation{}, err
	}
	c := graphCursor{b: body, maxOwnedBytes: 8*len(wire) + 4096}
	scope := c.take(allocationScopeBytes - 36)
	o := retainedFeedObservation{scope: readAllocationScope(&decoder{b: scope}), read: allocationReadID{session: c.array(), sequence: c.u64()}, nonce: c.array()}
	copy(o.definition[:], c.take(32))
	o.index, o.generation = c.u64(), c.u64()
	copy(o.root[:], c.take(172))
	o.publication = c.u64()
	o.status = c.tag()
	o.rows, o.bytes, o.output = c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	o.cursor = c.field(graphPinManifestBytes + 2048)
	o.page = c.field(retainedFeedMaxBytes)
	flag := c.tag()
	if flag > 1 {
		return retainedFeedObservation{}, errInvalid
	}
	if flag == 1 {
		o.frame = new(readFeedFrame(&c))
	}
	flag = c.tag()
	if flag > 1 {
		return retainedFeedObservation{}, errInvalid
	}
	if flag == 1 {
		o.payload = new(readFeedPayload(&c))
	}
	canonical, err := encodeRetainedFeedObservation(o)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedObservation{}, errors.Join(errInvalid, c.err, err)
	}
	return o, nil
}

func (s *graphPeerService) readRetainedFeed(ctx context.Context, r graphSourceRequest) ([]byte, error) {
	if s == nil || ctx == nil || !retainedFeedAgreement(s.host.machine.SemanticContractID()) {
		return nil, errInvalid
	}
	question, err := decodeRetainedFeedQuestion(r.command)
	if err != nil {
		return nil, err
	}
	codecCost := 4096 + 8*len(r.command)
	query := graphCutQuery{kind: 1, nonce: r.nonce, sourceRows: question.rows, sourceBytes: question.bytes, outputBytes: question.output - codecCost}
	reply, err := s.pump.Cut(ctx, query)
	if err != nil || reply.err != nil {
		return nil, errors.Join(err, reply.err)
	}
	if reply.question != query || reply.id != reply.proof.id || reply.proof.owner != s.host {
		return nil, errInvalid
	}
	if err = checkGraphCutProof(reply.proof, query); err != nil {
		return nil, err
	}
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if s.host.unavailableLocked() {
		return nil, replica.ErrStopped
	}
	m := s.host.machine
	captured := reply.proof.observation
	if captured.sourceRows >= question.rows || captured.sourceBytes >= question.bytes || captured.outputBytes >= question.output {
		return nil, errLimit
	}
	view, err := m.store.ApplicationView(captured.index)
	if err != nil {
		return nil, err
	}
	var out []byte
	base, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, errors.Join(err, view.Close())
	}
	arena, err := graphstate.NewOutputBudget(question.output - captured.outputBytes)
	if err != nil {
		return nil, errors.Join(err, view.Close())
	}
	if err = arena.Reserve(codecCost); err != nil {
		return nil, errors.Join(outputBudgetFailure(err), view.Close())
	}
	q := feedReader(ctx, m, view, base, arena)
	q.limits.readRows = min(q.limits.readRows, question.rows)
	q.limits.readBytes = min(q.limits.readBytes, question.bytes)
	q.rows, q.bytes = captured.sourceRows, captured.sourceBytes+cap(base.Image)
	inspect := func() (retainedFeedObservation, error) {
		d, e := decodeRetainedFeedDefinition(question.definition)
		if e != nil {
			return retainedFeedObservation{}, e
		}
		manifest, owner, status, e := m.feedLive(&q, d)
		if e != nil {
			return retainedFeedObservation{}, e
		}
		o := retainedFeedObservation{scope: m.scope(), read: reply.id, nonce: r.nonce, definition: sha256.Sum256(question.definition), index: base.Index, generation: base.Generation, publication: owner.publication, status: status}
		copy(o.root[:], base.Image)
		if status != feedApplied {
			return o, nil
		}
		cursor, found, e := readFeedCursor(&q, manifest.id())
		if e != nil {
			return o, e
		}
		if !found {
			o.status = feedPending
			return o, nil
		}
		stored, present, e := crossControl(&q, retainedFeedDefinitionKey(m.ns, manifest.id()), graphPinManifestBytes+128)
		if e != nil || !present || !bytes.Equal(stored.Value, question.definition) {
			return o, errors.Join(errInvalid, e)
		}
		if question.kind == feedReadCursor {
			o.cursor, e = encodeRetainedFeedCursor(cursor)
			if e != nil {
				return o, e
			}
			pageID := cursor.pending
			if pageID == ([32]byte{}) {
				pageID = cursor.acknowledged
			}
			if pageID != ([32]byte{}) {
				page, present, e := crossControl(&q, retainedFeedPageKey(m.ns, manifest.id(), pageID), d.policy.pageBytes)
				if e != nil || !present || sha256.Sum256(page.Value) != pageID {
					return o, errors.Join(errCorrupt, e)
				}
				o.page = page.Value
			}
			return o, nil
		}
		if question.kind == feedReadOriginal {
			action := question.request
			if question.phase == feedAcknowledge {
				action = retainedFeedPhaseAction(action, feedAcknowledge)
			}
			row, present, e := crossControl(&q, retainedFeedOriginalKey(m.ns, manifest.issuer, action), 512)
			if e != nil || !present {
				return o, e
			}
			body, e := graphBody(row.Value, "GFX\x01", 512)
			if e != nil || len(body) < 32 {
				return o, errors.Join(errCorrupt, e)
			}
			result, e := decodeRetainedFeedResult(body[32:])
			if e != nil {
				return o, e
			}
			if result.status != feedApplied {
				o.status = result.status
				return o, nil
			}
			page, present, e := crossControl(&q, retainedFeedPageKey(m.ns, manifest.id(), result.page), d.policy.pageBytes)
			if e != nil || !present || sha256.Sum256(page.Value) != result.page {
				return o, errors.Join(errCorrupt, e)
			}
			decoded, e := decodeRetainedFeedPage(page.Value, d.policy.pageBytes, arena)
			if e != nil || decoded.request != question.request || decoded.sequence != result.sequence {
				return o, errors.Join(errCorrupt, e)
			}
			command := retainedFeedCommand{scope: m.scope(), issuer: manifest.issuer, action: action, phase: question.phase, definition: question.definition, page: page.Value, original: manifest.id()}
			if e = reserveComposer(&q, 4096+8*(len(command.definition)+len(command.page))); e != nil {
				return o, e
			}
			wire, e := encodeRetainedFeedCommand(command)
			if e != nil {
				return o, e
			}
			exact, found, e := feedActionOriginal(&q, command, wire)
			if e != nil || !found || exact != result {
				return o, errors.Join(errCorrupt, e)
			}
			o.page = page.Value
			return o, nil
		}
		cert := question.boundary
		if cert.scope != m.scope() || cert.round < manifest.parts[0].certificate.round {
			return o, errInvalid
		}
		wire, present, e := q.getBounded(graphCertificateKey(m.ns, cert.round), 512)
		if e != nil || !present {
			return o, errors.Join(replica.ErrUnavailable, e)
		}
		actual, e := decodeGraphCertificate(wire)
		if e != nil || actual != cert {
			return o, errors.Join(errInvalid, e)
		}
		if e = m.checkFeedTip(&q, question.tip, cert); e != nil {
			return o, e
		}
		if question.kind == feedReadFrame {
			local := -1
			for i, p := range manifest.parts {
				if p.certificate.scope.partition == m.ns.partition {
					local = i
				}
			}
			if local < 0 || question.index != cursor.positions[local] || cursor.pending != ([32]byte{}) || question.index < owner.feedStart {
				return o, errInvalid
			}
			if question.index <= question.tip.index {
				f, e := m.feedProgress(&q, question.index, question.index+min(question.tip.index-question.index, retainedFeedProgressBatch-1), d.policy.pageBytes, cursor.positions[local])
				if e != nil {
					return o, e
				}
				o.frame = &f
			}
			return o, nil
		}
		if question.round < manifest.parts[0].certificate.round || question.round > cert.round {
			return o, errInvalid
		}
		_, index, image, e := m.feedPayloadSource(&q, manifest, cert.configuration, question.round)
		if e != nil {
			return o, e
		}
		payload := retainedFeedPayload{partition: m.ns.partition, index: index, root: image}
		e = m.feedAt(&q, index, func(at *reader) error {
			o.generation = at.base.Generation
			lookup := question.lookup
			if lookup.kind == 0 {
				if len(lookup.key)+len(lookup.upper)+len(lookup.after)+lookup.rows+lookup.bytes != 0 {
					return errInvalid
				}
				return nil
			}
			if len(lookup.key) == 0 || lookup.rows < 1 || lookup.rows > at.limits.readRows-at.rows || lookup.bytes < 1 || lookup.bytes > at.limits.readBytes-at.bytes || lookup.bytes > at.view.ReadLimits().Bytes {
				return errLimit
			}
			// Admission reserves peak capacity logically while this serial reader
			// performs its one native operation. Debit only the actual owned output;
			// no concurrent consumer or fresh arena can spend the offered headroom.
			if 4096+4*lookup.bytes > at.arena.Remaining() {
				return errLimit
			}
			if e := reserveComposer(at, 1024+4*(len(lookup.key)+len(lookup.upper)+len(lookup.after))); e != nil {
				return e
			}
			if lookup.kind == 1 {
				if lookup.rows != 1 || len(lookup.upper)+len(lookup.after) != 0 {
					return errInvalid
				}
				row, found, e := at.view.Get(ctx, lookup.key, lookup.bytes)
				at.rows++
				if e != nil {
					at.bytes += lookup.bytes
					return e
				}
				at.bytes += 128 + len(lookup.key) + len(row.Key) + len(row.Value)
				if e = reserveComposer(at, 256+4*(cap(row.Key)+cap(row.Value))); e != nil {
					return e
				}
				lookup.found = found
				if found {
					lookup.values = []raftlog.KV{row}
				}
			} else if lookup.kind == 2 {
				page, e := at.view.Scan(ctx, lookup.key, lookup.upper, lookup.after, raftlog.ReadBudget{Rows: lookup.rows, Bytes: lookup.bytes})
				at.rows += page.Visited
				at.bytes += page.Bytes
				if e != nil {
					return e
				}
				if e = reserveComposer(at, 256+4*page.Bytes); e != nil {
					return e
				}
				lookup.values, lookup.complete, lookup.next = page.Rows, page.Complete, page.Next
				lookup.visited, lookup.workBytes = page.Visited, page.Bytes
			} else {
				return errInvalid
			}
			payload.lookups = []retainedFeedLookup{lookup}
			return nil
		})
		if e != nil {
			return o, e
		}
		o.payload = &payload
		return o, nil
	}
	observation, inspectErr := inspect()
	observation.rows, observation.bytes = q.rows, q.bytes
	observation.output = question.output - arena.Remaining()
	if inspectErr == nil {
		// Size the exact delivered response, then fund its ownership/codec on
		// this SAME native residual before allocation. A small lookup/census is
		// not charged as a configured-maximum page on every RPC.
		size := boundedWriter{max: 2*retainedFeedMaxBytes + graphPinManifestBytes + 8192 - 32, sizing: true}
		emitRetainedFeedObservation(&size, observation)
		inspectErr = encodingFailure(size.err)
		if inspectErr == nil {
			inspectErr = reserveComposer(&q, 2048+4*(size.n+32))
		}
		observation.output = question.output - arena.Remaining()
		if inspectErr == nil {
			out, inspectErr = encodeRetainedFeedObservation(observation)
		}
	}
	closeErr := view.Close()
	if err = errors.Join(inspectErr, closeErr); err != nil {
		return nil, err
	}
	return out, nil
}

func (e *graphAuthenticatedEndpoint) retainedFeedRead(ctx context.Context, q retainedFeedQuestion) (out retainedFeedObservation, err error) {
	if e == nil || ctx == nil || e.budget == nil || !retainedFeedAgreement(e.peer.scope.semantic) {
		return out, errInvalid
	}
	b := e.budget
	const replyMax = 2*retainedFeedMaxBytes + graphPinManifestBytes + 8192
	b.mu.Lock()
	codecErr := b.output.Reserve(4096 + 8*(len(q.definition)+len(q.lookup.key)+len(q.lookup.upper)+len(q.lookup.after)+1024))
	q.rows = min(1<<20, b.maxRows-b.rows)
	q.bytes = min(64<<20, b.maxBytes-b.bytes-(replyMax+len(q.definition)+4096))
	q.output = min(64<<20, b.output.Remaining()-(4*(replyMax+len(q.definition)+4096)))
	beforeCalls, beforeRows, beforeBytes, beforeOutput := b.calls, b.rows, b.bytes, b.output.Remaining()
	b.mu.Unlock()
	if codecErr != nil {
		return out, outputBudgetFailure(codecErr)
	}
	if q.rows < 16 || q.bytes < 32768 || q.output < 32768 {
		return out, errLimit
	}
	wire, err := encodeRetainedFeedQuestion(q)
	if err != nil {
		return out, err
	}
	if q.output < 32768+4096+8*len(wire) {
		return out, errLimit
	}
	responseReceived := false
	defer func() {
		if err != nil {
			origin, cause := fmt.Sprintf("%T", err), graphErrorMask(err)
			b.mu.Lock()
			b.rows += q.rows
			b.bytes += q.bytes
			charge := b.output.Reserve(q.output)
			b.mu.Unlock()
			err = errors.Join(err, outputBudgetFailure(charge))
			err = fmt.Errorf("retained feed read kind=%d part=%d index=%d beforeCalls=%d beforeRows=%d beforeBytes=%d beforeOutput=%d offered=%d/%d/%d response=%t origin=%s cause=%04x: %w", q.kind, e.peer.scope.partition, q.index, beforeCalls, beforeRows, beforeBytes, beforeOutput, q.rows, q.bytes, q.output, responseReceived, origin, cause, err)
			out = retainedFeedObservation{}
		}
	}()
	response, err := e.call(ctx, graphSourceRequest{kind: sourceCrossRead, command: wire}, replyMax)
	if err != nil {
		return out, err
	}
	responseReceived = true
	if err = b.output.Reserve(4096 + 8*len(response.body)); err != nil {
		return out, outputBudgetFailure(err)
	}
	out, err = decodeRetainedFeedObservation(response.body)
	if err != nil {
		return out, err
	}
	if out.scope != e.peer.scope || out.read.session != response.issuer || out.nonce != response.nonce || out.definition != sha256.Sum256(q.definition) || out.rows > q.rows || out.bytes > q.bytes || out.output > q.output {
		return retainedFeedObservation{}, errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if out.rows > b.maxRows-b.rows || out.bytes > b.maxBytes-b.bytes {
		return retainedFeedObservation{}, errLimit
	}
	if err = b.output.Reserve(out.output); err != nil {
		return retainedFeedObservation{}, outputBudgetFailure(err)
	}
	b.rows += out.rows
	b.bytes += out.bytes
	return out, nil
}

func retainedFeedStatusError(status byte) error {
	switch status {
	case feedApplied:
		return nil
	case feedPending:
		return errors.Join(errRetainedFeedPending, replica.ErrUnavailable)
	case feedExpired:
		return errGraphPinExpired
	case feedReleased:
		return errGraphPinReleased
	case feedLimited:
		return errLimit
	default:
		return errInvalid
	}
}

// This private data source records only native completed reads at its original
// round root. The same invocation budget funds every request and every proof
// copy. Its contents gain original authority only after all local Stage checks.
type retainedFeedRecordingSource struct {
	ctx        context.Context
	endpoint   *graphAuthenticatedEndpoint
	question   retainedFeedQuestion
	payload    retainedFeedPayload
	generation uint64
	arena      *graphstate.OutputBudget
}

func newRetainedFeedRecordingSource(ctx context.Context, e *graphAuthenticatedEndpoint, q retainedFeedQuestion, arena *graphstate.OutputBudget) (*retainedFeedRecordingSource, error) {
	if ctx == nil || arena == nil {
		return nil, errInvalid
	}
	q.kind = feedReadPayload
	q.lookup = retainedFeedLookup{}
	o, err := e.retainedFeedRead(ctx, q)
	if err != nil {
		return nil, err
	}
	if err = retainedFeedStatusError(o.status); err != nil {
		return nil, err
	}
	if o.payload == nil || o.payload.partition != e.peer.scope.partition || len(o.payload.lookups) != 0 {
		return nil, errInvalid
	}
	return &retainedFeedRecordingSource{ctx: ctx, endpoint: e, question: q, payload: *o.payload, generation: o.generation, arena: arena}, nil
}
func (s *retainedFeedRecordingSource) Root() (raftlog.ApplicationRoot, error) {
	if s == nil {
		return raftlog.ApplicationRoot{}, errInvalid
	}
	return s.RootBounded(s.ctx, 172)
}
func (s *retainedFeedRecordingSource) RootBounded(ctx context.Context, maximum int) (raftlog.ApplicationRoot, error) {
	if s == nil || ctx == nil {
		return raftlog.ApplicationRoot{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	if maximum < 172 {
		return raftlog.ApplicationRoot{}, errLimit
	}
	if err := s.arena.Reserve(512); err != nil {
		return raftlog.ApplicationRoot{}, outputBudgetFailure(err)
	}
	wire := owned(s.payload.root[:])
	return raftlog.ApplicationRoot{Generation: s.generation, Index: s.payload.index, Image: wire, ImageHash: sha256.Sum256(wire)}, nil
}
func (s *retainedFeedRecordingSource) ApplicationBinding(ctx context.Context) (raftlog.ApplicationBinding, error) {
	if s == nil || ctx == nil {
		return raftlog.ApplicationBinding{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return raftlog.ApplicationBinding{}, err
	}
	p := s.endpoint.peer.scope
	return raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(p.graph), Partition: p.partition, Group: p.group}, SemanticContractID: p.semantic}, nil
}
func (s *retainedFeedRecordingSource) ReadLimits() raftlog.ReadBudget {
	if s == nil {
		return raftlog.ReadBudget{}
	}
	return raftlog.ReadBudget{Rows: 4096, Bytes: retainedFeedMaxBytes}
}
func (s *retainedFeedRecordingSource) read(ctx context.Context, lookup retainedFeedLookup) (retainedFeedLookup, error) {
	if s == nil || ctx == nil {
		return retainedFeedLookup{}, errInvalid
	}
	if len(s.payload.lookups) >= retainedFeedMaxProofs {
		return retainedFeedLookup{}, errLimit
	}
	if err := s.arena.Reserve(1024 + 4*(len(lookup.key)+len(lookup.upper)+len(lookup.after))); err != nil {
		return retainedFeedLookup{}, outputBudgetFailure(err)
	}
	q := s.question
	q.lookup = lookup
	o, err := s.endpoint.retainedFeedRead(ctx, q)
	if err != nil {
		return retainedFeedLookup{}, err
	}
	if err = retainedFeedStatusError(o.status); err != nil {
		return retainedFeedLookup{}, err
	}
	if o.payload == nil || o.payload.partition != s.payload.partition || o.payload.index != s.payload.index || o.payload.root != s.payload.root || len(o.payload.lookups) != 1 {
		return retainedFeedLookup{}, errInvalid
	}
	actual := o.payload.lookups[0]
	if actual.kind != lookup.kind || actual.rows != lookup.rows || actual.bytes != lookup.bytes || !bytes.Equal(actual.key, lookup.key) || !bytes.Equal(actual.upper, lookup.upper) || !bytes.Equal(actual.after, lookup.after) {
		return retainedFeedLookup{}, errInvalid
	}
	capacity, err := s.arena.ReserveSlice(len(s.payload.lookups), cap(s.payload.lookups), 256)
	if err != nil {
		return retainedFeedLookup{}, outputBudgetFailure(err)
	}
	if capacity > cap(s.payload.lookups) {
		next := make([]retainedFeedLookup, len(s.payload.lookups), capacity)
		copy(next, s.payload.lookups)
		s.payload.lookups = next
	}
	s.payload.lookups = append(s.payload.lookups, actual)
	return actual, nil
}
func (s *retainedFeedRecordingSource) Get(ctx context.Context, key []byte, maximum int) (raftlog.KV, bool, error) {
	q, err := s.read(ctx, retainedFeedLookup{kind: 1, key: key, rows: 1, bytes: maximum})
	if err != nil {
		return raftlog.KV{}, false, err
	}
	if !q.found {
		return raftlog.KV{}, false, nil
	}
	if len(q.values) != 1 {
		return raftlog.KV{}, false, errInvalid
	}
	return q.values[0], true, nil
}
func (s *retainedFeedRecordingSource) Scan(ctx context.Context, lower, upper, after []byte, budget raftlog.ReadBudget) (raftlog.ApplicationPage, error) {
	q, err := s.read(ctx, retainedFeedLookup{kind: 2, key: lower, upper: upper, after: after, rows: budget.Rows, bytes: budget.Bytes})
	if err != nil {
		return raftlog.ApplicationPage{}, err
	}
	return raftlog.ApplicationPage{Rows: q.values, Next: q.next, Complete: q.complete, Visited: q.visited, Bytes: q.workBytes}, nil
}

var _ graphstore.ApplicationReadSource = (*retainedFeedRecordingSource)(nil)

func (e *graphAuthenticatedEndpoint) submitRetainedFeed(ctx context.Context, c retainedFeedCommand) (retainedFeedResult, error) {
	if e == nil || ctx == nil || e.budget == nil || c.scope != e.peer.scope || !retainedFeedAgreement(c.scope.semantic) {
		return retainedFeedResult{}, errInvalid
	}
	if err := e.budget.output.Reserve(4096 + 8*(len(c.definition)+len(c.page)+allocationScopeBytes+128)); err != nil {
		return retainedFeedResult{}, outputBudgetFailure(err)
	}
	wire, err := encodeRetainedFeedCommand(c)
	if err != nil {
		return retainedFeedResult{}, err
	}
	b := e.budget
	b.mu.Lock()
	if pinLookupRows > b.maxRows-b.rows || pinLookupBytes > b.maxBytes-b.bytes {
		b.mu.Unlock()
		return retainedFeedResult{}, errLimit
	}
	err = b.output.Reserve(pinLookupOutput)
	if err == nil {
		b.rows += pinLookupRows
		b.bytes += pinLookupBytes
	}
	b.mu.Unlock()
	if err != nil {
		return retainedFeedResult{}, outputBudgetFailure(err)
	}
	proposal, err := encodeGraphSourceProposal(allocationProposal{target: c.scope, wire: wire})
	if err != nil {
		return retainedFeedResult{}, err
	}
	reply, err := e.call(ctx, graphSourceRequest{kind: sourceSubmit, command: proposal}, 1024)
	if err != nil {
		return retainedFeedResult{}, err
	}
	body, err := graphBody(reply.body, "GFR\x01", 1024)
	if err != nil {
		return retainedFeedResult{}, err
	}
	r := graphCursor{b: body}
	rows, byteWork, output := r.count(pinLookupRows, 0), r.count(pinLookupBytes, 0), r.count(pinLookupOutput, 0)
	result, err := decodeRetainedFeedResult(r.field(256))
	if r.err != nil || len(r.b) != 0 || err != nil || result.command != sha256.Sum256(wire) || result.action != c.action || result.phase != c.phase {
		return retainedFeedResult{}, errors.Join(errInvalid, r.err, err)
	}
	_ = rows
	_ = byteWork
	_ = output // this fixed lookup grant was prepaid, without refunds
	return result, nil
}
func (s *graphPeerService) submitRetainedFeed(ctx context.Context, p allocationProposal) ([]byte, error) {
	c, err := decodeRetainedFeedCommand(p.wire)
	if err != nil || p.target != s.peer.scope || c.scope != s.peer.scope || !retainedFeedAgreement(c.scope.semantic) {
		return nil, errors.Join(errInvalid, err)
	}
	// Full canonical GFL/GFD/GPM decode is funded by the variable command debit
	// on the original invocation, not hidden inside this fixed native lookup.
	arena, err := graphstate.NewOutputBudget(pinLookupOutput)
	if err != nil {
		return nil, err
	}
	if err = arena.Reserve(2048); err != nil {
		return nil, outputBudgetFailure(err)
	}
	var result retainedFeedResult
	var found bool
	var rows, byteWork int
	lookup := func() (err error) {
		s.host.mu.Lock()
		defer s.host.mu.Unlock()
		if s.host.unavailableLocked() {
			return replica.ErrStopped
		}
		view, err := s.host.machine.store.ApplicationView(s.host.driver.Applied())
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, view.Close()) }()
		base, err := view.RootBounded(ctx, 172)
		if err != nil {
			return err
		}
		q := feedReader(ctx, s.host.machine, view, base, arena)
		q.limits.readRows, q.limits.readBytes = pinLookupRows, pinLookupBytes
		q.rows, q.bytes = 1, 512+4*cap(base.Image)
		result, found, err = feedActionOriginal(&q, c, p.wire)
		rows, byteWork = q.rows, q.bytes
		return err
	}
	if err = lookup(); err != nil {
		return nil, err
	}
	var outcome []byte
	if found {
		outcome, err = encodeRetainedFeedResult(result)
	} else {
		outcome, err = s.pump.Submit(ctx, p)
	}
	if err != nil {
		return nil, err
	}
	if _, err = decodeRetainedFeedResult(outcome); err != nil {
		return nil, err
	}
	return boundedEncoding(1024, func(w *boundedWriter) {
		w.add([]byte("GFR\x01"))
		w.u32(rows)
		w.u32(byteWork)
		w.u32(pinLookupOutput - arena.Remaining())
		w.field(outcome)
	})
}

// The inherited durable-column projector authenticates the same fresh native
// metadata originals. It remains a derived column path, never feed authority.
func (s *nativeColumnSource) retainedFeedMetadataFrame(ctx context.Context, group int, out nativeColumnFrame) (nativeColumnFrame, error) {
	if s == nil || ctx == nil || group < 0 || group >= len(s.hosts) || s.hosts[group] == nil || !retainedFeedAgreement(s.hosts[group].machine.SemanticContractID()) {
		return out, errInvalid
	}
	h, view := s.hosts[group], s.views[group]
	arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if err != nil {
		return out, err
	}
	q := feedReader(ctx, h.machine, view, s.roots[group], arena)
	previous, err := view.RootAt(ctx, out.Index-1, 172)
	if err != nil || !bytes.Equal(previous.Image, out.Root[:]) {
		return out, errors.Join(errCorrupt, err)
	}
	index := uint64(0)
	pending := false
	switch {
	case bytes.HasPrefix(out.Outcome, []byte("GPO\x01")):
		r, e := decodeGraphPinResult(out.Outcome)
		if e != nil {
			return out, e
		}
		index, pending = r.index, r.status == pinPending
		if len(out.CDC) > 0 && (index != out.Index || r.status != pinApplied || !bytes.Equal(out.CDC, out.Outcome)) {
			return out, errCorrupt
		}
	case bytes.HasPrefix(out.Outcome, []byte("GGO\x01")):
		r, e := decodeGraphRetentionResult(out.Outcome)
		if e != nil {
			return out, e
		}
		index = r.index
		if len(out.CDC) != 0 {
			return out, errCorrupt
		}
	case retainedFeedOutcomeFor(h.machine.SemanticContractID(), out.Outcome):
		r, e := decodeRetainedFeedResult(out.Outcome)
		if e != nil {
			return out, e
		}
		index, pending = r.index, r.status == feedPending
		if len(out.CDC) != 0 {
			return out, errCorrupt
		}
	default:
		return out, errInvalid
	}
	if index == 0 || index > out.Index || pending && (index != out.Index || len(out.CDC) != 0) {
		return out, errCorrupt
	}
	if pending {
		return out, nil
	}
	locator, err := feedMetadataOriginal(&q, index, out.Outcome, h.machine.SemanticContractID())
	if err != nil {
		return out, err
	}
	body, err := graphBody(locator, "GFM\x01", 1024)
	if err != nil {
		return out, err
	}
	c := graphCursor{b: body}
	key := c.field(512)
	c.take(32)
	if c.err != nil || len(c.b) != 0 {
		return out, errors.Join(errCorrupt, c.err)
	}
	row, found, err := crossControl(&q, key, retainedFeedCommandBytes+512)
	if err != nil || !found {
		return out, errors.Join(errCorrupt, err)
	}
	if err = reserveComposer(&q, 2048+8*(len(row.Key)+len(row.Value))); err != nil {
		return out, err
	}
	out.Registration, err = boundedEncoding(s.binding.Policy.FrameBytes, func(w *boundedWriter) { w.add([]byte("NCO1")); w.field(row.Key); w.u64(row.Index); w.field(row.Value) })
	if err != nil {
		return out, err
	}
	if index < out.Index {
		if len(out.CDC) != 0 {
			return out, errCorrupt
		}
		if err = s.replay(ctx, group, &out, index); err != nil {
			return out, err
		}
		if !bytes.Equal(out.OriginalOutcome, out.Outcome) {
			return out, errCorrupt
		}
	}
	return out, nil
}
