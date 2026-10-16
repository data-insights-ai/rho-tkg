package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
)

// GPQ is an opt-in question on the existing nonempty Cross door. Empty Cross
// and GCC capacity questions keep their original wire and protocol meaning.
type retainedPinQuestion struct {
	manifest            []byte // nil asks only for the actual current replicated policy epoch
	rows, bytes, output int
	nonce               [16]byte
}
type retainedPinObservation struct {
	scope               allocationScope
	read                allocationReadID
	nonce               [16]byte
	index, epoch        uint64
	configuration       [32]byte
	result              graphPinResult
	issued              graphPinResult
	cancelled           bool
	owner               graphPinOwner
	manifest            []byte
	status              byte
	rows, bytes, output int
}

func encodeRetainedPinQuestion(q retainedPinQuestion) ([]byte, error) {
	if q.rows < 16 || q.rows > 1<<20 || q.bytes < 32768 || q.bytes > 64<<20 || q.output < 32768 || q.output > 64<<20 || len(q.manifest) > graphPinManifestBytes {
		return nil, errInvalid
	}
	if len(q.manifest) != 0 {
		if _, err := decodeGraphPinManifest(q.manifest); err != nil {
			return nil, err
		}
	}
	return boundedEncoding(graphPinManifestBytes+128, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'Q', 1})
		w.u32(q.rows)
		w.u32(q.bytes)
		w.u32(q.output)
		w.field(q.manifest)
	})
}
func decodeRetainedPinQuestion(wire []byte) (retainedPinQuestion, error) {
	body, err := graphBody(wire, "GPQ\x01", graphPinManifestBytes+128)
	if err != nil {
		return retainedPinQuestion{}, err
	}
	c := graphCursor{b: body}
	q := retainedPinQuestion{rows: c.count(1<<20, 0), bytes: c.count(64<<20, 0), output: c.count(64<<20, 0), manifest: c.field(graphPinManifestBytes)}
	canonical, err := encodeRetainedPinQuestion(q)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedPinQuestion{}, errors.Join(errInvalid, c.err, err)
	}
	return q, nil
}
func encodeRetainedPinObservation(o retainedPinObservation) ([]byte, error) {
	if !o.scope.valid() || !retainedGraphAgreement(o.scope.semantic) || o.read.session == ([16]byte{}) || o.read.sequence == 0 || o.nonce == ([16]byte{}) || o.index == 0 || o.configuration == ([32]byte{}) || o.status < 1 || o.status > 5 || o.rows < 0 || o.bytes < 0 || o.output < 0 || len(o.manifest) > graphPinManifestBytes {
		return nil, errInvalid
	}
	var result, owner []byte
	var err error
	if len(o.manifest) > 0 {
		m, e := decodeGraphPinManifest(o.manifest)
		if e != nil {
			return nil, e
		}
		if o.result.index == 0 {
			if (o.status != pinPending && !(durableColumnGraphAgreement(o.scope.semantic) && (o.cancelled && o.status == pinReleased || o.issued.index != 0 && o.status == pinExpired))) || o.result != (graphPinResult{}) || o.owner != (graphPinOwner{}) {
				return nil, errInvalid
			}
		} else {
			result, err = encodeGraphPinResult(o.result)
			if err != nil {
				return nil, err
			}
			if o.result.id != m.id() || o.result.digest != sha256.Sum256(o.manifest) || o.result.action != m.request || o.result.phase != pinPublish || o.result.index > o.index {
				return nil, errInvalid
			}
		}
		if o.owner.publication != 0 {
			owner, err = encodePinOwner(o.owner)
			if err != nil {
				return nil, err
			}
			if o.owner.digest != o.result.digest || o.owner.publication != o.result.publication || o.owner.terminalIndex > o.index {
				return nil, errInvalid
			}
		}
	} else if o.result != (graphPinResult{}) || o.owner != (graphPinOwner{}) || o.status != pinApplied {
		return nil, errInvalid
	}
	version := byte(1)
	var issued []byte
	if durableColumnGraphAgreement(o.scope.semantic) {
		version = 2
		if o.issued.index != 0 {
			manifest, e := decodeGraphPinManifest(o.manifest)
			if e != nil || o.scope != manifest.publisher || o.issued.phase != pinIssue || o.issued.status != pinApplied || o.issued.id != manifest.id() || o.issued.digest != sha256.Sum256(o.manifest) || o.issued.action != pinTerminalAction(manifest, o.manifest, pinIssue) || o.issued.index > o.index {
				return nil, errors.Join(errInvalid, e)
			}
			issued, err = encodeGraphPinResult(o.issued)
			if err != nil {
				return nil, err
			}
		}
		if o.cancelled && o.status != pinReleased && o.status != pinExpired {
			return nil, errInvalid
		}
	} else if o.issued != (graphPinResult{}) || o.cancelled {
		return nil, errInvalid
	}
	return boundedEncoding(graphPinManifestBytes+2048, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'I', version})
		w.add(appendAllocationScope(nil, o.scope))
		w.add(o.read.session[:])
		w.u64(o.read.sequence)
		w.add(o.nonce[:])
		w.u64(o.index)
		w.u64(o.epoch)
		w.add(o.configuration[:])
		w.tag(o.status)
		w.u32(o.rows)
		w.u32(o.bytes)
		w.u32(o.output)
		w.field(result)
		w.field(owner)
		w.field(o.manifest)
		if version == 2 {
			w.field(issued)
			if o.cancelled {
				w.tag(1)
			} else {
				w.tag(0)
			}
		}
	})
}
func decodeRetainedPinObservation(wire []byte) (retainedPinObservation, error) {
	version, magic := byte(1), "GPI\x01"
	if bytes.HasPrefix(wire, []byte("GPI\x02")) {
		version, magic = 2, "GPI\x02"
	}
	body, err := graphBody(wire, magic, graphPinManifestBytes+2048)
	if err != nil {
		return retainedPinObservation{}, err
	}
	c := graphCursor{b: body}
	scope := c.take(allocationScopeBytes - 36)
	o := retainedPinObservation{read: allocationReadID{session: c.array(), sequence: c.u64()}, nonce: c.array(), index: c.u64(), epoch: c.u64()}
	copy(o.configuration[:], c.take(32))
	o.status = c.tag()
	o.rows = c.count(1<<20, 0)
	o.bytes = c.count(64<<20, 0)
	o.output = c.count(64<<20, 0)
	result, owner := c.field(256), c.field(128)
	o.manifest = c.field(graphPinManifestBytes)
	var issuance []byte
	if version == 2 {
		issuance = c.field(256)
		flag := c.tag()
		if flag > 1 {
			return retainedPinObservation{}, errCorrupt
		}
		o.cancelled = flag == 1
	}
	if c.err != nil || len(c.b) != 0 {
		return retainedPinObservation{}, errors.Join(errCorrupt, c.err)
	}
	o.scope = readAllocationScope(&decoder{b: scope})
	if version == 2 && !durableColumnGraphAgreement(o.scope.semantic) || version == 1 && durableColumnGraphAgreement(o.scope.semantic) {
		return retainedPinObservation{}, errInvalid
	}
	if len(issuance) > 0 {
		o.issued, err = decodeGraphPinResult(issuance)
		if err != nil {
			return retainedPinObservation{}, err
		}
	}
	if len(result) > 0 {
		o.result, err = decodeGraphPinResult(result)
		if err != nil {
			return retainedPinObservation{}, err
		}
	}
	if len(owner) > 0 {
		b, e := graphBody(owner, "GPA\x01", 128)
		if e != nil {
			return retainedPinObservation{}, e
		}
		d := graphCursor{b: b}
		copy(o.owner.digest[:], d.take(32))
		o.owner.publication = d.u64()
		o.owner.feedStart = d.u64()
		o.owner.terminal = d.tag()
		o.owner.terminalIndex = d.u64()
		if d.err != nil || len(d.b) != 0 {
			return retainedPinObservation{}, errors.Join(errCorrupt, d.err)
		}
	}
	canonical, err := encodeRetainedPinObservation(o)
	if err != nil || !bytes.Equal(canonical, wire) {
		return retainedPinObservation{}, errors.Join(errCorrupt, err)
	}
	o.manifest = owned(o.manifest)
	return o, nil
}

// The actual completed budgeted cut barrier supplies the index/configuration.
// The remaining native inspection uses the SAME grant; a new arena only
// represents its residual allowance, with the barrier's owned bytes deducted.
func (s *graphPeerService) readRetainedPin(ctx context.Context, r graphSourceRequest) ([]byte, error) {
	if s == nil || !s.host.machine.retainedPins() {
		return nil, errInvalid
	}
	question, err := decodeRetainedPinQuestion(r.command)
	if err != nil {
		return nil, err
	}
	question.nonce = r.nonce
	q := graphCutQuery{kind: 1, nonce: r.nonce, sourceRows: question.rows, sourceBytes: question.bytes, outputBytes: question.output}
	reply, err := s.pump.Cut(ctx, q)
	if err != nil || reply.err != nil {
		return nil, errors.Join(err, reply.err)
	}
	if reply.question != q || reply.id != reply.proof.id || reply.proof.owner != s.host {
		return nil, errInvalid
	}
	if err := checkGraphCutProof(reply.proof, q); err != nil {
		return nil, err
	}
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if s.host.unavailableLocked() {
		return nil, replica.ErrStopped
	}
	observation, err := s.host.machine.observeRetainedPin(ctx, question, reply.proof)
	if err != nil {
		return nil, err
	}
	return encodeRetainedPinObservation(observation)
}
func (m *declaredMaterializer) observeRetainedPin(ctx context.Context, question retainedPinQuestion, proof graphCutProof) (out retainedPinObservation, err error) {
	if ctx == nil || !m.retainedPins() || proof.observation.scope != m.scope() || question.nonce != proof.question.nonce {
		return out, errInvalid
	}
	captured := proof.observation
	if captured.sourceRows < 0 || captured.sourceRows >= question.rows || captured.sourceBytes < 0 || captured.sourceBytes >= question.bytes || captured.outputBytes < 0 || captured.outputBytes >= question.output {
		return out, errLimit
	}
	view, err := m.store.ApplicationView(captured.index)
	if err != nil {
		return out, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			out = retainedPinObservation{}
		}
	}()
	base, err := view.RootBounded(ctx, 172)
	if err != nil {
		return out, err
	}
	l := m.limits
	l.outputBytes = min(l.outputBytes, question.output-captured.outputBytes)
	limits := l.allocation
	limits.readRows = min(limits.readRows, question.rows)
	limits.readBytes = min(limits.readBytes, question.bytes)
	q := reader{ctx: ctx, view: view, ns: m.ns, base: base, limits: limits, rows: captured.sourceRows, bytes: captured.sourceBytes + cap(base.Image)}
	if err := joinGraphOutputBudget(&q, graphRequest{}, 4096+8*len(question.manifest), l); err != nil {
		return out, err
	}
	defer func() {
		if err == nil {
			out.rows, out.bytes = q.rows, q.bytes
			out.output = captured.outputBytes + l.outputBytes - q.arena.Remaining()
		}
	}()
	if err := m.checkRetainedPinRoster(&q, false); err != nil {
		return out, err
	}
	head, err := readPinHead(&q)
	if err != nil {
		return out, err
	}
	out = retainedPinObservation{scope: m.scope(), read: proof.id, nonce: question.nonce, index: base.Index, epoch: head.epoch, configuration: captured.configuration, status: pinApplied}
	if len(question.manifest) == 0 {
		return out, nil
	}
	manifest, err := decodeGraphPinManifest(question.manifest)
	if err != nil {
		return out, err
	}
	if err := m.bindPinManifest(manifest); err != nil {
		return out, err
	}
	if manifest.parts[0].certificate.configuration != out.configuration {
		return out, replica.ErrUnavailable
	}
	command := graphPinCommand{scope: m.scope(), issuer: manifest.issuer, action: manifest.request, id: manifest.id(), digest: sha256.Sum256(question.manifest), phase: pinPublish, epoch: manifest.epoch, manifest: question.manifest}
	wire, err := encodeGraphPinCommand(command)
	if err != nil {
		return out, err
	}
	if durableColumnGraphAgreement(m.SemanticContractID()) {
		out.issued, err = readPinIssuance(&q, manifest, question.manifest)
		if err != nil {
			return out, err
		}
		out.cancelled, err = pinIssuedCancellation(&q, command.id, command.digest, command.issuer)
		if err != nil {
			return out, err
		}
	}
	result, found, err := pinActionOriginal(&q, command, wire)
	if err != nil {
		return out, err
	}
	if !found {
		// Genuine completed absence is explicit pending, never publication or
		// permission to resurrect an unknown original from caller-supplied bytes.
		out.status, out.manifest = pinPending, owned(question.manifest)
		if out.cancelled {
			out.status = pinReleased
		} else if out.issued.index != 0 && head.epoch >= manifest.expires {
			out.status = pinExpired
		}
		return out, nil
	}
	out.result, out.manifest = result, owned(question.manifest)
	owner, found, err := readPinOwner(&q, command.id)
	if err != nil {
		return out, err
	}
	if !found {
		out.status = result.status
		if result.status == pinApplied {
			return out, errCorrupt
		}
		return out, nil
	}
	out.owner = owner
	actual, original, found, err := readPinOriginal(&q, command.id)
	if err != nil || !found || !bytes.Equal(original, question.manifest) || actual.id() != command.id || owner.digest != command.digest || owner.publication != result.publication {
		return out, errors.Join(errCorrupt, err)
	}
	if owner.terminal != 0 {
		out.status = pinReleased
		if owner.terminal == pinExpire {
			out.status = pinExpired
		}
		return out, nil
	}
	if head.epoch >= actual.expires {
		out.status = pinExpired
		return out, nil
	}
	if err := m.checkPinRetainedSource(&q, actual, owner, head); err != nil {
		return out, err
	}
	return out, nil
}

func (e *graphAuthenticatedEndpoint) retainedPinRead(ctx context.Context, manifest []byte) (out retainedPinObservation, err error) {
	if e == nil || e.budget == nil || !retainedGraphAgreement(e.peer.scope.semantic) {
		return out, errInvalid
	}
	b := e.budget
	const replyMax = graphPinManifestBytes + 4096
	b.mu.Lock()
	q := retainedPinQuestion{manifest: manifest, rows: min(1<<20, b.maxRows-b.rows), bytes: min(64<<20, b.maxBytes-b.bytes-(replyMax+len(manifest)+2048)), output: min(64<<20, b.output.Remaining()-(4*(replyMax+len(manifest)+2048)+2048))}
	b.mu.Unlock()
	if q.rows < 16 || q.bytes < 32768 || q.output < 32768 {
		return out, errLimit
	}
	wire, err := encodeRetainedPinQuestion(q)
	if err != nil {
		return out, err
	}
	// Failure without authenticated actual work retires the offered residual
	// grant; retry never makes uncertain native work free on this invocation.
	defer func() {
		if err != nil {
			b.mu.Lock()
			b.rows += q.rows
			b.bytes += q.bytes
			charge := b.output.Reserve(q.output)
			b.mu.Unlock()
			err = errors.Join(err, outputBudgetFailure(charge))
			out = retainedPinObservation{}
		}
	}()
	response, err := e.call(ctx, graphSourceRequest{kind: sourceCrossRead, command: wire}, replyMax)
	if err != nil {
		return out, err
	}
	out, err = decodeRetainedPinObservation(response.body)
	if err != nil {
		return out, err
	}
	if out.scope != e.peer.scope || out.read.session != response.issuer || out.nonce != response.nonce || out.rows > q.rows || out.bytes > q.bytes || out.output > q.output || !bytes.Equal(out.manifest, manifest) {
		return out, errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if out.rows > b.maxRows-b.rows || out.bytes > b.maxBytes-b.bytes {
		return out, errLimit
	}
	if err := b.output.Reserve(out.output); err != nil {
		return out, outputBudgetFailure(err)
	}
	b.rows += out.rows
	b.bytes += out.bytes
	return out, nil
}

// Pinned opens first re-observe the original/live lease through a completed
// native barrier. A caller token/hash cannot bless an already released source.
func (s *graphPeerService) openRetainedPin(ctx context.Context, r graphSourceRequest) ([]byte, error) {
	body, err := graphBody(r.command, "GPA\x02", graphPinManifestBytes+128)
	if err != nil {
		return nil, err
	}
	question, err := decodeRetainedPinQuestion(body)
	if err != nil {
		return nil, err
	}
	question.nonce = r.nonce
	authority, err := s.readRetainedPin(ctx, graphSourceRequest{kind: sourceCrossRead, nonce: r.nonce, command: body})
	if err != nil {
		return nil, err
	}
	observed, err := decodeRetainedPinObservation(authority)
	if err != nil {
		return nil, err
	}
	if err := retainedPinLive(observed); err != nil {
		return nil, err
	}
	manifest, err := decodeGraphPinManifest(observed.manifest)
	if err != nil {
		return nil, err
	}
	part, found := pinPart(manifest, s.peer.scope.partition)
	if !found {
		return nil, errInvalid
	}
	if question.output-observed.output < 8192 {
		return nil, errLimit
	}
	arena, err := graphstate.NewOutputBudget(question.output - observed.output)
	if err != nil {
		return nil, outputBudgetFailure(err)
	}
	// This is the residual of the SAME offered grant, not a new default arena.
	// Fixed ordinary lease/codec ownership is funded before native acquisition.
	if err := arena.Reserve(8192); err != nil {
		return nil, outputBudgetFailure(err)
	}
	rows, byteWork := observed.rows, observed.bytes
	inspect := func(capture context.Context, acquire bool) (source *raftlog.ApplicationView, cfg genesisAllocationConfig, opErr error) {
		s.host.mu.Lock()
		defer s.host.mu.Unlock()
		if s.host.unavailableLocked() {
			return nil, cfg, replica.ErrStopped
		}
		m := s.host.machine
		current, err := m.store.ApplicationView(s.host.driver.Applied())
		if err != nil {
			return nil, cfg, err
		}
		defer func() { opErr = errors.Join(opErr, current.Close()) }()
		base, err := current.RootBounded(capture, 172)
		if err != nil {
			return nil, cfg, err
		}
		l := m.limits.allocation
		l.readRows, l.readBytes = min(l.readRows, question.rows), min(l.readBytes, question.bytes)
		q := reader{arena: arena, ctx: capture, view: current, ns: m.ns, base: base, limits: l, rows: rows, bytes: byteWork + cap(base.Image)}
		defer func() { rows, byteWork = q.rows, q.bytes }()
		if err := reserveComposer(&q, 1024+2*cap(base.Image)); err != nil {
			return nil, cfg, err
		}
		c := graphPinCommand{scope: m.scope(), issuer: manifest.issuer, id: manifest.id(), digest: sha256.Sum256(observed.manifest)}
		owner, actual, found, err := m.pinKnownOwner(&q, c)
		if err != nil {
			return nil, cfg, err
		}
		if !found {
			return nil, cfg, replica.ErrUnavailable
		}
		if owner.terminal == pinExpire {
			return nil, cfg, errGraphPinExpired
		}
		if owner.terminal != 0 {
			return nil, cfg, errGraphPinReleased
		}
		head, err := readPinHead(&q)
		if err != nil {
			return nil, cfg, err
		}
		if head.epoch >= actual.expires {
			return nil, cfg, errGraphPinExpired
		}
		if err := m.checkRetainedPinRoster(&q, false); err != nil {
			return nil, cfg, err
		}
		// A live feed keeper no longer promises a new physical snapshot source
		// after this authenticated irreversible transition. Check again during
		// final lease admission, so a racing capture cannot publish a new lease.
		if err := m.feedSnapshotAvailable(&q, actual); err != nil {
			return nil, cfg, err
		}
		if !acquire {
			return nil, cfg, nil
		}
		part.feedStart = owner.feedStart
		if err := m.checkPinSource(&q, part, head); err != nil {
			return nil, cfg, err
		}
		cfg, err = m.readGenesisConfig(&q)
		if err != nil {
			return nil, cfg, err
		}
		digest, err := cfg.digest()
		if err != nil || digest != part.certificate.configuration {
			return nil, cfg, errors.Join(errInvalid, err)
		}
		// Acquisition is serialized with the actual current owner/epoch check.
		// The returned owner remains registered through the final admission callback.
		if q.rows >= q.limits.readRows || 2048 > q.limits.readBytes-q.bytes {
			return nil, cfg, errLimit
		}
		if err := reserveComposer(&q, 2048); err != nil {
			return nil, cfg, err
		}
		q.rows++
		q.bytes += 2048 // fixed172-byte root handle/frame/hash/copy envelope
		source, err = m.store.ApplicationView(part.certificate.sourceIndex)
		if err != nil {
			return nil, cfg, err
		}
		root, err := source.RootBounded(capture, 172)
		if err != nil || !bytes.Equal(root.Image, part.certificate.image[:]) {
			return source, cfg, errors.Join(errCorrupt, err)
		}
		return source, cfg, nil
	}
	info, err := s.sources.openCertifiedCapture(ctx, part.certificate, r.nonce, func(capture context.Context) (*raftlog.ApplicationView, genesisAllocationConfig, error) {
		return inspect(capture, true)
	}, func(capture context.Context) error { _, _, err := inspect(capture, false); return err })
	if err != nil {
		return nil, err
	}
	encoded, err := encodeGraphCertifiedSourceInfo(info)
	if err != nil {
		return nil, errors.Join(err, s.sources.closeSource(info.cookie))
	}
	workOutput := question.output - arena.Remaining()
	reply, err := boundedEncoding(8192, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'S', 2})
		w.u32(rows)
		w.u32(byteWork)
		w.u32(workOutput)
		w.field(encoded)
	})
	if err != nil {
		return nil, errors.Join(err, s.sources.closeSource(info.cookie))
	}
	return reply, nil
}
func retainedPinLive(o retainedPinObservation) error {
	if o.status == pinExpired {
		return errGraphPinExpired
	}
	if o.status == pinReleased {
		return errGraphPinReleased
	}
	if o.status != pinApplied || o.owner.publication == 0 || o.owner.terminal != 0 {
		return replica.ErrUnavailable
	}
	return nil
}

func (e *graphAuthenticatedEndpoint) openRetainedPinSource(ctx context.Context, manifest []byte) (source *remoteGraphSource, cfg genesisAllocationConfig, err error) {
	if e == nil || ctx == nil {
		return nil, cfg, errInvalid
	}
	cleanup, err := e.budget.cleanup()
	if err != nil {
		return nil, cfg, err
	}
	b := e.budget
	b.mu.Lock()
	q := retainedPinQuestion{manifest: manifest, rows: min(1<<20, b.maxRows-b.rows), bytes: min(64<<20, b.maxBytes-b.bytes-(8192+len(manifest)+2048)), output: min(64<<20, b.output.Remaining()-(4*(8192+len(manifest)+2048)+2048))}
	b.mu.Unlock()
	if q.rows < 16 || q.bytes < 32768 || q.output < 32768 {
		return nil, cfg, errLimit
	}
	question, err := encodeRetainedPinQuestion(q)
	if err != nil {
		return nil, cfg, err
	}
	command := seal(append([]byte{'G', 'P', 'A', 2}, question...))
	defer func() {
		if err != nil {
			b.mu.Lock()
			b.rows += q.rows
			b.bytes += q.bytes
			charge := b.output.Reserve(q.output)
			b.mu.Unlock()
			err = errors.Join(err, outputBudgetFailure(charge))
		}
	}()
	started := time.Now()
	response, err := e.call(ctx, graphSourceRequest{kind: sourceCertifiedOpen, command: command}, 8192)
	if err != nil {
		return nil, cfg, err
	}
	body, err := graphBody(response.body, "GPS\x02", 8192)
	if err != nil {
		return nil, cfg, err
	}
	c := graphCursor{b: body}
	rows, byteWork, output := c.count(q.rows, 0), c.count(q.bytes, 0), c.count(q.output, 0)
	encoded := c.field(4096)
	if c.err != nil || len(c.b) != 0 {
		return nil, cfg, errors.Join(errCorrupt, c.err)
	}
	info, err := decodeGraphCertifiedSourceInfo(encoded)
	if err != nil {
		return nil, cfg, err
	}
	if info.cookie.issuer != response.issuer || info.nonce != response.nonce {
		return nil, cfg, errInvalid
	}
	// A valid correlated reply supplies a known opaque handle. If any later
	// admission/budget check fails, spend the already prepaid Close exchange;
	// cancellation of the caller does not cancel cleanup ownership.
	defer func() {
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, closeErr := e.callUsing(closeCtx, graphSourceRequest{kind: sourceClose, cookie: info.cookie}, 1024, cleanup)
			err = errors.Join(err, closeErr)
			source, cfg = nil, genesisAllocationConfig{}
		}
	}()
	decoded, err := decodeGraphPinManifest(manifest)
	if err != nil {
		return nil, cfg, err
	}
	part, found := pinPart(decoded, e.peer.scope.partition)
	if !found || info.certificate != part.certificate {
		return nil, cfg, errInvalid
	}
	b.mu.Lock()
	if rows > b.maxRows-b.rows || byteWork > b.maxBytes-b.bytes {
		b.mu.Unlock()
		return nil, cfg, errLimit
	}
	charge := b.output.Reserve(output)
	if charge == nil {
		b.rows += rows
		b.bytes += byteWork
	}
	b.mu.Unlock()
	if charge != nil {
		return nil, cfg, outputBudgetFailure(charge)
	}
	owner, cancel := context.WithDeadline(ctx, started.Add(info.lifetime))
	source = &remoteGraphSource{mu: newGraphRequestGate(), endpoint: e, certified: &info, info: graphSourceInfo{cookie: info.cookie, root: info.root, limits: info.limits, lifetime: info.lifetime}, ctx: owner, cancel: cancel, deadline: started.Add(info.lifetime), cleanup: cleanup}
	return source, info.configuration, nil
}

// These fixed finite credits cover a <=172-byte root handle and one <=512-byte
// immutable GPX lookup, including bounded decoder/output ownership. They are
// not a configured-max page charge or an independent/default operation arena.
const (
	pinLookupRows   = 3
	pinLookupBytes  = 4096
	pinLookupOutput = 4096
)

func reserveRetainedPinLookup(b *graphRemoteBudget, commandBytes int) error {
	if b == nil || b.output == nil || commandBytes < 1 || commandBytes > graphPinCommandBytes {
		return errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if pinLookupRows > b.maxRows-b.rows || pinLookupBytes > b.maxBytes-b.bytes {
		return errLimit
	}
	if err := b.output.Reserve(pinLookupOutput + 8*commandBytes); err != nil {
		return outputBudgetFailure(err)
	}
	b.rows += pinLookupRows
	b.bytes += pinLookupBytes
	return nil
}
func retainedPinTerminalBudget(b *graphRemoteBudget) (*graphRemoteBudget, error) {
	if b == nil || b.output == nil {
		return nil, errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	const bytes = 2048 + pinLookupBytes
	const output = 8192 + pinLookupOutput + 8*512
	if b.calls >= b.maxCalls || bytes > b.maxBytes-b.bytes || pinLookupRows > b.maxRows-b.rows || output > b.output.Remaining() {
		return nil, errLimit
	}
	arena, err := graphstate.NewOutputBudget(output)
	if err != nil {
		return nil, outputBudgetFailure(err)
	}
	if err := b.output.Reserve(output); err != nil {
		return nil, outputBudgetFailure(err)
	}
	b.calls++
	b.bytes += bytes
	b.rows += pinLookupRows
	return &graphRemoteBudget{maxCalls: 1, maxBytes: bytes, maxRows: pinLookupRows, output: arena}, nil
}

// Local retained originals are immutable installed outcomes, not live-owner or
// global-absence authority. An exact replay needs no new quorum/log capacity;
// completed local absence delegates to the ordinary authenticated Submit path.
func (s *graphPeerService) retainedPinOriginal(ctx context.Context, proposal allocationProposal, command graphPinCommand) (result graphPinResult, found bool, rows, byteWork, output int, err error) {
	if s == nil || ctx == nil || !s.host.machine.retainedPins() || proposal.target != s.peer.scope {
		return result, false, 0, 0, 0, errInvalid
	}
	if command.scope != s.peer.scope {
		return result, false, 0, 0, 0, errInvalid
	}
	// The caller has already prepaid this exact fixed grant, including the wire
	// codec ownership on its RPC ledger. No default allowance is created here.
	arena, err := graphstate.NewOutputBudget(pinLookupOutput)
	if err != nil {
		return result, false, 0, 0, 0, err
	}
	if err := arena.Reserve(2048); err != nil {
		return result, false, 0, 0, 0, outputBudgetFailure(err)
	}
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if s.host.unavailableLocked() {
		return result, false, 0, 0, 0, replica.ErrStopped
	}
	m := s.host.machine
	index := s.host.driver.Applied()
	view, err := m.store.ApplicationView(index)
	if err != nil {
		return result, false, 0, 0, 0, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			result, found = graphPinResult{}, false
		}
	}()
	base, err := view.RootBounded(ctx, 172)
	if err != nil || base.Index != index {
		return result, false, 0, 0, 0, errors.Join(errInvalid, err)
	}
	limits := m.limits.allocation
	limits.readRows, limits.readBytes = min(limits.readRows, pinLookupRows), min(limits.readBytes, pinLookupBytes)
	// Root acquisition/frame/hash/copies have a finite conservative debit. The
	// immutable control API reports its actual examined rows and byte work.
	q := reader{arena: arena, ctx: ctx, view: view, ns: m.ns, base: base, limits: limits, rows: 1, bytes: 512 + 4*cap(base.Image)}
	result, found, err = pinActionOriginal(&q, command, proposal.wire)
	if err == nil && found && result.index > base.Index {
		err = errCorrupt
	}
	return result, found, q.rows, q.bytes, pinLookupOutput - arena.Remaining(), err
}
func (s *graphPeerService) submitRetainedPin(ctx context.Context, proposal allocationProposal) ([]byte, error) {
	// Full GPL1/GPM canonicalization is covered by the separately prepaid
	// variable 8*commandBytes debit on the original RPC ledger, NOT the 4KiB
	// native lookup arena. Decode once and pass the same canonical value onward.
	command, err := decodeGraphPinCommand(proposal.wire)
	if err != nil {
		return nil, err
	}
	original, found, rows, byteWork, output, err := s.retainedPinOriginal(ctx, proposal, command)
	if err != nil {
		return nil, err
	}
	var outcome []byte
	if found {
		outcome, err = encodeGraphPinResult(original)
	} else {
		outcome, err = s.pump.Submit(ctx, proposal)
	}
	if err != nil {
		return nil, err
	}
	if _, err := decodeGraphPinResult(outcome); err != nil {
		return nil, err
	}
	return boundedEncoding(1024, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'R', 1})
		w.u32(rows)
		w.u32(byteWork)
		w.u32(output)
		w.field(outcome)
	})
}
