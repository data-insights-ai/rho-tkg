package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

var errGraphReadConflict = errors.New("graphapply: conditional read changed; read and evaluate again with a new request")

// A fresh-store agreement adds conditional admission without changing any older
// agreement's descriptor, unconditional commands or retained original bytes.
func conditionalGraphSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := rangeMoveSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("conditional-graph=complete-native-prefix-v1\x00original-register-exception=exact-v1\x00")...)))
}

// These values are inert wire metadata. Neither a hash nor decoding produces
// evidence that a caller read a graph, authenticated an issuer or covered scope.
// Prefix is a local applied coordinate, never a graph cut or allocation floor.
type graphReadConditionPart struct {
	certificate graphCertificate
	prefix      uint64
	authority   [32]byte
}
type graphReadConditionValue struct{ parts []graphReadConditionPart }
type graphReadCondition struct {
	value graphReadConditionValue
	stamp *graphReadConditionStamp
}
type graphReadConditionStamp struct {
	digest [32]byte
	owner  *conditionalGraphRead
}

const graphConditionPartBytes = 1024

func (v graphReadConditionValue) valid() bool {
	if len(v.parts) == 0 || len(v.parts) > 256 {
		return false
	}
	for i, p := range v.parts {
		c := p.certificate
		if !conditionalGraphAgreement(c.scope.semantic) || p.prefix < c.index || p.prefix == math.MaxUint64 || p.authority == ([32]byte{}) {
			return false
		}
		if _, err := encodeGraphCertificate(c); err != nil {
			return false
		}
		if i > 0 {
			first, previous := v.parts[0].certificate, v.parts[i-1].certificate
			if c.scope.partition <= previous.scope.partition || c.scope.graph != first.scope.graph || c.scope.declaration != first.scope.declaration || c.scope.topology != first.scope.topology || c.configuration != first.configuration || c.round != first.round {
				return false
			}
		}
	}
	return true
}
func (v graphReadConditionValue) part(partition uint64) (graphReadConditionPart, bool) {
	for _, p := range v.parts {
		if p.certificate.scope.partition == partition {
			return p, true
		}
	}
	return graphReadConditionPart{}, false
}
func encodeGraphReadConditionValue(v graphReadConditionValue, l materializerLimits) ([]byte, error) {
	if !v.valid() {
		return nil, errInvalid
	}
	return boundedEncoding(l.commandBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'U', 'C', 1})
		w.u32(len(v.parts))
		for _, p := range v.parts {
			wire, err := encodeGraphCertificate(p.certificate)
			if err != nil {
				w.err = err
				return
			}
			w.field(wire)
			w.u64(p.prefix)
			w.add(p.authority[:])
		}
	})
}
func decodeGraphReadConditionValue(wire []byte, l materializerLimits, arena *graphstate.OutputBudget) (graphReadConditionValue, error) {
	if arena == nil {
		return graphReadConditionValue{}, errInvalid
	}
	body, err := graphBody(wire, "GUC\x01", l.commandBytes)
	if err != nil {
		return graphReadConditionValue{}, err
	}
	c := graphCursor{b: body}
	n := c.count(256, 44)
	if c.err != nil || n == 0 {
		return graphReadConditionValue{}, errors.Join(errCorrupt, c.err)
	}
	if err := arena.Reserve(1024 + 4*len(wire) + graphConditionPartBytes*n); err != nil {
		return graphReadConditionValue{}, outputBudgetFailure(err)
	}
	v := graphReadConditionValue{parts: make([]graphReadConditionPart, n)}
	for i := range v.parts {
		cert := c.field(512)
		v.parts[i].prefix = c.u64()
		copy(v.parts[i].authority[:], c.take(32))
		if c.err != nil {
			return graphReadConditionValue{}, c.err
		}
		v.parts[i].certificate, err = decodeGraphCertificate(cert)
		if err != nil {
			return graphReadConditionValue{}, err
		}
	}
	if c.err != nil || len(c.b) != 0 || !v.valid() {
		return graphReadConditionValue{}, errors.Join(errCorrupt, c.err)
	}
	return v, nil
}
func (c graphReadCondition) validSeal() bool {
	if c.stamp == nil || c.stamp.owner == nil || c.stamp != c.stamp.owner.condition.stamp {
		return false
	}
	owner := c.stamp.owner
	return sha256.Sum256(owner.conditionWire) == c.stamp.digest && slices.Equal(c.value.parts, owner.condition.value.parts)
}

func reserveConditionBudget(b *graphRemoteBudget, bytes int) error {
	if b == nil || b.output == nil {
		return errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return outputBudgetFailure(b.output.Reserve(bytes))
}

// conditionalGraphRead is opt-in ownership. Ordinary certified sessions incur
// neither complete-scope capture nor these native authority scans.
type conditionalGraphRead struct {
	mu            sync.Mutex
	read          *certifiedGraphRead
	ctx           context.Context
	condition     graphReadCondition
	conditionWire []byte
	budget        *graphRemoteBudget
	closed        bool
	closeErr      error
}

func newConditionalGraphMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationControls().Version != 1 || s.ApplicationRecovery().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, conditionalGraphSemanticContractID())
}

// OpenForUpdate captures a complete declared native footprint before the client
// reasons about this read. It is explicit and requires the fresh conditional
// agreement; ordinary OpenCertified keeps its existing scope and wire cost.
func (c *crossProcessCoordinator) OpenForUpdate(ctx context.Context, request graphReadRequest) (result *conditionalGraphRead, err error) {
	if c == nil || c.host == nil || c.host.machine == nil || ctx == nil {
		return nil, errInvalid
	}
	if !conditionalGraphAgreement(c.host.machine.SemanticContractID()) {
		return nil, errGraphCutScope
	}
	read, err := c.OpenCertified(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, read.Close())
			result = nil
		}
	}()
	read.mu.Lock()
	var budget *graphRemoteBudget
	for _, source := range read.sources {
		remote, ok := source.(*remoteGraphSource)
		if !ok || remote.endpoint == nil || remote.endpoint.budget == nil || budget != nil && remote.endpoint.budget != budget {
			read.mu.Unlock()
			return nil, errGraphCutScope
		}
		budget = remote.endpoint.budget
	}
	parts := len(read.sources)
	read.mu.Unlock()
	// Own the condition/certificate arrays, bounded codec scratch and retained
	// sealed wire on the same complete read allowance before allocating them.
	if err = reserveConditionBudget(budget, 1024+8192*parts); err != nil {
		return nil, err
	}
	cut, err := read.Cut()
	if err != nil {
		return nil, err
	}
	if len(read.sources) != c.host.machine.declaration.Len() || len(cut.certificates) != len(read.sources) {
		return nil, errGraphCutScope
	}
	v := graphReadConditionValue{parts: make([]graphReadConditionPart, len(read.sources))}
	for i, source := range read.sources {
		remote, ok := source.(*remoteGraphSource)
		if !ok || remote.certified == nil || remote.certified.certificate != cut.certificates[i] {
			return nil, errGraphCutScope
		}
		capture, captureErr := remote.captureCondition(ctx, c.host.machine.limits)
		err = captureErr
		v.parts[i] = capture.part
		if err != nil {
			return nil, err
		}
	}
	wire, err := encodeGraphReadConditionValue(v, c.host.machine.limits)
	if err != nil {
		return nil, err
	}
	result = &conditionalGraphRead{read: read, ctx: ctx, budget: budget, conditionWire: wire}
	result.condition = graphReadCondition{value: v, stamp: &graphReadConditionStamp{digest: sha256.Sum256(wire), owner: result}}
	return result, nil
}
func (r *conditionalGraphRead) active() error {
	if r == nil {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.read == nil {
		return graphstore.ErrClosed
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	r.read.mu.Lock()
	defer r.read.mu.Unlock()
	if r.read.closed {
		return graphstore.ErrClosed
	}
	for _, source := range r.read.sources {
		remote, ok := source.(*remoteGraphSource)
		if !ok {
			return errGraphCutScope
		}
		if err := remote.ctx.Err(); err != nil {
			return errors.Join(replica.ErrUnavailable, err)
		}
	}
	return nil
}

// Condition returns the already authenticated immutable capture. It does not
// acquire a newer condition or certify reasoning performed on another read.
func (r *conditionalGraphRead) Condition() (graphReadCondition, error) {
	if err := r.active(); err != nil {
		return graphReadCondition{}, err
	}
	if err := reserveConditionBudget(r.budget, 128+graphConditionPartBytes*len(r.condition.value.parts)); err != nil {
		return graphReadCondition{}, err
	}
	return graphReadCondition{value: graphReadConditionValue{parts: slices.Clone(r.condition.value.parts)}, stamp: r.condition.stamp}, nil
}
func (r *conditionalGraphRead) ReadView() (graphstate.ReadView, error) {
	if err := r.active(); err != nil {
		return nil, err
	}
	return r.read.ReadView()
}
func (r *conditionalGraphRead) Cut() (graphCut, error) {
	if err := r.active(); err != nil {
		return graphCut{}, err
	}
	if err := reserveConditionBudget(r.budget, 128+512*len(r.condition.value.parts)); err != nil {
		return graphCut{}, err
	}
	return r.read.Cut()
}
func (r *conditionalGraphRead) Close() error {
	if r == nil {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.closeErr = r.read.Close()
	}
	return r.closeErr
}

// Only explicit conditional-open bodies add this bounded work grant. A decoded
// body is inert until its endpoint/nonce/issuer and retained source are matched.
func encodeConditionCapture(cert graphCertificate, rows, bytes, output int) ([]byte, error) {
	wire, err := encodeGraphCertificate(cert)
	if err != nil || rows < 1 || bytes < 1 || output < 1 {
		return nil, errors.Join(errInvalid, err)
	}
	return boundedEncoding(1024, func(w *boundedWriter) {
		w.add([]byte{'G', 'U', 'Q', 1})
		w.field(wire)
		w.u32(rows)
		w.u32(bytes)
		w.u32(output)
	})
}
func decodeConditionCapture(wire []byte) (graphCertificate, int, int, int, error) {
	body, err := graphBody(wire, "GUQ\x01", 1024)
	if err != nil {
		return graphCertificate{}, 0, 0, 0, err
	}
	c := graphCursor{b: body}
	certWire := c.field(512)
	rows, bytes, output := c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	if c.err != nil || len(c.b) != 0 || rows < 1 || bytes < 1 || output < 1 {
		return graphCertificate{}, 0, 0, 0, errors.Join(errInvalid, c.err)
	}
	cert, err := decodeGraphCertificate(certWire)
	return cert, rows, bytes, output, err
}
func conditionPrefixUpper(prefix []byte) []byte {
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 255 {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

// Catalog/schema/profile bytes are bound by the initialized configuration and
// semantic image. Routing publications/ownership live outside that image, so
// compare their ENTIRE exact native namespace footprint, including empty pages.
func conditionAuthority(q *reader, configuration [32]byte) ([32]byte, error) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("rho-tkg:conditional-native-authority:v1\x00"))
	_, _ = hash.Write(configuration[:])
	n := graphstore.Namespace{Graph: graphstate.GraphID(q.ns.graph), Partition: q.ns.partition}
	definition := graphstore.RoutingDefinitionKey(n)
	value, found, err := q.getBounded(definition, 1<<20)
	if err != nil {
		return [32]byte{}, err
	}
	if !found {
		return [32]byte{}, replica.ErrUnavailable
	}
	row := raftlog.KV{Key: definition, Value: value}
	write := func(row raftlog.KV) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(row.Key)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(row.Key)
		binary.BigEndian.PutUint64(length[:], uint64(len(row.Value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(row.Value)
	}
	write(row)
	prefixes := [][]byte{graphstore.RangePublicationKey(n, 0), graphstore.RangeOwnershipKey(n, 0), graphstore.RangeEndKey(n, 0)}
	for _, key := range prefixes {
		prefix := key[:len(key)-8]
		var after []byte
		for {
			rows := min(32, q.view.ReadLimits().Rows, q.limits.readRows-q.rows)
			byteLimit := min(32<<10, q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)
			if rows < 1 || byteLimit < 512 {
				return [32]byte{}, errLimit
			}
			if err := reserveComposer(q, 1024+4*byteLimit); err != nil {
				return [32]byte{}, err
			}
			page, err := q.view.Scan(q.ctx, prefix, conditionPrefixUpper(prefix), after, raftlog.ReadBudget{Rows: rows, Bytes: byteLimit})
			q.rows += page.Visited
			q.bytes += page.Bytes
			if err != nil {
				return [32]byte{}, errors.Join(replica.ErrUnavailable, err)
			}
			for _, row := range page.Rows {
				if !bytes.HasPrefix(row.Key, prefix) || row.Deleted {
					return [32]byte{}, errCorrupt
				}
				write(row)
			}
			if page.Complete {
				break
			}
			if len(page.Next) == 0 || len(after) > 0 && bytes.Compare(page.Next, after) <= 0 {
				return [32]byte{}, errCorrupt
			}
			after = page.Next
		}
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func (s *graphPeerService) captureConditional(ctx context.Context, r graphSourceRequest) (body []byte, err error) {
	cert, rows, byteBudget, output, err := decodeConditionCapture(r.command)
	if err != nil {
		return nil, err
	}
	if rows < 16 || byteBudget < 32768 || output < 32768 {
		return nil, errLimit
	}
	if !conditionalGraphAgreement(s.host.machine.SemanticContractID()) || cert.scope != s.peer.scope {
		return nil, errInvalid
	}
	question := allocationQuery{kind: observeConfiguration, nonce: r.nonce}
	reply, err := s.pump.Allocation(ctx, question)
	if err != nil || reply.err != nil {
		return nil, errors.Join(replica.ErrUnavailable, err, reply.err)
	}
	if reply.proof.owner != s.host || reply.question != question || reply.id != reply.proof.id {
		return nil, errInvalid
	}
	info, view, err := s.sources.capture(ctx, reply.proof)
	if err != nil {
		if view != nil {
			err = errors.Join(err, view.Close())
		}
		return nil, errors.Join(replica.ErrUnavailable, err)
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			body = nil
		}
	}()
	lease, err := s.sources.lease(r.cookie)
	if err != nil {
		return nil, errors.Join(replica.ErrUnavailable, err)
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.lifetimeNow != nil && lease.remainingLifetime(ctx) == 0 {
		return nil, replica.ErrUnavailable
	}
	if lease.closed || lease.ctx.Err() != nil || lease.info.root.Index != cert.sourceIndex || !bytes.Equal(lease.info.root.Image, cert.image[:]) {
		return nil, replica.ErrUnavailable
	}
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if s.host.unavailableLocked() {
		return nil, replica.ErrUnavailable
	}
	base := info.root
	arena, err := graphstate.NewOutputBudget(min(output, s.host.machine.limits.outputBytes))
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(16384); err != nil {
		return nil, outputBudgetFailure(err)
	}
	l := s.host.machine.limits.allocation
	l.readRows = min(rows, s.host.machine.limits.sourceRows) - 16
	l.readBytes = min(byteBudget, s.host.machine.limits.sourceBytes) - 16384
	if l.readRows < 1 || l.readBytes < 1024 {
		return nil, errLimit
	}
	q := reader{ctx: ctx, view: view, ns: s.host.machine.ns, base: base, limits: l, arena: arena, bytes: cap(base.Image)}
	part, err := s.host.machine.captureReadCondition(&q, lease.view, cert)
	status := byte(1)
	if errors.Is(err, errGraphReadConflict) {
		status = 2
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if err := lease.ctx.Err(); err != nil {
		return nil, errors.Join(replica.ErrUnavailable, err)
	}
	observation, err := encodeAllocationObservation(info.observation)
	if err != nil {
		return nil, err
	}
	remaining := time.Duration(0)
	if lease.lifetimeNow != nil {
		remaining = lease.remainingLifetime(ctx)
		if remaining == 0 {
			return nil, replica.ErrUnavailable
		}
	}
	return boundedEncoding(8192, func(w *boundedWriter) {
		w.add([]byte{'G', 'U', 'I', 1})
		w.tag(status)
		w.u32(q.rows + 16)
		w.u32(q.bytes + 16384)
		w.u32(output - arena.Remaining())
		w.u64(part.prefix)
		w.add(part.authority[:])
		// #nosec G115 -- Remaining native lease lifetime is clamped nonnegative and bounded by one minute.
		if lease.lifetimeNow == nil {
			w.u64(uint64(max(time.Duration(0), time.Until(lease.deadline))))
		} else {
			w.u64(uint64(remaining))
		}
		w.add(info.readID.session[:])
		w.u64(info.readID.sequence)
		w.add(info.nonce[:])
		w.field(observation)
		emitSourceRoot(w, info.root)
		w.u64(info.floor)
	})
}
func (m *declaredMaterializer) captureReadCondition(q *reader, old *raftlog.ApplicationView, cert graphCertificate) (graphReadConditionPart, error) {
	actual, found, err := q.getBounded(graphCertificateKey(q.ns, cert.round), 512)
	if err != nil || !found {
		return graphReadConditionPart{}, errors.Join(replica.ErrUnavailable, err)
	}
	stored, err := decodeGraphCertificate(actual)
	if err != nil || stored != cert {
		return graphReadConditionPart{}, errors.Join(errInvalid, err)
	}
	cfg, err := m.initialized(q)
	if err != nil {
		return graphReadConditionPart{}, err
	}
	configuration, err := cfg.digest()
	if err != nil || configuration != cert.configuration {
		return graphReadConditionPart{}, errors.Join(errGraphCutScope, err)
	}
	if !bytes.Equal(q.base.Image, cert.image[:]) {
		return graphReadConditionPart{}, errGraphReadConflict
	}
	_, held, err := m.readCrossResourceOwner(q)
	if err != nil {
		return graphReadConditionPart{}, err
	}
	move, err := readRangeMoveActive(q)
	if err != nil {
		return graphReadConditionPart{}, err
	}
	if held || move != ([16]byte{}) {
		return graphReadConditionPart{}, replica.ErrUnavailable
	}
	current, err := conditionAuthority(q, configuration)
	if err != nil {
		return graphReadConditionPart{}, err
	}
	prior := *q
	prior.view = old
	oldAuthority, err := conditionAuthority(&prior, configuration)
	q.rows, q.bytes = prior.rows, prior.bytes
	if err != nil {
		return graphReadConditionPart{}, err
	}
	if current != oldAuthority {
		return graphReadConditionPart{}, errGraphReadConflict
	}
	return graphReadConditionPart{certificate: cert, prefix: q.base.Index, authority: current}, nil
}

type graphConditionCapture struct {
	part   graphReadConditionPart
	source capturedGraphSource
}

func (s *remoteGraphSource) captureCondition(ctx context.Context, l materializerLimits) (result graphConditionCapture, err error) {
	var part graphReadConditionPart
	if s == nil || s.certified == nil || ctx == nil {
		return result, errInvalid
	}
	if err := s.ctx.Err(); err != nil {
		return result, errors.Join(replica.ErrUnavailable, err)
	}
	b := s.endpoint.budget
	b.mu.Lock()
	rows := min(l.sourceRows, 1<<20, b.maxRows-b.rows)
	byteBudget := min(l.sourceBytes, 64<<20, b.maxBytes-b.bytes-16384)
	output := min(l.outputBytes, 64<<20, b.output.Remaining()-65536)
	b.mu.Unlock()
	if rows < 16 || byteBudget < 32768 || output < 32768 {
		return result, errLimit
	}
	wire, err := encodeConditionCapture(s.certified.certificate, rows, byteBudget, output)
	if err != nil {
		return result, err
	}
	defer func() {
		if err != nil {
			b.mu.Lock()
			b.rows += rows
			b.bytes += byteBudget
			reserveErr := b.output.Reserve(output)
			b.mu.Unlock()
			err = errors.Join(err, outputBudgetFailure(reserveErr))
		}
	}()
	r, err := s.endpoint.call(ctx, graphSourceRequest{kind: sourceCertifiedOpen, cookie: s.info.cookie, command: wire}, 8192)
	if err != nil {
		return result, err
	}
	body, err := graphBody(r.body, "GUI\x01", 8192)
	if err != nil {
		return result, err
	}
	c := graphCursor{b: body}
	status := c.tag()
	usedRows, usedBytes, usedOutput := c.count(rows, 0), c.count(byteBudget, 0), c.count(output, 0)
	part = graphReadConditionPart{certificate: s.certified.certificate, prefix: c.u64()}
	copy(part.authority[:], c.take(32))
	ttl := c.u64()
	id := allocationReadID{session: c.array(), sequence: c.u64()}
	nonce := c.array()
	observationWire := c.field(allocationObservationMaxBytes)
	root := readSourceRoot(&c)
	floor := c.u64()
	if c.err != nil || len(c.b) != 0 || status < 1 || status > 2 || ttl == 0 || ttl > uint64(time.Minute) {
		return graphConditionCapture{}, errors.Join(errCorrupt, c.err)
	}
	observation, decodeErr := decodeAllocationObservation(observationWire)
	if decodeErr != nil || id.session != r.issuer || id.sequence == 0 || nonce != r.nonce || observation.kind != observeConfiguration || observation.source.scope != s.certified.certificate.scope || observation.configuration != s.certified.certificate.configuration || root.Index != observation.source.index {
		return graphConditionCapture{}, errors.Join(errInvalid, decodeErr)
	}
	if status == 2 {
		return graphConditionCapture{}, errGraphReadConflict
	}
	metadata, rootErr := graphstore.DecodeRoot(root.Image)
	if rootErr != nil || metadata.SemanticEpoch() != observation.epoch || metadata.EffectDigest() != observation.effect || part.prefix != root.Index || !bytes.Equal(root.Image, part.certificate.image[:]) || part.prefix < s.certified.certificate.index || part.authority == ([32]byte{}) {
		return graphConditionCapture{}, errCorrupt
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if usedRows > b.maxRows-b.rows || usedBytes > b.maxBytes-b.bytes {
		return graphConditionCapture{}, errLimit
	}
	if err := b.output.Reserve(usedOutput); err != nil {
		return graphConditionCapture{}, outputBudgetFailure(err)
	}
	b.rows += usedRows
	b.bytes += usedBytes
	return graphConditionCapture{part: part, source: capturedGraphSource{observation: observation, root: root, floor: floor, data: s}}, nil
}

func conditionalBusiness(base []byte, value graphReadConditionValue, l materializerLimits) ([]byte, error) {
	wire, err := encodeGraphReadConditionValue(value, l)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(l.commandBytes, func(w *boundedWriter) { w.add([]byte{'G', 'U', 'B', 1}); w.field(base); w.field(wire) })
}

// RunIfUnchanged binds this exact captured condition to the original business
// identity. Any intervening native prefix conservatively conflicts; the caller
// must read/evaluate again and use a new request. Unavailable evidence is pending.
func (c *crossProcessCoordinator) RunIfUnchanged(ctx context.Context, id requestID, ops []graphstate.Operation, provenance uint64, claims []freshBinding, condition graphReadCondition) (crossProcessResult, error) {
	if !condition.validSeal() || condition.stamp.owner == nil {
		return crossProcessResult{}, errInvalid
	}
	return c.run(ctx, id, ops, provenance, claims, &condition, false, nil)
}

// Only ResumeConditional constructs this continuation after its authenticated
// original lookup. Lookup, retained codecs and remaining closure consume one
// existing ledger; it cannot fund fresh registration or bless a client condition.
type conditionalResumeInvocation struct {
	arena     *graphstate.OutputBudget
	budget    *graphRemoteBudget
	endpoints map[uint64]*graphAuthenticatedEndpoint
}

// ResumeConditional only uses an authenticated retained ORIGINAL. Decoding an
// arbitrary condition cannot open this door or authorize a new registration.
func (c *crossProcessCoordinator) ResumeConditional(ctx context.Context, resume crossProcessResume) (crossProcessResult, error) {
	if c == nil || c.host == nil || c.host.machine == nil || ctx == nil || resume.scope != c.host.machine.scope() || resume.request == (requestID{}) || resume.tx != crossTransactionID(c.host.machine.ns, resume.request) {
		return crossProcessResult{}, errInvalid
	}
	l := c.host.machine.limits
	arena, err := graphstate.NewOutputBudget(min(l.outputBytes, l.sourceBytes))
	if err != nil {
		return crossProcessResult{}, err
	}
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		return crossProcessResult{}, err
	}
	original, err := endpoints[c.host.machine.ns.partition].lookup(ctx, resume.tx)
	if err != nil || !original.present {
		return crossProcessResult{resume: resume}, errors.Join(errCrossProcessPending, err)
	}
	m, err := decodeCrossManifest(original.manifest, l, arena)
	if err != nil || m.condition == nil {
		return crossProcessResult{}, errors.Join(errInvalid, err)
	}
	revision, err := state.NewRevision(1, m.request.revision.Provenance())
	if err != nil {
		return crossProcessResult{}, err
	}
	request := m.request
	request.revision = revision
	if err := arena.Reserve(1024 + 8*len(m.original)); err != nil {
		return crossProcessResult{}, outputBudgetFailure(err)
	}
	base, err := encodePartitionGraphCommand(partitionGraphCommand{configuration: m.configuration, declaration: m.declaration, routing: m.routing, request: request}, l)
	if err != nil {
		return crossProcessResult{}, err
	}
	if err := arena.Reserve(1024 + 8*len(base) + 8192*len(m.condition.parts)); err != nil {
		return crossProcessResult{}, outputBudgetFailure(err)
	}
	business, err := conditionalBusiness(base, *m.condition, l)
	if err != nil || sha256.Sum256(business) != resume.business {
		return crossProcessResult{}, errors.Join(errInvalid, err)
	}
	condition := graphReadCondition{value: *m.condition}
	if err := arena.Reserve(256); err != nil {
		return crossProcessResult{}, outputBudgetFailure(err)
	}
	return c.run(ctx, resume.request, request.operations, request.revision.Provenance(), request.claims, &condition, true, &conditionalResumeInvocation{arena: arena, budget: budget, endpoints: endpoints})
}
func checkConditionSources(v graphReadConditionValue, sources []capturedGraphSource, d graphstore.OwnershipDeclaration) error {
	if !v.valid() || len(v.parts) != d.Len() || len(sources) != d.Len() {
		return errGraphCutScope
	}
	for i, part := range v.parts {
		declared, ok := d.Partition(part.certificate.scope.partition)
		if !ok || declared.Group != part.certificate.scope.group || declared.OwnershipEpoch != part.certificate.scope.ownership {
			return errGraphCutScope
		}
		source := sources[i]
		if source.observation.source.scope != part.certificate.scope || source.observation.configuration != part.certificate.configuration {
			return errGraphCutScope
		}
		if source.root.Index < part.prefix {
			return replica.ErrUnavailable
		}
		if source.root.Index != part.prefix || !bytes.Equal(source.root.Image, part.certificate.image[:]) {
			return errGraphReadConflict
		}
	}
	return nil
}

// Every conditional vote checks native evidence BEFORE writing a YES kernel,
// lock or effects. The only permitted prefix advancement is THIS coordinator's
// exact original Register. Raft election no-ops/read fences conservatively
// conflict too; no payload guessed to be harmless is silently skipped.
func (m *declaredMaterializer) checkConditionalPrepare(q *reader, manifest crossManifest) (bool, error) {
	if manifest.condition == nil {
		return true, nil
	}
	if !conditionalGraphAgreement(m.SemanticContractID()) {
		return false, errInvalid
	}
	part, ok := manifest.condition.part(m.ns.partition)
	if !ok || part.certificate.scope != m.scope() {
		return false, errGraphCutScope
	}
	if q.base.Index < part.prefix {
		return false, replica.ErrUnavailable
	}
	wire, found, err := q.getBounded(graphCertificateKey(q.ns, part.certificate.round), 512)
	if err != nil || !found {
		return false, errors.Join(replica.ErrUnavailable, err)
	}
	cert, err := decodeGraphCertificate(wire)
	if err != nil || cert != part.certificate {
		return false, errors.Join(errInvalid, err)
	}
	expected := part.prefix
	if m.ns.partition == manifest.coordinator {
		registration, present, err := crossControl(q, crossControlKey(q.ns, manifest.tx, 'R'), m.limits.commandBytes)
		if err != nil || !present {
			return false, errors.Join(replica.ErrUnavailable, err)
		}
		if sha256.Sum256(registration.Value) != manifest.digest {
			return false, errCorrupt
		}
		if registration.Index != part.prefix+1 {
			return false, nil
		}
		expected = registration.Index
	}
	if q.base.Index != expected || !bytes.Equal(q.base.Image, part.certificate.image[:]) {
		return false, nil
	}
	current, err := conditionAuthority(q, part.certificate.configuration)
	if err != nil {
		return false, err
	}
	return current == part.authority, nil
}

// Borrow an already authenticated native retained source without another lease
// or cleanup owner. Only the new invocation's endpoint/work budget changes;
// cookie, historical root, limits, context and lifetime remain the original.
// No Close is called on these borrowed adapters: the conditional read owns it.
func borrowConditionSources(ctx context.Context, condition graphReadCondition, endpoints map[uint64]*graphAuthenticatedEndpoint) ([]capturedGraphSource, error) {
	owner := condition.stamp.owner
	if err := owner.active(); err != nil {
		return nil, err
	}
	owner.read.mu.Lock()
	if err := reserveConditionBudget(owner.budget, 256+32*len(owner.read.sources)); err != nil {
		owner.read.mu.Unlock()
		return nil, err
	}
	sources := slices.Clone(owner.read.sources)
	owner.read.mu.Unlock()
	if len(sources) != len(condition.value.parts) {
		return nil, errGraphCutScope
	}
	first := endpoints[condition.value.parts[0].certificate.scope.partition]
	if first == nil {
		return nil, replica.ErrUnavailable
	}
	if err := reserveConditionBudget(first.budget, 256+2048*len(sources)); err != nil {
		return nil, err
	}
	captured := make([]capturedGraphSource, len(sources))
	for i, source := range sources {
		original, ok := source.(*remoteGraphSource)
		if !ok || original.certified == nil {
			return nil, errGraphCutScope
		}
		endpoint := endpoints[original.certified.certificate.scope.partition]
		if endpoint == nil || endpoint.peer != original.endpoint.peer {
			return nil, replica.ErrUnavailable
		}
		if err := original.mu.lock(ctx); err != nil {
			return nil, err
		}
		if original.closed {
			original.mu.unlock()
			return nil, graphstore.ErrClosed
		}
		borrowed := &remoteGraphSource{mu: original.mu, endpoint: endpoint, certified: original.certified, info: original.info, ctx: original.ctx, cancel: original.cancel, deadline: original.deadline}
		original.mu.unlock()
		capture, err := borrowed.captureCondition(ctx, defaultMaterializerLimits())
		if err != nil {
			return nil, err
		}
		if capture.part != condition.value.parts[i] {
			return nil, errGraphReadConflict
		}
		captured[i] = capture.source
	}
	return captured, nil
}
