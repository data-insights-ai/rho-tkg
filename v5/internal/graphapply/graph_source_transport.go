package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type graphPeerIdentity struct {
	scope allocationScope
	voter uint64
}

// Peer is the embedding's authenticated endpoint association, established from
// its configured connection/pipe. It must not be obtained from self-reported
// reply fields. RoundTrip keeps that same association under the crash-only model.
// No decoded application body or caller bool grants authenticated identity.
type graphPeerWireTransport interface {
	Peer() graphPeerIdentity
	RoundTrip(context.Context, []byte) ([]byte, error)
}
type graphWireResponse struct {
	peer     graphPeerIdentity
	issuer   [16]byte
	kind     byte
	sequence uint64
	nonce    [16]byte
	body     []byte
	err      error
}
type graphWireError struct{ mask uint16 }

var errRemoteOperationFailure = errors.New("graphapply: remote operation failed without classified cause")

var graphWireCauses = []error{errInvalid, errLimit, errCorrupt, replica.ErrUnavailable, replica.ErrStopped, raftlog.ErrInvalid, raftlog.ErrLimit, raftlog.ErrCorrupt, raftlog.ErrClosed, raftlog.ErrPoisoned, graphstore.ErrSourceUnavailable, context.Canceled, context.DeadlineExceeded, errRemoteOperationFailure, graphstore.ErrResourceLimit, graphstate.ErrResourceLimit}

func (e graphWireError) Error() string { return "graphapply: remote source operation refused" }
func (e graphWireError) Is(target error) bool {
	for i, cause := range graphWireCauses {
		if target == cause && e.mask&(1<<i) != 0 {
			return true
		}
	}
	return false
}
func graphErrorMask(err error) uint16 {
	var mask uint16
	for i, cause := range graphWireCauses {
		if errors.Is(err, cause) {
			mask |= 1 << i
		}
	}
	if err != nil && mask == 0 {
		mask = 1 << 13
	}
	return mask
}
func encodeGraphWireResponse(r graphWireResponse) ([]byte, error) {
	scope, err := encodeAllocationScope(r.peer.scope)
	if err != nil {
		return nil, err
	}
	if r.peer.voter == 0 || r.sequence == 0 || r.nonce == ([16]byte{}) || r.issuer == ([16]byte{}) || r.kind < sourceOpen || r.kind > sourceCertifiedOpen || len(r.body) > graphSourceFrameBytes-512 {
		return nil, errInvalid
	}
	if r.err != nil && len(r.body) != 0 {
		return nil, errInvalid
	}
	return boundedEncoding(graphSourceFrameBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'S', 'R', 1})
		w.field(scope)
		w.u64(r.peer.voter)
		w.add(r.issuer[:])
		w.tag(r.kind)
		w.u64(r.sequence)
		w.add(r.nonce[:])
		w.u16(graphErrorMask(r.err))
		w.field(r.body)
	})
}
func decodeGraphWireResponse(wire []byte) (graphWireResponse, error) {
	body, err := graphBody(wire, "GSR\x01", graphSourceFrameBytes)
	if err != nil {
		return graphWireResponse{}, err
	}
	c := graphCursor{b: body}
	scope := c.field(allocationScopeBytes)
	r := graphWireResponse{peer: graphPeerIdentity{voter: c.u64()}, issuer: c.array(), kind: c.tag(), sequence: c.u64(), nonce: c.array()}
	mask := c.u16()
	r.body = c.field(graphSourceFrameBytes - 512)
	if c.err != nil || len(c.b) != 0 || int(mask) >= 1<<len(graphWireCauses) {
		return graphWireResponse{}, errors.Join(errCorrupt, c.err)
	}
	r.peer.scope, err = decodeAllocationScope(scope)
	if err != nil {
		return graphWireResponse{}, err
	}
	if mask != 0 {
		r.err = graphWireError{mask}
	}
	if r.peer.voter == 0 || r.sequence == 0 || r.nonce == ([16]byte{}) || r.issuer == ([16]byte{}) || r.kind < sourceOpen || r.kind > sourceCertifiedOpen || mask != 0 && len(r.body) != 0 {
		return graphWireResponse{}, errCorrupt
	}
	return r, nil
}

// graphRemoteBudget spans ALL endpoints and data operations of one request.
// Representation is conservatively charged independently of server work limits.
type graphRemoteBudget struct {
	mu                          sync.Mutex
	calls, bytes, rows          int
	maxCalls, maxBytes, maxRows int
	output                      graphstateBudget
}

// This tiny wrapper avoids an independent/default arena at each RPC.
type graphstateBudget interface {
	Reserve(int) error
	Remaining() int
}

func newGraphRemoteBudget(l materializerLimits, output graphstateBudget) *graphRemoteBudget {
	return &graphRemoteBudget{maxCalls: 1024, maxBytes: l.sourceBytes, maxRows: l.sourceRows, output: output}
}
func (b *graphRemoteBudget) admit(request int, maximum int) error {
	if b == nil || b.output == nil || request < 0 || maximum < 0 || maximum > graphSourceFrameBytes {
		return errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.calls >= b.maxCalls || request > b.maxBytes-b.bytes || 4*(request+maximum)+512 > b.output.Remaining() {
		return errLimit
	}
	if err := b.output.Reserve(4*request + 512); err != nil {
		return outputBudgetFailure(err)
	}
	b.calls++
	b.bytes += request
	return nil
}
func (b *graphRemoteBudget) receive(length int, rows int) error {
	if b == nil || b.output == nil {
		return errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if length < 0 || rows < 0 || length > b.maxBytes-b.bytes || rows > b.maxRows-b.rows {
		return errLimit
	}
	if err := b.output.Reserve(4*length + 128); err != nil {
		return outputBudgetFailure(err)
	}
	b.bytes += length
	b.rows += rows
	return nil
}

// cleanup precharges a known handle's one close exchange before opening it.
// The private pool cannot widen any parent limit: its maximum call/bytes/output
// are already consumed on the original shared ledger, even if never used.
func (b *graphRemoteBudget) cleanup() (*graphRemoteBudget, error) {
	if b == nil || b.output == nil {
		return nil, errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	const bytes = 2048
	const output = 8192
	if b.calls >= b.maxCalls || bytes > b.maxBytes-b.bytes || output > b.output.Remaining() {
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
	return &graphRemoteBudget{maxCalls: 1, maxBytes: bytes, maxRows: 0, output: arena}, nil
}

// protectCompletion prepays a conservative finite control phase before YES.
// The detached arena spends only capacity already consumed on the original
// ledger; it cannot reset the cumulative RPC byte/call counters.
func (b *graphRemoteBudget) protectCompletion(parts, manifestBytes int) (*graphstate.OutputBudget, error) {
	if b == nil || b.output == nil || parts < 1 || parts > 16 || manifestBytes < 1 || manifestBytes > 1<<20 {
		return nil, errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	calls := 4*parts + 6
	frame := manifestBytes + 4096
	wire := 2 * calls * frame
	minimum := 4*wire + calls*1024
	retained := b.output.Remaining()
	if calls > b.maxCalls-b.calls || wire > b.maxBytes-b.bytes || minimum > retained {
		return nil, errLimit
	}
	arena, err := graphstate.NewOutputBudget(retained)
	if err != nil {
		return nil, outputBudgetFailure(err)
	}
	if err := b.output.Reserve(retained); err != nil {
		return nil, outputBudgetFailure(err)
	}
	b.output = arena
	return arena, nil
}

// One token serializes a stream without making canceled waiters borrow the
// lifetime of an unrelated operation already using that connection.
type graphRequestGate struct{ token chan struct{} }

func newGraphRequestGate() graphRequestGate {
	g := graphRequestGate{token: make(chan struct{}, 1)}
	g.token <- struct{}{}
	return g
}
func (g graphRequestGate) lock(ctx context.Context) error {
	if ctx == nil || g.token == nil {
		return errInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
	}
	if err := ctx.Err(); err != nil {
		g.unlock()
		return err
	}
	return nil
}
func (g graphRequestGate) tryLock() bool {
	if g.token == nil {
		return false
	}
	select {
	case <-g.token:
		return true
	default:
		return false
	}
}
func (g graphRequestGate) unlock() { g.token <- struct{}{} }

type graphAuthenticatedStream struct {
	mu       graphRequestGate
	sequence uint64
	issuer   [16]byte
}

type graphAuthenticatedEndpoint struct {
	stream    *graphAuthenticatedStream
	transport graphPeerWireTransport
	peer      graphPeerIdentity
	budget    *graphRemoteBudget
}

func newGraphAuthenticatedEndpoint(d graphstore.OwnershipDeclaration, transport graphPeerWireTransport, budget *graphRemoteBudget) (*graphAuthenticatedEndpoint, error) {
	if transport == nil || budget == nil {
		return nil, errInvalid
	}
	peer := transport.Peer()
	part, found := d.Partition(peer.scope.partition)
	if !found || peer.voter == 0 || peer.scope.graph != [16]byte(d.Graph()) || peer.scope.declaration != d.Digest() || peer.scope.topology != d.TopologyEpoch() || peer.scope.group != part.Group || peer.scope.ownership != part.OwnershipEpoch || !crossGraphAgreement(peer.scope.semantic) {
		return nil, errInvalid
	}
	return &graphAuthenticatedEndpoint{stream: &graphAuthenticatedStream{mu: newGraphRequestGate()}, transport: transport, peer: peer, budget: budget}, nil
}
func (e *graphAuthenticatedEndpoint) call(ctx context.Context, request graphSourceRequest, maximum int) (graphWireResponse, error) {
	if e == nil {
		return graphWireResponse{}, errInvalid
	}
	return e.callUsing(ctx, request, maximum, e.budget)
}
func (e *graphAuthenticatedEndpoint) callUsing(ctx context.Context, request graphSourceRequest, maximum int, budget *graphRemoteBudget) (graphWireResponse, error) {
	if e == nil || ctx == nil {
		return graphWireResponse{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphWireResponse{}, err
	}
	if e.stream == nil {
		return graphWireResponse{}, errInvalid
	}
	if err := e.stream.mu.lock(ctx); err != nil {
		return graphWireResponse{}, err
	}
	defer e.stream.mu.unlock()
	if e.stream.sequence == math.MaxUint64 {
		return graphWireResponse{}, errLimit
	}
	e.stream.sequence++
	request.sequence = e.stream.sequence
	if _, err := rand.Read(request.nonce[:]); err != nil {
		return graphWireResponse{}, err
	}
	wire, err := encodeGraphSourceRequest(request)
	if err != nil {
		return graphWireResponse{}, err
	}
	if err := budget.admit(len(wire), maximum); err != nil {
		return graphWireResponse{}, err
	}
	reply, err := e.transport.RoundTrip(ctx, wire)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return graphWireResponse{}, errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	if len(reply) > maximum {
		return graphWireResponse{}, errLimit
	}
	if err := budget.receive(len(reply), 0); err != nil {
		return graphWireResponse{}, err
	}
	r, err := decodeGraphWireResponse(reply)
	if err != nil {
		return graphWireResponse{}, err
	}
	if r.peer != e.peer || r.kind != request.kind || r.sequence != request.sequence || r.nonce != request.nonce {
		return graphWireResponse{}, errInvalid
	}
	if e.stream.issuer == ([16]byte{}) {
		e.stream.issuer = r.issuer
	} else if r.issuer != e.stream.issuer {
		return graphWireResponse{}, graphstore.ErrSourceUnavailable
	}
	if r.err != nil {
		return graphWireResponse{}, r.err
	}
	return r, nil
}

type remoteGraphSource struct {
	mu        graphRequestGate
	endpoint  *graphAuthenticatedEndpoint
	info      graphSourceInfo
	certified *graphCertifiedSourceInfo
	ctx       context.Context
	cancel    context.CancelFunc
	deadline  time.Time
	closed    bool
	closeErr  error
	cleanup   *graphRemoteBudget
}

func (e *graphAuthenticatedEndpoint) openSource(ctx context.Context) (*remoteGraphSource, error) {
	if e == nil || ctx == nil {
		return nil, errInvalid
	}
	cleanup, err := e.budget.cleanup()
	if err != nil {
		return nil, err
	}
	started := time.Now()
	r, err := e.call(ctx, graphSourceRequest{kind: sourceOpen}, 8192)
	if err != nil {
		return nil, err
	}
	info, err := decodeGraphSourceInfo(r.body)
	if err != nil {
		return nil, err
	}
	if info.observation.source.scope != e.peer.scope || info.cookie.issuer != r.issuer || info.nonce != r.nonce || info.readID.session != r.issuer {
		return nil, errInvalid
	}
	ownerCtx, cancel := context.WithDeadline(ctx, started.Add(info.lifetime))
	return &remoteGraphSource{mu: newGraphRequestGate(), endpoint: e, info: info, ctx: ownerCtx, cancel: cancel, deadline: started.Add(info.lifetime), cleanup: cleanup}, nil
}
func (s *remoteGraphSource) Root() (raftlog.ApplicationRoot, error) {
	if s == nil {
		return raftlog.ApplicationRoot{}, errInvalid
	}
	if !s.mu.tryLock() {
		return raftlog.ApplicationRoot{}, graphstore.ErrSourceUnavailable
	}
	defer s.mu.unlock()
	if s.closed {
		return raftlog.ApplicationRoot{}, raftlog.ErrClosed
	}
	if time.Now().After(s.deadline) {
		return raftlog.ApplicationRoot{}, graphstore.ErrSourceUnavailable
	}
	if err := s.ctx.Err(); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	// Compatibility metadata copy is nonblocking and still consumes ownership.
	if err := s.endpoint.budget.receive(256+cap(s.info.root.Image), 0); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	r := s.info.root
	r.Image = owned(r.Image)
	return r, nil
}

// RootBounded owns the authenticated capture's immutable metadata. Success is
// not a live-peer, readiness, quorum or cut assertion. Required graph data must
// still be read through the retained identity-bound Get/Scan source session.
func (s *remoteGraphSource) RootBounded(ctx context.Context, maximum int) (raftlog.ApplicationRoot, error) {
	if s == nil || ctx == nil || maximum < 0 {
		return raftlog.ApplicationRoot{}, raftlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	if !s.mu.tryLock() {
		return raftlog.ApplicationRoot{}, graphstore.ErrSourceUnavailable
	}
	defer s.mu.unlock()
	if s.closed {
		return raftlog.ApplicationRoot{}, raftlog.ErrClosed
	}
	if err := s.ctx.Err(); err != nil {
		return raftlog.ApplicationRoot{}, errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	if time.Now().After(s.deadline) {
		return raftlog.ApplicationRoot{}, graphstore.ErrSourceUnavailable
	}
	if len(s.info.root.Image) > maximum {
		return raftlog.ApplicationRoot{}, raftlog.ErrLimit
	}
	if err := s.endpoint.budget.receive(256+cap(s.info.root.Image), 0); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	root := s.info.root
	root.Image = owned(root.Image)
	return root, nil
}

func (s *remoteGraphSource) ApplicationBinding(ctx context.Context) (raftlog.ApplicationBinding, error) {
	if s == nil || ctx == nil {
		return raftlog.ApplicationBinding{}, errInvalid
	}
	if _, err := s.RootBounded(ctx, 172); err != nil {
		return raftlog.ApplicationBinding{}, err
	}
	p := s.info.observation.source.scope
	if s.certified != nil {
		p = s.certified.certificate.scope
	}
	return raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(p.graph), Partition: p.partition, Group: p.group}, SemanticContractID: p.semantic}, nil
}
func (s *remoteGraphSource) ReadLimits() raftlog.ReadBudget {
	if s == nil {
		return raftlog.ReadBudget{}
	}
	return s.info.limits
}
func (s *remoteGraphSource) Get(ctx context.Context, key []byte, maximum int) (raftlog.KV, bool, error) {
	if s == nil || ctx == nil {
		return raftlog.KV{}, false, errInvalid
	}
	if err := s.mu.lock(ctx); err != nil {
		return raftlog.KV{}, false, err
	}
	defer s.mu.unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := s.ctx.Err(); err != nil {
		return raftlog.KV{}, false, errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	if time.Now().After(s.deadline) {
		return raftlog.KV{}, false, graphstore.ErrSourceUnavailable
	}
	if s.closed {
		return raftlog.KV{}, false, raftlog.ErrClosed
	}
	if maximum < 0 || maximum > min(s.info.limits.Bytes, graphSourceFrameBytes-1024) {
		return raftlog.KV{}, false, errLimit
	}
	r, err := s.endpoint.call(ctx, graphSourceRequest{kind: sourceGet, cookie: s.info.cookie, key: key, maximum: maximum}, maximum+1024)
	if err != nil {
		return raftlog.KV{}, false, err
	}
	body, err := graphBody(r.body, "GSK\x01", graphSourceFrameBytes-512)
	if err != nil {
		return raftlog.KV{}, false, err
	}
	c := graphCursor{b: body}
	flag := c.tag()
	if flag > 1 {
		return raftlog.KV{}, false, errCorrupt
	}
	if flag == 0 {
		if len(c.b) != 0 {
			return raftlog.KV{}, false, errCorrupt
		}
		if err := s.endpoint.budget.receive(0, 1); err != nil {
			return raftlog.KV{}, false, err
		}
		return raftlog.KV{}, false, nil
	}
	row := readSourceKV(&c, maximum)
	if c.err != nil || len(c.b) != 0 || !bytes.Equal(row.Key, key) {
		return raftlog.KV{}, false, errors.Join(errCorrupt, c.err)
	}
	if err := s.endpoint.budget.receive(0, 1); err != nil {
		return raftlog.KV{}, false, err
	}
	row.Key, row.Value = owned(row.Key), owned(row.Value)
	return row, true, nil
}
func (s *remoteGraphSource) Scan(ctx context.Context, lower, upper, after []byte, b raftlog.ReadBudget) (raftlog.ApplicationPage, error) {
	if s == nil || ctx == nil {
		return raftlog.ApplicationPage{}, errInvalid
	}
	if err := s.mu.lock(ctx); err != nil {
		return raftlog.ApplicationPage{}, err
	}
	defer s.mu.unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := s.ctx.Err(); err != nil {
		return raftlog.ApplicationPage{}, errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	if time.Now().After(s.deadline) {
		return raftlog.ApplicationPage{}, graphstore.ErrSourceUnavailable
	}
	if s.closed {
		return raftlog.ApplicationPage{}, raftlog.ErrClosed
	}
	b.Bytes = min(b.Bytes, graphSourceFrameBytes-1024)
	r, err := s.endpoint.call(ctx, graphSourceRequest{kind: sourceScan, cookie: s.info.cookie, key: lower, upper: upper, after: after, maximum: b.Bytes, rows: b.Rows}, b.Bytes+1024)
	if err != nil {
		return raftlog.ApplicationPage{}, err
	}
	page, err := decodeGraphSourcePage(r.body, b)
	if err != nil {
		return raftlog.ApplicationPage{}, err
	}
	if err := s.endpoint.budget.receive(page.Bytes, page.Visited); err != nil {
		return raftlog.ApplicationPage{}, err
	}
	return page, nil
}
func (s *remoteGraphSource) Close() error {
	if s == nil {
		return errInvalid
	}
	// Cancel the owner BEFORE waiting for an in-flight query's mutex/endpoint.
	s.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.mu.lock(ctx); err != nil {
		return err
	}
	defer s.mu.unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	_, s.closeErr = s.endpoint.callUsing(ctx, graphSourceRequest{kind: sourceClose, cookie: s.info.cookie}, 1024, s.cleanup)
	return s.closeErr
}

var _ graphstore.ApplicationReadSource = (*remoteGraphSource)(nil)
