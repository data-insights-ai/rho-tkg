package graphapply

import (
	"context"
	"crypto/rand"
	"errors"
	"math"
	"reflect"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type graphReadMode byte

const (
	graphReadFresh graphReadMode = iota + 1
	graphReadStable
	graphReadAt
	graphReadAfter
)

// These are process-private certificates; transport authenticates graph/group
// identities under the crash-only model. There is no scalar/log-offset fallback.
type graphCut struct{ certificates []graphCertificate }
type graphCommitReceipt struct {
	proof crossProof
	local graphCutProof
}
type graphReadRequest struct {
	mode    graphReadMode
	cut     graphCut
	receipt graphCommitReceipt
}

func graphReceiptFromDecision(p crossProof) (graphCommitReceipt, error) {
	if e := checkCrossProof(p, p.status.tx, p.status.digest); e != nil {
		return graphCommitReceipt{}, e
	}
	if p.status.phase != crossDecide || !p.status.commit || p.status.round == 0 || p.owner.machine.SemanticContractID() != certifiedGraphSemanticContractID() {
		return graphCommitReceipt{}, errInvalid
	}
	return graphCommitReceipt{proof: p}, nil
}

// Transport drives actual Host events and matches each accepted ReadID. Submit
// replies are decoded committed application outcomes, never enqueue booleans.
// Reads own cancellation of an accepted pending request on all failure paths.
type certifiedGraphTransport interface {
	Host(context.Context, uint64) (*allocationHost, error)
	Read(context.Context, *allocationHost, graphCutQuery) (graphCutReply, error)
	Submit(context.Context, *allocationHost, allocationProposal) (graphCutResult, error)
	Decision(context.Context, *allocationHost, [16]byte, [16]byte) (crossReply, error)
	Resolve(context.Context, *allocationHost, allocationProposal) (crossStatus, error)
}
type certifiedReadCoordinator struct {
	mu          sync.Mutex
	declaration graphstore.OwnershipDeclaration
	transport   certifiedGraphTransport
	stable      graphCut
}
type certifiedGraphRead struct {
	mu       sync.Mutex
	cut      graphCut
	full     *graphstore.PartitionTransactionView
	sources  []*raftlog.ApplicationView
	initial  graphstore.PageWork
	closed   bool
	closeErr error
}

func graphReadNonce() ([16]byte, error) { var n [16]byte; _, e := rand.Read(n[:]); return n, e }
func certifiedScope(d graphstore.OwnershipDeclaration, scope []uint64) ([]uint64, error) {
	if d.Len() == 0 || len(scope) > d.Len() {
		return nil, errInvalid
	}
	if len(scope) == 0 {
		scope = make([]uint64, d.Len())
		for i := range scope {
			p, _ := d.PartitionAt(i)
			scope[i] = p.Partition
		}
	} else {
		scope = slices.Clone(scope)
		slices.Sort(scope)
	}
	for i, x := range scope {
		if _, ok := d.Partition(x); !ok || i > 0 && scope[i-1] == x {
			return nil, errInvalid
		}
	}
	return scope, nil
}
func (c *certifiedReadCoordinator) host(ctx context.Context, partition uint64) (*allocationHost, error) {
	h, e := c.transport.Host(ctx, partition)
	if e != nil {
		return nil, e
	}
	if h == nil || h.machine == nil || h.machine.ns.partition != partition || !h.machine.certifiedReads() || h.machine.declaration.Digest() != c.declaration.Digest() {
		return nil, errInvalid
	}
	return h, nil
}
func (c *certifiedReadCoordinator) read(ctx context.Context, h *allocationHost, kind byte, round uint64) (graphCutProof, error) {
	nonce, e := graphReadNonce()
	if e != nil {
		return graphCutProof{}, e
	}
	q := graphCutQuery{kind: kind, round: round, nonce: nonce}
	r, e := c.transport.Read(ctx, h, q)
	if e != nil || r.err != nil {
		return graphCutProof{}, errors.Join(e, r.err)
	}
	if r.question != q || r.id != r.proof.id || r.proof.owner != h {
		return graphCutProof{}, errInvalid
	}
	if e := checkGraphCutProof(r.proof, q); e != nil {
		return graphCutProof{}, e
	}
	return r.proof, nil
}
func (c *certifiedReadCoordinator) resolvePending(ctx context.Context, o graphCutObservation) error {
	if o.pending == ([16]byte{}) {
		return nil
	}
	h, e := c.host(ctx, o.coordinator)
	if e != nil {
		return e
	}
	nonce, e := graphReadNonce()
	if e != nil {
		return e
	}
	r, e := c.transport.Decision(ctx, h, o.pending, nonce)
	if e != nil || r.err != nil {
		return errors.Join(errGraphCutPending, e, r.err)
	}
	p := r.proof
	if p.owner != h || p.question.tx != o.pending || p.question.nonce != nonce || p.question.phase != crossDecide {
		return errInvalid
	}
	if e := checkCrossProof(p, o.pending, p.status.digest); e != nil {
		return e
	}
	if p.status.phase != crossDecide {
		return errGraphCutPending
	}
	// Every production cross transaction includes the complete declared authority
	// set. Resolve all participants, including ones outside requested data scope.
	for i := range c.declaration.Len() {
		part, _ := c.declaration.PartitionAt(i)
		target, e := c.host(ctx, part.Partition)
		if e != nil {
			return e
		}
		if !p.status.commit {
			status, e := c.participant(ctx, target, p.status.tx, p.status.digest)
			if e != nil {
				return e
			}
			// Absence is a completed transaction+manifest-bound observation,
			// never inferred from missing quorum/transport/application replies.
			// A racing late Prepare is checked again by certification's owner
			// gate; after the fence it cannot produce an effect at/below B.
			if status.phase == 0 || status.phase == crossResolve {
				continue
			}
		}
		proposal, e := h.ResolveCrossParticipant(part.Partition, p)
		if e != nil {
			return e
		}
		s, e := c.transport.Resolve(ctx, target, proposal)
		if e != nil {
			return e
		}
		if s.ns != target.machine.ns || s.tx != p.status.tx || s.digest != p.status.digest || s.phase != crossResolve || s.commit != p.status.commit || s.round != p.status.round {
			return errInvalid
		}
	}
	return nil
}

// Open orchestrates the selected certified mode through actual source Hosts.
// Cache locking never spans external transport or a cancellable read.
func (c *certifiedReadCoordinator) Open(ctx context.Context, request graphReadRequest, scope []uint64) (read *certifiedGraphRead, err error) {
	if c == nil || ctx == nil || nilCertifiedTransport(c.transport) {
		return nil, errInvalid
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	scope, err = certifiedScope(c.declaration, scope)
	if err != nil {
		return nil, err
	}
	cut := request.cut
	switch request.mode {
	case graphReadAt:
	case graphReadStable:
		if len(cut.certificates) == 0 {
			c.mu.Lock()
			cut = c.stable
			c.mu.Unlock()
			if len(cut.certificates) == 0 {
				return nil, replica.ErrUnavailable
			}
		}
	case graphReadFresh, graphReadAfter:
		after := uint64(0)
		if request.mode == graphReadAfter {
			after, err = c.receiptRound(ctx, request.receipt)
			if err != nil {
				return nil, err
			}
		}
		cut, err = c.fresh(ctx, scope, after)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errInvalid
	}
	if len(cut.certificates) != len(scope) {
		return nil, errGraphCutScope
	}
	sources := make([]*raftlog.ApplicationView, len(scope))
	var full *graphstore.PartitionTransactionView
	defer func() {
		if err != nil {
			if full != nil {
				err = errors.Join(err, full.Close())
			}
			for _, s := range sources {
				if s != nil {
					err = errors.Join(err, s.Close())
				}
			}
			read = nil
		}
	}()
	var first *allocationHost
	var aggregate materializerLimits
	for i, cert := range cut.certificates {
		if cert.scope.partition != scope[i] || cert.scope.declaration != c.declaration.Digest() || graphstate.GraphID(cert.scope.graph) != c.declaration.Graph() || i > 0 && (cert.round != cut.certificates[0].round || cert.configuration != cut.certificates[0].configuration) {
			return nil, errGraphCutScope
		}
		h, e := c.host(ctx, scope[i])
		if e != nil {
			return nil, e
		}
		if i == 0 {
			first = h
			aggregate = h.machine.limits
		}
		if i > 0 {
			aggregate = intersectCertifiedReadLimits(aggregate, h.machine.limits)
		}
		sources[i], err = h.AtGraphCertificate(ctx, cert)
		if err != nil {
			return nil, err
		}
	}
	// Constructor caps span all source handles and all later routed read work.
	l := aggregate
	arena, e := graphstate.NewOutputBudget(l.outputBytes)
	if e != nil {
		return nil, e
	}
	retained := graphReadSessionBytes + 4096*len(sources)
	if e := arena.Reserve(retained); e != nil {
		return nil, outputBudgetFailure(e)
	}
	cfg, e := first.currentConfiguration()
	if e != nil {
		return nil, e
	}
	configuration, e := cfg.digest()
	if e != nil || configuration != cut.certificates[0].configuration {
		return nil, errors.Join(errInvalid, e)
	}
	work := graphstore.PageWork{Records: 8 * len(sources), Bytes: retained}
	budget := graphstore.OwnershipBudget{SourceRows: l.sourceRows - work.Records, SourceBytes: l.sourceBytes - work.Bytes, OutputBytes: min(l.outputBytes-retained, arena.Remaining())}
	var opened graphstore.PageWork
	full, opened, err = graphstore.OpenPartitionScopedReadView(ctx, sources, scope, c.declaration, cfg.defaultAxis, configuration, l.catalog, l.graph, budget, arena)
	if err != nil {
		return nil, err
	}
	work.Records += opened.Records
	work.Bytes += opened.Bytes
	read = &certifiedGraphRead{cut: graphCut{certificates: slices.Clone(cut.certificates)}, full: full, sources: sources, initial: graphstore.PageWork{Records: 8 * len(sources), Bytes: retained}}
	if request.mode == graphReadFresh || request.mode == graphReadAfter {
		c.mu.Lock()
		c.stable = graphCut{certificates: slices.Clone(cut.certificates)}
		c.mu.Unlock()
	}
	return read, nil
}
func (c *certifiedReadCoordinator) fresh(ctx context.Context, scope []uint64, after uint64) (graphCut, error) {
	b := max(uint64(1), after)
	hosts := make([]*allocationHost, len(scope))
	for i, id := range scope {
		h, e := c.host(ctx, id)
		if e != nil {
			return graphCut{}, e
		}
		hosts[i] = h
		p, e := c.read(ctx, h, 1, 0)
		if e != nil {
			return graphCut{}, e
		}
		b = max(b, p.observation.floor)
		if p.observation.pending != ([16]byte{}) {
			if e := c.resolvePending(ctx, p.observation); e != nil {
				return graphCut{}, e
			}
			p, e = c.read(ctx, h, 1, 0)
			if e != nil {
				return graphCut{}, e
			}
			b = max(b, p.observation.floor)
		}
	}
	if b == math.MaxUint64 {
		return graphCut{}, errLimit
	}
	for _, h := range hosts {
		p, e := h.PrepareGraphFence(b)
		if e != nil {
			return graphCut{}, e
		}
		r, e := c.transport.Submit(ctx, h, p)
		if e != nil {
			return graphCut{}, e
		}
		if r.phase != graphCutFence || r.round != b || r.index == 0 {
			return graphCut{}, errInvalid
		}
		if e := graphResultError(r.status); e != nil {
			return graphCut{}, e
		}
	}
	cut := graphCut{certificates: make([]graphCertificate, len(scope))}
	for i, h := range hosts {
		p, e := c.read(ctx, h, 1, 0)
		if e != nil {
			return graphCut{}, e
		}
		if e := c.resolvePending(ctx, p.observation); e != nil {
			return graphCut{}, e
		}
		command, e := h.PrepareGraphCertificate(b)
		if e != nil {
			return graphCut{}, e
		}
		r, e := c.transport.Submit(ctx, h, command)
		if e != nil {
			return graphCut{}, e
		}
		if r.phase != graphCutCertify || r.round != b || r.index == 0 {
			return graphCut{}, errInvalid
		}
		if e := graphResultError(r.status); e != nil {
			return graphCut{}, e
		}
		p, e = c.read(ctx, h, 2, b)
		if e != nil {
			return graphCut{}, e
		}
		cut.certificates[i] = p.observation.certificate
	}
	return cut, nil
}

// Cut returns owned exact coverage; it does not obtain a fresh barrier.
func (s *certifiedGraphRead) Cut() (graphCut, error) {
	if s == nil {
		return graphCut{}, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return graphCut{}, graphstore.ErrClosed
	}
	return graphCut{certificates: slices.Clone(s.cut.certificates)}, nil
}

// ReadView borrows the immutable routed view until this session closes.
func (s *certifiedGraphRead) ReadView() (graphstate.ReadView, error) {
	if s == nil {
		return nil, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, graphstore.ErrClosed
	}
	return s.full, nil
}

// Work preserves cumulative source accounting, including failed reads.
func (s *certifiedGraphRead) Work() graphstore.PageWork {
	if s == nil {
		return graphstore.PageWork{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.initial
	if s.full != nil {
		x := s.full.Work()
		w.Records += x.Records
		w.Bytes += x.Bytes
		w.DirectoryPages += x.DirectoryPages
		w.CheckpointPages += x.CheckpointPages
		w.PatchPages += x.PatchPages
		w.DecodedCells += x.DecodedCells
	}
	return w
}

// Close releases all owned sources once and caches the cleanup result.
func (s *certifiedGraphRead) Close() error {
	if s == nil {
		return errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	if s.full != nil {
		x := s.full.Work()
		s.initial.Records += x.Records
		s.initial.Bytes += x.Bytes
		s.initial.DirectoryPages += x.DirectoryPages
		s.initial.CheckpointPages += x.CheckpointPages
		s.initial.PatchPages += x.PatchPages
		s.initial.DecodedCells += x.DecodedCells
		s.closeErr = s.full.Close()
		s.full = nil
	}
	for _, v := range s.sources {
		s.closeErr = errors.Join(s.closeErr, v.Close())
	}
	s.sources = nil
	return s.closeErr
}

// Interface nil detection is once at this provider boundary, never per graph row.
func nilCertifiedTransport(t certifiedGraphTransport) bool {
	if t == nil {
		return true
	}
	v := reflect.ValueOf(t)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Func, reflect.Slice, reflect.Chan:
		return v.IsNil()
	default:
		return false
	}
}

func (c *certifiedReadCoordinator) participant(ctx context.Context, h *allocationHost, tx [16]byte, digest [32]byte) (graphParticipantObservation, error) {
	nonce, e := graphReadNonce()
	if e != nil {
		return graphParticipantObservation{}, e
	}
	q := graphCutQuery{kind: 4, request: tx, digest: digest, nonce: nonce}
	r, e := c.transport.Read(ctx, h, q)
	if e != nil || r.err != nil {
		return graphParticipantObservation{}, errors.Join(e, r.err)
	}
	if r.question != q || r.id != r.proof.id || r.proof.owner != h {
		return graphParticipantObservation{}, errInvalid
	}
	if e := checkGraphCutProof(r.proof, q); e != nil {
		return graphParticipantObservation{}, e
	}
	status := r.proof.observation.participant
	if status.transaction != tx || status.digest != digest {
		return graphParticipantObservation{}, errInvalid
	}
	return status, nil
}

// Receipt authority remains bound to the exact current declaration until an
// explicit transfer-lineage protocol exists. Validation is pure with respect
// to the old issuer; its store/driver may already be closed after reopen.
func (c *certifiedReadCoordinator) receiptRound(ctx context.Context, r graphCommitReceipt) (uint64, error) {
	var scope allocationScope
	var configuration [32]byte
	var round uint64
	if r.local.owner != nil {
		p := r.local
		if _, e := graphReceiptFromLocal(p); e != nil {
			return 0, e
		}
		scope, configuration, round = p.observation.local.scope, p.observation.configuration, p.observation.local.round
	} else {
		p := r.proof
		if _, e := graphReceiptFromDecision(p); e != nil {
			return 0, e
		}
		scope = p.owner.machine.scope()
		round = p.status.round
		if p.owner.machine.declaration.Digest() != c.declaration.Digest() {
			return 0, errInvalid
		}
	}
	if graphstate.GraphID(scope.graph) != c.declaration.Graph() || scope.declaration != c.declaration.Digest() || scope.topology != c.declaration.TopologyEpoch() {
		return 0, errInvalid
	}
	h, e := c.host(ctx, scope.partition)
	if e != nil {
		return 0, e
	}
	if scope != h.machine.scope() {
		return 0, errInvalid
	}
	cfg, e := h.currentConfiguration()
	if e != nil {
		return 0, e
	}
	expected, e := cfg.digest()
	if e != nil {
		return 0, e
	}
	if r.local.owner != nil {
		if configuration != expected {
			return 0, errInvalid
		}
	} else {
		l := h.machine.limits
		arena, e := graphstate.NewOutputBudget(l.outputBytes)
		if e != nil {
			return 0, e
		}
		manifest, e := decodeCrossManifest(r.proof.manifest, l, arena)
		if e != nil {
			return 0, e
		}
		if manifest.configuration != expected || manifest.declaration != c.declaration.Digest() || manifest.request.ns != r.proof.status.ns || manifest.coordinator != scope.partition {
			return 0, errInvalid
		}
	}
	return round, nil
}
