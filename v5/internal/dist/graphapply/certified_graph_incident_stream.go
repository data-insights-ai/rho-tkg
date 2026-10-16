package graphapply

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Whole-query allowances are independent, consumptive, and explicitly opt-in.
// Every invocation prepays its entire finite ordinary allowance. There are no
// refunds, including ambiguous RPC/close replies or unsuccessful hydration.
type certifiedIncidentBudget struct {
	sourceRows, sourceBytes, decodedCells, rpcCalls, wireBytes, outputBytes, invocations int
}

func (b certifiedIncidentBudget) values() [7]int {
	return [7]int{b.sourceRows, b.sourceBytes, b.decodedCells, b.rpcCalls, b.wireBytes, b.outputBytes, b.invocations}
}
func (b certifiedIncidentBudget) admit(g certifiedIncidentBudget) bool {
	have, want := b.values(), g.values()
	for i := range have {
		if want[i] < 1 || want[i] > have[i] {
			return false
		}
	}
	return true
}
func (b certifiedIncidentBudget) add(g certifiedIncidentBudget) certifiedIncidentBudget {
	return certifiedIncidentBudget{b.sourceRows + g.sourceRows, b.sourceBytes + g.sourceBytes, b.decodedCells + g.decodedCells, b.rpcCalls + g.rpcCalls, b.wireBytes + g.wireBytes, b.outputBytes + g.outputBytes, b.invocations + g.invocations}
}
func (b certifiedIncidentBudget) subtract(g certifiedIncidentBudget) certifiedIncidentBudget {
	return certifiedIncidentBudget{b.sourceRows - g.sourceRows, b.sourceBytes - g.sourceBytes, b.decodedCells - g.decodedCells, b.rpcCalls - g.rpcCalls, b.wireBytes - g.wireBytes, b.outputBytes - g.outputBytes, b.invocations - g.invocations}
}
func incidentProduct(a, b int) (int, error) {
	if a < 1 || b < 1 || a > math.MaxInt/b {
		return 0, errLimit
	}
	return a * b, nil
}
func incidentGrant(l materializerLimits, rows int) (certifiedIncidentBudget, error) {
	if err := l.validate(); err != nil {
		return certifiedIncidentBudget{}, err
	}
	d := graphstore.DefaultGraphLimits()
	// A charged checkpoint decodes at most MaxCells; one charged patch can
	// replay at most MaxTailAtoms. Both paths spend the source-record ledger.
	decoded, err := incidentProduct(l.sourceRows, max(cmp.Or(l.graph.Pages.MaxCells, d.Pages.MaxCells), cmp.Or(l.graph.Pages.MaxTailAtoms, d.Pages.MaxTailAtoms)))
	if err != nil {
		return certifiedIncidentBudget{}, err
	}
	projection, err := incidentProduct(rows, cmp.Or(l.graph.Planner.MaxDeltaBytes, d.Planner.MaxDeltaBytes)+512)
	if err != nil || projection > math.MaxInt-l.outputBytes {
		return certifiedIncidentBudget{}, errLimit
	}
	return certifiedIncidentBudget{l.sourceRows, l.sourceBytes, decoded, 1024, l.sourceBytes, l.outputBytes + projection, 1}, nil
}

// A chunk is owned by its query and borrowed read-only until the next successful
// Next or Close. Only this last genuine sequence can be replayed. No arbitrary
// sequence, after-ID, decoded capsule, or EOF construction is exposed.
type certifiedIncidentChunk struct {
	request  [16]byte
	sequence uint64
	complete bool
	rows     []graphstate.Projection
	work     graphstore.PageWork
}
type incidentCertifiedOpener func(context.Context, graphReadRequest, []uint64) (*certifiedGraphRead, error)
type certifiedIncidentStream struct {
	mu                        sync.Mutex
	closeMu                   sync.Mutex
	operation                 graphRequestGate
	ctx                       context.Context
	cancel                    context.CancelFunc
	open                      incidentCertifiedOpener
	hydrateChunk              func(context.Context, graphstate.ReadView, *certifiedIncidentChunk) error
	pin                       *certifiedGraphRead // process-local retained-cut ownership; not a durable pin
	cut                       graphCut
	scope                     []uint64
	request                   [16]byte
	predicate                 graphstate.IncidentPredicate
	at                        temporal.Position
	visible                   graphstate.Visibility
	rows                      int
	limits                    materializerLimits
	grant, remaining, charged certifiedIncidentBudget
	capsule                   graphstore.PartitionIncidentContinuation
	started                   bool
	sequence                  uint64
	last                      *certifiedIncidentChunk
	closed                    bool
	closeErr                  error
}

// OpenIncidentStream retains the original certified At cut in this process.
// Each chunk obtains fresh authenticated source sessions at that SAME cut via
// existing source Get/Scan APIs. Existing per-invocation limits are unchanged.
// Durable/portable pins and physical restart require a separate genuine query;
// loss of this owner/capsule cannot be repaired with a caller's last ID.
func (c *crossProcessCoordinator) OpenIncidentStream(ctx context.Context, cut graphCut, scope []uint64, p graphstate.IncidentPredicate, at temporal.Position, visible graphstate.Visibility, rows int, deadline time.Time, budget certifiedIncidentBudget) (*certifiedIncidentStream, error) {
	if c == nil || c.host == nil || c.host.machine == nil {
		return nil, errInvalid
	}
	return openCertifiedIncidentStream(ctx, c.OpenCertified, c.host.machine.limits, cut, scope, p, at, visible, rows, deadline, budget)
}
func openCertifiedIncidentStream(ctx context.Context, open incidentCertifiedOpener, l materializerLimits, cut graphCut, scope []uint64, p graphstate.IncidentPredicate, at temporal.Position, visible graphstate.Visibility, rows int, deadline time.Time, budget certifiedIncidentBudget) (*certifiedIncidentStream, error) {
	if ctx == nil || open == nil || p.Endpoint == 0 || rows < 1 || rows > cmp.Or(l.graph.Pages.MaxWorkRecords, graphstore.DefaultGraphLimits().Pages.MaxWorkRecords) || len(cut.certificates) > 16 || len(scope) > 16 || visible != graphstate.Declared && visible != graphstate.Effective || deadline.IsZero() || len(cut.certificates) == 0 {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	grant, err := incidentGrant(l, rows)
	if err != nil {
		return nil, err
	}
	if !budget.admit(grant) {
		return nil, errLimit
	}
	// This owner cannot outlive the ordinary held remote source's one-minute TTL.
	if !deadline.After(time.Now()) {
		return nil, context.DeadlineExceeded
	}
	deadline = minIncidentDeadline(deadline, time.Now().Add(time.Minute))
	point, err := temporal.Point(at)
	if err != nil {
		return nil, err
	}
	a, err := temporal.AppendScope(nil, point, l.catalog.Temporal)
	if err != nil {
		return nil, err
	}
	b, err := temporal.AppendScope(nil, p.Window, l.catalog.Temporal)
	if err != nil || !bytes.Equal(a, b) {
		return nil, errors.Join(errInvalid, err)
	}
	owner, cancel := context.WithDeadline(ctx, deadline)
	s := &certifiedIncidentStream{operation: newGraphRequestGate(), ctx: owner, cancel: cancel, open: open, cut: graphCut{certificates: slices.Clone(cut.certificates)}, scope: slices.Clone(scope), predicate: p, at: at, visible: visible, rows: rows, limits: l, grant: grant, remaining: budget}
	if s.request, err = graphReadNonce(); err != nil {
		cancel()
		return nil, err
	}
	// Pin capture is itself a fully charged ordinary invocation.
	s.remaining = s.remaining.subtract(grant)
	s.charged = s.charged.add(grant)
	s.pin, err = open(owner, graphReadRequest{mode: graphReadAt, cut: s.cut}, s.scope)
	if err != nil {
		cancel()
		return nil, err
	}
	actual, err := s.pin.Cut()
	if err == nil && !slices.Equal(actual.certificates, s.cut.certificates) {
		err = errInvalid
	}
	if err != nil {
		cancel()
		return nil, errors.Join(err, s.pin.Close())
	}
	for _, source := range s.pin.sources {
		if remote, ok := source.(*remoteGraphSource); ok {
			deadline = minIncidentDeadline(deadline, remote.deadline)
		}
	}
	short, shortCancel := context.WithDeadline(owner, deadline)
	s.ctx = short
	s.cancel = func() { shortCancel(); cancel() }
	context.AfterFunc(short, func() { _ = s.Close() })
	return s, nil
}
func minIncidentDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Next is once-only except replay of the last committed request/sequence.
// accept may reject a hydrated chunk before commit. Failure consumes the grant
// but preserves the previous capsule/sequence and never publishes partial EOF.
func (s *certifiedIncidentStream) Next(ctx context.Context, request [16]byte, sequence uint64, accept func(*certifiedIncidentChunk) error) (*certifiedIncidentChunk, error) {
	if s == nil || ctx == nil || s.ctx == nil {
		return nil, errInvalid
	}
	// Refuse overlapping/reentrant attempts. The owner gate never waits behind
	// a callback that might invoke Next again or cancel the owner with Close.
	if !s.operation.tryLock() {
		return nil, errLimit
	}
	defer s.operation.unlock()
	s.mu.Lock()
	if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.closed {
		s.mu.Unlock()
		return nil, graphstore.ErrClosed
	}
	if request != s.request || sequence == 0 {
		s.mu.Unlock()
		return nil, errInvalid
	}
	if s.last != nil && sequence == s.sequence {
		last := s.last
		s.mu.Unlock()
		if accept != nil {
			if err := accept(last); err != nil {
				return nil, err
			}
		}
		if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
			return nil, err
		}
		return last, nil
	}
	if s.sequence == math.MaxUint64 || sequence != s.sequence+1 || s.last != nil && s.last.complete {
		s.mu.Unlock()
		return nil, errInvalid
	}
	if !s.remaining.admit(s.grant) {
		s.mu.Unlock()
		return nil, errLimit
	}
	s.remaining = s.remaining.subtract(s.grant)
	s.charged = s.charged.add(s.grant)
	started, capsule := s.started, s.capsule
	s.mu.Unlock()
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	read, err := s.open(call, graphReadRequest{mode: graphReadAt, cut: s.cut}, s.scope)
	if err != nil {
		return nil, err
	}
	chunk, next, err := s.hydrate(call, read, sequence, started, capsule)
	err = errors.Join(err, read.Close())
	if err == nil {
		err = errors.Join(call.Err(), s.ctx.Err())
	}
	if err == nil && accept != nil {
		err = accept(chunk)
	}
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := errors.Join(call.Err(), s.ctx.Err()); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, graphstore.ErrClosed
	}
	s.capsule, s.started, s.sequence, s.last = next, true, sequence, chunk
	return chunk, nil
}
func (s *certifiedIncidentStream) hydrate(ctx context.Context, read *certifiedGraphRead, sequence uint64, started bool, capsule graphstore.PartitionIncidentContinuation) (*certifiedIncidentChunk, graphstore.PartitionIncidentContinuation, error) {
	var next graphstore.PartitionIncidentContinuation
	actual, err := read.Cut()
	if err != nil || !slices.Equal(actual.certificates, s.cut.certificates) || read.full == nil {
		return nil, next, errors.Join(errInvalid, err)
	}
	token := graphstate.Cursor(0)
	if started {
		token, err = read.full.ImportIncidentContinuation(ctx, s.predicate, capsule)
	}
	if err != nil {
		return nil, next, err
	}
	pages := s.limits.graph.Pages
	defaults := graphstore.DefaultGraphLimits()
	page, err := read.full.IncidentRelationshipsChunk(ctx, s.predicate, token, raftlog.ReadBudget{Rows: cmp.Or(pages.MaxWorkRecords, defaults.Pages.MaxWorkRecords), Bytes: cmp.Or(pages.MaxWorkBytes, defaults.Pages.MaxWorkBytes)}, s.rows)
	if err != nil {
		return nil, next, err
	}
	if page.View != read.full.Identity() || page.Complete != (page.Next == 0) || !page.Complete && len(page.Entities) == 0 || len(page.Entities) > s.rows {
		return nil, next, graphstate.ErrIncompleteRead
	}
	chunk := &certifiedIncidentChunk{request: s.request, sequence: sequence, complete: page.Complete, rows: make([]graphstate.Projection, 0, len(page.Entities))}
	for _, id := range page.Entities {
		projection, err := graphstate.Project(ctx, read.full, id, s.at, s.visible, s.limits.graph.Planner)
		if err != nil {
			return nil, next, err
		}
		r := projection.Record
		if !projection.Exists || r.Kind != graphstate.Relationship || r.Source != s.predicate.Endpoint && r.Target != s.predicate.Endpoint {
			return nil, next, graphstate.ErrContradictoryRead
		}
		if !projection.Active {
			continue
		}
		if s.predicate.Life != 0 && r.Mode == graphstate.LifeBound {
			matched := false
			for _, endpoint := range projection.Endpoints {
				if endpoint.ID == s.predicate.Endpoint && endpoint.BoundLife == s.predicate.Life {
					matched = true
				}
			}
			if !matched {
				continue
			}
		}
		chunk.rows = append(chunk.rows, projection)
	}
	// A fixed trusted owner hook may hydrate additional typed component cells
	// on this same live read and ordinary ledgers. It cannot import authority.
	if s.hydrateChunk != nil {
		if err := s.hydrateChunk(ctx, read.full, chunk); err != nil {
			return nil, next, err
		}
	}
	if !page.Complete {
		next, err = read.full.ExportIncidentContinuation(ctx, s.predicate, page.Next)
	}
	if err != nil {
		return nil, next, err
	}
	w := read.Work()
	chunk.work = w
	if w.Records > s.grant.sourceRows || w.Bytes > s.grant.sourceBytes || w.DecodedCells > s.grant.decodedCells {
		return nil, next, errLimit
	}
	return chunk, next, nil
}
func (s *certifiedIncidentStream) Charged() certifiedIncidentBudget {
	if s == nil {
		return certifiedIncidentBudget{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.charged
}
func (s *certifiedIncidentStream) Close() error {
	if s == nil || s.cancel == nil {
		return errInvalid
	}
	s.cancel() // active attempts keep their own bounded source cleanup ownership
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	pin := s.pin
	s.pin = nil
	s.capsule = graphstore.PartitionIncidentContinuation{}
	s.last = nil
	s.mu.Unlock()
	var err error
	if pin != nil {
		err = pin.Close()
	}
	s.mu.Lock()
	s.closeErr = err
	s.mu.Unlock()
	return err
}
