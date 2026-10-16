package graphapply

import (
	"context"
	"errors"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// Conditional metadata, never a cut, lease or proof of client query completeness.
// Group zero has only the continued local singleton scope; clone/reset/network
// forwarding is not supported. Nonzero Group names the configured incarnation.
type graphReadBase struct {
	group [16]byte
	guard graphstore.SemanticGuard
}

func (b graphReadBase) valid(n namespace) bool {
	g := b.guard
	return g.Namespace.Graph == graphstate.GraphID(n.graph) && g.Namespace.Partition == n.partition && g.OwnershipEpoch > 0 && g.TopologyEpoch > 0 && g.SchemaVersion > 0 && g.SemanticEpoch > 0 && g.EffectDigest != ([32]byte{})
}

// Fixed conservative representation charges, not heap/RSS measurements.
// Application mode permits only singleton/fixed-three membership; scope metadata
// covers copied HardState/ConfState and its at most three voter IDs.
const graphReadScopeBytes = 512
const graphReadSessionBytes = 2048

func chargeGraphWork(q *reader, work graphstore.PageWork) error {
	if work.Records < 0 || work.Bytes < 0 || work.Records > q.limits.readRows-q.rows || work.Bytes > q.limits.readBytes-q.bytes {
		return errLimit
	}
	q.rows += work.Records
	q.bytes += work.Bytes
	return nil
}
func graphReadScope(ctx context.Context, s *raftlog.Store, n namespace, q *reader) ([16]byte, error) {
	if ctx == nil || s == nil || !n.valid() || q == nil {
		return [16]byte{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return [16]byte{}, err
	}
	if !s.ApplicationLimits().Enabled() {
		return [16]byte{}, errInvalid
	}
	if err := chargeGraphWork(q, graphstore.PageWork{Bytes: graphReadScopeBytes}); err != nil {
		return [16]byte{}, err
	}
	binding := s.ApplicationBinding()
	if binding != (raftlog.ApplicationBinding{}) {
		if err := binding.Validate(); err != nil {
			return [16]byte{}, errors.Join(errInvalid, err)
		}
		if binding.SemanticContractID != SemanticContractID() {
			return [16]byte{}, errInvalid
		}
		if binding.Identity.Graph != [16]byte(n.graph) || binding.Identity.Partition != n.partition {
			return [16]byte{}, graphstore.ErrNamespace
		}
		return binding.Identity.Group, nil
	}
	identity := s.ApplicationIdentity()
	if identity != (raftlog.ApplicationIdentity{}) && (identity.Graph != [16]byte(n.graph) || identity.Partition != n.partition) {
		return [16]byte{}, graphstore.ErrNamespace
	}
	_, membership, err := s.InitialState()
	if err != nil {
		return [16]byte{}, err
	}
	p := s.ApplicationLimits()
	if membership == nil || len(membership.ProtoReflect().GetUnknown()) != 0 || len(membership.GetVoters()) != 1 || membership.GetVoters()[0] != p.LocalVoter || len(membership.GetVotersOutgoing()) != 0 || len(membership.GetLearners()) != 0 || len(membership.GetLearnersNext()) != 0 || membership.GetAutoLeave() {
		return [16]byte{}, errInvalid
	}
	return [16]byte{}, nil
}

// Owns its backend and Full handles. Read-only MVCC is not a Fresh/At certified
// cut. The source ledger includes constructor/co-init work and all later reads.
type graphReadSession struct {
	mu       sync.Mutex
	backend  *raftlog.ApplicationView
	full     *graphstore.ReadView
	base     graphReadBase
	ns       namespace
	limits   materializerLimits
	initial  graphstore.PageWork
	closed   bool
	closeErr error
}

func closeGraphReadHandles(full *graphstore.ReadView, backend *raftlog.ApplicationView) error {
	var a, b error
	if full != nil {
		a = full.Close()
	}
	if backend != nil {
		b = backend.Close()
	}
	return errors.Join(a, b)
}
func openGraphRead(ctx context.Context, m *materializer, index uint64) (session *graphReadSession, err error) {
	if ctx == nil || m == nil || m.store == nil || !m.ns.valid() || index == 0 {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := m.limits.validate(); err != nil {
		return nil, err
	}
	backend, err := m.store.ApplicationView(index)
	if err != nil {
		return nil, err
	}
	var full *graphstore.ReadView
	defer func() {
		if err != nil {
			err = errors.Join(err, closeGraphReadHandles(full, backend))
			session = nil
		}
	}()
	base, err := backend.Root()
	if err != nil {
		return nil, err
	}
	c, root, err := m.catalog(backend)
	if err != nil {
		return nil, err
	}
	if root.SemanticEpoch() == 0 {
		return nil, errNotInitialized
	}
	l := m.limits.allocation
	l.readRows = min(l.readRows, m.limits.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: ctx, view: backend, ns: m.ns, base: base, limits: l}
	if err := chargeGraphWork(&q, graphstore.PageWork{Bytes: graphReadSessionBytes + 4*cap(base.Image)}); err != nil {
		return nil, err
	}
	group, err := graphReadScope(ctx, m.store, m.ns, &q)
	if err != nil {
		return nil, err
	}
	if _, found, err := q.allocator(); err != nil {
		return nil, err
	} else if !found {
		return nil, errCorrupt
	}
	guard, err := graphstore.NewSemanticGuard(root)
	if err != nil {
		return nil, err
	}
	gl, err := m.graphBudget(&q)
	if err != nil {
		return nil, err
	}
	// Bound transient decode plus session/base ownership conservatively by outer
	// output headroom too. It is a representation ledger, not process memory.
	retained := graphReadSessionBytes + 4*cap(base.Image)
	if retained >= m.limits.outputBytes {
		return nil, errLimit
	}
	gl.MaxSourceBytes = min(gl.MaxSourceBytes, m.limits.outputBytes-retained)
	gl.MaxOutputBytes = min(gl.MaxOutputBytes, m.limits.outputBytes-retained)
	full, err = graphstore.OpenReadView(ctx, c, gl)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &graphReadSession{backend: backend, full: full, base: graphReadBase{group, guard}, ns: m.ns, limits: m.limits, initial: graphstore.PageWork{Records: q.rows, Bytes: q.bytes}}, nil
}
func (s *graphReadSession) ReadView() (graphstate.ReadView, error) {
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
func (s *graphReadSession) Work() graphstore.PageWork {
	if s == nil {
		return graphstore.PageWork{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	work := s.initial
	if s.full != nil {
		v := s.full.Work()
		work.Records += v.Records
		work.Bytes += v.Bytes
		work.DirectoryPages += v.DirectoryPages
		work.CheckpointPages += v.CheckpointPages
		work.PatchPages += v.PatchPages
		work.DecodedCells += v.DecodedCells
	}
	return work
}

// Borrow input only for this bounded encoding call; returned canonical bytes own
// their backing. This door always emits kind7 and cannot omit its read guard.
func (s *graphReadSession) Request(id requestID, ops []graphstate.Operation, revision state.Revision, claims []freshBinding) ([]byte, error) {
	if s == nil {
		return nil, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, graphstore.ErrClosed
	}
	return encodeGraphRequest(graphRequest{ns: s.ns, kind: guardedGraphOperations, id: id, readBase: s.base, operations: ops, revision: revision, claims: claims}, s.limits)
}

// Cache the original cleanup result; repeats neither clean up again nor grow an
// errors.Join chain. Constructor failures also close both owned handles.
func (s *graphReadSession) Close() error {
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
		v := s.full.Work()
		s.initial.Records += v.Records
		s.initial.Bytes += v.Bytes
		s.initial.DirectoryPages += v.DirectoryPages
		s.initial.CheckpointPages += v.CheckpointPages
		s.initial.PatchPages += v.PatchPages
		s.initial.DecodedCells += v.DecodedCells
	}
	s.closeErr = closeGraphReadHandles(s.full, s.backend)
	s.full, s.backend = nil, nil
	return s.closeErr
}
