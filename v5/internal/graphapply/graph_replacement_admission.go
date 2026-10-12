package graphapply

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// RetireAndAcquire is the trusted crash-model embedding boundary. It retires
// the old physical incarnation before returning a unique live authenticated
// association. Revoked credentials/connections must not deliver its delayed
// votes. Exclusivity lasts until Close or Context cancellation; releasing it
// cannot revive the old incarnation. The input context bounds acquisition; the
// returned Context owns the incarnation lifetime independently of that request.
// Local storage cannot distinguish copies of an entire credential set.
type graphReplacementAssociator interface {
	RetireAndAcquire(context.Context, graphPeerIdentity) (graphReplacementAssociation, error)
}
type graphReplacementAssociation interface {
	Peer() graphPeerIdentity
	Incarnation() [16]byte
	Context() context.Context
	Close() error
}
type graphReplacementTarget struct {
	voter       uint64
	incarnation [16]byte
}

// Inert wire data: a quorum read after retirement, not activation permission,
// current leadership, retained application data or a certified graph cut.
type graphReplacementReadBody struct {
	scope         allocationScope
	source        uint64
	id            allocationReadID
	nonce         [16]byte
	target        graphReplacementTarget
	voters        [3]uint64
	index, term   uint64
	configuration [32]byte
}

func (p graphReplacementReadBody) validate() error {
	if _, err := encodeAllocationScope(p.scope); err != nil {
		return err
	}
	if !partitionGraphAgreement(p.scope.semantic) || p.id.session == ([16]byte{}) || p.id.sequence == 0 || p.nonce == ([16]byte{}) || p.target.incarnation == ([16]byte{}) || p.index == 0 || p.term == 0 || p.configuration == ([32]byte{}) {
		return errInvalid
	}
	if p.voters[0] == 0 || p.voters[1] <= p.voters[0] || p.voters[2] <= p.voters[1] || p.source == p.target.voter || !slices.Contains(p.voters[:], p.source) || !slices.Contains(p.voters[:], p.target.voter) {
		return errInvalid
	}
	return nil
}
func encodeGraphReplacementTarget(p graphReplacementTarget) ([]byte, error) {
	if p.voter == 0 || p.incarnation == ([16]byte{}) {
		return nil, errInvalid
	}
	return boundedEncoding(60, func(w *boundedWriter) { w.add([]byte{'G', 'R', 'T', 1}); w.u64(p.voter); w.add(p.incarnation[:]) })
}
func decodeGraphReplacementTarget(wire []byte) (graphReplacementTarget, error) {
	body, err := graphBody(wire, "GRT\x01", 60)
	if err != nil {
		return graphReplacementTarget{}, err
	}
	c := graphCursor{b: body}
	p := graphReplacementTarget{voter: c.u64(), incarnation: c.array()}
	if c.err != nil || len(c.b) != 0 || p.voter == 0 || p.incarnation == ([16]byte{}) {
		return graphReplacementTarget{}, errors.Join(errCorrupt, c.err)
	}
	return p, nil
}
func encodeGraphReplacementRead(p graphReplacementReadBody) ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	scope, _ := encodeAllocationScope(p.scope)
	return boundedEncoding(512, func(w *boundedWriter) {
		w.add([]byte{'G', 'R', 'B', 1})
		w.field(scope)
		w.u64(p.source)
		w.add(p.id.session[:])
		w.u64(p.id.sequence)
		w.add(p.nonce[:])
		w.u64(p.target.voter)
		w.add(p.target.incarnation[:])
		for _, voter := range p.voters {
			w.u64(voter)
		}
		w.u64(p.index)
		w.u64(p.term)
		w.add(p.configuration[:])
	})
}

// The decoder never constructs admission or restores an association lifetime.
func decodeGraphReplacementRead(wire []byte) (graphReplacementReadBody, error) {
	body, err := graphBody(wire, "GRB\x01", 512)
	if err != nil {
		return graphReplacementReadBody{}, err
	}
	c := graphCursor{b: body}
	scope := c.field(allocationScopeBytes)
	p := graphReplacementReadBody{source: c.u64(), id: allocationReadID{session: c.array(), sequence: c.u64()}, nonce: c.array(), target: graphReplacementTarget{voter: c.u64(), incarnation: c.array()}}
	for i := range p.voters {
		p.voters[i] = c.u64()
	}
	p.index, p.term = c.u64(), c.u64()
	copy(p.configuration[:], c.take(32))
	if c.err != nil || len(c.b) != 0 {
		return graphReplacementReadBody{}, errors.Join(errCorrupt, c.err)
	}
	p.scope, err = decodeAllocationScope(scope)
	if err != nil {
		return graphReplacementReadBody{}, err
	}
	checked, err := encodeGraphReplacementRead(p)
	if err != nil || !bytes.Equal(checked, wire) {
		return graphReplacementReadBody{}, errors.Join(errCorrupt, err)
	}
	return p, nil
}
func (h *allocationHost) exportReplacementRead(p allocationProof, target graphReplacementTarget) (graphReplacementReadBody, error) {
	if h == nil {
		return graphReplacementReadBody{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return graphReplacementReadBody{}, replica.ErrStopped
	}
	if err := h.ownProof(p, observeConfiguration); err != nil {
		return graphReplacementReadBody{}, err
	}
	index := p.observation.source.index
	if index == 0 || index > h.driver.Applied() {
		return graphReplacementReadBody{}, errInvalid
	}
	_, conf, err := h.machine.store.InitialState()
	if err != nil {
		return graphReplacementReadBody{}, err
	}
	if conf == nil || len(conf.ProtoReflect().GetUnknown()) != 0 || len(conf.GetVoters()) != 3 || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearners()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return graphReplacementReadBody{}, errInvalid
	}
	// Driver.ReadIndex requires a leader with a current-term committed entry;
	// its correlated completion is applied before this native proof is built.
	// Use that applied entry's real retained term T, never HardState.Term as a
	// leadership assertion. Compaction refuses rather than guessing.
	term, err := h.machine.store.Term(index)
	if err != nil {
		return graphReplacementReadBody{}, errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	body := graphReplacementReadBody{scope: h.machine.scope(), source: h.machine.store.ApplicationLimits().LocalVoter, id: p.id, nonce: p.question.nonce, target: target, index: index, term: term, configuration: p.observation.configuration}
	copy(body.voters[:], conf.GetVoters())
	if err := body.validate(); err != nil {
		return graphReplacementReadBody{}, err
	}
	return body, nil
}

// Purpose-specific constructors support all lifecycle agreements. Existing
// cross-graph source kinds retain their original constructor/admission checks.
func newGraphReplacementService(h *allocationHost, pump graphHostReadPump) (*graphPeerService, error) {
	if h == nil || nilGraphReplacementInterface(pump) || !partitionGraphAgreement(h.machine.SemanticContractID()) {
		return nil, errInvalid
	}
	return &graphPeerService{mu: newGraphRequestGate(), host: h, pump: pump, peer: graphPeerIdentity{scope: h.machine.scope(), voter: h.machine.store.ApplicationLimits().LocalVoter}, replacementOnly: true}, nil
}
func newGraphReplacementEndpoint(d graphstore.OwnershipDeclaration, transport graphPeerWireTransport, budget *graphRemoteBudget) (*graphAuthenticatedEndpoint, error) {
	if nilGraphReplacementInterface(transport) || budget == nil {
		return nil, errInvalid
	}
	peer := transport.Peer()
	part, found := d.Partition(peer.scope.partition)
	if !found || peer.voter == 0 || peer.scope.graph != [16]byte(d.Graph()) || peer.scope.declaration != d.Digest() || peer.scope.topology != d.TopologyEpoch() || peer.scope.group != part.Group || peer.scope.ownership != part.OwnershipEpoch || !partitionGraphAgreement(peer.scope.semantic) {
		return nil, errInvalid
	}
	return &graphAuthenticatedEndpoint{stream: &graphAuthenticatedStream{mu: newGraphRequestGate()}, transport: transport, peer: peer, budget: budget}, nil
}
func (s *graphPeerService) readReplacement(ctx context.Context, r graphSourceRequest) ([]byte, error) {
	target, err := decodeGraphReplacementTarget(r.command)
	if err != nil {
		return nil, err
	}
	q := allocationQuery{kind: observeConfiguration, nonce: r.nonce}
	reply, err := s.pump.Allocation(ctx, q)
	if err != nil {
		return nil, err
	}
	if reply.err != nil {
		return nil, reply.err
	}
	if reply.question != q || reply.proof.question != q || reply.id != reply.proof.id || reply.proof.owner != s.host {
		return nil, errInvalid
	}
	body, err := s.host.exportReplacementRead(reply.proof, target)
	if err != nil {
		return nil, err
	}
	return encodeGraphReplacementRead(body)
}

// Owns only a pending receiving lifetime: no disk/seed, membership, materializer
// or Host is created here. The next receiving path must require real native
// MsgSnap/SnapshotSend provenance, C >= index and snapshot entry term == message
// term == term before same-store activation. It must not Initialize/Bootstrap
// old identity or enable votes/operations while receiving. This volatile owner
// is never persisted or replayed as distributed durable authority.
type graphReplacementAdmission struct {
	owner       *graphReplacementAdmission
	body        graphReplacementReadBody
	issued      graphReplacementReadBody
	association graphReplacementAssociation
	endpoint    *graphAuthenticatedEndpoint
	ctx         context.Context
	finish      func() error
	lifetime    *graphReplacementLifetime
}

func nilGraphReplacementInterface(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	default:
		return false
	}
}
func beginGraphReplacement(ctx context.Context, e *graphAuthenticatedEndpoint, targetVoter uint64, associator graphReplacementAssociator) (*graphReplacementAdmission, error) {
	if ctx == nil || e == nil || e.stream == nil || nilGraphReplacementInterface(associator) || targetVoter == 0 || targetVoter == e.peer.voter {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	targetPeer := graphPeerIdentity{scope: e.peer.scope, voter: targetVoter}
	association, err := associator.RetireAndAcquire(ctx, targetPeer)
	if err != nil {
		if !nilGraphReplacementInterface(association) {
			err = errors.Join(err, association.Close())
		}
		return nil, err
	}
	if nilGraphReplacementInterface(association) {
		return nil, errInvalid
	}
	fail := func(err error) (*graphReplacementAdmission, error) { return nil, errors.Join(err, association.Close()) }
	associationCtx := association.Context()
	target := graphReplacementTarget{voter: targetVoter, incarnation: association.Incarnation()}
	if association.Peer() != targetPeer || associationCtx == nil || target.incarnation == ([16]byte{}) {
		return fail(errInvalid)
	}
	if err := associationCtx.Err(); err != nil {
		return fail(errors.Join(graphstore.ErrSourceUnavailable, err))
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(associationCtx, cancel)
	lifetime := &graphReplacementLifetime{association: association, ctx: associationCtx, cancelPending: cancel, stopPending: stop, closeAssociation: sync.OnceValue(association.Close)}
	finish := lifetime.closePending
	command, err := encodeGraphReplacementTarget(target)
	if err != nil {
		return nil, errors.Join(err, finish())
	}
	r, err := e.call(ownerCtx, graphSourceRequest{kind: sourceReplacementRead, command: command}, 1024)
	if err != nil {
		return nil, errors.Join(err, finish())
	}
	body, err := decodeGraphReplacementRead(r.body)
	if err == nil && (body.scope != e.peer.scope || body.source != e.peer.voter || body.id.session != r.issuer || body.nonce != r.nonce || body.target != target) {
		err = errInvalid
	}
	if err != nil {
		return nil, errors.Join(err, finish())
	}
	a := &graphReplacementAdmission{body: body, issued: body, association: association, endpoint: e, ctx: ownerCtx, finish: finish, lifetime: lifetime}
	a.owner = a
	if err := a.check(); err != nil {
		return nil, errors.Join(err, finish())
	}
	return a, nil
}
func (a *graphReplacementAdmission) check() error {
	if a == nil {
		return errInvalid
	}
	return a.checkContext(a.ctx)
}

func (a *graphReplacementAdmission) checkContext(ctx context.Context) error {
	if a == nil || ctx == nil || a.owner != a || a.ctx == nil || a.finish == nil || nilGraphReplacementInterface(a.association) || a.endpoint == nil || a.endpoint.stream == nil {
		return errInvalid
	}
	if a.body != a.issued {
		return errInvalid
	}
	if a.lifetime != nil && !a.lifetime.pending() {
		return errors.Join(graphstore.ErrSourceUnavailable, a.ctx.Err(), ctx.Err())
	}
	if err := a.body.validate(); err != nil {
		return err
	}
	if err := errors.Join(a.ctx.Err(), ctx.Err()); err != nil {
		return errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	associationCtx := a.association.Context()
	if associationCtx == nil {
		return graphstore.ErrSourceUnavailable
	}
	if err := associationCtx.Err(); err != nil {
		return errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	if a.association.Peer() != (graphPeerIdentity{scope: a.body.scope, voter: a.body.target.voter}) || a.association.Incarnation() != a.body.target.incarnation || a.endpoint.peer != (graphPeerIdentity{scope: a.body.scope, voter: a.body.source}) {
		return graphstore.ErrSourceUnavailable
	}
	if err := a.endpoint.stream.mu.lock(ctx); err != nil {
		return errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	defer a.endpoint.stream.mu.unlock()
	if a.endpoint.stream.invalid || a.endpoint.stream.issuer != a.body.id.session {
		return graphstore.ErrSourceUnavailable
	}
	return nil
}
func (a *graphReplacementAdmission) close() error {
	if a == nil || a.owner != a || a.finish == nil {
		return errInvalid
	}
	return a.finish()
}
