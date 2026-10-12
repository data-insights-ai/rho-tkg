package graphapply

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Unique ownership can move from pending admission to one real Host. Closing
// any former pending object after transfer cannot release live credentials.
type graphReplacementLifetime struct {
	mu               sync.Mutex
	association      graphReplacementAssociation
	ctx              context.Context
	cancelPending    context.CancelFunc
	stopPending      func() bool
	closeAssociation func() error
	holder           *graphReplacementReceiver
	closed, moved    bool
	closeErr         error
}

func (l *graphReplacementLifetime) pending() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.closed && !l.moved
}
func (l *graphReplacementLifetime) closePending() error {
	l.mu.Lock()
	if l.moved {
		l.mu.Unlock()
		return nil
	}
	if l.closed {
		err := l.closeErr
		l.mu.Unlock()
		return err
	}
	l.closed = true
	l.mu.Unlock()
	l.cancelPending()
	l.stopPending()
	err := l.closeAssociation()
	l.mu.Lock()
	l.closeErr = err
	l.mu.Unlock()
	return err
}
func (l *graphReplacementLifetime) transfer(o *graphReplacementReceiver, h *allocationHost) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.moved || l.holder != o || l.ctx.Err() != nil || o.admission.ctx.Err() != nil {
		return graphstore.ErrSourceUnavailable
	}
	l.moved = true
	h.mu.Lock()
	h.replacementCtx, h.replacementClose = l.ctx, l.closeAssociation
	h.replacementStop = context.AfterFunc(l.ctx, func() { h.ended.Store(true); _ = h.Close() })
	h.mu.Unlock()
	// Only the request-linked pending context is retired; association.Context
	// independently owns the new Host. No association.Close occurs here.
	l.cancelPending()
	l.stopPending()
	return nil
}

// This unexported owner is created only from a live opaque A admission. The
// embedding forwards packets only from that captured authenticated source
// association; RP1 bytes/numeric peers alone do not establish authentication.
type graphReplacementReceiver struct {
	self                *graphReplacementReceiver
	mu                  sync.Mutex
	admission           *graphReplacementAdmission
	store               *raftlog.Store
	receiver            *replica.ApplicationReceiver
	declaration         graphstore.OwnershipDeclaration
	limits              materializerLimits
	closed, transferred bool
}

func beginGraphReplacementReceiving(a *graphReplacementAdmission, c raftlog.Config, g provisionedLogicalGraph, l materializerLimits) (*graphReplacementReceiver, error) {
	if a == nil || a.lifetime == nil {
		return nil, errInvalid
	}
	if err := a.check(); err != nil {
		return nil, err
	}
	if err := l.validate(); err != nil {
		return nil, err
	}
	r, err := decodeDeclaredInit(g.genesis, l)
	if err != nil {
		return nil, err
	}
	part, found := r.declaration.Partition(a.body.scope.partition)
	if !found || g.agreement != a.body.scope.semantic || g.voters != a.body.voters || c.Application.LocalVoter != a.body.target.voter ||
		[16]byte(r.declaration.Graph()) != [16]byte(a.body.scope.graph) || r.declaration.Digest() != a.body.scope.declaration || r.declaration.TopologyEpoch() != a.body.scope.topology || part.Group != a.body.scope.group || part.OwnershipEpoch != a.body.scope.ownership {
		return nil, errInvalid
	}
	cfg, err := r.validate(l)
	if err != nil {
		return nil, err
	}
	digest, err := cfg.digest()
	if err != nil || digest != a.body.configuration {
		return nil, errors.Join(errInvalid, err)
	}
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(a.body.scope.graph), Partition: a.body.scope.partition, Group: a.body.scope.group}, SemanticContractID: a.body.scope.semantic}
	if c.Transfer.Identity != (raftlog.ApplicationIdentity{}) && c.Transfer.Identity != binding.Identity || c.SemanticContractID != (raftlog.ApplicationSemanticContractID{}) && c.SemanticContractID != binding.SemanticContractID || c.Replication != (raftlog.ApplicationReplicationConfig{}) && c.Replication.Voters != a.body.voters {
		return nil, errInvalid
	}
	c.Transfer.Identity, c.SemanticContractID, c.Replication = binding.Identity, binding.SemanticContractID, raftlog.ApplicationReplicationConfig{Voters: a.body.voters}
	s, err := raftlog.Open(c)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*graphReplacementReceiver, error) { return nil, errors.Join(err, s.Close()) }
	state, err := s.ApplicationReceivingState()
	if err != nil || state.Binding != binding || state.Voters != a.body.voters || state.Term > a.body.term {
		return fail(errors.Join(errInvalid, err))
	}
	b, err := replica.OpenApplicationReceiver(replica.ApplicationReceivingConfig{Store: s, Binding: binding, ID: a.body.target.voter, Source: a.body.source, Term: a.body.term})
	if err != nil {
		return fail(err)
	}
	o := &graphReplacementReceiver{admission: a, store: s, receiver: b, declaration: r.declaration, limits: l}
	o.self = o
	a.lifetime.mu.Lock()
	if a.lifetime.closed || a.lifetime.moved || a.lifetime.holder != nil {
		a.lifetime.mu.Unlock()
		_ = b.Close()
		return fail(errInvalid)
	}
	a.lifetime.holder = o
	a.lifetime.mu.Unlock()
	if err := o.check(); err != nil {
		return nil, errors.Join(err, o.close())
	}
	return o, nil
}

func (o *graphReplacementReceiver) check() error {
	if o == nil || o.admission == nil {
		return errInvalid
	}
	return o.checkContext(o.admission.ctx)
}

func (o *graphReplacementReceiver) checkContext(ctx context.Context) error {
	if o == nil || o.self != o || o.closed || o.transferred || o.admission == nil {
		return errInvalid
	}
	if err := o.admission.checkContext(ctx); err != nil {
		return err
	}
	if o.admission.endpoint.transport.Peer() != o.admission.endpoint.peer {
		return graphstore.ErrSourceUnavailable
	}
	return nil
}

func (o *graphReplacementReceiver) step(p replica.Packet) ([]replica.Packet, error) {
	if o == nil || o.self != o {
		return nil, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.check(); err != nil {
		return nil, err
	}
	packets, err := o.receiver.StepReceiving(p)
	if err != nil {
		return nil, err
	}
	if err := o.check(); err != nil {
		return nil, err
	}
	return packets, nil
}

func (o *graphReplacementReceiver) activate(ctx context.Context, packet replica.Packet, prepared *raftlog.PreparedApplicationSnapshot) (*allocationHost, []replica.Packet, error) {
	if o == nil || o.self != o || o.admission == nil || ctx == nil {
		return nil, nil, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	operationCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.admission.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := o.checkContext(operationCtx); err != nil {
		return nil, nil, err
	}
	n := namespace{graph: o.admission.body.scope.graph, partition: o.admission.body.scope.partition}
	auditor, err := newDeclaredSnapshotAuditor(o.store, n, o.declaration, o.limits, o.admission.body.scope.semantic)
	if err != nil {
		return nil, nil, err
	}
	var machine *declaredMaterializer
	newDriver := func() (replica.Config, error) {
		var err error
		machine, err = newDeclaredMaterializerBinding(o.store, n, o.declaration, o.limits, o.admission.body.scope.semantic)
		if err != nil {
			return replica.Config{}, err
		}
		index, _, err := o.store.Checkpoint()
		if err != nil {
			return replica.Config{}, err
		}
		observation, err := machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: o.admission.body.nonce}, index)
		if err != nil || observation.configuration != o.admission.body.configuration {
			return replica.Config{}, errors.Join(errInvalid, err)
		}
		return replica.Config{ID: o.admission.body.target.voter, Store: o.store, ApplicationMachine: machine, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}}, nil
	}
	driver, output, err := o.receiver.ActivateApplicationSnapshot(operationCtx, packet, replica.ApplicationReceivingActivation{Prepared: prepared, Auditor: auditor, Budget: auditor.advance, MinimumIndex: o.admission.body.index, Check: func() error { return o.checkContext(operationCtx) }, NewDriver: newDriver})
	if err != nil {
		return nil, nil, err
	}
	h := &allocationHost{driver: driver, machine: machine, pending: make(map[allocationReadID]allocationQuery), recipients: make(map[[16]byte]*allocationRecipientHandle)}
	if _, err := rand.Read(h.session[:]); err != nil {
		return nil, nil, errors.Join(err, driver.Close())
	}
	if err := errors.Join(operationCtx.Err(), o.checkContext(operationCtx)); err != nil {
		return nil, nil, errors.Join(err, h.Close())
	}
	if err := o.admission.lifetime.transfer(o, h); err != nil {
		return nil, nil, errors.Join(err, h.Close())
	}
	o.transferred = true
	if h.replacementCtx.Err() != nil {
		return nil, nil, errors.Join(graphstore.ErrSourceUnavailable, h.Close())
	}
	return h, output.Packets, nil
}

func (o *graphReplacementReceiver) close() error {
	if o == nil || o.self != o {
		return errInvalid
	}
	// Cancellation comes before waiting for an audit/receiving event. Moved
	// pending cleanup cannot cancel or close the live Host's association.
	o.admission.lifetime.mu.Lock()
	moved := o.admission.lifetime.moved
	o.admission.lifetime.mu.Unlock()
	if !moved {
		o.admission.lifetime.cancelPending()
	}
	o.mu.Lock()
	if o.closed || o.transferred {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	receiver, store, admission := o.receiver, o.store, o.admission
	o.mu.Unlock()
	// Association.Close is an embedding callback: it may reenter pending Close.
	// The closed fence is published before callbacks or backend cleanup begin.
	return errors.Join(receiver.Close(), store.Close(), admission.close())
}
