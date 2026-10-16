package graphapply

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// All handles and completed replies remain in the actual child. The parent
// routes inert requests/proposals; it cannot provide a delivery proof or cursor.
type allocationP08Attempt struct {
	ctx      context.Context
	cancel   context.CancelFunc
	request  idalloc.GrantRequest
	question allocationQuery
	id       allocationReadID
	actual   allocationReply
	ready    bool
	work     chan struct{}
	deliver  chan bool
	done     chan allocationP08CursorResult
}
type allocationP08CursorResult struct {
	cursor *idalloc.Cursor
	err    error
}
type allocationP08Cursor struct {
	cursor *idalloc.Cursor
	grant  grantReference
}
type allocationP08Owners struct {
	mu      sync.Mutex
	ctx     context.Context
	host    *allocationHost
	owners  *certifiedProcessOwners
	attempt *allocationP08Attempt
	cursors map[uint64]allocationP08Cursor
	next    uint64
	wg      sync.WaitGroup
}

func newAllocationP08Owners(ctx context.Context, h *allocationHost, o *certifiedProcessOwners) *allocationP08Owners {
	if os.Getenv("RHO_GRAPH_ALLOCATION_PROCESS") != "1" {
		return nil
	}
	if os.Getenv("RHO_GRAPH_PROTOCOL_STATE_PROCESS") == "1" {
		// Only the explicit D constructor borrows the already-admitted original
		// native observation lifetime. Recipient issuance cannot outlive it.
		if h == nil || o == nil || o.stableBatch == nil || h.machine.SemanticContractID() != rangeMoveSpanSemanticContractID() {
			return nil
		}
		ctx = o.stableBatch.ctx
	}
	return &allocationP08Owners{ctx: ctx, host: h, owners: o, cursors: make(map[uint64]allocationP08Cursor)}
}
func (s *allocationP08Owners) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.attempt != nil {
		s.attempt.cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
func (s *allocationP08Owners) begin(sequence, count uint64) ([]byte, error) {
	if s == nil || count == 0 || count > 16 || sequence == 0 || s.owners.recipient == nil {
		return nil, errInvalid
	}
	s.mu.Lock()
	if s.attempt != nil || len(s.cursors) >= 2 {
		s.mu.Unlock()
		return nil, errLimit
	}
	ctx, cancel := context.WithCancel(s.ctx)
	a := &allocationP08Attempt{ctx: ctx, cancel: cancel, work: make(chan struct{}, 1), deliver: make(chan bool, 1), done: make(chan allocationP08CursorResult, 1)}
	s.attempt = a
	recipient := s.owners.recipient
	s.mu.Unlock()
	s.wg.Go(func() {
		cursor, err := recipient.Acquire(ctx, sequence, count, func(ctx context.Context, r idalloc.GrantRequest, q allocationQuery) (allocationReply, error) {
			s.mu.Lock()
			a.request, a.question = r, q
			s.mu.Unlock()
			a.work <- struct{}{}
			select {
			case live := <-a.deliver:
				if !live {
					return allocationReply{}, idalloc.ErrUnknown
				}
				s.mu.Lock()
				reply, ready := a.actual, a.ready
				s.mu.Unlock()
				if !ready {
					return allocationReply{}, errInvalid
				}
				return reply, nil
			case <-ctx.Done():
				return allocationReply{}, ctx.Err()
			}
		})
		a.done <- allocationP08CursorResult{cursor, err}
	})
	select {
	case <-a.work:
		s.mu.Lock()
		r := a.request
		s.mu.Unlock()
		return encodeAllocationP08Request(r)
	case result := <-a.done:
		s.mu.Lock()
		s.attempt = nil
		s.mu.Unlock()
		cancel()
		return encodeAllocationP08Result(0, result.err)
	case <-s.ctx.Done():
		cancel()
		return nil, s.ctx.Err()
	}
}
func (s *allocationP08Owners) read() (allocationEvent, error) {
	if s == nil {
		return allocationEvent{}, errInvalid
	}
	s.mu.Lock()
	a := s.attempt
	if a == nil || a.id != (allocationReadID{}) {
		s.mu.Unlock()
		return allocationEvent{}, errInvalid
	}
	q := a.question
	s.mu.Unlock()
	event, err := s.host.Read(q)
	if err != nil {
		return allocationEvent{}, err
	}
	s.mu.Lock()
	a.id = event.readID
	s.mu.Unlock()
	if err := s.event(&event); err != nil {
		return allocationEvent{}, err
	}
	return event, nil
}
func (s *allocationP08Owners) event(e *allocationEvent) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.attempt
	if a == nil || a.id == (allocationReadID{}) {
		return nil
	}
	for i := 0; i < len(e.replies); {
		r := e.replies[i]
		if r.id != a.id {
			i++
			continue
		}
		if a.ready || r.question != a.question {
			return errInvalid
		}
		if r.err == nil && (r.proof.owner != s.host || r.proof.id != r.id || r.proof.question != a.question) {
			return errInvalid
		}
		a.actual, a.ready = r, true
		e.replies = append(e.replies[:i], e.replies[i+1:]...)
	}
	return nil
}
func (s *allocationP08Owners) finish(live bool) ([]byte, error) {
	if s == nil {
		return nil, errInvalid
	}
	s.mu.Lock()
	a := s.attempt
	if a == nil || live && !a.ready {
		s.mu.Unlock()
		return nil, errInvalid
	}
	s.mu.Unlock()
	a.deliver <- live
	select {
	case result := <-a.done:
		s.mu.Lock()
		s.attempt = nil
		alias := uint64(0)
		if result.err == nil {
			if result.cursor == nil || s.next >= 2 {
				s.mu.Unlock()
				a.cancel()
				return nil, errCorrupt
			}
			s.next++
			alias = s.next
			s.cursors[alias] = allocationP08Cursor{result.cursor, grantReference{session: a.request.Session, sequence: a.request.Sequence}}
		}
		s.mu.Unlock()
		a.cancel()
		return encodeAllocationP08Result(alias, result.err)
	case <-s.ctx.Done():
		a.cancel()
		return nil, s.ctx.Err()
	}
}
func (s *allocationP08Owners) mint(alias uint64) ([]byte, error) {
	if s == nil {
		return nil, errInvalid
	}
	s.mu.Lock()
	c, found := s.cursors[alias]
	s.mu.Unlock()
	if !found {
		return encodeAllocationP08Result(0, errInvalid)
	}
	id, err := c.cursor.Next()
	return encodeAllocationP08Result(id, err)
}
func encodeAllocationP08Request(r idalloc.GrantRequest) ([]byte, error) {
	if err := idalloc.ValidateGrantRequest(r); err != nil {
		return nil, err
	}
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte("P8Q1"))
		w.add(r.Graph[:])
		w.add(r.Session.ID[:])
		w.add(r.Session.Incarnation[:])
		w.u64(r.Session.Epoch)
		w.u64(r.Sequence)
		w.u64(r.Count)
	})
}
func decodeAllocationP08Request(wire []byte) (idalloc.GrantRequest, error) {
	c := graphCursor{b: wire}
	if string(c.take(4)) != "P8Q1" {
		return idalloc.GrantRequest{}, errCorrupt
	}
	r := idalloc.GrantRequest{Graph: idalloc.GraphID(c.array()), Session: idalloc.RecipientSession{ID: c.array(), Incarnation: c.array(), Epoch: c.u64()}, Sequence: c.u64(), Count: c.u64()}
	if c.err != nil || len(c.b) != 0 || idalloc.ValidateGrantRequest(r) != nil {
		return idalloc.GrantRequest{}, errors.Join(errCorrupt, c.err)
	}
	return r, nil
}

// Native semantic refusals are inert diagnostic tags, never completion authority.
func encodeAllocationP08Result(id uint64, err error) ([]byte, error) {
	tag := byte(0)
	if err != nil {
		switch {
		case errors.Is(err, idalloc.ErrStaleAuthority):
			tag = 1
		case errors.Is(err, idalloc.ErrAlreadyReserved):
			tag = 2
		case errors.Is(err, idalloc.ErrUnknown):
			tag = 3
		case errors.Is(err, idalloc.ErrExhausted):
			tag = 4
		case errors.Is(err, idalloc.ErrPayloadMismatch):
			tag = 5
		case errors.Is(err, errInvalid), errors.Is(err, idalloc.ErrInvalid):
			tag = 6
		default:
			return nil, err
		}
	}
	return processEncoding(func(w *boundedWriter) { w.add([]byte("P8R1")); w.tag(tag); w.u64(id) })
}
func decodeAllocationP08Result(wire []byte) (uint64, error) {
	c := graphCursor{b: wire}
	if string(c.take(4)) != "P8R1" {
		return 0, errCorrupt
	}
	tag, id := c.tag(), c.u64()
	if c.err != nil || len(c.b) != 0 || tag > 6 || tag != 0 && id != 0 {
		return 0, errors.Join(errCorrupt, c.err)
	}
	causes := []error{nil, idalloc.ErrStaleAuthority, idalloc.ErrAlreadyReserved, idalloc.ErrUnknown, idalloc.ErrExhausted, idalloc.ErrPayloadMismatch, errInvalid}
	return id, causes[tag]
}
func (s *allocationP08Owners) proposal(r processRequest) (allocationProposal, error) {
	if s == nil {
		return allocationProposal{}, errInvalid
	}
	s.mu.Lock()
	cursor, found := s.cursors[r.index]
	s.mu.Unlock()
	if !found {
		return allocationProposal{}, errInvalid
	}
	cfg, err := s.host.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	id := requestID(s.owners.incarnation)
	binary.BigEndian.PutUint64(id[8:], r.sequence)
	next := func() (uint64, error) { return cursor.cursor.Next() }
	entity, err := next()
	if err != nil {
		return allocationProposal{}, err
	}
	var ops []graphstate.Operation
	var claims []freshBinding
	switch r.entity {
	case 1, 2:
		life, err := next()
		if err != nil {
			return allocationProposal{}, err
		}
		kind := graphstate.CreateNode
		record := graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}
		binding := graphstate.LifeRecord{}
		if r.entity == 2 {
			if len(r.data) != 32 {
				return allocationProposal{}, errInvalid
			}
			kind = graphstate.CreateRelationship
			record = graphstate.EntityRecord{Type: "p08-edge", Source: graphstate.EntityID(binary.BigEndian.Uint64(r.data[:8])), Target: graphstate.EntityID(binary.BigEndian.Uint64(r.data[8:16])), Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}
			binding = graphstate.LifeRecord{SourceLife: graphstate.LifeID(binary.BigEndian.Uint64(r.data[16:24])), TargetLife: graphstate.LifeID(binary.BigEndian.Uint64(r.data[24:]))}
		}
		ops = []graphstate.Operation{{Kind: kind, Owner: graphstate.EntityID(entity), Life: graphstate.LifeID(life), Scope: scope, Record: record, Binding: binding}}
		claims = []freshBinding{{role: entityBinding, id: entity, grant: cursor.grant}, {role: lifeBinding, owner: graphstate.EntityID(entity), id: life, grant: cursor.grant}}
	case 4:
		if len(r.data) != 8 {
			return allocationProposal{}, errInvalid
		}
		owner := binary.BigEndian.Uint64(r.data)
		ops = []graphstate.Operation{{Kind: graphstate.Reopen, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(entity), Scope: scope}}
		claims = []freshBinding{{role: lifeBinding, owner: graphstate.EntityID(owner), id: entity, grant: cursor.grant}}
	case 3:
		if len(r.data) != 24 {
			return allocationProposal{}, errInvalid
		}
		owner, life, value := binary.BigEndian.Uint64(r.data[:8]), binary.BigEndian.Uint64(r.data[8:16]), binary.BigEndian.Uint64(r.data[16:])
		if value > 100 {
			return allocationProposal{}, errInvalid
		}
		ops = []graphstate.Operation{{Kind: graphstate.Set, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Scope: scope, Name: "p", ValueID: graphstate.ValueID(entity), Value: graphstate.I64(int64(value))}}
		claims = []freshBinding{{role: valueBinding, id: entity, grant: cursor.grant}}
	default:
		return allocationProposal{}, errInvalid
	}
	return s.host.PreparePartitionGraphOperations(id, ops, 41, claims)
}

func allocationP08ReadRequest(o *certifiedProcessOwners, r processRequest) (allocationEvent, error) {
	request, err := decodeAllocationP08Request(r.data)
	if err != nil {
		return allocationEvent{}, err
	}
	if request.Graph != o.host.machine.ns.graph {
		return allocationEvent{}, errInvalid
	}
	q := allocationQuery{kind: observeGrant, session: request.Session, sequence: request.Sequence, count: request.Count}
	copy(q.nonce[:8], o.incarnation[:8])
	binary.BigEndian.PutUint64(q.nonce[8:], r.sequence)
	o.ready, o.proof = false, allocationProof{}
	event, err := o.host.Read(q)
	if err == nil {
		o.readID, o.question = event.readID, q
	}
	return event, err
}

// Fixed-size native observations are inspection data. They cannot enter Acquire
// as a reply, manufacture a cursor or serve as a completed ReadIndex proof.
func allocationP08Metadata(ctx context.Context, h *allocationHost, wire []byte) ([]byte, error) {
	if h == nil || ctx == nil {
		return nil, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	index := h.driver.Applied()
	last, err := h.machine.store.LastIndex()
	if err != nil {
		return nil, err
	}
	config, err := h.currentConfiguration()
	if err != nil {
		return nil, err
	}
	// A fixed three-word query inspects owner-qualified Life and Value data on
	// this actual voter; it is never allocation delivery authority.
	if len(wire) == 24 {
		backend, err := h.machine.store.ApplicationView(index)
		if err != nil {
			return nil, err
		}
		defer backend.Close()
		digest, err := config.digest()
		if err != nil {
			return nil, err
		}
		l := h.machine.limits
		view, _, err := graphstore.OpenPartitionGraphReadView(ctx, backend, h.machine.declaration, config.defaultAxis, digest, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: l.outputBytes})
		if err != nil {
			return nil, err
		}
		defer view.Close()
		life, err := view.Life(ctx, graphstate.EntityID(binary.BigEndian.Uint64(wire[:8])), graphstate.LifeID(binary.BigEndian.Uint64(wire[8:16])))
		if err != nil {
			return nil, err
		}
		value, err := view.Value(ctx, graphstate.ValueID(binary.BigEndian.Uint64(wire[16:])))
		if err != nil {
			return nil, err
		}
		flags := byte(0)
		if life.Found {
			flags |= 1
		}
		if value.Found {
			flags |= 2
		}
		return []byte{flags}, nil
	}
	var allocator, grant []byte
	if h.machine.ns.partition == config.home {
		o, err := h.machine.observeAllocation(allocationQuery{kind: observeAllocator, nonce: [16]byte{1}}, index)
		if err != nil {
			return nil, err
		}
		allocator, err = idalloc.MarshalCheckpoint(&o.allocator)
		if err != nil {
			return nil, err
		}
	}
	if len(wire) != 0 {
		request, err := decodeAllocationP08Request(wire)
		if err != nil {
			return nil, err
		}
		o, err := h.machine.observeAllocation(allocationQuery{kind: observeGrant, session: request.Session, sequence: request.Sequence, count: request.Count, nonce: [16]byte{1}}, index)
		if err == nil {
			grant, err = encodeProtocolGrant(h.machine.ns, o.grant)
		}
		if err != nil && !errors.Is(err, replica.ErrUnavailable) {
			return nil, err
		}
	}
	return processEncoding(func(w *boundedWriter) { w.u64(index); w.u64(last); w.field(allocator); w.field(grant) })
}
