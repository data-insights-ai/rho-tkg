package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

type allocationReadID struct {
	session  [16]byte
	sequence uint64
}

type allocationQuery struct {
	kind     allocationObservationKind
	session  idalloc.RecipientSession
	sequence uint64
	count    uint64
	nonce    [16]byte
}

type allocationProof struct {
	owner       *allocationHost
	id          allocationReadID
	question    allocationQuery
	observation allocationObservation
}

type allocationReply struct {
	id       allocationReadID
	question allocationQuery
	proof    allocationProof
	err      error
}

type allocationEvent struct {
	readID        allocationReadID
	packets       []replica.Packet
	snapshotSends []*replica.SnapshotSend
	replies       []allocationReply
}

// allocationProposal is an owned source-created command. Its private constructor
// accepts a completed proof, not a caller observation/range or durability bool.
type allocationProposal struct {
	target allocationScope
	wire   []byte
}

// allocationHost owns the only completed-read proof constructor and serializes
// storage query capture with Driver events. Transport must authenticate the full
// graph/partition/group tuple under the crash-fault model; these opaque process
// capabilities are not cryptographic/Byzantine quorum certificates.
type allocationHost struct {
	mu         sync.Mutex
	driver     *replica.Driver
	machine    *declaredMaterializer
	session    [16]byte
	sequence   uint64
	pending    map[allocationReadID]allocationQuery
	recipients map[[16]byte]*allocationRecipientHandle
	closed     bool
	closeErr   error
}

const allocationReadContextBytes = 101

func encodeAllocationReadContext(id allocationReadID, q allocationQuery) []byte {
	b := make([]byte, 0, allocationReadContextBytes)
	b = append(b, 'A', 'Q', 'R', 1)
	b = append(b, id.session[:]...)
	b = binary.BigEndian.AppendUint64(b, id.sequence)
	b = append(b, byte(q.kind))
	b = appendSession(b, q.session)
	b = binary.BigEndian.AppendUint64(b, q.sequence)
	b = binary.BigEndian.AppendUint64(b, q.count)
	return append(b, q.nonce[:]...)
}

func decodeAllocationReadContext(b []byte) (allocationReadID, allocationQuery, error) {
	if len(b) != allocationReadContextBytes || !bytes.Equal(b[:4], []byte{'A', 'Q', 'R', 1}) {
		return allocationReadID{}, allocationQuery{}, errCorrupt
	}
	d := decoder{b: b[4:]}
	id := allocationReadID{session: d.array(), sequence: d.number()}
	q := allocationQuery{kind: allocationObservationKind(d.b[0])}
	d.b = d.b[1:]
	q.session, q.sequence, q.count, q.nonce = d.session(), d.number(), d.number(), d.array()
	if len(d.b) != 0 || id.session == ([16]byte{}) || id.sequence == 0 {
		return allocationReadID{}, allocationQuery{}, errCorrupt
	}
	return id, q, nil
}

func openAllocationHost(m *declaredMaterializer, id uint64, cfg replica.Config) (*allocationHost, error) {
	if m == nil || m.store == nil || cfg.Store != m.store || cfg.ApplicationMachine != nil || cfg.Machine != nil {
		return nil, errInvalid
	}
	cfg.ID, cfg.ApplicationMachine = id, m
	d, err := replica.Open(cfg)
	if err != nil {
		return nil, err
	}
	h := &allocationHost{driver: d, machine: m, pending: make(map[allocationReadID]allocationQuery), recipients: make(map[[16]byte]*allocationRecipientHandle)}
	if _, err := rand.Read(h.session[:]); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return h, nil
}

func (h *allocationHost) event(out replica.Output, eventErr error) (allocationEvent, error) {
	if eventErr != nil {
		return allocationEvent{}, eventErr
	}
	e := allocationEvent{packets: out.Packets, snapshotSends: out.SnapshotSends}
	if len(out.Reads) > 16 {
		return allocationEvent{}, errLimit
	}
	for _, ready := range out.Reads {
		id, query, err := decodeAllocationReadContext(ready.Context)
		if err != nil || id.session != h.session || id.sequence > h.sequence || ready.Index > out.Applied || h.driver.Applied() < ready.Index {
			return allocationEvent{}, errors.Join(errInvalid, err)
		}
		pending, found := h.pending[id]
		if !found || pending != query {
			return allocationEvent{}, errInvalid
		}
		delete(h.pending, id)
		observation, err := h.machine.observeAllocation(query, out.Applied)
		reply := allocationReply{id: id, question: query, err: err}
		if err == nil {
			reply.proof = allocationProof{owner: h, id: id, question: query, observation: observation}
		}
		e.replies = append(e.replies, reply)
	}
	return e, nil
}

func (h *allocationHost) Campaign() (allocationEvent, error) {
	if h == nil {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationEvent{}, replica.ErrStopped
	}
	return h.event(h.driver.Campaign())
}

func (h *allocationHost) Tick() (allocationEvent, error) {
	if h == nil {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationEvent{}, replica.ErrStopped
	}
	return h.event(h.driver.Tick())
}

func (h *allocationHost) Step(p replica.Packet) (allocationEvent, error) {
	if h == nil {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationEvent{}, replica.ErrStopped
	}
	return h.event(h.driver.Step(p))
}

func (h *allocationHost) Submit(p allocationProposal) (allocationEvent, error) {
	if h == nil {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationEvent{}, replica.ErrStopped
	}
	local := h.machine.scope()
	if p.target != local || len(p.wire) < requestHeaderBytes || len(p.wire) > h.machine.limits.commandBytes {
		return allocationEvent{}, errInvalid
	}
	if commandKind(p.wire[4]) == initDeclaredPartition {
		r, err := decodeDeclaredInit(p.wire, h.machine.limits)
		if err != nil {
			return allocationEvent{}, err
		}
		if r.semanticContractID() != h.machine.SemanticContractID() {
			return allocationEvent{}, errInvalid
		}

	} else if _, err := decodeAllocationProtocolCommand(p.wire, h.machine.limits); err != nil {
		return allocationEvent{}, err
	}
	return h.event(h.driver.Propose(p.wire))
}

func (q allocationQuery) validate(graph idalloc.GraphID) error {
	if q.nonce == ([16]byte{}) {
		return errInvalid
	}
	switch q.kind {
	case observeConfiguration, observeAllocator:
		if q.session != (idalloc.RecipientSession{}) || q.sequence != 0 || q.count != 0 {
			return errInvalid
		}
	case observeRecipient:
		if q.session.ID == ([16]byte{}) || q.session.Incarnation != ([16]byte{}) || q.session.Epoch != 0 || q.sequence != 0 || q.count != 0 {
			return errInvalid
		}
	case observeGrant:
		if idalloc.ValidateGrantRequest(idalloc.GrantRequest{Graph: graph, Session: q.session, Sequence: q.sequence, Count: q.count}) != nil {
			return errInvalid
		}
	default:
		return errInvalid
	}
	return nil
}

func (h *allocationHost) Read(q allocationQuery) (allocationEvent, error) {
	if h == nil {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationEvent{}, replica.ErrStopped
	}
	if err := q.validate(h.machine.ns.graph); err != nil {
		return allocationEvent{}, err
	}
	if h.sequence == math.MaxUint64 || len(h.pending) >= 16 {
		return allocationEvent{}, errLimit
	}
	h.sequence++
	id := allocationReadID{session: h.session, sequence: h.sequence}
	h.pending[id] = q
	out, err := h.driver.ReadIndex(encodeAllocationReadContext(id, q))
	if err != nil {
		delete(h.pending, id)
		return allocationEvent{}, err
	}
	e, err := h.event(out, nil)
	if err != nil {
		return allocationEvent{}, err
	}
	e.readID = id
	return e, nil
}

func (h *allocationHost) Close() error {
	if h == nil {
		return errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return h.closeErr
	}
	h.closed = true
	clear(h.pending)
	clear(h.recipients)
	h.closeErr = h.driver.Close()
	return h.closeErr
}

func (h *allocationHost) ownProof(p allocationProof, kind allocationObservationKind) error {
	if h.closed || p.owner != h || p.id.session != h.session || p.id.sequence == 0 || p.id.sequence > h.sequence || p.question.kind != kind || p.observation.kind != kind {
		return errInvalid
	}
	if err := p.question.validate(h.machine.ns.graph); err != nil {
		return err
	}
	if p.observation.source.scope != h.machine.scope() {
		return errInvalid
	}
	return nil
}

func (m *declaredMaterializer) scope() allocationScope {
	return allocationScope{graph: m.ns.graph, partition: m.ns.partition, group: m.binding.Identity.Group, ownership: m.owner, topology: m.declaration.TopologyEpoch(), declaration: m.declaration.Digest(), semantic: m.binding.SemanticContractID}
}

func (m *declaredMaterializer) observeAllocation(query allocationQuery, index uint64) (observation allocationObservation, err error) {
	if err := query.validate(m.ns.graph); err != nil {
		return observation, err
	}
	view, err := m.store.ApplicationView(index)
	if err != nil {
		return observation, err
	}
	defer func() {
		if closeErr := view.Close(); closeErr != nil {
			observation = allocationObservation{}
			err = errors.Join(err, closeErr)
		}
	}()
	base, err := view.RootBounded(context.Background(), 172)
	if err != nil {
		return observation, err
	}
	l := m.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, m.limits.sourceRows), min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: context.Background(), view: view, ns: m.ns, limits: l, base: base, bytes: cap(base.Image)}
	info, err := m.readOwnership(q.ctx, view, &q)
	if err != nil {
		return observation, err
	}
	cfg, err := m.initialized(&q)
	if err != nil {
		return observation, err
	}
	digest, err := cfg.digest()
	if err != nil {
		return observation, err
	}
	observation = allocationObservation{kind: query.kind, source: allocationCoordinate{scope: m.scope(), index: base.Index}, configuration: digest, epoch: info.Root().SemanticEpoch(), effect: info.Root().EffectDigest()}
	a := allocationRecordReader{q: &q, scope: m.scope(), config: cfg, declaration: m.declaration}
	switch query.kind {
	case observeConfiguration:
		observation.config = cfg
	case observeAllocator:
		if m.ns.partition != cfg.home {
			return allocationObservation{}, errInvalid
		}
		state, found, err := q.allocator()
		if err != nil || !found {
			return allocationObservation{}, errors.Join(errCorrupt, err)
		}
		observation.allocator = state
	case observeRecipient:
		r, err := a.recipient(query.session.ID)
		if err != nil {
			return allocationObservation{}, err
		}
		if r == nil {
			return allocationObservation{}, replica.ErrUnavailable
		}
		observation.recipient = *r
	case observeGrant:
		g, err := a.grant(query.session, query.sequence)
		if err != nil {
			return allocationObservation{}, err
		}
		if g == nil {
			return allocationObservation{}, replica.ErrUnavailable
		}
		if g.grant.Request.Count != query.count {
			return allocationObservation{}, idalloc.ErrPayloadMismatch
		}
		observation.grant = *g
	}
	if _, err := observation.payload(); err != nil {
		return allocationObservation{}, err
	}
	return observation, nil
}

func (h *allocationHost) target(partition uint64) (allocationScope, error) {
	entry, found := h.machine.declaration.Partition(partition)
	if !found {
		return allocationScope{}, errInvalid
	}
	return allocationScope{graph: h.machine.ns.graph, partition: partition, group: entry.Group, ownership: entry.OwnershipEpoch, topology: h.machine.declaration.TopologyEpoch(), declaration: h.machine.declaration.Digest(), semantic: h.machine.SemanticContractID()}, nil
}

func (h *allocationHost) currentConfiguration() (genesisAllocationConfig, error) {
	query := allocationQuery{kind: observeConfiguration, nonce: [16]byte{1}}
	observed, err := h.machine.observeAllocation(query, h.driver.Applied())
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	return observed.config, nil
}

func (h *allocationHost) protocolProposal(r allocationProtocolCommand, partition uint64) (allocationProposal, error) {
	target, err := h.target(partition)
	if err != nil {
		return allocationProposal{}, err
	}
	r.ns = namespace{graph: h.machine.ns.graph, partition: partition}
	wire, err := encodeAllocationProtocolCommand(r, h.machine.limits)
	if err != nil {
		return allocationProposal{}, err
	}
	return allocationProposal{target: target, wire: wire}, nil
}

// PrepareInitialization is source-owned. A non-authority target requires this
// allocation home's own completed genesis configuration read before any target
// command is produced; supplied schemas/maxBlock must match that observation.
func (h *allocationHost) PrepareInitialization(partition uint64, attempt bootstrapAttemptID, schemas []graphstate.PropertyDefinition, maxBlock uint64, genesis allocationProof) (allocationProposal, error) {
	return h.prepareInitialization(partition, attempt, schemas, maxBlock, types.DefaultAxisBinding{}, genesis, declaredSemanticContractID())
}

func (h *allocationHost) prepareInitialization(partition uint64, attempt bootstrapAttemptID, schemas []graphstate.PropertyDefinition, maxBlock uint64, binding types.DefaultAxisBinding, genesis allocationProof, agreement raftlog.ApplicationSemanticContractID) (allocationProposal, error) {
	if h == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationProposal{}, replica.ErrStopped
	}
	if h.machine.SemanticContractID() != agreement {
		return allocationProposal{}, errInvalid
	}
	target, err := h.target(partition)
	if err != nil {
		return allocationProposal{}, err
	}
	first, _ := h.machine.declaration.PartitionAt(0)
	if h.machine.ns.partition != first.Partition {
		return allocationProposal{}, errInvalid
	}
	r := declaredInitCommand{ns: namespace{graph: h.machine.ns.graph, partition: partition}, attempt: attempt, declaration: h.machine.declaration, maxBlock: maxBlock, schemas: schemas, defaultAxis: binding}
	if partition == first.Partition {
		if genesis != (allocationProof{}) {
			return allocationProposal{}, errInvalid
		}
		r.authority, err = idalloc.NewAuthority(h.session, 1)
		if err != nil {
			return allocationProposal{}, err
		}
	} else {
		if err := h.ownProof(genesis, observeConfiguration); err != nil {
			return allocationProposal{}, err
		}
		o := genesis.observation
		if o.config.home != first.Partition || o.source.scope.partition != first.Partition {
			return allocationProposal{}, errInvalid
		}
		r.genesis = genesisObservation{source: o.source, configuration: o.config, epoch: o.epoch, effect: o.effect}
	}
	wire, err := encodeDeclaredInit(r, h.machine.limits)
	if err != nil {
		return allocationProposal{}, err
	}
	return allocationProposal{target: target, wire: wire}, nil
}

func (h *allocationHost) BeginRecipient(id requestID, recipient [16]byte, current allocationProof) (allocationProposal, error) {
	if h == nil || recipient == ([16]byte{}) {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	kind := current.observation.kind
	if kind != observeConfiguration && kind != observeRecipient {
		return allocationProposal{}, errInvalid
	}
	if err := h.ownProof(current, kind); err != nil {
		return allocationProposal{}, err
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	previous := uint64(0)
	if kind == observeRecipient {
		r := current.observation.recipient
		if r.session.ID != recipient || r.home != h.machine.ns.partition || r.homePhase != recipientActive && r.homePhase != recipientFenced {
			return allocationProposal{}, errInvalid
		}
		previous = r.session.Epoch
	}
	if previous == math.MaxUint64 {
		return allocationProposal{}, idalloc.ErrExhausted
	}
	r := allocationProtocolCommand{id: id, kind: beginDeclaredRecipient, configuration: current.observation.configuration, session: idalloc.RecipientSession{ID: recipient, Incarnation: h.session, Epoch: previous + 1}, home: h.machine.ns.partition, previous: previous, remote: current.observation}
	return h.protocolProposal(r, cfg.home)
}

func (h *allocationHost) RecipientTransition(id requestID, kind commandKind, target uint64, current allocationProof) (allocationProposal, error) {
	if h == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ownProof(current, observeRecipient); err != nil {
		return allocationProposal{}, err
	}
	switch kind {
	case fenceDeclaredRecipient, acknowledgeDeclaredHome, activateDeclaredRecipient, publishDeclaredRecipient:
	default:
		return allocationProposal{}, errInvalid
	}
	return h.protocolProposal(allocationProtocolCommand{id: id, kind: kind, configuration: current.observation.configuration, remote: current.observation}, target)
}

func (h *allocationHost) TransferAllocator(id requestID, current allocationProof) (allocationProposal, error) {
	if h == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ownProof(current, observeAllocator); err != nil {
		return allocationProposal{}, err
	}
	view, err := idalloc.Inspect(&current.observation.allocator)
	if err != nil {
		return allocationProposal{}, err
	}
	if view.Authority.Epoch() == math.MaxUint64 {
		return allocationProposal{}, idalloc.ErrExhausted
	}
	replacement, err := idalloc.NewAuthority(h.session, view.Authority.Epoch()+1)
	if err != nil {
		return allocationProposal{}, err
	}
	return h.protocolProposal(allocationProtocolCommand{id: id, kind: transferDeclaredService, configuration: current.observation.configuration, replacement: replacement, remote: current.observation}, h.machine.ns.partition)
}

func (h *allocationHost) AuthorizeReserve(id requestID, request idalloc.GrantRequest, service allocationProof) (allocationProposal, error) {
	if h == nil || request.Graph != h.machine.ns.graph {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ownProof(service, observeAllocator); err != nil {
		return allocationProposal{}, err
	}
	view, err := idalloc.Inspect(&service.observation.allocator)
	if err != nil {
		return allocationProposal{}, err
	}
	if view.Authority.Owner() != h.session {
		return allocationProposal{}, idalloc.ErrStaleAuthority
	}
	actual, err := h.machine.observeAllocation(allocationQuery{kind: observeAllocator, nonce: [16]byte{1}}, h.driver.Applied())
	if err != nil {
		return allocationProposal{}, err
	}
	current, err := idalloc.Inspect(&actual.allocator)
	if err != nil {
		return allocationProposal{}, err
	}
	if current.Authority != view.Authority || current.Authority.Owner() != h.session {
		return allocationProposal{}, idalloc.ErrStaleAuthority
	}
	return h.protocolProposal(allocationProtocolCommand{id: id, kind: reserveDeclaredGrant, configuration: service.observation.configuration, session: request.Session, sequence: request.Sequence, count: request.Count, remote: service.observation}, h.machine.ns.partition)
}

func (h *allocationHost) InstallGrant(id requestID, home uint64, grant allocationProof) (allocationProposal, error) {
	if h == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ownProof(grant, observeGrant); err != nil {
		return allocationProposal{}, err
	}
	return h.protocolProposal(allocationProtocolCommand{id: id, kind: installDeclaredGrant, configuration: grant.observation.configuration, remote: grant.observation}, home)
}

type allocationRecipientHandle struct {
	host      *allocationHost
	recipient *idalloc.Recipient
	session   idalloc.RecipientSession
}

func (h *allocationHost) OpenRecipient(active allocationProof) (*allocationRecipientHandle, error) {
	if h == nil {
		return nil, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ownProof(active, observeRecipient); err != nil {
		return nil, err
	}
	r := active.observation.recipient
	if r.home != h.machine.ns.partition || r.homePhase != recipientActive || r.session.Incarnation != h.session {
		return nil, idalloc.ErrStaleAuthority
	}
	current, err := h.machine.observeAllocation(allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: r.session.ID}, nonce: [16]byte{1}}, h.driver.Applied())
	if err != nil {
		return nil, err
	}
	if !sameProtocolRecipient(current.recipient, r) || current.recipient.homePhase != recipientActive {
		return nil, idalloc.ErrStaleAuthority
	}
	if old := h.recipients[r.session.ID]; old != nil && old.session == r.session {
		return nil, idalloc.ErrAlreadyReserved
	}
	if len(h.recipients) >= 128 && h.recipients[r.session.ID] == nil {
		return nil, errLimit
	}
	recipient, err := idalloc.NewRecipient(h.machine.ns.graph, r.session)
	if err != nil {
		return nil, err
	}
	i := &allocationRecipientHandle{host: h, recipient: recipient, session: r.session}
	h.recipients[r.session.ID] = i
	return i, nil
}

// Acquire uses one refill callback. It must return the HOME host's exact own
// completed query/nonce reply after installing the source-produced grant. No
// source inspection/retry can construct a cursor, and no per-ID RPC is needed.
func (i *allocationRecipientHandle) Acquire(ctx context.Context, sequence, count uint64, transport func(context.Context, idalloc.GrantRequest, allocationQuery) (allocationReply, error)) (*idalloc.Cursor, error) {
	if i == nil || i.host == nil || i.recipient == nil || ctx == nil || transport == nil {
		return nil, errInvalid
	}
	h := i.host
	request := idalloc.GrantRequest{Graph: h.machine.ns.graph, Session: i.session, Sequence: sequence, Count: count}
	return i.recipient.Acquire(ctx, request, func(request idalloc.GrantRequest) (idalloc.Grant, error) {
		h.mu.Lock()
		if h.closed || h.recipients[i.session.ID] != i {
			h.mu.Unlock()
			return idalloc.Grant{}, idalloc.ErrStaleAuthority
		}
		current, err := h.machine.observeAllocation(allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: i.session.ID}, nonce: [16]byte{1}}, h.driver.Applied())
		if err != nil || current.recipient.homePhase != recipientActive || current.recipient.session != i.session {
			h.mu.Unlock()
			return idalloc.Grant{}, errors.Join(idalloc.ErrStaleAuthority, err)
		}
		h.mu.Unlock()
		query := allocationQuery{kind: observeGrant, session: request.Session, sequence: request.Sequence, count: request.Count}
		if _, err := rand.Read(query.nonce[:]); err != nil {
			return idalloc.Grant{}, err
		}
		reply, err := transport(ctx, request, query)
		if err != nil || reply.err != nil {
			return idalloc.Grant{}, errors.Join(idalloc.ErrUnknown, err, reply.err)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.closed || h.recipients[i.session.ID] != i {
			return idalloc.Grant{}, idalloc.ErrStaleAuthority
		}
		if reply.question != query || reply.proof.question != query || reply.id != reply.proof.id {
			return idalloc.Grant{}, idalloc.ErrPayloadMismatch
		}
		if err := h.ownProof(reply.proof, observeGrant); err != nil {
			return idalloc.Grant{}, err
		}
		current, err = h.machine.observeAllocation(allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: i.session.ID}, nonce: [16]byte{1}}, h.driver.Applied())
		if err != nil {
			return idalloc.Grant{}, err
		}
		if current.recipient.homePhase != recipientActive || current.recipient.session != i.session {
			return idalloc.Grant{}, idalloc.ErrStaleAuthority
		}
		grant := reply.proof.observation.grant.grant
		if grant.Request != request {
			return idalloc.Grant{}, idalloc.ErrPayloadMismatch
		}
		return grant, nil
	})
}
