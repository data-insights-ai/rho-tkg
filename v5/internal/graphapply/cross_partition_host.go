package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// These process-private proofs are completed by the existing Host ReadIndex
// event seam. They are neither caller data nor certified global graph cuts.
type crossQuery struct {
	tx    [16]byte
	nonce [16]byte
	phase crossPhase
}
type crossProof struct {
	owner    *allocationHost
	id       allocationReadID
	question crossQuery
	status   crossStatus
	manifest []byte
	index    uint64 // actual applied source view used by this completed Host reply
}
type crossReply struct {
	proof crossProof
	err   error
}
type crossTransaction struct {
	owner    *allocationHost
	manifest []byte
	digest   [32]byte
}

func encodeCrossReadContext(id allocationReadID, q crossQuery) []byte {
	b := append([]byte{'G', 'C', 'R', 1}, id.session[:]...)
	b = binary.BigEndian.AppendUint64(b, id.sequence)
	b = append(b, q.tx[:]...)
	b = append(b, q.nonce[:]...)
	return append(b, byte(q.phase))
}
func (h *allocationHost) ReadCrossTransaction(tx, nonce [16]byte) (allocationEvent, error) {
	return h.ReadCrossPhase(tx, 0, nonce)
}
func (h *allocationHost) ReadCrossPhase(tx [16]byte, phase crossPhase, nonce [16]byte) (allocationEvent, error) {
	if h == nil || tx == ([16]byte{}) || nonce == ([16]byte{}) || phase > crossResolve {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return allocationEvent{}, replica.ErrStopped
	}
	if h.machine == nil || h.driver == nil || !crossGraphAgreement(h.machine.SemanticContractID()) {
		return allocationEvent{}, errInvalid
	}
	if h.sequence == math.MaxUint64 || h.pendingReadCount() >= 16 {
		return allocationEvent{}, errLimit
	}
	if h.crossPending == nil {
		h.crossPending = make(map[allocationReadID]crossQuery)
	}
	h.sequence++
	h.issuedReads.Store(h.sequence)
	id := allocationReadID{h.session, h.sequence}
	query := crossQuery{tx: tx, nonce: nonce, phase: phase}
	h.crossPending[id] = query
	out, err := h.driver.ReadIndex(encodeCrossReadContext(id, query))
	if err != nil {
		delete(h.crossPending, id)
		return allocationEvent{}, err
	}
	e, err := h.event(out, nil)
	e.readID = id
	return e, err
}

func (h *allocationHost) completeCrossRead(wire []byte, index uint64) (crossReply, error) {
	if len(wire) != 61 || !bytes.Equal(wire[:4], []byte{'G', 'C', 'R', 1}) {
		return crossReply{}, errCorrupt
	}
	var id allocationReadID
	copy(id.session[:], wire[4:20])
	id.sequence = binary.BigEndian.Uint64(wire[20:28])
	var q crossQuery
	copy(q.tx[:], wire[28:44])
	copy(q.nonce[:], wire[44:60])
	q.phase = crossPhase(wire[60])
	pending, found := h.crossPending[id]
	if !found || pending != q || id.session != h.session || id.sequence > h.sequence {
		return crossReply{}, errInvalid
	}
	delete(h.crossPending, id)
	status, manifest, err := h.machine.observeCrossTransaction(q, index)
	if err != nil {
		return crossReply{err: err}, nil
	}
	return crossReply{proof: crossProof{h, id, q, status, manifest, index}}, nil
}

func (m *declaredMaterializer) observeCrossTransaction(query crossQuery, index uint64) (status crossStatus, manifest []byte, err error) {
	tx := query.tx
	view, err := m.store.ApplicationView(index)
	if err != nil {
		return status, nil, err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	base, err := view.RootBounded(context.Background(), 172)
	if err != nil {
		return status, nil, err
	}
	l := m.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, m.limits.sourceRows), min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: context.Background(), view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	wire, found, err := q.getBounded(crossKey(m.ns, tx), 256)
	if err != nil {
		return status, nil, err
	}
	if !found {
		return status, nil, replica.ErrUnavailable
	}
	status, err = decodeCrossStatus(wire)
	if err != nil || status.ns != m.ns || status.tx != tx || status.index > index {
		return crossStatus{}, nil, errors.Join(errCorrupt, err)
	}
	record, found, err := crossControl(&q, crossControlKey(m.ns, tx, 'R'), m.limits.commandBytes)
	if err != nil || !found {
		return crossStatus{}, nil, errors.Join(errCorrupt, err)
	}
	if sha256.Sum256(record.Value) != status.digest {
		return crossStatus{}, nil, errCorrupt
	}
	if query.phase == crossRegister {
		status = crossStatus{ns: m.ns, tx: tx, digest: status.digest, phase: crossRegister, index: record.Index}
	} else if query.phase == crossPrepare || query.phase == crossDecide {
		tag := byte('P')
		if query.phase == crossDecide {
			tag = 'D'
		}
		phase, found, e := crossControl(&q, crossControlKey(q.ns, tx, tag), 256)
		if e != nil {
			return crossStatus{}, nil, e
		}
		if !found {
			return crossStatus{}, nil, replica.ErrUnavailable
		}
		fixed, e := decodeCrossStatus(phase.Value)
		if e != nil || fixed.phase != query.phase || fixed.ns != m.ns || fixed.tx != tx || fixed.digest != status.digest || fixed.index != phase.Index {
			return crossStatus{}, nil, errors.Join(errCorrupt, e)
		}
		status = fixed
	} else if query.phase != 0 && query.phase != status.phase {
		return crossStatus{}, nil, replica.ErrUnavailable
	}
	return status, record.Value, nil
}

func checkCrossProof(p crossProof, tx [16]byte, digest [32]byte) error {
	if p.owner == nil || p.owner.machine == nil || p.id.session != p.owner.session || p.id.sequence == 0 || p.id.sequence > p.owner.issuedReads.Load() || p.question.tx != tx || p.question.nonce == ([16]byte{}) || p.status.tx != tx || p.status.digest != digest || p.status.ns != p.owner.machine.ns || !crossGraphAgreement(p.owner.machine.SemanticContractID()) || sha256.Sum256(p.manifest) != digest {
		return errInvalid
	}
	return nil
}

// PrepareCrossPartitionGraphOperations plans typed operations once, borrowing
// each source only at its own completed request-bound ReadIndex coordinate.
func (h *allocationHost) PrepareCrossPartitionGraphOperations(ctx context.Context, id requestID, ops []graphstate.Operation, provenance uint64, claims []freshBinding, sources []allocationProof) (tx crossTransaction, proposal allocationProposal, err error) {
	if h == nil || ctx == nil || id == (requestID{}) {
		return tx, proposal, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return tx, proposal, replica.ErrStopped
	}
	if !crossGraphAgreement(h.machine.SemanticContractID()) || len(sources) != h.machine.declaration.Len() {
		return tx, proposal, errInvalid
	}
	l := h.machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return tx, proposal, err
	}
	if err := arena.Reserve(4096 + 1024*len(sources)); err != nil {
		return tx, proposal, outputBudgetFailure(err)
	}
	views := make([]*raftlog.ApplicationView, len(sources))
	captured := make([]capturedGraphSource, len(sources))
	defer func() {
		for _, v := range views {
			if v != nil {
				err = errors.Join(err, v.Close())
			}
		}
		if err != nil {
			tx = crossTransaction{}
			proposal = allocationProposal{}
		}
	}()
	var configuration [32]byte
	seen := make(map[uint64]bool)
	work := graphstore.PageWork{}
	for i, p := range sources {
		if p.owner == nil || p.owner.machine == nil || p.owner.issuerUnavailable() || p.id.session != p.owner.session || p.id.sequence == 0 || p.id.sequence > p.owner.issuedReads.Load() || p.question.kind != observeConfiguration || p.question.nonce == ([16]byte{}) || p.observation.source.scope != p.owner.machine.scope() || p.observation.source.scope.semantic != h.machine.SemanticContractID() || seen[p.observation.source.scope.partition] || p.observation.source.scope.declaration != h.machine.declaration.Digest() {
			return tx, proposal, errInvalid
		}
		seen[p.observation.source.scope.partition] = true
		readLimits := l.allocation
		readLimits.readRows = min(readLimits.readRows, l.sourceRows-work.Records)
		readLimits.readBytes = min(readLimits.readBytes, l.sourceBytes-work.Bytes)
		if readLimits.readRows < 1 || readLimits.readBytes < 172 {
			return tx, proposal, errLimit
		}
		if i == 0 {
			configuration = p.observation.configuration
		} else if p.observation.configuration != configuration {
			return tx, proposal, errInvalid
		}
		v, e := p.owner.machine.store.ApplicationView(p.observation.source.index)
		if e != nil {
			return tx, proposal, e
		}
		views[i] = v
		base, e := v.RootBounded(ctx, 172)
		if e != nil {
			return tx, proposal, e
		}
		root, e := graphstore.DecodeRoot(base.Image)
		if e != nil || root.SemanticEpoch() != p.observation.epoch || root.EffectDigest() != p.observation.effect {
			return tx, proposal, errors.Join(errInvalid, e)
		}
		q := reader{ctx: ctx, view: v, ns: p.owner.machine.ns, base: base, limits: readLimits, bytes: cap(base.Image)}
		floor, e := readPartitionRoundFloor(&q, configuration)
		if e != nil {
			return tx, proposal, e
		}
		captured[i] = capturedGraphSource{observation: p.observation, root: base, floor: floor, data: v}
		work.Records += q.rows
		work.Bytes += q.bytes
	}
	manifest, err := h.planCrossCaptured(ctx, id, ops, provenance, claims, captured, arena, work)
	if err != nil {
		return tx, proposal, err
	}
	proposal, err = h.crossProposalWithBudget(crossRegister, h.machine.ns.partition, manifest, nil, arena)
	if err != nil {
		return tx, proposal, err
	}
	return crossTransaction{h, manifest, sha256.Sum256(manifest)}, proposal, nil
}

// planCrossCaptured shares the typed planner and private kernel construction.
// It runs without a Host event lock and performs no Submit or remote authority
// construction; its source capabilities have already been checked by the owner.
func (h *allocationHost) planCrossCaptured(ctx context.Context, id requestID, ops []graphstate.Operation, provenance uint64, claims []freshBinding, sources []capturedGraphSource, arena *graphstate.OutputBudget, work graphstore.PageWork) (manifest []byte, err error) {
	l := h.machine.limits
	if ctx == nil || len(sources) != h.machine.declaration.Len() || len(sources) == 0 || arena == nil {
		return nil, errInvalid
	}
	if err := arena.Reserve(512 + 64*len(sources)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	views := make([]graphstore.ApplicationReadSource, len(sources))
	configuration, cfg := sources[0].observation.configuration, sources[0].observation.config
	maxFloor := uint64(0)
	floors := make([]uint64, len(sources))
	for i, source := range sources {
		if source.data == nil || source.observation.kind != observeConfiguration || source.observation.configuration != configuration || source.observation.source.scope.declaration != h.machine.declaration.Digest() || source.observation.source.scope.graph != h.machine.ns.graph || source.observation.source.scope.semantic != h.machine.SemanticContractID() {
			return nil, errInvalid
		}
		views[i], floors[i] = source.data, source.floor
		maxFloor = max(maxFloor, source.floor)
	}
	if maxFloor == math.MaxUint64 {
		return nil, errLimit
	}
	revision, err := state.NewRevision(maxFloor+1, provenance)
	if err != nil {
		return nil, err
	}
	original := partitionGraphCommand{configuration: configuration, declaration: h.machine.declaration.Digest(), routing: cfg.routingDigest, request: graphRequest{ns: h.machine.ns, kind: graphOperations, id: id, revision: revision, operations: ops, claims: claims}}
	wire, err := encodePartitionGraphCommand(original, l)
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(4096 + 8*len(wire)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	b := graphstore.OwnershipBudget{SourceRows: l.sourceRows - work.Records, SourceBytes: l.sourceBytes - work.Bytes, OutputBytes: arena.Remaining()}
	v, w, err := graphstore.OpenPartitionTransactionSources(ctx, views, h.machine.declaration, cfg.defaultAxis, configuration, l.catalog, l.graph, b, arena)
	work.Records += w.Records
	work.Bytes += w.Bytes
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, v.Close())
		if err != nil {
			manifest = nil
		}
	}()
	template, _, err := graphstore.PlanPartitionTransaction(ctx, v, ops, revision)
	if err != nil {
		return nil, err
	}
	parts, err := template.Participants(arena)
	if err != nil {
		return nil, err
	}
	m := crossManifest{tx: crossTransactionID(h.machine.ns, id), coordinator: h.machine.ns.partition, original: wire, configuration: configuration, declaration: h.machine.declaration.Digest(), routing: cfg.routingDigest, template: template.Digest(), parts: make([]crossPart, len(parts))}
	for i, partition := range parts {
		k, e := template.ParticipantKernel(partition, m.tx, arena)
		if e != nil {
			return nil, e
		}
		binding, e := k.Binding(arena)
		if e != nil {
			return nil, e
		}
		kernel, e := k.Bytes(arena)
		if e != nil {
			return nil, e
		}
		floor := uint64(0)
		for j, p := range sources {
			if p.observation.source.scope.partition == partition {
				floor = floors[j]
			}
		}
		m.parts[i] = crossPart{binding: binding, kernel: kernel, floor: floor}
	}
	manifest, err = encodeCrossManifest(m, l)
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(8 * len(manifest)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return manifest, nil
}

func (h *allocationHost) crossProposal(phase crossPhase, partition uint64, manifest []byte, proofs []crossStatus) (allocationProposal, error) {
	arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if err != nil {
		return allocationProposal{}, err
	}
	return h.crossProposalWithBudget(phase, partition, manifest, proofs, arena)
}
func (h *allocationHost) crossProposalWithBudget(phase crossPhase, partition uint64, manifest []byte, proofs []crossStatus, arena *graphstate.OutputBudget) (allocationProposal, error) {
	return h.crossCommandProposalWithBudget(crossCommand{ns: namespace{h.machine.ns.graph, partition}, phase: phase, manifest: manifest, proofs: proofs}, arena)
}

func (h *allocationHost) crossCommandProposalWithBudget(r crossCommand, arena *graphstate.OutputBudget) (allocationProposal, error) {
	if h.unavailableLocked() {
		return allocationProposal{}, replica.ErrStopped
	}
	if h.machine == nil || h.driver == nil {
		return allocationProposal{}, errInvalid
	}
	target, err := h.target(r.ns.partition)
	if err != nil {
		return allocationProposal{}, err
	}
	l := h.machine.limits
	m, err := decodeCrossManifest(r.manifest, l, arena)
	if err != nil {
		return allocationProposal{}, err
	}
	if err := validateCrossPhaseProofs(r, m); err != nil {
		return allocationProposal{}, err
	}
	if err := arena.Reserve(4096 + 8*len(r.manifest) + 256*len(r.proofs)); err != nil {
		return allocationProposal{}, outputBudgetFailure(err)
	}
	wire, err := encodeCrossCommand(r, l)
	if err != nil {
		return allocationProposal{}, err
	}
	return allocationProposal{target: target, wire: wire}, nil
}

func (h *allocationHost) PrepareCrossParticipant(tx crossTransaction, partition uint64, registration crossProof, previous []crossProof) (allocationProposal, error) {
	if h == nil || tx.owner != h {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := checkCrossProof(registration, registration.status.tx, tx.digest); err != nil || !bytes.Equal(registration.manifest, tx.manifest) || registration.status.phase != crossRegister {
		return allocationProposal{}, errors.Join(errInvalid, err)
	}
	proofs := []crossStatus{registration.status}
	for _, p := range previous {
		if err := checkCrossProof(p, registration.status.tx, tx.digest); err != nil {
			return allocationProposal{}, err
		}
		proofs = append(proofs, p.status)
	}
	return h.crossProposal(crossPrepare, partition, tx.manifest, proofs)
}
func (h *allocationHost) DecideCrossTransaction(tx crossTransaction, votes []crossProof) (allocationProposal, error) {
	if h == nil || tx.owner != h {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	proofs := make([]crossStatus, 0, len(votes))
	for _, p := range votes {
		if err := checkCrossProof(p, p.status.tx, tx.digest); err != nil || !bytes.Equal(p.manifest, tx.manifest) {
			return allocationProposal{}, errors.Join(errInvalid, err)
		}
		proofs = append(proofs, p.status)
	}
	return h.crossProposal(crossDecide, h.machine.ns.partition, tx.manifest, proofs)
}
func (h *allocationHost) ResolveCrossParticipant(partition uint64, decision crossProof) (allocationProposal, error) {
	if h == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := checkCrossProof(decision, decision.status.tx, decision.status.digest); err != nil || decision.status.phase != crossDecide {
		return allocationProposal{}, errors.Join(errInvalid, err)
	}
	return h.crossProposal(crossResolve, partition, decision.manifest, []crossStatus{decision.status})
}

// RecoverCrossTransaction resumes the retained original typed template after
// leader loss. A completed coordinator proof binds its identity. It performs
// no planning, remote old-root lookup or revision substitution.
func (h *allocationHost) RecoverCrossTransaction(original crossProof) (crossTransaction, error) {
	if h == nil {
		return crossTransaction{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return crossTransaction{}, replica.ErrStopped
	}
	if err := checkCrossProof(original, original.status.tx, original.status.digest); err != nil || original.status.ns != h.machine.ns {
		return crossTransaction{}, errors.Join(errInvalid, err)
	}
	l := h.machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return crossTransaction{}, err
	}
	manifest, err := decodeCrossManifest(original.manifest, l, arena)
	if err != nil {
		return crossTransaction{}, err
	}
	if manifest.coordinator != h.machine.ns.partition {
		return crossTransaction{}, errInvalid
	}
	if err := arena.Reserve(len(original.manifest)); err != nil {
		return crossTransaction{}, outputBudgetFailure(err)
	}
	return crossTransaction{h, owned(original.manifest), original.status.digest}, nil
}
