package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// A capacity observation is a completed Host query, never a participant vote.
// Only fixed bindings enter the losing coordinator's protected ABORT command.
const crossCapacityObservationMaxBytes = 512

type crossCapacityObservation struct {
	source                                allocationScope
	configuration, routing                [32]byte
	generation, index                     uint64
	imageHash                             [32]byte
	desired, holder                       [16]byte
	desiredDigest, holderDigest           [32]byte
	desiredCoordinator, holderCoordinator uint64
	prepareHash                           [32]byte
	nonce                                 [16]byte
	readID                                allocationReadID
}
type crossCapacityQuestion struct {
	nonce   [16]byte
	hash    [32]byte
	command crossCommand
	limits  graphCapacityLimits // zero preserves the original native allowance
	extra   int                 // bounded remote dispatch/Host decode owners
}
type crossCapacityProof struct {
	owner       *allocationHost
	observation crossCapacityObservation
	binding     [32]byte
	work        graphstore.PageWork
	output      int
	workBinding [32]byte
}
type crossCapacityReply struct {
	id    allocationReadID
	proof crossCapacityProof
	err   error
}

func (o crossCapacityObservation) loser() ([16]byte, [32]byte, uint64) {
	if bytes.Compare(o.desired[:], o.holder[:]) > 0 {
		return o.desired, o.desiredDigest, o.desiredCoordinator
	}
	return o.holder, o.holderDigest, o.holderCoordinator
}
func (o crossCapacityObservation) valid() bool {
	return o.source.valid() && crossGraphAgreement(o.source.semantic) && o.configuration != ([32]byte{}) && o.routing != ([32]byte{}) && o.generation != 0 && o.index != 0 && o.imageHash != ([32]byte{}) && o.desired != ([16]byte{}) && o.holder != ([16]byte{}) && o.desired != o.holder && o.desiredDigest != ([32]byte{}) && o.holderDigest != ([32]byte{}) && o.desiredCoordinator != 0 && o.holderCoordinator != 0 && o.prepareHash != ([32]byte{}) && o.nonce != ([16]byte{}) && o.readID.session != ([16]byte{}) && o.readID.sequence != 0
}
func (o crossCapacityObservation) checkManifest(m crossManifest) error {
	tx, digest, coordinator := o.loser()
	if !o.valid() || tx != m.tx || digest != m.digest || coordinator != m.coordinator || o.source.graph != m.request.ns.graph || o.configuration != m.configuration || o.source.declaration != m.declaration || o.routing != m.routing {
		return errInvalid
	}
	if _, found := m.part(o.source.partition); !found {
		return errInvalid
	}
	return nil
}
func (m *declaredMaterializer) checkCrossCapacityBindings(o *crossCapacityObservation, manifest crossManifest) error {
	if o == nil {
		return errInvalid
	}
	if err := o.checkManifest(manifest); err != nil {
		return err
	}
	p, found := m.declaration.Partition(o.source.partition)
	if !found || p.Group != o.source.group || p.OwnershipEpoch != o.source.ownership || o.source.topology != m.declaration.TopologyEpoch() {
		return errInvalid
	}
	if _, found := m.declaration.Partition(o.desiredCoordinator); !found {
		return errInvalid
	}
	if _, found := m.declaration.Partition(o.holderCoordinator); !found {
		return errInvalid
	}
	return nil
}
func emitCrossCapacityObservation(w *boundedWriter, o crossCapacityObservation) {
	w.add(appendAllocationScope(nil, o.source))
	w.add(o.configuration[:])
	w.add(o.routing[:])
	w.u64(o.generation)
	w.u64(o.index)
	w.add(o.imageHash[:])
	w.add(o.desired[:])
	w.add(o.desiredDigest[:])
	w.u64(o.desiredCoordinator)
	w.add(o.holder[:])
	w.add(o.holderDigest[:])
	w.u64(o.holderCoordinator)
	w.add(o.prepareHash[:])
	w.add(o.nonce[:])
	w.add(o.readID.session[:])
	w.u64(o.readID.sequence)
}
func readCrossCapacityObservation(c *graphCursor) crossCapacityObservation {
	o := crossCapacityObservation{}
	o.source.graph = idalloc.GraphID(c.array())
	o.source.partition = c.u64()
	o.source.group = c.array()
	o.source.ownership = c.u64()
	o.source.topology = c.u64()
	copy(o.source.declaration[:], c.take(32))
	copy(o.source.semantic[:], c.take(32))
	copy(o.configuration[:], c.take(32))
	copy(o.routing[:], c.take(32))
	o.generation = c.u64()
	o.index = c.u64()
	copy(o.imageHash[:], c.take(32))
	o.desired = c.array()
	copy(o.desiredDigest[:], c.take(32))
	o.desiredCoordinator = c.u64()
	o.holder = c.array()
	copy(o.holderDigest[:], c.take(32))
	o.holderCoordinator = c.u64()
	copy(o.prepareHash[:], c.take(32))
	o.nonce = c.array()
	o.readID.session = c.array()
	o.readID.sequence = c.u64()
	if !o.valid() {
		c.err = errCorrupt
	}
	return o
}
func encodeCrossCapacityObservation(o crossCapacityObservation) ([]byte, error) {
	if !o.valid() {
		return nil, errInvalid
	}
	return boundedEncoding(crossCapacityObservationMaxBytes, func(w *boundedWriter) { w.add([]byte{'G', 'C', 'F', 1}); emitCrossCapacityObservation(w, o) })
}
func crossResourceOwnerKey(n namespace) []byte { return recordKey(n, 13) }
func writeCrossResourceOwner(q *reader, tx [16]byte, digest [32]byte) error {
	if err := reserveComposer(q, 512); err != nil {
		return err
	}
	b := append([]byte{'G', 'C', 'O', 1}, tx[:]...)
	b = append(b, digest[:]...)
	return q.put(crossResourceOwnerKey(q.ns), seal(b))
}
func (m *declaredMaterializer) readCrossResourceOwner(q *reader) (crossManifest, bool, error) {
	remaining := raftlog.ReadBudget{Rows: min(q.view.ReadLimits().Rows, q.limits.readRows-q.rows), Bytes: min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)}
	if remaining.Rows < 1 || remaining.Bytes < 1 {
		return crossManifest{}, false, errLimit
	}
	owner, work, err := q.view.RecoveryOwner(q.ctx, remaining)
	if charged := chargeGraphWork(q, graphstore.PageWork{Records: work.Records, Bytes: work.Bytes}); charged != nil {
		return crossManifest{}, false, errors.Join(charged, err)
	}
	if err != nil {
		return crossManifest{}, false, err
	}
	if owner.ID != ([32]byte{}) && m.rangeMoves() {
		move, err := m.moveOwnsRecovery(q, owner)
		if err != nil {
			return crossManifest{}, false, err
		}
		if move {
			return crossManifest{}, false, nil
		}
	}
	pointer, found, err := q.getBounded(crossResourceOwnerKey(q.ns), 84)
	if err != nil {
		return crossManifest{}, false, err
	}
	if owner.ID == ([32]byte{}) {
		if found && len(pointer) != 0 {
			return crossManifest{}, false, errCorrupt
		}
		return crossManifest{}, false, nil
	}
	body, err := graphBody(pointer, "GCO\x01", 84)
	if err != nil || !found || len(body) != 48 {
		return crossManifest{}, false, errors.Join(errCorrupt, err)
	}
	var tx [16]byte
	var digest [32]byte
	copy(tx[:], body[:16])
	copy(digest[:], body[16:])
	if tx == ([16]byte{}) || digest != owner.Digest || crossRecoveryID(q.ns, tx) != owner.ID {
		return crossManifest{}, false, errCorrupt
	}
	r, found, err := crossControl(q, crossControlKey(q.ns, tx, 'R'), m.limits.commandBytes)
	if err != nil || !found || sha256.Sum256(r.Value) != digest {
		return crossManifest{}, false, errors.Join(errCorrupt, err)
	}
	if q.arena == nil {
		q.arena, err = graphstate.NewOutputBudget(m.limits.outputBytes)
		if err != nil {
			return crossManifest{}, false, err
		}
	}
	manifest, err := m.crossManifest(q, r.Value)
	return manifest, err == nil, err
}

// ReadCrossCapacityConflict asks this participant's quorum for a completed,
// request-bound observation of a different applied completion owner. It creates
// no vote, decision, reservation or graph effect.
func (h *allocationHost) ReadCrossCapacityConflict(prepare allocationProposal, nonce [16]byte) (allocationEvent, error) {
	return h.readCrossCapacityConflict(prepare, nonce, graphCapacityLimits{})
}

// The remote embedding grants a bounded child of its existing invocation. The
// ordinary public door selects precisely its previous machine policy.
func (h *allocationHost) readCrossCapacityConflict(prepare allocationProposal, nonce [16]byte, grant graphCapacityLimits) (allocationEvent, error) {
	if h == nil || nonce == ([16]byte{}) {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return allocationEvent{}, replica.ErrStopped
	}
	if h.machine == nil || h.driver == nil || prepare.target != h.machine.scope() || !crossGraphAgreement(h.machine.SemanticContractID()) {
		return allocationEvent{}, errInvalid
	}
	if grant != (graphCapacityLimits{}) && (!grant.valid(h.machine.limits) || 8192+16*len(prepare.wire) > grant.output) {
		return allocationEvent{}, errLimit
	}
	r, _, err := decodeCrossCommand(prepare.wire, h.machine.limits)
	if err != nil || r.ns != h.machine.ns || r.phase != crossPrepare || r.conflict != nil {
		return allocationEvent{}, errors.Join(errInvalid, err)
	}
	if h.sequence == math.MaxUint64 || h.pendingReadCount() >= 16 {
		return allocationEvent{}, errLimit
	}
	if h.capacityPending == nil {
		h.capacityPending = make(map[allocationReadID]crossCapacityQuestion)
	}
	h.sequence++
	h.issuedReads.Store(h.sequence)
	id := allocationReadID{h.session, h.sequence}
	question := crossCapacityQuestion{nonce: nonce, hash: sha256.Sum256(prepare.wire), command: r, limits: grant}
	if grant != (graphCapacityLimits{}) {
		question.extra = 8192 + 16*len(prepare.wire)
	}
	h.capacityPending[id] = question
	context := append([]byte{'G', 'C', 'C', 1}, id.session[:]...)
	context = binary.BigEndian.AppendUint64(context, id.sequence)
	context = append(context, question.hash[:]...)
	context = append(context, nonce[:]...)
	out, err := h.driver.ReadIndex(context)
	if err != nil {
		delete(h.capacityPending, id)
		return allocationEvent{}, err
	}
	e, err := h.event(out, nil)
	e.readID = id
	return e, err
}
func (h *allocationHost) completeCrossCapacityRead(wire []byte, index uint64) (crossCapacityReply, error) {
	if len(wire) != 76 || !bytes.Equal(wire[:4], []byte{'G', 'C', 'C', 1}) {
		return crossCapacityReply{}, errCorrupt
	}
	var id allocationReadID
	copy(id.session[:], wire[4:20])
	id.sequence = binary.BigEndian.Uint64(wire[20:28])
	q, found := h.capacityPending[id]
	if !found || id.session != h.session || !bytes.Equal(wire[28:60], q.hash[:]) || !bytes.Equal(wire[60:], q.nonce[:]) {
		return crossCapacityReply{}, errInvalid
	}
	delete(h.capacityPending, id)
	o, work, output, err := h.machine.observeCrossCapacity(q, index)
	if err != nil {
		return crossCapacityReply{id: id, err: err}, nil
	}
	o.readID = id
	encoded, err := encodeCrossCapacityObservation(o)
	if err != nil {
		return crossCapacityReply{}, err
	}
	usage, err := encodeCapacityAuthorityBody(crossCapacityAuthority{observation: o, work: work, output: output})
	if err != nil {
		return crossCapacityReply{}, err
	}
	return crossCapacityReply{id: id, proof: crossCapacityProof{owner: h, observation: o, binding: sha256.Sum256(encoded), work: work, output: output, workBinding: sha256.Sum256(usage)}}, nil
}
func (m *declaredMaterializer) observeCrossCapacity(question crossCapacityQuestion, index uint64) (observation crossCapacityObservation, work graphstore.PageWork, output int, err error) {
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return observation, work, output, err
	}
	defer func() {
		err = errors.Join(err, v.Close())
		if err != nil {
			observation = crossCapacityObservation{}
		}
	}()
	base, err := v.RootBounded(context.Background(), 172)
	if err != nil {
		return observation, work, output, err
	}
	l := m.limits.allocation
	l.readRows = min(l.readRows, m.limits.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes)
	policy := m.limits
	if question.limits != (graphCapacityLimits{}) {
		if !question.limits.valid(m.limits) {
			return observation, work, output, errInvalid
		}
		l.readRows = min(l.readRows, question.limits.rows)
		l.readBytes = min(l.readBytes, question.limits.bytes)
		policy.outputBytes = question.limits.output
	}
	q := reader{ctx: context.Background(), view: v, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	defer func() {
		work = graphstore.PageWork{Records: q.rows, Bytes: q.bytes}
		if q.arena != nil {
			output = q.arena.Used()
		}
	}()
	if question.limits != (graphCapacityLimits{}) && 8192+8*(len(question.command.manifest)+4096+256*len(question.command.proofs))+question.extra > policy.outputBytes {
		return observation, work, output, errLimit
	}
	_, decoded, err := decodeCrossCommandMustBound(question, m.limits)
	if err != nil {
		return observation, work, output, err
	}
	if err := joinGraphOutputBudget(&q, graphRequest{}, decoded+question.extra, policy); err != nil {
		return observation, work, output, err
	}
	desired, err := m.crossManifest(&q, question.command.manifest)
	if err != nil {
		return observation, work, output, err
	}
	if err := validateCrossPhaseProofs(question.command, desired); err != nil {
		return observation, work, output, err
	}
	holder, found, err := m.readCrossResourceOwner(&q)
	if err != nil {
		return observation, work, output, err
	}
	if !found || holder.tx == desired.tx {
		return observation, work, output, replica.ErrUnavailable
	}
	return crossCapacityObservation{source: m.scope(), configuration: desired.configuration, routing: desired.routing, generation: base.Generation, index: base.Index, imageHash: base.ImageHash, desired: desired.tx, desiredDigest: desired.digest, desiredCoordinator: desired.coordinator, holder: holder.tx, holderDigest: holder.digest, holderCoordinator: holder.coordinator, prepareHash: question.hash, nonce: question.nonce}, work, output, nil
}
func decodeCrossCommandMustBound(q crossCapacityQuestion, l materializerLimits) (crossCommand, int, error) {
	wire, err := encodeCrossCommand(q.command, l)
	if err != nil {
		return crossCommand{}, 0, err
	}
	if sha256.Sum256(wire) != q.hash {
		return crossCommand{}, 0, errInvalid
	}
	return decodeCrossCommand(wire, l)
}

// AbortCrossTransactionForCapacity is owned by the bytewise larger TxID's
// coordinator. The source observation is neither an unavailable-quorum result
// nor a durable NO; replay validates fixed bindings without a remote reread.
func (h *allocationHost) AbortCrossTransactionForCapacity(tx crossTransaction, proof crossCapacityProof) (allocationProposal, crossStatus, error) {
	if h == nil || tx.owner != h || proof.owner == nil {
		return allocationProposal{}, crossStatus{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return allocationProposal{}, crossStatus{}, replica.ErrStopped
	}
	if h.machine == nil || h.driver == nil || !crossGraphAgreement(h.machine.SemanticContractID()) {
		return allocationProposal{}, crossStatus{}, errInvalid
	}
	o := proof.observation
	if !o.valid() || proof.owner.machine == nil || proof.owner.session != o.readID.session || o.readID.sequence > proof.owner.issuedReads.Load() || proof.owner.machine.scope() != o.source {
		return allocationProposal{}, crossStatus{}, errInvalid
	}
	encoded, err := encodeCrossCapacityObservation(o)
	if err != nil || sha256.Sum256(encoded) != proof.binding {
		return allocationProposal{}, crossStatus{}, errors.Join(errInvalid, err)
	}
	arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if err != nil {
		return allocationProposal{}, crossStatus{}, err
	}
	m, err := decodeCrossManifest(tx.manifest, h.machine.limits, arena)
	if err != nil {
		return allocationProposal{}, crossStatus{}, err
	}
	if tx.digest != m.digest || m.coordinator != h.machine.ns.partition {
		return allocationProposal{}, crossStatus{}, errInvalid
	}
	if err := h.machine.checkCrossCapacityBindings(&o, m); err != nil {
		return allocationProposal{}, crossStatus{}, err
	}
	current, manifest, err := h.machine.observeCrossTransaction(crossQuery{tx: m.tx}, h.driver.Applied())
	if err != nil || !bytes.Equal(manifest, tx.manifest) || current.digest != m.digest {
		return allocationProposal{}, crossStatus{}, errors.Join(errInvalid, err)
	}
	if current.phase >= crossDecide {
		decision, _, err := h.machine.observeCrossTransaction(crossQuery{tx: m.tx, phase: crossDecide}, h.driver.Applied())
		return allocationProposal{}, decision, err
	}
	p, err := h.crossCommandProposalWithBudget(crossCommand{ns: h.machine.ns, phase: crossDecide, manifest: tx.manifest, conflict: new(o)}, arena)
	if err != nil {
		return allocationProposal{}, crossStatus{}, err
	}
	r, err := h.machine.crossReservation(m, crossRegister)
	if err != nil {
		return allocationProposal{}, crossStatus{}, err
	}
	for _, credit := range r.Credits {
		if credit.Phase == uint32(crossDecide) && len(p.wire) <= credit.CommandBytes {
			return p, crossStatus{}, nil
		}
	}
	return allocationProposal{}, crossStatus{}, errLimit
}
