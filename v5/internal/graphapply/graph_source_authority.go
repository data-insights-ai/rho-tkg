package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type crossRequestQuestion struct{ tx, nonce [16]byte }
type crossRequestProof struct {
	owner         *allocationHost
	id            allocationReadID
	question      crossRequestQuestion
	scope         allocationScope
	configuration [32]byte
	index         uint64
	status        crossStatus
	manifest      []byte
	present       bool // native completed point lookup, never transport/caller authority
}
type crossRequestReply struct {
	proof crossRequestProof
	err   error
}

func encodeCrossRequestContext(id allocationReadID, q crossRequestQuestion) []byte {
	b := append([]byte{'G', 'Q', 'R', 1}, id.session[:]...)
	b = binary.BigEndian.AppendUint64(b, id.sequence)
	b = append(b, q.tx[:]...)
	return append(b, q.nonce[:]...)
}

// readCrossRequest performs authoritative lookup. A completed absence is distinct
// from a missing/unavailable ReadCrossPhase result and is scoped to one TxID.
func (h *allocationHost) readCrossRequest(q crossRequestQuestion) (allocationEvent, error) {
	if h == nil || q.tx == ([16]byte{}) || q.nonce == ([16]byte{}) {
		return allocationEvent{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationEvent{}, replica.ErrStopped
	}
	if !crossGraphAgreement(h.machine.SemanticContractID()) {
		return allocationEvent{}, errInvalid
	}
	if h.sequence == math.MaxUint64 || len(h.pending)+len(h.crossPending)+len(h.capacityPending)+len(h.cutPending)+len(h.requestPending) >= 16 {
		return allocationEvent{}, errLimit
	}
	if h.requestPending == nil {
		h.requestPending = make(map[allocationReadID]crossRequestQuestion)
	}
	h.sequence++
	h.issuedReads.Store(h.sequence)
	id := allocationReadID{h.session, h.sequence}
	h.requestPending[id] = q
	out, err := h.driver.ReadIndex(encodeCrossRequestContext(id, q))
	if err != nil {
		delete(h.requestPending, id)
		delete(h.cutPending, id)
		return allocationEvent{}, err
	}
	e, err := h.event(out, nil)
	e.readID = id
	return e, err
}
func (h *allocationHost) completeCrossRequestRead(wire []byte, index uint64) (crossRequestReply, error) {
	if len(wire) != 60 || !bytes.Equal(wire[:4], []byte{'G', 'Q', 'R', 1}) {
		return crossRequestReply{}, errCorrupt
	}
	var id allocationReadID
	copy(id.session[:], wire[4:20])
	id.sequence = binary.BigEndian.Uint64(wire[20:28])
	q, ok := h.requestPending[id]
	if !ok || id.session != h.session || !bytes.Equal(wire, encodeCrossRequestContext(id, q)) {
		return crossRequestReply{}, errInvalid
	}
	delete(h.requestPending, id)
	delete(h.cutPending, id)
	proof, err := h.machine.observeCrossRequest(context.Background(), q, index)
	if err != nil {
		return crossRequestReply{err: err}, nil
	}
	proof.owner, proof.id = h, id
	return crossRequestReply{proof: proof}, nil
}
func (m *declaredMaterializer) observeCrossRequest(ctx context.Context, question crossRequestQuestion, index uint64) (proof crossRequestProof, err error) {
	view, err := m.store.ApplicationView(index)
	if err != nil {
		return proof, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			proof = crossRequestProof{}
		}
	}()
	base, err := view.RootBounded(ctx, 172)
	if err != nil {
		return proof, err
	}
	l := m.limits.allocation
	l.readRows = min(l.readRows, m.limits.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: ctx, view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	cfg, err := m.initialized(&q)
	if err != nil {
		return proof, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return proof, err
	}
	status, present, err := crossCurrent(&q, question.tx)
	if err != nil {
		return proof, err
	}
	proof = crossRequestProof{question: question, scope: m.scope(), configuration: configuration, index: index, status: status, present: present}
	if present {
		record, found, err := crossControl(&q, crossControlKey(m.ns, question.tx, 'R'), m.limits.commandBytes)
		if err != nil || !found {
			return crossRequestProof{}, errors.Join(errCorrupt, err)
		}
		if sha256.Sum256(record.Value) != status.digest {
			return crossRequestProof{}, errCorrupt
		}
		proof.manifest = record.Value
	} else {
		// No negative inference from missing transport/control replies. The actual
		// recovery ledger must not contain this TxID without its registration.
		owner, held, err := m.readCrossResourceOwner(&q)
		if err != nil {
			return crossRequestProof{}, err
		}
		if held && owner.tx == question.tx {
			return crossRequestProof{}, errCorrupt
		}
	}
	return proof, nil
}

func (h *allocationHost) cancelSourceRead(id allocationReadID) error {
	if h == nil {
		return errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return replica.ErrStopped
	}
	var wire []byte
	if q, ok := h.pending[id]; ok {
		wire = encodeAllocationReadContext(id, q)
	} else if q, ok := h.crossPending[id]; ok {
		wire = encodeCrossReadContext(id, q)
	} else if q, ok := h.cutPending[id]; ok {
		wire = encodeGraphCutContext(id, q)
	} else if q, ok := h.requestPending[id]; ok {
		wire = encodeCrossRequestContext(id, q)
	} else {
		return errInvalid
	}
	if err := h.driver.CancelReadIndex(wire); err != nil {
		return err
	}
	delete(h.pending, id)
	delete(h.crossPending, id)
	delete(h.requestPending, id)
	delete(h.cutPending, id)
	return nil
}

// crossAuthority is constructed by native export or matched authenticated
// endpoint completion, never by decoding caller bytes or fake Host ownership.
type crossAuthorityBody struct {
	scope    allocationScope
	id       allocationReadID
	question crossQuery
	index    uint64
	status   crossStatus
	manifest []byte
}
type crossAuthority struct {
	crossAuthorityBody
	stamp *crossAuthorityStamp
}
type crossAuthorityStamp struct {
	scope    allocationScope
	id       allocationReadID
	question crossQuery
	index    uint64
	status   crossStatus
	manifest [32]byte
}

// Only native export and authenticated endpoint completion call this seal.
// Wire decoders return the inert body, never the provenance capability.
func sealCrossAuthority(p crossAuthorityBody) crossAuthority {
	return crossAuthority{crossAuthorityBody: p, stamp: &crossAuthorityStamp{p.scope, p.id, p.question, p.index, p.status, sha256.Sum256(p.manifest)}}
}
func (p crossAuthority) validSeal() bool {
	s := p.stamp
	return s != nil && s.scope == p.scope && s.id == p.id && s.question == p.question && s.index == p.index && s.status == p.status && s.manifest == sha256.Sum256(p.manifest)
}

func (h *allocationHost) exportCrossAuthority(p crossProof) (crossAuthority, error) {
	if h == nil {
		return crossAuthority{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return crossAuthority{}, replica.ErrStopped
	}
	if p.owner != h || p.index == 0 || p.status.index > p.index || p.index > h.driver.Applied() {
		return crossAuthority{}, errInvalid
	}
	if err := checkCrossProof(p, p.status.tx, p.status.digest); err != nil {
		return crossAuthority{}, err
	}
	return sealCrossAuthority(crossAuthorityBody{h.machine.scope(), p.id, p.question, p.index, p.status, owned(p.manifest)}), nil
}

// proposalFromAuthority admits already verified evidence through the SAME
// native phase validation and proposal builder. It acquires no remote source
// and holds the Host lock only for local checks/encoding, never an RPC.
func (h *allocationHost) proposalFromAuthority(phase crossPhase, partition uint64, tx crossTransaction, evidence []crossAuthority, arena *graphstate.OutputBudget) (allocationProposal, error) {
	if h == nil || arena == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationProposal{}, replica.ErrStopped
	}
	if tx.owner != h || sha256.Sum256(tx.manifest) != tx.digest {
		return allocationProposal{}, errInvalid
	}
	if len(evidence) > h.machine.declaration.Len()+1 {
		return allocationProposal{}, errLimit
	}
	if err := arena.Reserve(128 + 256*len(evidence)); err != nil {
		return allocationProposal{}, outputBudgetFailure(err)
	}
	statuses := make([]crossStatus, len(evidence))
	for i, p := range evidence {
		if !p.validSeal() || p.scope.declaration != h.machine.declaration.Digest() || p.scope.graph != h.machine.ns.graph || p.scope.semantic != h.machine.SemanticContractID() || p.id.session == ([16]byte{}) || p.id.sequence == 0 || p.question.nonce == ([16]byte{}) || p.index < p.status.index || p.question.tx != p.status.tx || p.status.digest != tx.digest || !bytes.Equal(p.manifest, tx.manifest) {
			return allocationProposal{}, errInvalid
		}
		part, found := h.machine.declaration.Partition(p.scope.partition)
		if !found || p.scope.group != part.Group || p.scope.ownership != part.OwnershipEpoch || p.scope.topology != h.machine.declaration.TopologyEpoch() || p.status.ns != (namespace{h.machine.ns.graph, p.scope.partition}) {
			return allocationProposal{}, errInvalid
		}
		statuses[i] = p.status
	}
	return h.crossProposalWithBudget(phase, partition, tx.manifest, statuses, arena)
}
