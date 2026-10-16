package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

// Optional embedding capability. Ordinary Cross reads retain their empty
// command and original pump API; an unavailable capability never proves a NO.
type graphHostCapacityPump interface {
	Capacity(context.Context, allocationProposal, [16]byte, graphCapacityLimits) (crossCapacityReply, error)
}

type graphCapacityLimits struct{ rows, bytes, output int }

func (l graphCapacityLimits) valid(m materializerLimits) bool {
	return l.rows > 0 && l.rows <= m.sourceRows && l.bytes > 0 && l.bytes <= m.sourceBytes && l.output > 0 && l.output <= m.outputBytes
}

// The complete question is inert. Only the actual participant ReadIndex
// completion and authenticated endpoint correspondence can grant authority.
func encodeCapacityQuestion(p allocationProposal, l graphCapacityLimits) ([]byte, error) {
	if l.rows < 1 || l.rows > 65536 || l.bytes < 1 || l.bytes > 64<<20 || l.output < 1 || l.output > 64<<20 {
		return nil, errInvalid
	}
	proposal, err := encodeGraphSourceProposal(p)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(1<<20, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'Q', 1})
		w.field(proposal)
		w.u32(l.rows)
		w.u32(l.bytes)
		w.u32(l.output)
	})
}

func decodeCapacityQuestion(wire []byte, limits materializerLimits) (allocationProposal, graphCapacityLimits, error) {
	body, err := graphBody(wire, "GCQ\x01", 1<<20)
	if err != nil {
		return allocationProposal{}, graphCapacityLimits{}, err
	}
	c := graphCursor{b: body}
	proposal := c.field(1 << 20)
	l := graphCapacityLimits{c.count(limits.sourceRows, 0), c.count(limits.sourceBytes, 0), c.count(limits.outputBytes, 0)}
	if c.err != nil || len(c.b) != 0 || !l.valid(limits) {
		return allocationProposal{}, l, errors.Join(errInvalid, c.err)
	}
	if 8192+16*len(proposal) > l.output {
		return allocationProposal{}, l, errLimit
	}
	p, err := decodeGraphSourceProposal(proposal)
	return p, l, err
}

type crossCapacityAuthority struct {
	observation crossCapacityObservation
	work        graphstore.PageWork
	output      int
	stamp       *crossCapacityAuthorityStamp
}
type crossCapacityAuthorityStamp struct {
	observation crossCapacityObservation
	work        graphstore.PageWork
	output      int
}

func (p crossCapacityAuthority) validSeal() bool {
	s := p.stamp
	return s != nil && s.observation == p.observation && s.work == p.work && s.output == p.output
}

func encodeCapacityAuthorityBody(p crossCapacityAuthority) ([]byte, error) {
	wire, err := encodeCrossCapacityObservation(p.observation)
	if err != nil || p.work.Records < 0 || p.work.Bytes < 0 || p.output < 0 {
		return nil, errors.Join(errInvalid, err)
	}
	return boundedEncoding(1024, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'A', 1})
		w.field(wire)
		w.u32(p.work.Records)
		w.u32(p.work.Bytes)
		w.u32(p.output)
	})
}

// Decoding remains inert: no stamp is obtained from a body supplied by a caller.
func decodeCapacityAuthorityBody(wire []byte, limits graphCapacityLimits) (crossCapacityAuthority, error) {
	body, err := graphBody(wire, "GCA\x01", 1024)
	if err != nil {
		return crossCapacityAuthority{}, err
	}
	c := graphCursor{b: body}
	observation := c.field(crossCapacityObservationMaxBytes)
	p := crossCapacityAuthority{work: graphstore.PageWork{Records: c.count(limits.rows, 0), Bytes: c.count(limits.bytes, 0)}, output: c.count(limits.output, 0)}
	if c.err != nil || len(c.b) != 0 {
		return crossCapacityAuthority{}, errors.Join(errCorrupt, c.err)
	}
	b, err := graphBody(observation, "GCF\x01", crossCapacityObservationMaxBytes)
	if err != nil {
		return crossCapacityAuthority{}, err
	}
	inner := graphCursor{b: b}
	p.observation = readCrossCapacityObservation(&inner)
	if inner.err != nil || len(inner.b) != 0 {
		return crossCapacityAuthority{}, errors.Join(errCorrupt, inner.err)
	}
	return p, nil
}

func (h *allocationHost) exportCapacityAuthority(proof crossCapacityProof, nonce [16]byte, prepareHash [32]byte, limits graphCapacityLimits) (crossCapacityAuthority, error) {
	if h == nil || h.machine == nil || !limits.valid(h.machine.limits) {
		return crossCapacityAuthority{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return crossCapacityAuthority{}, replica.ErrStopped
	}
	o := proof.observation
	if proof.owner != h || o.source != h.machine.scope() || o.readID.session != h.session || o.readID.sequence == 0 || o.readID.sequence > h.issuedReads.Load() || o.index > h.driver.Applied() || o.nonce != nonce || o.prepareHash != prepareHash || proof.work.Records < 0 || proof.work.Bytes < 0 || proof.output < 0 || proof.work.Records > limits.rows || proof.work.Bytes > limits.bytes || proof.output > limits.output {
		return crossCapacityAuthority{}, errInvalid
	}
	encoded, err := encodeCrossCapacityObservation(o)
	if err != nil || sha256.Sum256(encoded) != proof.binding {
		return crossCapacityAuthority{}, errors.Join(errInvalid, err)
	}
	authority := crossCapacityAuthority{observation: o, work: proof.work, output: proof.output}
	usage, err := encodeCapacityAuthorityBody(authority)
	if err != nil || sha256.Sum256(usage) != proof.workBinding {
		return crossCapacityAuthority{}, errors.Join(errInvalid, err)
	}
	return authority, nil
}

func (s *graphPeerService) readCapacity(ctx context.Context, r graphSourceRequest) ([]byte, error) {
	pump, ok := s.pump.(graphHostCapacityPump)
	if !ok {
		return nil, replica.ErrUnavailable
	}
	p, limits, err := decodeCapacityQuestion(r.command, s.host.machine.limits)
	if err != nil || p.target != s.peer.scope || r.phase != crossPrepare {
		return nil, errors.Join(errInvalid, err)
	}
	command, _, err := decodeCrossCommand(p.wire, s.host.machine.limits)
	if err != nil || command.phase != crossPrepare || command.ns != (namespace{s.peer.scope.graph, s.peer.scope.partition}) {
		return nil, errors.Join(errInvalid, err)
	}
	// The native observer authenticates the desired manifest and its actual owner;
	// this frame supplies no vote, reservation or decision.
	if len(command.manifest) < 20 || command.conflict != nil {
		return nil, errInvalid
	}
	var reply crossCapacityReply
	reply, err = pump.Capacity(ctx, p, r.nonce, limits)
	if err == nil {
		err = reply.err
	}
	if err != nil {
		return nil, err
	}
	if reply.id != reply.proof.observation.readID {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	authority, err := s.host.exportCapacityAuthority(reply.proof, r.nonce, sha256.Sum256(p.wire), limits)
	if err != nil || authority.observation.desired != r.tx {
		return nil, errors.Join(errInvalid, err)
	}
	return encodeCapacityAuthorityBody(authority)
}

func (e *graphAuthenticatedEndpoint) capacity(ctx context.Context, p allocationProposal, tx [16]byte, limits graphCapacityLimits) (crossCapacityAuthority, error) {
	if e == nil || ctx == nil || e.budget == nil || e.budget.output == nil || p.target != e.peer.scope {
		return crossCapacityAuthority{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return crossCapacityAuthority{}, err
	}
	if err := e.budget.reserveCapacityNative(limits); err != nil {
		return crossCapacityAuthority{}, err
	}
	if err := e.budget.output.Reserve(4096 + 8*len(p.wire)); err != nil {
		return crossCapacityAuthority{}, outputBudgetFailure(err)
	}
	question, err := encodeCapacityQuestion(p, limits)
	if err != nil {
		return crossCapacityAuthority{}, err
	}
	r, err := e.call(ctx, graphSourceRequest{kind: sourceCrossRead, tx: tx, phase: crossPrepare, command: question}, 2048)
	if err != nil {
		return crossCapacityAuthority{}, err
	}
	authority, err := decodeCapacityAuthorityBody(r.body, limits)
	if err != nil {
		return crossCapacityAuthority{}, err
	}
	o := authority.observation
	if o.source != e.peer.scope || o.readID.session != r.issuer || o.nonce != r.nonce || o.prepareHash != sha256.Sum256(p.wire) || o.desired != tx {
		return crossCapacityAuthority{}, errInvalid
	}
	// The private child was prepaid on the parent's cumulative native-work ledger.
	authority.stamp = &crossCapacityAuthorityStamp{o, authority.work, authority.output}
	return authority, nil
}

func (h *allocationHost) proposalFromCapacityAuthority(tx crossTransaction, authority crossCapacityAuthority, arena *graphstate.OutputBudget) (allocationProposal, error) {
	if h == nil || h.machine == nil || arena == nil || !authority.validSeal() || tx.owner != h || sha256.Sum256(tx.manifest) != tx.digest {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return allocationProposal{}, replica.ErrStopped
	}
	manifest, err := decodeCrossManifest(tx.manifest, h.machine.limits, arena)
	if err != nil || manifest.digest != tx.digest {
		return allocationProposal{}, errors.Join(errInvalid, err)
	}
	o := authority.observation
	if o.source.semantic != h.machine.SemanticContractID() {
		return allocationProposal{}, errInvalid
	}
	if err := h.machine.checkCrossCapacityBindings(&o, manifest); err != nil {
		return allocationProposal{}, err
	}
	// The foreign coordinator's genuine retained lookup precedes this builder;
	// its reducer keeps any existing/racing durable decision immutable.
	return h.crossCommandProposalWithBudget(crossCommand{ns: namespace{h.machine.ns.graph, manifest.coordinator}, phase: crossDecide, manifest: tx.manifest, conflict: new(o)}, arena)
}

// Bounded helping closes only the globally larger original. Unknown authority
// is pending, never a timeout vote. The caller may retry its same Prepare once.
func (c *crossProcessCoordinator) recoverCapacity(ctx context.Context, desired crossTransaction, manifest crossManifest, prepare allocationProposal, endpoints map[uint64]*graphAuthenticatedEndpoint, budget *graphRemoteBudget) (ownDecision crossAuthority, err error) {
	if c == nil || c.host == nil || c.host.machine == nil || ctx == nil || desired.owner != c.host || sha256.Sum256(desired.manifest) != manifest.digest || endpoints[prepare.target.partition] == nil {
		return ownDecision, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return ownDecision, err
	}
	probe, err := budget.capacityChild(len(manifest.parts), len(desired.manifest), 1, len(prepare.wire)+8192, graphCapacityLimits{rows: 128, bytes: 128 << 10, output: 1 << 20})
	if err != nil {
		return ownDecision, err
	}
	peer := *endpoints[prepare.target.partition]
	peer.budget = probe
	authority, err := peer.capacity(ctx, prepare, manifest.tx, graphCapacityLimits{rows: 128, bytes: 128 << 10, output: 1 << 20})
	if err != nil {
		return ownDecision, err
	}
	o := authority.observation
	if o.desiredDigest != manifest.digest || o.desiredCoordinator != manifest.coordinator || o.configuration != manifest.configuration || o.routing != manifest.routing {
		return ownDecision, errInvalid
	}
	txID, digest, coordinator := o.loser()
	if endpoints[coordinator] == nil {
		return ownDecision, errCrossProcessPending
	}
	lookupBudget, err := budget.capacityChild(len(manifest.parts), len(desired.manifest), 1, c.host.machine.limits.commandBytes+4096, graphCapacityLimits{})
	if err != nil {
		return ownDecision, err
	}
	owner := *endpoints[coordinator]
	owner.budget = lookupBudget
	lookup, err := owner.lookupCapacityOriginal(ctx, txID, c.host.machine.limits.commandBytes)
	if err != nil || !lookup.present {
		return ownDecision, errors.Join(errCrossProcessPending, err)
	}
	if lookup.configuration != manifest.configuration || lookup.status.digest != digest || sha256.Sum256(lookup.manifest) != digest {
		return ownDecision, errInvalid
	}
	if txID == manifest.tx && !bytes.Equal(lookup.manifest, desired.manifest) {
		return ownDecision, errInvalid
	}
	// The holder lookup already prepaid bounded decode ownership. Inspect its
	// authentic footprint before sizing any secondary phases; its participant
	// count need not match the blocked original's count.
	decodeArena, ok := lookupBudget.output.(*graphstate.OutputBudget)
	if !ok {
		return ownDecision, errInvalid
	}
	loser, err := decodeCrossManifest(lookup.manifest, c.host.machine.limits, decodeArena)
	if err != nil || loser.tx != txID || loser.coordinator != coordinator || loser.configuration != manifest.configuration {
		return ownDecision, errors.Join(errInvalid, err)
	}
	if err := o.checkManifest(loser); err != nil {
		return ownDecision, err
	}
	if len(loser.parts) < 1 || len(loser.parts) > 16 {
		return ownDecision, errLimit
	}
	// Original completion headroom stays on the shared parent. This separate
	// closure is funded using ONLY the actual retained holder's footprint.
	closure, err := budget.capacityChild(len(manifest.parts), len(desired.manifest), 4*len(loser.parts)+6, len(lookup.manifest)+4096, graphCapacityLimits{})
	if err != nil {
		return ownDecision, err
	}
	arena, ok := closure.output.(*graphstate.OutputBudget)
	if !ok {
		return ownDecision, errInvalid
	}
	owner.budget = closure
	tx := crossTransaction{owner: c.host, manifest: lookup.manifest, digest: digest}
	if lookup.status.phase < crossDecide {
		proposal, err := c.host.proposalFromCapacityAuthority(tx, authority, arena)
		if err != nil {
			return ownDecision, err
		}
		if c.beforeDecision != nil {
			if err := c.observeDecision(ctx, tx, proposal, arena); err != nil {
				return ownDecision, err
			}
		}
		if _, err := owner.submit(ctx, proposal); err != nil {
			return ownDecision, err
		}
	}
	decision, err := owner.cross(ctx, txID, crossDecide, len(lookup.manifest))
	if err != nil {
		return ownDecision, err
	}
	if !decision.validSeal() || decision.status.tx != txID || decision.status.phase != crossDecide || decision.status.ns.partition != coordinator || decision.status.digest != digest || !bytes.Equal(decision.manifest, lookup.manifest) {
		return ownDecision, errInvalid
	}
	for _, part := range loser.parts {
		if endpoints[part.binding.Partition] == nil {
			return ownDecision, errCrossProcessPending
		}
		endpoint := *endpoints[part.binding.Partition]
		endpoint.budget = closure
		if !decision.status.commit {
			present, err := endpoint.lookupCapacityOriginal(ctx, txID, len(lookup.manifest))
			if err != nil {
				return ownDecision, errors.Join(errCrossProcessPending, err)
			}
			if present.configuration != loser.configuration {
				return ownDecision, errInvalid
			}
			if !present.present {
				continue
			}
			if present.status.digest != digest || !bytes.Equal(present.manifest, lookup.manifest) {
				return ownDecision, errInvalid
			}
			if present.status.phase == crossResolve {
				continue
			}
		}
		proposal, err := c.host.proposalFromAuthority(crossResolve, part.binding.Partition, tx, []crossAuthority{decision}, arena)
		if err != nil {
			return ownDecision, err
		}
		resolved, err := endpoint.submit(ctx, proposal)
		if err != nil {
			return ownDecision, err
		}
		if resolved.tx != txID || resolved.digest != digest || resolved.phase != crossResolve || resolved.commit != decision.status.commit || resolved.round != decision.status.round {
			return ownDecision, errInvalid
		}
	}
	if txID == manifest.tx {
		return decision, nil
	}
	return ownDecision, nil
}

func (e *graphAuthenticatedEndpoint) lookupCapacityOriginal(ctx context.Context, tx [16]byte, manifestBytes int) (crossRequestBody, error) {
	r, err := e.call(ctx, graphSourceRequest{kind: sourceRequestLookup, tx: tx}, min(graphSourceFrameBytes, manifestBytes+4096))
	if err != nil {
		return crossRequestBody{}, err
	}
	p, err := decodeCrossRequestBody(r.body)
	if err != nil || p.scope != e.peer.scope || p.id.session != r.issuer || p.question.tx != tx || p.question.nonce != r.nonce {
		return crossRequestBody{}, errors.Join(errInvalid, err)
	}
	return p, nil
}

// Conservatively prepay a finite auxiliary control path on this same ledger.
// It cannot spend the original's completion floor, reset quotas, or refund unused
// work. The child owns only counters/output already consumed by its parent.
func (b *graphRemoteBudget) capacityChild(parts, originalBytes, calls, frame int, native graphCapacityLimits) (*graphRemoteBudget, error) {
	if b == nil || b.output == nil || parts < 1 || parts > 16 || originalBytes < 1 || originalBytes > 1<<20 || calls < 1 || calls > 70 || frame < 1 || frame > graphSourceFrameBytes || native.rows < 0 || native.bytes < 0 || native.output < 0 {
		return nil, errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	originalCalls := 4*parts + 6
	originalWire := 2 * originalCalls * (originalBytes + 4096)
	originalOutput := 4*originalWire + originalCalls*1024
	wire := 2 * calls * frame
	output := 4*wire + calls*4096 + native.output
	if originalCalls+calls > b.maxCalls-b.calls || originalWire+wire+native.bytes > b.maxBytes-b.bytes || native.rows > b.maxRows-b.rows || originalOutput+output > b.output.Remaining() {
		return nil, errLimit
	}
	arena, err := graphstate.NewOutputBudget(output)
	if err != nil {
		return nil, outputBudgetFailure(err)
	}
	if err := b.output.Reserve(output); err != nil {
		return nil, outputBudgetFailure(err)
	}
	b.calls += calls
	b.bytes += wire + native.bytes
	b.rows += native.rows
	return &graphRemoteBudget{maxCalls: calls, maxBytes: wire + native.bytes, maxRows: native.rows, output: arena}, nil
}

func (b *graphRemoteBudget) reserveCapacityNative(l graphCapacityLimits) error {
	if b == nil || b.output == nil || l.rows < 1 || l.bytes < 1 || l.output < 1 {
		return errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.rows > b.maxRows-b.rows || l.bytes > b.maxBytes-b.bytes || l.output > b.output.Remaining() {
		return errLimit
	}
	if err := b.output.Reserve(l.output); err != nil {
		return outputBudgetFailure(err)
	}
	b.rows += l.rows
	b.bytes += l.bytes
	return nil
}
