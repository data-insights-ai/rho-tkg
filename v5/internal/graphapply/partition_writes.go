package graphapply

import (
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func isPartitionGraphWire(data []byte) bool { return len(data) >= 5 && string(data[:4]) == "PGQ\x01" }
func (m *declaredMaterializer) stagePartitionGraph(q *reader, info graphstore.OwnershipRead, entry replica.Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if m.SemanticContractID() != partitionWriteSemanticContractID() {
		return raftlog.ApplicationBatch{}, errCorrupt
	}
	if info.Root().SemanticEpoch() == 0 {
		return raftlog.ApplicationBatch{}, errNotInitialized
	}
	cfg, err := m.initialized(q)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	decodedOwned := 0
	r, err := decodePartitionGraphCommandOwned(entry.Data, m.limits, &decodedOwned)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if r.request.ns != m.ns || r.configuration != configuration || r.declaration != m.declaration.Digest() || r.routing != cfg.routingDigest {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	hash := sha256.Sum256(entry.Data)
	old, found, err := sharedReplayWithDecoder(q, request{ns: m.ns, kind: r.request.kind, id: r.request.id}, hash, entry.Index, decodePartitionMappedOutcome)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	legacy := materializer{store: m.store, ns: m.ns, owner: m.owner, limits: m.limits}
	if found {
		return legacy.finishWithCodec(q, old, hash, false, budget, nil, graphChanges{}, true)
	}
	result := outcome{ns: m.ns, kind: r.request.kind, identity: r.request.identity(), index: entry.Index, disposition: applied}
	if r.request.kind == guardedGraphOperations {
		expected := r.request.readBase.guard
		actual := graphstore.SemanticGuard{Namespace: info.Root().Namespace(), OwnershipEpoch: m.owner, TopologyEpoch: m.declaration.TopologyEpoch(), SchemaVersion: 1, SemanticEpoch: info.Root().SemanticEpoch(), EffectDigest: info.Root().EffectDigest()}
		if r.request.readBase.group != m.binding.Identity.Group || expected != actual {
			result.reason = reasonReadConflict
			return legacy.finishWithCodec(q, result, hash, true, budget, nil, graphChanges{}, true)
		}
	}
	floor, err := readPartitionRoundFloor(q, configuration)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	revision, err := state.NewRevision(floor+1, r.request.revision.Provenance())
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	// Retain decoded operations/claims, canonical codec backing and the original
	// base through staging. Remaining source and output cannot gain fresh defaults.
	outer := 4096 + compositionMetadataBytes + graphRequestMetadataBytes + 640*cap(r.request.operations) + 128*cap(r.request.claims) + 8*len(entry.Data) + 4*cap(q.base.Image)
	for _, row := range q.writes {
		outer += cap(row.Key) + cap(row.Value) + 128
	}
	if err := joinGraphOutputBudget(q, r.request, decodedOwned, m.limits); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	gl := m.limits.graph
	gl.MaxSourceRows = min(gl.MaxSourceRows, q.limits.readRows-q.rows)
	gl.MaxSourceBytes = min(gl.MaxSourceBytes, q.limits.readBytes-q.bytes)
	// The child retains the graph cap; the parent retains the complete outer cap.
	if gl.MaxSourceRows < 1 || gl.MaxSourceBytes < 1 || gl.MaxOutputBytes < 1 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	effects, work, err := graphstore.StagePartitionOperationsWithOutputBudget(q.ctx, q.view, m.declaration, cfg.defaultAxis, configuration, r.request.operations, revision, m.limits.catalog, gl, graphstore.OwnershipBudget{SourceRows: gl.MaxSourceRows, SourceBytes: gl.MaxSourceBytes, OutputBytes: gl.MaxOutputBytes}, q.arena)
	if charged := chargeGraphWork(q, work); charged != nil {
		return raftlog.ApplicationBatch{}, errors.Join(charged, err)
	}
	if err != nil {
		if errors.Is(err, graphstore.ErrRoutingUnknown) || errors.Is(err, graphstore.ErrRemoteParticipant) || errors.Is(err, graphstore.ErrStaleOwner) {
			result.reason = reasonStale
			if errors.Is(err, graphstore.ErrRemoteParticipant) {
				result.reason = reasonRemoteParticipant
			} else if errors.Is(err, graphstore.ErrRoutingUnknown) {
				result.reason = reasonRoutingUnknown
			}
			return legacy.finishWithCodec(q, result, hash, true, budget, nil, graphChanges{}, true)
		}
		if why, ok := businessGraphReason(err); ok {
			result.reason = why
			return legacy.finishWithCodec(q, result, hash, true, budget, nil, graphChanges{}, true)
		}
		return raftlog.ApplicationBatch{}, err
	}
	if !sameBase(q.base, effects.Base) {
		return raftlog.ApplicationBatch{}, errCorrupt
	}
	scope := m.scope()
	a := allocationRecordReader{q: q, scope: scope, config: cfg, declaration: m.declaration}
	if why, err := a.admitGraphDelta(r.request, effects.Delta); err != nil {
		return raftlog.ApplicationBatch{}, err
	} else if why != reasonNone {
		result.reason = why
		return legacy.finishWithCodec(q, result, hash, true, budget, nil, graphChanges{}, true)
	}
	changes := graphChanges{ns: m.ns, entities: effects.Delta.Entities, lives: effects.Delta.Lives, values: effects.Delta.Values, groups: effects.Groups}
	if !changes.nonempty() {
		return legacy.finishWithCodec(q, result, hash, true, budget, nil, changes, true)
	}
	if err := writePartitionRoundFloor(q, configuration, floor+1); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	output := legacy
	// q.arena carries all prior child/parent consumption into this composition.
	batch, err := output.finishWithCodec(q, result, hash, true, budget, &effects, changes, true)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return m.finishPartitionRoundMap(q, batch, configuration, floor+1, entry.Index, budget, outer)
}

// Only newly introduced identities in the actual planned Delta need grants.
// Unused fresh IDs remain burned, without manufacturing a binding or authority.
func (a allocationRecordReader) admitGraphDelta(r graphRequest, d graphstate.Delta) (reason, error) {
	var grantCache [4]protocolGrant
	var recipientCache [4]protocolRecipient
	next := 0
	check := func(role bindingRole, owner graphstate.EntityID, id uint64) (reason, error) {
		claim, found := findBinding(r.claims, role, owner, id)
		if !found {
			return reasonInvalid, nil
		}
		var grant protocolGrant
		var recipient protocolRecipient
		cached := false
		for i, g := range grantCache {
			if g.grant.Request.Session == claim.grant.session && g.grant.Request.Sequence == claim.grant.sequence && g.configuration != ([32]byte{}) {
				grant, recipient, cached = g, recipientCache[i], true
				break
			}
		}
		if !cached {
			g, err := a.grant(claim.grant.session, claim.grant.sequence)
			if err != nil {
				return reasonNone, err
			}
			if g == nil {
				return reasonInvalid, nil
			}
			rec, err := a.recipient(claim.grant.session.ID)
			if err != nil {
				return reasonNone, err
			}
			if rec == nil {
				return reasonStale, nil
			}
			grant, recipient = *g, *rec
			grantCache[next], recipientCache[next] = grant, recipient
			next = (next + 1) % len(grantCache)
		}
		publishedSequence := recipient.sequence
		if a.q.ns.partition == a.config.home && recipient.home == a.config.home && recipient.sourcePhase == recipientActive && grant.source.scope == a.scope {
			publishedSequence = max(publishedSequence, recipient.sourceSequence)
		}
		if recipient.home != a.q.ns.partition || recipient.homePhase != recipientActive || recipient.session != claim.grant.session || grant.grant.Request.Sequence > publishedSequence {
			return reasonStale, nil
		}
		p := grant.publication
		if !p.Contains(id) {
			return reasonInvalid, nil
		}
		current, err := a.rangeOwnership(p)
		if err != nil {
			return reasonNone, err
		}
		if err := current.Check(p, a.q.ns.partition, current.Epoch); err != nil {
			if errors.Is(err, graphstore.ErrStaleOwner) {
				return reasonStale, nil
			}
			return reasonNone, err
		}
		return reasonNone, nil
	}
	if err := chargeGraphWork(a.q, graphstore.PageWork{Bytes: 4096}); err != nil {
		return reasonNone, err
	}
	for _, e := range d.Entities {
		if why, err := check(entityBinding, 0, uint64(e.ID)); why != reasonNone || err != nil {
			return why, err
		}
	}
	for _, life := range d.Lives {
		if why, err := check(lifeBinding, life.Owner, uint64(life.Life)); why != reasonNone || err != nil {
			return why, err
		}
	}
	for _, value := range d.Values {
		if why, err := check(valueBinding, 0, uint64(value.ID)); why != reasonNone || err != nil {
			return why, err
		}
	}
	return reasonNone, nil
}

// PreparePartitionGraphOperations owns a versioned typed local-write proposal.
// It selects no remote participants and grants no authority from caller effects.
// The local installation round is assigned by Stage from its durable floor.
func (h *allocationHost) PreparePartitionGraphOperations(id requestID, ops []graphstate.Operation, provenance uint64, claims []freshBinding) (allocationProposal, error) {
	if h == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return allocationProposal{}, replica.ErrStopped
	}
	if h.machine == nil || h.driver == nil || h.machine.SemanticContractID() != partitionWriteSemanticContractID() {
		return allocationProposal{}, errInvalid
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	digest, err := cfg.digest()
	if err != nil {
		return allocationProposal{}, err
	}
	revision, err := state.NewRevision(1, provenance)
	if err != nil {
		return allocationProposal{}, err
	}
	r := partitionGraphCommand{configuration: digest, declaration: h.machine.declaration.Digest(), routing: cfg.routingDigest, request: graphRequest{ns: h.machine.ns, kind: graphOperations, id: id, revision: revision, operations: ops, claims: claims}}
	wire, err := encodePartitionGraphCommand(r, h.machine.limits)
	if err != nil {
		return allocationProposal{}, err
	}
	target, err := h.target(h.machine.ns.partition)
	if err != nil {
		return allocationProposal{}, err
	}
	return allocationProposal{target: target, wire: wire}, nil
}
