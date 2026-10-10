package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// declaredMaterializer stages declared-partition initialization/allocation.
// Actual graph mutations, prepare and certified cuts require the following
// transaction/claim/fence-quiescence agreement; local readiness is insufficient.
type declaredMaterializer struct {
	store       *raftlog.Store
	ns          namespace
	owner       uint64
	declaration graphstore.OwnershipDeclaration
	binding     raftlog.ApplicationBinding
	limits      materializerLimits
	agreement   raftlog.ApplicationSemanticContractID
}

var _ replica.ApplicationMachine = (*declaredMaterializer)(nil)

func (m *declaredMaterializer) SemanticContractID() raftlog.ApplicationSemanticContractID {
	if m != nil && m.agreement != (raftlog.ApplicationSemanticContractID{}) {
		return m.agreement
	}
	return declaredSemanticContractID()
}

func newDeclaredMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	return newDeclaredMaterializerWithAgreement(s, n, d, l, declaredSemanticContractID())
}

func newDeclaredMaterializerWithAgreement(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits, agreement raftlog.ApplicationSemanticContractID) (*declaredMaterializer, error) {
	if agreement != declaredSemanticContractID() && agreement != graphGenesisSemanticContractID() {
		return nil, errInvalid
	}
	if s == nil || !n.valid() || idalloc.GraphID(d.Graph()) != n.graph {
		return nil, errInvalid
	}
	if err := l.validate(); err != nil {
		return nil, err
	}
	entry, found := d.Partition(n.partition)
	if !found || !s.ApplicationLimits().Enabled() {
		return nil, errInvalid
	}
	binding := s.ApplicationBinding()
	m := &declaredMaterializer{store: s, ns: n, owner: entry.OwnershipEpoch, declaration: d, binding: binding, limits: l, agreement: agreement}
	if err := replica.ValidateApplicationMachineBinding(binding, m); err != nil || binding == (raftlog.ApplicationBinding{}) {
		return nil, errors.Join(errInvalid, err)
	}
	if binding.Identity.Graph != [16]byte(n.graph) || binding.Identity.Partition != n.partition || binding.Identity.Group != entry.Group {
		return nil, graphstore.ErrNamespace
	}
	if s.ApplicationLimits().MaxImageBytes > l.sourceBytes {
		return nil, errLimit
	}
	index, image, err := s.Checkpoint()
	if err != nil {
		return nil, err
	}
	if err := m.Restore(index, image); err != nil {
		return nil, err
	}
	return m, nil
}

func genesisConfigurationKey(n namespace) []byte { return recordKey(n, 6) }
func declaredAttemptKey(n namespace, id [16]byte) []byte {
	return append(recordKey(n, 5), id[:]...)
}

func (m *declaredMaterializer) readOwnership(ctx context.Context, view *raftlog.ApplicationView, q *reader) (graphstore.OwnershipRead, error) {
	b := graphstore.OwnershipBudget{SourceRows: q.limits.readRows - q.rows, SourceBytes: q.limits.readBytes - q.bytes, OutputBytes: m.limits.outputBytes}
	if b.SourceRows < 1 || b.SourceBytes < 1 {
		return graphstore.OwnershipRead{}, errLimit
	}
	info, work, err := graphstore.ReadOwnershipDeclaration(ctx, view, m.limits.catalog, b)
	if chargeErr := chargeGraphWork(q, work); chargeErr != nil {
		return graphstore.OwnershipRead{}, errors.Join(chargeErr, err)
	}
	if err != nil {
		return graphstore.OwnershipRead{}, err
	}
	ref := info.Reference()
	if info.Binding() != m.binding || info.Root().Namespace() != (graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}) || info.Root().OwnershipEpoch() != m.owner || ref.Generation != q.base.Generation || ref.Index != q.base.Index || ref.ImageHash != q.base.ImageHash {
		return graphstore.OwnershipRead{}, errCorrupt
	}
	if info.Published() && info.Declaration().Digest() != m.declaration.Digest() {
		return graphstore.OwnershipRead{}, graphstore.ErrRebinding
	}
	return info, nil
}

func (m *declaredMaterializer) ownershipBudget(q *reader) (graphstore.OwnershipBudget, error) {
	b := graphstore.OwnershipBudget{SourceRows: q.limits.readRows - q.rows, SourceBytes: q.limits.readBytes - q.bytes, OutputBytes: m.limits.outputBytes - graphResultMetadataBytes - cap(q.base.Image)}
	if b.SourceRows < 1 || b.SourceBytes < 1 || b.OutputBytes < 1 {
		return b, errLimit
	}
	return b, nil
}

func (m *declaredMaterializer) seed(q *reader) error {
	b, err := m.ownershipBudget(q)
	if err != nil {
		return err
	}
	base, _, work, err := graphstore.CheckDeclaredPartitionSeed(q.ctx, q.view, m.declaration, m.limits.catalog, b)
	if chargeErr := chargeGraphWork(q, work); chargeErr != nil {
		return errors.Join(chargeErr, err)
	}
	if err != nil {
		return err
	}
	if !sameBase(q.base, base) {
		return errInvalid
	}
	return nil
}

func (m *declaredMaterializer) initialized(q *reader) (genesisAllocationConfig, error) {
	b, err := m.ownershipBudget(q)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	var cfg genesisAllocationConfig
	if m.SemanticContractID() == graphGenesisSemanticContractID() {
		cfg, err = m.readGenesisConfig(q)
		if err != nil {
			return genesisAllocationConfig{}, err
		}
		b, err = m.ownershipBudget(q)
		if err != nil {
			return genesisAllocationConfig{}, err
		}
	}
	var c *graphstore.Catalog
	var work graphstore.PageWork
	if m.SemanticContractID() == graphGenesisSemanticContractID() {
		c, work, err = graphstore.OpenPartitionCatalogWithDefaultAxis(q.ctx, q.view, graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}, m.owner, cfg.defaultAxis, m.limits.catalog, m.limits.graph, b)
	} else {
		c, work, err = graphstore.OpenPartitionCatalog(q.ctx, q.view, graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}, m.owner, m.limits.catalog, m.limits.graph, b)
	}
	if chargeErr := chargeGraphWork(q, work); chargeErr != nil {
		return genesisAllocationConfig{}, errors.Join(chargeErr, err)
	}
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	root, err := c.Root()
	if err != nil || root.SemanticEpoch() == 0 {
		return genesisAllocationConfig{}, errors.Join(errCorrupt, err)
	}
	if m.SemanticContractID() != graphGenesisSemanticContractID() {
		cfg, err = m.readGenesisConfig(q)
		if err != nil {
			return genesisAllocationConfig{}, err
		}
	}
	_, present, err := q.allocator()
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	if m.ns.partition == cfg.home {
		if !present || q.allocatorInfo.MaxBlock != cfg.maxBlock {
			return genesisAllocationConfig{}, errCorrupt
		}
	} else if present {
		// A home may never create a second independent graph allocator.
		return genesisAllocationConfig{}, errCorrupt
	}
	return cfg, nil
}

func (m *declaredMaterializer) Restore(index uint64, image []byte) (err error) {
	if m == nil || m.store == nil || index == 0 {
		return errInvalid
	}
	if len(image) != 172 {
		return errCorrupt
	}
	view, err := m.store.ApplicationView(index)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	base, err := view.RootBounded(context.Background(), 172)
	if err != nil {
		return err
	}
	if !bytes.Equal(base.Image, image) {
		return errInvalid
	}
	l := m.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, m.limits.sourceRows), min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: context.Background(), view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	info, err := m.readOwnership(q.ctx, view, &q)
	if err != nil {
		return err
	}
	if info.Root().SemanticEpoch() == 0 {
		return m.seed(&q)
	}
	_, err = m.initialized(&q)
	return err
}

func (m *declaredMaterializer) preflight(b raftlog.ApplicationBatch, budget raftlog.ApplicationBudget) error {
	old := materializer{store: m.store, ns: m.ns, owner: m.owner, limits: m.limits}
	return old.preflight(b, budget)
}

func (m *declaredMaterializer) finish(q *reader, o outcome, hash [32]byte, mapRecord bool, budget raftlog.ApplicationBudget, effects *graphstore.GraphEffects, r *declaredInitCommand, cfg genesisAllocationConfig) (raftlog.ApplicationBatch, error) {
	o.hash = hash
	wire, err := encodeDeclaredOutcome(o)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if mapRecord {
		if err := q.put(declaredRequestKey(m.ns, o.kind, o.identity), wire); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	b := batchAt(q.base)
	b.Outcome = wire
	b.Writes = q.writes
	// Control CDC is the lean typed original outcome, matching the accepted
	// control-v1 convention. Only an actual state change emits it; the graph
	// logical epoch/digest and immutable range records remain separate.
	if isAllocationProtocolCommand(o.kind) && o.reason == reasonNone && o.disposition == applied && len(q.writes) > 1 {
		b.Changes = owned(wire)
	}
	if effects != nil {
		if r == nil || !sameBase(q.base, effects.Base) || effects.OwnedBytes <= 0 {
			return raftlog.ApplicationBatch{}, errCorrupt
		}
		retained := effects.OwnedBytes + declaredInitMetadataBytes + 32*m.declaration.Len() + cap(q.base.Image) + 64*cap(q.writes) + cap(wire)
		for _, row := range q.writes {
			retained += cap(row.Key) + cap(row.Value)
		}
		for _, d := range r.schemas {
			retained += 64 + len(d.Name)
		}
		if m.SemanticContractID() == graphGenesisSemanticContractID() {
			outer, err := graphGenesisOuterOutput(*r, m.limits, q.base)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			retained = max(retained, effects.OwnedBytes+outer)
		}
		if retained > m.limits.outputBytes {
			return raftlog.ApplicationBatch{}, errLimit
		}
		logical, err := encodeDeclaredInitialization(*r, cfg, min(m.limits.changeBytes, (m.limits.outputBytes-retained)/4))
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := reserveComposition(&retained, 4*cap(logical)+2*(len(logical)+97)+4*172+64*(len(q.writes)+len(effects.Writes)), m.limits.outputBytes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		root, err := effects.Root.AdvanceEffects(graphEffectDigest(effects.Root.EffectDigest(), logical))
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b.Image, err = graphstore.EncodeRoot(root)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b.Changes, err = encodeChangeEnvelope(o, logical, m.limits)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b.Writes = make([]raftlog.KV, 0, len(q.writes)+len(effects.Writes))
		b.Writes = append(b.Writes, q.writes...)
		b.Writes = append(b.Writes, effects.Writes...)
	}
	slices.SortFunc(b.Writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	for i := 1; i < len(b.Writes); i++ {
		if bytes.Equal(b.Writes[i-1].Key, b.Writes[i].Key) {
			return raftlog.ApplicationBatch{}, errCorrupt
		}
	}
	if err := m.preflight(b, budget); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return b, nil
}

func (m *declaredMaterializer) Stage(entry replica.Entry, budget raftlog.ApplicationBudget) (batch raftlog.ApplicationBatch, err error) {
	if m == nil || m.store == nil || entry.Index < 2 || budget.Writes < 1 || budget.Bytes < 1 || budget.ImageBytes < 1 || budget.ChangeBytes < 1 || budget.OutcomeBytes < 1 {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if len(entry.Data) > 4<<20 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	if m.store.ApplicationLimits().MaxImageBytes > m.limits.sourceBytes {
		return raftlog.ApplicationBatch{}, errLimit
	}
	view, err := m.store.ApplicationView(entry.Index - 1)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	defer func() {
		if closeErr := view.Close(); closeErr != nil {
			batch = raftlog.ApplicationBatch{}
			err = errors.Join(err, closeErr)
		}
	}()
	base, err := view.RootBounded(context.Background(), 172)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if base.Index == math.MaxUint64 || entry.Index != base.Index+1 || entry.Generation != base.Generation {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	l := m.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, m.limits.sourceRows), min(l.readBytes, m.limits.sourceBytes)
	l.stageRows, l.stageBytes = min(l.stageRows, m.limits.stageRows), min(l.stageBytes, m.limits.stageBytes)
	q := reader{ctx: context.Background(), view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image), stageBytes: 128}
	info, err := m.readOwnership(q.ctx, view, &q)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if len(entry.Data) == 0 {
		if info.Root().SemanticEpoch() == 0 {
			if err := m.seed(&q); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		} else if _, err := m.initialized(&q); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b := batchAt(base)
		if err := m.preflight(b, budget); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return b, nil
	}
	if len(entry.Data) >= 5 && commandKind(entry.Data[4]) == initDeclaredPartition {
		version := byte(4)
		if m.SemanticContractID() == graphGenesisSemanticContractID() {
			version = 5
		}
		if entry.Data[3] != version {
			return raftlog.ApplicationBatch{}, errCorrupt
		}
	}
	identity, err := declaredCommandIdentity(entry.Data, m.ns)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	hash := sha256.Sum256(entry.Data)
	kind := commandKind(entry.Data[4])
	old, found, err := declaredReplay(&q, kind, identity, hash, entry.Index)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if found {
		if info.Root().SemanticEpoch() == 0 {
			if err := m.seed(&q); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		} else if m.SemanticContractID() == graphGenesisSemanticContractID() {
			if _, err := m.initialized(&q); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}
		return m.finish(&q, old, hash, false, budget, nil, nil, genesisAllocationConfig{})
	}
	if isAllocationProtocolCommand(kind) {
		if info.Root().SemanticEpoch() == 0 {
			if err := m.seed(&q); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			return raftlog.ApplicationBatch{}, errNotInitialized
		}
		command, err := decodeAllocationProtocolCommand(entry.Data, m.limits)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		cfg, err := m.initialized(&q)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		scope := allocationScope{graph: m.ns.graph, partition: m.ns.partition, group: m.binding.Identity.Group, ownership: m.owner, topology: m.declaration.TopologyEpoch(), declaration: m.declaration.Digest(), semantic: m.binding.SemanticContractID}
		allocator := allocationRecordReader{q: &q, scope: scope, config: cfg, declaration: m.declaration}
		result, err := allocator.transition(command, entry.Index)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if result.reason != reasonNone {
			q.writes = nil
			q.stageBytes = 128
		}
		return m.finish(&q, result, hash, true, budget, nil, nil, cfg)
	}
	r, decodeErr := decodeDeclaredInit(entry.Data, m.limits)
	if errors.Is(decodeErr, errLimit) || errors.Is(decodeErr, graphstore.ErrResourceLimit) {
		return raftlog.ApplicationBatch{}, decodeErr
	}
	o := outcome{ns: m.ns, kind: initDeclaredPartition, identity: identity, hash: hash, index: entry.Index, disposition: applied, reason: reasonInvalid}
	if info.Root().SemanticEpoch() != 0 {
		if _, err := m.initialized(&q); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		o.reason = reasonAlreadyInitialized
		return m.finish(&q, o, hash, true, budget, nil, nil, genesisAllocationConfig{})
	}
	// A recognized unready attempt is not an ordinary dedup identity. Rejection
	// appends only fixed envelopes, preserving future empty/sole-record proof.
	if decodeErr != nil {
		if err := m.seed(&q); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return m.finish(&q, o, hash, false, budget, nil, nil, genesisAllocationConfig{})
	}
	cfg, err := r.validate(m.limits)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if r.declaration.Digest() != m.declaration.Digest() {
		if err := m.seed(&q); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return m.finish(&q, o, hash, false, budget, nil, nil, genesisAllocationConfig{})
	}
	remaining, err := m.ownershipBudget(&q)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if m.SemanticContractID() == graphGenesisSemanticContractID() {
		outer, err := graphGenesisOuterOutput(r, m.limits, q.base)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if outer >= m.limits.outputBytes {
			return raftlog.ApplicationBatch{}, errLimit
		}
		remaining.OutputBytes = min(remaining.OutputBytes, m.limits.outputBytes-outer)
	}
	var effects graphstore.GraphEffects
	var work graphstore.PageWork
	if m.SemanticContractID() == graphGenesisSemanticContractID() {
		effects, work, err = graphstore.InitializeDeclaredPartitionWithDefaultAxis(q.ctx, view, m.declaration, r.defaultAxis, r.schemas, m.limits.catalog, m.limits.graph, remaining)
	} else {
		effects, work, err = graphstore.InitializeDeclaredPartition(q.ctx, view, m.declaration, r.schemas, m.limits.catalog, m.limits.graph, remaining)
	}
	if chargeErr := chargeGraphWork(&q, work); chargeErr != nil {
		return raftlog.ApplicationBatch{}, errors.Join(chargeErr, err)
	}
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	wire, err := encodeGenesisAllocationConfig(cfg)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := q.put(genesisConfigurationKey(m.ns), wire); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if m.ns.partition == cfg.home {
		state, err := idalloc.NewState(m.ns.graph, r.authority, cfg.maxBlock)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		wire, err := idalloc.MarshalCheckpoint(&state)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := q.put(allocatorKey(m.ns), wire); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	o.reason = reasonNone
	return m.finish(&q, o, hash, true, budget, &effects, &r, cfg)
}
