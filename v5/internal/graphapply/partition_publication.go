package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// This fresh contract adds actual range publication and checked all-local graph
// writes in a declared topology. Cross-group graph writes/cuts remain unavailable.
const partitionWriteSemanticDescriptor = "rho-tkg:graphapply:partition-writes:v1\x00" +
	"genesis=default-schema-routing-v1\x00range-publication=allocator-coinstalled-v1\x00" +
	"routing=explicit-current-range-and-typed-buckets-v1\x00axis-authority=AxisID-v1\x00" +
	"local-graph-writes=complete-footprint-v1\x00global-value-identity=exact-v1\x00" +
	"request-recovery=1\x00cross-group-writes=unavailable\x00certified-cuts=unavailable\x00"
const partitionGenesisConfigBytes = graphGenesisConfigBytes + 32
const partitionGenesisObservationBytes = graphGenesisObservationBytes + 32

func partitionWriteSemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(partitionWriteSemanticDescriptor)))
}
func newPartitionWriteMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	return newDeclaredMaterializerWithAgreement(s, n, d, l, partitionWriteSemanticContractID())
}
func (m *declaredMaterializer) requiresDefaultAxis() bool {
	return m.SemanticContractID() == graphGenesisSemanticContractID() || m.SemanticContractID() == partitionWriteSemanticContractID()
}
func (h *allocationHost) PreparePartitionInitialization(partition uint64, attempt bootstrapAttemptID, schemas []graphstate.PropertyDefinition, maxBlock uint64, binding types.DefaultAxisBinding, routing graphstore.PartitionRouting, genesis allocationProof) (allocationProposal, error) {
	if routing.Digest() == ([32]byte{}) || binding == (types.DefaultAxisBinding{}) {
		return allocationProposal{}, errInvalid
	}
	return h.prepareInitialization(partition, attempt, schemas, maxBlock, binding, genesis, partitionWriteSemanticContractID(), new(routing))
}
func (m *declaredMaterializer) readRouting(q *reader, cfg genesisAllocationConfig) (graphstore.PartitionRouting, error) {
	if cfg.version() != 3 || cfg.routingDigest == ([32]byte{}) {
		return graphstore.PartitionRouting{}, errCorrupt
	}
	n := graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}
	wire, found, err := q.getBounded(graphstore.RoutingDefinitionKey(n), m.limits.catalog.MaxRecordBytes)
	if err != nil {
		return graphstore.PartitionRouting{}, err
	}
	if !found {
		return graphstore.PartitionRouting{}, errCorrupt
	}
	if err := chargeGraphWork(q, graphstore.PageWork{Bytes: 512 + 8*len(wire)}); err != nil {
		return graphstore.PartitionRouting{}, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return graphstore.PartitionRouting{}, err
	}
	routing, err := graphstore.DecodePartitionRoutingBinding(wire, m.declaration, cfg.defaultAxis, configuration, m.limits.catalog)
	if err != nil || routing.Digest() != cfg.routingDigest {
		return graphstore.PartitionRouting{}, errors.Join(errCorrupt, err)
	}
	return routing, nil
}

func (a allocationRecordReader) putPublication(p graphstore.RangePublication, o graphstore.RangeOwnership) error {
	if p.Configuration == ([32]byte{}) || p.SourcePartition != a.config.home || p.Graph != graphstate.GraphID(a.q.ns.graph) {
		return errCorrupt
	}
	if err := o.Check(p, o.Partition, o.Epoch); err != nil {
		return err
	}
	n := graphstore.Namespace{Graph: p.Graph, Partition: a.q.ns.partition}
	wire, err := graphstore.EncodeRangePublication(p)
	if err != nil {
		return err
	}
	key := graphstore.RangePublicationKey(n, p.RangeID)
	old, found, err := a.q.getBounded(key, len(wire))
	if err != nil {
		return err
	}
	if found {
		if !bytes.Equal(old, wire) {
			return errCorrupt
		}
		return nil
	}
	if err := a.q.put(key, wire); err != nil {
		return err
	}
	if err := a.q.put(graphstore.RangeEndKey(n, p.Last), wire); err != nil {
		return err
	}
	ownership, err := graphstore.EncodeRangeOwnership(o)
	if err != nil {
		return err
	}
	return a.q.put(graphstore.RangeOwnershipKey(n, p.RangeID), ownership)
}
func (a allocationRecordReader) rangeOwnership(p graphstore.RangePublication) (graphstore.RangeOwnership, error) {
	n := graphstore.Namespace{Graph: p.Graph, Partition: a.q.ns.partition}
	wire, found, err := a.q.getBounded(graphstore.RangeOwnershipKey(n, p.RangeID), 109)
	if err != nil {
		return graphstore.RangeOwnership{}, err
	}
	if !found {
		return graphstore.RangeOwnership{}, errCorrupt
	}
	o, err := graphstore.DecodeRangeOwnership(wire)
	if err != nil || o.Graph != p.Graph || o.RangeID != p.RangeID || o.Configuration != p.Configuration {
		return graphstore.RangeOwnership{}, errors.Join(errCorrupt, err)
	}
	if err := o.Check(p, o.Partition, o.Epoch); err != nil && !errors.Is(err, graphstore.ErrStaleOwner) {
		return graphstore.RangeOwnership{}, errors.Join(errCorrupt, err)
	}
	return o, nil
}

// finishRoutingPublication binds logical routing changes, independently of the
// physical End locator. Replays have no new publication writes and no new CDC.
func (m *declaredMaterializer) finishRoutingPublication(q *reader, o outcome, cfg genesisAllocationConfig, b *raftlog.ApplicationBatch) error {
	if cfg.version() != 3 || o.reason != reasonNone || o.disposition != applied {
		return nil
	}
	n := graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}
	var publicationWire, ownerWire []byte
	var publication graphstore.RangePublication
	for _, row := range q.writes {
		if len(row.Value) < 4 || !bytes.Equal(row.Value[:4], []byte{'R', 'P', 'B', 1}) {
			continue
		}
		p, err := graphstore.DecodeRangePublication(row.Value)
		if err != nil {
			return err
		}
		if !bytes.Equal(row.Key, graphstore.RangePublicationKey(n, p.RangeID)) {
			continue
		}
		if publicationWire != nil || row.Deleted || p.Graph != n.Graph {
			return errCorrupt
		}
		publication, publicationWire = p, row.Value
	}
	if publicationWire == nil {
		return nil
	}
	for _, row := range q.writes {
		if bytes.Equal(row.Key, graphstore.RangeOwnershipKey(n, publication.RangeID)) {
			if ownerWire != nil || row.Deleted {
				return errCorrupt
			}
			ownerWire = row.Value
		}
	}
	current, err := graphstore.DecodeRangeOwnership(ownerWire)
	if err != nil {
		return err
	}
	if err := current.Check(publication, current.Partition, current.Epoch); err != nil {
		return err
	}
	config, err := cfg.digest()
	if err != nil || publication.Configuration != config {
		return errors.Join(errCorrupt, err)
	}
	// Existing write backing remains retained. Admit both canonical encoder
	// passes, sealed CDC copies and root buffers before allocating any of them.
	logicalBytes := 4 + 16 + 8 + 32 + 4 + len(b.Outcome) + 4 + len(publicationWire) + 4 + len(ownerWire) + 32
	retained := 512 + cap(q.base.Image) + 64*cap(q.writes) + cap(b.Outcome)
	for _, row := range q.writes {
		retained += cap(row.Key) + cap(row.Value)
	}
	if err := reserveComposition(&retained, 8*logicalBytes+4*172+4*97, m.limits.outputBytes); err != nil {
		return err
	}
	logical, err := boundedEncoding(min(m.limits.changeBytes, logicalBytes), func(w *boundedWriter) {
		w.add([]byte{'R', 'C', 'D', 1})
		w.add(m.ns.graph[:])
		w.u64(m.ns.partition)
		w.add(config[:])
		w.field(b.Outcome)
		w.field(publicationWire)
		w.field(ownerWire)
	})
	if err != nil {
		return err
	}
	root, err := graphstore.DecodeRoot(q.base.Image)
	if err != nil {
		return err
	}
	if root.Namespace() != n || root.OwnershipEpoch() != m.owner || root.SemanticEpoch() == 0 {
		return errCorrupt
	}
	root, err = root.AdvanceEffects(graphEffectDigest(root.EffectDigest(), logical))
	if err != nil {
		return err
	}
	b.Image, err = graphstore.EncodeRoot(root)
	if err != nil {
		return err
	}
	b.Changes, err = encodeChangeEnvelope(o, logical, m.limits)
	return err
}
