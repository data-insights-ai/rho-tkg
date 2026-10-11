package graphapply

import (
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// This fresh agreement adds graph-qualified durable default-axis genesis. It
// deliberately adds no graph writes, distributed prepare, routing or cut door.
const graphGenesisSemanticDescriptor = "rho-tkg:graphapply:graph-genesis-contract:v1\x00" +
	"typed-native-values=1\x00partition-initialization=2\x00allocation-home=genesis-first-v1\x00" +
	"recipient-fencing=txnproto-v1\x00request-recovery=1\x00logical-initialization-effects=3\x00" +
	"default-axis=graph-qualified-Q-ms-POSIX-v1\x00graph-writes=unavailable\x00"

// The V1 policy has exactly78 descriptor bytes (27 fixed + reference + unit).
// This is framing admission, not a policy definition: BindDefaultAxis validates
// every field against the public policy before this codec can emit it.
const defaultAxisDescriptorBytes = 78
const graphGenesisConfigBytes = genesisConfigBytes + defaultAxisDescriptorBytes
const graphGenesisObservationBytes = genesisObservationBytes + defaultAxisDescriptorBytes

func graphGenesisSemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(graphGenesisSemanticDescriptor)))
}
func newGraphGenesisMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	return newDeclaredMaterializerWithAgreement(s, n, d, l, graphGenesisSemanticContractID())
}
func (c genesisAllocationConfig) version() byte {
	if c.routingDigest != ([32]byte{}) {
		return 3
	}
	if c.defaultAxis != (types.DefaultAxisBinding{}) {
		return 2
	}
	return 1
}
func (c genesisAllocationConfig) semanticContractID() raftlog.ApplicationSemanticContractID {
	if c.version() == 3 {
		return partitionWriteSemanticContractID()
	}
	if c.version() == 2 {
		return graphGenesisSemanticContractID()
	}
	return declaredSemanticContractID()
}
func (c genesisAllocationConfig) validDefaultAxis() bool {
	return c.version() == 1 || c.defaultAxis.Check(types.GraphID(c.graph), c.defaultAxis.Axis(), temporal.DefaultLimits()) == nil
}
func (r declaredInitCommand) version() byte {
	if r.routing != nil {
		return 6
	}
	if r.defaultAxis != (types.DefaultAxisBinding{}) {
		return 5
	}
	return 4
}
func (r declaredInitCommand) semanticContractID() raftlog.ApplicationSemanticContractID {
	if r.version() == 6 {
		return partitionWriteSemanticContractID()
	}
	if r.version() == 5 {
		return graphGenesisSemanticContractID()
	}
	return declaredSemanticContractID()
}
func (g genesisObservation) wireBytes() int {
	if g.configuration.version() == 3 {
		return partitionGenesisObservationBytes
	}
	if g.configuration.version() == 2 {
		return graphGenesisObservationBytes
	}
	return genesisObservationBytes
}

func encodeGraphGenesisConfig(c genesisAllocationConfig) ([]byte, error) {
	if !c.valid() || c.version() < 2 {
		return nil, errInvalid
	}
	size := graphGenesisConfigBytes
	if c.version() == 3 {
		size = partitionGenesisConfigBytes
	}
	return boundedEncoding(size, func(w *boundedWriter) {
		w.add([]byte{'G', 'A', 'C', c.version()})
		w.add(c.graph[:])
		w.u64(c.topology)
		w.add(c.declaration[:])
		w.u64(c.home)
		w.u64(c.maxBlock)
		w.add(c.schemas[:])
		writeAxis(w, c.defaultAxis.Axis(), defaultMaterializerLimits())
		if c.version() == 3 {
			w.add(c.routingDigest[:])
		}
	})
}
func decodeGraphGenesisConfig(b []byte, graph idalloc.GraphID) (genesisAllocationConfig, error) {
	version, size := byte(2), graphGenesisConfigBytes
	if len(b) >= 4 && b[3] == 3 {
		version, size = 3, partitionGenesisConfigBytes
	}
	body, err := wireBody(b, string([]byte{'G', 'A', 'C', version}), size)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	// Fixed bounded preflight covers the temporary axis strings/hash backing.
	c := graphCursor{b: body, maxOwnedBytes: genesisCodecMetadataBytes + 8*partitionGenesisConfigBytes}
	if !c.charge(genesisCodecMetadataBytes + 8*len(b)) {
		return genesisAllocationConfig{}, c.err
	}
	cfg := genesisAllocationConfig{graph: idalloc.GraphID(c.array()), topology: c.u64()}
	copy(cfg.declaration[:], c.take(32))
	cfg.home, cfg.maxBlock = c.u64(), c.u64()
	copy(cfg.schemas[:], c.take(32))
	axis := readAxis(&c, defaultMaterializerLimits())
	if version == 3 {
		copy(cfg.routingDigest[:], c.take(32))
	}
	if c.err != nil || len(c.b) != 0 {
		return genesisAllocationConfig{}, errors.Join(errCorrupt, c.err)
	}
	cfg.defaultAxis, err = types.BindDefaultAxis(types.GraphID(cfg.graph), axis, temporal.DefaultLimits())
	if err != nil || cfg.graph != graph || !cfg.valid() || cfg.version() != version {
		return genesisAllocationConfig{}, errors.Join(errCorrupt, err)
	}
	return cfg, nil
}

func (m *declaredMaterializer) readGenesisConfig(q *reader) (genesisAllocationConfig, error) {
	var wire []byte
	var found bool
	var err error
	if m.requiresDefaultAxis() {
		wire, found, err = q.getBounded(genesisConfigurationKey(m.ns), partitionGenesisConfigBytes)
	} else {
		wire, found, err = q.get(genesisConfigurationKey(m.ns))
	}
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	if !found {
		return genesisAllocationConfig{}, errCorrupt
	}
	// Admission precedes nested descriptor reconstruction. Work is cumulative,
	// distinct from the returned binding's immutable representation.
	if m.requiresDefaultAxis() {
		if err := chargeGraphWork(q, graphstore.PageWork{Bytes: genesisCodecMetadataBytes + 8*len(wire)}); err != nil {
			return genesisAllocationConfig{}, err
		}
	}
	cfg, err := decodeGenesisAllocationConfig(wire, m.ns.graph)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	if cfg.semanticContractID() != m.SemanticContractID() {
		return genesisAllocationConfig{}, errCorrupt
	}
	if err := cfg.checkGenesisDeclaration(m.declaration); err != nil {
		return genesisAllocationConfig{}, errors.Join(errCorrupt, err)
	}
	return cfg, nil
}

// PrepareGraphInitialization selects the explicit HOME binding or adopts that
// same binding from this HOME Host's completed configuration proof. It never
// derives IDs, synthesizes a default or promotes an allocation-only store.
func (h *allocationHost) PrepareGraphInitialization(partition uint64, attempt bootstrapAttemptID, schemas []graphstate.PropertyDefinition, maxBlock uint64, binding types.DefaultAxisBinding, genesis allocationProof) (allocationProposal, error) {
	if binding == (types.DefaultAxisBinding{}) {
		return allocationProposal{}, errors.Join(errInvalid, types.ErrInvalidGraphIdentity)
	}
	return h.prepareInitialization(partition, attempt, schemas, maxBlock, binding, genesis, graphGenesisSemanticContractID(), nil)
}

// The new genesis variant preserves V1 descriptor schemas but never admits
// descriptor equality/range/unique semantics. Legacy writeSchema stays unchanged.
func validGenesisSchema(d graphstate.PropertyDefinition, l materializerLimits, graphGenesis bool) bool {
	if !graphGenesis || d.Type != graphstate.ScalarDescriptor {
		return validSchema(d, l)
	}
	return validGraphName(d.Name, l) && (d.Owner == graphstate.Node || d.Owner == graphstate.Relationship) && d.Cardinality == graphstate.ScalarCardinality && d.Unique == graphstate.UniqueNone
}
func writeGenesisSchema(w *boundedWriter, d graphstate.PropertyDefinition, graphGenesis bool) {
	if graphGenesis && d.Type == graphstate.ScalarDescriptor {
		w.text(d.Name)
		w.add([]byte{byte(d.Owner), byte(d.Type), byte(d.Cardinality), byte(d.Unique)})
		return
	}
	writeSchema(w, d)
}

// Keep the decoded command/proof and all known outer result backing reserved
// through call completion. The decoder ledger covers fixed values, schema/axis
// ownership and temporary canonical encoders. The additional terms cover two
// sealed/copied config/allocator/outcome buffers and their key/header copies.
// This is conservative representation admission, not exact live heap/RSS.
func graphGenesisOuterOutput(r declaredInitCommand, l materializerLimits, base raftlog.ApplicationRoot) (int, error) {
	wireBytes, err := declaredInitWireBytes(r, l)
	if err != nil {
		return 0, err
	}
	configBytes := graphGenesisConfigBytes
	if r.version() == 6 {
		configBytes = partitionGenesisConfigBytes
	}
	return declaredInitDecodeCost(wireBytes, r.declaration.Len(), len(r.schemas)) + cap(base.Image) +
		4*(configBytes+idalloc.CheckpointSize+graphOutcomeBytes) + 4*43 + 64*4, nil
}
