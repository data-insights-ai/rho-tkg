package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// genesisAllocationConfig is immutable graph-qualified configuration. Home is
// chosen once by genesis-first-v1, then stored: topology additions and ownership
// moves must recover this configuration, never select a new minimum or reset a
// high-water. Allocation-service and recipient incarnations are separate state.
type genesisAllocationConfig struct {
	graph         idalloc.GraphID
	topology      uint64
	declaration   [32]byte
	home          uint64
	maxBlock      uint64
	schemas       [32]byte
	defaultAxis   types.DefaultAxisBinding
	routingDigest [32]byte
}

const genesisConfigBytes = 140
const genesisCodecMetadataBytes = 576
const initDeclaredPartition commandKind = 8

// genesisObservation is serialized source-produced inspection data. Only the
// allocation host's completed-read path can construct its opaque proof and
// authorize an initialization proposal from it. This codec is not that path.
type genesisObservation struct {
	source        allocationCoordinate
	configuration genesisAllocationConfig
	epoch         uint64
	effect        [32]byte
}

const genesisObservationBytes = 344

func (g genesisObservation) valid() bool {
	c := g.configuration
	s := g.source.scope
	return g.source.valid() && c.valid() && s.graph == c.graph && s.partition == c.home && s.topology == c.topology && s.declaration == c.declaration && s.semantic == c.semanticContractID() && g.epoch != 0 && g.effect != ([32]byte{})
}

func encodeGenesisObservation(g genesisObservation) ([]byte, error) {
	if !g.valid() {
		return nil, errInvalid
	}
	configuration, err := encodeGenesisAllocationConfig(g.configuration)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, g.wireBytes())
	b = append(b, 'A', 'G', 'I', g.configuration.version())
	b = appendAllocationScope(b, g.source.scope)
	b = binary.BigEndian.AppendUint64(b, g.source.index)
	b = append(b, configuration...)
	b = binary.BigEndian.AppendUint64(b, g.epoch)
	b = append(b, g.effect[:]...)
	return seal(b), nil
}

func decodeGenesisObservation(b []byte) (genesisObservation, error) {
	version, size := byte(1), genesisObservationBytes
	if len(b) >= 4 && b[3] == 2 {
		version, size = 2, graphGenesisObservationBytes
	}
	if len(b) >= 4 && b[3] == 3 {
		version, size = 3, partitionGenesisObservationBytes
	}
	body, err := wireBody(b, string([]byte{'A', 'G', 'I', version}), size)
	if err != nil {
		return genesisObservation{}, err
	}
	d := decoder{b: body}
	g := genesisObservation{source: allocationCoordinate{scope: readAllocationScope(&d), index: d.number()}}
	configBytes := size - (genesisObservationBytes - genesisConfigBytes)
	g.configuration, err = decodeGenesisAllocationConfig(d.b[:configBytes], g.source.scope.graph)
	d.b = d.b[configBytes:]
	g.epoch = d.number()
	copy(g.effect[:], d.b[:32])
	d.b = d.b[32:]
	if err != nil || len(d.b) != 0 || !g.valid() || g.configuration.version() != version {
		return genesisObservation{}, errCorrupt
	}
	return g, nil
}

type declaredInitCommand struct {
	ns          namespace
	attempt     bootstrapAttemptID
	declaration graphstore.OwnershipDeclaration
	authority   idalloc.Authority
	maxBlock    uint64
	schemas     []graphstate.PropertyDefinition
	genesis     genesisObservation
	defaultAxis types.DefaultAxisBinding
	routing     *graphstore.PartitionRouting
}

func (r declaredInitCommand) validate(l materializerLimits) (genesisAllocationConfig, error) {
	if !r.ns.valid() || r.attempt == (bootstrapAttemptID{}) || idalloc.GraphID(r.declaration.Graph()) != r.ns.graph {
		return genesisAllocationConfig{}, errInvalid
	}
	if _, found := r.declaration.Partition(r.ns.partition); !found {
		return genesisAllocationConfig{}, errInvalid
	}
	cfg, err := newGenesisAllocationConfigVersion(r.declaration, r.schemas, r.maxBlock, l, r.version() >= 5)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	if r.defaultAxis != (types.DefaultAxisBinding{}) {
		if err := r.defaultAxis.Check(types.GraphID(r.ns.graph), r.defaultAxis.Axis(), l.catalog.Temporal); err != nil {
			return genesisAllocationConfig{}, encodingFailure(errors.Join(errInvalid, err))
		}
		cfg.defaultAxis = r.defaultAxis
	}
	if r.routing != nil {
		if r.routing.Graph() != r.declaration.Graph() || r.routing.DeclarationDigest() != r.declaration.Digest() {
			return genesisAllocationConfig{}, errInvalid
		}
		cfg.routingDigest = r.routing.Digest()
	}
	if r.ns.partition == cfg.home {
		if r.authority.Owner() == ([16]byte{}) || r.authority.Epoch() == 0 || r.genesis != (genesisObservation{}) {
			return genesisAllocationConfig{}, errInvalid
		}
	} else {
		if r.authority != (idalloc.Authority{}) || !r.genesis.valid() || r.genesis.configuration != cfg {
			return genesisAllocationConfig{}, errors.Join(errInvalid, idalloc.ErrPayloadMismatch)
		}
		if err := r.genesis.source.scope.check(r.declaration, cfg); err != nil {
			return genesisAllocationConfig{}, err
		}
	}
	return cfg, nil
}

// Portable fixed reservation: command696 + config240 + two64-byte writers +
// cursor56 =1120 bytes on the supported64-bit layout, rounded up to1152.
// Descriptor/schema backing and nested codec scratch are separately8*wire.
const declaredInitMetadataBytes = 1152

func declaredInitDecodeCost(wireBytes, partitions, schemas int) int {
	return declaredInitMetadataBytes + 8*wireBytes + 64*partitions + 64*schemas
}

func declaredInitWireBytes(r declaredInitCommand, l materializerLimits) (int, error) {
	if err := l.validate(); err != nil {
		return 0, err
	}
	if r.declaration.Len() > (l.catalog.MaxRecordBytes-64)/32 || len(r.schemas) > l.maxSchemas {
		return 0, errLimit
	}
	variant := 24
	if r.genesis != (genesisObservation{}) {
		variant = r.genesis.wireBytes()
	}
	n := requestHeaderBytes + 8 + 4 + 32*r.declaration.Len() + 32 + 1 + variant + 8 + 4 + sha256.Size
	if r.defaultAxis != (types.DefaultAxisBinding{}) {
		n += defaultAxisDescriptorBytes
	}
	if r.routing != nil {
		size, err := r.routing.EncodedBytes(l.catalog)
		if err != nil {
			return 0, encodingFailure(err)
		}
		n += 4 + size
	}
	for _, d := range r.schemas {
		if len(d.Name) > l.commandBytes-n-8 {
			return 0, errLimit
		}
		n += 8 + len(d.Name)
	}
	if n > l.commandBytes || declaredInitDecodeCost(n, r.declaration.Len(), len(r.schemas)) > l.commandOwnedBytes {
		return 0, errLimit
	}
	return n, nil
}

func emitDeclaredInit(w *boundedWriter, r declaredInitCommand, l materializerLimits) {
	w.add([]byte{'G', 'R', 'Q', r.version(), byte(initDeclaredPartition)})
	w.add(r.ns.graph[:])
	w.u64(r.ns.partition)
	w.add(r.attempt[:])
	w.u64(r.declaration.TopologyEpoch())
	w.u32(r.declaration.Len())
	for i := range r.declaration.Len() {
		entry, _ := r.declaration.PartitionAt(i)
		w.u64(entry.Partition)
		w.u64(entry.OwnershipEpoch)
		w.add(entry.Group[:])
	}
	digest := r.declaration.Digest()
	w.add(digest[:])
	if r.genesis == (genesisObservation{}) {
		w.tag(1)
		w.add(appendAuthority(nil, r.authority))
	} else {
		w.tag(0)
		wire, err := encodeGenesisObservation(r.genesis)
		if err != nil {
			w.err = err
			return
		}
		w.add(wire)
	}
	w.u64(r.maxBlock)
	if r.version() >= 5 {
		writeAxis(w, r.defaultAxis.Axis(), l)
	}
	if r.routing != nil {
		wire, err := graphstore.EncodePartitionRouting(*r.routing, r.declaration, l.catalog)
		if err != nil {
			w.err = err
			return
		}
		w.field(wire)
	}
	w.u32(len(r.schemas))
	for _, d := range r.schemas {
		writeGenesisSchema(w, d, r.version() >= 5)
	}
}

func encodeDeclaredInit(r declaredInitCommand, l materializerLimits) ([]byte, error) {
	// Host admission uses the decoder's complete representation ledger, not a
	// smaller encoder-only allowance. Count/length inspection allocates nothing
	// and precedes schema hashing and nested observation encoding.
	wireBytes, err := declaredInitWireBytes(r, l)
	if err != nil {
		return nil, err
	}
	if _, err := r.validate(l); err != nil {
		return nil, err
	}
	wire, err := boundedEncoding(l.commandBytes, func(w *boundedWriter) { emitDeclaredInit(w, r, l) })
	if err != nil {
		return nil, err
	}
	if len(wire) != wireBytes {
		return nil, errCorrupt
	}
	return wire, nil
}

func decodeDeclaredInit(b []byte, l materializerLimits) (declaredInitCommand, error) {
	if err := l.validate(); err != nil {
		return declaredInitCommand{}, err
	}
	if len(b) > 4<<20 {
		return declaredInitCommand{}, errLimit
	}
	if len(b) < requestHeaderBytes || b[3] != 4 && b[3] != 5 && b[3] != 6 || !bytes.Equal(b[:3], []byte{'G', 'R', 'Q'}) || commandKind(b[4]) != initDeclaredPartition {
		return declaredInitCommand{}, errCorrupt
	}
	body, err := graphBody(b, string([]byte{'G', 'R', 'Q', b[3]}), l.commandBytes)
	if err != nil {
		return declaredInitCommand{}, err
	}
	c := graphCursor{b: body, maxOwnedBytes: l.commandOwnedBytes}
	if !c.charge(declaredInitMetadataBytes + 8*len(b)) {
		return declaredInitCommand{}, c.err
	}
	c.tag()
	r := declaredInitCommand{ns: namespace{graph: idalloc.GraphID(c.array()), partition: c.u64()}, attempt: bootstrapAttemptID(c.array())}
	topology := c.u64()
	count := c.count((l.catalog.MaxRecordBytes-64)/32, 32)
	if c.err != nil || !c.charge(64*count) {
		return declaredInitCommand{}, c.err
	}
	entries := make([]graphstore.PartitionOwnership, count)
	for i := range entries {
		entries[i] = graphstore.PartitionOwnership{Partition: c.u64(), OwnershipEpoch: c.u64(), Group: c.array()}
		if i > 0 && entries[i-1].Partition >= entries[i].Partition {
			return declaredInitCommand{}, errCorrupt
		}
	}
	var digest [32]byte
	copy(digest[:], c.take(32))
	if c.err != nil {
		return declaredInitCommand{}, c.err
	}
	r.declaration, err = graphstore.NewOwnershipDeclaration(graphstate.GraphID(r.ns.graph), topology, entries, l.catalog)
	if err != nil {
		if errors.Is(err, graphstore.ErrResourceLimit) {
			return declaredInitCommand{}, errors.Join(errLimit, err)
		}
		return declaredInitCommand{}, errors.Join(errCorrupt, err)
	}
	if r.declaration.Digest() != digest {
		return declaredInitCommand{}, errCorrupt
	}
	switch c.tag() {
	case 1:
		r.authority, err = idalloc.NewAuthority(c.array(), c.u64())
	case 0:
		observationBytes := genesisObservationBytes
		if b[3] >= 5 {
			observationBytes = graphGenesisObservationBytes
		}
		if b[3] == 6 {
			observationBytes = partitionGenesisObservationBytes
		}
		wire := c.take(observationBytes)
		if c.err != nil {
			return declaredInitCommand{}, c.err
		}
		r.genesis, err = decodeGenesisObservation(wire)
	default:
		return declaredInitCommand{}, errCorrupt
	}
	if err != nil {
		return declaredInitCommand{}, errors.Join(errCorrupt, err)
	}
	r.maxBlock = c.u64()
	if b[3] >= 5 {
		axis := readAxis(&c, l)
		if c.err != nil {
			return declaredInitCommand{}, encodingFailure(c.err)
		}
		r.defaultAxis, err = types.BindDefaultAxis(types.GraphID(r.ns.graph), axis, l.catalog.Temporal)
		if err != nil {
			return declaredInitCommand{}, encodingFailure(errors.Join(errCorrupt, err))
		}
	}
	if b[3] == 6 {
		wire := c.field(l.catalog.MaxRecordBytes)
		if c.err != nil {
			return declaredInitCommand{}, c.err
		}
		routing, err := graphstore.DecodePartitionRouting(wire, r.declaration, l.catalog)
		if err != nil {
			return declaredInitCommand{}, encodingFailure(errors.Join(errCorrupt, err))
		}
		r.routing = new(routing)
	}
	count = c.count(l.maxSchemas, 8)
	if c.err != nil || !c.charge(64*count) {
		return declaredInitCommand{}, c.err
	}
	r.schemas = make([]graphstate.PropertyDefinition, count)
	for i := range r.schemas {
		r.schemas[i] = readSchema(&c, l)
	}
	if c.err != nil {
		return declaredInitCommand{}, c.err
	}
	if len(c.b) != 0 {
		return declaredInitCommand{}, errCorrupt
	}
	// The canonical pass's temporary backing fits the original conservative
	// command ledger; it does not create a source observation or fresh authority.
	canonical, err := encodeDeclaredInit(r, l)
	if err != nil {
		return declaredInitCommand{}, errors.Join(errCorrupt, err)
	}
	if !bytes.Equal(b, canonical) {
		return declaredInitCommand{}, errCorrupt
	}
	return r, nil
}

func declaredCommandIdentity(b []byte, n namespace) ([16]byte, error) {
	if len(b) < requestHeaderBytes || len(b) > 4<<20 {
		return [16]byte{}, errCorrupt
	}
	initialization := (b[3] == 4 || b[3] == 5 || b[3] == 6) && bytes.Equal(b[:3], []byte{'G', 'R', 'Q'}) && commandKind(b[4]) == initDeclaredPartition
	allocation := bytes.Equal(b[:4], []byte{'A', 'Q', 'P', 1}) && isAllocationProtocolCommand(commandKind(b[4]))
	if !initialization && !allocation {
		return [16]byte{}, errCorrupt
	}
	d := decoder{b: b[5:requestHeaderBytes]}
	stored, id := d.ns(), d.array()
	if stored != n || id == ([16]byte{}) {
		return [16]byte{}, errInvalid
	}
	return id, nil
}

func encodeDeclaredOutcome(o outcome) ([]byte, error) {
	if !o.ns.valid() || o.kind != initDeclaredPartition && !isAllocationProtocolCommand(o.kind) || o.identity == ([16]byte{}) || o.hash == ([32]byte{}) || o.index == 0 || o.disposition < applied || o.disposition > controlRecovery || o.grant != (idalloc.Grant{}) || o.grantIndex != 0 {
		return nil, errInvalid
	}
	switch o.reason {
	case reasonNone, reasonInvalid, reasonMismatch:
	case reasonAlreadyInitialized:
		if o.kind != initDeclaredPartition {
			return nil, errInvalid
		}
	case reasonStale, reasonExhausted:
		if !isAllocationProtocolCommand(o.kind) {
			return nil, errInvalid
		}
	default:
		return nil, errInvalid
	}
	if o.kind == initDeclaredPartition && o.disposition != applied && o.disposition != requestReplay || o.disposition == grantRecovery && (o.kind != reserveDeclaredGrant || o.reason != reasonNone) || o.disposition == controlRecovery && (o.reason != reasonNone || !isAllocationProtocolCommand(o.kind) || o.kind == reserveDeclaredGrant) {
		return nil, errInvalid
	}
	b := appendNamespace([]byte{'G', 'R', 'O', 2, byte(o.kind)}, o.ns)
	b = append(b, o.identity[:]...)
	b = append(b, o.hash[:]...)
	b = binary.BigEndian.AppendUint64(b, o.index)
	b = append(b, byte(o.disposition))
	b = binary.BigEndian.AppendUint16(b, uint16(o.reason))
	return seal(b), nil
}

func decodeDeclaredOutcome(b []byte, n namespace) (outcome, error) {
	body, err := wireBody(b, "GRO\x02", graphOutcomeBytes)
	if err != nil {
		return outcome{}, err
	}
	d := decoder{b: body[1:]}
	o := outcome{kind: commandKind(body[0]), ns: d.ns(), identity: d.array()}
	copy(o.hash[:], d.b[:32])
	d.b = d.b[32:]
	o.index = d.number()
	o.disposition, o.reason = disposition(d.b[0]), reason(binary.BigEndian.Uint16(d.b[1:3]))
	d.b = d.b[3:]
	if o.ns != n || len(d.b) != 0 {
		return outcome{}, errCorrupt
	}
	if _, err := encodeDeclaredOutcome(o); err != nil {
		return outcome{}, errors.Join(errCorrupt, err)
	}
	return o, nil
}

func declaredRequestKey(n namespace, kind commandKind, identity [16]byte) []byte {
	if kind == initDeclaredPartition {
		return declaredAttemptKey(n, identity)
	}
	return append(recordKey(n, 4), identity[:]...)
}

func declaredReplay(q *reader, kind commandKind, identity [16]byte, hash [32]byte, index uint64, partition bool) (outcome, bool, error) {
	wire, found, err := q.get(declaredRequestKey(q.ns, kind, identity))
	if err != nil || !found {
		return outcome{}, false, err
	}
	var old outcome
	if len(wire) >= 5 && bytes.Equal(wire[:4], []byte{'G', 'R', 'O', 4}) {
		if !partition {
			return outcome{}, false, errCorrupt
		}
		old, err = decodePartitionGraphOutcome(wire, q.ns)
	} else if len(wire) >= 5 && (commandKind(wire[4]) == initDeclaredPartition || isAllocationProtocolCommand(commandKind(wire[4]))) {
		old, err = decodeDeclaredOutcome(wire, q.ns)
	} else {
		old, err = decodeAnyOutcome(wire, q.ns)
	}
	if err != nil {
		return outcome{}, false, err
	}
	if old.identity != identity || old.index > q.base.Index || old.disposition == requestReplay {
		return outcome{}, false, errCorrupt
	}
	if old.hash != hash {
		return outcome{ns: q.ns, kind: kind, identity: identity, hash: hash, index: index, disposition: applied, reason: reasonMismatch}, true, nil
	}
	if old.kind != kind {
		return outcome{}, false, errCorrupt
	}
	old.disposition = requestReplay
	return old, true, nil
}

func encodeDeclaredInitialization(r declaredInitCommand, cfg genesisAllocationConfig, maxBytes int, l materializerLimits) ([]byte, error) {
	digest, err := cfg.digest()
	if err != nil {
		return nil, err
	}
	return boundedEncoding(maxBytes, func(w *boundedWriter) {
		version := byte(2)
		if cfg.version() >= 2 {
			version = 3
		}
		if cfg.version() == 3 {
			version = 4
		}
		w.add([]byte{'G', 'C', 'D', version})
		w.add(r.ns.graph[:])
		w.u64(r.ns.partition)
		w.u64(cfg.topology)
		w.u64(1) // immutable schema semantics, independent of physical indexes.
		w.add(cfg.declaration[:])
		w.u64(cfg.home)
		w.u64(cfg.maxBlock)
		w.add(cfg.schemas[:])
		w.add(digest[:])
		if cfg.version() >= 2 {
			writeAxis(w, cfg.defaultAxis.Axis(), l)
		}
		if r.routing != nil {
			wire, err := graphstore.EncodePartitionRouting(*r.routing, r.declaration, l.catalog)
			if err != nil {
				w.err = err
				return
			}
			w.add(cfg.routingDigest[:])
			w.field(wire)
			w.u64(0) // fresh graph-round floor; control application indexes are separate.
		}
		w.u32(len(r.schemas))
		for _, d := range r.schemas {
			writeGenesisSchema(w, d, cfg.version() >= 2)
		}
	})
}

func (c genesisAllocationConfig) valid() bool {
	return c.graph != (idalloc.GraphID{}) && c.topology != 0 && c.declaration != ([32]byte{}) && c.home != 0 && c.maxBlock != 0 && c.maxBlock <= idalloc.MaxBlockSize && c.schemas != ([32]byte{}) && c.validDefaultAxis()
}

func genesisSchemaDigestVersion(graph idalloc.GraphID, schemas []graphstate.PropertyDefinition, l materializerLimits, graphGenesis bool) ([32]byte, error) {
	if err := l.validate(); err != nil {
		return [32]byte{}, err
	}
	if graph == (idalloc.GraphID{}) {
		return [32]byte{}, errInvalid
	}
	if len(schemas) > l.maxSchemas {
		return [32]byte{}, errLimit
	}
	// Count/length preflight precedes UTF-8 validation, hashing and encoding.
	// The two encoder buffers coexist during sealing; fixed scratch is charged
	// independently of wire bytes. Borrowed input definitions are not cloned.
	wireBytes := len("rho-tkg:genesis-schemas:v1\x00") + 16 + 4 + sha256.Size
	for _, d := range schemas {
		if len(d.Name) > l.commandBytes-wireBytes-8 {
			return [32]byte{}, errLimit
		}
		wireBytes += 8 + len(d.Name)
	}
	if wireBytes > l.commandBytes || wireBytes > (l.commandOwnedBytes-genesisCodecMetadataBytes)/2 {
		return [32]byte{}, errLimit
	}
	for i, d := range schemas {
		if len(d.Name) > l.catalog.MaxNameBytes {
			return [32]byte{}, errLimit
		}
		if !validGenesisSchema(d, l, graphGenesis) {
			return [32]byte{}, errInvalid
		}
		if i > 0 {
			old := schemas[i-1]
			if old.Owner > d.Owner || old.Owner == d.Owner && old.Name >= d.Name {
				return [32]byte{}, errInvalid
			}
		}
	}
	wire, err := boundedEncoding(l.commandBytes, func(w *boundedWriter) {
		domain := "rho-tkg:genesis-schemas:v1\x00"
		if graphGenesis {
			domain = "rho-tkg:genesis-schemas:v2\x00"
		}
		w.add([]byte(domain))
		w.add(graph[:])
		w.u32(len(schemas))
		for _, d := range schemas {
			writeGenesisSchema(w, d, graphGenesis)
		}
	})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(wire[:len(wire)-sha256.Size]), nil
}

func newGenesisAllocationConfig(d graphstore.OwnershipDeclaration, schemas []graphstate.PropertyDefinition, maxBlock uint64, l materializerLimits) (genesisAllocationConfig, error) {
	return newGenesisAllocationConfigVersion(d, schemas, maxBlock, l, false)
}
func newGenesisAllocationConfigVersion(d graphstore.OwnershipDeclaration, schemas []graphstate.PropertyDefinition, maxBlock uint64, l materializerLimits, graphGenesis bool) (genesisAllocationConfig, error) {
	if err := l.validate(); err != nil {
		return genesisAllocationConfig{}, err
	}
	first, found := d.PartitionAt(0)
	if !found || d.Graph() == (graphstate.GraphID{}) || d.TopologyEpoch() == 0 || d.Digest() == ([32]byte{}) || maxBlock == 0 || maxBlock > idalloc.MaxBlockSize {
		return genesisAllocationConfig{}, errInvalid
	}
	hash, err := genesisSchemaDigestVersion(idalloc.GraphID(d.Graph()), schemas, l, graphGenesis)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	return genesisAllocationConfig{graph: idalloc.GraphID(d.Graph()), topology: d.TopologyEpoch(), declaration: d.Digest(), home: first.Partition, maxBlock: maxBlock, schemas: hash}, nil
}

func encodeGenesisAllocationConfig(c genesisAllocationConfig) ([]byte, error) {
	if !c.valid() {
		return nil, errInvalid
	}
	if c.version() >= 2 {
		return encodeGraphGenesisConfig(c)
	}
	b := make([]byte, 0, genesisConfigBytes)
	b = append(b, 'G', 'A', 'C', 1)
	b = append(b, c.graph[:]...)
	b = binary.BigEndian.AppendUint64(b, c.topology)
	b = append(b, c.declaration[:]...)
	b = binary.BigEndian.AppendUint64(b, c.home)
	b = binary.BigEndian.AppendUint64(b, c.maxBlock)
	b = append(b, c.schemas[:]...)
	return seal(b), nil
}

func decodeGenesisAllocationConfig(b []byte, graph idalloc.GraphID) (genesisAllocationConfig, error) {
	if len(b) >= 4 && (b[3] == 2 || b[3] == 3) {
		return decodeGraphGenesisConfig(b, graph)
	}
	body, err := wireBody(b, "GAC\x01", genesisConfigBytes)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	d := decoder{b: body}
	c := genesisAllocationConfig{graph: idalloc.GraphID(d.array()), topology: d.number()}
	copy(c.declaration[:], d.b[:32])
	d.b = d.b[32:]
	c.home, c.maxBlock = d.number(), d.number()
	copy(c.schemas[:], d.b[:32])
	d.b = d.b[32:]
	if len(d.b) != 0 || c.graph != graph || !c.valid() {
		return genesisAllocationConfig{}, errCorrupt
	}
	return c, nil
}

func (c genesisAllocationConfig) digest() ([32]byte, error) {
	wire, err := encodeGenesisAllocationConfig(c)
	if err != nil {
		return [32]byte{}, err
	}
	domain := "rho-tkg:genesis-allocation:v1\x00"
	if c.version() >= 2 {
		domain = "rho-tkg:graph-genesis:v1\x00"
	}
	if c.version() == 3 {
		domain = "rho-tkg:partition-genesis:v1\x00"
	}
	b := append([]byte(domain), wire[:len(wire)-sha256.Size]...)
	return sha256.Sum256(b), nil
}

func (c genesisAllocationConfig) checkGenesisDeclaration(d graphstore.OwnershipDeclaration) error {
	first, found := d.PartitionAt(0)
	if !c.valid() || !found || idalloc.GraphID(d.Graph()) != c.graph || d.TopologyEpoch() != c.topology || d.Digest() != c.declaration || first.Partition != c.home {
		// This version has no authority-home/topology migration. A new declaration
		// cannot silently recreate configuration or an independent allocator.
		return errInvalid
	}
	return nil
}

func sameGenesisConfiguration(a, b []byte, graph idalloc.GraphID) (genesisAllocationConfig, error) {
	c, err := decodeGenesisAllocationConfig(a, graph)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	other, err := decodeGenesisAllocationConfig(b, graph)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	if c != other || !bytes.Equal(a, b) {
		return genesisAllocationConfig{}, errors.Join(errInvalid, idalloc.ErrPayloadMismatch)
	}
	return c, nil
}
