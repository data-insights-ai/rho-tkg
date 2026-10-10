package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

// genesisAllocationConfig is immutable graph-qualified configuration. Home is
// chosen once by genesis-first-v1, then stored: topology additions and ownership
// moves must recover this configuration, never select a new minimum or reset a
// high-water. Allocation-service and recipient incarnations are separate state.
type genesisAllocationConfig struct {
	graph       idalloc.GraphID
	topology    uint64
	declaration [32]byte
	home        uint64
	maxBlock    uint64
	schemas     [32]byte
}

const genesisConfigBytes = 140
const genesisCodecMetadataBytes = 512
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
	return g.source.valid() && c.valid() && s.graph == c.graph && s.partition == c.home && s.topology == c.topology && s.declaration == c.declaration && g.epoch != 0 && g.effect != ([32]byte{})
}

func encodeGenesisObservation(g genesisObservation) ([]byte, error) {
	if !g.valid() {
		return nil, errInvalid
	}
	configuration, err := encodeGenesisAllocationConfig(g.configuration)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, genesisObservationBytes)
	b = append(b, 'A', 'G', 'I', 1)
	b = appendAllocationScope(b, g.source.scope)
	b = binary.BigEndian.AppendUint64(b, g.source.index)
	b = append(b, configuration...)
	b = binary.BigEndian.AppendUint64(b, g.epoch)
	b = append(b, g.effect[:]...)
	return seal(b), nil
}

func decodeGenesisObservation(b []byte) (genesisObservation, error) {
	body, err := wireBody(b, "AGI\x01", genesisObservationBytes)
	if err != nil {
		return genesisObservation{}, err
	}
	d := decoder{b: body}
	g := genesisObservation{source: allocationCoordinate{scope: readAllocationScope(&d), index: d.number()}}
	g.configuration, err = decodeGenesisAllocationConfig(d.b[:genesisConfigBytes], g.source.scope.graph)
	d.b = d.b[genesisConfigBytes:]
	g.epoch = d.number()
	copy(g.effect[:], d.b[:32])
	d.b = d.b[32:]
	if err != nil || len(d.b) != 0 || !g.valid() {
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
}

func (r declaredInitCommand) validate(l materializerLimits) (genesisAllocationConfig, error) {
	if !r.ns.valid() || r.attempt == (bootstrapAttemptID{}) || idalloc.GraphID(r.declaration.Graph()) != r.ns.graph {
		return genesisAllocationConfig{}, errInvalid
	}
	if _, found := r.declaration.Partition(r.ns.partition); !found {
		return genesisAllocationConfig{}, errInvalid
	}
	cfg, err := newGenesisAllocationConfig(r.declaration, r.schemas, r.maxBlock, l)
	if err != nil {
		return genesisAllocationConfig{}, err
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

const declaredInitMetadataBytes = 1024

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
		variant = genesisObservationBytes
	}
	n := requestHeaderBytes + 8 + 4 + 32*r.declaration.Len() + 32 + 1 + variant + 8 + 4 + sha256.Size
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

func emitDeclaredInit(w *boundedWriter, r declaredInitCommand) {
	w.add([]byte{'G', 'R', 'Q', 4, byte(initDeclaredPartition)})
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
	w.u32(len(r.schemas))
	for _, d := range r.schemas {
		writeSchema(w, d)
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
	wire, err := boundedEncoding(l.commandBytes, func(w *boundedWriter) { emitDeclaredInit(w, r) })
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
	if len(b) < requestHeaderBytes || !bytes.Equal(b[:5], []byte{'G', 'R', 'Q', 4, byte(initDeclaredPartition)}) {
		return declaredInitCommand{}, errCorrupt
	}
	body, err := graphBody(b, "GRQ\x04", l.commandBytes)
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
		wire := c.take(genesisObservationBytes)
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
	initialization := bytes.Equal(b[:5], []byte{'G', 'R', 'Q', 4, byte(initDeclaredPartition)})
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

func declaredReplay(q *reader, kind commandKind, identity [16]byte, hash [32]byte, index uint64) (outcome, bool, error) {
	wire, found, err := q.get(declaredRequestKey(q.ns, kind, identity))
	if err != nil || !found {
		return outcome{}, false, err
	}
	var old outcome
	if len(wire) >= 5 && (commandKind(wire[4]) == initDeclaredPartition || isAllocationProtocolCommand(commandKind(wire[4]))) {
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

func encodeDeclaredInitialization(r declaredInitCommand, cfg genesisAllocationConfig, maxBytes int) ([]byte, error) {
	digest, err := cfg.digest()
	if err != nil {
		return nil, err
	}
	return boundedEncoding(maxBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'D', 2})
		w.add(r.ns.graph[:])
		w.u64(r.ns.partition)
		w.u64(cfg.topology)
		w.u64(1) // immutable schema semantics, independent of physical indexes.
		w.add(cfg.declaration[:])
		w.u64(cfg.home)
		w.u64(cfg.maxBlock)
		w.add(cfg.schemas[:])
		w.add(digest[:])
		w.u32(len(r.schemas))
		for _, d := range r.schemas {
			writeSchema(w, d)
		}
	})
}

func (c genesisAllocationConfig) valid() bool {
	return c.graph != (idalloc.GraphID{}) && c.topology != 0 && c.declaration != ([32]byte{}) && c.home != 0 && c.maxBlock != 0 && c.maxBlock <= idalloc.MaxBlockSize && c.schemas != ([32]byte{})
}

func genesisSchemaDigest(graph idalloc.GraphID, schemas []graphstate.PropertyDefinition, l materializerLimits) ([32]byte, error) {
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
		if !validSchema(d, l) {
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
		w.add([]byte("rho-tkg:genesis-schemas:v1\x00"))
		w.add(graph[:])
		w.u32(len(schemas))
		for _, d := range schemas {
			writeSchema(w, d)
		}
	})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(wire[:len(wire)-sha256.Size]), nil
}

func newGenesisAllocationConfig(d graphstore.OwnershipDeclaration, schemas []graphstate.PropertyDefinition, maxBlock uint64, l materializerLimits) (genesisAllocationConfig, error) {
	if err := l.validate(); err != nil {
		return genesisAllocationConfig{}, err
	}
	first, found := d.PartitionAt(0)
	if !found || d.Graph() == (graphstate.GraphID{}) || d.TopologyEpoch() == 0 || d.Digest() == ([32]byte{}) || maxBlock == 0 || maxBlock > idalloc.MaxBlockSize {
		return genesisAllocationConfig{}, errInvalid
	}
	hash, err := genesisSchemaDigest(idalloc.GraphID(d.Graph()), schemas, l)
	if err != nil {
		return genesisAllocationConfig{}, err
	}
	return genesisAllocationConfig{graph: idalloc.GraphID(d.Graph()), topology: d.TopologyEpoch(), declaration: d.Digest(), home: first.Partition, maxBlock: maxBlock, schemas: hash}, nil
}

func encodeGenesisAllocationConfig(c genesisAllocationConfig) ([]byte, error) {
	if !c.valid() {
		return nil, errInvalid
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
	b := append([]byte("rho-tkg:genesis-allocation:v1\x00"), wire[:len(wire)-sha256.Size]...)
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
