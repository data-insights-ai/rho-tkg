package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const (
	rootBytes                = 116 // Primitive v1 image; retained for byte-for-byte compatibility.
	singlePartitionRootBytes = rootBytes + 3*8
)

// EncodeRoot returns a constant-size owned provisional local root image. Its
// checksum protects bytes; it does not certify effects or distributed closure.
func EncodeRoot(r Root) ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	header := []byte{'G', 'R', 1, 1}
	if r.topology != (topologyDeclaration{}) {
		header = []byte{'G', 'R', 2, 2}
	}
	b := append(header, r.namespace.Graph[:]...)
	for _, n := range []uint64{r.namespace.Partition, r.owner, r.epoch, r.next} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	b = append(b, r.effect[:]...)
	if r.topology != (topologyDeclaration{}) {
		for _, n := range []uint64{r.topology.epoch, r.topology.schema, r.topology.index} {
			b = binary.BigEndian.AppendUint64(b, n)
		}
	}
	digest := sha256.Sum256(b)
	return exactCopy(append(b, digest[:]...)), nil
}

// DecodeRoot rejects unknown formats/modes, truncation and trailing bytes. It
// retains no input alias. Routing/ownership are checked by OpenCatalog.
func DecodeRoot(src []byte) (Root, error) {
	primitive := len(src) == rootBytes && bytes.Equal(src[:4], []byte{'G', 'R', 1, 1})
	single := len(src) == singlePartitionRootBytes && bytes.Equal(src[:4], []byte{'G', 'R', 2, 2})
	if !primitive && !single {
		return Root{}, ErrCorrupt
	}
	bodyBytes := len(src) - sha256.Size
	sum := sha256.Sum256(src[:bodyBytes])
	if !bytes.Equal(src[bodyBytes:], sum[:]) {
		return Root{}, ErrCorrupt
	}
	var r Root
	copy(r.namespace.Graph[:], src[4:20])
	r.namespace.Partition = binary.BigEndian.Uint64(src[20:28])
	r.owner = binary.BigEndian.Uint64(src[28:36])
	r.epoch = binary.BigEndian.Uint64(src[36:44])
	r.next = binary.BigEndian.Uint64(src[44:52])
	copy(r.effect[:], src[52:84])
	if single {
		r.topology = topologyDeclaration{binary.BigEndian.Uint64(src[84:92]), binary.BigEndian.Uint64(src[92:100]), binary.BigEndian.Uint64(src[100:108])}
		if r.topology != bootstrapTopology && r.topology != keysOnlyTopology && !isFullTopology(r.topology) {
			return Root{}, ErrCorrupt
		}
	}
	if err := r.validate(); err != nil {
		return Root{}, ErrCorrupt
	}
	return r, nil
}

func recordHeader(n Namespace, kind recordKind) []byte {
	b := append([]byte{'G', 'C', 1, byte(kind)}, n.Graph[:]...)
	return binary.BigEndian.AppendUint64(b, n.Partition)
}

type cursor struct{ src []byte }

func (c *cursor) take(n int) ([]byte, error) {
	if n < 0 || n > len(c.src) {
		return nil, ErrCorrupt
	}
	v := c.src[:n]
	c.src = c.src[n:]
	return v, nil
}
func (c *cursor) number() (uint64, error) {
	b, err := c.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}
func (c *cursor) tag() (byte, error) {
	b, err := c.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}
func (c *cursor) field(maxBytes int) ([]byte, error) {
	b, err := c.take(4)
	if err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(b)
	if uint64(n) > uint64(len(c.src)) {
		return nil, ErrCorrupt
	}
	if uint64(n) > uint64(maxBytes) {
		return nil, ErrResourceLimit
	}
	return c.take(int(n))
} // #nosec G115 -- n bounded by delivered input and positive validated record limit.
func (c *cursor) done() error {
	if len(c.src) != 0 {
		return ErrCorrupt
	}
	return nil
}
func inspectRecord(src []byte, n Namespace, kind recordKind, l Limits) (cursor, error) {
	if len(src) > l.MaxRecordBytes {
		return cursor{}, ErrResourceLimit
	}
	if len(src) < 28 || !bytes.Equal(src[:4], []byte{'G', 'C', 1, byte(kind)}) || !bytes.Equal(src[4:20], n.Graph[:]) || binary.BigEndian.Uint64(src[20:28]) != n.Partition {
		return cursor{}, ErrCorrupt
	}
	return cursor{src[28:]}, nil
}
func appendField(dst []byte, data []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(data)))
	return append(dst, data...)
} // #nosec G115 -- fields checked against <= 1 MiB limits before encoding.
func validName(name string, l Limits) bool {
	return len(name) > 0 && len(name) <= l.MaxNameBytes && utf8.ValidString(name) && strings.TrimSpace(name) != ""
}
func checkRecord(b []byte, l Limits) ([]byte, error) {
	if len(b) > l.MaxRecordBytes {
		return nil, ErrResourceLimit
	}
	return b, nil
}
func appendAxis(dst []byte, a temporal.Axis, l Limits) ([]byte, error) {
	d := a.Descriptor()
	validated, err := temporal.NewAxis(d, l.Temporal)
	if err != nil {
		return nil, err
	}
	if validated.DefinitionHash() != a.DefinitionHash() {
		return nil, ErrInvalid
	}
	dst = append(dst, d.ID[:]...)
	dst = append(dst, byte(d.Profile))
	dst = binary.BigEndian.AppendUint16(dst, d.Version)
	dst = appendField(dst, []byte(d.Reference))
	dst = appendField(dst, []byte(d.CanonicalUnit))
	hash := a.DefinitionHash()
	return append(dst, hash[:]...), nil
}
func decodeAxis(c *cursor, l Limits) (temporal.Axis, error) {
	id, err := c.take(16)
	if err != nil {
		return temporal.Axis{}, err
	}
	profile, err := c.tag()
	if err != nil {
		return temporal.Axis{}, err
	}
	version, err := c.take(2)
	if err != nil {
		return temporal.Axis{}, err
	}
	reference, err := c.field(l.Temporal.MaxDescriptorBytes)
	if err != nil {
		return temporal.Axis{}, err
	}
	unit, err := c.field(l.Temporal.MaxDescriptorBytes)
	if err != nil {
		return temporal.Axis{}, err
	}
	hash, err := c.take(32)
	if err != nil {
		return temporal.Axis{}, err
	}
	var d temporal.AxisDescriptor
	copy(d.ID[:], id)
	d.Profile = temporal.Profile(profile)
	d.Version = binary.BigEndian.Uint16(version)
	d.Reference = string(reference)
	d.CanonicalUnit = string(unit)
	a, err := temporal.NewAxis(d, l.Temporal)
	if err != nil {
		if errors.Is(err, temporal.ErrResourceLimit) {
			return temporal.Axis{}, ErrResourceLimit
		}
		return temporal.Axis{}, ErrCorrupt
	}
	actual := a.DefinitionHash()
	if !bytes.Equal(hash, actual[:]) {
		return temporal.Axis{}, ErrCorrupt
	}
	return a, nil
}
func encodeAxis(n Namespace, a temporal.Axis, l Limits) ([]byte, error) {
	b, err := appendAxis(recordHeader(n, axisRecord), a, l)
	if err != nil {
		return nil, callerError(err)
	}
	return checkRecord(b, l)
}
func readAxis(src []byte, n Namespace, l Limits) (temporal.Axis, error) {
	c, err := inspectRecord(src, n, axisRecord, l)
	if err != nil {
		return temporal.Axis{}, err
	}
	a, err := decodeAxis(&c, l)
	if err != nil {
		return temporal.Axis{}, err
	}
	if err := c.done(); err != nil {
		return temporal.Axis{}, err
	}
	return a, nil
}
func validProperty(d graphstate.PropertyDefinition, l Limits) bool {
	return validName(d.Name, l) && (d.Owner == graphstate.Node || d.Owner == graphstate.Relationship) && d.Type >= graphstate.ScalarString && d.Type <= graphstate.ScalarScope && (d.Cardinality == graphstate.ScalarCardinality || d.Cardinality == graphstate.SetCardinality) && d.Unique <= graphstate.UniqueMembers && (d.Unique != graphstate.UniqueScalar || d.Cardinality == graphstate.ScalarCardinality) && (d.Unique != graphstate.UniqueMembers || d.Cardinality == graphstate.SetCardinality)
}
func encodeProperty(n Namespace, d graphstate.PropertyDefinition, l Limits) ([]byte, error) {
	if !validProperty(d, l) {
		return nil, ErrInvalid
	}
	b := append(recordHeader(n, schemaRecord), byte(d.Owner), byte(d.Type), byte(d.Cardinality), byte(d.Unique))
	return checkRecord(appendField(b, []byte(d.Name)), l)
}
func readProperty(src []byte, n Namespace, l Limits) (graphstate.PropertyDefinition, error) {
	c, err := inspectRecord(src, n, schemaRecord, l)
	if err != nil {
		return graphstate.PropertyDefinition{}, err
	}
	tags, err := c.take(4)
	if err != nil {
		return graphstate.PropertyDefinition{}, err
	}
	name, err := c.field(l.MaxNameBytes)
	if err != nil {
		return graphstate.PropertyDefinition{}, err
	}
	d := graphstate.PropertyDefinition{Name: string(name), Owner: graphstate.EntityKind(tags[0]), Type: graphstate.ScalarKind(tags[1]), Cardinality: graphstate.Cardinality(tags[2]), Unique: graphstate.UniqueMode(tags[3])}
	if !validProperty(d, l) || c.done() != nil {
		return graphstate.PropertyDefinition{}, ErrCorrupt
	}
	return d, nil
}
func validEntity(r graphstate.EntityRecord, l Limits) bool {
	if r.ID == 0 {
		return false
	}
	switch r.Kind {
	case graphstate.Node:
		return r.Type == "" && r.Source == 0 && r.Target == 0 && r.Mode == 0
	case graphstate.Relationship:
		return validName(r.Type, l) && r.Source != 0 && r.Target != 0 && (r.Mode == graphstate.LifeBound || r.Mode == graphstate.IdentityReference)
	}
	return false
}
func encodeEntity(n Namespace, r graphstate.EntityRecord, l Limits) ([]byte, error) {
	if !validEntity(r, l) {
		return nil, ErrInvalid
	}
	b := binary.BigEndian.AppendUint64(recordHeader(n, entityRecord), uint64(r.ID))
	b = append(b, byte(r.Kind), byte(r.Mode))
	b = binary.BigEndian.AppendUint64(b, uint64(r.Source))
	b = binary.BigEndian.AppendUint64(b, uint64(r.Target))
	b = appendField(b, []byte(r.Type))
	b, err := appendAxis(b, r.Axis, l)
	if err != nil {
		return nil, callerError(err)
	}
	return checkRecord(b, l)
}
func readEntity(src []byte, n Namespace, l Limits) (graphstate.EntityRecord, error) {
	c, err := inspectRecord(src, n, entityRecord, l)
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	id, err := c.number()
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	tags, err := c.take(2)
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	source, err := c.number()
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	target, err := c.number()
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	name, err := c.field(l.MaxNameBytes)
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	axis, err := decodeAxis(&c, l)
	if err != nil {
		return graphstate.EntityRecord{}, err
	}
	r := graphstate.EntityRecord{ID: graphstate.EntityID(id), Kind: graphstate.EntityKind(tags[0]), Mode: graphstate.ReferenceMode(tags[1]), Source: graphstate.EntityID(source), Target: graphstate.EntityID(target), Type: string(name), Axis: axis}
	if !validEntity(r, l) || c.done() != nil {
		return graphstate.EntityRecord{}, ErrCorrupt
	}
	return r, nil
}
func encodeLife(n Namespace, r graphstate.LifeRecord, l Limits) ([]byte, error) {
	if r.Owner == 0 || r.Life == 0 {
		return nil, ErrInvalid
	}
	b := recordHeader(n, lifeRecord)
	for _, v := range []uint64{uint64(r.Owner), uint64(r.Life), uint64(r.SourceLife), uint64(r.TargetLife)} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	return checkRecord(b, l)
}
func readLife(src []byte, n Namespace, l Limits) (graphstate.LifeRecord, error) {
	c, err := inspectRecord(src, n, lifeRecord, l)
	if err != nil {
		return graphstate.LifeRecord{}, err
	}
	var fields [4]uint64
	for i := range fields {
		fields[i], err = c.number()
		if err != nil {
			return graphstate.LifeRecord{}, err
		}
	}
	r := graphstate.LifeRecord{Owner: graphstate.EntityID(fields[0]), Life: graphstate.LifeID(fields[1]), SourceLife: graphstate.LifeID(fields[2]), TargetLife: graphstate.LifeID(fields[3])}
	if r.Owner == 0 || r.Life == 0 || c.done() != nil {
		return graphstate.LifeRecord{}, ErrCorrupt
	}
	return r, nil
}
func encodeValue(n Namespace, ref ValueRef, v graphstate.Scalar, key string, ordinal uint64, l Limits) ([]byte, error) {
	if ref.ID == 0 || len(key) > l.MaxValueBytes {
		return nil, ErrInvalid
	}
	b := binary.BigEndian.AppendUint64(recordHeader(n, valueRecord), uint64(ref.ID))
	b = binary.BigEndian.AppendUint64(b, ordinal)
	b = appendField(b, []byte(key))
	if axis, ok := v.Scope(); ok {
		b = append(b, 1)
		var err error
		b, err = appendAxis(b, axis.Axis(), l)
		if err != nil {
			return nil, callerError(err)
		}
	} else {
		b = append(b, 0)
	}
	return checkRecord(b, l)
}
func decodeScalar(key []byte, axis temporal.Axis, hasAxis bool, l Limits) (graphstate.Scalar, error) {
	if len(key) == 0 || len(key) > l.MaxValueBytes {
		return graphstate.Scalar{}, ErrCorrupt
	}
	kind := graphstate.ScalarKind(key[0])
	if hasAxis != (kind == graphstate.ScalarScope) {
		return graphstate.Scalar{}, ErrCorrupt
	}
	switch kind {
	case graphstate.ScalarNull:
		if len(key) == 1 {
			return graphstate.Null(), nil
		}
	case graphstate.ScalarString:
		return graphstate.String(string(key[1:])), nil
	case graphstate.ScalarBool:
		if len(key) == 2 && key[1] <= 1 {
			return graphstate.Bool(key[1] == 1), nil
		}
	case graphstate.ScalarI64:
		if len(key) == 9 {
			return graphstate.I64(int64(binary.BigEndian.Uint64(key[1:]))), nil
		} // #nosec G115 -- exact two's-complement payload.
	case graphstate.ScalarF64:
		if len(key) == 9 {
			bits := binary.BigEndian.Uint64(key[1:])
			if bits != 1<<63 {
				v, err := graphstate.F64(math.Float64frombits(bits))
				if err == nil {
					return v, nil
				}
			}
		}
	case graphstate.ScalarScope:
		s, err := temporal.DecodeScope(key[1:], axis, l.Temporal)
		if err == nil {
			return graphstate.ScopeValue(s), nil
		}
		if errors.Is(err, temporal.ErrResourceLimit) {
			return graphstate.Scalar{}, ErrResourceLimit
		}
	}
	return graphstate.Scalar{}, ErrCorrupt
}
func readValue(src []byte, n Namespace, l Limits) (ValueEntry, []byte, uint64, error) {
	c, err := inspectRecord(src, n, valueRecord, l)
	if err != nil {
		return ValueEntry{}, nil, 0, err
	}
	id, err := c.number()
	if err != nil || id == 0 {
		return ValueEntry{}, nil, 0, ErrCorrupt
	}
	ordinal, err := c.number()
	if err != nil {
		return ValueEntry{}, nil, 0, err
	}
	key, err := c.field(l.MaxValueBytes)
	if err != nil {
		return ValueEntry{}, nil, 0, err
	}
	tag, err := c.tag()
	if err != nil || tag > 1 {
		return ValueEntry{}, nil, 0, ErrCorrupt
	}
	var axis temporal.Axis
	if tag == 1 {
		axis, err = decodeAxis(&c, l)
		if err != nil {
			return ValueEntry{}, nil, 0, err
		}
	}
	v, err := decodeScalar(key, axis, tag == 1, l)
	if err != nil {
		return ValueEntry{}, nil, 0, err
	}
	if c.done() != nil {
		return ValueEntry{}, nil, 0, ErrCorrupt
	}
	size := len(key)
	if tag == 1 {
		d := axis.Descriptor()
		size += 27 + len(d.Reference) + len(d.CanonicalUnit)
	}
	return ValueEntry{Ref: ValueRef{n.Graph, graphstate.ValueID(id)}, Value: v, PayloadBytes: uint64(size), ordinal: ordinal}, key, ordinal, nil
} // #nosec G115 -- size derived from delivered <= 1 MiB record bytes and bounded axis descriptor.
func encodeNumber(n Namespace, kind recordKind, v uint64) []byte {
	return binary.BigEndian.AppendUint64(recordHeader(n, kind), v)
}
func readNumber(src []byte, n Namespace, kind recordKind, l Limits) (uint64, error) {
	c, err := inspectRecord(src, n, kind, l)
	if err != nil {
		return 0, err
	}
	v, err := c.number()
	if err != nil || c.done() != nil {
		return 0, ErrCorrupt
	}
	return v, nil
}
