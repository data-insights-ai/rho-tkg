package graphapply

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Logical allowances, not physical allocator/heap/RSS measurements.
// AxisDescriptor's 64-bit layout is56 bytes; PropertyDefinition's is24.
// Fixed metadata covers local frames, cursors, hash contexts and fixed scratch.
const registeredMetadataBytes = 2048
const registeredAxisBytes = 64
const registeredSchemaBytes = 32
const registeredPOSIXSource = "POSIX/Unix/millisecond/1970-01-01T00:00:00Z"

type registeredLedger struct{ walk, owned, limit int }

func (l *registeredLedger) charge(n int, owned bool) error {
	if n < 0 || n > l.limit-l.walk-l.owned {
		return errLimit
	}
	if owned {
		l.owned += n
	} else {
		l.walk += n
	}
	return nil
}
func registeredBudget(l registeredFormatBudget) error {
	for _, p := range [][2]int{{l.preparedBytes, 4 << 20}, {l.domainBytes, 4 << 20}, {l.axes, 1024}, {l.descriptorBytes, 4096}, {l.schemas, 4096}, {l.schemaNameBytes, 256}, {l.walkBytes, 32 << 20}} {
		if p[0] < 1 || p[0] > p[1] {
			return errInvalid
		}
	}
	return nil
}
func registeredStart(l registeredFormatBudget) (*registeredLedger, error) {
	if err := registeredBudget(l); err != nil {
		return nil, err
	}
	q := &registeredLedger{limit: l.walkBytes}
	return q, q.charge(registeredMetadataBytes, true)
}
func registeredDigest(parts ...[]byte) (out [32]byte) {
	h := sha256.New()
	for _, p := range parts {
		_, _ = h.Write(p)
	}
	h.Sum(out[:0])
	return out
}
func registeredOriginCheck(o registeredOriginScope, key [16]byte, q *registeredLedger) error {
	if o.graph == ([16]byte{}) || o.partition == 0 || o.group == ([16]byte{}) || key == ([16]byte{}) || o.protocol != 1 {
		return errInvalid
	}
	if err := q.charge(86, false); err != nil {
		return err
	}
	var scope [46]byte
	copy(scope[:6], "JGS1\x00\x01")
	copy(scope[6:22], o.graph[:])
	binary.BigEndian.PutUint64(scope[22:30], o.partition)
	copy(scope[30:], o.group[:])
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], 46)
	if registeredDigest([]byte("rho-tkg:registered-genesis-scope:v1\x00"), length[:], scope[:]) != o.lineage {
		return errInvalid
	}
	return nil
}
func registeredText(b []byte, q *registeredLedger) error {
	if err := q.charge(2*len(b), false); err != nil {
		return err
	}
	if !utf8.Valid(b) || len(bytes.TrimSpace(b)) == 0 {
		return errInvalid
	}
	return nil
}
func registeredSchema(d graphstate.PropertyDefinition) bool {
	return (d.Owner == graphstate.Node || d.Owner == graphstate.Relationship) &&
		d.Type >= graphstate.ScalarString && d.Type <= graphstate.ScalarScope &&
		(d.Cardinality == graphstate.ScalarCardinality || d.Cardinality == graphstate.SetCardinality) &&
		d.Unique <= graphstate.UniqueMembers &&
		(d.Unique != graphstate.UniqueScalar || d.Cardinality == graphstate.ScalarCardinality) &&
		(d.Unique != graphstate.UniqueMembers || d.Cardinality == graphstate.SetCardinality)
}

// Validate borrowed typed strings and reserve descriptor hash work before
// output allocation. Hashing later reads emitted descriptor slices directly.
func registeredTypedAxis(d temporal.AxisDescriptor, q *registeredLedger) error {
	if d.ID == (temporal.AxisID{}) || d.Profile < 1 || d.Profile > 3 || d.Version != 1 {
		return errInvalid
	}
	n := len(d.Reference) + len(d.CanonicalUnit)
	if err := q.charge(2*n+16+27+n, false); err != nil {
		return err
	}
	if !utf8.ValidString(d.Reference) || !utf8.ValidString(d.CanonicalUnit) || strings.TrimSpace(d.Reference) == "" || strings.TrimSpace(d.CanonicalUnit) == "" {
		return errInvalid
	}
	return nil
}
func registeredSizes(s registeredBootstrapSpec, l registeredFormatBudget) (int, int, error) {
	if len(s.domain.axes) > l.axes || len(s.schemas) > l.schemas {
		return 0, 0, errLimit
	}
	domain := 26
	for _, d := range s.domain.axes {
		if len(d.Reference) > l.descriptorBytes || len(d.CanonicalUnit) > l.descriptorBytes-len(d.Reference) || 27 > l.descriptorBytes-len(d.Reference)-len(d.CanonicalUnit) {
			return 0, 0, errLimit
		}
		n := 27 + len(d.Reference) + len(d.CanonicalUnit)
		if n > l.domainBytes-domain {
			return 0, 0, errLimit
		}
		domain += n
	}
	if s.domain.mapping.tag == 1 {
		if len(s.domain.mapping.sourceReference) > l.domainBytes-domain-55 {
			return 0, 0, errLimit
		}
		domain += 55 + len(s.domain.mapping.sourceReference)
	}
	total := 136 + 46 + domain
	for _, d := range s.schemas {
		if len(d.Name) > l.schemaNameBytes || len(d.Name)+8 > l.preparedBytes-total {
			return 0, 0, errLimit
		}
		total += 8 + len(d.Name)
	}
	if domain > l.domainBytes || total > l.preparedBytes {
		return 0, 0, errLimit
	}
	return total, domain, nil
}
func registeredMapping(m registeredMappingSpec, kind byte, id temporal.AxisID, profile temporal.Profile, unit string, q *registeredLedger) error {
	if kind == 1 && m == (registeredMappingSpec{}) {
		return nil
	}
	if err := q.charge(2*len(m.sourceReference), false); err != nil {
		return err
	}
	if kind != 2 || m.tag != 1 || m.version != 1 || m.sourceReference != registeredPOSIXSource || m.targetAxis != id || m.rule != 1 || profile == 3 || unit != "millisecond" {
		return errInvalid
	}
	return nil
}

// Typed input is borrowed. Count/length preflight precedes index backing,
// validation/hash scratch and output. Sorting never mutates caller containers.
// Hash reservations precede output; complete mapping digest verification uses
// emitted descriptor slices before any owned output can escape.
func encodePreparedBootstrap(o registeredOriginScope, key [16]byte, s registeredBootstrapSpec, l registeredFormatBudget) ([]byte, error) {
	q, err := registeredStart(l)
	if err != nil {
		return nil, err
	}
	n, domain, err := registeredSizes(s, l)
	if err != nil {
		return nil, err
	}
	if err := q.charge(n, false); err != nil {
		return nil, err
	}
	if err := registeredOriginCheck(o, key, q); err != nil {
		return nil, err
	}
	if s.kind < 1 || s.kind > 2 || s.revisionTag != 0 || s.domain.version != 1 || s.domain.flags != 0 || len(s.domain.axes) == 0 || s.allocator.tag != 1 || s.allocator.owner == ([16]byte{}) || s.allocator.epoch == 0 || s.allocator.maxBlock == 0 || s.allocator.maxBlock > 1<<20 {
		return nil, errInvalid
	}
	var defaultAxis temporal.AxisDescriptor
	var defaultHash [32]byte
	for _, d := range s.domain.axes {
		if err := registeredTypedAxis(d, q); err != nil {
			return nil, err
		}
		if d.ID == s.domain.defaultValidityAxis {
			defaultAxis = d
		}
	}
	if defaultAxis.ID == (temporal.AxisID{}) {
		return nil, errInvalid
	}
	if err := registeredMapping(s.domain.mapping, s.kind, defaultAxis.ID, defaultAxis.Profile, defaultAxis.CanonicalUnit, q); err != nil {
		return nil, err
	}
	for _, d := range s.schemas {
		if err := q.charge(2*len(d.Name), false); err != nil {
			return nil, err
		}
		if !registeredSchema(d) || !utf8.ValidString(d.Name) || strings.TrimSpace(d.Name) == "" {
			return nil, errInvalid
		}
	}
	if err := q.charge(2*(len(s.domain.axes)+len(s.schemas)), true); err != nil {
		return nil, err
	}
	axes, schemas := make([]uint16, len(s.domain.axes)), make([]uint16, len(s.schemas))
	for i := range axes {
		axes[i] = uint16(i)
	} // #nosec G115 -- count <=1024.
	for i := range schemas {
		schemas[i] = uint16(i)
	} // #nosec G115 -- count <=4096.
	var sortErr error
	axisCompare := func(a, b uint16) int {
		if sortErr != nil {
			return 0
		}
		sortErr = q.charge(32, false)
		if sortErr != nil {
			return 0
		}
		return bytes.Compare(s.domain.axes[a].ID[:], s.domain.axes[b].ID[:])
	}
	schemaCompare := func(a, b uint16) int {
		if sortErr != nil {
			return 0
		}
		x, y := s.schemas[a], s.schemas[b]
		cost := 2
		if x.Owner == y.Owner {
			cost += len(x.Name) + len(y.Name)
		}
		sortErr = q.charge(cost, false)
		if sortErr != nil {
			return 0
		}
		if c := cmp.Compare(x.Owner, y.Owner); c != 0 {
			return c
		}
		return strings.Compare(x.Name, y.Name)
	}
	slices.SortFunc(axes, axisCompare)
	slices.SortFunc(schemas, schemaCompare)
	for i := 1; i < len(axes); i++ {
		if axisCompare(axes[i-1], axes[i]) == 0 && sortErr == nil {
			return nil, errInvalid
		}
	}
	for i := 1; i < len(schemas); i++ {
		if schemaCompare(schemas[i-1], schemas[i]) == 0 && sortErr == nil {
			return nil, errInvalid
		}
	}
	if sortErr != nil {
		return nil, sortErr
	}
	if err := q.charge(n, true); err != nil {
		return nil, err
	}
	if err := q.charge(n, false); err != nil {
		return nil, err
	}
	b := make([]byte, 0, n)
	b = append(b, "PIJ1\x00\x01\x00"...)
	b = append(b, o.graph[:]...)
	b = binary.BigEndian.AppendUint64(b, o.partition)
	b = append(b, o.group[:]...)
	b = append(b, o.lineage[:]...)
	b = binary.BigEndian.AppendUint16(b, o.protocol)
	b = append(b, 1)
	b = append(b, key[:]...)
	b = append(b, s.kind)
	meaning := registeredSemanticContractID()
	b = append(b, meaning[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(n-136)) // #nosec G115 -- size <=4MiB.
	b = append(b, "BDS1\x00\x01\x00"...)
	b = binary.BigEndian.AppendUint32(b, uint32(domain)) // #nosec G115 -- domain <=4MiB.
	b = append(b, "DGP1\x00\x01\x00"...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(axes))) // #nosec G115 -- count <=1024.
	field := func(text string) {
		b = binary.BigEndian.AppendUint32(b, uint32(len(text))) // #nosec G115 -- lengths preflight <=4MiB.
		b = append(b, text...)
	}
	for _, index := range axes {
		d := s.domain.axes[index]
		start := len(b)
		b = append(b, d.ID[:]...)
		b = append(b, byte(d.Profile))
		b = binary.BigEndian.AppendUint16(b, d.Version)
		field(d.Reference)
		field(d.CanonicalUnit)
		digest := registeredDigest([]byte("rho-tkg:axis:v1\x00"), b[start:])
		if d.ID == s.domain.defaultValidityAxis {
			defaultHash = digest
		}
	}
	b = append(b, s.domain.defaultValidityAxis[:]...)
	b = append(b, s.domain.mapping.tag)
	if s.domain.mapping.tag == 1 {
		m := s.domain.mapping
		b = binary.BigEndian.AppendUint16(b, m.version)
		field(m.sourceReference)
		b = append(b, m.targetAxis[:]...)
		b = append(b, m.targetDefinitionHash[:]...)
		b = append(b, m.rule)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(schemas))) // #nosec G115 -- count <=4096.
	for _, index := range schemas {
		d := s.schemas[index]
		b = append(b, byte(d.Owner))
		field(d.Name)
		b = append(b, byte(d.Type), byte(d.Cardinality), byte(d.Unique))
	}
	b = append(b, s.allocator.tag)
	b = append(b, s.allocator.owner[:]...)
	b = binary.BigEndian.AppendUint64(b, s.allocator.epoch)
	b = binary.BigEndian.AppendUint64(b, s.allocator.maxBlock)
	b = append(b, s.revisionTag)
	if len(b) != n || cap(b) != n {
		return nil, errCorrupt
	}
	if s.domain.mapping.tag == 1 && s.domain.mapping.targetDefinitionHash != defaultHash {
		return nil, errInvalid
	}
	return b, nil
}

type registeredCursor struct {
	b   []byte
	q   *registeredLedger
	err error
}

func (c *registeredCursor) take(n int) []byte {
	if c.err != nil {
		return nil
	}
	if n < 0 || n > len(c.b) {
		c.err = errInvalid
		return nil
	}
	if c.q != nil {
		c.err = c.q.charge(n, false)
		if c.err != nil {
			return nil
		}
	}
	b := c.b[:n]
	c.b = c.b[n:]
	return b
}
func (c *registeredCursor) number(n int) uint64 {
	b := c.take(n)
	if c.err != nil {
		return 0
	}
	var out uint64
	for _, v := range b {
		out = out<<8 | uint64(v)
	}
	return out
}
func (c *registeredCursor) id() (out [16]byte)     { copy(out[:], c.take(16)); return out }
func (c *registeredCursor) digest() (out [32]byte) { copy(out[:], c.take(32)); return out }
func (c *registeredCursor) literal(s string) {
	if !bytes.Equal(c.take(len(s)), []byte(s)) && c.err == nil {
		c.err = errInvalid
	}
}
func (c *registeredCursor) field(maxBytes int, nested bool) []byte {
	n := c.number(4)
	if c.err != nil {
		return nil
	}
	if n > uint64(maxBytes) {
		c.err = errLimit
		return nil
	} // #nosec G115 -- positive format budget.
	if n > uint64(len(c.b)) {
		c.err = errInvalid
		return nil
	} // #nosec G115 -- bounded remaining bytes.
	if !nested {
		return c.take(int(n))
	} // #nosec G115 -- checked <=4MiB.
	b := c.b[:int(n)]  // #nosec G115 -- n <= validated maxBytes (4MiB) and len(c.b), so n fits int.
	c.b = c.b[int(n):] // #nosec G115 -- checked <=4MiB; child charges nested payload exactly once.
	return b
}
func (c *registeredCursor) count(maxCount, minimum, reserved int) int {
	n := c.number(2)
	if c.err != nil {
		return 0
	}
	if n > uint64(maxCount) {
		c.err = errLimit
		return 0
	} // #nosec G115 -- positive format maximum.
	if reserved > len(c.b) || int(n) > (len(c.b)-reserved)/minimum {
		c.err = errInvalid
		return 0
	} // #nosec G115 -- count <=4096.
	return int(n) // #nosec G115 -- count <=4096.
}

type registeredParsed struct {
	o                             registeredOriginScope
	key                           [16]byte
	s                             registeredBootstrapSpec
	domain, schema, mappingSource []byte
	axes, schemas                 int
	stringBytes                   int
}
type registeredAxisView struct {
	id      temporal.AxisID
	profile temporal.Profile
	unit    []byte
	digest  [32]byte
}

// Non-first defaults require a separately charged borrowed rescan. Count/length
// admission and all descriptor validation already happened; no container is owned.
func registeredFindDefault(domain []byte, count int, target temporal.AxisID, l registeredFormatBudget, q *registeredLedger) (registeredAxisView, error) {
	c := registeredCursor{b: domain[9:], q: q}
	for range count {
		start := c.b
		id := temporal.AxisID(c.id())
		profile := temporal.Profile(c.number(1)) // #nosec G115 -- number(1) reads one unsigned byte, <=255.
		c.number(2)
		c.field(l.descriptorBytes, false)
		unit := c.field(l.descriptorBytes, false)
		if c.err != nil {
			return registeredAxisView{}, c.err
		}
		if err := q.charge(32, false); err != nil {
			return registeredAxisView{}, err
		}
		if id == target {
			n := len(start) - len(c.b)
			if err := q.charge(16+n, false); err != nil {
				return registeredAxisView{}, err
			}
			return registeredAxisView{id, profile, unit, registeredDigest([]byte("rho-tkg:axis:v1\x00"), start[:n])}, nil
		}
	}
	return registeredAxisView{}, errInvalid
}

func registeredParse(b []byte, l registeredFormatBudget, q *registeredLedger) (registeredParsed, error) {
	fail := func(err error) (registeredParsed, error) { return registeredParsed{}, err }
	if len(b) < 136 {
		return fail(errInvalid)
	}
	if len(b) > l.preparedBytes {
		return fail(errLimit)
	}
	c := registeredCursor{b: b, q: q}
	c.literal("PIJ1\x00\x01\x00")
	o := registeredOriginScope{graph: c.id(), partition: c.number(8), group: c.id(), lineage: c.digest(), protocol: uint16(c.number(2))} // #nosec G115 -- protocol is a two-byte unsigned read, <=65535.
	class := c.number(1)
	key := c.id()
	kind := byte(c.number(1)) // #nosec G115 -- number(1) reads one unsigned byte, <=255.
	meaning := c.digest()
	if c.err != nil {
		return fail(c.err)
	}
	if class != 1 || kind < 1 || kind > 2 || meaning != [32]byte(registeredSemanticContractID()) {
		return fail(errInvalid)
	}
	if err := registeredOriginCheck(o, key, q); err != nil {
		return fail(err)
	}
	body := c.field(l.preparedBytes, true)
	revision := c.number(1)
	if c.err != nil {
		return fail(c.err)
	}
	if revision != 0 || len(c.b) != 0 {
		return fail(errInvalid)
	}
	bc := registeredCursor{b: body, q: q}
	bc.literal("BDS1\x00\x01\x00")
	domain := bc.field(l.domainBytes, true)
	dc := registeredCursor{b: domain, q: q}
	dc.literal("DGP1\x00\x01\x00")
	axisCount := dc.count(l.axes, 27, 17)
	if dc.err != nil {
		return fail(dc.err)
	}
	if axisCount == 0 {
		return fail(errInvalid)
	}
	var previous temporal.AxisID
	var chosen registeredAxisView
	stringBytes := 0
	addStrings := func(n int) error {
		if n < 0 || n > len(b)-stringBytes {
			return errInvalid
		}
		stringBytes += n
		return nil
	}
	for i := range axisCount {
		start := dc.b
		id := temporal.AxisID(dc.id())
		profile, version := temporal.Profile(dc.number(1)), dc.number(2) // #nosec G115 -- Profile is a one-byte unsigned read, <=255.
		reference, unit := dc.field(l.descriptorBytes, false), dc.field(l.descriptorBytes, false)
		if dc.err != nil {
			return fail(dc.err)
		}
		n := len(start) - len(dc.b)
		if n > l.descriptorBytes {
			return fail(errLimit)
		}
		if id == (temporal.AxisID{}) || profile < 1 || profile > 3 || version != 1 {
			return fail(errInvalid)
		}
		if err := registeredText(reference, q); err != nil {
			return fail(err)
		}
		if err := registeredText(unit, q); err != nil {
			return fail(err)
		}
		if err := addStrings(len(reference) + len(unit)); err != nil {
			return fail(err)
		}
		if i != 0 {
			if err := q.charge(32, false); err != nil {
				return fail(err)
			}
			if bytes.Compare(previous[:], id[:]) >= 0 {
				return fail(errInvalid)
			}
		}
		if err := q.charge(16+n, false); err != nil {
			return fail(err)
		}
		digest := registeredDigest([]byte("rho-tkg:axis:v1\x00"), start[:n])
		if i == 0 {
			chosen = registeredAxisView{id, profile, unit, digest}
		}
		previous = id
	}
	defaultID := temporal.AxisID(dc.id())
	if dc.err != nil {
		return fail(dc.err)
	}
	if defaultID != chosen.id {
		var err error
		chosen, err = registeredFindDefault(domain, axisCount, defaultID, l, q)
		if err != nil {
			return fail(err)
		}
	}
	m := registeredMappingSpec{tag: byte(dc.number(1))} // #nosec G115 -- tag is a one-byte unsigned read, <=255.
	var mappingSource []byte
	if m.tag == 1 {
		m.version = uint16(dc.number(2)) // #nosec G115 -- version is a two-byte unsigned read, <=65535.
		mappingSource = dc.field(l.domainBytes, false)
		m.targetAxis, m.targetDefinitionHash, m.rule = temporal.AxisID(dc.id()), dc.digest(), byte(dc.number(1)) // #nosec G115 -- rule is a one-byte unsigned read, <=255.
		if dc.err != nil {
			return fail(dc.err)
		}
		if err := q.charge(2*len(mappingSource), false); err != nil {
			return fail(err)
		}
		if kind != 2 || m.version != 1 || !bytes.Equal(mappingSource, []byte(registeredPOSIXSource)) || m.targetAxis != defaultID || m.targetDefinitionHash != chosen.digest || m.rule != 1 || chosen.profile == 3 || !bytes.Equal(chosen.unit, []byte("millisecond")) {
			return fail(errInvalid)
		}
		if err := addStrings(len(mappingSource)); err != nil {
			return fail(err)
		}
	} else if m.tag != 0 || kind != 1 {
		return fail(errInvalid)
	}
	if dc.err != nil {
		return fail(dc.err)
	}
	if len(dc.b) != 0 {
		return fail(errInvalid)
	}
	schemaCount := bc.count(l.schemas, 8, 33)
	schemaWire := bc.b
	var previousName []byte
	var previousOwner graphstate.EntityKind
	for i := range schemaCount {
		owner := graphstate.EntityKind(bc.number(1)) // #nosec G115 -- Owner is a one-byte unsigned read, <=255.
		name := bc.field(l.schemaNameBytes, false)
		d := graphstate.PropertyDefinition{Owner: owner, Type: graphstate.ScalarKind(bc.number(1)), Cardinality: graphstate.Cardinality(bc.number(1)), Unique: graphstate.UniqueMode(bc.number(1))} // #nosec G115 -- Type, Cardinality and Unique each read one unsigned byte, <=255.
		if bc.err != nil {
			return fail(bc.err)
		}
		if !registeredSchema(d) {
			return fail(errInvalid)
		}
		if err := registeredText(name, q); err != nil {
			return fail(err)
		}
		if err := addStrings(len(name)); err != nil {
			return fail(err)
		}
		if i != 0 {
			cost := 2
			if previousOwner == owner {
				cost += len(previousName) + len(name)
			}
			if err := q.charge(cost, false); err != nil {
				return fail(err)
			}
			if previousOwner > owner || previousOwner == owner && bytes.Compare(previousName, name) >= 0 {
				return fail(errInvalid)
			}
		}
		previousOwner, previousName = owner, name
	}
	a := registeredAllocatorIntent{tag: byte(bc.number(1)), owner: bc.id(), epoch: bc.number(8), maxBlock: bc.number(8)} // #nosec G115 -- tag is a one-byte unsigned read, <=255.
	if bc.err != nil {
		return fail(bc.err)
	}
	if len(bc.b) != 0 || a.tag != 1 || a.owner == ([16]byte{}) || a.epoch == 0 || a.maxBlock == 0 || a.maxBlock > 1<<20 {
		return fail(errInvalid)
	}
	s := registeredBootstrapSpec{kind: kind, domain: registeredDomainPolicy{version: 1, defaultValidityAxis: defaultID, mapping: m}, allocator: a}
	return registeredParsed{o, key, s, domain, schemaWire, mappingSource, axisCount, schemaCount, stringBytes}, nil
}

// Check immutable budget and wire size before reserving operation metadata.
func registeredReadStart(b []byte, l registeredFormatBudget) (*registeredLedger, error) {
	if err := registeredBudget(l); err != nil {
		return nil, err
	}
	if len(b) < 136 {
		return nil, errInvalid
	}
	if len(b) > l.preparedBytes {
		return nil, errLimit
	}
	return registeredStart(l)
}

func validatePreparedV1(b []byte, l registeredFormatBudget) error {
	q, err := registeredReadStart(b, l)
	if err != nil {
		return err
	}
	_, err = registeredParse(b, l, q)
	return err
}

func decodePreparedBootstrap(b []byte, l registeredFormatBudget) (registeredOriginScope, [16]byte, registeredBootstrapSpec, error) {
	q, err := registeredReadStart(b, l)
	if err != nil {
		return registeredOriginScope{}, [16]byte{}, registeredBootstrapSpec{}, err
	}
	p, err := registeredParse(b, l, q)
	if err != nil {
		return registeredOriginScope{}, [16]byte{}, registeredBootstrapSpec{}, err
	}
	// Reserve the COMPLETE materialization before any count-sized allocation:
	// one second structural walk, every string copy and all backing/string bytes.
	if err = q.charge(len(b)+p.stringBytes, false); err == nil {
		err = q.charge(registeredAxisBytes*p.axes+registeredSchemaBytes*p.schemas+p.stringBytes, true)
	}
	if err != nil {
		return registeredOriginScope{}, [16]byte{}, registeredBootstrapSpec{}, err
	}
	s := p.s
	s.domain.axes = make([]temporal.AxisDescriptor, p.axes)
	if p.schemas != 0 {
		s.schemas = make([]graphstate.PropertyDefinition, p.schemas)
	}
	c := registeredCursor{b: p.domain[9:]}
	for i := range s.domain.axes {
		d := temporal.AxisDescriptor{ID: temporal.AxisID(c.id()), Profile: temporal.Profile(c.number(1)), Version: uint16(c.number(2))} // #nosec G115 -- Profile reads one unsigned byte <=255; Version reads two unsigned bytes <=65535.
		d.Reference = string(c.field(l.descriptorBytes, false))
		d.CanonicalUnit = string(c.field(l.descriptorBytes, false))
		s.domain.axes[i] = d
	}
	c = registeredCursor{b: p.schema}
	for i := range s.schemas {
		d := graphstate.PropertyDefinition{Owner: graphstate.EntityKind(c.number(1))} // #nosec G115 -- Owner is a one-byte unsigned read, <=255.
		d.Name = string(c.field(l.schemaNameBytes, false))
		d.Type, d.Cardinality, d.Unique = graphstate.ScalarKind(c.number(1)), graphstate.Cardinality(c.number(1)), graphstate.UniqueMode(c.number(1)) // #nosec G115 -- Type, Cardinality and Unique each read one unsigned byte, <=255.
		s.schemas[i] = d
	}
	if s.domain.mapping.tag == 1 {
		s.domain.mapping.sourceReference = string(p.mappingSource)
	}
	return p.o, p.key, s, nil
}

func submissionProjectionV1(b []byte, l registeredFormatBudget) ([]byte, error) {
	q, err := registeredReadStart(b, l)
	if err != nil {
		return nil, err
	}
	if _, err = registeredParse(b, l, q); err != nil {
		return nil, err
	}
	if err = q.charge(len(b), true); err == nil {
		err = q.charge(len(b), false)
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(b))
	copy(out, b)
	copy(out[:4], "SIJ1")
	return out, nil
}

func hashPreparedV1(b []byte, l registeredFormatBudget) ([32]byte, [32]byte, error) {
	q, err := registeredReadStart(b, l)
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	if _, err = registeredParse(b, l, q); err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	if err = q.charge(31+4+len(b)+33+4+len(b), false); err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(b))) // #nosec G115 -- validated <=4MiB.
	ph := registeredDigest([]byte("rho-tkg:registered-prepared:v1\x00"), length[:], b)
	sh := registeredDigest([]byte("rho-tkg:registered-submission:v1\x00"), length[:], []byte("SIJ1"), b[4:])
	return sh, ph, nil
}
