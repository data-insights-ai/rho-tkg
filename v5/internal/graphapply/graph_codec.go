package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const graphOutcomeBytes = 120

// boundedWriter never grows beyond the finite representation bound. A sizing
// pass uses the same traversal without retaining buffers, before final allocation.
type boundedWriter struct {
	b      []byte
	n, max int
	err    error
	sizing bool
}

func (w *boundedWriter) add(b []byte) {
	if w.err != nil {
		return
	}
	if len(b) > w.max-w.n {
		w.err = errLimit
		return
	}
	w.n += len(b)
	if !w.sizing {
		w.b = append(w.b, b...)
	}
}
func (w *boundedWriter) tag(b byte) { w.add([]byte{b}) }
func (w *boundedWriter) u16(n uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], n)
	w.add(b[:])
}
func (w *boundedWriter) u32(n int) {
	if n < 0 || n > 64<<20 {
		w.err = errLimit
		return
	}
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(n))
	w.add(b[:])
} // #nosec G115 -- n checked <=64MiB.
func (w *boundedWriter) u64(n uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	w.add(b[:])
}
func (w *boundedWriter) text(s string) {
	w.u32(len(s))
	if w.err != nil {
		return
	}
	if len(s) > w.max-w.n {
		w.err = errLimit
		return
	}
	w.n += len(s)
	if !w.sizing {
		w.b = append(w.b, s...)
	}
}
func (w *boundedWriter) field(b []byte) { w.u32(len(b)); w.add(b) }
func encodingFailure(err error) error {
	if errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, state.ErrResourceLimit) || errors.Is(err, graphstate.ErrResourceLimit) {
		return errors.Join(errLimit, err)
	}
	return err
}
func boundedEncoding(maxBytes int, emit func(*boundedWriter)) ([]byte, error) {
	w := boundedWriter{max: maxBytes - 32, sizing: true}
	emit(&w)
	if w.err != nil {
		return nil, encodingFailure(w.err)
	}
	out := boundedWriter{max: maxBytes - 32, b: make([]byte, 0, w.n+32)}
	emit(&out)
	if out.err != nil {
		return nil, encodingFailure(out.err)
	}
	return seal(out.b), nil
}

type graphCursor struct {
	b                         []byte
	err                       error
	ownedBytes, maxOwnedBytes int
}

func (c *graphCursor) charge(n int) bool {
	if c.err != nil {
		return false
	}
	if n < 0 || n > c.maxOwnedBytes-c.ownedBytes {
		c.err = errLimit
		return false
	}
	c.ownedBytes += n
	return true
}
func (c *graphCursor) take(n int) []byte {
	if c.err != nil {
		return nil
	}
	if n < 0 || n > len(c.b) {
		c.err = errCorrupt
		return nil
	}
	b := c.b[:n]
	c.b = c.b[n:]
	return b
}
func (c *graphCursor) tag() byte {
	b := c.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (c *graphCursor) u16() uint16 {
	b := c.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (c *graphCursor) u64() uint64 {
	b := c.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
func (c *graphCursor) count(maxCount, minBytes int) int {
	b := c.take(4)
	if b == nil {
		return 0
	}
	n := binary.BigEndian.Uint32(b)
	if uint64(n) > uint64(maxCount) {
		c.err = errLimit
		return 0
	}
	if minBytes > 0 && uint64(n) > uint64(len(c.b)/minBytes) {
		c.err = errCorrupt
		return 0
	}
	return int(n)
}                                                // #nosec G115 -- count checked against finite policy.
func (c *graphCursor) field(maxBytes int) []byte { n := c.count(maxBytes, 1); return c.take(n) }
func (c *graphCursor) array() [16]byte           { var a [16]byte; copy(a[:], c.take(16)); return a }
func validGraphName(s string, l materializerLimits) bool {
	return len(s) > 0 && len(s) <= l.catalog.MaxNameBytes && utf8.ValidString(s) && strings.TrimSpace(s) != ""
}
func validSchema(d graphstate.PropertyDefinition, l materializerLimits) bool {
	return validGraphName(d.Name, l) && (d.Owner == graphstate.Node || d.Owner == graphstate.Relationship) && d.Type >= graphstate.ScalarString && d.Type <= graphstate.ScalarScope && (d.Cardinality == graphstate.ScalarCardinality || d.Cardinality == graphstate.SetCardinality) && d.Unique <= graphstate.UniqueMembers && (d.Unique != graphstate.UniqueScalar || d.Cardinality == graphstate.ScalarCardinality) && (d.Unique != graphstate.UniqueMembers || d.Cardinality == graphstate.SetCardinality)
}
func writeSchema(w *boundedWriter, d graphstate.PropertyDefinition) {
	w.text(d.Name)
	w.add([]byte{byte(d.Owner), byte(d.Type), byte(d.Cardinality), byte(d.Unique)})
}
func readSchema(c *graphCursor, l materializerLimits) graphstate.PropertyDefinition {
	return graphstate.PropertyDefinition{Name: string(c.field(l.catalog.MaxNameBytes)), Owner: graphstate.EntityKind(c.tag()), Type: graphstate.ScalarKind(c.tag()), Cardinality: graphstate.Cardinality(c.tag()), Unique: graphstate.UniqueMode(c.tag())}
}
func writeAxis(w *boundedWriter, a temporal.Axis, l materializerLimits) {
	if err := a.Validate(l.catalog.Temporal); err != nil {
		w.err = errors.Join(errInvalid, err)
		return
	}
	d := a.Descriptor()
	w.add(d.ID[:])
	w.tag(byte(d.Profile))
	w.u16(d.Version)
	w.text(d.Reference)
	w.text(d.CanonicalUnit)
}
func readAxis(c *graphCursor, l materializerLimits) temporal.Axis {
	d := temporal.AxisDescriptor{ID: temporal.AxisID(c.array()), Profile: temporal.Profile(c.tag()), Version: c.u16(), Reference: string(c.field(l.catalog.Temporal.MaxDescriptorBytes)), CanonicalUnit: string(c.field(l.catalog.Temporal.MaxDescriptorBytes))}
	if c.err != nil {
		return temporal.Axis{}
	}
	a, err := temporal.NewAxis(d, l.catalog.Temporal)
	if err != nil {
		c.err = errors.Join(errCorrupt, err)
	}
	return a
}

type axisTable []temporal.Axis

func (t *axisTable) add(a temporal.Axis, maxAxes int) error {
	if a.Descriptor().ID == (temporal.AxisID{}) {
		return nil
	}
	for _, old := range *t {
		if old.Descriptor().ID == a.Descriptor().ID {
			if old.DefinitionHash() != a.DefinitionHash() {
				return errInvalid
			}
			return nil
		}
	}
	if len(*t) == maxAxes {
		return errLimit
	}
	*t = append(*t, a)
	return nil
}
func (t axisTable) index(a temporal.Axis) int {
	for i, old := range t {
		if old.DefinitionHash() == a.DefinitionHash() {
			return i + 1
		}
	}
	return 0
}
func (t axisTable) read(c *graphCursor) temporal.Axis {
	n := c.count(len(t), 0)
	if n == 0 {
		return temporal.Axis{}
	}
	return t[n-1]
}

// scopeBackingCost inspects count framing without constructing coordinates.
// Two Bound values plus conservative immutable big.Int metadata/limb rounding
// fit 768 bytes per interval. Four times wire bytes additionally covers wide
// magnitudes. These allowances describe owned representation, not heap/RSS.
const scopeIntervalMetadataBytes = 768
const changeMetadataBytes = 512

func scopeBackingCost(b []byte, l materializerLimits) (int, error) {
	if len(b) < 53 || !bytes.Equal(b[:3], []byte{'T', 'S', 1}) {
		return 0, errCorrupt
	}
	pieces := 0
	switch temporal.ScopeKind(b[52]) {
	case temporal.ScopeEmpty, temporal.ScopeUnplaced:
	case temporal.ScopeAll, temporal.ScopePoint, temporal.ScopeSpan:
		pieces = 1
	case temporal.ScopeRegion:
		if len(b) < 57 {
			return 0, errCorrupt
		}
		n := binary.BigEndian.Uint32(b[53:57])
		if n < 2 {
			return 0, errCorrupt
		}
		if uint64(n) > uint64(l.catalog.Temporal.MaxRegionPieces) {
			return 0, errLimit
		}
		minimum := 4
		switch temporal.Profile(b[3]) {
		case temporal.ProfileRationalQ:
			minimum = 8
		case temporal.ProfileLexicographicQN:
			minimum = 11
		}
		if uint64(n) > uint64((len(b)-57)/minimum) {
			return 0, errCorrupt
		}
		pieces = int(n) // #nosec G115 -- bounded region count.
	default:
		return 0, errCorrupt
	}
	return scopeIntervalMetadataBytes*pieces + 4*len(b), nil
}
func preflightScopeBacking(c *graphCursor, b []byte, l materializerLimits) bool {
	if c.err != nil {
		return false
	}
	cost, err := scopeBackingCost(b, l)
	if err != nil {
		c.err = err
		return false
	}
	return c.charge(cost)
}
func preflightChangesBacking(c *graphCursor, b []byte, l materializerLimits) bool {
	if c.err != nil {
		return false
	}
	if len(b) < 56 || !bytes.Equal(b[:3], []byte{'C', 'C', 1}) {
		c.err = errCorrupt
		return false
	}
	n := binary.BigEndian.Uint32(b[52:56])
	if int64(n) > int64(l.graph.Planner.Component.MaxChangePieces) {
		c.err = errLimit
		return false
	}
	if uint64(n) > uint64((len(b)-56)/125) {
		c.err = errCorrupt
		return false
	}
	if !c.charge(changeMetadataBytes * int(n)) {
		return false
	} // #nosec G115 -- count bounded by policy.
	src := graphCursor{b: b[56:]}
	for range int(n) {
		scope := src.field(l.catalog.Temporal.MaxValueBytes)
		if src.err != nil {
			c.err = src.err
			return false
		}
		if !preflightScopeBacking(c, scope, l) {
			return false
		}
		src.take(68)
	}
	if src.err != nil || len(src.b) != 0 {
		c.err = errCorrupt
		return false
	}
	return true
}
func writeScope(w *boundedWriter, s temporal.Scope, t axisTable, l materializerLimits) {
	w.u32(t.index(s.Axis()))
	available := w.max - w.n - 4
	if available < 1 {
		w.err = errLimit
		return
	}
	tl := l.catalog.Temporal
	tl.MaxValueBytes = min(tl.MaxValueBytes, available)
	b, err := temporal.AppendScope(nil, s, tl)
	if err != nil {
		w.err = errors.Join(errInvalid, err)
		return
	}
	w.field(b)
}
func readScope(c *graphCursor, t axisTable, l materializerLimits) temporal.Scope {
	a := t.read(c)
	b := c.field(l.catalog.Temporal.MaxValueBytes)
	if c.err != nil {
		return temporal.Scope{}
	}
	if !preflightScopeBacking(c, b, l) {
		return temporal.Scope{}
	}
	s, err := temporal.DecodeScope(b, a, l.catalog.Temporal)
	if err != nil {
		c.err = errors.Join(errCorrupt, err)
	}
	return s
}
func writeScalar(w *boundedWriter, s graphstate.Scalar, t axisTable, l materializerLimits) {
	if s.Kind() == graphstate.ScalarInvalid {
		w.field(nil)
		w.u32(0)
		return
	}
	available := w.max - w.n - 8
	if available < 1 {
		w.err = errLimit
		return
	}
	vl := l.graph.Planner
	vl.MaxReadBytes = min(vl.MaxReadBytes, l.catalog.MaxValueBytes, available)
	if s.Kind() == graphstate.ScalarScope {
		if vl.MaxReadBytes < 2 {
			w.err = errLimit
			return
		}
		vl.Component.Temporal.MaxValueBytes = min(vl.Component.Temporal.MaxValueBytes, vl.MaxReadBytes-1)
	}
	key, err := s.EqualityKey(vl)
	if err != nil {
		w.err = errors.Join(errInvalid, err)
		return
	}
	w.text(key)
	n := 0
	if v, ok := s.Scope(); ok {
		n = t.index(v.Axis())
	}
	w.u32(n)
}
func readScalar(c *graphCursor, t axisTable, l materializerLimits) graphstate.Scalar {
	b := c.field(l.catalog.MaxValueBytes)
	a := t.read(c)
	if c.err != nil {
		return graphstate.Scalar{}
	}
	if len(b) == 0 {
		if a.Descriptor().ID != (temporal.AxisID{}) {
			c.err = errCorrupt
		}
		return graphstate.Scalar{}
	}
	if (graphstate.ScalarKind(b[0]) == graphstate.ScalarScope) != (a.Descriptor().ID != (temporal.AxisID{})) {
		c.err = errCorrupt
		return graphstate.Scalar{}
	}
	switch graphstate.ScalarKind(b[0]) {
	case graphstate.ScalarNull:
		if len(b) == 1 {
			return graphstate.Null()
		}
	case graphstate.ScalarString:
		return graphstate.String(string(b[1:]))
	case graphstate.ScalarBool:
		if len(b) == 2 && b[1] <= 1 {
			return graphstate.Bool(b[1] == 1)
		}
	case graphstate.ScalarI64:
		if len(b) == 9 {
			return graphstate.I64(int64(binary.BigEndian.Uint64(b[1:])))
		} // #nosec G115 -- exact two's-complement payload.
	case graphstate.ScalarF64:
		if len(b) == 9 && binary.BigEndian.Uint64(b[1:]) != 1<<63 {
			v, err := graphstate.F64(math.Float64frombits(binary.BigEndian.Uint64(b[1:])))
			if err == nil {
				return v
			}
		}
	case graphstate.ScalarScope:
		if !preflightScopeBacking(c, b[1:], l) {
			return graphstate.Scalar{}
		}
		s, err := temporal.DecodeScope(b[1:], a, l.catalog.Temporal)
		if err == nil {
			return graphstate.ScopeValue(s)
		}
		c.err = errors.Join(errCorrupt, err)
		return graphstate.Scalar{}
	}
	c.err = errCorrupt
	return graphstate.Scalar{}
}
func writeEntity(w *boundedWriter, e graphstate.EntityRecord, t axisTable) {
	w.u64(uint64(e.ID))
	w.tag(byte(e.Kind))
	w.u32(t.index(e.Axis))
	w.text(e.Type)
	w.u64(uint64(e.Source))
	w.u64(uint64(e.Target))
	w.tag(byte(e.Mode))
}
func readEntity(c *graphCursor, t axisTable, l materializerLimits) graphstate.EntityRecord {
	return graphstate.EntityRecord{ID: graphstate.EntityID(c.u64()), Kind: graphstate.EntityKind(c.tag()), Axis: t.read(c), Type: string(c.field(l.catalog.MaxNameBytes)), Source: graphstate.EntityID(c.u64()), Target: graphstate.EntityID(c.u64()), Mode: graphstate.ReferenceMode(c.tag())}
}
func writeLife(w *boundedWriter, e graphstate.LifeRecord) {
	for _, n := range []uint64{uint64(e.Owner), uint64(e.Life), uint64(e.SourceLife), uint64(e.TargetLife)} {
		w.u64(n)
	}
}
func readLife(c *graphCursor) graphstate.LifeRecord {
	return graphstate.LifeRecord{Owner: graphstate.EntityID(c.u64()), Life: graphstate.LifeID(c.u64()), SourceLife: graphstate.LifeID(c.u64()), TargetLife: graphstate.LifeID(c.u64())}
}
func requestAxes(r graphRequest, l materializerLimits) (axisTable, error) {
	var t axisTable
	for _, op := range r.operations {
		for _, a := range []temporal.Axis{op.Scope.Axis(), op.Record.Axis} {
			if err := t.add(a, l.maxAxes); err != nil {
				return nil, err
			}
		}
		if s, ok := op.Value.Scope(); ok {
			if err := t.add(s.Axis(), l.maxAxes); err != nil {
				return nil, err
			}
		}
	}
	return t, nil
}
func validateGraphRequest(r graphRequest, l materializerLimits) error {
	if !r.ns.valid() || r.identity() == ([16]byte{}) {
		return errInvalid
	}
	if r.kind != guardedGraphOperations && r.readBase != (graphReadBase{}) {
		return errInvalid
	}
	if r.kind == guardedGraphOperations && !r.readBase.valid(r.ns) {
		return errInvalid
	}
	if len(r.operations) > l.maxOperations || len(r.claims) > l.maxClaims || len(r.schemas) > l.maxSchemas {
		return errLimit
	}
	switch r.kind {
	case initGraph:
		if r.id != (requestID{}) || r.authority.Owner() == ([16]byte{}) || r.authority.Epoch() == 0 || r.maxBlock == 0 || r.maxBlock > idalloc.MaxBlockSize || len(r.operations) > 0 || len(r.claims) > 0 || r.revision != (state.Revision{}) {
			return errInvalid
		}
		for i, d := range r.schemas {
			if len(d.Name) > l.catalog.MaxNameBytes {
				return errLimit
			}
			if !validSchema(d, l) {
				return errInvalid
			}
			if i > 0 {
				p := r.schemas[i-1]
				if p.Owner > d.Owner || p.Owner == d.Owner && p.Name >= d.Name {
					return errInvalid
				}
			}
		}
	case graphOperations, guardedGraphOperations:
		if r.attempt != (bootstrapAttemptID{}) || r.authority != (idalloc.Authority{}) || r.maxBlock != 0 || len(r.schemas) > 0 || r.revision.ID() == 0 || len(r.operations) == 0 {
			return errInvalid
		}
		for _, op := range r.operations {
			if len(op.Name) > min(l.catalog.MaxNameBytes, l.graph.Planner.MaxNameBytes) || len(op.Record.Type) > min(l.catalog.MaxNameBytes, l.graph.Planner.MaxNameBytes) {
				return errLimit
			}
			if op.Kind < graphstate.CreateNode || op.Kind > graphstate.Remove {
				return errInvalid
			}
		}
		for i, c := range r.claims {
			if c.role < entityBinding || c.role > valueBinding || c.id == 0 || c.role == lifeBinding && c.owner == 0 || c.role != lifeBinding && c.owner != 0 || c.grant.sequence == 0 || idalloc.ValidateRecipientSession(c.grant.session) != nil {
				return errInvalid
			}
			for _, old := range r.claims[:i] {
				if old.role == c.role && old.owner == c.owner && old.id == c.id {
					return errInvalid
				}
			}
		}
	default:
		return errInvalid
	}
	return nil
}
func emitGraphRequest(w *boundedWriter, r graphRequest, t axisTable, l materializerLimits) {
	version := byte(2)
	if r.kind == guardedGraphOperations {
		version = 3
	}
	w.add([]byte{'G', 'R', 'Q', version})
	w.tag(byte(r.kind))
	w.add(r.ns.graph[:])
	w.u64(r.ns.partition)
	id := r.identity()
	w.add(id[:])
	if r.kind == initGraph {
		w.add(appendAuthority(nil, r.authority))
		w.u64(r.maxBlock)
		w.u32(len(r.schemas))
		for _, d := range r.schemas {
			writeSchema(w, d)
		}
		return
	}
	if r.kind == guardedGraphOperations {
		w.add(r.readBase.group[:])
		g := r.readBase.guard
		w.u64(g.OwnershipEpoch)
		w.u64(g.TopologyEpoch)
		w.u64(g.SchemaVersion)
		w.u64(g.SemanticEpoch)
		w.add(g.EffectDigest[:])
	}
	w.u64(r.revision.ID())
	w.u64(r.revision.Provenance())
	w.u32(len(t))
	for _, a := range t {
		writeAxis(w, a, l)
	}
	w.u32(len(r.operations))
	for _, op := range r.operations {
		w.tag(byte(op.Kind))
		w.u64(uint64(op.Owner))
		w.u64(uint64(op.Life))
		writeScope(w, op.Scope, t, l)
		writeEntity(w, op.Record, t)
		writeLife(w, op.Binding)
		w.text(op.Name)
		writeScalar(w, op.Value, t, l)
		w.u64(uint64(op.ValueID))
		if op.Present {
			w.tag(1)
		} else {
			w.tag(0)
		}
	}
	w.u32(len(r.claims))
	for _, c := range r.claims {
		w.tag(byte(c.role))
		w.u64(uint64(c.owner))
		w.u64(c.id)
		w.add(appendSession(nil, c.grant.session))
		w.u64(c.grant.sequence)
	}
}
func encodeGraphRequest(r graphRequest, l materializerLimits) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	if err := validateGraphRequest(r, l); err != nil {
		return nil, err
	}
	t, err := requestAxes(r, l)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(l.commandBytes, func(w *boundedWriter) { emitGraphRequest(w, r, t, l) })
}
func graphBody(b []byte, magic string, maxBytes int) ([]byte, error) {
	if len(b) > maxBytes {
		return nil, errLimit
	}
	if len(b) < 36 || !bytes.Equal(b[:4], []byte(magic)) {
		return nil, errCorrupt
	}
	hash := sha256.Sum256(b[:len(b)-32])
	if !bytes.Equal(hash[:], b[len(b)-32:]) {
		return nil, errCorrupt
	}
	return b[4 : len(b)-32], nil
}
func decodeGraphRequest(b []byte, l materializerLimits) (graphRequest, error) {
	if err := l.validate(); err != nil {
		return graphRequest{}, err
	}
	if len(b) < 5 || !validGraphWire(b[3], commandKind(b[4])) {
		return graphRequest{}, errCorrupt
	}
	magic := "GRQ\x02"
	if b[3] == 3 {
		magic = "GRQ\x03"
	}
	body, err := graphBody(b, magic, l.commandBytes)
	if err != nil {
		return graphRequest{}, err
	}
	c := graphCursor{b: body, maxOwnedBytes: l.commandOwnedBytes}
	if !c.charge(graphRequestMetadataBytes + 8*len(b)) {
		return graphRequest{}, c.err
	}
	r := graphRequest{kind: commandKind(c.tag()), ns: namespace{graph: idalloc.GraphID(c.array()), partition: c.u64()}}
	id := c.array()
	// Count/remaining checks precede every count-sized allocation. Conservative
	// commandOwnedBytes covers copies, axis definitions and scope/value backing;
	// the finite input envelope also bounds scratch and re-encoding passes.
	switch r.kind {
	case initGraph:
		r.attempt = bootstrapAttemptID(id)
		r.authority, err = idalloc.NewAuthority(c.array(), c.u64())
		r.maxBlock = c.u64()
		n := c.count(l.maxSchemas, 8)
		if c.charge(64 * n) {
			r.schemas = make([]graphstate.PropertyDefinition, n)
			for i := range r.schemas {
				r.schemas[i] = readSchema(&c, l)
			}
		}
	case graphOperations, guardedGraphOperations:
		r.id = requestID(id)
		if r.kind == guardedGraphOperations {
			r.readBase.group = c.array()
			g := &r.readBase.guard
			g.Namespace = graphstore.Namespace{Graph: graphstate.GraphID(r.ns.graph), Partition: r.ns.partition}
			g.OwnershipEpoch, g.TopologyEpoch, g.SchemaVersion, g.SemanticEpoch = c.u64(), c.u64(), c.u64(), c.u64()
			copy(g.EffectDigest[:], c.take(32))
		}
		r.revision, err = state.NewRevision(c.u64(), c.u64())
		n := c.count(l.maxAxes, 27)
		if !c.charge(axisMetadataBytes * n) {
			return graphRequest{}, c.err
		}
		t := make(axisTable, n)
		for i := range t {
			t[i] = readAxis(&c, l)
		}
		n = c.count(l.maxOperations, 80)
		if c.charge(operationMetadataBytes * n) {
			r.operations = make([]graphstate.Operation, n)
			for i := range r.operations {
				op := graphstate.Operation{Kind: graphstate.OperationKind(c.tag()), Owner: graphstate.EntityID(c.u64()), Life: graphstate.LifeID(c.u64()), Scope: readScope(&c, t, l), Record: readEntity(&c, t, l), Binding: readLife(&c), Name: string(c.field(l.catalog.MaxNameBytes)), Value: readScalar(&c, t, l), ValueID: graphstate.ValueID(c.u64())}
				present := c.tag()
				if present > 1 {
					c.err = errCorrupt
				}
				op.Present = present == 1
				r.operations[i] = op
			}
		}
		n = c.count(l.maxClaims, 65)
		if c.charge(claimMetadataBytes * n) {
			r.claims = make([]freshBinding, n)
			for i := range r.claims {
				r.claims[i] = freshBinding{role: bindingRole(c.tag()), owner: graphstate.EntityID(c.u64()), id: c.u64(), grant: grantReference{session: idalloc.RecipientSession{ID: c.array(), Incarnation: c.array(), Epoch: c.u64()}, sequence: c.u64()}}
			}
		}
	default:
		return graphRequest{}, errCorrupt
	}
	if c.err != nil {
		return graphRequest{}, c.err
	}
	if err != nil || len(c.b) != 0 {
		return graphRequest{}, errCorrupt
	}
	if err := validateGraphRequest(r, l); err != nil {
		return graphRequest{}, errors.Join(errCorrupt, err)
	}
	cost := graphRequestMetadataBytes + operationMetadataBytes*cap(r.operations) + claimMetadataBytes*cap(r.claims) + 64*cap(r.schemas) + 8*len(b)
	if cost > l.commandOwnedBytes {
		return graphRequest{}, errLimit
	}
	canonical, err := encodeGraphRequest(r, l)
	if err != nil {
		return graphRequest{}, err
	}
	if !bytes.Equal(canonical, b) {
		return graphRequest{}, errCorrupt
	}
	return r, nil
}
func validGraphOutcomeReason(kind commandKind, why reason) bool {
	switch why {
	case reasonNone, reasonInvalid, reasonMismatch:
		return isGraphCommand(kind)
	case reasonStale:
		return isGraphMutation(kind)
	case reasonAlreadyInitialized:
		return kind == initGraph
	case reasonReadConflict:
		return kind == guardedGraphOperations
	default:
		return false
	}
}
func encodeGraphOutcome(o outcome) ([]byte, error) {
	if !o.ns.valid() || !isGraphCommand(o.kind) || o.identity == ([16]byte{}) || o.hash == ([32]byte{}) || o.index == 0 || o.disposition != applied && o.disposition != requestReplay || !validGraphOutcomeReason(o.kind, o.reason) || o.grant != (idalloc.Grant{}) || o.grantIndex != 0 {
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
func decodeGraphOutcome(b []byte, n namespace) (outcome, error) {
	body, err := wireBody(b, "GRO\x02", graphOutcomeBytes)
	if err != nil {
		return outcome{}, err
	}
	c := graphCursor{b: body}
	o := outcome{kind: commandKind(c.tag()), ns: namespace{graph: idalloc.GraphID(c.array()), partition: c.u64()}, identity: c.array()}
	copy(o.hash[:], c.take(32))
	o.index = c.u64()
	o.disposition = disposition(c.tag())
	o.reason = reason(c.u16())
	if c.err != nil || len(c.b) != 0 || o.ns != n {
		return outcome{}, errCorrupt
	}
	if _, err := encodeGraphOutcome(o); err != nil {
		return outcome{}, errCorrupt
	}
	return o, nil
}
func decodeAnyOutcome(b []byte, n namespace) (outcome, error) {
	if len(b) >= 4 && bytes.Equal(b[:4], []byte{'G', 'R', 'O', 2}) {
		return decodeGraphOutcome(b, n)
	}
	return decodeOutcome(b, n)
}
func encodeAnyOutcome(o outcome) ([]byte, error) {
	if isGraphCommand(o.kind) {
		return encodeGraphOutcome(o)
	}
	return encodeOutcome(o)
}
