package assertion

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Provisional AR v1: graph(16), assertion(8), target kind/entity/component
// owner/life/kind/member, sized name, revision/provenance/predecessor(8 each),
// interpretation, sized role, placement/retracted/knowledge tags. Active placement
// and knowledge each have one sized canonical child codec. Sizes are uint32 BE;
// IDs are uint64 BE. Inactive target fields encode zero. NoAssociation encodes
// no temporal child. Native scope binds an external full axis definition.
// This preservation/association envelope is not a frozen sealed-row layout.
const codecVersion = 1
const recordFixedBytes = 97

func axisBytes(a temporal.Axis) int {
	d := a.Descriptor()
	return 27 + len(d.Reference) + len(d.CanonicalUnit)
}

func childError(err error) error {
	if errors.Is(err, temporal.ErrResourceLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	return err
}

func decodeChildError(err error) error {
	if errors.Is(err, temporal.ErrResourceLimit) {
		return childError(err)
	}
	// Expected-axis/policy failures do not certify that the delivered bytes are
	// corrupt. All other child decoding refusals identify malformed child bytes.
	if errors.Is(err, temporal.ErrAxisMismatch) || errors.Is(err, temporal.ErrInvalidAxis) || errors.Is(err, temporal.ErrInvalidLimits) {
		return err
	}
	return errors.Join(ErrInvalidEncoding, err)
}

func encodeSpec(s Spec, l Limits) ([]byte, error) {
	if err := validateSpec(s, l); err != nil {
		return nil, err
	}
	size := recordFixedBytes + len(s.Target.Component.Name) + len(s.Role)
	retained := 0
	var placement, knowledge []byte
	var err error
	switch s.Placement.Kind {
	case NativePlacement:
		retained = axisBytes(s.Placement.Native.Axis())
		if retained > l.MaxRecordBytes-size {
			return nil, ErrResourceLimit
		}
		placement, err = temporal.AppendScope(nil, s.Placement.Native, l.Temporal)
	case SymbolicPlacement:
		placement, err = temporal.AppendOpaqueDescriptor(nil, s.Placement.Symbolic, l.Temporal)
	}
	if err != nil {
		return nil, childError(err)
	}
	if s.Placement.Kind != NoAssociation {
		size += 4 + len(placement)
	}
	if size > l.MaxRecordBytes-retained {
		return nil, ErrResourceLimit
	}
	if s.Knowledge.Kind == PointKnowledge {
		knowledge, err = temporal.AppendPointKnowledge(nil, s.Knowledge.Point, l.Temporal)
		if err != nil {
			return nil, childError(err)
		}
		size += 4 + len(knowledge)
	}
	if size > l.MaxRecordBytes-retained {
		return nil, ErrResourceLimit
	}
	dst := make([]byte, 0, size)
	dst = append(dst, 'A', 'R', codecVersion)
	dst = append(dst, s.Ref.Graph[:]...)
	dst = binary.BigEndian.AppendUint64(dst, uint64(s.Ref.ID))
	dst = append(dst, byte(s.Target.Kind))
	dst = binary.BigEndian.AppendUint64(dst, uint64(s.Target.Entity))
	k := s.Target.Component
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Owner))
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Life))
	dst = append(dst, byte(k.Kind))
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Member))
	dst = field(dst, []byte(k.Name))
	dst = binary.BigEndian.AppendUint64(dst, s.Revision.ID())
	dst = binary.BigEndian.AppendUint64(dst, s.Revision.Provenance())
	dst = binary.BigEndian.AppendUint64(dst, s.Previous)
	dst = append(dst, byte(s.Interpretation))
	dst = field(dst, []byte(s.Role))
	dst = append(dst, byte(s.Placement.Kind))
	flag := byte(0)
	if s.Retracted {
		flag = 1
	}
	dst = append(dst, flag, byte(s.Knowledge.Kind))
	if s.Placement.Kind != NoAssociation {
		dst = field(dst, placement)
	}
	if s.Knowledge.Kind == PointKnowledge {
		dst = field(dst, knowledge)
	}
	return dst, nil
}

func field(dst, src []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(src))) // #nosec G115 -- complete input bounded to 1 MiB before encoding.
	return append(dst, src...)
}

// AppendRecord validates/encodes fully before touching destination bytes. The
// record budget excludes dst's existing prefix; failure leaves its backing bytes
// unchanged. Child errors retain their temporal sentinels through errors.Is.
func AppendRecord(dst []byte, r Record, l Limits) ([]byte, error) {
	l, err := l.resolve()
	if err != nil {
		return dst, err
	}
	wire, err := encodeSpec(r.Spec(), l)
	if err != nil {
		return dst, err
	}
	if len(wire) > int(^uint(0)>>1)-len(dst) {
		return dst, ErrResourceLimit
	}
	return append(dst, wire...), nil
}

type cursor struct{ src []byte }

func (c *cursor) take(n int) ([]byte, error) {
	if n < 0 || n > len(c.src) {
		return nil, ErrInvalidEncoding
	}
	b := c.src[:n]
	c.src = c.src[n:]
	return b, nil
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

func (c *cursor) sized() ([]byte, error) {
	b, err := c.take(4)
	if err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(b)
	if uint64(n) > uint64(len(c.src)) {
		return nil, ErrInvalidEncoding
	}
	return c.take(int(n)) // #nosec G115 -- n <= delivered input <= 1 MiB.
}

type wireView struct {
	spec                             Spec
	name, role, placement, knowledge []byte
}

// inspect borrows delivered bytes and completes framing validation before any
// child decode or owned string/payload allocation. No declared length allocates.
func inspect(src []byte, l Limits) (wireView, error) {
	if len(src) > l.MaxRecordBytes {
		return wireView{}, ErrResourceLimit
	}
	if len(src) < recordFixedBytes || src[0] != 'A' || src[1] != 'R' {
		return wireView{}, ErrInvalidEncoding
	}
	if src[2] != codecVersion {
		return wireView{}, errors.Join(ErrInvalidEncoding, ErrUnknownVersion)
	}
	w := wireView{}
	copy(w.spec.Ref.Graph[:], src[3:19])
	c := cursor{src: src[19:]}
	id, _ := c.number()
	w.spec.Ref.ID = ID(id)
	target, _ := c.tag()
	w.spec.Target.Kind = TargetKind(target)
	entity, _ := c.number()
	w.spec.Target.Entity = graphstate.EntityID(entity)
	owner, _ := c.number()
	life, _ := c.number()
	kind, _ := c.tag()
	member, _ := c.number()
	w.spec.Target.Component = graphstate.ComponentKey{Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Kind: graphstate.ComponentKind(kind), Member: graphstate.ValueID(member)}
	var err error
	w.name, err = c.sized()
	if err != nil {
		return wireView{}, err
	}
	revision, err := c.number()
	if err != nil {
		return wireView{}, err
	}
	provenance, err := c.number()
	if err != nil {
		return wireView{}, err
	}
	w.spec.Revision, err = state.NewRevision(revision, provenance)
	if err != nil {
		return wireView{}, errors.Join(ErrInvalidEncoding, err)
	}
	w.spec.Previous, err = c.number()
	if err != nil {
		return wireView{}, err
	}
	interpretation, err := c.tag()
	if err != nil {
		return wireView{}, err
	}
	w.spec.Interpretation = Interpretation(interpretation)
	w.role, err = c.sized()
	if err != nil {
		return wireView{}, err
	}
	placement, err := c.tag()
	if err != nil {
		return wireView{}, err
	}
	w.spec.Placement.Kind = PlacementKind(placement)
	retracted, err := c.tag()
	if err != nil || retracted > 1 {
		return wireView{}, ErrInvalidEncoding
	}
	w.spec.Retracted = retracted == 1
	knowledge, err := c.tag()
	if err != nil {
		return wireView{}, err
	}
	w.spec.Knowledge.Kind = KnowledgeKind(knowledge)
	if w.spec.Placement.Kind < NoAssociation || w.spec.Placement.Kind > SymbolicPlacement || w.spec.Knowledge.Kind < NoKnowledge || w.spec.Knowledge.Kind > PointKnowledge {
		return wireView{}, ErrInvalidEncoding
	}
	if w.spec.Placement.Kind != NoAssociation {
		w.placement, err = c.sized()
		if err != nil || len(w.placement) == 0 {
			return wireView{}, ErrInvalidEncoding
		}
	}
	if w.spec.Knowledge.Kind == PointKnowledge {
		w.knowledge, err = c.sized()
		if err != nil || len(w.knowledge) == 0 {
			return wireView{}, ErrInvalidEncoding
		}
	}
	if len(c.src) != 0 {
		return wireView{}, ErrInvalidEncoding
	}
	return w, nil
}

// DecodeRecord owns returned bytes/metadata. Native placement requires its full
// expected axis; axisless/symbolic placement requires a zero nativeAxis. Unknown
// envelope versions decline. Unknown symbolic payload schemas preserve only.
// Malformed child bytes also match ErrInvalidEncoding; resource and expected-axis
// refusals retain their own sentinels without claiming byte corruption.
func DecodeRecord(src []byte, nativeAxis temporal.Axis, l Limits) (Record, error) {
	l, err := l.resolve()
	if err != nil {
		return Record{}, err
	}
	w, err := inspect(src, l)
	if err != nil {
		return Record{}, err
	}
	if w.spec.Placement.Kind != NativePlacement && nativeAxis != (temporal.Axis{}) {
		return Record{}, ErrInvalid
	}
	if w.spec.Placement.Kind == NativePlacement && axisBytes(nativeAxis) > l.MaxRecordBytes-len(src) {
		return Record{}, ErrResourceLimit
	}
	if len(w.role) > l.MaxRoleBytes {
		return Record{}, ErrResourceLimit
	}
	w.spec.Target.Component.Name = string(w.name)
	w.spec.Role = TemporalRole(w.role)
	switch w.spec.Placement.Kind {
	case NativePlacement:
		w.spec.Placement.Native, err = temporal.DecodeScope(w.placement, nativeAxis, l.Temporal)
	case SymbolicPlacement:
		w.spec.Placement.Symbolic, err = temporal.DecodeOpaqueDescriptor(w.placement, l.Temporal)
	}
	if err != nil {
		return Record{}, decodeChildError(err)
	}
	if w.spec.Knowledge.Kind == PointKnowledge {
		if w.spec.Placement.Kind != NativePlacement {
			return Record{}, errors.Join(ErrInvalidEncoding, ErrKnowledgeAssociation)
		}
		w.spec.Knowledge.Point, err = temporal.DecodePointKnowledge(w.knowledge, nativeAxis, l.Temporal)
		if err != nil {
			return Record{}, decodeChildError(err)
		}
	}
	r, err := New(w.spec, l)
	if err != nil {
		if errors.Is(err, ErrResourceLimit) {
			return Record{}, err
		}
		return Record{}, errors.Join(ErrInvalidEncoding, err)
	}
	return r, nil
}

// IntegrityHash commits complete canonical association bytes, not semantic
// equality, source payload bytes alone, historical uniqueness or a proof.
func IntegrityHash(r Record, l Limits) ([32]byte, error) {
	b, err := AppendRecord(nil, r, l)
	if err != nil {
		return [32]byte{}, err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:assertion-integrity:v1\x00"))
	_, _ = h.Write(b)
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}
