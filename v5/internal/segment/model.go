package segment

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// CoordKind says whether a Coord is a finite position or an explicit
// infinity. The zero value is invalid on purpose: no numeric value, zero
// included, stands for "open" or "unset" (PLAN: zero is a coordinate).
type CoordKind uint8

// Coordinate kinds. The numeric order is the temporal order of the kinds and
// matches temporal.BoundKind (invalid, -inf, finite, +inf).
const (
	CoordInvalid CoordKind = iota
	CoordNegInf
	CoordFinite
	CoordPosInf
)

// Coord is one exact int64 coordinate on an integer axis, or an explicit
// infinity. It replaces v4's temporal.Instant, where 0 meant "unset/open".
type Coord struct {
	kind CoordKind
	v    int64
}

// Finite returns the finite coordinate v. Zero is an ordinary value.
func Finite(v int64) Coord { return Coord{kind: CoordFinite, v: v} }

// NegInf returns the lower infinity.
func NegInf() Coord { return Coord{kind: CoordNegInf} }

// PosInf returns the upper infinity.
func PosInf() Coord { return Coord{kind: CoordPosInf} }

// Kind returns the coordinate kind.
func (c Coord) Kind() CoordKind { return c.kind }

// Value returns the finite value, or false for an infinity or invalid Coord.
func (c Coord) Value() (int64, bool) { return c.v, c.kind == CoordFinite }

// Compare orders -inf < every finite value < +inf; invalid sorts first.
func (c Coord) Compare(o Coord) int {
	if c.kind != o.kind {
		return cmp.Compare(c.kind, o.kind)
	}
	if c.kind != CoordFinite {
		return 0
	}
	return cmp.Compare(c.v, o.v)
}

// CoordFromBound converts a bound of an integer-profile axis. An infinite
// bound maps to the matching infinity. A finite bound must hold a position
// whose integer fits int64; its inclusive flag is returned because the codec
// stores coordinates only (the interval shape belongs to the caller).
func CoordFromBound(b temporal.Bound) (c Coord, inclusive bool, err error) {
	switch b.Kind() {
	case temporal.BoundNegativeInfinity:
		return NegInf(), false, nil
	case temporal.BoundPositiveInfinity:
		return PosInf(), false, nil
	case temporal.BoundFinite:
		p, _ := b.Position()
		n, ok := p.Integer()
		if !ok {
			return Coord{}, false, fmt.Errorf("%w: position is not on an integer axis", ErrInvalidRow)
		}
		v, ok := n.Int64()
		if !ok {
			return Coord{}, false, fmt.Errorf("%w: coordinate does not fit int64", ErrInvalidRow)
		}
		return Finite(v), b.Inclusive(), nil
	}
	return Coord{}, false, fmt.Errorf("%w: invalid bound", ErrInvalidRow)
}

// Bound converts c back to a bound on axis (an integer-profile axis).
// inclusive applies to a finite coordinate only.
func (c Coord) Bound(axis temporal.Axis, inclusive bool) (temporal.Bound, error) {
	switch c.kind {
	case CoordNegInf:
		return temporal.NegativeInfinity(), nil
	case CoordPosInf:
		return temporal.PositiveInfinity(), nil
	case CoordFinite:
		p, err := temporal.IntegerPosition(axis, temporal.Int64(c.v))
		if err != nil {
			return temporal.Bound{}, err
		}
		return temporal.FiniteBound(p, inclusive)
	}
	return temporal.Bound{}, fmt.Errorf("%w: invalid coordinate", ErrInvalidRow)
}

// Property is one named scalar value. Value must be a graphstate.Scalar of
// kind I64, F64, Bool or String (null, scope, descriptor and the zero Scalar
// are refused with ErrInvalidRow).
type Property struct {
	Key   string
	Value graphstate.Scalar
}

// Row is one relationship assertion revision as the codec carries it. It is
// pointer-free input/output of Encode and the readers. Identity, endpoints and
// the four coordinates are the system columns; Props are the properties.
//
// Valid time is [ValidFrom, ValidTo) in the caller's interval shape and
// transaction time is [TxFrom, TxTo); the codec stores the four coordinates
// without interpreting inclusivity or checking from <= to (the Scope layer
// owns interval validity). Allowed kinds: ValidFrom and TxFrom finite or -inf,
// ValidTo and TxTo finite or +inf.
type Row struct {
	ID       types.RelID
	Start    types.NodeID
	End      types.NodeID
	Revision uint32

	ValidFrom, ValidTo Coord
	TxFrom, TxTo       Coord

	Props []Property
}

// valid reports whether k is a storable column kind.
func (k Kind) valid() bool {
	return k == KindString || k == KindBool || k == KindI64 || k == KindF64
}

// kindOf returns the column Kind a value would fill (0 for a value no column
// can hold).
func kindOf(v graphstate.Scalar) Kind {
	if k := Kind(v.Kind()); k.valid() {
		return k
	}
	return 0
}

// ValueBits maps a non-string value to the int64 a column stores: the integer
// for I64, 0/1 for Bool, the IEEE-754 bits for F64 (0 for anything else).
func ValueBits(v graphstate.Scalar) int64 { return valueBits(v) }

func valueBits(v graphstate.Scalar) int64 {
	if x, ok := v.Int64Value(); ok {
		return x
	}
	if x, ok := v.BoolValue(); ok && x {
		return 1
	}
	if x, ok := v.Float64Value(); ok {
		return int64(math.Float64bits(x)) // #nosec G115 -- bit pattern
	}
	return 0
}

// kindValue turns a stored int64 back into a Scalar of kind k, rejecting a
// value the kind cannot hold (a corrupt column, never a truncation).
func kindValue(k Kind, x int64) (graphstate.Scalar, bool) {
	switch k {
	case KindBool:
		return graphstate.Bool(x == 1), x == 0 || x == 1
	case KindI64:
		return graphstate.I64(x), true
	case KindF64:
		// Canonical F64 is finite and never -0 (graphstate.F64 folds it).
		f := math.Float64frombits(uint64(x)) // #nosec G115 -- bit pattern
		v, err := graphstate.F64(f)
		return v, err == nil && uint64(x) != 1<<63 // #nosec G115 -- bit pattern
	}
	return graphstate.Scalar{}, false
}

// wireLimits bounds one value's kind-preserving wire (graphstate's maximum).
var wireLimits = graphstate.Limits{MaxReadBytes: 64 << 20}

// appendValue appends u32 length | graphstate wire (kind byte + payload) of a
// scalar: the one value encoding of both the row hash and the fallback
// column. ok is false for a value the segment cannot store.
func appendValue(b []byte, v graphstate.Scalar) ([]byte, bool) {
	if kindOf(v) == 0 {
		return b, false
	}
	at := len(b)
	b = append(b, 0, 0, 0, 0)
	b, err := v.AppendWire(b, wireLimits)
	if err != nil {
		return b[:at], false
	}
	binary.BigEndian.PutUint32(b[at:], uint32(len(b)-at-4)) // #nosec G115 -- bounded by wireLimits
	return b, true
}

// maxStringLen bounds one string property value (u32 length prefixes).
const maxStringLen = 64<<20 - 2

// sortedProps returns a copy of ps sorted by key, after checking keys are
// non-empty, at most 65535 bytes and unique and values are scalars.
func sortedProps(ps []Property) ([]Property, error) {
	out := slices.Clone(ps)
	slices.SortStableFunc(out, func(a, b Property) int { return cmp.Compare(a.Key, b.Key) })
	for i, p := range out {
		if p.Key == "" || len(p.Key) > 1<<16-1 {
			return nil, fmt.Errorf("%w: property key of %d bytes", ErrInvalidRow, len(p.Key))
		}
		if i > 0 && out[i-1].Key == p.Key {
			return nil, fmt.Errorf("%w: property %q twice", ErrInvalidRow, p.Key)
		}
		if s, isStr := p.Value.StringValue(); isStr && len(s) > maxStringLen {
			return nil, fmt.Errorf("%w: property %q string of %d bytes", ErrInvalidRow, p.Key, len(s))
		}
		if kindOf(p.Value) == 0 {
			return nil, fmt.Errorf("%w: property %q has unsupported scalar kind %d", ErrInvalidRow, p.Key, p.Value.Kind())
		}
	}
	return out, nil
}

// validate checks the row's own invariants (no schema involved).
func (r *Row) validate() error {
	if r.ID < 1 || r.Start < 1 || r.End < 1 { // 1..MaxInt64 (types.RelID, NodeID)
		return fmt.Errorf("%w: id %d, start %d, end %d must be in 1..MaxInt64", ErrInvalidRow, r.ID, r.Start, r.End)
	}
	for _, c := range []struct {
		name      string
		c         Coord
		forbidden CoordKind
	}{
		{"ValidFrom", r.ValidFrom, CoordPosInf}, {"ValidTo", r.ValidTo, CoordNegInf},
		{"TxFrom", r.TxFrom, CoordPosInf}, {"TxTo", r.TxTo, CoordNegInf},
	} {
		if c.c.kind == CoordInvalid || c.c.kind == c.forbidden || c.c.kind > CoordPosInf {
			return fmt.Errorf("%w: %s is %v", ErrInvalidRow, c.name, c.c)
		}
	}
	return nil
}

// RowHash is the content hash of a row of type typeName: SHA-256 over a
// canonical big-endian encoding of identity, endpoints, revision, the four
// coordinates (kind byte and value) and the properties sorted by key. v4
// stored this hash per row and chained it; the v5 codec never stores it, it
// recomputes it from decoded columns and keeps only group roots (see Verify).
func RowHash(typeName string, r *Row) ([32]byte, error) {
	ps, err := sortedProps(r.Props)
	if err != nil {
		return [32]byte{}, err
	}
	return rowHash(typeName, r, ps), nil
}

// rowHash hashes r whose properties are already sorted and validated.
func rowHash(typeName string, r *Row, ps []Property) [32]byte {
	b := make([]byte, 0, 128)
	b = append(b, "tkg5.segment.row.v2\x00"...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(typeName))) // #nosec G115 -- validated <= 65535
	b = append(b, typeName...)
	b = binary.BigEndian.AppendUint64(b, uint64(r.ID))    // #nosec G115 -- positive
	b = binary.BigEndian.AppendUint64(b, uint64(r.Start)) // #nosec G115 -- positive
	b = binary.BigEndian.AppendUint64(b, uint64(r.End))   // #nosec G115 -- positive
	b = binary.BigEndian.AppendUint32(b, r.Revision)
	for _, c := range [4]Coord{r.ValidFrom, r.ValidTo, r.TxFrom, r.TxTo} {
		b = append(b, byte(c.kind))
		if c.kind == CoordFinite {
			b = binary.BigEndian.AppendUint64(b, uint64(c.v)) // #nosec G115 -- bit pattern
		}
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(ps))) // #nosec G115 -- bounded by the row
	for _, p := range ps {
		b = binary.BigEndian.AppendUint16(b, uint16(len(p.Key))) // #nosec G115 -- validated
		b = append(b, p.Key...)
		b, _ = appendValue(b, p.Value)
	}
	return sha256.Sum256(b)
}

// rowDiff names the first field in which got differs from want ("" when
// equal). Both property lists must be sorted by key. Values compare by kind
// and exact bits, so NaN payloads and -0 are distinguished.
func rowDiff(want, got *Row) string {
	if want.ID != got.ID || want.Start != got.Start || want.End != got.End || want.Revision != got.Revision {
		return "identity"
	}
	if want.ValidFrom != got.ValidFrom || want.ValidTo != got.ValidTo || want.TxFrom != got.TxFrom || want.TxTo != got.TxTo {
		return "coordinates"
	}
	if len(want.Props) != len(got.Props) {
		return fmt.Sprintf("%d properties, want %d", len(got.Props), len(want.Props))
	}
	var wb, gb []byte
	for k := range want.Props {
		wp, gp := want.Props[k], got.Props[k]
		if wp.Key != gp.Key {
			return fmt.Sprintf("property key %q, want %q", gp.Key, wp.Key)
		}
		wb, _ = appendValue(wb[:0], wp.Value)
		gb, _ = appendValue(gb[:0], gp.Value)
		if !bytes.Equal(wb, gb) {
			return fmt.Sprintf("property %q: %s, want %s", wp.Key, gp.Value.Render(), wp.Value.Render())
		}
	}
	return ""
}

// lessRow is segment order: (start, end, valid_from, id, revision).
func lessRow(a, b *Row) bool {
	if a.Start != b.Start {
		return a.Start < b.Start
	}
	if a.End != b.End {
		return a.End < b.End
	}
	if c := a.ValidFrom.Compare(b.ValidFrom); c != 0 {
		return c < 0
	}
	if a.ID != b.ID {
		return a.ID < b.ID
	}
	return a.Revision < b.Revision
}

// String renders the coordinate for messages.
func (c Coord) String() string {
	switch c.kind {
	case CoordNegInf:
		return "-inf"
	case CoordPosInf:
		return "+inf"
	case CoordFinite:
		return fmt.Sprint(c.v)
	}
	return "invalid"
}
