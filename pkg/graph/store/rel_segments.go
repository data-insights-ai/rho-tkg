package store

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Column segments for declared bulk relationship types (ADR-0011).
//
// A relationship type can be DECLARED bulk: the store then seals the type's
// unsealed rows into immutable column segments once they exceed the memtable
// budget, and every read door answers from the union of the unsealed rows
// (the row store, the "memtable") and the segments. A declaration changes
// where rows live and what they cost in memory, never what any door answers:
// the same writes into a store with and without the declaration give
// identical answers on every read door.
//
// S2 (this version): the memory store seals into in-RAM segments. Other
// backends do not implement RelSegmentCapability; a graph that declares a
// type on them fails closed at New with ErrCapabilityNotSupported.

// SegmentColumnKind is the exact Go kind of a declared segment column. A value
// of another kind (or an undeclared property) is still stored exactly — in the
// segment's fallback column — so the kind is a layout hint, never a filter.
type SegmentColumnKind uint8

// Declared column kinds. The values equal the integrity hash's property type
// tags for the scalar kinds.
const (
	SegmentBool    SegmentColumnKind = 1
	SegmentInt     SegmentColumnKind = 2
	SegmentInt8    SegmentColumnKind = 3
	SegmentInt16   SegmentColumnKind = 4
	SegmentInt32   SegmentColumnKind = 5
	SegmentInt64   SegmentColumnKind = 6
	SegmentUint    SegmentColumnKind = 7
	SegmentUint8   SegmentColumnKind = 8
	SegmentUint16  SegmentColumnKind = 9
	SegmentUint32  SegmentColumnKind = 10
	SegmentUint64  SegmentColumnKind = 11
	SegmentFloat32 SegmentColumnKind = 12
	SegmentFloat64 SegmentColumnKind = 13
	SegmentString  SegmentColumnKind = 14
)

// SegmentColumn is one declared property column.
type SegmentColumn struct {
	Name string
	Kind SegmentColumnKind
}

// RelSegmentSpec declares one bulk relationship type (graph.Config.RelSegments).
type RelSegmentSpec struct {
	// Type is the relationship type name.
	Type string
	// Columns is the declared property schema; order is irrelevant.
	Columns []SegmentColumn
	// IntegrityBlockRows is the number of rows per stored integrity root: a
	// power of two from 1 to 4,096; 0 means 64 (ADR-0011 Decision).
	IntegrityBlockRows int
}

// RelSegmentDeclaration is what the graph hands a store: the spec with its
// resolved type token and the memtable budget.
type RelSegmentDeclaration struct {
	TypeName           string
	TypeToken          uint16
	Columns            []SegmentColumn
	IntegrityBlockRows int
	// MemtableBudget is the byte budget (types.Relationship.ApproxHeapBytes)
	// of the unsealed rows of ALL declared types together; when a write
	// pushes the sum over it, the declared type with the most unsealed bytes
	// is sealed. 0 means DefaultSegmentMemtableBudget. The first declaration
	// sets it; later declarations must repeat it or pass 0.
	MemtableBudget int64
}

// DefaultSegmentMemtableBudget is the memtable budget when none is configured
// (ADR-0011 §3.5: 256 MiB).
const DefaultSegmentMemtableBudget int64 = 256 << 20

// RelSegmentStats describes one declared type's physical layout. It is a
// measurement surface: no read door's answer depends on it.
type RelSegmentStats struct {
	// Segments is the number of sealed segments.
	Segments int
	// SealedRows is the number of rows in the segments; LiveSealedRows the
	// ones that are still current (not superseded by a later write).
	SealedRows, LiveSealedRows int64
	// SegmentBytes is the encoded size of the segments.
	SegmentBytes int64
	// UnsealedRows / UnsealedBytes describe the type's rows in the row store
	// (bytes by types.Relationship.ApproxHeapBytes).
	UnsealedRows  int64
	UnsealedBytes int64
	// Seals is the number of seals that produced a segment.
	Seals uint64
	// Sealing is true while the store's background sealer is running.
	Sealing bool
}

// RelSegmentCapability is the optional store capability behind
// graph.Config.RelSegments and g.Admin().SealRelSegments.
type RelSegmentCapability interface {
	// DeclareRelSegment declares a type bulk. Re-declaring an identical
	// declaration is a no-op; a different one for the same token or name
	// fails with ErrRelSegmentDeclaration. Rows of the type that already
	// exist become eligible for sealing.
	DeclareRelSegment(d RelSegmentDeclaration) error
	// SealRelSegments seals every eligible unsealed row of a declared type
	// now, regardless of the budget. A type with no eligible rows is a no-op.
	SealRelSegments(typeToken uint16) error
	// RelSegmentStats reports a declared type's layout.
	RelSegmentStats(typeToken uint16) (RelSegmentStats, error)
}

// Relationship-segment errors.
var (
	// ErrRelSegmentDeclaration: a declaration is malformed or conflicts with
	// an existing one.
	ErrRelSegmentDeclaration = errors.New("graph: invalid relationship segment declaration")
	// ErrRelSegmentNotDeclared: the type is not declared bulk.
	ErrRelSegmentNotDeclared = errors.New("graph: relationship type is not declared as a segment type")
)

// MaxSegmentIntegrityBlockRows bounds RelSegmentSpec.IntegrityBlockRows.
const MaxSegmentIntegrityBlockRows = 4096

// ValidateRelSegmentSpec checks a spec's shape (not the type's existence).
func ValidateRelSegmentSpec(s RelSegmentSpec) error {
	if strings.TrimSpace(s.Type) == "" {
		return fmt.Errorf("%w: empty type name", ErrRelSegmentDeclaration)
	}
	b := s.IntegrityBlockRows
	if b < 0 || b > MaxSegmentIntegrityBlockRows || (b != 0 && b&(b-1) != 0) {
		return fmt.Errorf("%w: type %q: IntegrityBlockRows %d is not 0 or a power of two in [1,%d]",
			ErrRelSegmentDeclaration, s.Type, b, MaxSegmentIntegrityBlockRows)
	}
	seen := make(map[string]struct{}, len(s.Columns))
	for _, c := range s.Columns {
		if strings.TrimSpace(c.Name) == "" || strings.HasPrefix(c.Name, "tkg_") {
			return fmt.Errorf("%w: type %q: column name %q", ErrRelSegmentDeclaration, s.Type, c.Name)
		}
		if c.Kind < SegmentBool || c.Kind > SegmentString {
			return fmt.Errorf("%w: type %q: column %q has unsupported kind %d", ErrRelSegmentDeclaration, s.Type, c.Name, c.Kind)
		}
		if _, dup := seen[c.Name]; dup {
			return fmt.Errorf("%w: type %q: column %q declared twice", ErrRelSegmentDeclaration, s.Type, c.Name)
		}
		seen[c.Name] = struct{}{}
	}
	return nil
}

// RelSegmentBatch is one batch of a declared type's current rows handed out
// by RelSegmentScanCapability.ScanRelSegments (ADR-0011 §5.3, S5): the rows'
// columns as the segments store them, without building a Relationship per
// row. Rows come in ascending ID order across all batches of a scan — the
// order of RelationshipsByType / ForEachRelByType — so a consumer that builds
// output in scan order gets the row doors' output. A batch holds up to one
// page of consecutive rows (in that order) of one source: one segment
// (Segment > 0) or the unsealed memtable rows (Segment 0). The slices are
// reused between batches: a consumer copies what it keeps, and must not
// modify them.
type RelSegmentBatch struct {
	Segment uint64
	// Sorted is always false: batches are in ID order, not in a segment's
	// (start, end, valid_from) storage order. Kept for source compatibility.
	Sorted   bool
	IDs      []types.RelID
	StartIDs []types.NodeID
	EndIDs   []types.NodeID
	// ValidFrom, ValidTo and TxFrom are the stored temporal fields (0 =
	// unset: a row without temporal metadata has all three 0), Versions the
	// row versions — what Relationship.Temporal() / Version() hold.
	ValidFrom, ValidTo, TxFrom []int64
	Versions                   []uint32
	// HasTemporal is false for a row without temporal metadata (its
	// temporal fields read 0 and its temporal shadow properties are absent).
	HasTemporal []bool
	// Cols holds one entry per requested property, in request order.
	Cols []SegmentColumnValues
	row  func(k int) (*types.Relationship, error)
}

// Len is the number of rows in the batch.
func (b *RelSegmentBatch) Len() int { return len(b.IDs) }

// Row returns row k in full, frozen and equal to what the row doors return
// for it (Integrity().Hash included). It costs a row decode; consumers call it
// only for rows whose column says Other, or when they need fields the batch
// does not carry.
func (b *RelSegmentBatch) Row(k int) (*types.Relationship, error) {
	if b == nil || b.row == nil || k < 0 || k >= len(b.IDs) {
		return nil, fmt.Errorf("%w: segment batch row %d", ErrInvalidStoreMutation, k)
	}
	return b.row(k)
}

// SetRowFunc installs the Row door (for store implementations).
func (b *RelSegmentBatch) SetRowFunc(fn func(k int) (*types.Relationship, error)) { b.row = fn }

// SegmentColumnValues is one requested declared property over a batch.
//
//   - Present[k]: row k holds the property with the declared Kind; its value
//     is Value(k).
//   - Other[k]: row k may hold the property in another Go kind or shape
//     (stored outside the column); read it from Row(k).
//   - neither: row k does not hold the property.
//
// A string column comes as dictionary Codes into Dict (Dict is the store's
// shared dictionary when the segment uses one — then the same slice for every
// segment — or the segment's own), or, when the segment stored it without a
// dictionary, as Strs. Other kinds come as Ints: the stored bits (the integer
// for integer kinds, uint64 bit-cast; 0/1 for bool; IEEE-754 bits for
// floats).
type SegmentColumnValues struct {
	Name    string
	Kind    SegmentColumnKind
	Present []bool
	Other   []bool
	Ints    []int64
	Codes   []uint32
	Dict    []string
	Strs    []string
}

// Value returns row k's value with its exact declared Go kind (nil when the
// row does not hold it in that kind — check Other).
func (c *SegmentColumnValues) Value(k int) any {
	if k < 0 || k >= len(c.Present) || !c.Present[k] {
		return nil
	}
	if c.Kind == SegmentString {
		if c.Codes != nil {
			return c.Dict[c.Codes[k]]
		}
		return c.Strs[k]
	}
	return SegmentKindValue(c.Kind, c.Ints[k])
}

// SegmentKindValue turns a declared kind's stored bits back into its Go value.
func SegmentKindValue(kind SegmentColumnKind, x int64) any {
	switch kind {
	case SegmentBool:
		return x == 1
	case SegmentInt:
		return int(x)
	case SegmentInt8:
		return int8(x) // #nosec G115 -- stored from an int8
	case SegmentInt16:
		return int16(x) // #nosec G115 -- stored from an int16
	case SegmentInt32:
		return int32(x) // #nosec G115 -- stored from an int32
	case SegmentInt64:
		return x
	case SegmentUint:
		return uint(x) // #nosec G115 -- bit-cast back
	case SegmentUint8:
		return uint8(x) // #nosec G115 -- stored from a uint8
	case SegmentUint16:
		return uint16(x) // #nosec G115 -- stored from a uint16
	case SegmentUint32:
		return uint32(x) // #nosec G115 -- stored from a uint32
	case SegmentUint64:
		return uint64(x) // #nosec G115 -- bit-cast back
	case SegmentFloat32:
		return math.Float32frombits(uint32(x)) // #nosec G115 -- IEEE bits
	case SegmentFloat64:
		return math.Float64frombits(uint64(x)) // #nosec G115 -- IEEE bits
	}
	return nil
}

// RelSegmentScanCapability is the optional columnar door over a declared bulk
// type (ADR-0011 §5.3). ScanRelSegments hands every CURRENT row of the type
// to fn in batches (see RelSegmentBatch) from one consistent snapshot, in
// ascending ID order, until fn returns false. ok is false — and fn is never called — when the type is
// not declared or a requested property is not one of its declared columns;
// the caller then uses the row doors.
type RelSegmentScanCapability interface {
	ScanRelSegments(typeToken uint16, props []string, fn func(*RelSegmentBatch) bool) (ok bool, err error)
}
