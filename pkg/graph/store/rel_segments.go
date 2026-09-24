package store

import (
	"errors"
	"fmt"
	"strings"
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
