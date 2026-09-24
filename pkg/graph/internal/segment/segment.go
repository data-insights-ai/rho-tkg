// Package segment is the column-segment codec of ADR-0011 (step S1): an
// immutable, self-contained, versioned byte format holding the rows of ONE
// relationship type in column form, and a reader over it.
//
// # Contract
//
//   - Encode takes rows exactly as a store holds them (each with its stored
//     integrity Hash) and returns one segment. Rows are sorted by (start node,
//     end node, valid_from, id, version); a row is keyed (id, version).
//   - Every decoded row equals its source row bit for bit: identity,
//     endpoints, version, every temporal and integrity field, and every
//     property's key, exact Go type and bits (NaN payloads, -0). Declared
//     columns hold values of their declared kind; any other value — an
//     undeclared key or a value of another kind — goes to the fallback column
//     with the entity wire's type tags (storeutil.MarshalPropertySlice).
//   - The per-row hash is NOT stored. It is recomputed from the decoded row
//     (integrity.ComputeRelHash) and installed as Integrity().Hash. The stored
//     witness is one SHA-256 root per IntegrityBlockRows rows over the rows'
//     stored hashes, plus a segment root over the group roots (ADR-0011 §4.3).
//   - Encode re-opens what it wrote, decodes every row and refuses to return
//     a segment in which any row differs from its source (ErrInvalidRow) or
//     recomputes a hash other than its stored one (ErrHashMismatch). A row the
//     entity wire cannot reproduce is therefore refused, never sealed wrong.
//   - Open is a trust boundary (lessons 44, 47, 48): magic, versions, every
//     CRC32C and every length are checked before use; nothing is allocated in
//     proportion to a count the bytes only claim; decoding never panics on
//     hostile input. An unknown format or footer version fails closed with
//     ErrUnsupportedVersion; damage fails with a *CorruptError naming the
//     section (errors.Is ErrCorrupt).
//
// # Layout (little-endian)
//
//	header (64 B)  magic "TKGSEG\x00\x01" | format u16 | flags u16 | typeToken u16 |
//	               pageRows u16 | rowCount u32 | nodeCount u32 | id min/max i64 |
//	               valid_from min/max i64 | header CRC32C u32 | reserved u32
//	sections       contiguous, in directory order (section names: format.go)
//	footer         footerVersion u16 | integrityBlockRows u16 | type name |
//	               declared schema | segment root [32] | section directory
//	               (name, offset u64, length u64, CRC32C u32)
//	trailer (16 B) footer length u32 | footer CRC32C u32 | magic "TKGSEGFT"
//
// Int columns are pages of pageRows values (4,096 by default, the DocValues
// zone size), frame-of-reference bit-packed with a per-page transform (see
// intcol.go); string columns are dictionary, plain or hex32 (strcol.go). A
// section whose values are all zero / empty / nil is omitted.
package segment

import (
	"errors"
	"hash/crc32"
)

// Kind is the exact Go kind of a declared column: the property type tag the
// integrity hash uses for that kind (types.PropertyHashTypeTag).
type Kind uint8

// Declared column kinds (the scalar property kinds).
const (
	KindBool    Kind = 1
	KindInt     Kind = 2
	KindInt8    Kind = 3
	KindInt16   Kind = 4
	KindInt32   Kind = 5
	KindInt64   Kind = 6
	KindUint    Kind = 7
	KindUint8   Kind = 8
	KindUint16  Kind = 9
	KindUint32  Kind = 10
	KindUint64  Kind = 11
	KindFloat32 Kind = 12
	KindFloat64 Kind = 13
	KindString  Kind = 14
)

// Column is one declared property column.
type Column struct {
	Name string
	Kind Kind
}

// Schema declares one relationship type's segment layout. TypeName is what
// the integrity hash covers; TypeToken is what decoded rows carry.
type Schema struct {
	TypeName  string
	TypeToken uint16
	Columns   []Column
}

// Options tune the writer.
type Options struct {
	// IntegrityBlockRows is the number of rows per stored integrity root:
	// a power of two from 1 to 4,096; 0 means DefaultIntegrityBlockRows.
	// It is written into the footer, so a reader never assumes it.
	IntegrityBlockRows int
}

// Format constants.
const (
	PageRows                  = 4096
	DefaultIntegrityBlockRows = 64
	MaxIntegrityBlockRows     = 4096
	// MaxRows bounds one segment (ADR-0011 §3.4: a merge never produces more
	// than 64 M rows); it keeps every u32 offset of the format in range.
	MaxRows = 64 << 20
	// MaxColumns bounds the declared columns (two sections each, u16 section
	// count in the footer).
	MaxColumns           = 1024
	CurrentFormatVersion = 1
	CurrentFooterVersion = 1
)

// Errors. Every error the package returns wraps exactly one of these.
var (
	// ErrCorrupt: the bytes are damaged or malformed (see CorruptError).
	ErrCorrupt = errors.New("segment: corrupt")
	// ErrUnsupportedVersion: an unknown format or footer version (fail closed).
	ErrUnsupportedVersion = errors.New("segment: unsupported version")
	// ErrInvalidSchema: the declared schema is not usable.
	ErrInvalidSchema = errors.New("segment: invalid schema")
	// ErrInvalidOptions: a writer option is out of range.
	ErrInvalidOptions = errors.New("segment: invalid options")
	// ErrInvalidRow: an input row cannot be sealed (nil, wrong type, duplicate
	// key, or a value that does not round-trip).
	ErrInvalidRow = errors.New("segment: invalid row")
	// ErrHashMismatch: a row's stored hash is missing or differs from the hash
	// recomputed from its sealed columns; the seal is refused.
	ErrHashMismatch = errors.New("segment: row hash mismatch")
	// ErrIntegrity: a stored integrity root does not match the rows.
	ErrIntegrity = errors.New("segment: integrity root mismatch")
	// ErrOutOfRange: a row or group index outside the segment.
	ErrOutOfRange = errors.New("segment: index out of range")
)

// CorruptError names the damaged section: "header", "footer", "trailer", a
// section name from the directory, or "row" for a row whose columns decode
// but do not form a valid relationship.
type CorruptError struct {
	Section string
	Reason  string
}

func (e *CorruptError) Error() string { return "segment: corrupt " + e.Section + ": " + e.Reason }

// Unwrap returns ErrCorrupt.
func (e *CorruptError) Unwrap() error { return ErrCorrupt }

var castagnoli = crc32.MakeTable(crc32.Castagnoli)
