package segment

import (
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Kind is the engine Scalar kind of a declared column. The tags are the
// graphstate.ScalarKind numbers, so a column tag, a fallback wire tag and the
// row-hash tag are one byte. The format-1 width model (int8..uint64, float32)
// is gone: format 2 stores I64, F64, Bool and String only.
type Kind uint8

// Declared column kinds.
const (
	KindString Kind = Kind(graphstate.ScalarString)
	KindBool   Kind = Kind(graphstate.ScalarBool)
	KindI64    Kind = Kind(graphstate.ScalarI64)
	KindF64    Kind = Kind(graphstate.ScalarF64)
)

// Column is one declared property column.
type Column struct {
	Name string
	Kind Kind
}

// Schema declares one relationship type's segment layout. TypeName is what
// the row hash covers.
type Schema struct {
	TypeName string
	Columns  []Column
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
	// MaxRows bounds one segment (a merge never produces more than 64 M rows);
	// it keeps every u32 offset of the format in range.
	MaxRows = 64 << 20
	// MaxColumns bounds the declared columns (two sections each, u16 section
	// count in the footer).
	MaxColumns           = 1024
	CurrentFormatVersion = 2
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
	// ErrInvalidRow: an input row cannot be sealed (invalid IDs or
	// coordinates, unsupported or duplicate property, duplicate key, too many
	// rows, or a value that does not round-trip).
	ErrInvalidRow = errors.New("segment: invalid row")
	// ErrHashMismatch: a decoded row's recomputed hash differs from the hash
	// of its source row; the seal is refused.
	ErrHashMismatch = errors.New("segment: row hash mismatch")
	// ErrIntegrity: a stored integrity root does not match the rows.
	ErrIntegrity = errors.New("segment: integrity root mismatch")
	// ErrOutOfRange: a row or group index outside the segment.
	ErrOutOfRange = errors.New("segment: index out of range")
)

// RowError names the input row a seal refused (Encode). It wraps exactly one
// of ErrInvalidRow or ErrHashMismatch.
type RowError struct {
	ID       types.RelID
	Revision uint32
	Err      error
	Reason   string
}

func (e *RowError) Error() string { return e.Err.Error() + ": " + e.Reason }

// Unwrap returns ErrInvalidRow or ErrHashMismatch.
func (e *RowError) Unwrap() error { return e.Err }

func rowErr(r *Row, sentinel error, format string, args ...any) error {
	return &RowError{ID: r.ID, Revision: r.Revision, Err: sentinel, Reason: fmt.Sprintf(format, args...)}
}

// ValidateSchema reports whether s can be used to Encode (ErrInvalidSchema).
func ValidateSchema(s Schema) error { return s.validate() }

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
