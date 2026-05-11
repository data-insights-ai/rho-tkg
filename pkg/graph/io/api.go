// Package io is a sub-API accessor for graph export/import.
package io

import (
	"errors"
	"io"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
)

// Sentinel errors surfaced by Import / ImportWithOptions. The same
// values are re-exported from pkg/graph (graph.ErrImportSizeLimit,
// etc.) — callers that already import pkg/graph/io can use the
// `tkgio.Err*` qualifier directly to keep `errors.Is` checks tidy.
var (
	// ErrNilReader is returned when Import / ImportWithOptions receives a nil
	// io.Reader, including typed nil reader values.
	ErrNilReader = errors.New("graph: import reader must not be nil")

	// ErrNilWriter is returned when Export receives a nil io.Writer, including
	// typed nil writer values.
	ErrNilWriter = errors.New("graph: export writer must not be nil")

	// ErrImportSizeLimit is returned during Phase 1 when the staging
	// file would exceed ImportOptions.MaxStagedBytes. Graph state is
	// unchanged on this error.
	ErrImportSizeLimit = errors.New("graph: import staging exceeds MaxStagedBytes")

	// ErrIncompatibleExport is returned when the export format
	// version on the wire differs from the runtime's
	// exportFormatVersion. Phase 1 surfaces it from header parsing.
	ErrIncompatibleExport = errors.New("graph: incompatible export format version")

	// ErrIncompatibleRegistry is returned when an existing non-empty
	// registry maps tokens differently from the export's registry.
	// Idempotent re-import (identical mappings) is silent.
	ErrIncompatibleRegistry = errors.New("graph: imported registry conflicts with existing registry")

	// ErrCorruptExport wraps every structural-validity failure on
	// import (token 0 in primary label, token outside [1, 65535],
	// malformed MsgPack, missing records, count mismatches, duplicate
	// stream records, unknown record tags, etc.). Distinct from
	// ErrIncompatibleExport: the format version matches but a record is
	// malformed.
	ErrCorruptExport = errors.New("graph: corrupt export record")
)

// ImportOptions configures the staging behaviour of IO.Import.
//
// The fields are advisory — defaults preserve the prior behaviour for
// callers that pass an empty struct or call Import without options.
//
// Failure semantics:
//   - Phase-1 errors (read from r, staging-disk write, MaxStagedBytes
//     exceeded) leave the graph state unchanged.
//   - Phase-2 errors (replay under the graph write lock) roll back
//     touched current rows, history rows, and registries to the
//     pre-import state unless the underlying store also fails during
//     rollback.
type ImportOptions struct {
	// StagingDir is the directory in which the per-import temp file is
	// created. Empty means use the platform default temp dir
	// (`os.TempDir()`). For multi-GB restores this can point at a
	// directory on the same volume as the graph data so the staging
	// file does not fill `/tmp`.
	StagingDir string

	// MaxStagedBytes caps the total size of the staging file. Zero
	// means unlimited; negative values are invalid. When exceeded,
	// Import returns an error during Phase 1 with no live graph mutation.
	MaxStagedBytes int64
}

// Ops is the subset of *core.IOOps the io sub-API forwards to.
type Ops interface {
	Export(w io.Writer) error
	Import(r io.Reader) error
	ImportWithOptions(r io.Reader, opts ImportOptions) error
}

// API is the io sub-API accessor.
type API struct{ ops Ops }

// New constructs an io sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

func (a *API) ready() (Ops, error) {
	if a == nil || grapherr.IsNil(a.ops) {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// Export writes a length-prefixed msgpack record stream of the graph.
func (a *API) Export(w io.Writer) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Export(w)
}

// Import reads a length-prefixed msgpack record stream into the graph
// using default ImportOptions (platform default temp dir, no size cap).
func (a *API) Import(r io.Reader) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Import(r)
}

// ImportWithOptions is the explicit-options variant of Import.
// StagingDir directs the temp file to a specific volume and
// MaxStagedBytes bounds the staging-disk usage.
func (a *API) ImportWithOptions(r io.Reader, opts ImportOptions) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ImportWithOptions(r, opts)
}
