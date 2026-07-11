// Package io is a sub-API accessor for graph export/import.
package io

import (
	"errors"
	"io"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
)

// Sentinel errors surfaced by Import. The same
// values are re-exported from pkg/graph (graph.ErrImportSizeLimit,
// etc.) — callers that already import pkg/graph/io can use the
// `tkgio.Err*` qualifier directly to keep `errors.Is` checks tidy.
var (
	// ErrNilReader is returned when Import receives a nil
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
	// exportFormatVersion. Import surfaces it before replaying staged
	// records into graph state.
	ErrIncompatibleExport = errors.New("graph: incompatible export format version")

	// ErrIncompatibleRegistry is returned when an existing non-empty
	// registry maps tokens differently from the export's registry.
	// Idempotent re-import (identical mappings) is silent.
	ErrIncompatibleRegistry = errors.New("graph: imported registry conflicts with existing registry")

	// ErrCorruptExport wraps every structural-validity failure on
	// import (token 0 in primary label, token outside [1, 65535],
	// malformed MsgPack, missing records, count mismatches, duplicate
	// stream records, unknown record tags, truncated record headers or
	// bodies, and rows whose content does not match their integrity hash
	// or whose imported hash chain does not verify — import recomputes
	// both, since the stream is untrusted input). Distinct from
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

	// SkipUniqueValidation disables the post-replay unique-constraint check.
	// By default (ADR-0002 Decision 4: default-strict) Import validates the
	// replayed current state against every ACTIVE unique constraint and rolls
	// the whole import back with ErrUniqueViolation if a stream carries two
	// current nodes sharing a constrained value. Set this for a TRUSTED restore
	// (e.g. re-importing an export the graph itself produced) to skip that scan.
	SkipUniqueValidation bool
}

// Ops is the subset of *core.IOOps the io sub-API forwards to.
type Ops interface {
	Export(w io.Writer) error
	Import(r io.Reader, opts ImportOptions) error
	Watermark() (Cursor, error)
	ExportSince(w io.Writer, since Cursor) error
	ImportMerge(r io.Reader, opts MergeOptions) error
}

// API is the io sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs an io sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
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

// Import reads a length-prefixed msgpack record stream into the graph.
// Pass ImportOptions{} for default behavior (platform default temp dir,
// no size cap).
//
// API 4.0 change: the previous separate `Import(r)` and `ImportWithOptions(r, opts)`
// are merged into this single method. To migrate `Import(r)` calls, pass
// `ImportOptions{}` as the second argument.
func (a *API) Import(r io.Reader, opts ImportOptions) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Import(r, opts)
}

// Watermark returns a Cursor marking the graph's CURRENT change-log point. It is
// the value to (1) store alongside a full or delta backup as the parent cursor
// for the NEXT delta, and (2) pass as `since` to a later ExportSince to get
// everything committed after this point.
//
// Returns a wrapped store.ErrCapabilityNotSupported when the graph has no
// change-log (graph.Config.ChangeLog) or the backend exposes no change-feed; the
// caller then performs a full Export rather than a delta. ErrNilGraph /
// ErrGraphClosed on a not-ready or closed graph.
func (a *API) Watermark() (Cursor, error) {
	ops, err := a.ready()
	if err != nil {
		return Cursor{}, err
	}
	return ops.Watermark()
}

// ExportSince writes a DELTA stream to w: only the mutations committed after
// `since`, framed like Export plus a delta header so ImportMerge can apply it
// onto a base. See the package-level delta documentation for semantics.
//
// Returns ErrCursorUnknown when `since` is from another graph, ahead of the log,
// or predates retained history (the caller falls back to a full Export); a
// wrapped store.ErrCapabilityNotSupported when the graph has no change-log;
// ErrNilWriter / ErrNilGraph / ErrGraphClosed as appropriate.
func (a *API) ExportSince(w io.Writer, since Cursor) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ExportSince(w, since)
}

// ImportMerge reads a stream produced by ExportSince (a delta) and MERGES it
// into the current graph, reproducing the source's rows verbatim through the
// same trust pipeline Import uses (per-record hash recompute-and-compare plus a
// full hash-chain verification after replay, rolling back all touched rows on
// any failure). Unlike Import it does not require an empty target. Applying the
// same delta more than once is a no-op (idempotent).
//
// Pass MergeOptions{} for the lenient default. Errors mirror Import
// (ErrNilReader, ErrImportSizeLimit, ErrIncompatibleExport,
// ErrIncompatibleRegistry, ErrCorruptExport) plus ErrDeltaBaseMismatch when the
// delta is applied onto the wrong base (ExpectBase / Strict).
func (a *API) ImportMerge(r io.Reader, opts MergeOptions) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ImportMerge(r, opts)
}
