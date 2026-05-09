// Package io is a sub-API accessor for graph export/import.
package io

import "io"

// ImportOptions configures the staging behaviour of IO.Import.
//
// The fields are advisory — defaults preserve the prior behaviour for
// callers that pass an empty struct or call Import without options.
//
// Failure semantics:
//   - Phase-1 errors (read from r, staging-disk write, MaxStagedBytes
//     exceeded) leave the graph state unchanged.
//   - Phase-2 errors (replay under the graph write lock) may leave a
//     partially populated graph — callers requiring transactional
//     restore should import into a fresh graph and swap stores on
//     success.
type ImportOptions struct {
	// StagingDir is the directory in which the per-import temp file is
	// created. Empty means use the platform default temp dir
	// (`os.TempDir()`). For multi-GB restores callers should set this
	// to a directory on the same volume as the graph data so the
	// staging file does not fill `/tmp`.
	StagingDir string

	// MaxStagedBytes caps the total size of the staging file. Zero
	// means unlimited. When exceeded, Import returns an error during
	// Phase 1 with no live graph mutation.
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

// Export writes a length-prefixed msgpack record stream of the graph.
func (a *API) Export(w io.Writer) error { return a.ops.Export(w) }

// Import reads a length-prefixed msgpack record stream into the graph
// using default ImportOptions (platform default temp dir, no size cap).
func (a *API) Import(r io.Reader) error { return a.ops.Import(r) }

// ImportWithOptions is the explicit-options variant of Import. Callers
// set StagingDir to direct the temp file to a specific volume and
// MaxStagedBytes to bound the staging-disk usage.
func (a *API) ImportWithOptions(r io.Reader, opts ImportOptions) error {
	return a.ops.ImportWithOptions(r, opts)
}
