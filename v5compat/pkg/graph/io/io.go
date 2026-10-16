// Package io mirrors the v4 import path pkg/graph/io: the v4 export/import and backup format. Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package io

import (
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	stdio "io"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// API is the sub-API handle returned by the graph accessor.
type API struct{}

// ImportOptions is a placeholder for the v4 type of the same name.
type ImportOptions struct{}

// Cursor is a placeholder for the v4 type of the same name.
type Cursor struct{}

// MergeOptions is a placeholder for the v4 type of the same name.
type MergeOptions struct{}

// Export is declined.
func (a *API) Export(w stdio.Writer) error {
	return declined("io.Export")
}

// Import is declined.
func (a *API) Import(r stdio.Reader, opts ImportOptions) error {
	return declined("io.Import")
}

// Watermark is declined.
func (a *API) Watermark() (r0 Cursor, err error) {
	err = declined("io.Watermark")
	return
}

// ExportSince is declined.
func (a *API) ExportSince(w stdio.Writer, since Cursor) error {
	return declined("io.ExportSince")
}

// ImportMerge is declined.
func (a *API) ImportMerge(r stdio.Reader, opts MergeOptions) error {
	return declined("io.ImportMerge")
}

// BackupTo is declined.
func (a *API) BackupTo(dir string) error {
	return declined("io.BackupTo")
}

// BackupDeltaTo is declined.
func (a *API) BackupDeltaTo(dir string) error {
	return declined("io.BackupDeltaTo")
}
