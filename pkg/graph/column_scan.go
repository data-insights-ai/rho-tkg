package graph

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ColumnBatch and ColumnKind are re-exported so a consumer never imports
// pkg/graph/store directly — the same rule every other store type here follows.
type (
	ColumnBatch = storepkg.ColumnBatch
	ColumnKind  = storepkg.ColumnKind
)

// ErrMixedNumericColumn is re-exported so a consumer can errors.Is against it
// without importing pkg/graph/store — the rule every other store sentinel here
// follows.
var ErrMixedNumericColumn = storepkg.ErrMixedNumericColumn

// Column kinds, re-exported so a consumer never imports pkg/graph/store directly.
// Column kinds, re-exported so a consumer never imports pkg/graph/store directly.
const (
	ColInt64   = storepkg.ColInt64
	ColFloat64 = storepkg.ColFloat64
	ColString  = storepkg.ColString
	ColBool    = storepkg.ColBool
)

// ScanNodeColumns reads the named properties of a label's nodes as TYPED COLUMNS,
// when the backend supports it.
//
// ok=false means the backend has no column scan and the caller should fall back to
// Nodes().ByLabel — the capability is optional and consumers must handle both, the
// same contract as every other optional capability.
//
// The point is boxing. Reading properties through GetProperty returns `any`, so a
// caller wanting int64 pays a heap box per value for anything stored as Instant,
// int or int32 — after the concrete type has already been erased. A column scan
// converts inside the store, where the type is still known, and hands back slices.
//
// The callback MUST NOT retain the batch: its slices are reused between calls.
func (g *Graph) ScanNodeColumns(label string, props []string, opts QueryOpts,
	fn func(*ColumnBatch) bool) (ok bool, err error) {

	if g == nil {
		return false, nil
	}
	return g.core.ScanNodeColumns(label, props, opts, fn)
}
