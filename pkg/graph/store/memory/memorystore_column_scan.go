package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ScanNodeColumns implements store.NodeColumnScanCapability.
//
// WHY THIS EXISTS ALONGSIDE NodesByLabel. A consumer that wants scalar columns gets
// []*types.Node and reads each property through GetProperty, which returns `any`.
// Values stored as Instant, int or int32 then cost a heap box each on the way to
// int64, and the caller cannot avoid that because the conversion happens after the
// type has been erased. Here the stored type is still known, so the conversion goes
// straight into a typed slice and no box is created at all.
//
// The kind-resolution and refusal rules live in store.ScanColumnsFromNodes, shared
// with every other backend — two copies would be two chances to disagree about when
// a column refuses versus reports a row absent, and a consumer would see different
// answers from two stores holding the same data.
func (ms *Store) ScanNodeColumns(token uint16, props []string, opts storecontract.QueryOpts,
	fn func(*storecontract.ColumnBatch) bool) error {

	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	nodes, err := ms.NodesByLabel(token, opts)
	if err != nil {
		return err
	}
	return storecontract.ScanColumnsFromNodes(nodes, props, fn)
}
