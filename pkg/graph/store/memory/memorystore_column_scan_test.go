package memory

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ScanNodeColumns hands out TYPED slices so a consumer wanting scalars never
// boxes. Input validation is pinned here; the data path is exercised through the
// store contract suite alongside NodesByLabel, which it delegates to.

// A NIL CALLBACK must error rather than panic, and a nil store must error rather
// than dereference.
func TestScanNodeColumns_RejectsNilInputs(t *testing.T) {
	ms := New()
	defer func() { _ = ms.Close() }()
	const tok uint16 = 1
	if err := ms.ScanNodeColumns(tok, nil, storecontract.QueryOpts{}, nil); err == nil {
		t.Error("a nil callback was accepted")
	}
	var nilStore *Store
	if err := nilStore.ScanNodeColumns(tok, nil, storecontract.QueryOpts{},
		func(*storecontract.ColumnBatch) bool { return true }); err == nil {
		t.Error("a nil store was accepted")
	}
}
