package graph

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// mustPropertySlice is a shared test helper for constructing a PropertySlice
// from a map. Lives here in pkg/graph alongside the integration tests; the
// store-package wire_test has its own copy because that package can't
// import pkg/graph.
func mustPropertySlice(t *testing.T, m map[string]any) types.PropertySlice {
	t.Helper()
	ps, err := types.NewPropertySlice(m)
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	return ps
}
