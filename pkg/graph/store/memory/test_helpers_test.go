package memory

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// mustPropertySlice constructs a PropertySlice from a map and fails the test on
// validation error. Local copy of the helper that lives in pkg/graph for the
// integration tests.
func mustPropertySlice(t *testing.T, m map[string]any) types.PropertySlice {
	t.Helper()
	ps, err := types.NewPropertySlice(m)
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	return ps
}
