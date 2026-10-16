package graphapply

import (
	"os"
	"testing"
)

// Reuses the genuine scalar/original/source workflow; the explicit Cost10
// branch adds actual column lifetime, complete scan/probe and timing doors.
func TestNativeGraphImmutableColumnsWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeImmutableColumnsFormat {
		t.Skip("explicit native immutable-columns workload required")
	}
	nativeScalarCreateUpdateWorkload(t, nativeImmutableColumnsFamily)
}
