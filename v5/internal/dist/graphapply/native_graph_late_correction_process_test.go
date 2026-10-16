package graphapply

import (
	"os"
	"testing"
)

func TestNativeGraphLateCorrectionWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeLateCorrectionFormat {
		t.Skip("explicit native half-correction workload required")
	}
	nativeLateCorrectionWorkload(t)
}
