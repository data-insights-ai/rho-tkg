package graphapply

import (
	"os"
	"testing"
)

// The complete retained-C / selector lifecycle is shared with the original
// explicit parent; only this admitted profile adds genuine endpoint guards.
func TestNativeGraphGuardedSnapshotFeedWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeGuardedFeedFormat {
		t.Skip("explicit guarded feed workload required")
	}
	spec, err := nativeGuardedFeedSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	a, digest, err := nativeGuardedFeedAdmissionFromEnv(spec)
	if err != nil {
		t.Fatal(err)
	}
	runNativeSnapshotFeedWorkload(t, spec, a, digest)
}
func TestNativeGraphGuardedSnapshotSelectorsWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeGuardedSelectorsFormat {
		t.Skip("explicit guarded selectors workload required")
	}
	spec, err := nativeGuardedSelectorsSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	a, digest, err := nativeGuardedSelectorsAdmissionFromEnv(spec)
	if err != nil {
		t.Fatal(err)
	}
	runNativeSnapshotSelectorsWorkload(t, spec, a, digest)
}
