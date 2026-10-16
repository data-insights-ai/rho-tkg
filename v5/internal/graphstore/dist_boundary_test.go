package graphstore_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestSinglePartitionPathDoesNotDependOnDist pins the WP-20 build boundary:
// the v5.0 path (pkg/graph -> graphstore -> raftlog) must not pull in any
// package under internal/dist (graphapply, txnproto, replica).
func TestSinglePartitionPathDoesNotDependOnDist(t *testing.T) {
	for _, root := range []string{"../../pkg/graph", ".", "../raftlog"} {
		args := []string{"list", "-test", "-deps", root}
		if root == "." {
			// ownership_replica_test.go drives the replica package through
			// graphstore's unexported ownership helpers and cannot move into
			// dist without exporting them; only its test tier is exempt.
			args = []string{"list", "-deps", root}
		}
		out, err := exec.Command("go", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps %s: %v\n%s", root, err, out)
		}
		for _, p := range strings.Fields(string(out)) {
			if strings.Contains(p, "/internal/dist/") || strings.HasSuffix(p, "/internal/dist") {
				t.Errorf("%s depends on dist package %s", root, p)
			}
		}
	}
}

// TestDistPackagesLiveUnderDist fails while a distributed-engine package
// still sits at its old internal/ path.
func TestDistPackagesLiveUnderDist(t *testing.T) {
	out, err := exec.Command("go", "list", "../...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, p := range strings.Fields(string(out)) {
		for _, name := range []string{"graphapply", "txnproto", "replica"} {
			if strings.HasSuffix(p, "/internal/"+name) {
				t.Errorf("%s must live under internal/dist/", p)
			}
		}
	}
}
