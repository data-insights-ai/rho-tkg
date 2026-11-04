//go:build measure

// Measurement level (René 4 Nov, `make measure`): live heap after a GC moves
// with load and other goroutines, so this bound runs by hand, not in a gate.

package badger

import (
	"os"
	"os/exec"
	"testing"
)

// existMapChildEnv marks the child process TestExistMapResidentBytesPerNode
// measures in.
const existMapChildEnv = "RHO_EXISTMAP_CHILD"

// TestExistMapResidentBytesPerNode pins EXISTMAP's budget (SCALE, lesson 89):
// an open shard may keep at most 12 B of live heap per stored node (v4.83.0:
// ~83 B live, ~118 B of in-use spans, mostly the nodeIDs and nodeRevs maps,
// so a 4-billion-fact day-shard needed ~250 GB). Measured as live bytes after
// a GC (HeapAlloc; HeapInuse adds span slack) and as the slope between two
// store sizes after a clean reopen, so the fixed costs cancel. It measures in
// a fresh run of the test binary: in the package's own process the heap that
// earlier tests' stores still hold moves by megabytes between the two reads
// (20 B per node read there, 6.5-8.4 B alone). The race detector's shadow
// allocations get their own bound, still far below the per-entity maps.
func TestExistMapResidentBytesPerNode(t *testing.T) {
	if os.Getenv(existMapChildEnv) == "" {
		if testing.Short() && !raceEnabled {
			t.Skip("builds and reopens two disk stores of 50 k and 250 k nodes")
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestExistMapResidentBytesPerNode$", "-test.count=1", "-test.v") // #nosec G204 -- the test binary itself
		cmd.Env = append(os.Environ(), existMapChildEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("measuring child: %v\n%s", err, out)
		}
		t.Logf("%s", out)
		return
	}
	const small, large = 50_000, 250_000
	hs := existMapHeapAfterReopen(t, small)
	hl := existMapHeapAfterReopen(t, large)
	perNode := (float64(hl) - float64(hs)) / (large - small)
	t.Logf("reopened store: %d B at %d nodes, %d B at %d nodes: %.1f B per node", hs, small, hl, large, perNode)
	budget := 12.0
	if raceEnabled {
		budget = 30
	}
	if perNode > budget {
		t.Fatalf("an open store keeps %.1f B of heap per node, budget %.0f B", perNode, budget)
	}
}
