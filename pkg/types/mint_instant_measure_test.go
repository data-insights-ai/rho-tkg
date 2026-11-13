//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package types

import "testing"

func TestMintInstant_NoAllocs(t *testing.T) {
	n, r := NodeID(mintID(1<<40, 4, 4)), RelID(mintID(1<<40, 5, 4))
	var sink Instant
	if a := testing.AllocsPerRun(1000, func() { sink += n.MintInstant() + r.MintInstant() }); a != 0 {
		t.Fatalf("MintInstant allocates %v per call, want 0", a)
	}
	_ = sink
}
