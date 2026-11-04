//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package idlayout

import "testing"

func TestMintInstantMillis_NoAllocs(t *testing.T) {
	id := compose(1<<40, 3, 3)
	var sink int64
	if n := testing.AllocsPerRun(1000, func() { sink += MintInstantMillis(id) }); n != 0 {
		t.Fatalf("MintInstantMillis allocates %v per call, want 0", n)
	}
	_ = sink
}
