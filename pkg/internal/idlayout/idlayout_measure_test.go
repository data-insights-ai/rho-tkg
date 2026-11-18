//go:build measure

// Measurement level (RH 4 Nov): testing.AllocsPerRun reads the process-wide
// malloc count, and this package runs parallel tests, so the bound runs by
// hand, not in a gate:
//
//	go test -tags measure -run TestMintInstantMillis_NoAllocs ./pkg/internal/idlayout/

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
