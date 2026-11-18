package types

import (
	"math"
	"testing"
)

// epochMsForMint is 2026-01-01T00:00:00Z in Unix milliseconds, a literal so a
// faulty epoch in the implementation cannot also corrupt the oracle.
const epochMsForMint Instant = 1767225600000

// mintID composes an ID by hand from the documented layout
// (1 zero bit | 48 bits microseconds since the epoch | 5 bits node | 10 bits step).
func mintID(us, nodeField, step int64) int64 { return us<<15 | nodeField<<10 | step }

// TestMintInstant_WiresIDLayout checks that NodeID.MintInstant and
// RelID.MintInstant hand the raw ID to idlayout.MintInstantMillis unchanged:
// the decode table, node-field parity, the non-positive rule and monotonicity
// are proven once, in pkg/internal/idlayout. The rows here catch a wrapper that
// shifts by the node field (even for nodes, odd for relationships), uses the
// wrong epoch, or passes a non-positive ID through.
func TestMintInstant_WiresIDLayout(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  int64
		want Instant
	}{
		{"one day, node field 2", mintID(86_400_000_000, 2, 5), epochMsForMint + 86_400_000},
		{"one day, node field 3", mintID(86_400_000_000, 3, 5), epochMsForMint + 86_400_000},
		{"999 us floors to the epoch", mintID(999, 0, 1), epochMsForMint},
		{"MaxInt64", math.MaxInt64, epochMsForMint + Instant((int64(1)<<48-1)/1000)},
		{"zero is unset", 0, 0},
		{"negative with plausible time bits", -mintID(5_000_000, 3, 3), 0},
	}
	for _, tc := range cases {
		if got := NodeID(tc.raw).MintInstant(); got != tc.want {
			t.Errorf("%s: NodeID(%d).MintInstant() = %d, want %d", tc.name, tc.raw, got, tc.want)
		}
		if got := RelID(tc.raw).MintInstant(); got != tc.want {
			t.Errorf("%s: RelID(%d).MintInstant() = %d, want %d", tc.name, tc.raw, got, tc.want)
		}
	}
}
