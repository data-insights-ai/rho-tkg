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

// mintCases are shared by the node and relationship tables (rule 2 parity).
var mintCases = []struct {
	name string
	raw  int64
	want Instant
}{
	{"time field 0 is exactly the epoch (off-by-epoch mutant fails)", mintID(0, 0, 1), epochMsForMint},
	{"999 us floors to the same ms (round instead of floor fails)", mintID(999, 0, 1), epochMsForMint},
	{"1000 us is +1 ms (unit confusion us/ms fails)", mintID(1000, 0, 1), epochMsForMint + 1},
	{"one second", mintID(1_000_000, 0, 0), epochMsForMint + 1000},
	{"one day", mintID(86_400_000_000, 2, 5), epochMsForMint + 86_400_000},
	{"bit 47 of the time field (sign/overflow at high bits)", mintID(1<<47, 0, 0), epochMsForMint + Instant((int64(1)<<47)/1000)},
	{"max time field, node 31, step 1023", mintID(1<<48-1, 31, 1023), epochMsForMint + Instant((int64(1)<<48-1)/1000)},
	{"MaxInt64", math.MaxInt64, epochMsForMint + Instant((int64(1)<<48-1)/1000)},
	{"zero is unset", 0, 0},
	{"minus one", -1, 0},
	{"negative with plausible time bits", -mintID(5_000_000, 3, 3), 0},
	{"MinInt64", math.MinInt64, 0},
}

func TestNodeID_MintInstant(t *testing.T) {
	t.Parallel()
	for _, tc := range mintCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got Instant = NodeID(tc.raw).MintInstant()
			if got != tc.want {
				t.Fatalf("NodeID(%d).MintInstant() = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

func TestRelID_MintInstant(t *testing.T) {
	t.Parallel()
	for _, tc := range mintCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got Instant = RelID(tc.raw).MintInstant()
			if got != tc.want {
				t.Fatalf("RelID(%d).MintInstant() = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

// Nodes mint with the even node field (SnowflakeNodeID*2), relationships with
// the odd one (*2+1); the time bits are the same, so the instant must be too —
// across every SnowflakeNodeID 0-15 and both kinds (a node-field-offset mutant
// that shifts by the node field fails here).
func TestMintInstant_NodeRelParityAcrossEverySnowflakeNodeID(t *testing.T) {
	t.Parallel()
	const us = 987_654_321
	want := epochMsForMint + us/1000
	for snowNode := int64(0); snowNode <= 15; snowNode++ {
		for _, step := range []int64{0, 7, 1023} {
			n := NodeID(mintID(us, snowNode*2, step))
			r := RelID(mintID(us, snowNode*2+1, step))
			if got := n.MintInstant(); got != want {
				t.Fatalf("SnowflakeNodeID %d step %d: node = %d, want %d", snowNode, step, got, want)
			}
			if got := r.MintInstant(); got != want {
				t.Fatalf("SnowflakeNodeID %d step %d: rel = %d, want %d", snowNode, step, got, want)
			}
		}
	}
}

func TestMintInstant_MonotoneInIDOrder(t *testing.T) {
	t.Parallel()
	raws := []int64{1, mintID(1, 31, 1023), mintID(1000, 0, 0), mintID(1000, 31, 1023), mintID(1001, 0, 0), mintID(1<<40, 3, 3), mintID(1<<47, 0, 0), math.MaxInt64}
	var prevN, prevR Instant
	for i, raw := range raws {
		if i > 0 && raw <= raws[i-1] {
			t.Fatalf("fixture not ascending at %d", i)
		}
		n, r := NodeID(raw).MintInstant(), RelID(raw).MintInstant()
		if n < prevN || r < prevR {
			t.Fatalf("not monotone at %d: node %d<%d or rel %d<%d", i, n, prevN, r, prevR)
		}
		if n != r {
			t.Fatalf("node/rel parity broken for the same raw ID %d: %d vs %d", raw, n, r)
		}
		prevN, prevR = n, r
	}
}

func TestMintInstant_NoAllocs(t *testing.T) {
	n, r := NodeID(mintID(1<<40, 4, 4)), RelID(mintID(1<<40, 5, 4))
	var sink Instant
	if a := testing.AllocsPerRun(1000, func() { sink += n.MintInstant() + r.MintInstant() }); a != 0 {
		t.Fatalf("MintInstant allocates %v per call, want 0", a)
	}
	_ = sink
}
