package idlayout

import (
	"math"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// epochMs is 2026-01-01T00:00:00Z in Unix milliseconds, written out as a
// literal so a wrong epoch in the implementation cannot also corrupt the oracle.
const epochMs int64 = 1767225600000

// compose builds an ID by hand from the documented layout
// (1 zero bit | 48 bits microseconds since the epoch | 5 bits node | 10 bits step).
func compose(us, node, step int64) snowflake.ID {
	return snowflake.ID(us<<15 | node<<10 | step)
}

func TestEpochIsFixed(t *testing.T) {
	t.Parallel()
	if got := Epoch.UnixMilli(); got != epochMs {
		t.Fatalf("Epoch = %d ms, want %d (2026-01-01T00:00:00Z)", got, epochMs)
	}
	if Epoch.Location() != time.UTC {
		t.Fatalf("Epoch location = %v, want UTC", Epoch.Location())
	}
}

func TestLayoutShape(t *testing.T) {
	t.Parallel()
	if !Layout.Micro() {
		t.Fatal("Layout is not microsecond mode (documented 48-bit microseconds)")
	}
	if !Layout.Epoch().Equal(Epoch) {
		t.Fatalf("Layout.Epoch() = %v, want %v", Layout.Epoch(), Epoch)
	}
	// 5 node bits, 10 step bits: node 31 / step 1023 must survive Decompose.
	p := Layout.Decompose(compose(7, 31, 1023))
	if p.Time != 7 || p.Node != 31 || p.Step != 1023 {
		t.Fatalf("Decompose = %+v, want {7 31 1023}", p)
	}
}

func TestMintInstantMillis_Table(t *testing.T) {
	t.Parallel()
	maxUS := int64(1)<<48 - 1
	tests := []struct {
		name string
		id   snowflake.ID
		want int64
	}{
		// A wrong epoch (1970, 2025) fails every row.
		{"time field 0 is exactly the epoch", compose(0, 0, 1), epochMs},
		{"999 us truncates down, not rounds", compose(999, 0, 1), epochMs},
		{"1000 us is exactly +1 ms", compose(1000, 0, 1), epochMs + 1},
		{"1500 us floors to +1 ms", compose(1500, 0, 1), epochMs + 1},
		{"one second of microseconds is +1000 ms (unit confusion us vs ms)", compose(1_000_000, 0, 0), epochMs + 1000},
		{"one hour", compose(3_600_000_000, 0, 0), epochMs + 3_600_000},
		{"high bit of the 48-bit time field set", compose(1<<47, 0, 0), epochMs + (int64(1)<<47)/1000},
		{"max 48-bit time field, all node and step bits set", compose(maxUS, 31, 1023), epochMs + maxUS/1000},
		{"node 31 step 1023 at us 0 does not leak into time", compose(0, 31, 1023), epochMs},
		{"MaxInt64 (all 63 bits set) decodes without overflow", snowflake.ID(math.MaxInt64), epochMs + maxUS/1000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := MintInstantMillis(tc.id); got != tc.want {
				t.Fatalf("MintInstantMillis(%d) = %d, want %d", int64(tc.id), got, tc.want)
			}
		})
	}
}

func TestMintInstantMillis_NodeFieldDoesNotMoveTheInstant(t *testing.T) {
	t.Parallel()
	// Nodes use the even node field (SnowflakeNodeID*2), rels the odd one
	// (*2+1); all 32 node-field values (every SnowflakeNodeID 0-15, both
	// kinds) at the same tick and step carry the same time bits.
	const us = 123_456_789
	want := epochMs + us/1000
	for node := int64(0); node < 32; node++ {
		for _, step := range []int64{0, 1, 1023} {
			if got := MintInstantMillis(compose(us, node, step)); got != want {
				t.Fatalf("node field %d step %d: %d, want %d", node, step, got, want)
			}
		}
	}
}

func TestMintInstantMillis_NonPositiveIsUnset(t *testing.T) {
	t.Parallel()
	for _, id := range []snowflake.ID{0, -1, -2, -(1 << 15), -(1 << 62), snowflake.ID(math.MinInt64)} {
		if got := MintInstantMillis(id); got != 0 {
			t.Fatalf("MintInstantMillis(%d) = %d, want 0 (non-positive IDs are never minted; 0 = unset)", int64(id), got)
		}
	}
}

func TestMintInstantMillis_MonotoneInIDOrder(t *testing.T) {
	t.Parallel()
	ids := []snowflake.ID{1, 1 << 14, 1 << 15, compose(1, 31, 1023), compose(1000, 0, 0), compose(1000, 31, 1023), compose(1001, 0, 0), compose(1<<40, 3, 3), compose(1<<47, 0, 0), snowflake.ID(math.MaxInt64)}
	prev := int64(-1)
	for i, id := range ids {
		if i > 0 && id <= ids[i-1] {
			t.Fatalf("test fixture not ascending at %d", i)
		}
		got := MintInstantMillis(id)
		if got < prev {
			t.Fatalf("not monotone: id %d -> %d after %d", int64(id), got, prev)
		}
		prev = got
	}
	if first, last := MintInstantMillis(ids[0]), MintInstantMillis(ids[len(ids)-1]); last <= first {
		t.Fatalf("not strictly increasing end to end: first %d last %d", first, last)
	}
}

func TestMintInstantMillis_AgreesWithLayoutCreatedAt(t *testing.T) {
	t.Parallel()
	for _, us := range []int64{0, 1, 999, 1000, 999_999, 1_000_001, 1 << 30, 1<<47 + 12345, 1<<48 - 1} {
		id := compose(us, 5, 9)
		if got, want := MintInstantMillis(id), Layout.CreatedAt(id).UnixMilli(); got != want {
			t.Fatalf("us %d: %d, want CreatedAt.UnixMilli %d", us, got, want)
		}
	}
}

func TestMintInstantMillis_NoAllocs(t *testing.T) {
	id := compose(1<<40, 3, 3)
	var sink int64
	if n := testing.AllocsPerRun(1000, func() { sink += MintInstantMillis(id) }); n != 0 {
		t.Fatalf("MintInstantMillis allocates %v per call, want 0", n)
	}
	_ = sink
}
