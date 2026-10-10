package index

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// BenchmarkRangeCardinality (round 4 R2): one range count over an index of
// 100,000 entries with D distinct integer values, the range covering about
// 60 % of them (sigma-tkgd C4h RangeCount_Broad is D=100 over 62 values) and
// a narrow one (5 values), on a plain index (the walk) and on one created with
// range counts (prefix sums).
func BenchmarkRangeCardinality(b *testing.B) {
	for _, mode := range []struct {
		name   string
		counts bool
	}{{"plain", false}, {"counted", true}} {
		for _, distinct := range []int{100, 10_000, 100_000} {
			benchRangeCardinality(b, mode.name, mode.counts, distinct)
		}
	}
}

func benchRangeCardinality(b *testing.B, mode string, counts bool, distinct int) {
	{
		pi := NewPropertyIndexWith(counts)
		for i := range 100_000 {
			pi.AddKey(snowflake.ID(i+1), fmt.Sprintf("i64:%d", i%distinct))
		}
		for _, r := range []struct {
			name   string
			lo, hi float64
		}{
			{"broad", float64(distinct) * 0.38, float64(distinct)},
			{"narrow", float64(distinct) / 2, float64(distinct)/2 + 4},
		} {
			b.Run(fmt.Sprintf("%s/distinct=%d/%s", mode, distinct, r.name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if n, ok := pi.RangeCardinality(r.lo, r.hi, false, true); !ok || n == 0 {
						b.Fatal(n, ok)
					}
				}
			})
		}
	}
}

// BenchmarkPropertyIndexAddRemove: 100,000 adds into 1,000 values, then
// their removal — the write cost of a plain index (unchanged by round 4) and
// of one created with range counts.
func BenchmarkPropertyIndexAddRemove(b *testing.B) {
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("i64:%d", i)
	}
	for _, mode := range []struct {
		name   string
		counts bool
	}{{"plain", false}, {"counted", true}} {
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				pi := NewPropertyIndexWith(mode.counts)
				for i := range 100_000 {
					pi.AddKey(snowflake.ID(i+1), keys[i%1000])
				}
				for i := range 100_000 {
					pi.removeKey(snowflake.ID(i+1), keys[i%1000])
				}
			}
		})
	}
}
