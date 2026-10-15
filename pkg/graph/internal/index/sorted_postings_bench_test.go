package index

import (
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// BenchmarkPostingsBytesPerMember (backlog 72) reports the heap bytes one
// posting costs in a value key of 1,000,000 members: the composite index and
// the plain property index (one int and one string value), each filled in
// ascending ID order (snowflake writes) and in shuffled order.
func BenchmarkPostingsBytesPerMember(b *testing.B) {
	const n = 1_000_000
	ids := func(shuffled bool) []snowflake.ID {
		out := make([]snowflake.ID, n)
		for i := range out {
			out[i] = snowflake.ID(int64(i+1) << 15)
		}
		if shuffled {
			x := uint64(88172645463325252)
			for i := n - 1; i > 0; i-- {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				j := int(x % uint64(i+1)) // #nosec G115 -- test shuffle
				out[i], out[j] = out[j], out[i]
			}
		}
		return out
	}
	cases := []struct {
		name     string
		shuffled bool
		build    func(ids []snowflake.ID) any
	}{
		{"composite/ascending", false, func(ids []snowflake.ID) any {
			ci := NewCompositePropertyIndex([]string{"a", "b"})
			for _, id := range ids {
				ci.AddKey(id, "vk")
			}
			return ci
		}},
		{"composite/shuffled", true, func(ids []snowflake.ID) any {
			ci := NewCompositePropertyIndex([]string{"a", "b"})
			for _, id := range ids {
				ci.AddKey(id, "vk")
			}
			return ci
		}},
		{"property-int/ascending", false, func(ids []snowflake.ID) any {
			pi := NewPropertyIndex()
			for _, id := range ids {
				pi.AddKey(id, "i64:1")
			}
			return pi
		}},
		{"property-int/shuffled", true, func(ids []snowflake.ID) any {
			pi := NewPropertyIndex()
			for _, id := range ids {
				pi.AddKey(id, "i64:1")
			}
			return pi
		}},
		{"property-string/ascending", false, func(ids []snowflake.ID) any {
			pi := NewPropertyIndex()
			for _, id := range ids {
				pi.AddKey(id, "s:x")
			}
			return pi
		}},
		{"property-string/shuffled", true, func(ids []snowflake.ID) any {
			pi := NewPropertyIndex()
			for _, id := range ids {
				pi.AddKey(id, "s:x")
			}
			return pi
		}},
		{"property-distinct/ascending", false, func(ids []snowflake.ID) any {
			// One member per value: the per-key overhead dominates.
			pi := NewPropertyIndex()
			for i, id := range ids {
				pi.AddKey(id, "i64:"+itoa(int64(i)))
			}
			return pi
		}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			in := ids(c.shuffled)
			for i := 0; i < b.N; i++ {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				idx := c.build(in)
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/n, "B/member") // #nosec G115 -- heap sizes
				runtime.KeepAlive(idx)
			}
		})
	}
}
