package temporal

import (
	"fmt"
	"testing"
)

// Disjoint sparse cuts make every membership result unchanged. Work should
// scale with source+cut+output pieces, rather than their Cartesian product.
func BenchmarkDifferenceSparseCuts(b *testing.B) {
	for _, count := range []int{128, 512, 2048, 8192} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			l := Limits{MaxValueBytes: 1 << 20, MaxRegionPieces: 65536}
			a, e := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: ProfileIntegerZ, Version: 1, Reference: "bench", CanonicalUnit: "step"}, l)
			if e != nil {
				b.Fatal(e)
			}
			source, cuts := make([]Scope, count), make([]Scope, count)
			for i := range count {
				p, _ := IntegerPosition(a, Int64(int64(i)*4))
				q, _ := IntegerPosition(a, Int64(int64(i)*4+2))
				source[i], e = Point(p)
				if e != nil {
					b.Fatal(e)
				}
				cuts[i], e = Point(q)
				if e != nil {
					b.Fatal(e)
				}
			}
			lhs, e := Region(a, source, l)
			if e != nil {
				b.Fatal(e)
			}
			rhs, e := Region(a, cuts, l)
			if e != nil {
				b.Fatal(e)
			}
			probe, e := lhs.Difference(rhs, l)
			if e != nil {
				b.Fatal(e)
			}
			same, e := probe.SameSupport(lhs, l)
			if e != nil || !same {
				b.Fatal(same, e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, e := lhs.Difference(rhs, l)
				if e != nil || result.Kind() != ScopeRegion {
					b.Fatal(e)
				}
			}
		})
	}
}
