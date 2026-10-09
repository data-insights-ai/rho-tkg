package state

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func benchmarkState(b *testing.B, count int) (State, temporal.Scope, temporal.Position, Limits) {
	b.Helper()
	a := axis(b, temporal.ProfileIntegerZ)
	l := Limits{MaxPieces: 65536, MaxChangePieces: 65536, MaxMetadataBytes: 64 << 20, MaxChangeMetadataBytes: 64 << 20, Temporal: temporal.Limits{MaxRegionPieces: 65536, MaxValueBytes: 1 << 20}}
	s, e := New(a, l)
	if e != nil {
		b.Fatal(e)
	}
	var fills, cuts []temporal.Scope
	for i := range count {
		p, _ := temporal.Point(position(b, a, int64(i*4), 1, 0))
		q, _ := temporal.Point(position(b, a, int64(i*4+2), 1, 0))
		fills = append(fills, p)
		cuts = append(cuts, q)
	}
	fill, e := temporal.Region(a, fills, l.Temporal)
	if e != nil {
		b.Fatal(e)
	}
	cut, e := temporal.Region(a, cuts, l.Temporal)
	if e != nil {
		b.Fatal(e)
	}
	result, e := s.Set(fill, Null(), revision(b, 1), l)
	if e != nil {
		b.Fatal(e)
	}
	return result.State(), cut, position(b, a, int64(count/2*4), 1, 0), l
}
func BenchmarkStateSparseCorrections(b *testing.B) {
	for _, count := range []int{128, 256, 512, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s, cut, _, l := benchmarkState(b, count)
			r := revision(b, 2)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, e := s.Unset(cut, r, l); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
func BenchmarkStateAtCachedPolicy(b *testing.B) {
	for _, count := range []int{128, 256, 512, 1024, 2048} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s, _, p, l := benchmarkState(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c, e := s.At(p, l)
				if e != nil || !c.Present() {
					b.Fatal(c, e)
				}
			}
		})
	}
}
