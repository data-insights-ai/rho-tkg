package state

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func BenchmarkStateSliceSmallWindow(b *testing.B) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		for _, count := range []int{64, 512, 4096} {
			b.Run(fmt.Sprintf("profile%d/%d", profile, count), func(b *testing.B) {
				s, _, l := codecFixture(b, profile, count)
				middle := s.Pieces()[count/2]
				lo, _, _ := middle.Scope().Bounds()
				p, _ := lo.Position()
				got, err := s.Slice(middle.Scope(), l.State)
				if err != nil {
					b.Fatal(err)
				}
				sliceAssert(b, got, s.Axis(), []temporal.Scope{middle.Scope()}, []Cell{middle.Cell()})
				far := sliceRegion(b, s.Axis(), s.Pieces()[0].Scope(), s.Pieces()[count-1].Scope())
				got, err = s.Slice(far, l.State)
				if err != nil {
					b.Fatal(err)
				}
				sliceAssert(b, got, s.Axis(), []temporal.Scope{s.Pieces()[0].Scope(), s.Pieces()[count-1].Scope()}, []Cell{s.Pieces()[0].Cell(), s.Pieces()[count-1].Cell()})
				b.Run("At", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						c, err := s.At(p, l.State)
						if err != nil || c != middle.Cell() {
							b.Fatal(c, err)
						}
					}
				})
				b.Run("Point", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						got, err := s.Slice(middle.Scope(), l.State)
						if err != nil || got.Usage().Pieces() != 1 {
							b.Fatal(got, err)
						}
					}
				})
				b.Run("FarDisjoint", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						got, err := s.Slice(far, l.State)
						if err != nil || got.Usage().Pieces() != 2 {
							b.Fatal(got, err)
						}
					}
				})
			})
		}
	}
}
