package state

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// codecFixture builds in linear time using the same checked linear builder,
// keeping fixture construction and exact roundtrip assertions outside timers.
func codecFixture(t testing.TB, profile temporal.Profile, count int) (State, []Change, CodecLimits) {
	t.Helper()
	a := axis(t, profile)
	l := DefaultCodecLimits()
	builder := stateBuilder{limits: l.State, usage: initialUsage(a), parts: make([]Piece, 0, count)}
	changes := changeBuilder{limits: l.State, usage: initialUsage(a), parts: make([]Change, 0, count)}
	for i := range count {
		p, err := temporal.Point(position(t, a, int64(i*2), 3, int64(i%7)))
		if err != nil {
			t.Fatal(err)
		}
		c := Cell{present: true, value: value(t, uint64(i+1), 1), revision: revision(t, uint64(i+1))}
		if err := builder.add(p, c); err != nil {
			t.Fatal(err)
		}
		if err := changes.add(p, Cell{}, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.finish(); err != nil {
		t.Fatal(err)
	}
	return State{axis: a, pieces: builder.parts, usage: builder.usage, policy: l.State, valid: true}, changes.parts, l
}
func BenchmarkCodec(b *testing.B) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		for _, count := range []int{64, 512, 4096} {
			b.Run(fmt.Sprintf("profile%d/%d", profile, count), func(b *testing.B) {
				s, changes, l := codecFixture(b, profile, count)
				swire, err := AppendState(nil, s, l)
				if err != nil {
					b.Fatal(err)
				}
				cwire, err := AppendChanges(nil, s.Axis(), changes, l)
				if err != nil {
					b.Fatal(err)
				}
				got, err := DecodeState(swire, s.Axis(), l)
				if err != nil {
					b.Fatal(err)
				}
				codecEqualState(b, s, got)
				cgot, usage, err := DecodeChanges(cwire, s.Axis(), l)
				if err != nil || usage != s.usageWithChanges(changes) {
					b.Fatal("fixture changes", usage, err)
				}
				codecEqualChanges(b, changes, cgot)
				again, err := AppendState(nil, got, l)
				if err != nil || !bytes.Equal(swire, again) {
					b.Fatal("fixture bytes", err)
				}
				b.Run("DecodeState", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(swire)))
					for b.Loop() {
						if _, err := DecodeState(swire, s.Axis(), l); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("DecodeChanges", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(cwire)))
					for b.Loop() {
						if _, _, err := DecodeChanges(cwire, s.Axis(), l); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("AppendState", func(b *testing.B) {
					dst := make([]byte, 0, len(swire))
					b.ReportAllocs()
					b.SetBytes(int64(len(swire)))
					for b.Loop() {
						if _, err := AppendState(dst[:0], s, l); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("AppendChanges", func(b *testing.B) {
					dst := make([]byte, 0, len(cwire))
					b.ReportAllocs()
					b.SetBytes(int64(len(cwire)))
					for b.Loop() {
						if _, err := AppendChanges(dst[:0], s.Axis(), changes, l); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

// usageWithChanges is test-only accounting, independent of codec inspection.
func (s State) usageWithChanges(changes []Change) Usage {
	u := initialUsage(s.axis)
	for _, c := range changes {
		u.pieces++
		u.metadataBytes += c.scopeBytes + 2*cellMetadataBytes
		u.references += c.before.references() + c.after.references()
	}
	return u
}
