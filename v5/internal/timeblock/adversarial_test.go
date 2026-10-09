package timeblock

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestRepairedDigestsStillRejectMalformedColumns(t *testing.T) {
	a := axisFor(t, temporal.ProfileIntegerZ)
	for _, l := range []Layout{Partitioned, Sparse, Tagged} {
		rows := makeRows(129, 50, false)
		data, err := Encode(a, rows, l, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		b, err := OpenBorrowed(data, a, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		kindAt := headerBytes + b.ledger.Starts + b.ledger.Ends
		joinAt := kindAt + b.ledger.Kinds
		cases := map[string]func([]byte){
			"reserved":       func(x []byte) { x[68] = 1 },
			"span-count":     func(x []byte) { binary.LittleEndian.PutUint16(x[14:], 130) },
			"missing-span":   func(x []byte) { binary.LittleEndian.PutUint16(x[14:], 0) },
			"unknown-layout": func(x []byte) { x[10] = 255 },
			"column-padding": func(x []byte) { x[headerBytes+b.ledger.Starts-1] |= 0x80 },
		}
		if l == Partitioned {
			cases["duplicate-permutation"] = func(x []byte) { copy(x[joinAt+2:joinAt+4], x[joinAt:joinAt+2]) }
			cases["range-permutation"] = func(x []byte) { binary.LittleEndian.PutUint16(x[joinAt:], 65535) }
		} else {
			cases["unknown-kind"] = func(x []byte) { x[kindAt] = (x[kindAt] &^ 7) | 7 }
			cases["kind-padding"] = func(x []byte) { x[kindAt+b.ledger.Kinds-1] |= 0x80 }
			if l == Sparse {
				cases["wrong-rank"] = func(x []byte) { binary.LittleEndian.PutUint16(x[joinAt:], 99) }
				cases["wrong-terminal-rank"] = func(x []byte) { binary.LittleEndian.PutUint16(x[len(x)-2:], 0) }
			}
		}
		for name, change := range cases {
			t.Run(name, func(t *testing.T) {
				bad := bytes.Clone(data)
				change(bad)
				sealDigest(bad)
				if _, err := OpenBorrowed(bad, a, Limits{}); !errors.Is(err, ErrCorrupt) {
					t.Fatal(l, err)
				}
			})
		}
	}
	// A width-zero column must really be constant; a wider encoding of identical
	// data is noncanonical even with a valid checksum/digest.
	for _, values := range [][]int64{{-1, 0, 1}, {math.MinInt64, math.MaxInt64}, {0}, {-8, -8, -8}} {
		encoded := encodeColumn(values)
		col, err := parseColumn(encoded, len(values))
		if err != nil {
			t.Fatal(err)
		}
		for i, v := range values {
			if col.at(i) != v {
				t.Fatal(values, i, col.at(i))
			}
		}
	}
	if _, err := parseColumn([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0}, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	nonminimal := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if _, err := parseColumn(nonminimal, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	overflow := []byte{64, 255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 255}
	if _, err := parseColumn(overflow, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
func TestOccupancyBoundariesAndRandomReferenceParity(t *testing.T) {
	rng := rand.New(rand.NewPCG(23, 91))
	a := axisFor(t, temporal.ProfileIntegerZ)
	for _, n := range []int{1, 63, 64, 65, 127, 129, 512, 4096} {
		for _, pct := range []int{0, 5, 50, 100} {
			rows := makeRows(n, pct, true)
			for _, l := range []Layout{Partitioned, Sparse, Tagged} {
				b := mustBlock(t, a, rows, l)
				if (l == Partitioned || l == Sparse) && pct == 0 && (b.Ledger().Ends != 0 || b.Ledger().Kinds != 0 || b.Ledger().Join != 0) {
					t.Fatal("point-only extension")
				}
				for i, want := range rows {
					got, err := b.Row(i)
					if err != nil || got != want {
						t.Fatal(n, pct, l, i, got, want, err)
					}
				}
				var visited []Row
				if err := b.Scan(t.Context(), func(_ int, r Row) bool { visited = append(visited, r); return true }); err != nil {
					t.Fatal(err)
				}
				if len(visited) != n {
					t.Fatal(n, len(visited))
				}
				for i, r := range rows {
					if visited[i] != r {
						t.Fatal("scan differs")
					}
				}
			}
		}
	}
	rows := make([]Row, 4096)
	for i := range rows {
		x, y := int64(rng.Uint64()), int64(rng.Uint64())
		if x > y {
			x, y = y, x
		}
		if i%4 == 0 {
			rows[i] = Row{Start: x, Kind: Point}
		} else {
			rows[i] = Row{Start: x, End: y, Kind: Kind(i%4 + 1)}
		}
	} // #nosec G115 -- random signed bit patterns exercise the complete codec range.
	for _, l := range []Layout{Partitioned, Sparse, Tagged} {
		b := mustBlock(t, a, rows, l)
		for i, r := range rows {
			got, err := b.Row(i)
			if err != nil || got != r {
				t.Fatal(i, got, r, err)
			}
		}
	}
}
func TestFromScopeDeclinesOtherShapesExactly(t *testing.T) {
	a := axisFor(t, temporal.ProfileRationalQ)
	lo, err := temporal.FiniteBound(position(t, a, 0), true)
	if err != nil {
		t.Fatal(err)
	}
	unbounded, err := temporal.Span(a, lo, temporal.PositiveInfinity(), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FromScope(a, unbounded); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	half, err := temporal.ParseRational("1/2", temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := temporal.RationalPosition(a, half)
	if err != nil {
		t.Fatal(err)
	}
	hi, err := temporal.FiniteBound(p, false)
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.Span(a, lo, hi, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FromScope(a, s); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	for _, r := range []Row{{Start: -2, End: 2, Kind: ClosedClosed}, {Start: -2, End: 2, Kind: ClosedOpen}, {Start: -2, End: 2, Kind: OpenClosed}} {
		s := referenceScope(t, a, r)
		got, err := FromScope(a, s)
		if err != nil || got != r {
			t.Fatal(got, r, err)
		}
	}
	if _, err := FromScope(temporal.Axis{}, s); !errors.Is(err, temporal.ErrInvalidAxis) {
		t.Fatal(err)
	}
	if _, err := Encode(a, make([]Row, MaxRows+1), Sparse, Limits{}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := Encode(a, nil, Sparse, Limits{MaxBytes: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	data, err := Encode(a, []Row{{Start: 3, Kind: Point}}, Sparse, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	z := axisFor(t, temporal.ProfileIntegerZ)
	if _, err := OpenBorrowed(data, z, Limits{}); !errors.Is(err, temporal.ErrAxisMismatch) {
		t.Fatal(err)
	}
	if _, err := OpenBorrowed(data, a, Limits{MaxRows: -1}); !errors.Is(err, ErrLimits) {
		t.Fatal(err)
	}
	if _, err := OpenBorrowed(data, temporal.Axis{}, Limits{}); !errors.Is(err, temporal.ErrInvalidAxis) {
		t.Fatal(err)
	}
	b := mustBlock(t, a, nil, Sparse)
	if err := b.Scan(nil, func(int, Row) bool { return true }); !errors.Is(err, ErrCallback) { //nolint:staticcheck // Deliberately adversarial nil-context API input.
		t.Fatal(err)
	}
}

func TestConcurrentReadsHaveIndependentValueLifetime(t *testing.T) {
	a := axisFor(t, temporal.ProfileIntegerZ)
	rows := makeRows(MaxRows, 50, true)
	for _, layout := range []Layout{Partitioned, Sparse, Tagged} {
		b := mustBlock(t, a, rows, layout)
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 4 {
					if err := b.Scan(t.Context(), func(i int, r Row) bool {
						if r != rows[i] {
							t.Errorf("shared scan changed row %d", i)
						}
						r.Start = 123 // Materialized values cannot affect other readers.
						got, err := b.Row(i)
						if err != nil || got != rows[i] {
							t.Errorf("lookup changed row %d: %v", i, err)
						}
						return true
					}); err != nil {
						t.Error(err)
					}
				}
			})
		}
		wg.Wait()
	}
}

func TestStressLaneScanAndLookupParity(t *testing.T) {
	a := axisFor(t, temporal.ProfileIntegerZ)
	for _, lane := range []string{"signed-entropy", "long-span-gap"} {
		rows := stressRows(lane)
		for _, layout := range []Layout{Partitioned, Sparse, Tagged} {
			b := mustBlock(t, a, rows, layout)
			if err := b.Scan(t.Context(), func(i int, r Row) bool {
				if r != rows[i] {
					t.Fatalf("%s/%v scan lost row %d", lane, layout, i)
				}
				lookup, err := b.Row(i)
				if err != nil || lookup != r {
					t.Fatal(lookup, r, err)
				}
				for _, x := range []int64{r.Start, r.End, 0, math.MinInt64, math.MaxInt64} {
					want, err := referenceScope(t, a, r).Contains(position(t, a, x), temporal.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					got, err := b.ContainsAt(i, x)
					if err != nil || got != want {
						t.Fatal(i, x, got, want, err)
					}
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
}
