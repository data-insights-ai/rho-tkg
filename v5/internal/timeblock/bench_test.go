package timeblock

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/golang/snappy"
)

// Each input has exactly the same logical rows for all layouts. Late order is
// a deterministic permutation, never a different population or duration.
func makeRows(n, spanPercent int, late bool) []Row {
	rows := make([]Row, n)
	for i := range rows {
		source := i
		if late {
			source = (i * 37) % n
		}
		r := Row{Start: int64(source*100 - 200000), Kind: Point}
		// Exactly floor(n*percentage/100) spans, interspersed deterministically.
		// Every fixture size used here is coprime to 73.
		if (source*73)%n < n*spanPercent/100 {
			r.End = r.Start + int64(50+source%97)
			r.Kind = Kind(source%4 + 1)
		}
		rows[i] = r
	}
	return rows
}

func stressRows(lane string) []Row {
	if lane == "long-span-gap" {
		rows := makeRows(MaxRows, 5, true)
		for i := range rows {
			if rows[i].Kind != Point && i%13 == 0 {
				rows[i].End = math.MaxInt64 - int64(i)
			}
		}
		return rows
	}
	rng := rand.New(rand.NewPCG(87, 123))
	rows := make([]Row, MaxRows)
	for i := range rows {
		x, y := int64(rng.Uint64()), int64(rng.Uint64()) // #nosec G115 -- signed bit patterns cover high-entropy coordinates.
		if x > y {
			x, y = y, x
		}
		rows[i] = Row{Start: x, Kind: Point}
		if i%100 < 5 {
			rows[i].End = y
			rows[i].Kind = Kind(i%4 + 1)
		}
	}
	return rows
}

func BenchmarkStressCoordinates(b *testing.B) {
	a := axisFor(b, temporal.ProfileIntegerZ)
	for _, lane := range []string{"signed-entropy", "long-span-gap"} {
		rows := stressRows(lane)
		for _, layout := range []Layout{Partitioned, Sparse, Tagged} {
			b.Run(fmt.Sprintf("%s/layout=%d", lane, layout), func(b *testing.B) {
				data, err := Encode(a, rows, layout, Limits{})
				if err != nil {
					b.Fatal(err)
				}
				block, err := OpenBorrowed(data, a, Limits{})
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				var sum int64
				b.ReportAllocs()
				for b.Loop() {
					if err := block.Scan(ctx, func(_ int, r Row) bool { sum ^= r.Start; sum ^= r.End; return true }); err != nil {
						b.Fatal(err)
					}
				}
				benchSink = sum
				b.ReportMetric(float64(block.Ledger().Total)/float64(len(rows)), "block-B/row")
				b.ReportMetric(float64(blockSnappyBytes(data))/float64(len(rows)), "snappy-B/row")
				b.ReportMetric(float64(len(rows))*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			})
		}
	}
}

// A 4 KiB block-chunk Snappy + 4-byte length estimate, not a shipping compressed
// format or a database footprint. Counts physical envelope/kinds/join as well.
func blockSnappyBytes(data []byte) int {
	total := 0
	for len(data) > 0 {
		n := min(4096, len(data))
		total += 4 + len(snappy.Encode(nil, data[:n]))
		data = data[n:]
	}
	return total
}
func BenchmarkLayouts(b *testing.B) {
	a := axisFor(b, temporal.ProfileIntegerZ)
	for _, n := range []int{64, 512, 4096} {
		for _, pct := range []int{0, 5, 50, 100} {
			for _, late := range []bool{false, true} {
				rows := makeRows(n, pct, late)
				for _, l := range []Layout{Partitioned, Sparse, Tagged} {
					b.Run(fmt.Sprintf("n=%d/spans=%d/late=%t/layout=%d", n, pct, late, l), func(b *testing.B) {
						data, err := Encode(a, rows, l, Limits{})
						if err != nil {
							b.Fatal(err)
						}
						block, err := OpenBorrowed(data, a, Limits{})
						if err != nil {
							b.Fatal(err)
						}
						ctx := context.Background()
						var sum int64
						b.ReportAllocs()

						for b.Loop() {
							if err := block.Scan(ctx, func(_ int, r Row) bool { sum ^= r.Start; sum ^= r.End; return true }); err != nil {
								b.Fatal(err)
							}
						}
						benchSink = sum
						b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
						spans := 0
						for _, r := range rows {
							if r.Kind != Point {
								spans++
							}
						}
						b.ReportMetric(float64(spans), "span-rows")
						ledger := block.Ledger()
						b.ReportMetric(float64(ledger.Total)/float64(n), "block-B/row")
						b.ReportMetric(float64(blockSnappyBytes(data))/float64(n), "snappy-B/row")
						b.ReportMetric(float64(ledger.Join)/float64(n), "join-B/row")
						b.ReportMetric(float64(ledger.Envelope)/float64(n), "meta-B/row")
					})
				}
			}
		}
	}
}

var benchSink int64

func BenchmarkLookupAndOpen(b *testing.B) {
	a := axisFor(b, temporal.ProfileIntegerZ)
	rows := makeRows(MaxRows, 5, true)
	for _, l := range []Layout{Partitioned, Sparse, Tagged} {
		data, err := Encode(a, rows, l, Limits{})
		if err != nil {
			b.Fatal(err)
		}
		block, err := OpenBorrowed(data, a, Limits{})
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("lookup/layout=%d", l), func(b *testing.B) {
			i := 0
			var sum int64
			b.ReportAllocs()
			for b.Loop() {
				r, err := block.Row(i)
				if err != nil {
					b.Fatal(err)
				}
				sum ^= r.Start
				i = (i + 37) % MaxRows
			}
			benchSink = sum
		})
		b.Run(fmt.Sprintf("open-borrowed/layout=%d", l), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				v, err := OpenBorrowed(data, a, Limits{})
				if err != nil {
					b.Fatal(err)
				}
				benchSink = int64(v.Len())
			}
		})
		b.Run(fmt.Sprintf("open-owned/layout=%d", l), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				v, err := OpenOwned(data, a, Limits{})
				if err != nil {
					b.Fatal(err)
				}
				benchSink = int64(v.Len())
			}
		})
		b.Run(fmt.Sprintf("encode/layout=%d", l), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				v, err := Encode(a, rows, l, Limits{})
				if err != nil {
					b.Fatal(err)
				}
				benchSink = int64(len(v))
			}
		})
	}
}

// Synthetic coordinate rows at the plan's HOP row-count scales, not the
// synthhop relationship corpus or a graph database. Each input page is bounded;
// the fixture retains only page bytes/views, not one object per coordinate.
func BenchmarkCorpusScan(b *testing.B) {
	a := axisFor(b, temporal.ProfileIntegerZ)
	for _, n := range []int{107113, 408282, 1584150} {
		for _, layout := range []Layout{Partitioned, Sparse, Tagged} {
			b.Run(fmt.Sprintf("rows=%d/layout=%d", n, layout), func(b *testing.B) {
				blocks := make([]*Block, 0, (n+MaxRows-1)/MaxRows)
				total, meta, join := 0, 0, 0
				for base := 0; base < n; base += MaxRows {
					rows := makeRows(min(MaxRows, n-base), 5, true)
					data, err := Encode(a, rows, layout, Limits{})
					if err != nil {
						b.Fatal(err)
					}
					block, err := OpenBorrowed(data, a, Limits{})
					if err != nil {
						b.Fatal(err)
					}
					blocks = append(blocks, block)
					ledger := block.Ledger()
					total += ledger.Total
					meta += ledger.Envelope
					join += ledger.Join
				}
				ctx := context.Background()
				var sum int64
				b.ReportAllocs()
				for b.Loop() {
					for _, block := range blocks {
						if err := block.Scan(ctx, func(_ int, r Row) bool { sum ^= r.Start; sum ^= r.End; return true }); err != nil {
							b.Fatal(err)
						}
					}
				}
				benchSink = sum
				b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
				b.ReportMetric(float64(total)/float64(n), "block-B/row")
				b.ReportMetric(float64(meta)/float64(n), "meta-B/row")
				b.ReportMetric(float64(join)/float64(n), "join-B/row")
			})
		}
	}
}
