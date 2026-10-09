package bench

import (
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Effective timeline (handover effective-read-cost fix 1c, backlog 27): the
// fixture holds effectiveFixtureRels() relationships of type "FLOW" (200 K by
// default; BENCH_EFFECTIVE_RELS overrides), every 100th (1 %) corrected by a
// bounded SetRelVersionInterval [2000,3000), so it reads as three segments; the
// rest are plain (one segment, no history). Built once per backend and kept
// for the process: the gate runs -count=3.

const effectiveCascadeStride = 100

func effectiveFixtureRels() int {
	if n, err := strconv.Atoi(os.Getenv("BENCH_EFFECTIVE_RELS")); err == nil && n > 0 {
		return n
	}
	return 200_000
}

type effectiveFixture struct {
	g                *graph.Graph
	plain, cascaded  []types.RelID
	pin              types.Instant
	cascadedSegments int
}

var (
	effectiveFixtures   = map[string]*effectiveFixture{}
	effectiveFixturesMu sync.Mutex
)

func effectiveFixtureFor(b *testing.B, bc backendCase) *effectiveFixture {
	b.Helper()
	effectiveFixturesMu.Lock()
	defer effectiveFixturesMu.Unlock()
	if f, ok := effectiveFixtures[bc.name]; ok {
		return f
	}
	cfg := graph.Config{SnowflakeNodeID: bc.snowflakeNode + 12}
	if bc.badger {
		cfg.BadgerInMemory = true
	}
	g, err := graph.New(cfg)
	if err != nil {
		b.Fatalf("%s: graph.New: %v", bc.name, err)
	}
	ctx := benchCtx()
	nodes := addLabeledNodes(b, g, "Host", 2000)
	f := &effectiveFixture{g: g}
	n := effectiveFixtureRels()
	for i := 0; i < n; i++ {
		r, err := g.Rels().AddByID(ctx, "FLOW", nodes[i%len(nodes)], nodes[(i*7+1)%len(nodes)],
			map[string]any{"tkg_valid_from": types.Instant(1000), "seq": i})
		if err != nil {
			b.Fatalf("add rel %d: %v", i, err)
		}
		if i%effectiveCascadeStride == 0 {
			if _, err := g.Temporal().SetRelVersionInterval(ctx, r.ID(), 2000, 3000, map[string]any{"seq": -i}); err != nil {
				b.Fatalf("cascade rel %d: %v", i, err)
			}
			f.cascaded = append(f.cascaded, r.ID())
			continue
		}
		f.plain = append(f.plain, r.ID())
	}
	if f.pin, err = g.Temporal().NowTx(); err != nil {
		b.Fatalf("NowTx: %v", err)
	}
	// Warm the history-presence set (badger builds it on the first call).
	if _, err := g.Rels().HasHistory(f.plain[0]); err != nil {
		b.Fatalf("HasHistory: %v", err)
	}
	segs, err := g.Temporal().RelEffectiveTimeline(f.cascaded[0], f.pin)
	if err != nil || len(segs) != 3 {
		b.Fatalf("cascaded timeline: %d segments, %v", len(segs), err)
	}
	f.cascadedSegments = len(segs)
	effectiveFixtures[bc.name] = f
	return f
}

// BenchmarkRelEffectiveTimeline measures g.Temporal().RelEffectiveTimeline for
// a plain relationship (one current row, no history) and a cascaded one (three
// rows in history ‖ current, three segments); plain-hot cycles over 64 plain
// relationships, so their rows stay in the store's caches (the row read hot).
func BenchmarkRelEffectiveTimeline(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			f := effectiveFixtureFor(b, bc)
			for _, tc := range []struct {
				name string
				ids  []types.RelID
				segs int
			}{{"plain", f.plain, 1}, {"plain-hot", f.plain[:64], 1}, {"cascaded", f.cascaded, 3}} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					i := 0
					for b.Loop() {
						segs, err := f.g.Temporal().RelEffectiveTimeline(tc.ids[i%len(tc.ids)], f.pin)
						if err != nil || len(segs) != tc.segs {
							b.Fatalf("%d segments, %v; want %d", len(segs), err, tc.segs)
						}
						i++
					}
				})
			}
		})
	}
}

// BenchmarkRelEffectiveLoop is the consumer's loop the timeline replaces:
// Get + History, then RelAtTx at every row bound to rebuild the segments
// (sigma-tkgd's effective read before fix 1c). Same fixture, same answers.
func BenchmarkRelEffectiveLoop(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			f := effectiveFixtureFor(b, bc)
			ctx := benchCtx()
			for _, tc := range []struct {
				name string
				ids  []types.RelID
				segs int
			}{{"plain", f.plain, 1}, {"plain-hot", f.plain[:64], 1}, {"cascaded", f.cascaded, 3}} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					i := 0
					for b.Loop() {
						id := tc.ids[i%len(tc.ids)]
						cur, err := f.g.Rels().Get(ctx, id)
						if err != nil {
							b.Fatalf("Get: %v", err)
						}
						hist, err := f.g.Rels().History(id)
						if err != nil {
							b.Fatalf("History: %v", err)
						}
						var bounds []types.Instant
						for _, r := range append(hist, cur) {
							tm := r.Temporal()
							bounds = append(bounds, tm.ValidFrom)
							if tm.ValidTo != 0 {
								bounds = append(bounds, tm.ValidTo)
							}
						}
						slices.Sort(bounds)
						bounds = slices.Compact(bounds)
						segs, last := 0, uint32(1<<31)
						for _, t := range bounds {
							r, err := f.g.Temporal().RelAtTx(id, t, f.pin)
							if err != nil {
								continue
							}
							if r.Version() != last {
								segs++
								last = r.Version()
							}
						}
						if segs != tc.segs {
							b.Fatalf("%d segments, want %d", segs, tc.segs)
						}
						i++
					}
				})
			}
		})
	}
}

// BenchmarkForEachRelEffectiveByType measures one full scan of the fixture's
// type at the pin (every relationship, its whole timeline); ns/op is per
// scan, ns/rel the per-relationship cost.
func BenchmarkForEachRelEffectiveByType(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			f := effectiveFixtureFor(b, bc)
			want := len(f.plain) + len(f.cascaded)*f.cascadedSegments
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				if err := f.g.Temporal().ForEachRelEffectiveByType("FLOW", f.pin, func(temporal.RelSegment) bool {
					n++
					return true
				}); err != nil || n != want {
					b.Fatalf("scan: %d segments, %v; want %d", n, err, want)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(f.plain)+len(f.cascaded)), "ns/rel")
		})
	}
}
