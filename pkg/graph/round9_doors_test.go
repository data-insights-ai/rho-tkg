package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// round9Values are the values the random histories write: integers,
// floats, a float32, NaN, an infinity and a string (never in a range).
var round9Values = []any{int64(5), int64(10), 10.0, 12.5, float32(0.1), 0.1, int32(-3), "s", math.NaN(), int64(40), math.Inf(1), int64(77)}

// round9InRange is the doors' value test (RangeCardinalityAt's: a float32
// widened, NaN never).
func round9InRange(v any, min, max float64, inclMin, inclMax bool) bool {
	var f float64
	switch x := v.(type) {
	case int64:
		f = float64(x)
	case int32:
		f = float64(x)
	case float32:
		f = float64(x)
	case float64:
		f = x
	default:
		return false
	}
	if math.IsNaN(f) || f < min || f > max || (f == min && !inclMin) || (f == max && !inclMax) {
		return false
	}
	return true
}

// round9History writes a random history of label nodes (and type R
// relationships from a hub) on g: creates with explicit valid-from, value
// updates, closes, deletes, retractions, label removal and re-adding and a
// re-import of a deleted ID; it returns the nodes, the relationships and a
// pin after every write.
func round9History(t *testing.T, b storeBackend, g *graphpkg.Graph, label string, seed uint64) (nodes []types.NodeID, rels []types.RelID, pins []types.Instant) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 9))
	pick := func() any { return round9Values[rng.IntN(len(round9Values))] }
	mark := func() {
		p, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		pins = append(pins, p)
	}
	hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 24 {
		props := map[string]any{"v": pick()}
		if i%3 != 0 { // a third untimed
			props["tkg_valid_from"] = types.Instant(1000 + 100*i)
		}
		n, err := g.Nodes().Add(ctx, []string{label}, props)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n.ID())
		r, err := g.Rels().Add(ctx, "R", hub, n, map[string]any{"w": pick(), "tkg_valid_from": types.Instant(1000 + 100*i)})
		if err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r.ID())
		mark()
	}
	dead, closed := map[int]bool{}, map[int]bool{}
	clock := map[int]bool{} // versions stamped by the clock: later explicit starts would be below them
	timed := func(i int, at types.Instant, props map[string]any) map[string]any {
		if i%3 != 0 && !clock[i] {
			props["tkg_valid_from"] = at
		}
		return props
	}
	for step := range 70 {
		i := rng.IntN(len(nodes))
		if closed[i] {
			continue
		}
		if dead[i] {
			if step%9 == 0 && !b.tiered {
				if _, err := g.Nodes().Import(ctx, nodes[i], []string{label}, map[string]any{"v": pick(), "tkg_valid_from": types.Instant(20000 + step)}); err == nil {
					dead[i], clock[i] = false, true
				}
				mark()
			}
			continue
		}
		switch step % 7 {
		case 0, 1, 2:
			if _, err := g.Nodes().Update(ctx, nodes[i], timed(i, types.Instant(5000+10*step), map[string]any{"v": pick()})); err != nil {
				t.Fatal(err)
			}
		case 3:
			if _, err := g.Rels().Update(ctx, rels[i], map[string]any{"w": pick(), "tkg_valid_from": types.Instant(5000 + 10*step)}); err != nil {
				t.Fatal(err)
			}
		case 4:
			if i%3 == 0 || clock[i] {
				continue // an untimed version starts at its TxFrom
			}
			if err := g.Nodes().CloseVersion(ctx, nodes[i], types.Instant(15000+10*step)); err != nil {
				t.Fatal(err)
			}
			closed[i] = true
		case 5:
			if b.tiered {
				continue
			}
			clock[i] = true
			if err := g.Nodes().AddLabel(ctx, nodes[i], "Keep"); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().RemoveLabel(ctx, nodes[i], label); err != nil {
				t.Fatal(err)
			}
			mark()
			if err := g.Nodes().AddLabel(ctx, nodes[i], label); err != nil {
				t.Fatal(err)
			}
		case 6:
			var err error
			if step%2 == 0 {
				err = g.Nodes().Delete(ctx, nodes[i])
			} else {
				err = g.Nodes().Retract(ctx, nodes[i])
			}
			if err != nil {
				t.Fatal(err)
			}
			dead[i] = true
		}
		mark()
	}
	return nodes, rels, pins
}

// TestRangeAnyVersionMatchesEquality (sigma-tkgd round 9 R9-1): over random
// histories on every backend, at pins before, between and after the
// writes, the nodes (relationships) ForEachByLabelPropertyRangeAnyVersion
// (ForEachByTypePropertyRangeAnyVersion) visits are exactly the union over
// the written values in range of ByLabelAndProperty's (ByTypeAndProperty's)
// matches at {ValidStart 1, ValidEnd MaxInt64, TxAt pin}, ascending, once
// each; RangeCardinalityAnyVersion counts them; tiered and sharded decline.
func TestRangeAnyVersionMatchesEquality(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		label := "Ref"
		if err := g.Index().CreateProperty(label, "v"); err != nil {
			t.Fatal(err)
		}
		relIndexed := g.Index().CreateRelProperty("R", "w") == nil
		served := b.name == "memory" || b.name == "badger"
		_, _, pins := round9History(t, b, g, label, 1)
		ranges := [][2]float64{{0, 50}, {math.Inf(-1), 10}, {10, 10}, {0.1, 0.1}, {12.5, math.Inf(1)}, {70, 80}, {-9, -8}, {50, 0}}
		probe := []types.Instant{1, pins[0], pins[len(pins)/3], pins[len(pins)/2], pins[len(pins)-1]}
		for _, pin := range probe {
			opts := storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pin}
			for _, rg := range ranges {
				for _, incl := range [][2]bool{{true, true}, {false, true}, {true, false}} {
					name := fmt.Sprintf("pin %d range %v incl %v", pin, rg, incl)
					wantSet := map[types.NodeID]bool{}
					wantRelSet := map[types.RelID]bool{}
					for _, v := range round9Values {
						if !round9InRange(v, rg[0], rg[1], incl[0], incl[1]) {
							continue
						}
						ns, err := g.Nodes().ByLabelAndProperty(label, "v", v, opts)
						if err != nil {
							t.Fatal(err)
						}
						for _, n := range ns {
							wantSet[n.ID()] = true
						}
						if relIndexed {
							rs, err := g.Rels().ByTypeAndProperty("R", "w", v, opts)
							if err != nil {
								t.Fatal(err)
							}
							for _, r := range rs {
								wantRelSet[r.ID()] = true
							}
						}
					}
					var got []types.NodeID
					err := g.Nodes().ForEachByLabelPropertyRangeAnyVersion(label, "v", rg[0], rg[1], incl[0], incl[1], pin, func(n *types.Node) bool {
						if v, _ := n.GetProperty("v"); !round9InRange(v, rg[0], rg[1], incl[0], incl[1]) {
							t.Fatalf("%s: node %d handed out with %v", name, n.ID(), v)
						}
						got = append(got, n.ID())
						return true
					})
					count, exact, cerr := g.Nodes().RangeCardinalityAnyVersion(label, "v", rg[0], rg[1], incl[0], incl[1], pin)
					if cerr != nil {
						t.Fatal(cerr)
					}
					if !served {
						if !errors.Is(err, storepkg.ErrCapabilityNotSupported) || exact {
							t.Fatalf("%s %s: err %v exact %v, want a decline", name, b.name, err, exact)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%s: %v", name, err)
					}
					want := mapKeysSorted(wantSet)
					if !slices.Equal(got, want) {
						t.Fatalf("%s nodes:\n got  %v\n want %v", name, got, want)
					}
					if !exact || count.Entities != int64(len(want)) || count.Rows < count.Entities {
						t.Fatalf("%s: count %+v exact %v, want %d entities", name, count, exact, len(want))
					}
					if !relIndexed {
						continue
					}
					var gotR []types.RelID
					if err := g.Rels().ForEachByTypePropertyRangeAnyVersion("R", "w", rg[0], rg[1], incl[0], incl[1], pin, func(r *types.Relationship) bool {
						gotR = append(gotR, r.ID())
						return true
					}); err != nil {
						t.Fatalf("%s rels: %v", name, err)
					}
					wantR := mapKeysSorted(wantRelSet)
					if !slices.Equal(gotR, wantR) {
						t.Fatalf("%s rels:\n got  %v\n want %v", name, gotR, wantR)
					}
					rc, rexact, err := g.Rels().RangeCardinalityAnyVersion("R", "w", rg[0], rg[1], incl[0], incl[1], pin)
					if err != nil || !rexact || rc.Entities != int64(len(wantR)) || rc.Rows < rc.Entities {
						t.Fatalf("%s: rel count %+v exact %v err %v, want %d", name, rc, rexact, err, len(wantR))
					}
				}
			}
		}
	})
}

func mapKeysSorted[K ~int64](m map[K]bool) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// TestRangeAnyVersionEarlierVersion (R9-1, backlog 16's shape, red first):
// a value in range only on the earlier version, read at a pin after the
// update: the any-version door visits the node and counts its one row;
// ForEachByLabelPropertyRange at the same opts does not (unchanged). Rows
// counts each version in range; early stop; ascending order; errors.
func TestRangeAnyVersionEarlierVersion(t *testing.T) {
	for _, b := range allStoreBackends()[:2] {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreateProperty("P", "v"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(5), "tkg_valid_from": types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(50), "tkg_valid_from": types.Instant(200)}); err != nil {
				t.Fatal(err)
			}
			c, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(6), "tkg_valid_from": types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, c.ID(), map[string]any{"v": int64(7), "tkg_valid_from": types.Instant(300)}); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			opts := storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pin}
			var resolved []types.NodeID
			if err := g.Nodes().ForEachByLabelPropertyRange("P", "v", 0, 10, true, true, opts, func(n *types.Node) bool {
				resolved = append(resolved, n.ID())
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(resolved, a.ID()) {
				t.Fatalf("the resolved-version door now visits %d: backlog 16 changed", a.ID())
			}
			var got []types.NodeID
			if err := g.Nodes().ForEachByLabelPropertyRangeAnyVersion("P", "v", 0, 10, true, true, pin, func(n *types.Node) bool {
				got = append(got, n.ID())
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if want := []types.NodeID{a.ID(), c.ID()}; !slices.Equal(got, want) {
				t.Fatalf("any version: %v, want %v", got, want)
			}
			count, exact, err := g.Nodes().RangeCardinalityAnyVersion("P", "v", 0, 10, true, true, pin)
			if err != nil || !exact || count != (storepkg.RangeAnyVersionCount{Entities: 2, Rows: 3}) {
				t.Fatalf("count %+v exact %v err %v, want 2 entities 3 rows", count, exact, err)
			}
			got = got[:0]
			_ = g.Nodes().ForEachByLabelPropertyRangeAnyVersion("P", "v", 0, 10, true, true, pin, func(n *types.Node) bool {
				got = append(got, n.ID())
				return false
			})
			if len(got) != 1 {
				t.Fatalf("early stop: %d visits", len(got))
			}
			// A version without the label holds a value in range: no row.
			k, err := g.Nodes().Add(ctx, []string{"P", "K"}, map[string]any{"v": int64(8), "tkg_valid_from": types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().RemoveLabel(ctx, k.ID(), "P"); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, k.ID(), map[string]any{"v": int64(9)}); err != nil {
				t.Fatal(err)
			}
			kpin, _ := g.Temporal().NowTx()
			if count, exact, err := g.Nodes().RangeCardinalityAnyVersion("P", "v", 8, 9, true, true, kpin); err != nil || !exact || count != (storepkg.RangeAnyVersionCount{Entities: 1, Rows: 1}) {
				t.Fatalf("label dropped on a later version: %+v %v %v, want 1 entity 1 row", count, exact, err)
			}
			// Before the update: the earlier version only.
			first, _, _ := a.TxStamps()
			if count, exact, _ := g.Nodes().RangeCardinalityAnyVersion("P", "v", 40, 60, true, true, first); !exact || count.Entities != 0 {
				t.Fatalf("at the first pin the later value is unknown: %+v", count)
			}
			if count, exact, _ := g.Nodes().RangeCardinalityAnyVersion("P", "v", 0, 10, true, true, 0); !exact || count.Entities != 0 {
				t.Fatalf("pin 0: %+v %v", count, exact)
			}
			if count, exact, _ := g.Nodes().RangeCardinalityAnyVersion("P", "v", math.NaN(), 10, true, true, pin); !exact || count.Entities != 0 {
				t.Fatalf("NaN bound: %+v %v", count, exact)
			}
			if err := g.Nodes().ForEachByLabelPropertyRangeAnyVersion("P", "nokey", 0, 10, true, true, pin, func(*types.Node) bool { return true }); !errors.Is(err, storepkg.ErrIndexNotFound) {
				t.Fatalf("no index: %v", err)
			}
			if _, exact, err := g.Nodes().RangeCardinalityAnyVersion("P", "nokey", 0, 10, true, true, pin); err != nil || exact {
				t.Fatalf("no index count: exact %v err %v", exact, err)
			}
			if err := g.Nodes().ForEachByLabelPropertyRangeAnyVersion("P", "v", 0, 10, true, true, pin, nil); err == nil {
				t.Fatal("nil callback: no error")
			}
			if err := g.Nodes().ForEachByLabelPropertyRangeAnyVersion("", "v", 0, 10, true, true, pin, func(*types.Node) bool { return true }); err == nil {
				t.Fatal("empty label: no error")
			}
			if count, exact, err := g.Nodes().RangeCardinalityAnyVersion("Unknown", "v", 0, 10, true, true, pin); err != nil || !exact || count.Entities != 0 {
				t.Fatalf("unknown label: %+v %v %v", count, exact, err)
			}
		})
	}
}

// round9Untimed is sigma-tkgd's untimed rule over an effective stream (the
// oracle of UntimedFromMinusInf): an entity's first segment starts at -inf
// where it starts at an untimed create.
func round9Untimed(segFrom, rowFrom, txFrom types.Instant, version uint32, genesis func() (types.Instant, types.Instant, bool)) bool {
	if version == 0 {
		return rowFrom == 0 && segFrom == txFrom
	}
	if rowFrom != segFrom {
		return false
	}
	gvf, gtx, ok := genesis()
	return ok && gvf == 0 && gtx == segFrom
}

// TestCountTimelineMatchesCountByLabelAt (R9-2): over random histories on
// every backend, at pins before, between and after the writes and at the
// current state (pin 0), the timeline's count at every segment boundary,
// one before it and between boundaries equals CountByLabelAt
// (CountByTypeAt) at {ValidAt, TxAt: pin}; segments are ascending, maximal,
// non-overlapping, positive; with UntimedFromMinusInf the count equals the
// members whose effective segments, an untimed start from -inf, hold;
// MaxSegments declines.
func TestCountTimelineMatchesCountByLabelAt(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		label := "Ref"
		_, _, pins := round9History(t, b, g, label, 2)
		probePins := []types.Instant{pins[0], pins[len(pins)/3], pins[len(pins)-1], 0}
		for _, pin := range probePins {
			for _, untimed := range []bool{false, true} {
				opts := storepkg.CountTimelineOpts{UntimedFromMinusInf: untimed}
				segs, exact, err := g.Nodes().CountTimelineByLabel(label, pin, opts)
				if err != nil || !exact {
					t.Fatalf("pin %d: exact %v err %v", pin, exact, err)
				}
				rsegs, rexact, err := g.Rels().CountTimelineByType("R", pin, opts)
				if err != nil || !rexact {
					t.Fatalf("pin %d rels: exact %v err %v", pin, rexact, err)
				}
				checkSegments(t, segs)
				checkSegments(t, rsegs)
				at := pin
				if at == 0 {
					if at, err = g.Temporal().NowTx(); err != nil {
						t.Fatal(err)
					}
				}
				// The oracle's spans.
				var spans [][2]types.Instant
				if untimed {
					var last types.NodeID
					if err := g.Temporal().ForEachNodeEffectiveByLabel(label, at, func(s temporal.NodeSegment) bool {
						from := s.ValidFrom
						if s.Node.ID() != last {
							last = s.Node.ID()
							vf, _, _ := s.Node.ValidRange()
							tx, _, _ := s.Node.TxStamps()
							if round9Untimed(s.ValidFrom, vf, tx, s.Node.Version(), func() (types.Instant, types.Instant, bool) {
								h, err := g.Nodes().History(s.Node.ID())
								if err != nil {
									return 0, 0, false
								}
								for _, v := range h {
									if v.Version() == 0 && v.Temporal() != nil {
										return v.Temporal().ValidFrom, v.Temporal().TxFrom, true
									}
								}
								return 0, 0, false
							}) {
								from = math.MinInt64
							}
						}
						spans = append(spans, [2]types.Instant{from, s.ValidTo})
						return true
					}); err != nil {
						t.Fatal(err)
					}
				}
				instants := map[types.Instant]bool{1: true, 999: true, 1500: true, 6000: true, 20000: true, at: true, at + 1000: true}
				for _, s := range segs {
					for _, x := range []types.Instant{s.From, s.From - 1, s.From + 1, s.To, s.To - 1} {
						if x > 0 {
							instants[x] = true
						}
					}
				}
				for _, s := range rsegs {
					for _, x := range []types.Instant{s.From, s.From - 1, s.To, s.To - 1} {
						if x > 0 {
							instants[x] = true
						}
					}
				}
				for v := range instants {
					got := timelineAt(segs, v)
					var want int64
					if untimed {
						for _, sp := range spans {
							if sp[0] <= v && (sp[1] == 0 || v < sp[1]) {
								want++
							}
						}
					} else {
						n, err := g.Nodes().CountByLabelAt(label, storepkg.QueryOpts{ValidAt: v, TxAt: at})
						if err != nil {
							t.Fatal(err)
						}
						want = int64(n)
					}
					if got != want {
						t.Fatalf("pin %d untimed %v at %d: timeline %d, want %d (segments %v)", pin, untimed, v, got, want, segs)
					}
					if !untimed {
						rn, err := g.Rels().CountByTypeAt("R", storepkg.QueryOpts{ValidAt: v, TxAt: at})
						if err != nil {
							t.Fatal(err)
						}
						if rgot := timelineAt(rsegs, v); rgot != int64(rn) {
							t.Fatalf("pin %d rels at %d: timeline %d, want %d", pin, v, rgot, rn)
						}
					}
				}
				if len(segs) > 1 {
					if _, exact, err := g.Nodes().CountTimelineByLabel(label, pin, storepkg.CountTimelineOpts{UntimedFromMinusInf: untimed, MaxSegments: len(segs) - 1}); err != nil || exact {
						t.Fatalf("MaxSegments %d of %d: exact %v err %v", len(segs)-1, len(segs), exact, err)
					}
				}
			}
		}
		if segs, exact, err := g.Nodes().CountTimelineByLabel("Unknown", 0, storepkg.CountTimelineOpts{}); err != nil || !exact || len(segs) != 0 {
			t.Fatalf("unknown label: %v %v %v", segs, exact, err)
		}
		if segs, exact, err := g.Nodes().CountTimelineByLabel(label, -5, storepkg.CountTimelineOpts{}); err != nil || !exact || len(segs) != 0 {
			t.Fatalf("negative pin: %v %v %v", segs, exact, err)
		}
		if _, _, err := g.Nodes().CountTimelineByLabel("", 0, storepkg.CountTimelineOpts{}); err == nil {
			t.Fatal("empty label: no error")
		}
	})
}

func timelineAt(segs []storepkg.CountSegment, v types.Instant) int64 {
	for _, s := range segs {
		if s.From <= v && (s.To == 0 || v < s.To) {
			return s.Count
		}
	}
	return 0
}

func checkSegments(t *testing.T, segs []storepkg.CountSegment) {
	t.Helper()
	for i, s := range segs {
		if s.Count <= 0 || (s.To != 0 && s.To <= s.From) {
			t.Fatalf("segment %d %+v", i, s)
		}
		if i > 0 {
			p := segs[i-1]
			if p.To == 0 || p.To > s.From {
				t.Fatalf("segments %d, %d overlap: %+v %+v", i-1, i, p, s)
			}
			if p.To == s.From && p.Count == s.Count {
				t.Fatalf("segments %d, %d not merged: %+v %+v", i-1, i, p, s)
			}
		}
	}
}

// TestCountTimelineFollowsWrites (R9-2, two-phase): a cached timeline at a
// pin stays the pin's after later writes, the current timeline (pin 0)
// follows them, and a past-dated write at or below the pin moves it.
func TestCountTimelineFollowsWrites(t *testing.T) {
	for _, b := range allStoreBackendsWith(txFromCfg)[:2] { // an UntimedFromTxFrom store (UntimedFromMinusInf below)
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			n1, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(300)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": types.Instant(200)}); err != nil {
				t.Fatal(err)
			}
			pin, _ := g.Temporal().NowTx()
			want := []storepkg.CountSegment{{From: 100, To: 200, Count: 1}, {From: 200, To: 300, Count: 2}, {From: 300, To: 0, Count: 1}}
			for range 3 { // first build, cached reads
				segs, exact, err := g.Nodes().CountTimelineByLabel("T", pin, storepkg.CountTimelineOpts{})
				if err != nil || !exact || !slices.Equal(segs, want) {
					t.Fatalf("at the pin: %v %v %v, want %v", segs, exact, err, want)
				}
			}
			// A delete keeps the belief that n1 held over [100, 300) (it ends
			// validity at the delete); a retraction withdraws it.
			// An untimed create closed later: the close stamps its start on
			// the closing row; from -inf under UntimedFromMinusInf.
			u, err := g.Nodes().Add(ctx, []string{"U"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			utx, _, _ := u.TxStamps()
			if err := g.Nodes().CloseVersion(ctx, u.ID(), utx+5000); err != nil {
				t.Fatal(err)
			}
			upin, _ := g.Temporal().NowTx()
			usegs, _, _ := g.Nodes().CountTimelineByLabel("U", upin, storepkg.CountTimelineOpts{UntimedFromMinusInf: true})
			if want := []storepkg.CountSegment{{From: math.MinInt64, To: utx + 5000, Count: 1}}; !slices.Equal(usegs, want) {
				t.Fatalf("closed untimed create: %v, want %v", usegs, want)
			}
			plain, _, _ := g.Nodes().CountTimelineByLabel("U", upin, storepkg.CountTimelineOpts{})
			if want := []storepkg.CountSegment{{From: utx, To: utx + 5000, Count: 1}}; !slices.Equal(plain, want) {
				t.Fatalf("closed untimed create without the option: %v, want %v", plain, want)
			}
			if err := g.Nodes().Retract(ctx, n1.ID()); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": types.Instant(50)}); err != nil {
				t.Fatal(err)
			}
			segs, _, _ := g.Nodes().CountTimelineByLabel("T", pin, storepkg.CountTimelineOpts{})
			if !slices.Equal(segs, want) {
				t.Fatalf("after later writes the pin's timeline changed: %v", segs)
			}
			now, _, _ := g.Nodes().CountTimelineByLabel("T", 0, storepkg.CountTimelineOpts{})
			if wantNow := []storepkg.CountSegment{{From: 50, To: 200, Count: 1}, {From: 200, To: 0, Count: 2}}; !slices.Equal(now, wantNow) {
				t.Fatalf("current: %v, want %v", now, wantNow)
			}
		})
	}
}

// TestPropertyValueStatsMatchesIndex (R9-3): over random histories on every
// backend, the current statistics equal a fold over the label's current
// members by index key (Distinct, Rows, MaxRows, MaxValue's key, the
// smallest key among equals), through value changes and deletes that move
// and shrink the heaviest value; at pins Distinct equals the values
// ByLabelAndProperty matches at {ValidStart 1, ValidEnd MaxInt64, TxAt pin}
// and Rows / MaxRows bound their entity counts from above (rows are
// versions); tiered and sharded decline.
func TestPropertyValueStatsMatchesIndex(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		label := "Ref"
		if err := g.Index().CreateProperty(label, "v"); err != nil {
			t.Fatal(err)
		}
		relIndexed := g.Index().CreateRelProperty("R", "w") == nil
		served := b.name == "memory" || b.name == "badger"
		check := func(stage string) {
			t.Helper()
			nodes, err := g.Nodes().ByLabel(label, storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			rows := map[string]int64{}
			for _, n := range nodes {
				if v, ok := n.GetProperty("v"); ok {
					if k := types.IndexablePropertyValueKey(v); k != "" {
						rows[k]++
					}
				}
			}
			want := storepkg.PropertyValueStats{Distinct: int64(len(rows))}
			var maxKey string
			for k, n := range rows {
				want.Rows += n
				if n > want.MaxRows || (n == want.MaxRows && k < maxKey) {
					want.MaxRows, maxKey = n, k
				}
			}
			got, exact, err := g.Stats().PropertyValueStats(label, "v", 0)
			if err != nil {
				t.Fatal(err)
			}
			if !served {
				if exact {
					t.Fatalf("%s %s: exact on a store without the statistics", stage, b.name)
				}
				return
			}
			gotKey := types.IndexablePropertyValueKey(got.MaxValue)
			if !exact || got.Distinct != want.Distinct || got.Rows != want.Rows || got.MaxRows != want.MaxRows || (want.MaxRows > 0 && gotKey != maxKey) {
				t.Fatalf("%s: %+v (key %s) exact %v, want %+v key %s", stage, got, gotKey, exact, want, maxKey)
			}
			if again, _, _ := g.Stats().PropertyValueStats(label, "v", 0); fmt.Sprint(again) != fmt.Sprint(got) {
				t.Fatalf("%s: cached read %+v, first %+v", stage, again, got)
			}
			if c, err := g.Stats().NodeCountByLabelAndPropertyKey(label, "v"); err == nil && int64(c) != got.Rows && !hasNaN(nodes) {
				t.Logf("%s: NodeCountByLabelAndPropertyKey %d, Rows %d", stage, c, got.Rows)
			}
			if relIndexed {
				if _, rexact, err := g.Stats().RelPropertyValueStats("R", "w", 0); err != nil || !rexact {
					t.Fatalf("%s rels: exact %v err %v", stage, rexact, err)
				}
			}
		}
		check("empty")
		_, _, pins := round9History(t, b, g, label, 3)
		check("history")
		// A hub value on half the members, then shrunk and moved.
		members, _ := g.Nodes().ByLabel(label, storepkg.QueryOpts{})
		ctx := context.Background()
		for i, n := range members {
			if i%2 == 0 {
				if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": "hub"}); err != nil && !errors.Is(err, graphpkg.ErrAlreadyClosed) {
					t.Fatal(err)
				}
			}
		}
		check("hub")
		if served {
			got, _, _ := g.Stats().PropertyValueStats(label, "v", 0)
			if got.MaxValue != "hub" || got.MaxRows < int64(len(members)/3) {
				t.Fatalf("hub: %+v, want hub heaviest of %d members", got, len(members))
			}
		}
		for i, n := range members {
			if i%2 == 0 && i%4 != 0 {
				if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
					t.Fatal(err)
				}
			}
		}
		check("hub shrunk by deletes")
		for i, n := range members {
			if i%4 == 0 && i > 0 {
				if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": int64(99)}); err != nil && !errors.Is(err, graphpkg.ErrAlreadyClosed) {
					t.Fatal(err)
				}
			}
		}
		check("hub moved")

		now, _ := g.Temporal().NowTx()
		for _, pin := range []types.Instant{pins[0], pins[len(pins)/2], pins[len(pins)-1], now} {
			opts := storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pin}
			var distinct, sum, max int64
			for _, v := range append(slices.Clone(round9Values), "hub", int64(99)) {
				ns, err := g.Nodes().ByLabelAndProperty(label, "v", v, opts)
				if err != nil {
					t.Fatal(err)
				}
				if len(ns) > 0 {
					distinct++
					sum += int64(len(ns))
					max = maxInt64(max, int64(len(ns)))
				}
			}
			got, exact, err := g.Stats().PropertyValueStats(label, "v", pin)
			if err != nil {
				t.Fatal(err)
			}
			if !served {
				if exact {
					t.Fatalf("pin %d: exact on %s", pin, b.name)
				}
				continue
			}
			if !exact || got.Distinct != distinct || got.Rows < sum || got.MaxRows < max || got.MaxRows > got.Rows {
				t.Fatalf("pin %d: %+v exact %v, want distinct %d, rows >= %d, max >= %d", pin, got, exact, distinct, sum, max)
			}
		}
		if _, exact, err := g.Stats().PropertyValueStats(label, "nokey", 0); err != nil || exact {
			t.Fatalf("no index: exact %v err %v", exact, err)
		}
		if _, exact, err := g.Stats().PropertyValueStats(label, "nokey", pins[0]); err != nil || exact {
			t.Fatalf("no index at a pin: exact %v err %v", exact, err)
		}
		if _, _, err := g.Stats().PropertyValueStats("", "v", 0); err == nil {
			t.Fatal("empty label: no error")
		}
	})
}

func hasNaN(nodes []*types.Node) bool {
	for _, n := range nodes {
		if v, ok := n.GetProperty("v"); ok {
			if f, isF := v.(float64); isF && math.IsNaN(f) {
				return true
			}
		}
	}
	return false
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// TestPropertyValueStatsPerVersion (R9-3, two-phase): at a pin the rows are
// versions — a member holding the same value on two versions counts twice
// in Rows and once in Distinct — and a later write changes neither the
// pin's statistics nor misses the current ones; an Elements index declines.
func TestPropertyValueStatsPerVersion(t *testing.T) {
	for _, b := range allStoreBackends()[:2] {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreateProperty("P", "v"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": "x", "tkg_valid_from": types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": "x", "n": int64(1), "tkg_valid_from": types.Instant(200)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": "y", "tkg_valid_from": types.Instant(100)}); err != nil {
				t.Fatal(err)
			}
			pin, _ := g.Temporal().NowTx()
			want := storepkg.PropertyValueStats{Distinct: 2, Rows: 3, MaxRows: 2, MaxValue: "x"}
			for range 2 {
				got, exact, err := g.Stats().PropertyValueStats("P", "v", pin)
				if err != nil || !exact || got != want {
					t.Fatalf("at the pin: %+v %v %v, want %+v", got, exact, err, want)
				}
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": "z", "tkg_valid_from": types.Instant(300)}); err != nil {
				t.Fatal(err)
			}
			if got, _, _ := g.Stats().PropertyValueStats("P", "v", pin); got != want {
				t.Fatalf("the pin's statistics changed after a later write: %+v", got)
			}
			cur, exact, err := g.Stats().PropertyValueStats("P", "v", 0)
			if wantCur := (storepkg.PropertyValueStats{Distinct: 2, Rows: 2, MaxRows: 1, MaxValue: "y"}); err != nil || !exact || cur != wantCur {
				t.Fatalf("current: %+v %v %v, want %+v", cur, exact, err, wantCur)
			}
			if err := g.Index().CreatePropertyWithOptions("E", "tags", storepkg.PropertyIndexOptions{Elements: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"E"}, map[string]any{"tags": []string{"a", "b"}}); err != nil {
				t.Fatal(err)
			}
			if _, exact, err := g.Stats().PropertyValueStats("E", "tags", 0); err != nil || exact {
				t.Fatalf("Elements index: exact %v err %v", exact, err)
			}
			if _, exact, err := g.Stats().PropertyValueStats("E", "tags", pin); err != nil || exact {
				t.Fatalf("Elements index at a pin: exact %v err %v", exact, err)
			}
		})
	}
}

// TestRound9DoorsAfterReopen: on a badger store reopened from disk, the
// value statistics (current and at a pin), the any-version range and the
// count timeline answer as before the close (the index, its sidecar and the
// histories are rebuilt or restored at open).
func TestRound9DoorsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	g, err := graphpkg.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateProperty("P", "v"); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(i % 7), "tkg_valid_from": types.Instant(100 + i)})
		if err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": int64(50 + i), "tkg_valid_from": types.Instant(1000 + i)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	pin, _ := g.Temporal().NowTx()
	type answers struct {
		cur, at storepkg.PropertyValueStats
		rng     storepkg.RangeAnyVersionCount
		segs    []storepkg.CountSegment
	}
	read := func(g *graphpkg.Graph) answers {
		var a answers
		var ok1, ok2, ok3, ok4 bool
		var err error
		a.cur, ok1, err = g.Stats().PropertyValueStats("P", "v", 0)
		if err != nil {
			t.Fatal(err)
		}
		a.at, ok2, _ = g.Stats().PropertyValueStats("P", "v", pin)
		a.rng, ok3, _ = g.Nodes().RangeCardinalityAnyVersion("P", "v", 0, 3, true, true, pin)
		a.segs, ok4, _ = g.Nodes().CountTimelineByLabel("P", pin, storepkg.CountTimelineOpts{})
		if !ok1 || !ok2 || !ok3 || !ok4 {
			t.Fatalf("declined: %v %v %v %v", ok1, ok2, ok3, ok4)
		}
		return a
	}
	before := read(g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = graphpkg.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	after := read(g)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("after reopen:\n %+v\nbefore:\n %+v", after, before)
	}
}

// TestRound9RelDoorsHonourRetention (review): the relationship any-version
// doors and RelPropertyValueStats at a pin below a type's purge watermark
// fail closed like the equality door, instead of answering from the purged
// sidecar (they checked the node retention scope).
func TestRound9RelDoorsHonourRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{BadgerInMemory: true, AllowRetentionPurge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.Index().CreateRelProperty("EVENT", "w"); err != nil {
		t.Fatal(err)
	}
	a, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
	if _, err := g.Rels().Add(ctx, "EVENT", a, b, map[string]any{"w": int64(5)}); err != nil {
		t.Fatal(err)
	}
	pin, _ := g.Temporal().NowTx()
	if _, err := g.Temporal().AdvanceClock(pin + 10); err != nil {
		t.Fatal(err)
	}
	before, _ := g.Temporal().NowTx()
	if _, err := g.Temporal().AdvanceClock(before + 10); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Add(ctx, "EVENT", a, b, map[string]any{"w": int64(6)}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: before}); err != nil {
		t.Fatal(err)
	}
	opts := storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pin}
	if _, err := g.Rels().ByTypeAndProperty("EVENT", "w", int64(5), opts); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("setup: equality door below the watermark: err = %v, want ErrRetentionExpired", err)
	}
	if _, _, err := g.Rels().RangeCardinalityAnyVersion("EVENT", "w", 0, 10, true, true, pin); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Errorf("RangeCardinalityAnyVersion below the watermark: err = %v, want ErrRetentionExpired", err)
	}
	if err := g.Rels().ForEachByTypePropertyRangeAnyVersion("EVENT", "w", 0, 10, true, true, pin, func(*types.Relationship) bool { return true }); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Errorf("ForEachByTypePropertyRangeAnyVersion below the watermark: err = %v, want ErrRetentionExpired", err)
	}
	if _, _, err := g.Stats().RelPropertyValueStats("EVENT", "w", pin); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Errorf("RelPropertyValueStats below the watermark: err = %v, want ErrRetentionExpired", err)
	}
}

// TestPropertyValueStatsDeclineAfterIndexDrop (review): a cached value
// statistic at a pin no longer answers once its index is dropped.
func TestPropertyValueStatsDeclineAfterIndexDrop(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends()[:2] {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreateProperty("L", "k"); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"L"}, map[string]any{"k": int64(1)}); err != nil {
				t.Fatal(err)
			}
			pin, _ := g.Temporal().NowTx()
			if _, ok, err := g.Stats().PropertyValueStats("L", "k", pin); err != nil || !ok {
				t.Fatalf("setup: PropertyValueStats at pin: ok=%v err=%v", ok, err)
			}
			if err := g.Index().DeleteProperty("L", "k"); err != nil {
				t.Fatal(err)
			}
			if s, ok, err := g.Stats().PropertyValueStats("L", "k", pin); err != nil || ok {
				t.Fatalf("PropertyValueStats after the index was dropped: %+v ok=%v err=%v, want a decline", s, ok, err)
			}
		})
	}
}
