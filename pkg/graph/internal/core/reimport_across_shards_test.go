package core

import (
	"context"
	"slices"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 49: an ID deleted and re-imported so that its new row lands on
// another tiered shard (a relationship whose start node moved between the
// reference and an event shard; a node whose primary label moved between the
// reference and the event class) keeps every earlier life reachable through
// every door: History, the as-of read at a pin inside each life, the
// effective timeline, and the scan doors ForEach*Effective, *EffectiveByIDs,
// ForEachRelEffectiveAtNodes. Each life carries its index as property "life";
// at the pin closing life k every door answers exactly lives 1..k.

// rasBackend opens a graph; reopen (nil: not persistent) closes it and opens
// the same data again.
type rasBackend struct {
	name string
	open func(t *testing.T) (g *Core, reopen func() *Core)
}

func rasBackends() []rasBackend {
	out := []rasBackend{}
	for _, b := range txbBackends() {
		out = append(out, rasBackend{b.name, func(t *testing.T) (*Core, func() *Core) { return b.open(t, false), nil }})
	}
	// tiered on disk, reopened: the earlier lives must stay reachable after a restart.
	out = append(out, rasBackend{"tiered-reopen", func(t *testing.T) (*Core, func() *Core) {
		dir := t.TempDir()
		open := func() *Core {
			ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			g, err := New(Config{Store: ts})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
			return g
		}
		g := open()
		return g, func() *Core {
			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if st, ok := g.store.(interface{ Close() error }); ok {
				_ = st.Close()
			}
			g = open()
			return g
		}
	}})
	return out
}

func rasLife(t *testing.T, props interface{ GetProperty(string) (any, bool) }) int64 {
	t.Helper()
	v, ok := props.GetProperty("life")
	if !ok {
		t.Fatalf("row without property life")
	}
	return v.(int64)
}

// rasLives returns the distinct lives of rows, sorted.
func rasLives[T interface{ GetProperty(string) (any, bool) }](t *testing.T, rows []T) []int64 {
	t.Helper()
	var out []int64
	for _, r := range rows {
		if l := rasLife(t, r); !slices.Contains(out, l) {
			out = append(out, l)
		}
	}
	slices.Sort(out)
	return out
}

func rasUpTo(k int) []int64 {
	out := make([]int64, k)
	for i := range out {
		out[i] = int64(i + 1)
	}
	return out
}

func TestReimportAcrossShardsKeepsEarlierLife(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	scenarios := []struct {
		name  string
		rel   bool
		lives []string // per life: the relationship's start-node label, or the node's label
	}{
		{"rel event->ref->event", true, []string{"Ev", "Ref", "Ev"}},
		{"rel ref->event", true, []string{"Ref", "Ev"}},
		{"rel same shard", true, []string{"Ev", "Ev"}},
		{"node event->ref->event", false, []string{"Ev", "Ref", "Ev"}},
		{"node ref->event", false, []string{"Ref", "Ev"}},
		{"node same shard", false, []string{"Ev", "Ev"}},
	}
	for _, be := range rasBackends() {
		for _, sc := range scenarios {
			t.Run(be.name+"/"+sc.name, func(t *testing.T) {
				g, reopen := be.open(t)
				clk := useTestClock(t, g)
				ends := map[string]types.NodeID{}
				for _, l := range []string{"Ref", "Ev", "End"} {
					label := l
					if l == "End" {
						label = "Ev"
					}
					n, err := g.Nodes.Add(ctx, []string{label}, nil)
					if err != nil {
						t.Fatal(err)
					}
					ends[l] = n.ID()
				}
				var id int64
				var pins []types.Instant
				for i, label := range sc.lives {
					props := map[string]any{"life": int64(i + 1), "tkg_valid_from": types.Instant(1000 * (i + 1))}
					// The pin of a life is its row's transaction start: inside it.
					var pin types.Instant
					switch {
					case sc.rel && i == 0:
						r, err := g.Rels.AddByID(ctx, "R", ends[label], ends["End"], props)
						if err != nil {
							t.Fatal(err)
						}
						id, pin = int64(r.ID()), r.Temporal().TxFrom
					case sc.rel:
						s, _ := g.Nodes.Get(ctx, ends[label])
						e, _ := g.Nodes.Get(ctx, ends["End"])
						r, err := g.Rels.Import(ctx, types.RelID(id), "R", s, e, props)
						if err != nil {
							t.Fatalf("life %d: Import: %v", i+1, err)
						}
						pin = r.Temporal().TxFrom
					case i == 0:
						n, err := g.Nodes.Add(ctx, []string{label}, props)
						if err != nil {
							t.Fatal(err)
						}
						id, pin = int64(n.ID()), n.Temporal().TxFrom
					default:
						n, err := g.Nodes.Import(ctx, types.NodeID(id), []string{label}, props)
						if err != nil {
							t.Fatalf("life %d: Import: %v", i+1, err)
						}
						pin = n.Temporal().TxFrom
					}
					pins = append(pins, pin)
					if i == len(sc.lives)-1 {
						break
					}
					var err error
					if sc.rel {
						err = g.Rels.Delete(ctx, types.RelID(id))
					} else {
						err = g.Nodes.Delete(ctx, types.NodeID(id))
					}
					if err != nil {
						t.Fatalf("life %d: Delete: %v", i+1, err)
					}
				}
				check := func(g *Core, when string) {
					t.Helper()
					if sc.rel {
						rasCheckRel(t, g, when, types.RelID(id), sc.lives, ends, pins)
					} else {
						rasCheckNode(t, g, when, types.NodeID(id), sc.lives, pins)
					}
				}
				check(g, "")
				if reopen != nil {
					g = reopen()
					g.SetClockForTest(t, clk.Now)
					check(g, "after reopen: ")
				}
				rasCheckStoreDoors(t, g, sc.rel, id, len(sc.lives))
			})
		}
	}
}

// rasCheckStoreDoors checks the store's per-version history doors over the
// lives: every earlier version reads back with its life, the version page
// holds every earlier life, and a truncation to one version keeps the newest
// history row across the lives (that of life n-1). Destructive: runs last.
func rasCheckStoreDoors(t *testing.T, g *Core, rel bool, id int64, n int) {
	t.Helper()
	page, _ := g.store.(storepkg.HistoryVersionPageCapability)
	if rel {
		rid := types.RelID(id)
		h, err := g.Rels.History(rid)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range h {
			v, err := g.store.GetRelVersion(rid, r.Version())
			if err != nil || rasLife(t, v) != rasLife(t, r) {
				t.Errorf("GetRelVersion(%d): %v, want the row of life %d", r.Version(), err, rasLife(t, r))
			}
		}
		if page != nil {
			rows, err := page.RelHistoryVersionsFrom(rid, 0, 0)
			if got := rasLives(t, rows); err != nil || !slices.Equal(got, rasUpTo(n-1)) {
				t.Errorf("RelHistoryVersionsFrom: lives %v (%v), want %v", got, err, rasUpTo(n-1))
			}
		}
		if err := g.store.TruncateRelHistory(rid, 1); err != nil {
			t.Fatal(err)
		}
		h, err = g.Rels.History(rid)
		if got := rasLives(t, h); err != nil || len(h) != 1 || !slices.Equal(got, []int64{int64(n - 1)}) {
			t.Errorf("after TruncateRelHistory(1): %d rows of lives %v (%v), want 1 row of life %d", len(h), got, err, n-1)
		}
		return
	}
	nid := types.NodeID(id)
	h, err := g.Nodes.History(nid)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range h {
		v, err := g.store.GetNodeVersion(nid, r.Version())
		if err != nil || rasLife(t, v) != rasLife(t, r) {
			t.Errorf("GetNodeVersion(%d): %v, want the row of life %d", r.Version(), err, rasLife(t, r))
		}
	}
	if page != nil {
		rows, err := page.NodeHistoryVersionsFrom(nid, 0, 0)
		if got := rasLives(t, rows); err != nil || !slices.Equal(got, rasUpTo(n-1)) {
			t.Errorf("NodeHistoryVersionsFrom: lives %v (%v), want %v", got, err, rasUpTo(n-1))
		}
	}
	if err := g.store.TruncateNodeHistory(nid, 1); err != nil {
		t.Fatal(err)
	}
	h, err = g.Nodes.History(nid)
	if got := rasLives(t, h); err != nil || len(h) != 1 || !slices.Equal(got, []int64{int64(n - 1)}) {
		t.Errorf("after TruncateNodeHistory(1): %d rows of lives %v (%v), want 1 row of life %d", len(h), got, err, n-1)
	}
}

func rasCheckRel(t *testing.T, g *Core, when string, id types.RelID, lives []string, ends map[string]types.NodeID, pins []types.Instant) {
	t.Helper()
	h, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("%sHistory: %v", when, err)
	}
	if got, want := rasLives(t, h), rasUpTo(len(lives)-1); !slices.Equal(got, want) {
		t.Errorf("%sHistory: lives %v, want %v", when, got, want)
	}
	for k, pin := range pins {
		want := rasUpTo(k + 1)
		r, err := g.Temporal.RelAsOf(id, pin)
		if err != nil {
			t.Errorf("%sRelAsOf(pin of life %d): %v", when, k+1, err)
		} else if got := rasLife(t, r); got != int64(k+1) || r.StartNodeID() != ends[lives[k]] {
			t.Errorf("%sRelAsOf(pin of life %d): life %d start %d, want life %d start %d", when, k+1, got, r.StartNodeID(), k+1, ends[lives[k]])
		}
		segRels := func(segs []temporal.RelSegment) []*types.Relationship {
			out := make([]*types.Relationship, 0, len(segs))
			for _, s := range segs {
				out = append(out, s.Rel)
			}
			return out
		}
		tl, err := g.Temporal.RelEffectiveTimeline(id, pin)
		if err != nil {
			t.Fatalf("%sRelEffectiveTimeline: %v", when, err)
		}
		collect := func(door string, run func(fn func(temporal.RelSegment) bool) error) {
			var segs []temporal.RelSegment
			if err := run(func(s temporal.RelSegment) bool {
				if s.Rel.ID() == id {
					segs = append(segs, s)
				}
				return true
			}); err != nil {
				t.Fatalf("%s%s: %v", when, door, err)
			}
			if got := rasLives(t, segRels(segs)); !slices.Equal(got, want) {
				t.Errorf("%s%s at pin of life %d: lives %v, want %v", when, door, k+1, got, want)
			}
		}
		collect("RelEffectiveTimeline", func(fn func(temporal.RelSegment) bool) error {
			for _, s := range tl {
				fn(s)
			}
			return nil
		})
		collect("ForEachRelEffective", func(fn func(temporal.RelSegment) bool) error {
			return g.Temporal.ForEachRelEffective(pin, fn)
		})
		collect("RelsEffectiveByIDs", func(fn func(temporal.RelSegment) bool) error {
			return g.Temporal.RelsEffectiveByIDs([]types.RelID{id}, pin, fn)
		})
		// AtNodes seeded with one start node answers exactly the lives that
		// started there.
		for _, label := range []string{"Ref", "Ev"} {
			var wantAt []int64
			for j := 0; j <= k; j++ {
				if lives[j] == label {
					wantAt = append(wantAt, int64(j+1))
				}
			}
			var segs []temporal.RelSegment
			if err := g.Temporal.ForEachRelEffectiveAtNodes([]types.NodeID{ends[label]}, temporal.Outgoing, nil, pin, func(s temporal.RelSegment) bool {
				if s.Rel.ID() == id {
					segs = append(segs, s)
				}
				return true
			}); err != nil {
				t.Fatalf("%sForEachRelEffectiveAtNodes: %v", when, err)
			}
			if got := rasLives(t, segRels(segs)); !slices.Equal(got, wantAt) {
				t.Errorf("%sForEachRelEffectiveAtNodes(%s) at pin of life %d: lives %v, want %v", when, label, k+1, got, wantAt)
			}
		}
	}
}

func rasCheckNode(t *testing.T, g *Core, when string, id types.NodeID, lives []string, pins []types.Instant) {
	t.Helper()
	h, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("%sHistory: %v", when, err)
	}
	if got, want := rasLives(t, h), rasUpTo(len(lives)-1); !slices.Equal(got, want) {
		t.Errorf("%sHistory: lives %v, want %v", when, got, want)
	}
	for k, pin := range pins {
		want := rasUpTo(k + 1)
		n, err := g.Temporal.NodeAsOf(id, pin)
		if err != nil {
			t.Errorf("%sNodeAsOf(pin of life %d): %v", when, k+1, err)
		} else if got := rasLife(t, n); got != int64(k+1) {
			t.Errorf("%sNodeAsOf(pin of life %d): life %d, want %d", when, k+1, got, k+1)
		}
		tl, err := g.Temporal.NodeEffectiveTimeline(id, pin)
		if err != nil {
			t.Fatalf("%sNodeEffectiveTimeline: %v", when, err)
		}
		collect := func(door string, run func(fn func(temporal.NodeSegment) bool) error) {
			var rows []*types.Node
			if err := run(func(s temporal.NodeSegment) bool {
				if s.Node.ID() == id {
					rows = append(rows, s.Node)
				}
				return true
			}); err != nil {
				t.Fatalf("%s%s: %v", when, door, err)
			}
			if got := rasLives(t, rows); !slices.Equal(got, want) {
				t.Errorf("%s%s at pin of life %d: lives %v, want %v", when, door, k+1, got, want)
			}
		}
		collect("NodeEffectiveTimeline", func(fn func(temporal.NodeSegment) bool) error {
			for _, s := range tl {
				fn(s)
			}
			return nil
		})
		collect("ForEachNodeEffective", func(fn func(temporal.NodeSegment) bool) error {
			return g.Temporal.ForEachNodeEffective(pin, fn)
		})
		collect("NodesEffectiveByIDs", func(fn func(temporal.NodeSegment) bool) error {
			return g.Temporal.NodesEffectiveByIDs([]types.NodeID{id}, pin, fn)
		})
	}
}
