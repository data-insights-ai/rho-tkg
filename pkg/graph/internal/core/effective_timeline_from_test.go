package core

import (
	"context"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// writeGrowNode builds sigma's derived row (WRITEGROW): one version per
// window, windows [1000+1000i, 2000+1000i) for i in [0, windows), so every
// segment is its own version. A window beside the current row's leaves the
// current row as it is: it holds the first window, history the rest.
func writeGrowNode(tb testing.TB, g *Core, windows int) types.NodeID {
	tb.Helper()
	ctx := context.Background()
	p := r10Props(0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	n, err := g.Nodes.Add(ctx, []string{"Derived"}, p)
	if err != nil {
		tb.Fatalf("Add: %v", err)
	}
	for i := 1; i < windows; i++ {
		from := types.Instant(1000 + 1000*i)
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), from, from+1000, r10Props(i)); err != nil {
			tb.Fatalf("SetNodeVersionInterval: %v", err)
		}
	}
	return n.ID()
}

// The tail is the whole timeline's segments ending at or after validFrom (a
// segment ending at validFrom counts, an open one always), cold and from the
// head entry: closed windows, rows without history (open, closed), windows
// ending open.
func TestNodeEffectiveTimelineFromIsTheTail(t *testing.T) {
	const windows = 20
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			a := g.axis()
			closed := writeGrowNode(t, g, windows)
			plain, err := g.Nodes.Add(ctx, []string{"Derived"}, map[string]any{"tkg_valid_from": int64(1000)})
			if err != nil {
				t.Fatal(err)
			}
			bare, err := g.Nodes.Add(ctx, []string{"Derived"}, map[string]any{"tkg_valid_from": int64(1000), "tkg_valid_to": int64(2000)})
			if err != nil {
				t.Fatal(err)
			}
			open := writeGrowNode(t, g, windows)
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, open, 1000+1000*windows, a.OpenEnd(), r10Props(windows)); err != nil {
				t.Fatal(err)
			}
			pin := g.peekNow()
			check := func(id types.NodeID, when string, cold bool) {
				t.Helper()
				for _, from := range []types.Instant{types.UnboundedValidFrom, -1, 0, 1000, 1999, 2000, 2001, 10_500, 20_000, 20_001, 21_000, 1 << 40} {
					if cold {
						g.heads.put(headKey{id: id.SnowflakeID()}, nil)
					}
					got, err := g.Temporal.NodeEffectiveTimelineFrom(id, from, pin)
					if err != nil {
						t.Fatalf("%s from %d: %v", when, from, err)
					}
					whole, err := g.Temporal.NodeEffectiveTimeline(id, pin)
					if err != nil {
						t.Fatal(err)
					}
					want := slices.DeleteFunc(whole, func(s temporal.NodeSegment) bool { return !a.Open(s.ValidTo) && s.ValidTo < from })
					if !sameSegments(got, want) {
						t.Fatalf("%s from %d: %d segments %v, want %d %v", when, from, len(got), spans(got), len(want), spans(want))
					}
				}
			}
			for name, id := range map[string]types.NodeID{"closed": closed, "plain": plain.ID(), "bare": bare.ID(), "open": open} {
				check(id, name+" cold", true)
				check(id, name+" head", false)
			}
		})
	}
}

// From the head entry the tail reads only the history rows it returns:
// sigma's write-back reads the last windows of a row with hundreds.
func TestNodeEffectiveTimelineFromReadsOnlyTheTail(t *testing.T) {
	const windows = 200
	for _, be := range r10Backends() {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			id := writeGrowNode(t, g, windows)
			pin := g.peekNow()
			tail := func(when string) {
				t.Helper()
				rows := 0
				g.historyRead = func(n int) { rows += n }
				segs, err := g.Temporal.NodeEffectiveTimelineFrom(id, types.Instant(1000*windows), pin)
				g.historyRead = nil
				if err != nil {
					t.Fatal(err)
				}
				if len(segs) != 2 {
					t.Fatalf("%s: %d segments, want the last 2: %v", when, len(segs), spans(segs))
				}
				if rows > len(segs) {
					t.Fatalf("%s: %d history rows read for a tail of %d history segments, want one per segment", when, rows, len(segs))
				}
			}
			if g.temporalMetaHistory != nil { // badger: the skeletons name the winners
				tail("skeletons")
				g.heads.put(headKey{id: id.SnowflakeID()}, nil)
			}
			if _, err := g.Temporal.NodeEffectiveTimeline(id, pin); err != nil { // fills the head entry
				t.Fatal(err)
			}
			tail("head")
		})
	}
}

func sameSegments(a, b []temporal.NodeSegment) bool {
	return slices.EqualFunc(a, b, func(x, y temporal.NodeSegment) bool {
		return x.ValidFrom == y.ValidFrom && x.ValidTo == y.ValidTo && x.Node.Version() == y.Node.Version()
	})
}

func spans(segs []temporal.NodeSegment) [][2]types.Instant {
	out := make([][2]types.Instant, len(segs))
	for i, s := range segs {
		out[i] = [2]types.Instant{s.ValidFrom, s.ValidTo}
	}
	return out
}
