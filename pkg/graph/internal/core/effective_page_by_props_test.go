package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// pageDoorStores are the backends the listing page door is pinned on: tiered
// with the label as a reference label (sigma's and ai-soc's shape) and as an
// event label.
var pageDoorStores = map[string]func(t *testing.T) storepkg.MandatoryStore{
	"memory": func(*testing.T) storepkg.MandatoryStore { return memory.New() },
	"badger": func(t *testing.T) storepkg.MandatoryStore {
		bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	},
	"sharded": func(t *testing.T) storepkg.MandatoryStore {
		st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
		if err != nil {
			t.Fatal(err)
		}
		return st
	},
	"tiered-ref": func(t *testing.T) storepkg.MandatoryStore {
		ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"D"}, ShardWindow: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		return ts
	},
	"tiered-event": func(t *testing.T) storepkg.MandatoryStore {
		ts, err := tiered.New(tiered.Config{InMemory: true, ShardWindow: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		return ts
	},
}

// pageDoorFixture writes label D nodes whose (ns, rel) current values match
// {ns: "n", rel: "r"} or not, through every shape the door must agree on:
// plain matches, updated (two segments), rewritten in valid time (an older
// version wins earlier pins), moved out of the match, moved into it, deleted,
// another label with the values, and matches created after the pin (no
// segment at it). It returns the graph and the pin.
func pageDoorFixture(t *testing.T, st storepkg.MandatoryStore, index bool) (*Core, types.Instant) {
	t.Helper()
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	if index {
		if err := g.Index.CreateComposite("D", []string{"ns", "rel"}); err != nil {
			t.Fatal(err)
		}
	}
	add := func(label, rel string, i int) types.NodeID {
		n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"ns": "n", "rel": rel, "k": i, "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		return n.ID()
	}
	var match []types.NodeID
	for i := range 40 {
		match = append(match, add("D", "r", i))
		add("D", "other", i)
		if i%8 == 0 {
			add("E", "r", i)
		}
	}
	for i, id := range match {
		var err error
		switch i % 8 {
		case 1:
			_, err = g.Nodes.Update(ctx, id, map[string]any{"k": -i})
		case 2:
			_, err = g.Temporal.SetNodeVersionInterval(ctx, id, 1000, 0, map[string]any{"k": 100 + i})
		case 3:
			_, err = g.Nodes.Update(ctx, id, map[string]any{"rel": "gone"})
		case 4:
			err = g.Nodes.Delete(ctx, id)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	moved := add("D", "other", 99)
	if _, err := g.Nodes.Update(ctx, moved, map[string]any{"rel": "r"}); err != nil {
		t.Fatal(err)
	}
	pin := g.readNow()
	for i := range 3 {
		add("D", "r", 200+i) // current matches with no segment at pin
	}
	return g, pin
}

// renderSegs renders one node's segments at the pin.
func renderSegs(id types.NodeID, segs []temporal.NodeSegment) string {
	parts := make([]string, len(segs))
	for i, s := range segs {
		k, _ := s.Node.GetProperty("k")
		rel, _ := s.Node.GetProperty("rel")
		parts[i] = fmt.Sprintf("[%d,%d) v%d k=%v rel=%v", s.ValidFrom, s.ValidTo, s.Node.Version(), k, rel)
	}
	return fmt.Sprintf("%d:%s", id, strings.Join(parts, " "))
}

// pageDoorOracle is sigma's composition the door replaces: the matches of
// ByLabelAndProperties ascending, each with NodeEffectiveTimelines at pin.
func pageDoorOracle(t *testing.T, g *Core, props map[string]any, pin types.Instant) []string {
	t.Helper()
	nodes, err := g.Nodes.ByLabelAndProperties("D", props, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	slices.Sort(ids)
	out := make([]string, len(ids))
	for i, id := range ids {
		segs, err := g.Temporal.NodeEffectiveTimelines(id, []types.Instant{pin})
		if err != nil {
			t.Fatal(err)
		}
		out[i] = renderSegs(id, segs[0])
	}
	return out
}

// pageDoorWalk pages the door with limit until next is 0.
func pageDoorWalk(t *testing.T, g *Core, props map[string]any, pin types.Instant, limit int) []string {
	t.Helper()
	var out []string
	var after types.NodeID
	for pages := 0; ; pages++ {
		if pages > 1000 {
			t.Fatal("the door never ends")
		}
		got := 0
		next, err := g.Temporal.ForEachNodeEffectiveByLabelAndProperties(context.Background(), "D", props, pin, temporal.EffectivePageOpts{After: after, Limit: limit}, func(id types.NodeID, segs []temporal.NodeSegment) bool {
			if id <= after {
				t.Fatalf("id %d at or below the cursor %d", id, after)
			}
			out = append(out, renderSegs(id, segs))
			got++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if limit > 0 && got > limit {
			t.Fatalf("page of %d matches, limit %d", got, limit)
		}
		if next == 0 {
			if limit > 0 && got == limit {
				t.Fatalf("a full page of %d answered next 0", got)
			}
			return out
		}
		if got != limit {
			t.Fatalf("next %d after a page of %d matches, limit %d", next, got, limit)
		}
		after = next
	}
}

// TestForEachNodeEffectiveByLabelAndPropertiesMatchesComposition pins row 121:
// on every backend, with the composite index (the ID page path) and without
// it (the ByLabelAndProperties fallback), the door hands fn exactly the
// matches ByLabelAndProperties lists, ascending, each with
// NodeEffectiveTimelines(id, pin)[0] (empty for a match without a segment at
// the pin), paged by any limit. Mutation: skipping a match with no segment
// drops the three late ones; a wrong cursor repeats or skips a node.
func TestForEachNodeEffectiveByLabelAndPropertiesMatchesComposition(t *testing.T) {
	t.Parallel()
	props := map[string]any{"ns": "n", "rel": "r"}
	for name, open := range pageDoorStores {
		for _, index := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/index=%v", name, index), func(t *testing.T) {
				t.Parallel()
				g, pin := pageDoorFixture(t, open(t), index)
				want := pageDoorOracle(t, g, props, pin)
				if len(want) < 30 {
					t.Fatalf("fixture lists %d matches, want >= 30", len(want))
				}
				empty := 0
				for _, w := range want {
					if strings.HasSuffix(w, ":") {
						empty++
					}
				}
				if empty != 3 {
					t.Fatalf("fixture has %d matches without a segment at the pin, want 3", empty)
				}
				for _, limit := range []int{0, 1, 3, 7, 256, 300} {
					if got := pageDoorWalk(t, g, props, pin, limit); !slices.Equal(got, want) {
						t.Fatalf("limit %d:\n got %v\nwant %v", limit, got, want)
					}
				}
			})
		}
	}
}

// TestForEachNodeEffectiveByLabelAndPropertiesNumericForms pins the door's
// equality on a value with several index forms (3 and 3.0, types.EqualIndexValues):
// it lists what ByLabelAndProperties lists. Mutation: reading the composite
// posting list of the value's one form drops the 3.0 rows.
func TestForEachNodeEffectiveByLabelAndPropertiesNumericForms(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"memory", "badger", "tiered-ref"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: pageDoorStores[name](t)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			if err := g.Index.CreateComposite("D", []string{"ns", "rel"}); err != nil {
				t.Fatal(err)
			}
			for i, v := range []any{3, 3.0, int64(3), 4} {
				if _, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"ns": v, "rel": "r", "k": i}); err != nil {
					t.Fatal(err)
				}
			}
			pin := g.readNow()
			props := map[string]any{"ns": 3, "rel": "r"}
			want := pageDoorOracle(t, g, props, pin)
			if len(want) != 3 {
				t.Fatalf("oracle lists %d nodes, want 3", len(want))
			}
			if got := pageDoorWalk(t, g, props, pin, 0); !slices.Equal(got, want) {
				t.Fatalf("got %v\nwant %v", got, want)
			}
		})
	}
}

// TestForEachNodeEffectiveByLabelAndPropertiesContract pins the door's edges:
// fn returning false stops with next = that node's ID; a cancelled ctx and
// the argument errors refuse before fn runs; an unknown label answers next 0
// without calling fn; segment rows are frozen (lent).
func TestForEachNodeEffectiveByLabelAndPropertiesContract(t *testing.T) {
	t.Parallel()
	g, pin := pageDoorFixture(t, pageDoorStores["badger"](t), true)
	props := map[string]any{"ns": "n", "rel": "r"}
	door := func(ctx context.Context, label string, props map[string]any, opts temporal.EffectivePageOpts, fn func(types.NodeID, []temporal.NodeSegment) bool) (types.NodeID, error) {
		return g.Temporal.ForEachNodeEffectiveByLabelAndProperties(ctx, label, props, pin, opts, fn)
	}
	ctx := context.Background()

	var seen []types.NodeID
	next, err := door(ctx, "D", props, temporal.EffectivePageOpts{}, func(id types.NodeID, segs []temporal.NodeSegment) bool {
		for _, s := range segs {
			if !s.Node.IsFrozen() {
				t.Fatalf("segment row of %d is not frozen", id)
			}
		}
		seen = append(seen, id)
		return len(seen) < 3
	})
	if err != nil || len(seen) != 3 || next != seen[2] {
		t.Fatalf("fn false at the 3rd match: next %d, err %v, seen %v", next, err, seen)
	}

	never := func(types.NodeID, []temporal.NodeSegment) bool { t.Fatal("fn called"); return false }
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for name, c := range map[string]struct {
		ctx   context.Context
		label string
		props map[string]any
		opts  temporal.EffectivePageOpts
		fn    func(types.NodeID, []temporal.NodeSegment) bool
		want  error
	}{
		"cancelled": {cancelled, "D", props, temporal.EffectivePageOpts{}, never, context.Canceled},
		"nil ctx":   {nil, "D", props, temporal.EffectivePageOpts{}, never, ErrNilContext},
		"limit":     {ctx, "D", props, temporal.EffectivePageOpts{Limit: -1}, never, ErrInvalidQueryLimit},
		"cursor":    {ctx, "D", props, temporal.EffectivePageOpts{After: -1}, never, ErrInvalidQueryCursor},
		"nil fn":    {ctx, "D", props, temporal.EffectivePageOpts{}, nil, grapherr.ErrNilCallback},
		"one key":   {ctx, "D", map[string]any{"ns": "n"}, temporal.EffectivePageOpts{}, never, storepkg.ErrInvalidStoreMutation},
	} {
		//nolint:staticcheck // SA1012: the nil context is the case under test
		if _, err := door(c.ctx, c.label, c.props, c.opts, c.fn); !errors.Is(err, c.want) {
			t.Fatalf("%s: err %v, want %v", name, err, c.want)
		}
	}
	if next, err := door(ctx, "Unknown", props, temporal.EffectivePageOpts{}, never); err != nil || next != 0 {
		t.Fatalf("unknown label: next %d, err %v", next, err)
	}
}

// widePostingStore lists every ID in extra as a composite candidate besides
// the index's own: a store whose posting list is wider than its matches (an
// orphaned or stale entry).
type widePostingStore struct {
	*badgerpkg.Store
	extra []types.NodeID
}

func (w *widePostingStore) NodeIDsByLabelAndPropertiesPage(tok uint16, values map[string]any, after types.NodeID, n int, depth storepkg.ShardDepth) ([]types.NodeID, bool, bool, error) {
	ids, more, handled, err := w.Store.NodeIDsByLabelAndPropertiesPage(tok, values, after, n, depth)
	if err != nil || !handled {
		return ids, more, handled, err
	}
	for _, id := range w.extra {
		if id > after {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) > n {
		ids, more = ids[:n], true
	}
	return ids, more, true, nil
}

// TestForEachNodeEffectiveByLabelAndPropertiesChecksTheRow pins the match
// re-check: a candidate whose current row does not carry the label and every
// pair (or is gone) never reaches fn and does not count toward the page, as a
// store's posting list is only a candidate list. Mutation: trusting the
// candidate (no match check) hands fn the deleted, moved-out and other-label
// nodes.
func TestForEachNodeEffectiveByLabelAndPropertiesChecksTheRow(t *testing.T) {
	t.Parallel()
	bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	wide := &widePostingStore{Store: bs}
	g, pin := pageDoorFixture(t, wide, true)
	if g.compositeIDPage == nil {
		t.Fatal("the graph does not use the test store's candidate page")
	}
	props := map[string]any{"ns": "n", "rel": "r"}
	want := pageDoorOracle(t, g, props, pin)
	all, err := g.Nodes.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range all {
		wide.extra = append(wide.extra, n.ID())
	}
	wide.extra = append(wide.extra, types.NodeID(1)) // never written
	for _, limit := range []int{0, 1, 7} {
		if got := pageDoorWalk(t, g, props, pin, limit); !slices.Equal(got, want) {
			t.Fatalf("limit %d:\n got %v\nwant %v", limit, got, want)
		}
	}
}
