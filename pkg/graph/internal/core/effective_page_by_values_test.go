package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// valuesDoorValues probe pageDoorFixture's k column: plain matches (0, 5, 7),
// a value's other numeric forms (6.0, int64 8), updated rows (k 1 -> -1,
// 9 -> -9: the old value and the new), valid-time rewrites (k 102, 110), moved
// out (3, rel gone), deleted (4, 12), moved in (99), matches with no segment
// at the pin (200-202), values label E and rel "other" also carry, and values
// nothing carries.
var valuesDoorValues = []any{0, 1, 6.0, 3, 4, int64(8), 5, -1, -9, 102, 110, 12, 16, 24, 7, 99, 200, 201, 202, "x", 500}

// valuesDoorOracle is the composition the door replaces: the label's nodes
// ByLabelAndPropertyValues lists that carry every fixed pair, ascending, each
// with NodeEffectiveTimelines at pin.
func valuesDoorOracle(t *testing.T, g *Core, props map[string]any, values []any, pin types.Instant) []string {
	t.Helper()
	nodes, err := g.Nodes.ByLabelAndPropertyValues("D", "k", values, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []types.NodeID
	for _, n := range nodes {
		all := true
		for k, v := range props {
			got, ok := n.GetProperty(k)
			all = all && ok && got == v
		}
		if all {
			ids = append(ids, n.ID())
		}
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

// valuesDoorWalk pages the value-list door with limit until next is 0.
func valuesDoorWalk(t *testing.T, g *Core, props map[string]any, values []any, pin types.Instant, limit int) []string {
	t.Helper()
	var out []string
	var after types.NodeID
	for pages := 0; ; pages++ {
		if pages > 1000 {
			t.Fatal("the door never ends")
		}
		got := 0
		next, err := g.Temporal.ForEachNodeEffectiveByLabelAndPropertyValues(context.Background(), "D", props, "k", values, pin, temporal.EffectivePageOpts{After: after, Limit: limit}, func(id types.NodeID, segs []temporal.NodeSegment) bool {
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

// TestForEachNodeEffectiveByLabelAndPropertyValuesMatchesComposition pins
// ROWPROBE's door: on every backend, with the index on k (in RAM and on disk)
// and without it, with fixed pairs and without, the door hands fn exactly
// the label's ByLabelAndPropertyValues matches carrying every fixed pair,
// ascending, each with NodeEffectiveTimelines(id, pin)[0], paged by any
// limit. Mutation: dropping the fixed-pair filter lists rel "other"; matching
// the value list on one numeric form drops the 2.0 and int64 probes; skipping
// a match with no segment drops 200-202.
func TestForEachNodeEffectiveByLabelAndPropertyValuesMatchesComposition(t *testing.T) {
	t.Parallel()
	cfgs := map[string]func(t *testing.T) Config{
		"badger-disk": func(*testing.T) Config { return Config{BadgerInMemory: true, PropertyIndexOnDisk: true} },
	}
	for name, open := range pageDoorStores {
		cfgs[name] = func(t *testing.T) Config { return Config{Store: open(t)} }
	}
	fixed := map[string]any{"ns": "n", "rel": "r"}
	for name, cfg := range cfgs {
		for _, index := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/index=%v", name, index), func(t *testing.T) {
				t.Parallel()
				g, pin := pageDoorFixtureCfg(t, cfg(t), false)
				if index {
					if err := g.Index.CreateProperty("D", "k"); err != nil {
						t.Fatal(err)
					}
				}
				for _, props := range []map[string]any{fixed, nil} {
					want := valuesDoorOracle(t, g, props, valuesDoorValues, pin)
					empty := 0
					for _, w := range want {
						if strings.HasSuffix(w, ":") {
							empty++
						}
					}
					// fixed: k 0 -1 6 8 5 -9 102 110 16 24 7 99 and the late 200-202;
					// without pairs also rel "other" (k 0 1 3-8 12 16 24) and the
					// moved-out 3.
					if n := map[bool]int{true: 15, false: 27}[props != nil]; len(want) != n || empty != 3 {
						t.Fatalf("props %v: the oracle lists %d matches (%d without a segment), want %d (3)\n%v", props, len(want), empty, n, want)
					}
					for _, limit := range []int{0, 1, 3, 7, 300} {
						if got := valuesDoorWalk(t, g, props, valuesDoorValues, pin, limit); !slices.Equal(got, want) {
							t.Fatalf("props %v limit %d:\n got %v\nwant %v", props, limit, got, want)
						}
					}
				}
			})
		}
	}
}

// TestForEachNodeEffectiveByLabelAndPropertyValuesContract pins the door's
// edges: fn returning false stops with next = that node's ID; a cancelled ctx
// and the argument errors refuse before fn runs; an unknown label and an empty
// value list answer next 0 without calling fn; segment rows are frozen.
func TestForEachNodeEffectiveByLabelAndPropertyValuesContract(t *testing.T) {
	t.Parallel()
	g, pin := pageDoorFixture(t, pageDoorStores["badger"](t), false)
	if err := g.Index.CreateProperty("D", "k"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index.CreatePropertyWithOptions("D", "tags", storepkg.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	props := map[string]any{"ns": "n", "rel": "r"}
	values := valuesDoorValues
	door := func(ctx context.Context, label string, props map[string]any, key string, values []any, opts temporal.EffectivePageOpts, fn func(types.NodeID, []temporal.NodeSegment) bool) (types.NodeID, error) {
		return g.Temporal.ForEachNodeEffectiveByLabelAndPropertyValues(ctx, label, props, key, values, pin, opts, fn)
	}
	ctx := context.Background()

	var seen []types.NodeID
	next, err := door(ctx, "D", props, "k", values, temporal.EffectivePageOpts{}, func(id types.NodeID, segs []temporal.NodeSegment) bool {
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
		ctx    context.Context
		props  map[string]any
		key    string
		values []any
		opts   temporal.EffectivePageOpts
		fn     func(types.NodeID, []temporal.NodeSegment) bool
		want   error
	}{
		"cancelled":       {cancelled, props, "k", values, temporal.EffectivePageOpts{}, never, context.Canceled},
		"nil ctx":         {nil, props, "k", values, temporal.EffectivePageOpts{}, never, ErrNilContext},
		"limit":           {ctx, props, "k", values, temporal.EffectivePageOpts{Limit: -1}, never, ErrInvalidQueryLimit},
		"cursor":          {ctx, props, "k", values, temporal.EffectivePageOpts{After: -1}, never, ErrInvalidQueryCursor},
		"nil fn":          {ctx, props, "k", values, temporal.EffectivePageOpts{}, nil, grapherr.ErrNilCallback},
		"key in props":    {ctx, map[string]any{"k": 1}, "k", values, temporal.EffectivePageOpts{}, never, storepkg.ErrInvalidStoreMutation},
		"bad value":       {ctx, props, "k", []any{1, struct{}{}}, temporal.EffectivePageOpts{}, never, types.ErrUnsupportedValueType},
		"bad fixed value": {ctx, map[string]any{"ns": struct{}{}}, "k", values, temporal.EffectivePageOpts{}, never, types.ErrUnsupportedValueType},
		"shadow key":      {ctx, props, "tkg_x", values, temporal.EffectivePageOpts{}, never, types.ErrReservedPrefix},
		"elements key":    {ctx, props, "tags", values, temporal.EffectivePageOpts{}, never, storepkg.ErrElementsIndexUnsupported},
		"elements pair":   {ctx, map[string]any{"tags": "a"}, "k", values, temporal.EffectivePageOpts{}, never, storepkg.ErrElementsIndexUnsupported},
	} {
		//nolint:staticcheck // SA1012: the nil context is the case under test
		if _, err := door(c.ctx, "D", c.props, c.key, c.values, c.opts, c.fn); !errors.Is(err, c.want) {
			t.Fatalf("%s: err %v, want %v", name, err, c.want)
		}
	}
	if next, err := door(ctx, "Unknown", props, "k", values, temporal.EffectivePageOpts{}, never); err != nil || next != 0 {
		t.Fatalf("unknown label: next %d, err %v", next, err)
	}
	if next, err := door(ctx, "D", props, "k", nil, temporal.EffectivePageOpts{}, never); err != nil || next != 0 {
		t.Fatalf("empty values: next %d, err %v", next, err)
	}
}
