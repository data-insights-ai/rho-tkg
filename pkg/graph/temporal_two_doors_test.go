package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Two doors, one answer (lesson 17 / testing rule 17): the named temporal
// method (NodesByLabelAt), the generic QueryOpts door (ByLabel with temporal
// opts → store push-down), and the per-ID resolver (NodeAt) must agree on the
// EXACT result set for the same adversarial dataset — entities with explicit
// closed intervals, explicit open intervals, no world-time assertion at all
// (snowflake fallback), a label that held historically but not on the current
// version, and a deleted entity with queryable history.
//
// Runs against memory AND badger so the store-level temporal push-down is
// exercised, not just the core fold.
func TestTemporalTwoDoorsAgreeOnLabelQueries(t *testing.T) {
	t.Parallel()

	backends := map[string]func(t *testing.T) *graphpkg.Graph{
		"memory": func(t *testing.T) *graphpkg.Graph {
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2})
			if err != nil {
				t.Fatalf("graph.New(memory): %v", err)
			}
			return g
		},
		"badger": func(t *testing.T) *graphpkg.Graph {
			bs, err := badgerstore.New(badgerstore.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, Store: bs})
			if err != nil {
				t.Fatalf("graph.New(badger): %v", err)
			}
			return g
		},
	}

	for name, open := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()
			ctx := context.Background()

			// A: explicit closed world-time interval [1000, 2000).
			a, err := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"tkg_valid_to":   types.Instant(2000),
				"who":            "a",
			})
			if err != nil {
				t.Fatalf("add A: %v", err)
			}
			// B: explicit open interval from 1500.
			b, err := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{
				"tkg_valid_from": types.Instant(1500),
				"who":            "b",
			})
			if err != nil {
				t.Fatalf("add B: %v", err)
			}
			// C: no world-time claim — effective valid-from falls back to the
			// snowflake creation instant (real wall clock, far after 2000).
			c, err := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{"who": "c"})
			if err != nil {
				t.Fatalf("add C: %v", err)
			}
			// D: label "Thing" held on the genesis version (VF=1000) but is
			// REMOVED on the current version. Historical queries must still
			// see it; current-time queries must not.
			d, err := g.Nodes().Add(ctx, []string{"Thing", "Keep"}, map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"who":            "d",
			})
			if err != nil {
				t.Fatalf("add D: %v", err)
			}
			if err := g.Nodes().RemoveLabel(ctx, d.ID(), "Thing"); err != nil {
				t.Fatalf("remove label from D: %v", err)
			}
			// E: created with VF=1000, then deleted. History must remain
			// queryable after deletion (lesson B32 territory).
			e, err := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"who":            "e",
			})
			if err != nil {
				t.Fatalf("add E: %v", err)
			}
			if err := g.Nodes().Delete(ctx, e.ID()); err != nil {
				t.Fatalf("delete E: %v", err)
			}

			allIDs := []types.NodeID{a.ID(), b.ID(), c.ID(), d.ID(), e.ID()}
			now := types.Instant(time.Now().UnixMilli())

			type pointCase struct {
				name     string
				at       types.Instant
				expected []types.NodeID
			}
			pointCases := []pointCase{
				// t=1200: A in [1000,2000) ✓; B starts 1500 ✗; C starts ~now ✗;
				// D genesis version still carries Thing ✓; E pre-deletion ✓.
				{"mid-early", 1200, []types.NodeID{a.ID(), d.ID(), e.ID()}},
				// t=1700: B has started; A still open at 1700 < 2000.
				{"mid-late", 1700, []types.NodeID{a.ID(), b.ID(), d.ID(), e.ID()}},
				// far future: A closed at 2000 ✗; D current version lost the
				// label ✗; E deleted ✗; B open ✓; C effective-from ≈ creation ✓.
				{"now-plus-hour", now + 3_600_000, []types.NodeID{b.ID(), c.ID()}},
			}

			for _, pc := range pointCases {
				want := sortedIDs(pc.expected)

				named, err := g.Temporal().NodesByLabelAt("Thing", pc.at)
				if err != nil {
					t.Fatalf("%s: NodesByLabelAt: %v", pc.name, err)
				}
				generic, err := g.Nodes().ByLabel("Thing", storepkg.QueryOpts{ValidAt: pc.at})
				if err != nil {
					t.Fatalf("%s: ByLabel(ValidAt): %v", pc.name, err)
				}
				perID := resolveLabeledAt(t, g, allIDs, "Thing", pc.at)

				assertExactIDSet(t, pc.name+"/named-door", named, want)
				assertExactIDSet(t, pc.name+"/generic-door", generic, want)
				if fmt.Sprint(perID) != fmt.Sprint(want) {
					t.Errorf("%s/per-id-door: got %v, want %v", pc.name, perID, want)
				}
			}

			// Interval door: [1100, 1600) — A overlaps, B starts inside,
			// D/E historical versions overlap, C (≈now) must NOT.
			want := sortedIDs([]types.NodeID{a.ID(), b.ID(), d.ID(), e.ID()})
			generic, err := g.Nodes().ByLabel("Thing", storepkg.QueryOpts{ValidStart: 1100, ValidEnd: 1600})
			if err != nil {
				t.Fatalf("ByLabel(interval): %v", err)
			}
			assertExactIDSet(t, "interval/generic-door", generic, want)

			// Touching interval (Allen meets): [2000, 2100) must EXCLUDE A
			// (ValidTo 2000 is exclusive) — and include B/D/E.
			wantMeets := sortedIDs([]types.NodeID{b.ID(), d.ID(), e.ID()})
			meets, err := g.Nodes().ByLabel("Thing", storepkg.QueryOpts{ValidStart: 2000, ValidEnd: 2100})
			if err != nil {
				t.Fatalf("ByLabel(meets interval): %v", err)
			}
			assertExactIDSet(t, "interval-meets/generic-door", meets, wantMeets)
		})
	}
}

// resolveLabeledAt walks every known ID through the per-ID resolver door and
// returns the sorted IDs whose at-t version carries the label.
func resolveLabeledAt(t *testing.T, g *graphpkg.Graph, ids []types.NodeID, label string, at types.Instant) []types.NodeID {
	t.Helper()
	var out []types.NodeID
	for _, id := range ids {
		n, err := g.Temporal().NodeAt(id, at)
		if err != nil {
			if errors.Is(err, graphpkg.ErrNodeNotFound) || errors.Is(err, storepkg.ErrNoVersionValidAt) {
				continue
			}
			t.Fatalf("NodeAt(%v, %d): %v", id, at, err)
		}
		for _, l := range g.Nodes().Labels(n) {
			if l == label {
				out = append(out, id)
				break
			}
		}
	}
	return sortedIDs(out)
}

func sortedIDs(ids []types.NodeID) []types.NodeID {
	out := append([]types.NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i].SnowflakeID() < out[j].SnowflakeID() })
	return out
}

func assertExactIDSet(t *testing.T, door string, got []*types.Node, want []types.NodeID) {
	t.Helper()
	gotIDs := make([]types.NodeID, 0, len(got))
	for _, n := range got {
		gotIDs = append(gotIDs, n.ID())
	}
	gotIDs = sortedIDs(gotIDs)
	if fmt.Sprint(gotIDs) != fmt.Sprint(want) {
		t.Errorf("%s: got %v, want %v", door, gotIDs, want)
	}
}
