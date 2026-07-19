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

// BACKLOG 10g: TestTemporalTwoDoorsAgreeOnLabelQueries (rule 17 — two doors,
// one answer) covered nodes only. The relationship-type doors are
// STRUCTURALLY ASYMMETRIC from their node/label counterparts: RelsByTypeAt
// (the named door) thinly delegates to the generic QueryOpts door, while
// nodesByLabelAtLocked is a SEPARATE hand-written implementation duplicating
// candidate-gathering + B4-prune + resolve — exactly the lessons 42/58 drift
// shape, but the equivalence test only guarded the node side. This is the
// relationship-type mirror.
//
// One structural adaptation: relationship TYPE is immutable after creation
// (no AddType/RemoveType door the way nodes have AddLabel/RemoveLabel — grep
// confirms no such door exists), so the node test's scenario D ("label held
// on genesis version, removed on current version") has no relationship
// analog and is omitted; A/B/C/E carry over directly.
func TestTemporalTwoDoorsAgreeOnTypeQueries(t *testing.T) {
	t.Parallel()

	backends := map[string]func(t *testing.T) *graphpkg.Graph{
		"memory": func(t *testing.T) *graphpkg.Graph {
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4})
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
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 5, Store: bs})
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

			start, err := g.Nodes().Add(ctx, []string{"Person"}, nil)
			if err != nil {
				t.Fatalf("add start: %v", err)
			}
			end, err := g.Nodes().Add(ctx, []string{"Place"}, nil)
			if err != nil {
				t.Fatalf("add end: %v", err)
			}

			// A: explicit closed world-time interval [1000, 2000).
			a, err := g.Rels().AddByID(ctx, "Visit", start.ID(), end.ID(), map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"tkg_valid_to":   types.Instant(2000),
				"who":            "a",
			})
			if err != nil {
				t.Fatalf("add A: %v", err)
			}
			// B: explicit open interval from 1500.
			b, err := g.Rels().AddByID(ctx, "Visit", start.ID(), end.ID(), map[string]any{
				"tkg_valid_from": types.Instant(1500),
				"who":            "b",
			})
			if err != nil {
				t.Fatalf("add B: %v", err)
			}
			// C: no world-time claim — effective valid-from falls back to the
			// snowflake creation instant (real wall clock, far after 2000).
			c, err := g.Rels().AddByID(ctx, "Visit", start.ID(), end.ID(), map[string]any{"who": "c"})
			if err != nil {
				t.Fatalf("add C: %v", err)
			}
			// E: created with VF=1000, then deleted. History must remain
			// queryable after deletion.
			e, err := g.Rels().AddByID(ctx, "Visit", start.ID(), end.ID(), map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"who":            "e",
			})
			if err != nil {
				t.Fatalf("add E: %v", err)
			}
			if err := g.Rels().Delete(ctx, e.ID()); err != nil {
				t.Fatalf("delete E: %v", err)
			}

			allIDs := []types.RelID{a.ID(), b.ID(), c.ID(), e.ID()}
			now := types.Instant(time.Now().UnixMilli())

			type pointCase struct {
				name     string
				at       types.Instant
				expected []types.RelID
			}
			pointCases := []pointCase{
				// t=1200: A in [1000,2000) ✓; B starts 1500 ✗; C starts ~now ✗; E pre-deletion ✓.
				{"mid-early", 1200, []types.RelID{a.ID(), e.ID()}},
				// t=1700: B has started; A still open at 1700 < 2000.
				{"mid-late", 1700, []types.RelID{a.ID(), b.ID(), e.ID()}},
				// far future: A closed at 2000 ✗; E deleted ✗; B open ✓; C effective-from ≈ creation ✓.
				{"now-plus-hour", now + 3_600_000, []types.RelID{b.ID(), c.ID()}},
			}

			for _, pc := range pointCases {
				want := sortedRelIDs(pc.expected)

				named, err := g.Temporal().RelsByTypeAt("Visit", pc.at)
				if err != nil {
					t.Fatalf("%s: RelsByTypeAt: %v", pc.name, err)
				}
				generic, err := g.Rels().ByType("Visit", storepkg.QueryOpts{ValidAt: pc.at})
				if err != nil {
					t.Fatalf("%s: ByType(ValidAt): %v", pc.name, err)
				}
				perID := resolveTypedAt(t, g, allIDs, pc.at)

				assertExactRelIDSet(t, pc.name+"/named-door", named, want)
				assertExactRelIDSet(t, pc.name+"/generic-door", generic, want)
				if fmt.Sprint(perID) != fmt.Sprint(want) {
					t.Errorf("%s/per-id-door: got %v, want %v", pc.name, perID, want)
				}
			}

			// Interval door: [1100, 1600) — A overlaps, B starts inside,
			// E historical version overlaps, C (≈now) must NOT.
			want := sortedRelIDs([]types.RelID{a.ID(), b.ID(), e.ID()})
			generic, err := g.Rels().ByType("Visit", storepkg.QueryOpts{ValidStart: 1100, ValidEnd: 1600})
			if err != nil {
				t.Fatalf("ByType(interval): %v", err)
			}
			assertExactRelIDSet(t, "interval/generic-door", generic, want)

			// Touching interval (Allen meets): [2000, 2100) must EXCLUDE A
			// (ValidTo 2000 is exclusive) — and include B/E.
			wantMeets := sortedRelIDs([]types.RelID{b.ID(), e.ID()})
			meets, err := g.Rels().ByType("Visit", storepkg.QueryOpts{ValidStart: 2000, ValidEnd: 2100})
			if err != nil {
				t.Fatalf("ByType(meets interval): %v", err)
			}
			assertExactRelIDSet(t, "interval-meets/generic-door", meets, wantMeets)
		})
	}
}

// resolveTypedAt walks every known ID through the per-ID resolver door and
// returns the sorted IDs alive (any type) at t — mirrors resolveLabeledAt,
// but a relationship's type is immutable so there's no "does it still carry
// this type" filter to apply beyond existence-at-t.
func resolveTypedAt(t *testing.T, g *graphpkg.Graph, ids []types.RelID, at types.Instant) []types.RelID {
	t.Helper()
	var out []types.RelID
	for _, id := range ids {
		if _, err := g.Temporal().RelAt(id, at); err != nil {
			if errors.Is(err, graphpkg.ErrRelNotFound) || errors.Is(err, storepkg.ErrNoVersionValidAt) {
				continue
			}
			t.Fatalf("RelAt(%v, %d): %v", id, at, err)
		}
		out = append(out, id)
	}
	return sortedRelIDs(out)
}

func sortedRelIDs(ids []types.RelID) []types.RelID {
	out := append([]types.RelID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i].SnowflakeID() < out[j].SnowflakeID() })
	return out
}

func assertExactRelIDSet(t *testing.T, door string, got []*types.Relationship, want []types.RelID) {
	t.Helper()
	gotIDs := make([]types.RelID, 0, len(got))
	for _, r := range got {
		gotIDs = append(gotIDs, r.ID())
	}
	gotIDs = sortedRelIDs(gotIDs)
	if fmt.Sprint(gotIDs) != fmt.Sprint(want) {
		t.Errorf("%s: got %v, want %v", door, gotIDs, want)
	}
}
