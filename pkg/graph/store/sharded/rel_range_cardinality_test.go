package sharded_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RelRangeCardinality on sharded (round 4 R2) sums the slots' prefix-sum
// counts: relationships on several slots, endpoints on others, each counted
// once, against a count over the relationships' own rows, before and after an
// update and a delete. Without the index every slot declines; token 0 and a
// closed store are errors.
func TestShardedRelRangeCardinality(t *testing.T) {
	ctx := context.Background()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 6})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
	if err != nil {
		t.Fatal(err)
	}
	ends, _ := addSpread(t, g, []string{"E"}, 8, func(i int) map[string]any { return map[string]any{"i": int64(i)} })
	var rels []*types.Relationship
	slots := map[int64]struct{}{}
	for i := range 40 {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatal(err)
		}
		r, err := sess.AddRelationship("T", ends[i%8], ends[(i+3)%8], map[string]any{"w": int64(i % 13)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatal(err)
		}
		_ = sess.Close()
		slots[g.Admin().DecomposeNodeID(types.NodeID(r.ID().SnowflakeID())).NodeID] = struct{}{}
		rels = append(rels, r)
	}
	if len(slots) < 2 {
		t.Fatalf("relationships on %d slot(s), want several", len(slots))
	}
	if _, exact, err := g.Rels().RangeCardinality("T", "w", 0, 100, true, true, storepkg.QueryOpts{}); err != nil || exact {
		t.Fatalf("without an index: exact=%v err=%v, want a decline", exact, err)
	}
	if err := g.Index().CreateRelProperty("T", "w"); err != nil {
		t.Fatal(err)
	}
	check := func(step string) {
		t.Helper()
		for _, b := range [][2]float64{{0, 12}, {3, 7}, {5, 5}, {-1, 2}, {12, 99}} {
			var want int64
			if err := g.Rels().ForEachByType("T", storepkg.QueryOpts{}, func(r *types.Relationship) bool {
				if v, ok := r.GetProperty("w"); ok {
					if f := float64(v.(int64)); f >= b[0] && f <= b[1] {
						want++
					}
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			got, exact, err := g.Rels().RangeCardinality("T", "w", b[0], b[1], true, true, storepkg.QueryOpts{})
			if err != nil || !exact || got != want {
				t.Fatalf("%s: [%v,%v] = %d exact=%v err=%v, want %d", step, b[0], b[1], got, exact, err, want)
			}
		}
	}
	check("created")
	if _, err := g.Rels().Update(ctx, rels[1].ID(), map[string]any{"w": int64(5)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Rels().Delete(ctx, rels[5].ID()); err != nil {
		t.Fatal(err)
	}
	check("updated and deleted")

	if _, _, err := st.RelRangeCardinality(0, "w", 0, 1, true, true); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("token 0: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.RelRangeCardinality(1, "w", 0, 1, true, true); err == nil {
		t.Fatal("a closed store answered")
	}
}
