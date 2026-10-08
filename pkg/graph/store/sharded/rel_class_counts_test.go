package sharded_test

import (
	"context"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// relClassTruth classifies key on every current relationship of typ from its
// own row.
func relClassTruth(t *testing.T, g *graph.Graph, typ, key string) storepkg.PropertyTypeClassCounts {
	t.Helper()
	var c storepkg.PropertyTypeClassCounts
	n := 0
	if err := g.Rels().ForEachByType(typ, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		n++
		r.ForEachPropertyTypeClass(func(k string, class types.PropertyTypeClass) bool {
			if k != key {
				return true
			}
			switch class {
			case types.ClassNumeric:
				c.Numeric++
			case types.ClassNaN:
				c.NaN++
			case types.ClassString:
				c.String++
			case types.ClassBool:
				c.Bool++
			default:
				c.Other++
			}
			return true
		})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	c.Missing = int64(n) - (c.Numeric + c.NaN + c.String + c.Bool + c.Other)
	return c
}

// RelPropertyTypeClassCounts on sharded is the exact partition of a type's
// current relationships, each counted once: relationships on several slots
// with endpoints on other slots, then an update to a string, a property
// removal and a delete; after each step the counts equal a classification of
// the relationships' own rows.
func TestShardedRelPropertyTypeClassCounts(t *testing.T) {
	ctx := context.Background()
	g := newLanedShardedGraph(t, 4)
	ends, _ := addSpread(t, g, []string{"E"}, 8, func(i int) map[string]any { return map[string]any{"i": int64(i)} })
	values := []any{int64(1), 2.5, math.NaN(), "s", true, []any{int64(1)}}
	var rels []*types.Relationship
	relSlots := map[int64]struct{}{}
	for i := range 24 {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatal(err)
		}
		props := map[string]any{"w": values[i%len(values)]}
		if i%5 == 0 {
			props = map[string]any{"other": int64(1)}
		}
		r, err := sess.AddRelationship("T", ends[i%8], ends[(i+3)%8], props)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatal(err)
		}
		_ = sess.Close()
		relSlots[g.Admin().DecomposeNodeID(types.NodeID(r.ID().SnowflakeID())).NodeID] = struct{}{}
		rels = append(rels, r)
	}
	if len(relSlots) < 2 {
		t.Fatalf("relationships on %d slot(s), want several", len(relSlots))
	}
	check := func(step string) {
		t.Helper()
		got, err := g.Stats().RelPropertyTypeClassCounts("T", "w")
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if want := relClassTruth(t, g, "T", "w"); got != want {
			t.Fatalf("%s: counts %+v, want %+v", step, got, want)
		}
	}
	check("created")
	if _, err := g.Rels().Update(ctx, rels[1].ID(), map[string]any{"w": "now a string"}); err != nil {
		t.Fatal(err)
	}
	check("updated")
	if err := g.Rels().DeleteProperty(ctx, rels[2].ID(), "w"); err != nil {
		t.Fatal(err)
	}
	check("property removed")
	if err := g.Rels().Delete(ctx, rels[3].ID()); err != nil {
		t.Fatal(err)
	}
	check("deleted")
	if c, err := g.Stats().RelPropertyTypeClassCounts("T", "never"); err != nil || c.Missing != 23 {
		t.Fatalf("an absent key: %+v %v", c, err)
	}
	if _, err := g.Stats().RelPropertyTypeClassCounts("T", "tkg_x"); err == nil {
		t.Fatal("a reserved key passed")
	}
}
