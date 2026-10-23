package core

import (
	"context"
	"reflect"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestHistoryTemporalMetaMatchesHistory pins the TemporalMetaHistoryCapability
// contract on memory, badger and tiered (sharded offers it on its read
// snapshot only): the skeleton has exactly the
// history's rows, ascending, with their versions and temporal blocks, after
// updates, a valid-time correction and a delete (row 120). Mutation: a meta
// read that skips a row, reorders, or reports another row's temporal block
// fails a comparison.
func TestHistoryTemporalMetaMatchesHistory(t *testing.T) {
	t.Parallel()
	stores := map[string]func(t *testing.T) storepkg.MandatoryStore{
		"memory": func(*testing.T) storepkg.MandatoryStore { return memory.New() },
		"badger": func(t *testing.T) storepkg.MandatoryStore {
			bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		},
		"tiered": func(t *testing.T) storepkg.MandatoryStore {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Fact"}, ShardWindow: 7 * 24 * time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return ts
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := open(t)
			meta, ok := st.(storepkg.TemporalMetaHistoryCapability)
			if !ok {
				t.Fatalf("%s does not offer TemporalMetaHistoryCapability", name)
			}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			a, err := g.Nodes.Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": 0})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes.Add(ctx, []string{"Fact"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.Add(ctx, "R", a, b, map[string]any{"tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"x": 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, a.ID(), 2000, 3000, map[string]any{"x": 2}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"y": 1}); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}

			nodeRows, err := st.GetNodeHistory(a.ID())
			if err != nil {
				t.Fatal(err)
			}
			nodeMeta, err := meta.NodeHistoryTemporalMeta(a.ID())
			if err != nil {
				t.Fatal(err)
			}
			if len(nodeRows) < 2 {
				t.Fatalf("fixture wrote %d node history rows, want >= 2", len(nodeRows))
			}
			compareMeta(t, "node", nodeMeta, len(nodeRows), func(i int) (uint32, *types.TemporalMetadata) {
				return nodeRows[i].Version(), nodeRows[i].Temporal()
			})

			relRows, err := st.GetRelHistory(r.ID())
			if err != nil {
				t.Fatal(err)
			}
			relMeta, err := meta.RelHistoryTemporalMeta(r.ID())
			if err != nil {
				t.Fatal(err)
			}
			if len(relRows) < 2 {
				t.Fatalf("fixture wrote %d relationship history rows, want >= 2", len(relRows))
			}
			compareMeta(t, "relationship", relMeta, len(relRows), func(i int) (uint32, *types.TemporalMetadata) {
				return relRows[i].Version(), relRows[i].Temporal()
			})
		})
	}
}

func compareMeta(t *testing.T, kind string, metas []storepkg.VersionTemporalMeta, rows int, row func(int) (uint32, *types.TemporalMetadata)) {
	t.Helper()
	if len(metas) != rows {
		t.Fatalf("%s: %d meta rows, %d history rows", kind, len(metas), rows)
	}
	for i := range metas {
		v, tm := row(i)
		if metas[i].Version != v {
			t.Fatalf("%s row %d: meta version %d, history version %d", kind, i, metas[i].Version, v)
		}
		if tm == nil || metas[i].Temporal == nil {
			if tm != metas[i].Temporal {
				t.Fatalf("%s row %d: meta temporal %v, history temporal %v", kind, i, metas[i].Temporal, tm)
			}
			continue
		}
		// The skeleton carries the selection fields; compare those.
		got, want := *metas[i].Temporal, *tm
		got.CreatedAt, want.CreatedAt = 0, 0
		got.CreatedBy, want.CreatedBy = "", ""
		got.UpdatedBy, want.UpdatedBy = "", ""
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s row %d (v%d): meta temporal %+v, history temporal %+v", kind, i, v, got, want)
		}
	}
}
