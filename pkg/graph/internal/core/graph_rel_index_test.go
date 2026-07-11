package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	memory "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newTieredTestCore builds an in-memory tiered-backed Core with "Ref" as a
// reference label so relationships between reference nodes exercise the
// cross-shard rel-property query fallback.
func newTieredTestCore(t *testing.T) *Core {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Ref"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{Store: ts})
	if err != nil {
		_ = ts.Close()
		t.Fatalf("New(tiered): %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// --- Relationship property indexes (K3b) end-to-end battery ---
//
// The relationship mirror of graph_index_test.go's node property-index tests
// (Testing Rule 2, Node/Rel parity). Each backend-parametrised test runs on both
// the memory and badger backends via relIndexBackends.

type relIndexBackend struct {
	name    string
	newCore func(t *testing.T) *Core
}

func relIndexBackends() []relIndexBackend {
	return []relIndexBackend{
		{
			name: "memory",
			newCore: func(t *testing.T) *Core {
				g, err := New(Config{Store: memory.New()})
				if err != nil {
					t.Fatalf("New(memory): %v", err)
				}
				t.Cleanup(func() { _ = g.Close() })
				return g
			},
		},
		{
			name: "badger",
			newCore: func(t *testing.T) *Core {
				bs, err := badger.New(badger.Config{InMemory: true})
				if err != nil {
					t.Fatalf("badger.New: %v", err)
				}
				g, err := New(Config{Store: bs})
				if err != nil {
					_ = bs.Close()
					t.Fatalf("New(badger): %v", err)
				}
				t.Cleanup(func() { _ = g.Close() })
				return g
			},
		},
	}
}

// relIDSetOf returns a set of the rel IDs in rels for exact-set assertions.
func relIDSetOf(rels []*types.Relationship) map[types.RelID]struct{} {
	out := make(map[types.RelID]struct{}, len(rels))
	for _, r := range rels {
		out[r.ID()] = struct{}{}
	}
	return out
}

func addTwoNodes(t *testing.T, g *Core) (*types.Node, *types.Node) {
	t.Helper()
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node b: %v", err)
	}
	return a, b
}

func TestGraphCreateRelProperty_RegistersTypeAndRejectsDuplicate(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			if _, ok := g.relTypes.Lookup("KNOWS"); !ok {
				t.Fatal("CreateRelProperty must register the rel-type token")
			}
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); !errors.Is(err, storepkg.ErrIndexExists) {
				t.Fatalf("duplicate CreateRelProperty err = %v, want ErrIndexExists", err)
			}
		})
	}
}

func TestGraphCreateRelPropertyBeforeTypeExistsIndexesFutureRels(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty before type exists: %v", err)
			}
			a, b := addTwoNodes(t, g)
			if _, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)}); err != nil {
				t.Fatalf("add future indexed rel: %v", err)
			}
			rels, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("ByTypeAndProperty: %v", err)
			}
			if len(rels) != 1 {
				t.Fatalf("future rel not indexed: got %d, want 1", len(rels))
			}
		})
	}
}

func TestGraphRelPropertyIndex_CreationBackfillAndExactSet(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)

			// Rels created BEFORE the index — backfill must pick them up.
			r1, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			r2, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			r3, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(9)})
			// Different type, same value — must NOT appear in the KNOWS index.
			rOther, _ := g.Rels.Add(ctx, "LIKES", a, b, map[string]any{"weight": int64(5)})

			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}

			got, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("ByTypeAndProperty(5): %v", err)
			}
			set := relIDSetOf(got)
			if len(set) != 2 {
				t.Fatalf("weight=5 backfill: got %d rels, want 2 (%v)", len(set), set)
			}
			for _, want := range []types.RelID{r1.ID(), r2.ID()} {
				if _, ok := set[want]; !ok {
					t.Fatalf("weight=5 missing rel %d; got %v", want, set)
				}
			}
			if _, ok := set[r3.ID()]; ok {
				t.Fatal("weight=5 must NOT contain the weight=9 rel")
			}
			if _, ok := set[rOther.ID()]; ok {
				t.Fatal("weight=5 must NOT contain the LIKES rel (different type)")
			}

			// Phantom value returns empty.
			empty, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(999), storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("ByTypeAndProperty(999): %v", err)
			}
			if len(empty) != 0 {
				t.Fatalf("phantom value returned %d rels, want 0", len(empty))
			}

			// Type-safe key: string "5" must not collide with int 5.
			strGot, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", "5", storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("ByTypeAndProperty(\"5\"): %v", err)
			}
			if len(strGot) != 0 {
				t.Fatalf("string \"5\" matched %d int-valued rels, want 0", len(strGot))
			}
		})
	}
}

func TestGraphRelPropertyIndex_UpdateReflected(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			r, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}

			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"weight": int64(7)}); err != nil {
				t.Fatalf("Update: %v", err)
			}

			old, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if len(old) != 0 {
				t.Fatalf("superseded value still indexed: %d", len(old))
			}
			cur, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(7), storepkg.QueryOpts{})
			if len(cur) != 1 || cur[0].ID() != r.ID() {
				t.Fatalf("new value not indexed: got %d rels", len(cur))
			}
		})
	}
}

func TestGraphRelPropertyIndex_InPlaceUpdateReflected(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			r, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}

			if _, err := g.Rels.UpdateInPlace(ctx, r.ID(), map[string]any{"weight": int64(8)}); err != nil {
				t.Fatalf("UpdateInPlace: %v", err)
			}

			old, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if len(old) != 0 {
				t.Fatalf("in-place: old value still indexed: %d", len(old))
			}
			cur, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(8), storepkg.QueryOpts{})
			if len(cur) != 1 {
				t.Fatalf("in-place: new value not indexed: %d", len(cur))
			}
		})
	}
}

func TestGraphRelPropertyIndex_CASReflected(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			r, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}

			ok, err := g.Rels.CompareAndSetProperty(ctx, r.ID(), "weight", int64(5), int64(11))
			if err != nil || !ok {
				t.Fatalf("CompareAndSetProperty = (%v, %v), want (true, nil)", ok, err)
			}

			old, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if len(old) != 0 {
				t.Fatalf("CAS: old value still indexed: %d", len(old))
			}
			cur, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(11), storepkg.QueryOpts{})
			if len(cur) != 1 {
				t.Fatalf("CAS: new value not indexed: %d", len(cur))
			}
		})
	}
}

func TestGraphRelPropertyIndex_DeleteRemoves(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			r, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			got, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if len(got) != 0 {
				t.Fatalf("deleted rel still in index: %d", len(got))
			}
		})
	}
}

func TestGraphRelPropertyIndex_CascadeDeleteRemoves(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			r, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			// Deleting the endpoint node cascades to the relationship.
			if err := g.Nodes.Delete(ctx, a.ID()); err != nil {
				t.Fatalf("Delete node (cascade): %v", err)
			}
			got, _ := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if len(got) != 0 {
				t.Fatalf("cascade-deleted rel %d still in index: %d", r.ID(), len(got))
			}
		})
	}
}

func TestGraphRelsByTypeAndProperty_NoIndexFallbackScan(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(9)})
			// No index created — must fall back to a type scan + filter.
			got, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("fallback ByTypeAndProperty: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("fallback scan: got %d rels, want 1", len(got))
			}
		})
	}
}

func TestGraphDropRelProperty(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			if err := g.Index.DeleteRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("DeleteRelProperty: %v", err)
			}
			// Second drop → not found.
			if err := g.Index.DeleteRelProperty("KNOWS", "weight"); !errors.Is(err, storepkg.ErrIndexNotFound) {
				t.Fatalf("double DeleteRelProperty err = %v, want ErrIndexNotFound", err)
			}
			// Unknown type → not found.
			if err := g.Index.DeleteRelProperty("NEVER", "weight"); !errors.Is(err, storepkg.ErrIndexNotFound) {
				t.Fatalf("DeleteRelProperty(unknown type) err = %v, want ErrIndexNotFound", err)
			}
		})
	}
}

func TestGraphRelPropertyRange_OverSelectAndExactRecheck(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			weights := []int64{1, 5, 9, 15}
			ids := make(map[int64]types.RelID)
			for _, w := range weights {
				r, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": w})
				if err != nil {
					t.Fatalf("add rel w=%d: %v", w, err)
				}
				ids[w] = r.ID()
			}
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}

			// Range [5, 12]: candidates over-select; apply exact recheck in fn.
			got := map[types.RelID]struct{}{}
			err := g.Rels.ForEachByTypePropertyRange("KNOWS", "weight", 5, 12, true, true, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
				w, ok := r.GetProperty("weight")
				if !ok {
					return true
				}
				if wv, ok := w.(int64); ok && wv >= 5 && wv <= 12 {
					got[r.ID()] = struct{}{}
				}
				return true
			})
			if err != nil {
				t.Fatalf("ForEachByTypePropertyRange: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("range [5,12] exact recheck: got %d, want 2 (%v)", len(got), got)
			}
			if _, ok := got[ids[5]]; !ok {
				t.Fatal("range missing w=5")
			}
			if _, ok := got[ids[9]]; !ok {
				t.Fatal("range missing w=9")
			}
			if _, ok := got[ids[15]]; ok {
				t.Fatal("range must NOT contain w=15 after exact recheck")
			}
			if _, ok := got[ids[1]]; ok {
				t.Fatal("range must NOT contain w=1 after exact recheck")
			}
		})
	}
}

func TestGraphRelPropertyRange_NoIndexReturnsErrIndexNotFound(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)})
			// No index — the range door declines so callers fall back to a scan.
			err := g.Rels.ForEachByTypePropertyRange("KNOWS", "weight", 0, 100, true, true, storepkg.QueryOpts{}, func(*types.Relationship) bool { return true })
			if !errors.Is(err, storepkg.ErrIndexNotFound) {
				t.Fatalf("range with no index err = %v, want ErrIndexNotFound", err)
			}
		})
	}
}

// TestGraphRelsByTypeAndProperty_Temporal is the two-phase temporal test
// (Testing Rule 15): a rel holds weight=5 at t0, is updated to weight=7 after
// t0; a query pinned at t0 must still see weight=5.
func TestGraphRelsByTypeAndProperty_TemporalTwoPhase(t *testing.T) {
	t.Parallel()
	for _, be := range relIndexBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			a, b := addTwoNodes(t, g)
			r, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5), "tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatalf("add rel: %v", err)
			}
			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			// Update the world-time slot: weight becomes 7 from t=2000 onward.
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"weight": int64(7), "tkg_valid_from": types.Instant(2000)}); err != nil {
				t.Fatalf("Update: %v", err)
			}

			// Pinned at t=1500 (inside the weight=5 slot): weight=5 must be visible.
			atT0, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{ValidAt: types.Instant(1500)})
			if err != nil {
				t.Fatalf("ByTypeAndProperty@t0: %v", err)
			}
			if len(atT0) != 1 || atT0[0].ID() != r.ID() {
				t.Fatalf("temporal query at t0 = %d rels, want the weight=5 version", len(atT0))
			}
			// And weight=7 must NOT be visible at t0.
			at0New, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(7), storepkg.QueryOpts{ValidAt: types.Instant(1500)})
			if err != nil {
				t.Fatalf("ByTypeAndProperty(7)@t0: %v", err)
			}
			if len(at0New) != 0 {
				t.Fatalf("weight=7 visible at t0 = %d, want 0", len(at0New))
			}
		})
	}
}

func TestGraphRelPropertyIndex_TieredDeclinesCreationButQueryWorks(t *testing.T) {
	t.Parallel()
	g := newTieredTestCore(t)
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatalf("add node a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatalf("add node b: %v", err)
	}
	if _, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)}); err != nil {
		t.Fatalf("add rel: %v", err)
	}

	// Creation declines with the clear sentinel.
	if err := g.Index.CreateRelProperty("KNOWS", "weight"); !errors.Is(err, storepkg.ErrRelPropertyIndexUnsupported) {
		t.Fatalf("tiered CreateRelProperty err = %v, want ErrRelPropertyIndexUnsupported", err)
	}

	// Query still works via the graph-layer type-scan fallback.
	got, err := g.Rels.ByTypeAndProperty("KNOWS", "weight", int64(5), storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("tiered ByTypeAndProperty: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("tiered fallback query got %d rels, want 1", len(got))
	}
}
