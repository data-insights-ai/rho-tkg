// Tests in this file extend TestSubAPISmoke (in subapi_smoke_test.go) so
// every public wrapper across the 13 sub-API accessors is invoked at least
// once. The base smoke test covered the happy-path accessors; this file
// covers the remaining "0% line in `go tool cover -func`" wrappers — Update,
// UpdateInPlace, History, ByLabelAndProperty, IndexProvider registration,
// every Temporal point-in-time/interval/AsOf entry, every Stats label-count,
// every Resolve lookup, the events SetAsync wrapper, the constraints Add
// wrapper, and the tiered-only Admin surface (driven through a tiered.Store
// helper so we can pin its sentinel-error contract end-to-end).
//
// Each wrapper is invoked with minimal valid args; failure assertions are
// kept loose because the goal is wrapper-level coverage of the forward call,
// not a re-test of the underlying behaviour.

package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestSubAPISmoke_NodesAllWrappers(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ctx := context.Background()

	a, err := g.Nodes.AddWithContext(ctx, []string{"Person"}, map[string]any{"name": "Alice", "age": int64(30)})
	if err != nil {
		t.Fatalf("AddWithContext: %v", err)
	}

	if _, err := g.Nodes.GetWithContext(ctx, a.ID()); err != nil {
		t.Errorf("GetWithContext: %v", err)
	}
	if _, err := g.Nodes.GetByIDs([]types.NodeID{a.ID()}); err != nil {
		t.Errorf("GetByIDs: %v", err)
	}
	if _, err := g.Nodes.GetByIDs([]types.NodeID{a.ID(), types.NodeID(999)}); !errors.Is(err, graphpkg.ErrNodeNotFound) {
		t.Errorf("GetByIDs missing: err = %v, want ErrNodeNotFound", err)
	}
	if _, err := g.Nodes.Update(a.ID(), map[string]any{"age": int64(31)}); err != nil {
		t.Errorf("Update: %v", err)
	}
	if _, err := g.Nodes.UpdateWithContext(ctx, a.ID(), map[string]any{"age": int64(32)}); err != nil {
		t.Errorf("UpdateWithContext: %v", err)
	}
	if _, err := g.Nodes.UpdateInPlace(a.ID(), map[string]any{"age": int64(33)}); err != nil {
		t.Errorf("UpdateInPlace: %v", err)
	}
	if _, err := g.Nodes.UpdateInPlaceWithContext(ctx, a.ID(), map[string]any{"age": int64(34)}); err != nil {
		t.Errorf("UpdateInPlaceWithContext: %v", err)
	}

	if err := g.Nodes.SetProperty(a.ID(), "since", int64(2026)); err != nil {
		t.Errorf("SetProperty: %v", err)
	}
	if err := g.Nodes.DeleteProperty(a.ID(), "since"); err != nil {
		t.Errorf("DeleteProperty: %v", err)
	}
	if _, err := g.Nodes.CompareAndSetProperty(a.ID(), "age", int64(34), int64(35)); err != nil {
		t.Errorf("CompareAndSetProperty: %v", err)
	}
	if _, err := g.Nodes.CompareAndSetPropertyWithContext(ctx, a.ID(), "age", int64(35), int64(36)); err != nil {
		t.Errorf("CompareAndSetPropertyWithContext: %v", err)
	}

	if err := g.Nodes.AddLabel(a.ID(), "VIP"); err != nil {
		t.Errorf("AddLabel: %v", err)
	}
	if err := g.Nodes.RemoveLabel(a.ID(), "VIP"); err != nil {
		t.Errorf("RemoveLabel: %v", err)
	}

	if _, err := g.Nodes.All(storepkg.QueryOpts{}); err != nil {
		t.Errorf("All: %v", err)
	}
	if _, err := g.Nodes.ByLabelAndProperty("Person", "age", int64(36), storepkg.QueryOpts{}); err != nil {
		t.Errorf("ByLabelAndProperty: %v", err)
	}

	if _, err := g.Nodes.History(a.ID()); err != nil {
		t.Errorf("History: %v", err)
	}
	if _, err := g.Nodes.NextVersion(a.ID(), 0); err != nil && !errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Errorf("NextVersion: %v", err)
	}
	if _, err := g.Nodes.PreviousVersion(a.ID(), 1); err != nil && !errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Errorf("PreviousVersion: %v", err)
	}
	if err := g.Nodes.CloseVersion(a.ID(), types.Instant(time.Now().Add(time.Hour).UnixMilli())); err != nil {
		t.Errorf("CloseVersion: %v", err)
	}
	_ = g.Nodes.NextID() // pure ID generator — coverage only

	// Import: caller-supplied ID
	importID := g.Nodes.NextID()
	if _, err := g.Nodes.Import(ctx, importID, []string{"Person"}, map[string]any{"name": "Imported"}); err != nil {
		t.Errorf("Import: %v", err)
	}

	if err := g.Nodes.Delete(a.ID()); err != nil {
		t.Errorf("Delete: %v", err)
	}
	if err := g.Nodes.DeleteWithContext(ctx, importID); err != nil {
		t.Errorf("DeleteWithContext: %v", err)
	}
}

func TestSubAPISmoke_RelsAllWrappers(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ctx := context.Background()
	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})

	r1, err := g.Rels.AddWithContext(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddWithContext: %v", err)
	}
	r2, err := g.Rels.AddByID("FOLLOWS", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	r3, err := g.Rels.AddByIDWithContext(ctx, "LIKES", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByIDWithContext: %v", err)
	}
	r4, _, err := g.Rels.AddByIDIfAbsent("UNIQUE", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByIDIfAbsent: %v", err)
	}
	r5, _, err := g.Rels.AddByIDIfAbsentWithContext(ctx, "UNIQUE2", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByIDIfAbsentWithContext: %v", err)
	}

	if _, err := g.Rels.GetWithContext(ctx, r1.ID()); err != nil {
		t.Errorf("GetWithContext: %v", err)
	}
	if _, err := g.Rels.GetByIDs([]types.RelID{r1.ID(), r2.ID()}); err != nil {
		t.Errorf("GetByIDs: %v", err)
	}
	if _, err := g.Rels.GetByIDs([]types.RelID{r1.ID(), types.RelID(999)}); !errors.Is(err, graphpkg.ErrRelNotFound) {
		t.Errorf("GetByIDs missing: err = %v, want ErrRelNotFound", err)
	}

	if _, err := g.Rels.Update(r1.ID(), map[string]any{"weight": float64(0.5)}); err != nil {
		t.Errorf("Update: %v", err)
	}
	if _, err := g.Rels.UpdateWithContext(ctx, r1.ID(), map[string]any{"weight": float64(0.6)}); err != nil {
		t.Errorf("UpdateWithContext: %v", err)
	}
	if _, err := g.Rels.UpdateInPlace(r1.ID(), map[string]any{"weight": float64(0.7)}); err != nil {
		t.Errorf("UpdateInPlace: %v", err)
	}
	if _, err := g.Rels.UpdateInPlaceWithContext(ctx, r1.ID(), map[string]any{"weight": float64(0.8)}); err != nil {
		t.Errorf("UpdateInPlaceWithContext: %v", err)
	}
	if err := g.Rels.SetProperty(r1.ID(), "since", int64(2026)); err != nil {
		t.Errorf("SetProperty: %v", err)
	}
	if _, err := g.Rels.CompareAndSetProperty(r1.ID(), "since", int64(2026), int64(2027)); err != nil {
		t.Errorf("CompareAndSetProperty: %v", err)
	}
	if _, err := g.Rels.CompareAndSetPropertyWithContext(ctx, r1.ID(), "since", int64(2027), int64(2028)); err != nil {
		t.Errorf("CompareAndSetPropertyWithContext: %v", err)
	}
	if err := g.Rels.DeleteProperty(r1.ID(), "since"); err != nil {
		t.Errorf("DeleteProperty: %v", err)
	}

	if _, err := g.Rels.All(storepkg.QueryOpts{}); err != nil {
		t.Errorf("All: %v", err)
	}
	if _, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{}); err != nil {
		t.Errorf("ByType: %v", err)
	}
	if _, err := g.Rels.CountByType("KNOWS"); err != nil {
		t.Errorf("CountByType: %v", err)
	}
	if _, err := g.Rels.Incoming(b.ID(), "KNOWS"); err != nil {
		t.Errorf("Incoming: %v", err)
	}
	if _, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID()}, "KNOWS"); err != nil {
		t.Errorf("OutgoingForNodes: %v", err)
	}
	if _, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID()}, "KNOWS"); err != nil {
		t.Errorf("IncomingForNodes: %v", err)
	}

	if _, err := g.Rels.History(r1.ID()); err != nil {
		t.Errorf("History: %v", err)
	}
	if _, err := g.Rels.NextVersion(r1.ID(), 0); err != nil && !errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Errorf("NextVersion: %v", err)
	}
	if _, err := g.Rels.PreviousVersion(r1.ID(), 1); err != nil && !errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Errorf("PreviousVersion: %v", err)
	}
	if err := g.Rels.CloseVersion(r1.ID(), types.Instant(time.Now().Add(time.Hour).UnixMilli())); err != nil {
		t.Errorf("CloseVersion: %v", err)
	}
	_ = g.Rels.NextID()

	importID := g.Rels.NextID()
	if _, err := g.Rels.Import(ctx, importID, "IMPORTED", a, b, nil); err != nil {
		t.Errorf("Import: %v", err)
	}

	if err := g.Rels.Delete(r2.ID()); err != nil {
		t.Errorf("Delete: %v", err)
	}
	if err := g.Rels.DeleteWithContext(ctx, r3.ID()); err != nil {
		t.Errorf("DeleteWithContext: %v", err)
	}
	_ = r4
	_ = r5
}

func TestSubAPISmoke_TemporalAllWrappers(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A", "age": int64(30)})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B", "age": int64(40)})
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"since": int64(2025)})

	now := types.Instant(time.Now().UnixMilli())
	earlier := now - 1
	later := now + 1
	txNow := r.Temporal().TxFrom

	if _, err := g.Temporal.NodeAt(a.ID(), now); err != nil {
		t.Errorf("NodeAt: %v", err)
	}
	if _, err := g.Temporal.NodesAt(now); err != nil {
		t.Errorf("NodesAt: %v", err)
	}
	if _, err := g.Temporal.NodesByLabelAt("Person", now); err != nil {
		t.Errorf("NodesByLabelAt: %v", err)
	}
	if _, err := g.Temporal.RelAt(r.ID(), now); err != nil {
		t.Errorf("RelAt: %v", err)
	}
	if _, err := g.Temporal.RelationshipsAt(now); err != nil {
		t.Errorf("RelationshipsAt: %v", err)
	}
	if _, err := g.Temporal.RelationshipsByTypeAt("KNOWS", now); err != nil {
		t.Errorf("RelationshipsByTypeAt: %v", err)
	}
	if _, err := g.Temporal.NeighborsAt(a.ID(), now); err != nil {
		t.Errorf("NeighborsAt: %v", err)
	}
	if _, err := g.Temporal.NodesByLabelPropertyAt("Person", "age", int64(30), now); err != nil {
		t.Errorf("NodesByLabelPropertyAt: %v", err)
	}
	if _, err := g.Temporal.RelsByTypePropertyAt("KNOWS", "since", int64(2025), now); err != nil {
		t.Errorf("RelsByTypePropertyAt: %v", err)
	}

	if _, err := g.Temporal.NodesDuring(earlier, later); err != nil {
		t.Errorf("NodesDuring: %v", err)
	}
	if _, err := g.Temporal.RelationshipsDuring(earlier, later); err != nil {
		t.Errorf("RelationshipsDuring: %v", err)
	}
	if _, err := g.Temporal.NodesByLabelPropertyDuring("Person", "age", int64(30), earlier, later); err != nil {
		t.Errorf("NodesByLabelPropertyDuring: %v", err)
	}
	if _, err := g.Temporal.RelsByTypePropertyDuring("KNOWS", "since", int64(2025), earlier, later); err != nil {
		t.Errorf("RelsByTypePropertyDuring: %v", err)
	}

	if _, err := g.Temporal.NodeAsOf(a.ID(), txNow); err != nil {
		t.Errorf("NodeAsOf: %v", err)
	}
	if _, err := g.Temporal.RelAsOf(r.ID(), txNow); err != nil {
		t.Errorf("RelAsOf: %v", err)
	}
	if _, err := g.Temporal.NodesAsOf(txNow); err != nil {
		t.Errorf("NodesAsOf: %v", err)
	}
	if _, err := g.Temporal.RelsAsOf(txNow); err != nil {
		t.Errorf("RelsAsOf: %v", err)
	}

	if _, err := g.Temporal.Diff(earlier, later); err != nil {
		t.Errorf("Diff: %v", err)
	}
	if err := g.Temporal.DiffCallback(earlier, later, temporalpkg.DiffHandlers{}); err != nil {
		t.Errorf("DiffCallback: %v", err)
	}

	// Allen relation helpers require finite intervals (ValidTo != 0). Close
	// the open versions before exercising the wrappers so each entity
	// carries a finite [valid_from, valid_to) window.
	endTime := types.Instant(time.Now().Add(time.Hour).UnixMilli())
	if err := g.Nodes.CloseVersion(a.ID(), endTime); err != nil {
		t.Fatalf("CloseVersion(a): %v", err)
	}
	if err := g.Nodes.CloseVersion(b.ID(), endTime); err != nil {
		t.Fatalf("CloseVersion(b): %v", err)
	}
	if err := g.Rels.CloseVersion(r.ID(), endTime); err != nil {
		t.Fatalf("CloseVersion(r): %v", err)
	}
	r2, err := g.Rels.Add("ALT", a, b, nil)
	if err != nil {
		t.Fatalf("Rels.Add(ALT): %v", err)
	}
	if err := g.Rels.CloseVersion(r2.ID(), endTime); err != nil {
		t.Fatalf("CloseVersion(r2): %v", err)
	}

	aClosed, err := g.Nodes.Get(a.ID())
	if err != nil {
		t.Fatalf("re-fetch a: %v", err)
	}
	bClosed, err := g.Nodes.Get(b.ID())
	if err != nil {
		t.Fatalf("re-fetch b: %v", err)
	}
	rClosed, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("re-fetch r: %v", err)
	}
	r2Closed, err := g.Rels.Get(r2.ID())
	if err != nil {
		t.Fatalf("re-fetch r2: %v", err)
	}

	if _, _, err := g.Temporal.NodeInterval(aClosed); err != nil {
		t.Errorf("NodeInterval: %v", err)
	}
	if _, _, err := g.Temporal.RelInterval(rClosed); err != nil {
		t.Errorf("RelInterval: %v", err)
	}
	if _, err := g.Temporal.RelateNodes(aClosed, bClosed); err != nil {
		t.Errorf("RelateNodes: %v", err)
	}
	if _, err := g.Temporal.RelateRels(rClosed, r2Closed); err != nil {
		t.Errorf("RelateRels: %v", err)
	}
}

func TestSubAPISmoke_StatsResolveConstraintsEvents(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})
	r, _ := g.Rels.Add("KNOWS", a, b, nil)
	_ = r

	var snapshot graphpkg.GraphStats = g.Stats.Get()
	if snapshot.NodesAdded != 2 || snapshot.RelsAdded != 1 {
		t.Fatalf("Stats.Get snapshot = %+v, want NodesAdded=2 RelsAdded=1", snapshot)
	}

	if _, err := g.Stats.NodeCountByLabel("Person"); err != nil {
		t.Errorf("Stats.NodeCountByLabel: %v", err)
	}
	if _, err := g.Stats.RelCountByType("KNOWS"); err != nil {
		t.Errorf("Stats.RelCountByType: %v", err)
	}
	if _, err := g.Stats.AllLabelCounts(); err != nil {
		t.Errorf("Stats.AllLabelCounts: %v", err)
	}
	if _, err := g.Stats.AllRelTypeCounts(); err != nil {
		t.Errorf("Stats.AllRelTypeCounts: %v", err)
	}

	if _, ok := g.Resolve.RelProperty(r, "tkg_type"); !ok {
		t.Errorf("Resolve.RelProperty(tkg_type): missing")
	}
	if _, err := g.Resolve.LabelToken("Person"); err != nil {
		t.Errorf("Resolve.LabelToken: %v", err)
	}
	if _, err := g.Resolve.RelTypeToken("KNOWS"); err != nil {
		t.Errorf("Resolve.RelTypeToken: %v", err)
	}
	if _, ok := g.Resolve.LookupRelType("KNOWS"); !ok {
		t.Errorf("Resolve.LookupRelType(KNOWS): missing")
	}

	if err := g.Constraints.Add(temporalpkg.TemporalConstraint{}); !errors.Is(err, temporalpkg.ErrTemporalConstraint) || !errors.Is(err, temporalpkg.ErrInvalidTemporalConstraint) {
		t.Errorf("Constraints.Add(invalid): %v", err)
	}

	_ = g.Events.SetAsync(nil) // tolerated: clears
}

func TestSubAPISmoke_IndexAllWrappers(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if _, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"name": "x", "embedding": []float32{1, 2, 3}}); err != nil {
		t.Fatalf("seed Doc: %v", err)
	}

	// Property index already covered in TestSubAPISmoke; here we hit
	// every other index wrapper.
	if err := g.Index.CreateHighFrequency("Doc", time.Hour); err != nil {
		t.Errorf("CreateHighFrequency: %v", err)
	}
	if err := g.Index.DropHighFrequency("Doc"); err != nil {
		t.Errorf("DropHighFrequency: %v", err)
	}
	if err := g.Index.CreateTemporal("Doc"); err != nil {
		t.Errorf("CreateTemporal: %v", err)
	}
	if err := g.Index.DropTemporal("Doc"); err != nil {
		t.Errorf("DropTemporal: %v", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 3, storepkg.DistanceCosine); err != nil {
		t.Errorf("CreateVector: %v", err)
	}
	if _, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 2, 3}, 1, storepkg.QueryOpts{}); err != nil {
		t.Errorf("SearchNearest: %v", err)
	}
	if err := g.Index.DropVector("Doc", "embedding"); err != nil {
		t.Errorf("DropVector: %v", err)
	}

	// IndexProvider registration: a fake provider so we can hit Register and
	// Unregister without depending on out-of-tree implementations.
	p := &fakeIndexProvider{name: "fake"}
	if err := g.Index.RegisterProvider(p); err != nil {
		t.Errorf("RegisterProvider: %v", err)
	}
	if err := g.Index.RegisterProvider(&fakeIndexProvider{name: "fake"}); !errors.Is(err, graphpkg.ErrIndexProviderExists) {
		t.Errorf("RegisterProvider duplicate: %v, want ErrIndexProviderExists", err)
	}
	if err := g.Index.RegisterProvider(&fakeIndexProvider{name: " \t\n "}); !errors.Is(err, graphpkg.ErrIndexProviderEmptyName) {
		t.Errorf("RegisterProvider blank name: %v, want ErrIndexProviderEmptyName", err)
	}
	if list := g.Index.Providers(); len(list) == 0 {
		t.Errorf("Providers: empty after RegisterProvider")
	}
	if err := g.Index.UnregisterProvider("fake"); err != nil {
		t.Errorf("UnregisterProvider: %v", err)
	}
	if err := g.Index.UnregisterProvider("missing"); !errors.Is(err, graphpkg.ErrIndexProviderNotFound) {
		t.Errorf("UnregisterProvider missing: %v, want ErrIndexProviderNotFound", err)
	}

	lp := &fakeLegacyIndexProvider{name: "legacy"}
	if err := g.Index.RegisterLegacyProvider(lp); err != nil {
		t.Errorf("RegisterLegacyProvider: %v", err)
	}
}

func TestSubAPISmoke_AdminWrappers_NonTieredSentinel(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	a, _ := g.Nodes.Add([]string{"Person"}, nil)

	// Each admin wrapper on a non-tiered store must surface
	// ErrNotTieredStore. Pinning the wrapper-level sentinel keeps the
	// "graph layer doesn't quietly drop the call" contract enforced.
	if err := g.Admin.Archive(a.ID()); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Errorf("Archive: %v, want ErrNotTieredStore", err)
	}
	if err := g.Admin.Restore(a.ID()); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Errorf("Restore: %v, want ErrNotTieredStore", err)
	}
	if err := g.Admin.ForceRotate(); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Errorf("ForceRotate: %v, want ErrNotTieredStore", err)
	}
	if _, err := g.Admin.ListShards(); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Errorf("ListShards: %v, want ErrNotTieredStore", err)
	}
	if err := g.Admin.RebuildCatalog(); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Errorf("RebuildCatalog: %v, want ErrNotTieredStore", err)
	}
	if _, err := g.Admin.Repair(); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Errorf("Repair: %v, want ErrNotTieredStore", err)
	}
	if _, err := g.Admin.VerifyShard("shard-name"); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Errorf("VerifyShard: %v, want ErrNotTieredStore", err)
	}
	// Reset is implemented as store.Clear() — not a tiered-only operation,
	// so it succeeds on the in-memory store. We exercise the wrapper for
	// coverage and accept any error or nil return.
	_ = g.Admin.Reset()
}

func TestSubAPISmoke_AdminWrappers_TieredHappyPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, err := tiered.New(tiered.Config{
		DataDir:     dir,
		ShardWindow: 7 * 24 * time.Hour,
		RefLabels:   []string{"Person"},
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatalf("graphpkg.New(tiered): %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if _, err := g.Admin.ListShards(); err != nil {
		t.Errorf("ListShards: %v", err)
	}
	if err := g.Admin.RebuildCatalog(); err != nil {
		t.Errorf("RebuildCatalog: %v", err)
	}
	if _, err := g.Admin.Repair(); err != nil {
		t.Errorf("Repair: %v", err)
	}
	if err := g.Admin.ForceRotate(); err != nil {
		t.Errorf("ForceRotate: %v", err)
	}
	if shards, err := g.Admin.ListShards(); err == nil && len(shards) > 0 {
		if _, err := g.Admin.VerifyShard(shards[0].Name); err != nil {
			t.Errorf("VerifyShard(%s): %v", shards[0].Name, err)
		}
	}
	if err := g.Admin.Reset(); err != nil {
		t.Errorf("Reset: %v", err)
	}
}

// ─── fakes ────────────────────────────────────────────────────────────────

type fakeIndexProvider struct{ name string }

func (p *fakeIndexProvider) Name() string               { return p.name }
func (p *fakeIndexProvider) OnEvent(events.Event) error { return nil }
func (p *fakeIndexProvider) Close() error               { return nil }

type fakeLegacyIndexProvider struct{ name string }

func (p *fakeLegacyIndexProvider) Name() string                               { return p.name }
func (p *fakeLegacyIndexProvider) OnEvent(events.Event, indexpkg.GraphReader) {}
func (p *fakeLegacyIndexProvider) Close() error                               { return nil }

// Compile-time assertions so the fakes pin the IndexProvider /
// LegacyIndexProvider interfaces. If those interfaces gain a method, this
// test file must be updated alongside the API change.
var (
	_ indexpkg.IndexProvider = (*fakeIndexProvider)(nil)
	// nolint:staticcheck // LegacyIndexProvider is deprecated by design;
	// this is the test that pins the wrapper still works for legacy
	// implementations. The deprecation warning will go away when the
	// wrapper itself is removed in a future major version.
	_ indexpkg.LegacyIndexProvider = (*fakeLegacyIndexProvider)(nil)
)
