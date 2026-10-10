package graph_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// The index-inventory epoch (round 4 R1) advances on every index create and
// drop of every kind, on a unique constraint's implicit property index and on
// Reset, on every backend; it stays put for refusals (exists, not found,
// unsupported), validation failures, data writes and reads.
func TestIndexInventoryEpochAdvancesExactlyOnInventoryChanges(t *testing.T) {
	backends := allStoreBackendsWith(func(cfg *graphpkg.Config) { cfg.AllowReset = true })
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			ix := g.Index()
			epoch := ix.InventoryEpoch()
			if epoch != 0 {
				t.Fatalf("a new graph's epoch = %d, want 0", epoch)
			}
			// step runs op and checks the epoch moved by exactly one when op
			// succeeded and not at all when it was refused with a sentinel in
			// unchanged. It reports whether op succeeded.
			step := func(name string, op func() error, unchanged ...error) bool {
				t.Helper()
				err := op()
				got := ix.InventoryEpoch()
				if err == nil {
					if got != epoch+1 {
						t.Fatalf("%s succeeded: epoch %d -> %d, want +1", name, epoch, got)
					}
					epoch = got
					return true
				}
				refused := false
				for _, s := range unchanged {
					refused = refused || errors.Is(err, s)
				}
				if !refused {
					t.Fatalf("%s: unexpected error %v", name, err)
				}
				if got != epoch {
					t.Fatalf("%s refused (%v): epoch %d -> %d, want unchanged", name, err, epoch, got)
				}
				t.Logf("%s refused: %v", name, err)
				return false
			}
			still := func(name string) {
				t.Helper()
				if got := ix.InventoryEpoch(); got != epoch {
					t.Fatalf("%s: epoch %d -> %d, want unchanged", name, epoch, got)
				}
			}
			unsupported := []error{graphpkg.ErrCapabilityNotSupported, storepkg.ErrRelPropertyIndexUnsupported, graphpkg.ErrEventPropertyIndex}
			label := "P" // tiered: property indexes on reference labels only
			if b.tiered {
				label = "Ref"
				step("CreateProperty on an event label", func() error { return ix.CreateProperty("P", "age") }, graphpkg.ErrEventPropertyIndex)
			}

			a, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"age": int64(3), "v": []float32{1, 0}})
			if err != nil {
				t.Fatal(err)
			}
			c, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"age": int64(4), "v": []float32{0, 1}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(ctx, "R", a, c, map[string]any{"w": int64(1)}); err != nil {
				t.Fatal(err)
			}
			still("data writes")

			// Property: create, duplicate, has, drop, drop again.
			step("CreateProperty", func() error { return ix.CreateProperty(label, "age") })
			step("CreateProperty again", func() error { return ix.CreateProperty(label, "age") }, graphpkg.ErrIndexExists)
			if ok, err := ix.HasProperty(label, "age"); err != nil || !ok {
				t.Fatalf("HasProperty = %v, %v", ok, err)
			}
			still("HasProperty")
			step("DeleteProperty", func() error { return ix.DeleteProperty(label, "age") })
			step("DeleteProperty again", func() error { return ix.DeleteProperty(label, "age") }, graphpkg.ErrIndexNotFound)
			step("DeleteProperty unknown label", func() error { return ix.DeleteProperty("Nope", "age") }, graphpkg.ErrIndexNotFound)
			if err := ix.CreateProperty(label, "tkg_reserved"); err == nil {
				t.Fatal("a reserved key was accepted")
			}
			still("a rejected reserved key")

			// Relationship property (tiered refuses: unsupported).
			if step("CreateRelProperty", func() error { return ix.CreateRelProperty("R", "w") }, unsupported...) {
				step("CreateRelProperty again", func() error { return ix.CreateRelProperty("R", "w") }, graphpkg.ErrIndexExists)
				step("DeleteRelProperty", func() error { return ix.DeleteRelProperty("R", "w") })
			}

			// Composite.
			keys := []string{"age", "name"}
			if step("CreateComposite", func() error { return ix.CreateComposite(label, keys) }, unsupported...) {
				step("CreateComposite again", func() error { return ix.CreateComposite(label, keys) }, graphpkg.ErrIndexExists)
				if _, err := ix.ListComposites(label); err != nil {
					t.Fatal(err)
				}
				still("ListComposites")
				step("DeleteComposite", func() error { return ix.DeleteComposite(label, keys) })
				step("DeleteComposite again", func() error { return ix.DeleteComposite(label, keys) }, graphpkg.ErrIndexNotFound)
			}
			if err := ix.CreateComposite(label, []string{"only"}); err == nil {
				t.Fatal("a one-key composite was accepted")
			}
			still("a rejected composite")

			// Temporal and high-frequency (one kind per label).
			if step("CreateTemporal", func() error { return ix.CreateTemporal(label) }, unsupported...) {
				step("CreateTemporal again", func() error { return ix.CreateTemporal(label) }, graphpkg.ErrTemporalIndexExists)
				step("CreateHighFrequency over temporal", func() error { return ix.CreateHighFrequency(label, time.Hour) }, graphpkg.ErrTemporalIndexExists)
				step("DeleteTemporal", func() error { return ix.DeleteTemporal(label) })
				step("DeleteTemporal again", func() error { return ix.DeleteTemporal(label) }, graphpkg.ErrTemporalIndexNotFound)
			}
			if step("CreateHighFrequency", func() error { return ix.CreateHighFrequency(label, time.Hour) }, unsupported...) {
				step("DeleteHighFrequency", func() error { return ix.DeleteHighFrequency(label) })
			}
			if err := ix.CreateHighFrequency(label, time.Microsecond); err == nil {
				t.Fatal("a sub-millisecond bucket was accepted")
			}
			still("a rejected bucket size")
			if step("CreateRelTemporal", func() error { return ix.CreateRelTemporal("R") }, unsupported...) {
				step("CreateRelTemporal again", func() error { return ix.CreateRelTemporal("R") }, graphpkg.ErrTemporalIndexExists)
				step("DeleteRelTemporal", func() error { return ix.DeleteRelTemporal("R") })
			}

			// Vector.
			if step("CreateVector", func() error { return ix.CreateVector(label, "v", 2, graphpkg.DistanceCosine) }, unsupported...) {
				step("CreateVector again", func() error { return ix.CreateVector(label, "v", 2, graphpkg.DistanceCosine) }, graphpkg.ErrVectorIndexExists)
				if _, err := ix.SearchNearest(label, "v", []float32{1, 0}, 1, storepkg.QueryOpts{}); err != nil {
					t.Fatal(err)
				}
				still("SearchNearest")
				step("DeleteVector", func() error { return ix.DeleteVector(label, "v") })
				step("DeleteVector again", func() error { return ix.DeleteVector(label, "v") }, graphpkg.ErrVectorIndexNotFound)
			}
			if err := ix.CreateVector(label, "v", 0, graphpkg.DistanceCosine); err == nil {
				t.Fatal("zero dims accepted")
			}
			still("a rejected vector config")

			// A unique constraint's implicit property index.
			step("CreateUnique", func() error { return g.Constraints().CreateUnique(ctx, label, "age") }, unsupported...)

			// Data writes and reads after the DDL leave it.
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"age": int64(9)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().ByLabelAndProperty(label, "age", int64(9), storepkg.QueryOpts{}); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().Delete(ctx, c.ID()); err != nil {
				t.Fatal(err)
			}
			still("writes and reads after DDL")

			before := ix.InventoryEpoch()
			if err := g.Admin().Reset(); err != nil {
				t.Fatal(err)
			}
			if got := ix.InventoryEpoch(); got != before+1 {
				t.Fatalf("Reset: epoch %d -> %d, want +1", before, got)
			}
			if ok, _ := ix.HasProperty("P", "age"); ok {
				t.Fatal("an index survived Reset")
			}
		})
	}
}

// Concurrent creates and drops (index DDL runs under the graph's read lock)
// each advance the epoch once, and a reader polling it never sees it go back
// (run with -race).
func TestIndexInventoryEpochConcurrentDDL(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	if _, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"k0": int64(1), "k1": int64(2)}); err != nil {
		t.Fatal(err)
	}
	ix := g.Index()
	const writers, rounds = 4, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := []string{"k0", "k1", "k2", "k3"}[w]
			for range rounds {
				if err := ix.CreateProperty("P", key); err != nil {
					t.Error(err)
					return
				}
				if err := ix.DeleteProperty("P", key); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	var last uint64
	for polling := true; polling; {
		select {
		case <-done:
			polling = false
		default:
		}
		e := ix.InventoryEpoch()
		if e < last {
			t.Fatalf("the epoch went back: %d after %d", e, last)
		}
		last = e
	}
	if got := ix.InventoryEpoch(); got != 2*writers*rounds {
		t.Fatalf("epoch %d after %d creates and drops", got, 2*writers*rounds)
	}
}
