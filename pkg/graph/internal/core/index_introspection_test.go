// Query-planner index-existence/config introspection doors (BACKLOG 21b) —
// HasProperty / HasTemporal / VectorIndexInfo / HasRelProperty, the
// single-key/vector/rel-property counterparts to HasComposite/ListComposites
// (sigma_r3_test.go). Mirrors that test's structure: multi-backend battery,
// unregistered-label/type = false (no error), create/drop lifecycle,
// capability-absent = ErrCapabilityNotSupported.

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	memory "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// introspectionBackend is relIndexBackend's sibling for the doors under test
// here, plus a sharded arm (sharded genuinely supports property/temporal/
// vector/rel-property indexes, unlike tiered's partial support — tiered is
// exercised separately below).
type introspectionBackend struct {
	name    string
	newCore func(t *testing.T) *Core
}

func introspectionBackends() []introspectionBackend {
	return []introspectionBackend{
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
		{
			name: "sharded",
			newCore: func(t *testing.T) *Core {
				st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
				if err != nil {
					t.Fatalf("sharded.New: %v", err)
				}
				g, err := New(Config{Store: st})
				if err != nil {
					_ = st.Close()
					t.Fatalf("New(sharded): %v", err)
				}
				t.Cleanup(func() { _ = g.Close() })
				return g
			},
		},
	}
}

func TestHasProperty_Lifecycle(t *testing.T) {
	t.Parallel()
	for _, be := range introspectionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			if _, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
				t.Fatalf("seed Add: %v", err)
			}

			// Unregistered label: false, no error.
			has, err := g.Index.HasProperty("NoSuchLabel", "name")
			if err != nil || has {
				t.Fatalf("unregistered label HasProperty = %v, %v, want false, nil", has, err)
			}

			// Registered label, no index yet: false.
			has, err = g.Index.HasProperty("Person", "name")
			if err != nil {
				t.Fatalf("HasProperty before create: %v", err)
			}
			if has {
				t.Fatal("HasProperty = true before CreateProperty")
			}

			if err := g.Index.CreateProperty("Person", "name"); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}
			has, err = g.Index.HasProperty("Person", "name")
			if err != nil {
				t.Fatalf("HasProperty after create: %v", err)
			}
			if !has {
				t.Fatal("HasProperty = false after CreateProperty")
			}

			// A different key on the same label must not be conflated.
			has, err = g.Index.HasProperty("Person", "age")
			if err != nil || has {
				t.Fatalf("HasProperty(different key) = %v, %v, want false, nil", has, err)
			}

			if err := g.Index.DeleteProperty("Person", "name"); err != nil {
				t.Fatalf("DeleteProperty: %v", err)
			}
			has, err = g.Index.HasProperty("Person", "name")
			if err != nil {
				t.Fatalf("HasProperty after drop: %v", err)
			}
			if has {
				t.Fatal("HasProperty = true after DeleteProperty")
			}
		})
	}
}

func TestHasProperty_BadgerReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.Index.CreateProperty("Person", "name"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	has, err := g2.Index.HasProperty("Person", "name")
	if err != nil {
		t.Fatalf("HasProperty after reopen: %v", err)
	}
	if !has {
		t.Fatal("property index definition lost across reopen")
	}
}

func TestHasProperty_TieredReferenceOnly(t *testing.T) {
	t.Parallel()
	g := newTieredTestCore(t)
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("seed Ref: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"name": "e1"}); err != nil {
		t.Fatalf("seed Ev: %v", err)
	}

	if err := g.Index.CreateProperty("Ref", "name"); err != nil {
		t.Fatalf("CreateProperty(Ref): %v", err)
	}
	has, err := g.Index.HasProperty("Ref", "name")
	if err != nil {
		t.Fatalf("HasProperty(Ref): %v", err)
	}
	if !has {
		t.Fatal("HasProperty(Ref) = false after CreateProperty on reference label")
	}

	// Event labels never carry a property index on tiered.
	has, err = g.Index.HasProperty("Ev", "name")
	if err != nil {
		t.Fatalf("HasProperty(Ev): %v", err)
	}
	if has {
		t.Fatal("HasProperty(Ev) = true, want false (event labels never indexed on tiered)")
	}
}

func TestHasTemporal_Lifecycle(t *testing.T) {
	t.Parallel()
	backends := introspectionBackends()
	backends = append(backends, introspectionBackend{name: "tiered", newCore: newTieredTestCore})
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			label := "Ref"
			if be.name != "tiered" {
				label = "Person"
			}
			if _, err := g.Nodes.Add(ctx, []string{label}, nil); err != nil {
				t.Fatalf("seed: %v", err)
			}

			has, err := g.Index.HasTemporal("NoSuchLabel")
			if err != nil || has {
				t.Fatalf("unregistered label HasTemporal = %v, %v, want false, nil", has, err)
			}

			has, err = g.Index.HasTemporal(label)
			if err != nil {
				t.Fatalf("HasTemporal before create: %v", err)
			}
			if has {
				t.Fatal("HasTemporal = true before CreateTemporal")
			}

			if err := g.Index.CreateTemporal(label); err != nil {
				t.Fatalf("CreateTemporal: %v", err)
			}
			has, err = g.Index.HasTemporal(label)
			if err != nil {
				t.Fatalf("HasTemporal after create: %v", err)
			}
			if !has {
				t.Fatal("HasTemporal = false after CreateTemporal")
			}

			// A high-frequency index on the SAME label is a different kind — must
			// not be conflated with the interval index this door reports.
			other := label + "2"
			if _, err := g.Nodes.Add(ctx, []string{other}, nil); err != nil {
				t.Fatalf("seed other: %v", err)
			}
			if err := g.Index.CreateHighFrequency(other, time.Hour); err != nil {
				t.Fatalf("CreateHighFrequency: %v", err)
			}
			has, err = g.Index.HasTemporal(other)
			if err != nil {
				t.Fatalf("HasTemporal(high-frequency label): %v", err)
			}
			if has {
				t.Fatal("HasTemporal must not report true for a high-frequency-only label")
			}

			if err := g.Index.DeleteTemporal(label); err != nil {
				t.Fatalf("DeleteTemporal: %v", err)
			}
			has, err = g.Index.HasTemporal(label)
			if err != nil {
				t.Fatalf("HasTemporal after drop: %v", err)
			}
			if has {
				t.Fatal("HasTemporal = true after DeleteTemporal")
			}
		})
	}
}

func TestVectorIndexInfo_Lifecycle(t *testing.T) {
	t.Parallel()
	backends := introspectionBackends()
	backends = append(backends, introspectionBackend{name: "tiered", newCore: newTieredTestCore})
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			label := "Ref"
			if be.name != "tiered" {
				label = "Doc"
			}
			if _, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"embedding": []float32{1, 2, 3}}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			info, found, err := g.Index.VectorIndexInfo("NoSuchLabel", "embedding")
			if err != nil || found {
				t.Fatalf("unregistered label VectorIndexInfo = %+v, %v, %v, want zero, false, nil", info, found, err)
			}

			info, found, err = g.Index.VectorIndexInfo(label, "embedding")
			if err != nil {
				t.Fatalf("VectorIndexInfo before create: %v", err)
			}
			if found {
				t.Fatal("VectorIndexInfo found = true before CreateVectorWithOptions")
			}

			opts := storepkg.VectorIndexOptions{UseBruteForce: true, M: 8, EfConstruction: 100, EfSearch: 32}
			if err := g.Index.CreateVectorWithOptions(label, "embedding", 3, storepkg.DistanceCosine, opts); err != nil {
				t.Fatalf("CreateVectorWithOptions: %v", err)
			}
			info, found, err = g.Index.VectorIndexInfo(label, "embedding")
			if err != nil {
				t.Fatalf("VectorIndexInfo after create: %v", err)
			}
			if !found {
				t.Fatal("VectorIndexInfo found = false after CreateVectorWithOptions")
			}
			want := storepkg.VectorIndexInfo{Dims: 3, Metric: storepkg.DistanceCosine, Options: opts}
			if info != want {
				t.Fatalf("VectorIndexInfo = %+v, want %+v", info, want)
			}

			if err := g.Index.DeleteVector(label, "embedding"); err != nil {
				t.Fatalf("DeleteVector: %v", err)
			}
			_, found, err = g.Index.VectorIndexInfo(label, "embedding")
			if err != nil {
				t.Fatalf("VectorIndexInfo after drop: %v", err)
			}
			if found {
				t.Fatal("VectorIndexInfo found = true after DeleteVector")
			}
		})
	}
}

// TestVectorIndexInfo_PlainCreateVectorReportsZeroSentinel documents that the
// plain CreateVector door (no explicit VectorIndexOptions) reports a
// zero-value Options — 0 is the DECLARED-config sentinel meaning "use the
// engine's documented default" (M=16, EfConstruction=200, EfSearch=64 for
// HNSW), resolved lazily at graph-build time rather than eagerly stamped
// into the stored definition (see indexpkg.ValidateVectorIndexOptions: "zero
// selects the documented default"). VectorIndexInfo intentionally reports
// the DECLARED value, not the resolved one.
func TestVectorIndexInfo_PlainCreateVectorReportsZeroSentinel(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 2, 3}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}
	info, found, err := g.Index.VectorIndexInfo("Doc", "embedding")
	if err != nil || !found {
		t.Fatalf("VectorIndexInfo = %+v, %v, %v", info, found, err)
	}
	want := storepkg.VectorIndexInfo{Dims: 3, Metric: storepkg.DistanceCosine}
	if info != want {
		t.Fatalf("VectorIndexInfo = %+v, want %+v (zero-value Options sentinel)", info, want)
	}
}

func TestHasRelProperty_Lifecycle(t *testing.T) {
	t.Parallel()
	for _, be := range introspectionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			a, b := addTwoNodes(t, g)
			ctx := context.Background()
			if _, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"weight": int64(5)}); err != nil {
				t.Fatalf("seed rel: %v", err)
			}

			has, err := g.Index.HasRelProperty("NEVER", "weight")
			if err != nil || has {
				t.Fatalf("unregistered type HasRelProperty = %v, %v, want false, nil", has, err)
			}

			has, err = g.Index.HasRelProperty("KNOWS", "weight")
			if err != nil {
				t.Fatalf("HasRelProperty before create: %v", err)
			}
			if has {
				t.Fatal("HasRelProperty = true before CreateRelProperty")
			}

			if err := g.Index.CreateRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			has, err = g.Index.HasRelProperty("KNOWS", "weight")
			if err != nil {
				t.Fatalf("HasRelProperty after create: %v", err)
			}
			if !has {
				t.Fatal("HasRelProperty = false after CreateRelProperty")
			}

			if err := g.Index.DeleteRelProperty("KNOWS", "weight"); err != nil {
				t.Fatalf("DeleteRelProperty: %v", err)
			}
			has, err = g.Index.HasRelProperty("KNOWS", "weight")
			if err != nil {
				t.Fatalf("HasRelProperty after drop: %v", err)
			}
			if has {
				t.Fatal("HasRelProperty = true after DeleteRelProperty")
			}
		})
	}
}

// Tiered declines rel-property indexes entirely (CreateRelProperty always
// returns ErrRelPropertyIndexUnsupported) — HasRelProperty must surface the
// SAME capability-absent signal, not silently answer false.
func TestHasRelProperty_TieredCapabilityNotSupported(t *testing.T) {
	t.Parallel()
	g := newTieredTestCore(t)
	if _, err := g.Index.HasRelProperty("KNOWS", "weight"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("tiered HasRelProperty err = %v, want ErrCapabilityNotSupported", err)
	}
}

// The introspection capabilities are optional: a backend satisfying only
// MandatoryStore must surface ErrCapabilityNotSupported on every new door,
// mirroring TestCapability_PropertyIndex_AbsentOnMandatoryOnlyBackend et al.
func TestIndexIntrospection_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := g.Index.HasProperty("Person", "name"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("HasProperty err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Index.HasTemporal("Person"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("HasTemporal err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, _, err := g.Index.VectorIndexInfo("Person", "embedding"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("VectorIndexInfo err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Index.HasRelProperty("KNOWS", "weight"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("HasRelProperty err = %v, want ErrCapabilityNotSupported", err)
	}
}
