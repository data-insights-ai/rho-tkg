package core

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Lesson 62 regression — the fallback / memory-native as-of resolver must not
// fall through past a retracted newest belief to an older open-TxTo row.
//
// Repro (verified as a genuine memory-vs-badger divergence by the bitemporal
// oracle harness): an append-only cascade (SetNodeVersionInterval) appends the
// corrected version but leaves the superseded genesis row's TxTo == 0; a hard
// Delete tombstones only the FINAL (corrected) version. At a pin AT/AFTER the
// delete the chain-scanning resolvers (memory's native nodeAsOfLocked and the
// core fallback used by tiered) selected the still-open genesis and reported
// the entity PRESENT, while badger's native reverse-scan stopped at the
// tombstone and reported ABSENT. Badger is correct: as-of selection is the
// newest-TxFrom belief recorded by the pin, and if THAT belief is retracted the
// entity is absent — never resurrect an older open-TxTo row.
//
// Runs on all three backends (memory, badger, tiered — the latter two exercise
// the native reverse-scan and the core fallback respectively), node AND rel
// mirrors. Pins are derived from the entities' own TxFrom stamps so the checks
// are wall-clock-independent.

type asofBackend struct {
	name    string
	newCore func(t *testing.T) *Core
}

func asofCascadeBackends() []asofBackend {
	return []asofBackend{
		{"memory", func(t *testing.T) *Core {
			g, err := New(Config{})
			if err != nil {
				t.Fatalf("New(memory): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
		{"badger", func(t *testing.T) *Core {
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				t.Fatalf("New(badger): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
		{"tiered", func(t *testing.T) *Core {
			g, _ := newTestTieredGraph(t)
			return g
		}},
	}
}

// nodeInAsOf reports the version of id present in NodesAsOf(pin), or ok=false.
func nodeInAsOf(t *testing.T, g *Core, id types.NodeID, pin types.Instant) (*types.Node, bool) {
	t.Helper()
	rows, err := g.Temporal.NodesAsOf(pin)
	if err != nil {
		t.Fatalf("NodesAsOf(%d): %v", pin, err)
	}
	for _, n := range rows {
		if n.ID() == id {
			return n, true
		}
	}
	return nil, false
}

// relInAsOf reports the version of id present in RelsAsOf(pin), or ok=false.
func relInAsOf(t *testing.T, g *Core, id types.RelID, pin types.Instant) (*types.Relationship, bool) {
	t.Helper()
	rows, err := g.Temporal.RelsAsOf(pin)
	if err != nil {
		t.Fatalf("RelsAsOf(%d): %v", pin, err)
	}
	for _, r := range rows {
		if r.ID() == id {
			return r, true
		}
	}
	return nil, false
}

func TestAsOfCascadeThenDeleteNode(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()

			// Genesis: valid-from 1000, recorded at t1.
			n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{
				"tkg_valid_from": types.Instant(1000), "state": "orig",
			})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			id := n.ID()
			t1 := txFromStamp(t, n.Temporal())

			// Append-only cascade: corrected version valid-from 2000, open valid-to.
			corrected, err := g.Temporal.SetNodeVersionInterval(ctx, id, 2000, 0, map[string]any{"state": "fixed"})
			if err != nil {
				t.Fatalf("SetNodeVersionInterval: %v", err)
			}
			t2 := txFromStamp(t, corrected.Temporal())

			// Hard delete: tombstones the corrected (final) version at t3 > t2.
			if err := g.Nodes.Delete(ctx, id); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			after := farFuturePin()

			// (1) At/after the delete the entity is ABSENT — the retracted newest
			// belief must not fall through to the open-TxTo genesis.
			if got, ok := nodeInAsOf(t, g, id, after); ok {
				t.Fatalf("[%s] NodesAsOf(after delete) resurrected node v%d (valid_from=%d); want ABSENT",
					be.name, got.Version(), got.Temporal().ValidFrom)
			}
			if _, err := g.Temporal.NodeAsOf(id, after); !errors.Is(err, ErrNoVersionAsOf) {
				t.Fatalf("[%s] NodeAsOf(after delete) err = %v; want ErrNoVersionAsOf", be.name, err)
			}

			// (2) Pin BETWEEN the cascade and the delete: present in corrected shape.
			got, ok := nodeInAsOf(t, g, id, t2)
			if !ok {
				t.Fatalf("[%s] NodesAsOf(between cascade and delete) missing node; want present", be.name)
			}
			if got.Temporal().ValidFrom != 2000 {
				t.Fatalf("[%s] NodesAsOf(t2) valid_from = %d; want 2000 (corrected shape)", be.name, got.Temporal().ValidFrom)
			}
			single, err := g.Temporal.NodeAsOf(id, t2)
			if err != nil {
				t.Fatalf("[%s] NodeAsOf(t2): %v", be.name, err)
			}
			if single.Temporal().ValidFrom != 2000 {
				t.Fatalf("[%s] NodeAsOf(t2) valid_from = %d; want 2000", be.name, single.Temporal().ValidFrom)
			}

			// (3) Pin BEFORE the cascade: original belief (valid-from 1000).
			got, ok = nodeInAsOf(t, g, id, t1)
			if !ok {
				t.Fatalf("[%s] NodesAsOf(before cascade) missing node; want present", be.name)
			}
			if got.Temporal().ValidFrom != 1000 {
				t.Fatalf("[%s] NodesAsOf(t1) valid_from = %d; want 1000 (original shape)", be.name, got.Temporal().ValidFrom)
			}
			single, err = g.Temporal.NodeAsOf(id, t1)
			if err != nil {
				t.Fatalf("[%s] NodeAsOf(t1): %v", be.name, err)
			}
			if single.Temporal().ValidFrom != 1000 {
				t.Fatalf("[%s] NodeAsOf(t1) valid_from = %d; want 1000", be.name, single.Temporal().ValidFrom)
			}
		})
	}
}

func TestAsOfCascadeThenDeleteRel(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()

			// Ref-label endpoints so tiered keeps them on the reference shard.
			start, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"k": "s"})
			if err != nil {
				t.Fatalf("Add(start): %v", err)
			}
			end, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": "e"})
			if err != nil {
				t.Fatalf("Add(end): %v", err)
			}

			r, err := g.Rels.AddByID(ctx, "LINK", start.ID(), end.ID(), map[string]any{
				"tkg_valid_from": types.Instant(1000), "state": "orig",
			})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			id := r.ID()
			t1 := txFromStamp(t, r.Temporal())

			corrected, err := g.Temporal.SetRelVersionInterval(ctx, id, 2000, 0, map[string]any{"state": "fixed"})
			if err != nil {
				t.Fatalf("SetRelVersionInterval: %v", err)
			}
			t2 := txFromStamp(t, corrected.Temporal())

			if err := g.Rels.Delete(ctx, id); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			after := farFuturePin()

			// (1) At/after the delete: ABSENT.
			if got, ok := relInAsOf(t, g, id, after); ok {
				t.Fatalf("[%s] RelsAsOf(after delete) resurrected rel v%d (valid_from=%d); want ABSENT",
					be.name, got.Version(), got.Temporal().ValidFrom)
			}
			if _, err := g.Temporal.RelAsOf(id, after); !errors.Is(err, ErrNoVersionAsOf) {
				t.Fatalf("[%s] RelAsOf(after delete) err = %v; want ErrNoVersionAsOf", be.name, err)
			}

			// (2) Pin BETWEEN cascade and delete: corrected shape.
			got, ok := relInAsOf(t, g, id, t2)
			if !ok {
				t.Fatalf("[%s] RelsAsOf(between) missing rel; want present", be.name)
			}
			if got.Temporal().ValidFrom != 2000 {
				t.Fatalf("[%s] RelsAsOf(t2) valid_from = %d; want 2000", be.name, got.Temporal().ValidFrom)
			}
			single, err := g.Temporal.RelAsOf(id, t2)
			if err != nil {
				t.Fatalf("[%s] RelAsOf(t2): %v", be.name, err)
			}
			if single.Temporal().ValidFrom != 2000 {
				t.Fatalf("[%s] RelAsOf(t2) valid_from = %d; want 2000", be.name, single.Temporal().ValidFrom)
			}

			// (3) Pin BEFORE cascade: original belief.
			got, ok = relInAsOf(t, g, id, t1)
			if !ok {
				t.Fatalf("[%s] RelsAsOf(before) missing rel; want present", be.name)
			}
			if got.Temporal().ValidFrom != 1000 {
				t.Fatalf("[%s] RelsAsOf(t1) valid_from = %d; want 1000", be.name, got.Temporal().ValidFrom)
			}
			single, err = g.Temporal.RelAsOf(id, t1)
			if err != nil {
				t.Fatalf("[%s] RelAsOf(t1): %v", be.name, err)
			}
			if single.Temporal().ValidFrom != 1000 {
				t.Fatalf("[%s] RelAsOf(t1) valid_from = %d; want 1000", be.name, single.Temporal().ValidFrom)
			}
		})
	}
}
