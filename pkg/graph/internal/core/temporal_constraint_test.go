package core

import (
	"context"
	"errors"
	"testing"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"

	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func addRelWithinEndpointsConstraint(t *testing.T, g *Core) {
	t.Helper()
	if err := g.Constraints.Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatalf("Constraints.Add: %v", err)
	}
}

// --- temporalpkg.ConstraintSet unit tests ---

func TestTemporalConstraintSet_Add_Len_Items(t *testing.T) {
	t.Parallel()

	cs := temporalpkg.NewConstraintSet()
	if cs.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", cs.Len())
	}
	if items := cs.Items(); items != nil {
		t.Fatalf("Items() = %v, want nil", items)
	}

	c1 := temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}
	cs2 := cs.Add(c1)

	// Original unchanged.
	if cs.Len() != 0 {
		t.Errorf("original Len() = %d after Add, want 0", cs.Len())
	}
	// New set has one item.
	if cs2.Len() != 1 {
		t.Errorf("cs2.Len() = %d, want 1", cs2.Len())
	}
	items := cs2.Items()
	if len(items) != 1 || items[0] != c1 {
		t.Errorf("cs2.Items() = %v, want [%v]", items, c1)
	}

	// Items returns a copy — mutation does not affect the set.
	items[0] = temporalpkg.TemporalConstraint{Kind: 99}
	if cs2.Items()[0] != c1 {
		t.Error("Items() returned a non-copy; modifying it changed the set")
	}
}

func TestNewConstraintSet(t *testing.T) {
	t.Parallel()

	c := temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}
	cs := temporalpkg.NewConstraintSet(c)
	if cs.Len() != 1 {
		t.Errorf("Len() = %d, want 1", cs.Len())
	}
}

func TestTemporalConstraintValidationRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	invalid := temporalpkg.TemporalConstraint{}
	if err := invalid.Validate(); !errors.Is(err, temporalpkg.ErrTemporalConstraint) || !errors.Is(err, temporalpkg.ErrInvalidTemporalConstraint) {
		t.Fatalf("TemporalConstraint.Validate() = %v, want temporal invalid constraint sentinels", err)
	}
	cs := temporalpkg.NewConstraintSet(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}, invalid)
	if err := cs.Validate(); !errors.Is(err, temporalpkg.ErrTemporalConstraint) || !errors.Is(err, temporalpkg.ErrInvalidTemporalConstraint) {
		t.Fatalf("ConstraintSet.Validate() = %v, want temporal invalid constraint sentinels", err)
	}
}

// --- TestSetTemporalConstraints_Replace ---

func TestSetTemporalConstraints_Replace(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	c := temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}
	if err := g.Constraints.Add(c); err != nil {
		t.Fatalf("AddTemporalConstraint: %v", err)
	}
	if g.Constraints.Get().Len() != 1 {
		t.Fatalf("Len() = %d after AddTemporalConstraint, want 1", g.Constraints.Get().Len())
	}

	// Replace with a set of two constraints.
	cs := temporalpkg.NewConstraintSet(c, c)
	if err := g.Constraints.Set(cs); err != nil {
		t.Fatalf("SetTemporalConstraints(2): %v", err)
	}
	if g.Constraints.Get().Len() != 2 {
		t.Errorf("Len() = %d after SetTemporalConstraints(2), want 2", g.Constraints.Get().Len())
	}

	// Replace with empty — clears all constraints.
	if err := g.Constraints.Set(temporalpkg.ConstraintSet{}); err != nil {
		t.Fatalf("SetTemporalConstraints(empty): %v", err)
	}
	if g.Constraints.Get().Len() != 0 {
		t.Errorf("Len() = %d after SetTemporalConstraints(empty), want 0", g.Constraints.Get().Len())
	}
}

func TestConstraintOpsNilReceiversReturnErrNilGraphOrZero(t *testing.T) {
	t.Parallel()

	valid := temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}
	for _, tc := range []struct {
		name string
		ops  *ConstraintOps
	}{
		{name: "nil", ops: nil},
		{name: "zero", ops: &ConstraintOps{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.ops.Add(valid); !errors.Is(err, ErrNilGraph) {
				t.Fatalf("Add = %v, want ErrNilGraph", err)
			}
			if err := tc.ops.Set(temporalpkg.NewConstraintSet(valid)); !errors.Is(err, ErrNilGraph) {
				t.Fatalf("Set = %v, want ErrNilGraph", err)
			}
			if got := tc.ops.Get().Len(); got != 0 {
				t.Fatalf("Get().Len = %d, want 0", got)
			}
		})
	}
}

func TestTemporalConstraintUnknownKindRejectedAtRegistration(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if err := g.Constraints.Add(temporalpkg.TemporalConstraint{}); !errors.Is(err, temporalpkg.ErrTemporalConstraint) || !errors.Is(err, temporalpkg.ErrInvalidTemporalConstraint) {
		t.Fatalf("Constraints.Add(invalid) = %v, want temporal invalid constraint sentinels", err)
	}
	if got := g.Constraints.Get().Len(); got != 0 {
		t.Fatalf("Constraints.Get().Len after rejected Add = %d, want 0", got)
	}
	valid := temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}
	if err := g.Constraints.Set(temporalpkg.NewConstraintSet(valid, temporalpkg.TemporalConstraint{})); !errors.Is(err, temporalpkg.ErrTemporalConstraint) || !errors.Is(err, temporalpkg.ErrInvalidTemporalConstraint) {
		t.Fatalf("Constraints.Set(invalid) = %v, want temporal invalid constraint sentinels", err)
	}
	if got := g.Constraints.Get().Len(); got != 0 {
		t.Fatalf("Constraints.Get().Len after rejected Set = %d, want 0", got)
	}

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err != nil {
		t.Fatalf("relationship write after rejected invalid constraint = %v, want nil", err)
	}
	count, err := g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("relationship count after accepted write = %d, want 1", count)
	}
}

func TestTemporalConstraintRejectedRelationshipDoesNotRegisterRelType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*Core, *types.Node, *types.Node, string) error
	}{
		{
			name: "Add",
			run: func(g *Core, a, b *types.Node, typ string) error {
				_, err := g.Rels.Add(context.Background(), typ, a, b, nil)
				return err
			},
		},
		{
			name: "AddByID",
			run: func(g *Core, a, b *types.Node, typ string) error {
				_, err := g.Rels.AddByID(context.Background(), typ, a.ID(), b.ID(), nil)
				return err
			},
		},
		{
			name: "AddByIDIfAbsent",
			run: func(g *Core, a, b *types.Node, typ string) error {
				_, _, err := g.Rels.AddByIDIfAbsent(context.Background(), typ, a.ID(), b.ID(), nil)
				return err
			},
		},
		{
			name: "Import",
			run: func(g *Core, a, b *types.Node, typ string) error {
				_, err := g.Rels.Import(context.Background(), g.nextRelID(), typ, a, b, nil)
				return err
			},
		},
		{
			name: "BatchExecute",
			run: func(g *Core, a, b *types.Node, typ string) error {
				bb, err := NewBatchBuilder(g)
				if err != nil {
					return err
				}
				if _, err := bb.AddRelationship(typ, a, b, nil); err != nil {
					return err
				}
				result, err := bb.Execute()
				if err == nil {
					return nil
				}
				if result != nil && len(result.Errors) > 0 {
					return result.Errors[0].Err
				}
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := newTestGraph(t)
			addRelWithinEndpointsConstraint(t, g)

			future := int64(1 << 60)
			a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, map[string]any{"tkg_valid_from": future})
			if err != nil {
				t.Fatalf("AddNode A: %v", err)
			}
			b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, map[string]any{"tkg_valid_from": future})
			if err != nil {
				t.Fatalf("AddNode B: %v", err)
			}

			typ := "REJECTED_" + tc.name
			err = tc.run(g, a, b, typ)
			if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
				t.Fatalf("%s error = %v, want ErrTemporalConstraint", tc.name, err)
			}
			if _, ok := g.Resolve.LookupRelType(typ); ok {
				t.Fatalf("%s registered rejected relationship type %q", tc.name, typ)
			}
		})
	}
}

// --- Graph-level constraint tests ---

func TestConstraintRelWithinEndpoints_NoConstraint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	// No constraint added.

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Even with an expired start node, no constraint means no error.
	a.SetTemporal(&types.TemporalMetadata{ValidTo: 1})

	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err != nil {
		t.Errorf("unexpected error without constraints: %v", err)
	}
}

func TestConstraintRelWithinEndpoints_Valid(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	// Open-ended nodes, open-ended rel — always valid.
	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err != nil {
		t.Errorf("AddRelationship with open-ended nodes: %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelBeforeStartNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	// Set start node's ValidFrom to the far future at creation time so the
	// store-side state seen by the constraint check (R4-F5) reflects it.
	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(9_999_999_999_999),
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected temporalpkg.ErrRelBeforeStartNode, got nil")
	}
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelBeforeStartNode) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelBeforeStartNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelBeforeEndNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// End node valid from the far future — persisted via shadow props so the
	// store-side state seen by the constraint check (R4-F5) reflects it.
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(9_999_999_999_999),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected temporalpkg.ErrRelBeforeEndNode, got nil")
	}
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelBeforeEndNode) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelBeforeEndNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelAfterStartNodeExpiry(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	// Start node has a deterministic expired interval — persisted via shadow
	// props (R4-F5). ValidFrom must be explicit; otherwise the effective start
	// comes from the snowflake timestamp and can classify the relationship as
	// before-validity instead of after-expiry under tight timing.
	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(1),
		"tkg_valid_to":   types.Instant(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected temporalpkg.ErrRelAfterStartNode, got nil")
	}
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelAfterStartNode) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelAfterStartNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelAfterEndNodeExpiry(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// End node has a deterministic expired interval — persisted via shadow
	// props (R4-F5).
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(1),
		"tkg_valid_to":   types.Instant(2),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected temporalpkg.ErrRelAfterEndNode, got nil")
	}
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelAfterEndNode) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelAfterEndNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelExceedsNodeValidity(t *testing.T) {
	// Checks (5)/(6): rel.ValidTo > node.ValidTo (both explicit).
	// Since AddRelationship doesn't set rel temporal, test via the internal method directly.
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Derive the rel's effective from time so we can set node ranges around it.
	relID := g.Rels.NextID()
	relFrom := storeutil.EntityValidFrom(relID.SnowflakeID(), nil)

	// Both endpoint nodes: valid from before rel, expires after rel.
	a.SetTemporal(&types.TemporalMetadata{
		ValidFrom: relFrom - 1000,
		ValidTo:   relFrom + 500, // node expires at relFrom+500
	})
	b.SetTemporal(&types.TemporalMetadata{
		ValidFrom: relFrom - 1000,
		ValidTo:   relFrom + 500,
	})

	// Manually create a relationship with ValidTo > node.ValidTo.
	rtok, _ := g.relTypes.GetOrCreate("LINK")
	startID := a.ID()
	endID := b.ID()
	r := types.NewRelationship(relID, rtok, startID, endID)
	r.SetTemporal(&types.TemporalMetadata{ValidTo: relFrom + 1000}) // exceeds node ValidTo of relFrom+500

	err = g.checkTemporalConstraints(r, a, b)
	if err == nil {
		t.Fatal("expected temporalpkg.ErrRelExceedsStartNodeValidity, got nil")
	}
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_OpenEndedRelExceedsFiniteEndpoint(t *testing.T) {
	t.Run("start endpoint", func(t *testing.T) {
		t.Parallel()
		g := newTestGraph(t)
		addRelWithinEndpointsConstraint(t, g)

		finiteFuture := types.Instant(1 << 60)
		a, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
			"tkg_valid_from": types.Instant(1),
			"tkg_valid_to":   finiteFuture,
		})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
		if err == nil {
			t.Fatal("expected open-ended relationship to exceed finite start endpoint, got nil")
		}
		if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
			t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
		}
		if !errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) {
			t.Errorf("errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) = false; err = %v", err)
		}
	})

	t.Run("end endpoint", func(t *testing.T) {
		t.Parallel()
		g := newTestGraph(t)
		addRelWithinEndpointsConstraint(t, g)

		a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		finiteFuture := types.Instant(1 << 60)
		b, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
			"tkg_valid_from": types.Instant(1),
			"tkg_valid_to":   finiteFuture,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = g.Rels.AddByID(context.Background(), "LINK", a.ID(), b.ID(), nil)
		if err == nil {
			t.Fatal("expected open-ended relationship to exceed finite end endpoint, got nil")
		}
		if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
			t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
		}
		if !errors.Is(err, temporalpkg.ErrRelExceedsEndNodeValidity) {
			t.Errorf("errors.Is(err, temporalpkg.ErrRelExceedsEndNodeValidity) = false; err = %v", err)
		}
	})
}

func TestConstraintRelWithinEndpoints_NodeClosePreservesExistingRelationships(t *testing.T) {
	t.Run("start endpoint rejects open relationship", func(t *testing.T) {
		t.Parallel()
		g := newTestGraph(t)
		addRelWithinEndpointsConstraint(t, g)

		a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Add(context.Background(), "LINK", a, b, nil); err != nil {
			t.Fatal(err)
		}

		closeAt := g.nodeValidFrom(a) + 10_000
		err = g.Nodes.CloseVersion(a.ID(), closeAt)
		if err == nil {
			t.Fatal("expected node close to reject open relationship, got nil")
		}
		if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
			t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
		}
		if !errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) {
			t.Errorf("errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) = false; err = %v", err)
		}
		loaded, getErr := g.Nodes.Get(context.Background(), a.ID())
		if getErr != nil {
			t.Fatal(getErr)
		}
		if tm := loaded.Temporal(); tm != nil && tm.ValidTo != 0 {
			t.Fatalf("node ValidTo changed after rejected constrained close: %d", tm.ValidTo)
		}
	})

	t.Run("end endpoint rejects open relationship", func(t *testing.T) {
		t.Parallel()
		g := newTestGraph(t)
		addRelWithinEndpointsConstraint(t, g)

		a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Add(context.Background(), "LINK", a, b, nil); err != nil {
			t.Fatal(err)
		}

		closeAt := g.nodeValidFrom(b) + 10_000
		err = g.Nodes.CloseVersion(b.ID(), closeAt)
		if err == nil {
			t.Fatal("expected node close to reject open incoming relationship, got nil")
		}
		if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
			t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
		}
		if !errors.Is(err, temporalpkg.ErrRelExceedsEndNodeValidity) {
			t.Errorf("errors.Is(err, temporalpkg.ErrRelExceedsEndNodeValidity) = false; err = %v", err)
		}
	})

	t.Run("accepts contained relationship", func(t *testing.T) {
		t.Parallel()
		g := newTestGraph(t)
		addRelWithinEndpointsConstraint(t, g)

		a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		closeAt := types.Instant(1 << 60)
		if _, err := g.Rels.Add(context.Background(), "LINK", a, b, map[string]any{"tkg_valid_to": closeAt - 1}); err != nil {
			t.Fatal(err)
		}

		if err := g.Nodes.CloseVersion(a.ID(), closeAt); err != nil {
			t.Fatalf("CloseVersion with contained relationship: %v", err)
		}
		loaded, getErr := g.Nodes.Get(context.Background(), a.ID())
		if getErr != nil {
			t.Fatal(getErr)
		}
		if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeAt {
			t.Fatalf("node ValidTo after accepted close = %v, want %d", tm, closeAt)
		}
	})
}

func TestConstraintRelWithinEndpoints_RelClosePreservesEndpointBounds(t *testing.T) {
	t.Run("rejects close after finite endpoint", func(t *testing.T) {
		t.Parallel()
		g := newTestGraph(t)

		nodeTo := types.Instant(1 << 60)
		a, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
			"tkg_valid_from": types.Instant(1),
			"tkg_valid_to":   nodeTo,
		})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add(context.Background(), "LINK", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}
		addRelWithinEndpointsConstraint(t, g)

		err = g.Rels.CloseVersion(r.ID(), nodeTo+1)
		if err == nil {
			t.Fatal("expected relationship close to reject endpoint overrun, got nil")
		}
		if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
			t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
		}
		if !errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) {
			t.Errorf("errors.Is(err, temporalpkg.ErrRelExceedsStartNodeValidity) = false; err = %v", err)
		}
		loaded, getErr := g.Rels.Get(context.Background(), r.ID())
		if getErr != nil {
			t.Fatal(getErr)
		}
		if tm := loaded.Temporal(); tm != nil && tm.ValidTo != 0 {
			t.Fatalf("relationship ValidTo changed after rejected constrained close: %d", tm.ValidTo)
		}
	})

	t.Run("accepts close within finite endpoint", func(t *testing.T) {
		t.Parallel()
		g := newTestGraph(t)

		nodeTo := types.Instant(1 << 60)
		a, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
			"tkg_valid_from": types.Instant(1),
			"tkg_valid_to":   nodeTo,
		})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add(context.Background(), "LINK", a, b, nil)
		if err != nil {
			t.Fatal(err)
		}
		addRelWithinEndpointsConstraint(t, g)

		closeAt := nodeTo - 1
		if err := g.Rels.CloseVersion(r.ID(), closeAt); err != nil {
			t.Fatalf("CloseVersion within endpoint bounds: %v", err)
		}
		loaded, getErr := g.Rels.Get(context.Background(), r.ID())
		if getErr != nil {
			t.Fatal(getErr)
		}
		if tm := loaded.Temporal(); tm == nil || tm.ValidTo != closeAt {
			t.Fatalf("relationship ValidTo after accepted close = %v, want %d", tm, closeAt)
		}
	})
}

func TestConstraintRelWithinEndpoints_RelExceedsEndNodeValidity(t *testing.T) {
	// rel.ValidTo > endNode.ValidTo — start node has no ValidTo but end node does.
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	relID := g.Rels.NextID()
	relFrom := storeutil.EntityValidFrom(relID.SnowflakeID(), nil)

	// Start node open-ended, end node expires at relFrom+500.
	// Start node: no ValidTo — checks (5) won't fire.
	// End node: ValidTo = relFrom+500 — check (6) fires.
	b.SetTemporal(&types.TemporalMetadata{
		ValidFrom: relFrom - 1000,
		ValidTo:   relFrom + 500,
	})

	rtok, _ := g.relTypes.GetOrCreate("LINK")
	startID := a.ID()
	endID := b.ID()
	r := types.NewRelationship(relID, rtok, startID, endID)
	r.SetTemporal(&types.TemporalMetadata{ValidTo: relFrom + 1000}) // exceeds endNode.ValidTo

	err = g.checkTemporalConstraints(r, a, b)
	if err == nil {
		t.Fatal("expected temporalpkg.ErrRelExceedsEndNodeValidity, got nil")
	}
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelExceedsEndNodeValidity) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelExceedsEndNodeValidity) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_ErrorsIs(t *testing.T) {
	// errors.Is must work on both the outer sentinel and specific leaf error.
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	// Start node has a deterministic expired interval — persisted via shadow
	// props (R4-F5).
	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(1),
		"tkg_valid_to":   types.Instant(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected constraint error")
	}

	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelAfterStartNode) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelAfterStartNode) = false; err = %v", err)
	}
}

// TestConstraintRelWithinEndpoints_ImportRel ensures the constraint hook also fires
// in ImportRelationshipWithID.
func TestConstraintRelWithinEndpoints_ImportRel(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// End node has a deterministic expired interval — persisted via shadow
	// props (R4-F5).
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(1),
		"tkg_valid_to":   types.Instant(2),
	})
	if err != nil {
		t.Fatal(err)
	}

	relID := g.Rels.NextID()
	_, err = g.Rels.Import(context.Background(), relID, "LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected temporalpkg.ErrRelAfterEndNode from ImportRelationshipWithID, got nil")
	}
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, temporalpkg.ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, temporalpkg.ErrRelAfterEndNode) {
		t.Errorf("errors.Is(err, temporalpkg.ErrRelAfterEndNode) = false; err = %v", err)
	}
}
