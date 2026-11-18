package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Constraint door-equivalence: ConstraintRelWithinEndpoints must reject the
// SAME violating relationship through every creation door — standalone Add,
// AddByID, AddByIDIfAbsent, batch Execute, and tx — with a classifiable
// constraint sentinel that also matches ErrTemporalConstraint, leaving no row
// and no leaked rel-type token behind.
// One door silently skipping the check is exactly the sibling-door failure the
// shared kernel was built to prevent; this pins the constraint ladder too.
func TestConstraintRelWithinEndpointsAllDoors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	newConstrainedGraph := func(t *testing.T) (*Core, *types.Node, *types.Node) {
		t.Helper()
		g, err := New(Config{})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { g.Close() })
		if err := g.Constraints.Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
			t.Fatalf("install constraint: %v", err)
		}
		start, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{
			"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(2000),
		})
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		end, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{
			"tkg_valid_from": types.Instant(1500), "tkg_valid_to": types.Instant(3000),
		})
		if err != nil {
			t.Fatalf("end: %v", err)
		}
		return g, start, end
	}

	// One violation per constraint clause, plus the only legal window.
	violations := map[string]map[string]any{
		// rel starts before BOTH endpoints exist (clauses 1+2).
		"before-both": {"tkg_valid_from": types.Instant(500), "tkg_valid_to": types.Instant(1900)},
		// rel starts inside start's window but before end's (clause 2).
		"before-end": {"tkg_valid_from": types.Instant(1200), "tkg_valid_to": types.Instant(1900)},
		// start has finite ValidTo ⇒ rel must close by then; open-ended rel
		// violates clause 5.
		"open-rel-bounded-endpoint": {"tkg_valid_from": types.Instant(1600)},
		// rel outlives start (clause 5).
		"outlives-start": {"tkg_valid_from": types.Instant(1600), "tkg_valid_to": types.Instant(2500)},
		// untimed rel starts now, after both endpoints expired: the ByID
		// doors must fetch the endpoints to see this.
		"after-expiry": nil,
	}
	valid := map[string]any{"tkg_valid_from": types.Instant(1600), "tkg_valid_to": types.Instant(1900)}

	constraintSentinels := []error{
		temporalpkg.ErrTemporalConstraint,
		temporalpkg.ErrRelBeforeStartNode,
		temporalpkg.ErrRelBeforeEndNode,
		temporalpkg.ErrRelExceedsStartNodeValidity,
		temporalpkg.ErrRelExceedsEndNodeValidity,
	}
	isConstraintErr := func(err error) bool {
		for _, s := range constraintSentinels {
			if errors.Is(err, s) {
				return true
			}
		}
		return false
	}

	type door struct {
		name   string
		create func(g *Core, start, end *types.Node, typeName string, props map[string]any) error
	}
	doors := []door{
		{"standalone-Add", func(g *Core, s, e *types.Node, ty string, p map[string]any) error {
			_, err := g.Rels.Add(ctx, ty, s, e, p)
			return err
		}},
		{"AddByID", func(g *Core, s, e *types.Node, ty string, p map[string]any) error {
			_, err := g.Rels.AddByID(ctx, ty, s.ID(), e.ID(), p)
			return err
		}},
		{"AddByIDIfAbsent", func(g *Core, s, e *types.Node, ty string, p map[string]any) error {
			_, _, err := g.Rels.AddByIDIfAbsent(ctx, ty, s.ID(), e.ID(), p)
			return err
		}},
		{"tx", func(g *Core, s, e *types.Node, ty string, p map[string]any) error {
			tx, err := g.BeginTx()
			if err != nil {
				return err
			}
			_, err = tx.AddRelationship(ty, s, e, p)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		}},
		{"batch", func(g *Core, s, e *types.Node, ty string, p map[string]any) error {
			b, err := NewBatchBuilder(g)
			if err != nil {
				return err
			}
			if _, err := b.AddRelationship(ty, s, e, p); err != nil {
				return err
			}
			res, execErr := b.Execute()
			if execErr == nil {
				return nil
			}
			if res != nil && len(res.Errors) > 0 {
				return res.Errors[0].Err
			}
			return execErr
		}},
	}

	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			t.Parallel()
			g, start, end := newConstrainedGraph(t)

			for vname, props := range violations {
				typeName := fmt.Sprintf("V_%s_%s", d.name, vname)
				err := d.create(g, start, end, typeName, props)
				if err == nil {
					t.Errorf("%s/%s: violating relationship was ACCEPTED", d.name, vname)
					continue
				}
				if !isConstraintErr(err) {
					t.Errorf("%s/%s: rejection not classifiable as a constraint violation: %v", d.name, vname, err)
				}
				// Every rejection also carries the parent sentinel, so a caller
				// can test one error for the whole constraint family.
				if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
					t.Errorf("%s/%s: rejection %v is not errors.Is(ErrTemporalConstraint)", d.name, vname, err)
				}
				// Fail-closed hygiene: the rejected create must not leak its
				// never-before-seen rel-type token.
				if tok, ok := g.relTypes.Lookup(typeName); ok {
					t.Errorf("%s/%s: rejected create leaked rel-type token %d", d.name, vname, tok)
				}
				// And no row: the start endpoint has no outgoing rel of any
				// type (token 0), since every create so far was rejected.
				rels, err := g.store.OutgoingRelationships(start.ID(), 0)
				if err != nil {
					t.Fatalf("%s/%s: OutgoingRelationships: %v", d.name, vname, err)
				}
				if len(rels) != 0 {
					t.Errorf("%s/%s: rejected create left %d row(s)", d.name, vname, len(rels))
				}
			}

			// The legal window must pass through the same door.
			if err := d.create(g, start, end, "VALID_"+d.name, valid); err != nil {
				t.Errorf("%s: legal relationship rejected: %v", d.name, err)
			}
			// The row probe above sees a committed rel: exactly the legal one.
			if rels, err := g.store.OutgoingRelationships(start.ID(), 0); err != nil || len(rels) != 1 {
				t.Errorf("%s: after the legal create OutgoingRelationships = %d rels, %v; want 1", d.name, len(rels), err)
			}
		})
	}
}
