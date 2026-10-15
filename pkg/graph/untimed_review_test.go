package graph_test

// Backlog 73, review round: the fixes the Opus review of 75f86596 asked for,
// each a red test first.

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestUntimedUnboundedEndpointConstraint (review 1): an untimed relationship
// under UntimedUnbounded starts at -inf; an endpoint whose FIRST version is
// unbounded is valid there too, also after it was updated (its current row
// then starts at the update's TxFrom). Every relationship create door must
// accept it. Break case: an endpoint whose first version is timed still
// refuses the -inf relationship.
func TestUntimedUnboundedEndpointConstraint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backends := allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = graphpkg.UntimedUnbounded })
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			g := b.open(t)
			if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			c, err := g.Nodes().Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, c.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(ctx, "R", a, c, nil); err != nil {
				t.Errorf("Rels().Add untimed between updated unbounded endpoints: %v", err)
			}
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bb.AddRelationship("R", c, a, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); err != nil {
				t.Errorf("batch AddRelationship: %v", err)
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.AddRelationship("R", a, c, nil)
				return err
			}); err != nil {
				t.Errorf("tx AddRelationship: %v", err)
			}
			for _, conc := range []bool{false, true} {
				s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, Concurrent: conc})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.AddRelationship("R", a, c, nil); err != nil {
					t.Errorf("ingest AddRelationship (concurrent=%v): %v", conc, err)
				}
				if err := s.Close(); err != nil {
					t.Errorf("ingest close (concurrent=%v): %v", conc, err)
				}
			}
			viol, err := g.Constraints().DryRunValidate(ctx, constraintspkg.DryRunFacts{Rels: []constraintspkg.DryRunRel{
				{Ref: "untimed", TypeName: "R", StartID: a.ID(), EndID: c.ID()},
			}})
			if err != nil || len(viol) != 0 {
				t.Errorf("DryRunValidate untimed rel: %+v (%v), want no violation", viol, err)
			}

			// Break case: a first version that is timed bounds the -inf relationship.
			timed, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": types.Instant(1_000_000)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(ctx, "R", timed, c, nil); !errors.Is(err, temporalpkg.ErrRelBeforeStartNode) {
				t.Errorf("untimed rel from a timed endpoint: err = %v, want ErrRelBeforeStartNode", err)
			}
		})
	}
}
