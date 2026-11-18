// Tests in this file pin that endpoint-hash capture holds on the ByID
// relationship-create doors, with and without constraints. The ByID variants
// accept endpoint IDs instead of node objects, but their relationship state
// must match Rels.Add. Constraint rejection through every door is pinned by
// TestConstraintRelWithinEndpointsAllDoors.
package core

import (
	"context"
	"testing"
)

// TestAddByID_CapturesEndpointHashes: ByID is only an input-shape variant.
// With and without configured constraints it fetches the live endpoints and
// records their integrity hashes on the relationship, exactly the hashes the
// endpoint nodes carry, so the relationship integrity shape matches Rels.Add.
func TestAddByID_CapturesEndpointHashes(t *testing.T) {
	t.Parallel()
	for _, constrained := range []bool{false, true} {
		g := newTestGraph(t)
		if constrained {
			addRelWithinEndpointsConstraint(t, g)
		}
		ctx := context.Background()
		a, err := g.Nodes.Add(ctx, []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(ctx, []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.AddByID(ctx, "LINK", a.ID(), b.ID(), nil)
		if err != nil {
			t.Fatalf("constrained=%v: AddByID: %v", constrained, err)
		}
		ig := r.Integrity()
		if ig == nil {
			t.Fatalf("constrained=%v: rel has nil Integrity", constrained)
		}
		wantFrom, wantTo := a.Integrity().Hash, b.Integrity().Hash
		if wantFrom == "" || wantTo == "" {
			t.Fatalf("fixture: endpoint hashes %q, %q", wantFrom, wantTo)
		}
		if ig.FromNodeHash != wantFrom || ig.ToNodeHash != wantTo {
			t.Errorf("constrained=%v: endpoint hashes From=%q To=%q; want %q, %q", constrained, ig.FromNodeHash, ig.ToNodeHash, wantFrom, wantTo)
		}
	}
}
