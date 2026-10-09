package core

import (
	"context"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Item 36: the tkg_created_at fallback (no explicit CreatedAt) is the ID's
// mint instant, derived by the ONE shared function behind NodeID/RelID
// .MintInstant. A fallback with its own arithmetic (generator CreatedAt) agrees
// for generated IDs but returns the epoch / a pre-epoch instant for zero and
// negative IDs where MintInstant returns 0 (unset).

func TestShadowCreatedAtFallbackEqualsMintInstant_Node(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Guard (passes with the own-arithmetic fallback too): a generated ID.
	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.SetTemporal(&types.TemporalMetadata{CreatedAt: 0})
	if got, _ := g.Resolve.NodeProperty(n, types.ShadowCreatedAt); got != n.ID().MintInstant() {
		t.Fatalf("generated node: tkg_created_at = %v, want MintInstant %d", got, n.ID().MintInstant())
	}
	// Red against the own-arithmetic fallback: hand-built zero and negative IDs, with and without metadata.
	for _, raw := range []int64{0, -1, -(1 << 15), math.MinInt64} {
		for _, withMeta := range []bool{false, true} {
			h := types.NewNode(types.NodeID(raw), 1, nil)
			if withMeta {
				h.SetTemporal(&types.TemporalMetadata{CreatedAt: 0})
			}
			got, ok := g.Resolve.NodeProperty(h, types.ShadowCreatedAt)
			if !ok || got != types.Instant(0) {
				t.Fatalf("node id %d meta=%v: tkg_created_at = (%v, %v), want (0, true) == MintInstant %d", raw, withMeta, got, ok, h.ID().MintInstant())
			}
		}
	}
}

func TestShadowCreatedAtFallbackEqualsMintInstant_Rel(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, err := g.Rels.Add(context.Background(), "R", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.SetTemporal(&types.TemporalMetadata{CreatedAt: 0})
	if got, _ := g.Resolve.RelProperty(r, types.ShadowCreatedAt); got != r.ID().MintInstant() {
		t.Fatalf("generated rel: tkg_created_at = %v, want MintInstant %d", got, r.ID().MintInstant())
	}
	for _, raw := range []int64{0, -1, -(1 << 15), math.MinInt64} {
		for _, withMeta := range []bool{false, true} {
			h := types.NewRelationship(types.RelID(raw), 1, a.ID(), b.ID())
			if withMeta {
				h.SetTemporal(&types.TemporalMetadata{CreatedAt: 0})
			}
			got, ok := g.Resolve.RelProperty(h, types.ShadowCreatedAt)
			if !ok || got != types.Instant(0) {
				t.Fatalf("rel id %d meta=%v: tkg_created_at = (%v, %v), want (0, true) == MintInstant %d", raw, withMeta, got, ok, h.ID().MintInstant())
			}
		}
	}
}
