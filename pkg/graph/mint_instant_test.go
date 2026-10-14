package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Item 36: the public mint-instant derivation decodes the wall-clock instant
// the ID was minted at, for Add and AddWithTx, node and relationship, on every
// backend. Since backlog 62 it is no longer the resolver's start of a row
// without a valid-from (that is the row's TxFrom: TestUntimedRowsStartAtTxFrom),
// only of a legacy row without a TxFrom.

func mintGraphs() []storeBackend {
	return allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true })
}

func assertMintInWindow(t *testing.T, what string, mint types.Instant, lo, hi time.Time) {
	t.Helper()
	if mint < types.Instant(lo.UnixMilli()-1) || mint > types.Instant(hi.UnixMilli()+1) {
		t.Fatalf("%s: MintInstant %d outside the wall-clock window [%d, %d] (epoch or us/ms unit error)", what, mint, lo.UnixMilli(), hi.UnixMilli())
	}
}

func TestMintInstant_DecodesTheWallClockMint_AllBackends(t *testing.T) {
	ctx := context.Background()
	for _, b := range mintGraphs() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			lo := time.Now()
			plain, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"k": "plain"})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			peer, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"k": "peer"})
			if err != nil {
				t.Fatalf("Add peer: %v", err)
			}
			backfilled, err := g.Nodes().AddWithTx(ctx, []string{"Event"}, map[string]any{"k": "bf"}, 1000)
			if err != nil {
				t.Fatalf("AddWithTx: %v", err)
			}
			rel, err := g.Rels().Add(ctx, "LINKS", plain, peer, nil)
			if err != nil {
				t.Fatalf("Rels.Add: %v", err)
			}
			relBF, err := g.Rels().AddWithTx(ctx, "LINKS", peer, plain, nil, 1000)
			if err != nil {
				t.Fatalf("Rels.AddWithTx: %v", err)
			}
			hi := time.Now()
			for _, n := range []*types.Node{plain, peer, backfilled} {
				assertMintInWindow(t, "node", n.ID().MintInstant(), lo, hi)
			}
			for _, r := range []*types.Relationship{rel, relBF} {
				assertMintInWindow(t, "rel", r.ID().MintInstant(), lo, hi)
			}
			// Later IDs from the same generator never mint earlier (monotone).
			if peer.ID().MintInstant() < plain.ID().MintInstant() || backfilled.ID().MintInstant() < peer.ID().MintInstant() {
				t.Fatalf("mint instants not monotone in ID order: %d %d %d", plain.ID().MintInstant(), peer.ID().MintInstant(), backfilled.ID().MintInstant())
			}
		})
	}
}

// Every SnowflakeNodeID 0-15: nodes carry the even node field, relationships
// the odd one, and both mint the same instant as the public decomposition door
// (Admin().DecomposeNodeID/RelID) reports.
func TestMintInstant_EverySnowflakeNodeID_AgreesWithAdminDecompose(t *testing.T) {
	ctx := context.Background()
	for sn := int64(0); sn <= 15; sn++ {
		lo := time.Now()
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: sn})
		if err != nil {
			t.Fatalf("SnowflakeNodeID %d: New: %v", sn, err)
		}
		a, err := g.Nodes().Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		b, err := g.Nodes().Add(ctx, []string{"B"}, nil)
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		r, err := g.Rels().Add(ctx, "R", a, b, nil)
		if err != nil {
			t.Fatalf("Rels.Add: %v", err)
		}
		hi := time.Now()

		nc := g.Admin().DecomposeNodeID(a.ID())
		rc := g.Admin().DecomposeRelID(r.ID())
		if nc.NodeID != sn*2 || rc.NodeID != sn*2+1 {
			t.Fatalf("SnowflakeNodeID %d: node field node=%d rel=%d, want %d/%d", sn, nc.NodeID, rc.NodeID, sn*2, sn*2+1)
		}
		if got, want := a.ID().MintInstant(), types.Instant(nc.CreatedAt.UnixMilli()); got != want {
			t.Fatalf("SnowflakeNodeID %d: node MintInstant %d != Admin decompose %d", sn, got, want)
		}
		if got, want := r.ID().MintInstant(), types.Instant(rc.CreatedAt.UnixMilli()); got != want {
			t.Fatalf("SnowflakeNodeID %d: rel MintInstant %d != Admin decompose %d", sn, got, want)
		}
		for _, m := range []types.Instant{a.ID().MintInstant(), b.ID().MintInstant(), r.ID().MintInstant()} {
			if m < types.Instant(lo.UnixMilli()-1) || m > types.Instant(hi.UnixMilli()+1) {
				t.Fatalf("SnowflakeNodeID %d: mint %d outside wall-clock window [%d,%d]", sn, m, lo.UnixMilli(), hi.UnixMilli())
			}
		}
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}
