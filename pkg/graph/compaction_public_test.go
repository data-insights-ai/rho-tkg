package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCompactHistory_PublicFacade exercises g.Admin().CompactHistoryNodes /
// CompactHistoryRels end-to-end through the exported façade: the report is
// returned, history is trimmed, VerifyChain stays valid, and a pin below the
// watermark surfaces graph.ErrHistoryCompacted (errors.Is at the public layer).
func TestCompactHistory_PublicFacade(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, BadgerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"state": "v0"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	id := n.ID()
	for i := 1; i <= 4; i++ {
		if _, err := g.Nodes().Update(ctx, id, map[string]any{"state": string(rune('0' + i))}); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	hist, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	lowPin := hist[0].Temporal().TxFrom // v0 knowledge — will be trimmed
	high, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	rep, err := g.Admin().CompactHistoryNodes(ctx, adminpkg.RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("CompactHistoryNodes: %v", err)
	}
	if rep.EntitiesCompacted != 1 || rep.VersionsTrimmed != 2 {
		t.Fatalf("report = %+v, want 1 entity / 2 versions", rep)
	}
	if got, err := g.Nodes().History(id); err != nil || len(got) != 2 {
		t.Fatalf("history len = %d (%v), want 2", len(got), err)
	}
	if ok, err := g.Hash().VerifyNodeChain(id); err != nil || !ok {
		t.Fatalf("VerifyNodeChain = (%v,%v), want (true,nil)", ok, err)
	}
	if _, err := g.Temporal().NodeAsOf(id, lowPin); !errors.Is(err, graphpkg.ErrHistoryCompacted) {
		t.Fatalf("NodeAsOf(low) err=%v, want ErrHistoryCompacted", err)
	}
	if _, err := g.Nodes().ByLabel("T", storepkg.QueryOpts{TxPin: lowPin}); !errors.Is(err, graphpkg.ErrHistoryCompacted) {
		t.Fatalf("ByLabel{TxPin=low} err=%v, want ErrHistoryCompacted", err)
	}
	if got, err := g.Temporal().NodeAsOf(id, high); err != nil {
		t.Fatalf("NodeAsOf(high): %v", err)
	} else if got.PropertiesMap()["state"].(string) != "4" {
		t.Fatalf("NodeAsOf(high) state=%v, want 4", got.PropertiesMap()["state"])
	}

	// Empty policy is rejected at the façade.
	if _, err := g.Admin().CompactHistoryRels(ctx, adminpkg.RetentionPolicy{}); !errors.Is(err, graphpkg.ErrInvalidRetentionPolicy) {
		t.Fatalf("empty policy err=%v, want ErrInvalidRetentionPolicy", err)
	}
}

// TestCompactHistory_NilGraphSafe confirms the nil-safe façade contract.
func TestCompactHistory_NilGraphSafe(t *testing.T) {
	t.Parallel()
	var g *graphpkg.Graph
	if _, err := g.Admin().CompactHistoryNodes(context.Background(), adminpkg.RetentionPolicy{KeepVersions: 1}); err == nil {
		t.Fatal("nil graph CompactHistoryNodes returned nil error")
	}
	if _, err := g.Admin().CompactHistoryRels(context.Background(), adminpkg.RetentionPolicy{KeepVersions: 1}); err == nil {
		t.Fatal("nil graph CompactHistoryRels returned nil error")
	}
	_ = types.NodeID(0)
}
