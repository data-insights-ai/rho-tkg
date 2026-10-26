package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestUniqueEventLabelStaleClaims pins UNIQIDX step (d): a claim in the owner
// index whose row no longer holds the value frees it again. After a value
// change, a delete, a label removal, a retention purge, a rolled-back
// transaction, or a claim written without its row (a crash between the two),
// another node may take the value, and lookups answer only the current holder.
// Catches an index that trusts its claims instead of the rows.
func TestUniqueEventLabelStaleClaims(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	open := func(t *testing.T) (*graphpkg.Graph, *tieredpkg.Store) {
		t.Helper()
		ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"Device"},
			ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts, AllowRetentionPurge: true})
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { g.Close() })
		if err := g.Constraints().CreateUnique(ctx, "Process", "key"); err != nil {
			t.Fatalf("CreateUnique: %v", err)
		}
		return g, ts
	}
	holder := func(t *testing.T, g *graphpkg.Graph, key string) []types.NodeID {
		t.Helper()
		got, err := g.Nodes().ByLabelAndProperty("Process", "key", key, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabelAndProperty: %v", err)
		}
		return nodeIDList(got)
	}
	add := func(t *testing.T, g *graphpkg.Graph, key string) types.NodeID {
		t.Helper()
		n, err := g.Nodes().Add(ctx, []string{"Process"}, map[string]any{"key": key})
		if err != nil {
			t.Fatalf("Add(%s): %v", key, err)
		}
		return n.ID()
	}
	takeOver := func(t *testing.T, g *graphpkg.Graph, key string) {
		t.Helper()
		b := add(t, g, key)
		if got := holder(t, g, key); len(got) != 1 || got[0] != b {
			t.Fatalf("holders of %s = %v, want only the new node %d", key, got, b)
		}
	}
	t.Run("value change", func(t *testing.T) {
		g, _ := open(t)
		a := add(t, g, "v")
		if _, err := g.Nodes().Update(ctx, a, map[string]any{"key": "w"}); err != nil {
			t.Fatal(err)
		}
		if got := holder(t, g, "w"); len(got) != 1 || got[0] != a {
			t.Fatalf("holders of w = %v, want %d", got, a)
		}
		takeOver(t, g, "v")
		if _, err := g.Nodes().Add(ctx, []string{"Process"}, map[string]any{"key": "w"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("a second w = %v, want ErrUniqueViolation", err)
		}
	})
	t.Run("delete", func(t *testing.T) {
		g, _ := open(t)
		a := add(t, g, "v")
		if err := g.Nodes().Delete(ctx, a); err != nil {
			t.Fatal(err)
		}
		takeOver(t, g, "v")
	})
	t.Run("label removed", func(t *testing.T) {
		g, _ := open(t)
		n, err := g.Nodes().Add(ctx, []string{"Process", "Seen"}, map[string]any{"key": "v"})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().RemoveLabel(ctx, n.ID(), "Process"); err != nil {
			t.Fatal(err)
		}
		takeOver(t, g, "v")
	})
	t.Run("retention purge", func(t *testing.T) {
		g, _ := open(t)
		add(t, g, "v")
		if err := g.Tier().ForceRotate(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		before := types.Instant(time.Now().UnixMilli())
		time.Sleep(5 * time.Millisecond)
		if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Process", Mode: adminpkg.PurgeByAge, Before: before}); err != nil {
			t.Fatalf("PurgeExpiredNodes: %v", err)
		}
		takeOver(t, g, "v")
	})
	t.Run("rollback", func(t *testing.T) {
		g, _ := open(t)
		var rolled types.NodeID
		err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			n, created, err := tx.GetOrCreateByKey("Process", "key", "v", nil)
			if err != nil || !created {
				t.Fatalf("tx GetOrCreateByKey: created %v, %v", created, err)
			}
			rolled = n.ID()
			return errors.New("roll back")
		})
		if err == nil {
			t.Fatal("the transaction did not roll back")
		}
		n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Process", "key", "v", nil)
		if err != nil || !created || n.ID() == rolled {
			t.Fatalf("GetOrCreateByKey after the rollback = node %v created %v (%v), want a new node", n, created, err)
		}
	})
	t.Run("claim without its row", func(t *testing.T) {
		g, ts := open(t)
		other, err := g.Nodes().Add(ctx, []string{"Process"}, map[string]any{"key": "other"})
		if err != nil {
			t.Fatal(err)
		}
		tok := other.LabelTokenRawAt(0)
		vk := types.CanonicalIndexKey(types.IndexablePropertyValueKey("v"))
		if err := ts.PutUniqueOwner(tok, "key", vk, types.NodeID(1<<40)); err != nil {
			t.Fatalf("PutUniqueOwner: %v", err)
		}
		if got := holder(t, g, "v"); len(got) != 0 {
			t.Fatalf("holders of v = %v, want none (the claim has no row)", got)
		}
		takeOver(t, g, "v")
	})
}
