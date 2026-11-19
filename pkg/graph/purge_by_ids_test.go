package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPurgeByIDsCtx_PagesLeaveWithTheirRAM (PURGEIDS): on a tiered graph, 300
// reference-label nodes (Sigma's page blocks), each with a history version and
// an edge to a session node, are purged by ID: a meter with room for 10
// candidates refuses before any work and removes nothing; then two calls (256,
// then the rest by the cursor) remove every node, edge and history row, and the
// reference shard's history presence and stamps sidecars keep no entry of them.
func TestPurgeByIDsCtx_PagesLeaveWithTheirRAM(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Page", "Session"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, Store: ts, AllowRetentionPurge: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	session, err := g.Nodes().Add(ctx, []string{"Session"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 300
	ids := make([]types.NodeID, 0, n)
	var rel0 types.RelID
	for i := range n {
		p, err := g.Nodes().Add(ctx, []string{"Page"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Update(ctx, p.ID(), map[string]any{"seq": int64(i + n)}); err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().AddByID(ctx, "OF", p.ID(), session.ID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(1)}); err != nil {
			t.Fatal(err)
		}
		rel0 = r.ID()
		ids = append(ids, p.ID())
	}
	ref := ts.RefShardForTest()
	_, _ = ref.HasNodeHistory(ids[0])
	_, _, _, _ = ref.NodeHistoryStamps(ids[0])
	_, _ = ref.HasRelHistory(rel0)
	_, _, _, _ = ref.RelHistoryStamps(rel0)
	if p, s := ref.HistoryPresenceStats(), ref.HistoryStampsStats(); p.NodeIDs < n || p.RelIDs != n || s.NodeIDs < n || s.RelIDs != n {
		t.Fatalf("before: presence %+v stamps %+v, want at least %d each", p, s, n)
	}

	mctx, m := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 10})
	if _, err := g.Nodes().PurgeByIDsCtx(mctx, ids); !errors.Is(err, store.ErrReadBudget) {
		t.Fatalf("10-candidate meter: err %v, want a read budget refusal", err)
	}
	if c, _ := g.Nodes().CountByLabel("Page"); c != n || m.Work().Candidates != store.PurgeByIDsChunk {
		t.Fatalf("refused purge: %d pages left, %d candidates charged; want %d and %d", c, m.Work().Candidates, n, store.PurgeByIDsChunk)
	}

	mctx, m = store.WithReadBudget(ctx, store.ReadBudget{})
	purged, calls := 0, 0
	for rest := ids; len(rest) > 0; calls++ {
		res, err := g.Nodes().PurgeByIDsCtx(mctx, rest)
		if err != nil {
			t.Fatal(err)
		}
		if want := min(len(rest), store.PurgeByIDsChunk); res.NodesPurged != want || res.RelsPurged != want || res.More != (len(rest) > want) {
			t.Fatalf("call %d: %+v, want %d nodes and edges, More %v", calls, res, want, len(rest) > want)
		}
		purged += res.NodesPurged
		rest = rest[min(len(rest), store.PurgeByIDsChunk):]
	}
	if purged != n || calls != 2 || m.Work().Candidates != n {
		t.Fatalf("purged %d in %d calls, %d candidates charged; want %d in 2, %d", purged, calls, m.Work().Candidates, n, n)
	}
	if c, _ := g.Nodes().CountByLabel("Page"); c != 0 {
		t.Fatalf("%d pages left", c)
	}
	if h, _ := g.Nodes().History(ids[0]); len(h) != 0 {
		t.Fatalf("purged page keeps %d history rows", len(h))
	}
	if in, _ := g.Rels().Incoming(session.ID(), ""); len(in) != 0 {
		t.Fatalf("session keeps %d incoming edges", len(in))
	}
	if p, s := ref.HistoryPresenceStats(), ref.HistoryStampsStats(); p.NodeIDs != 0 || p.RelIDs != 0 || s.NodeIDs != 0 || s.RelIDs != 0 {
		t.Fatalf("after: presence %+v stamps %+v, want no entry", p, s)
	}
}
