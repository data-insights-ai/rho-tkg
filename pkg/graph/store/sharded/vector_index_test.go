package sharded_test

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// addVectorNodesAcrossShards places node i (i=1..n) with vec [i, 0] on a
// distinct slot via lane pinning, so a query at the origin has an unambiguous
// nearest order (node 1 closest). Returns i -> NodeID and the slots used.
func addVectorNodesAcrossShards(t *testing.T, g *graph.Graph, n int) (map[int]types.NodeID, map[int64]struct{}) {
	t.Helper()
	byI := map[int]types.NodeID{}
	slots := map[int64]struct{}{}
	for i := 1; i <= n; i++ {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		nd, err := sess.AddNode([]string{"Doc"}, map[string]any{"vec": []float32{float32(i), 0}, "i": int64(i)})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		_ = sess.Close()
		byI[i] = nd.ID()
		slots[g.Admin().DecomposeNodeID(nd.ID()).NodeID] = struct{}{}
	}
	return byI, slots
}

// TestShardedVectorBruteForceExactCrossShard is the S5-D correctness oracle: a
// brute-force (exact) vector index over cross-shard-distributed vectors must
// return the EXACT global top-k in distance order — proving the cross-shard
// merge re-ranks correctly, not just returns some-per-shard-k.
func TestShardedVectorBruteForceExactCrossShard(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	byI, slots := addVectorNodesAcrossShards(t, g, 12)
	if len(slots) < 2 {
		t.Fatalf("vectors not spread across shards (slots=%d)", len(slots))
	}

	opts := storepkg.VectorIndexOptions{UseBruteForce: true}
	if err := g.Index().CreateVectorWithOptions("Doc", "vec", 2, storepkg.DistanceEuclidean, opts); err != nil {
		t.Fatalf("CreateVectorWithOptions: %v", err)
	}

	// Query at the origin: nearest are i=1,2,3,4 in that exact order.
	got, err := g.Index().SearchNearest("Doc", "vec", []float32{0, 0}, 4, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
	wantOrder := []types.NodeID{byI[1], byI[2], byI[3], byI[4]}
	if len(got) != len(wantOrder) {
		t.Fatalf("got %d results, want %d", len(got), len(wantOrder))
	}
	for idx, w := range wantOrder {
		if got[idx].ID() != w {
			t.Fatalf("result[%d] = %d, want node i=%d (%d)", idx, got[idx].ID().SnowflakeID(), idx+1, w.SnowflakeID())
		}
	}
}

// TestShardedVectorAutoMaintenance verifies per-shard auto-maintenance flows
// through to the merged search: a new node appears, an updated vector re-ranks,
// and a deleted node disappears.
func TestShardedVectorAutoMaintenance(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	byI, _ := addVectorNodesAcrossShards(t, g, 6)

	opts := storepkg.VectorIndexOptions{UseBruteForce: true}
	if err := g.Index().CreateVectorWithOptions("Doc", "vec", 2, storepkg.DistanceEuclidean, opts); err != nil {
		t.Fatalf("CreateVectorWithOptions: %v", err)
	}

	// Move node 5 to be the closest (vec -> [0.1, 0]).
	if _, err := g.Nodes().Update(t.Context(), byI[5], map[string]any{"vec": []float32{0.1, 0}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := g.Index().SearchNearest("Doc", "vec", []float32{0, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest post-update: %v", err)
	}
	if len(got) != 1 || got[0].ID() != byI[5] {
		t.Fatalf("nearest after update should be node 5")
	}

	// Delete node 1 (previously closest before the update); node 5 stays nearest,
	// and node 1 must not appear.
	if err := g.Nodes().Delete(t.Context(), byI[1]); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = g.Index().SearchNearest("Doc", "vec", []float32{0, 0}, 6, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest post-delete: %v", err)
	}
	for _, n := range got {
		if n.ID() == byI[1] {
			t.Fatal("deleted node 1 still present in search results")
		}
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 results after delete, got %d", len(got))
	}
}

// TestShardedVectorFilteredSearch exercises SearchNearestFiltered directly: a
// filter admitting only even-i nodes must yield the exact even-i top-k in
// distance order, re-ranked across shards.
func TestShardedVectorFilteredSearch(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 6})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()

	byI, slots := addVectorNodesAcrossShards(t, g, 12)
	if len(slots) < 2 {
		t.Fatalf("vectors not spread across shards (slots=%d)", len(slots))
	}
	opts := storepkg.VectorIndexOptions{UseBruteForce: true}
	if err := g.Index().CreateVectorWithOptions("Doc", "vec", 2, storepkg.DistanceEuclidean, opts); err != nil {
		t.Fatalf("CreateVectorWithOptions: %v", err)
	}

	// Resolve token + build the even-i ID set.
	token := uint16(0)
	for tok := uint16(1); tok <= 16; tok++ {
		nodes, e := st.NodesByLabel(tok, storepkg.QueryOpts{})
		if e != nil {
			t.Fatalf("NodesByLabel: %v", e)
		}
		if len(nodes) == 12 {
			token = tok
			break
		}
	}
	if token == 0 {
		t.Fatal("could not resolve Doc token")
	}
	evenIDs := map[snowflake.ID]struct{}{}
	for i := 2; i <= 12; i += 2 {
		evenIDs[snowflake.ID(byI[i].SnowflakeID())] = struct{}{}
	}
	filter := func(id snowflake.ID) bool {
		_, ok := evenIDs[id]
		return ok
	}

	got, err := st.SearchNearestFiltered(token, "vec", []float32{0, 0}, 3, filter)
	if err != nil {
		t.Fatalf("SearchNearestFiltered: %v", err)
	}
	// Nearest even-i to origin: i=2,4,6 in that order.
	want := []snowflake.ID{
		snowflake.ID(byI[2].SnowflakeID()),
		snowflake.ID(byI[4].SnowflakeID()),
		snowflake.ID(byI[6].SnowflakeID()),
	}
	if len(got) != len(want) {
		t.Fatalf("got %d filtered results, want %d", len(got), len(want))
	}
	for idx, w := range want {
		if got[idx] != w {
			t.Fatalf("filtered result[%d] = %d, want %d", idx, got[idx], w)
		}
	}
}

// TestShardedVectorDDLSentinels checks store-level vector DDL sentinels +
// dimension validation.
func TestShardedVectorDDLSentinels(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	defer func() { _ = st.Close() }()

	const label = uint16(1)
	if err := st.CreateVectorIndex(label, "vec", 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("first CreateVectorIndex: %v", err)
	}
	if err := st.CreateVectorIndex(label, "vec", 3, storepkg.DistanceCosine); !errors.Is(err, storepkg.ErrVectorIndexExists) {
		t.Fatalf("double create = %v, want ErrVectorIndexExists", err)
	}
	// Invalid dims rejected.
	if err := st.CreateVectorIndex(label, "bad", 0, storepkg.DistanceCosine); err == nil {
		t.Fatal("expected dims=0 to be rejected")
	}
	// Search for a missing index -> ErrVectorIndexNotFound.
	if _, err := st.SearchNearestNodes(label, "nope", []float32{1, 2, 3}, 3, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrVectorIndexNotFound) {
		t.Fatalf("search missing index = %v, want ErrVectorIndexNotFound", err)
	}
	if err := st.DropVectorIndex(label, "vec"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	if err := st.DropVectorIndex(label, "vec"); !errors.Is(err, storepkg.ErrVectorIndexNotFound) {
		t.Fatalf("drop missing = %v, want ErrVectorIndexNotFound", err)
	}
}

// TestShardedVectorReopenOnDisk verifies the store-level def metadata (dims +
// metric) is persisted and reloaded, so a vector search works after reopen.
func TestShardedVectorReopenOnDisk(t *testing.T) {
	dir := t.TempDir()
	var byI map[int]types.NodeID

	func() {
		st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 6})
		if err != nil {
			t.Fatalf("sharded.New: %v", err)
		}
		g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		defer func() { _ = g.Close() }()
		byI, _ = addVectorNodesAcrossShards(t, g, 6)
		opts := storepkg.VectorIndexOptions{UseBruteForce: true}
		if err := g.Index().CreateVectorWithOptions("Doc", "vec", 2, storepkg.DistanceEuclidean, opts); err != nil {
			t.Fatalf("CreateVectorWithOptions: %v", err)
		}
	}()

	// Reopen.
	st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 6})
	if err != nil {
		t.Fatalf("reopen sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
	if err != nil {
		t.Fatalf("reopen graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()

	got, err := g.Index().SearchNearest("Doc", "vec", []float32{0, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest after reopen: %v", err)
	}
	if len(got) != 1 || got[0].ID() != byI[1] {
		t.Fatalf("nearest after reopen should be node 1 (%d), got %v", byI[1].SnowflakeID(), got)
	}
}
