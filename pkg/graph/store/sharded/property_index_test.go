package sharded_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newLanedShardedGraph opens a sharded store claiming enough slots to cover the
// interactive pair {0,1} plus `lanes` lane slots, wrapped in a graph with that
// many IngestLanes. Concurrent ingest sessions then distribute nodes across
// shards (each session pins to one slot), which is how these tests exercise the
// CROSS-SHARD property-index fold — the interactive Add door alone would put
// every node on slot 0 (one shard) and never test the merge.
func newLanedShardedGraph(t *testing.T, lanes uint8) *graph.Graph {
	t.Helper()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2 + lanes})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: lanes})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// addCityNodesAcrossShards creates sessions sequentially (deterministic lane
// pinning), each adding one node per city so a given city's nodes land on
// SEVERAL distinct slots. Returns city -> sorted node IDs and the set of slots
// actually used, so a test can assert genuine cross-shard spread.
func addCityNodesAcrossShards(t *testing.T, g *graph.Graph, cities []string, sessionsPerCity int) (map[string][]types.NodeID, map[int64]struct{}) {
	t.Helper()
	byCity := map[string][]types.NodeID{}
	slots := map[int64]struct{}{}
	total := len(cities) * sessionsPerCity
	for s := 0; s < total; s++ {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		city := cities[s%len(cities)]
		n, err := sess.AddNode([]string{"Person"}, map[string]any{"city": city, "seq": int64(s)})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		_ = sess.Close()
		byCity[city] = append(byCity[city], n.ID())
		slots[g.Admin().DecomposeNodeID(n.ID()).NodeID] = struct{}{}
	}
	for c := range byCity {
		sortNodeIDs(byCity[c])
	}
	return byCity, slots
}

func sortNodeIDs(ids []types.NodeID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i].SnowflakeID() < ids[j].SnowflakeID() })
}

func idSet(nodes []*types.Node) map[types.NodeID]struct{} {
	m := make(map[types.NodeID]struct{}, len(nodes))
	for _, n := range nodes {
		m[n.ID()] = struct{}{}
	}
	return m
}

func assertNodeIDSet(t *testing.T, got []*types.Node, want []types.NodeID) {
	t.Helper()
	gs := idSet(got)
	if len(gs) != len(want) {
		t.Fatalf("result set size = %d, want %d", len(gs), len(want))
	}
	for _, id := range want {
		if _, ok := gs[id]; !ok {
			t.Fatalf("expected node %d missing from result set", id.SnowflakeID())
		}
	}
}

// TestShardedPropertyIndexCrossShard is the S5-A correctness gate: a property
// index over cross-shard-distributed nodes returns the EXACT set for a value
// (folded across shards), an empty set for a phantom value, and paginates over
// the merged global order. Index created AFTER nodes exist (per-shard backfill).
func TestShardedPropertyIndexCrossShard(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	cities := []string{"Berlin", "Paris", "Tokyo"}
	byCity, slots := addCityNodesAcrossShards(t, g, cities, 3) // 9 sessions

	if len(slots) < 2 {
		t.Fatalf("nodes did not spread across shards (slots used: %d) — test is not exercising the fold", len(slots))
	}

	if err := g.Index().CreateProperty("Person", "city"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}

	for _, city := range cities {
		got, err := g.Nodes().ByLabelAndProperty("Person", "city", city, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabelAndProperty(%s): %v", city, err)
		}
		assertNodeIDSet(t, got, byCity[city])
	}

	// Phantom value -> empty.
	got, err := g.Nodes().ByLabelAndProperty("Person", "city", "Atlantis", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("phantom query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("phantom value returned %d nodes, want 0", len(got))
	}

	// Pagination over the merged order: Limit=2 yields the two lowest-ID Berlins.
	page, err := g.Nodes().ByLabelAndProperty("Person", "city", "Berlin", storepkg.QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("paged query: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("Limit=2 returned %d nodes", len(page))
	}
	want2 := byCity["Berlin"][:2]
	assertNodeIDSet(t, page, want2)
}

// TestShardedPropertyIndexStoreFoldDirect exercises the store's own
// NodesByLabelAndProperty fold DIRECTLY (bypassing the graph layer's
// scan+filter fallback), so a green here proves the cross-shard merge — not the
// core fallback — produced the answer. Nodes are distributed via the graph, the
// store handle is kept, and the label token is resolved from the persisted
// registry for the raw store call.
func TestShardedPropertyIndexStoreFoldDirect(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 6})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()

	byCity, slots := addCityNodesAcrossShards(t, g, []string{"Berlin", "Paris"}, 3)
	if len(slots) < 2 {
		t.Fatalf("nodes not spread across shards (slots=%d)", len(slots))
	}
	if err := g.Index().CreateProperty("Person", "city"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}

	// Resolve the "Person" label token without depending on registry-flush
	// timing: it is the single label in play, so the token whose NodesByLabel
	// returns every created node is it (tokens start at 1; 0 is reserved).
	total := len(byCity["Berlin"]) + len(byCity["Paris"])
	token := uint16(0)
	for tok := uint16(1); tok <= 16; tok++ {
		nodes, e := st.NodesByLabel(tok, storepkg.QueryOpts{})
		if e != nil {
			t.Fatalf("NodesByLabel(%d): %v", tok, e)
		}
		if len(nodes) == total {
			token = tok
			break
		}
	}
	if token == 0 {
		t.Fatal("could not resolve the Person label token via NodesByLabel")
	}

	got, err := st.NodesByLabelAndProperty(token, "city", "Berlin", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("store.NodesByLabelAndProperty: %v", err)
	}
	assertNodeIDSet(t, got, byCity["Berlin"])

	// Store-level result must be globally ID-sorted (the merge contract).
	for i := 1; i < len(got); i++ {
		if got[i-1].ID().SnowflakeID() > got[i].ID().SnowflakeID() {
			t.Fatalf("store fold not ID-sorted at %d", i)
		}
	}
}

// TestShardedPropertyIndexAutoMaintenance verifies per-shard auto-maintenance:
// updating a node's indexed value moves it between value buckets, and deleting
// a node removes it — all reflected through the cross-shard fold.
func TestShardedPropertyIndexAutoMaintenance(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	cities := []string{"Berlin", "Paris"}
	byCity, _ := addCityNodesAcrossShards(t, g, cities, 3) // 6 sessions

	if err := g.Index().CreateProperty("Person", "city"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}

	// Move one Berlin node to Paris.
	moved := byCity["Berlin"][0]
	if _, err := g.Nodes().Update(t.Context(), moved, map[string]any{"city": "Paris"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	berlin, err := g.Nodes().ByLabelAndProperty("Person", "city", "Berlin", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("Berlin query: %v", err)
	}
	assertNodeIDSet(t, berlin, byCity["Berlin"][1:])

	// Delete one Paris node.
	del := byCity["Paris"][0]
	if err := g.Nodes().Delete(t.Context(), del); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	paris, err := g.Nodes().ByLabelAndProperty("Person", "city", "Paris", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("Paris query: %v", err)
	}
	// Paris now = original Paris minus the deleted one, plus the moved Berlin.
	wantParis := append([]types.NodeID{}, byCity["Paris"][1:]...)
	wantParis = append(wantParis, moved)
	assertNodeIDSet(t, paris, wantParis)
}

// TestShardedPropertyIndexDDLSentinels checks the store-level DDL contract:
// double-create returns ErrIndexExists, drop-missing returns ErrIndexNotFound,
// and both are errors.Is-able through the cross-shard coalesce.
func TestShardedPropertyIndexDDLSentinels(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	defer func() { _ = st.Close() }()

	const label = uint16(1)
	if err := st.CreatePropertyIndex(label, "city"); err != nil {
		t.Fatalf("first CreatePropertyIndex: %v", err)
	}
	if err := st.CreatePropertyIndex(label, "city"); !errors.Is(err, storepkg.ErrIndexExists) {
		t.Fatalf("double create = %v, want ErrIndexExists", err)
	}
	if err := st.DropPropertyIndex(label, "city"); err != nil {
		t.Fatalf("DropPropertyIndex: %v", err)
	}
	if err := st.DropPropertyIndex(label, "city"); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("drop missing = %v, want ErrIndexNotFound", err)
	}
	// Re-create after drop must succeed (clean lockstep state).
	if err := st.CreatePropertyIndex(label, "city"); err != nil {
		t.Fatalf("re-create after drop: %v", err)
	}
}
