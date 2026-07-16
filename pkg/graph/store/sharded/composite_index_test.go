package sharded_test

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// addCityDeptNodes distributes nodes carrying (city, dept) across shards.
func addCityDeptNodes(t *testing.T, g *graph.Graph, combos [][2]string, per int) (map[[2]string][]types.NodeID, map[int64]struct{}) {
	t.Helper()
	byCombo := map[[2]string][]types.NodeID{}
	slots := map[int64]struct{}{}
	total := len(combos) * per
	for s := 0; s < total; s++ {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		combo := combos[s%len(combos)]
		n, err := sess.AddNode([]string{"Person"}, map[string]any{"city": combo[0], "dept": combo[1], "seq": int64(s)})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		_ = sess.Close()
		byCombo[combo] = append(byCombo[combo], n.ID())
		slots[g.Admin().DecomposeNodeID(n.ID()).NodeID] = struct{}{}
	}
	return byCombo, slots
}

// TestShardedCompositeIndexCrossShard is the S5-B composite gate: an AND-match
// over two keys returns the EXACT cross-shard set, phantom combos return empty,
// and introspection reports the declared tuple.
func TestShardedCompositeIndexCrossShard(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	combos := [][2]string{{"Berlin", "Eng"}, {"Berlin", "Sales"}, {"Paris", "Eng"}}
	byCombo, slots := addCityDeptNodes(t, g, combos, 3) // 9 sessions
	if len(slots) < 2 {
		t.Fatalf("nodes not spread across shards (slots=%d)", len(slots))
	}

	keys := []string{"city", "dept"}
	if err := g.Index().CreateComposite("Person", keys); err != nil {
		t.Fatalf("CreateComposite: %v", err)
	}

	// Introspection: the declared tuple is visible.
	has, err := g.Index().HasComposite("Person", keys)
	if err != nil {
		t.Fatalf("HasComposite: %v", err)
	}
	if !has {
		t.Fatal("HasComposite = false, want true")
	}
	list, err := g.Index().ListComposites("Person")
	if err != nil {
		t.Fatalf("ListComposites: %v", err)
	}
	if len(list) != 1 || len(list[0]) != 2 || list[0][0] != "city" || list[0][1] != "dept" {
		t.Fatalf("ListComposites = %v, want [[city dept]]", list)
	}

	// Exact AND-match per combo, folded across shards.
	for _, combo := range combos {
		got, err := g.Nodes().ByLabelAndProperties("Person", map[string]any{"city": combo[0], "dept": combo[1]}, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabelAndProperties(%v): %v", combo, err)
		}
		assertNodeIDSet(t, got, byCombo[combo])
	}

	// Phantom combo -> empty (Berlin+Eng exists, but Tokyo+Eng does not).
	got, err := g.Nodes().ByLabelAndProperties("Person", map[string]any{"city": "Tokyo", "dept": "Eng"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("phantom combo: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("phantom combo returned %d, want 0", len(got))
	}
}

// TestShardedCompositeIndexDDLSentinels checks store-level composite DDL:
// duplicate ordered-key definition -> ErrIndexExists, drop-missing ->
// ErrIndexNotFound, and a different key ORDER is a distinct definition.
func TestShardedCompositeIndexDDLSentinels(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	defer func() { _ = st.Close() }()

	const label = uint16(1)
	if err := st.CreateCompositePropertyIndex(label, []string{"city", "dept"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := st.CreateCompositePropertyIndex(label, []string{"city", "dept"}); !errors.Is(err, storepkg.ErrIndexExists) {
		t.Fatalf("duplicate = %v, want ErrIndexExists", err)
	}
	// Different ORDER is a distinct definition — must succeed.
	if err := st.CreateCompositePropertyIndex(label, []string{"dept", "city"}); err != nil {
		t.Fatalf("reversed-order create: %v", err)
	}
	// Both definitions are now listed on the anchor.
	defs, err := st.ListCompositePropertyIndexes(label)
	if err != nil {
		t.Fatalf("ListCompositePropertyIndexes: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("expected 2 composite defs, got %d", len(defs))
	}
	if err := st.DropCompositePropertyIndex(label, []string{"city", "dept"}); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := st.DropCompositePropertyIndex(label, []string{"city", "dept"}); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("drop missing = %v, want ErrIndexNotFound", err)
	}
}
