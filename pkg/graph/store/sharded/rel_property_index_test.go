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

// addWeightedRelsAcrossShards creates sessions sequentially; each builds a
// co-located node pair and one KNOWS rel with weight = weights[i % len]. A
// session pins to one slot, so the rel lands on that slot — different sessions
// spread same-weight rels across shards, exercising the cross-shard fold.
func addWeightedRelsAcrossShards(t *testing.T, g *graph.Graph, weights []string, sessionsPerWeight int) (map[string][]types.RelID, map[int64]struct{}) {
	t.Helper()
	byWeight := map[string][]types.RelID{}
	slots := map[int64]struct{}{}
	total := len(weights) * sessionsPerWeight
	for s := 0; s < total; s++ {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		a, err := sess.AddNode([]string{"Person"}, map[string]any{"n": int64(s * 2)})
		if err != nil {
			t.Fatalf("AddNode a: %v", err)
		}
		b, err := sess.AddNode([]string{"Person"}, map[string]any{"n": int64(s*2 + 1)})
		if err != nil {
			t.Fatalf("AddNode b: %v", err)
		}
		w := weights[s%len(weights)]
		r, err := sess.AddRelationship("KNOWS", a, b, map[string]any{"weight": w})
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		_ = sess.Close()
		byWeight[w] = append(byWeight[w], r.ID())
		slots[g.Admin().DecomposeRelID(r.ID()).NodeID] = struct{}{}
	}
	for w := range byWeight {
		sort.Slice(byWeight[w], func(i, j int) bool {
			return byWeight[w][i].SnowflakeID() < byWeight[w][j].SnowflakeID()
		})
	}
	return byWeight, slots
}

func assertRelIDSet(t *testing.T, got []*types.Relationship, want []types.RelID) {
	t.Helper()
	gs := make(map[types.RelID]struct{}, len(got))
	for _, r := range got {
		gs[r.ID()] = struct{}{}
	}
	if len(gs) != len(want) {
		t.Fatalf("rel result set size = %d, want %d", len(gs), len(want))
	}
	for _, id := range want {
		if _, ok := gs[id]; !ok {
			t.Fatalf("expected rel %d missing from result set", id.SnowflakeID())
		}
	}
}

// TestShardedRelPropertyIndexCrossShard is the S5-B rel gate: a rel-property
// index over cross-shard-distributed rels returns the EXACT set per value
// (folded across shards), empty for a phantom, and paginates the merged order.
func TestShardedRelPropertyIndexCrossShard(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	weights := []string{"strong", "weak"}
	byWeight, slots := addWeightedRelsAcrossShards(t, g, weights, 4) // 8 sessions

	if len(slots) < 2 {
		t.Fatalf("rels did not spread across shards (slots=%d)", len(slots))
	}

	if err := g.Index().CreateRelProperty("KNOWS", "weight"); err != nil {
		t.Fatalf("CreateRelProperty: %v", err)
	}

	for _, w := range weights {
		got, err := g.Rels().ByTypeAndProperty("KNOWS", "weight", w, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByTypeAndProperty(%s): %v", w, err)
		}
		assertRelIDSet(t, got, byWeight[w])
	}

	// Phantom value -> empty.
	got, err := g.Rels().ByTypeAndProperty("KNOWS", "weight", "cosmic", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("phantom rel query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("phantom rel value returned %d, want 0", len(got))
	}

	// Pagination over the merged order.
	page, err := g.Rels().ByTypeAndProperty("KNOWS", "weight", "strong", storepkg.QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("paged rel query: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("Limit=2 rel query returned %d", len(page))
	}
	assertRelIDSet(t, page, byWeight["strong"][:2])
}

// TestShardedRelPropertyIndexDDLSentinels checks store-level DDL sentinels for
// the rel index (mirrors the node property-index DDL contract).
func TestShardedRelPropertyIndexDDLSentinels(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	defer func() { _ = st.Close() }()

	const relType = uint16(1)
	if err := st.CreateRelPropertyIndex(relType, "weight"); err != nil {
		t.Fatalf("first CreateRelPropertyIndex: %v", err)
	}
	if err := st.CreateRelPropertyIndex(relType, "weight"); !errors.Is(err, storepkg.ErrIndexExists) {
		t.Fatalf("double create = %v, want ErrIndexExists", err)
	}
	if err := st.DropRelPropertyIndex(relType, "weight"); err != nil {
		t.Fatalf("DropRelPropertyIndex: %v", err)
	}
	if err := st.DropRelPropertyIndex(relType, "weight"); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("drop missing = %v, want ErrIndexNotFound", err)
	}
}
