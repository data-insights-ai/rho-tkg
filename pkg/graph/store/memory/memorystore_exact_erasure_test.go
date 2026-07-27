package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMemoryExactEraseScopeHistoryIndexesAndIdempotency(t *testing.T) {
	ms := New()
	const label, relType = uint16(7), uint16(9)
	n1, n2, n3 := types.NodeID(1), types.NodeID(2), types.NodeID(3)
	for _, tc := range []struct {
		id    types.NodeID
		value string
	}{{n1, "erased-secret"}, {n2, "survivor-a"}, {n3, "survivor-b"}} {
		n := types.NewNode(tc.id, label, nil)
		n.SetVersion(1)
		if err := n.SetProperty("email", tc.value); err != nil {
			t.Fatal(err)
		}
		if err := n.SetProperty("embedding", []float32{float32(tc.id), 1}); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", tc.id, err)
		}
	}
	h := types.NewNode(n1, label, nil)
	_ = h.SetProperty("email", "older-secret")
	if err := ms.PutNodeVersion(n1, 0, h); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	r1 := types.NewRelationship(types.RelID(101), relType, n1, n2)
	r2 := types.NewRelationship(types.RelID(102), relType, n1, n3)
	_ = r1.SetProperty("note", "erased-rel-secret")
	for _, r := range []*types.Relationship{r1, r2} {
		r.SetVersion(1)
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		rh := types.NewRelationship(r.ID(), relType, r.StartNodeID(), r.EndNodeID())
		if err := ms.PutRelVersion(r.ID(), 0, rh); err != nil {
			t.Fatalf("PutRelVersion: %v", err)
		}
	}
	if err := ms.MetaSet("compact_stub_node/1", []byte("erased-stub")); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreatePropertyIndex(label, "email"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := ms.CreateVectorIndex(label, "embedding", 2, storecontract.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// A relationship is deliberately omitted: refusal must be preflight-only.
	_, err := ms.ExactErase(storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{n1},
		RelIDs:  []types.RelID{r1.ID()},
	})
	if !errors.Is(err, storecontract.ErrExactErasureRelationshipEscape) {
		t.Fatalf("scope escape = %v, want ErrExactErasureRelationshipEscape", err)
	}
	if _, err := ms.GetNode(n1); err != nil {
		t.Fatalf("escape refusal mutated node: %v", err)
	}
	if _, err := ms.GetRelationship(r1.ID()); err != nil {
		t.Fatalf("escape refusal mutated rel: %v", err)
	}
	// Corrupt away every adjacency leg. The live-row endpoint fold must still
	// detect the undeclared relationships and refuse.
	ms.mu.Lock()
	delete(ms.outIdx, n1)
	delete(ms.inIdx, n2)
	delete(ms.inIdx, n3)
	ms.mu.Unlock()
	_, err = ms.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{n1}})
	if !errors.Is(err, storecontract.ErrExactErasureRelationshipEscape) {
		t.Fatalf("missing-adjacency scope escape = %v, want ErrExactErasureRelationshipEscape", err)
	}

	req := storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{n1},
		RelIDs:  []types.RelID{r1.ID(), r2.ID()},
		MetaWrites: []storecontract.MetaWrite{
			{Key: "compact_stub_node/1"},
		},
	}
	got, err := ms.ExactErase(req)
	if err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	if got.NodesRemoved != 1 || got.RelsRemoved != 2 {
		t.Fatalf("ExactErase result = %+v, want 1 node / 2 rels", got)
	}
	if _, err := ms.GetNode(n1); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("erased node read = %v", err)
	}
	if hist, _ := ms.GetNodeHistory(n1); len(hist) != 0 {
		t.Fatalf("erased node history has %d rows", len(hist))
	}
	for _, rid := range []types.RelID{r1.ID(), r2.ID()} {
		if _, err := ms.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("erased rel %d read = %v", rid, err)
		}
		if hist, _ := ms.GetRelHistory(rid); len(hist) != 0 {
			t.Fatalf("erased rel %d history has %d rows", rid, len(hist))
		}
	}
	if v, err := ms.MetaGet("compact_stub_node/1"); err != nil || len(v) != 0 {
		t.Fatalf("erasure metadata survived = (%q, %v)", v, err)
	}
	for _, survivor := range []types.NodeID{n2, n3} {
		if _, err := ms.GetNode(survivor); err != nil {
			t.Fatalf("survivor %d: %v", survivor, err)
		}
	}
	if in, err := ms.IncomingRelationships(n2, 0); err != nil || len(in) != 0 {
		t.Fatalf("survivor adjacency = (%d, %v)", len(in), err)
	}
	stats, err := ms.NodePropertyStats(label, "email")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Count != 2 || stats.Min == "erased-secret" || stats.Max == "erased-secret" {
		t.Fatalf("planner stats retain erased value: %+v", stats)
	}
	if rows, err := ms.NodesByLabelAndProperty(label, "email", "erased-secret", QueryOpts{}); err != nil || len(rows) != 0 {
		t.Fatalf("property index retained erased value = (%d, %v)", len(rows), err)
	}
	if rows, err := ms.SearchNearestNodes(label, "embedding", []float32{1, 1}, 3, QueryOpts{}); err != nil || len(rows) != 2 {
		t.Fatalf("vector index after erasure = (%d, %v), want two survivors", len(rows), err)
	}
	if relStats, err := ms.RelPropertyStats(relType, "note"); err != nil || relStats.Count != 0 || relStats.Min != nil || relStats.Max != nil {
		t.Fatalf("relationship planner stats retain erased value = (%+v, %v)", relStats, err)
	}

	again, err := ms.ExactErase(req)
	if err != nil {
		t.Fatalf("idempotent ExactErase: %v", err)
	}
	if again != (storecontract.ExactErasureResult{}) {
		t.Fatalf("idempotent result = %+v, want zero", again)
	}

	// Relationship-only parity: no node declaration is required when the legal
	// scope is solely one relationship.
	standalone := types.NewRelationship(types.RelID(103), relType, n2, n3)
	if err := ms.PutRelationship(standalone); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelVersion(standalone.ID(), 0, standalone); err != nil {
		t.Fatal(err)
	}
	relOnly, err := ms.ExactErase(storecontract.ExactErasureRequest{RelIDs: []types.RelID{standalone.ID()}})
	if err != nil || relOnly.RelsRemoved != 1 {
		t.Fatalf("relationship-only ExactErase = (%+v, %v)", relOnly, err)
	}
	if hist, _ := ms.GetRelHistory(standalone.ID()); len(hist) != 0 {
		t.Fatalf("relationship-only history survived: %d", len(hist))
	}
	if _, err := ms.GetNode(n2); err != nil {
		t.Fatalf("relationship-only erasure removed endpoint: %v", err)
	}
}

func TestMemoryExactEraseRefusesEnabledChangeLog(t *testing.T) {
	ms := New(WithChangeLog())
	_, err := ms.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}})
	if !errors.Is(err, storecontract.ErrExactErasureChangeLogRetained) {
		t.Fatalf("ExactErase with log = %v, want ErrExactErasureChangeLogRetained", err)
	}
}
