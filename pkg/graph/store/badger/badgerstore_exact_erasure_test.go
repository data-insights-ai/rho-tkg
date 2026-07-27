package badger

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBadgerExactEraseIsAtomicIdempotentAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, SyncWrites: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const label, relType = uint16(7), uint16(9)
	n1, n2, n3 := types.NodeID(1), types.NodeID(2), types.NodeID(3)
	for _, tc := range []struct {
		id    types.NodeID
		value string
	}{{n1, "erased-secret"}, {n2, "survivor-a"}, {n3, "survivor-b"}} {
		n := types.NewNode(tc.id, label, nil)
		n.SetVersion(1)
		_ = n.SetProperty("email", tc.value)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", tc.id, err)
		}
	}
	h := types.NewNode(n1, label, nil)
	_ = h.SetProperty("email", "older-secret")
	if err := bs.PutNodeVersion(n1, 0, h); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	r1 := types.NewRelationship(types.RelID(101), relType, n1, n2)
	r2 := types.NewRelationship(types.RelID(102), relType, n1, n3)
	_ = r1.SetProperty("note", "erased-rel-secret")
	for _, r := range []*types.Relationship{r1, r2} {
		r.SetVersion(1)
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		rh := types.NewRelationship(r.ID(), relType, r.StartNodeID(), r.EndNodeID())
		if err := bs.PutRelVersion(r.ID(), 0, rh); err != nil {
			t.Fatalf("PutRelVersion: %v", err)
		}
	}
	if err := bs.MetaSet("compact_stub_node/1", []byte("erased-stub")); err != nil {
		t.Fatal(err)
	}

	_, err = bs.ExactErase(storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{n1},
		RelIDs:  []types.RelID{r1.ID()},
	})
	if !errors.Is(err, storecontract.ErrExactErasureRelationshipEscape) {
		t.Fatalf("scope escape = %v, want ErrExactErasureRelationshipEscape", err)
	}
	if _, err := bs.GetNode(n1); err != nil {
		t.Fatalf("escape refusal mutated node: %v", err)
	}

	req := storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{n1},
		RelIDs:  []types.RelID{r1.ID(), r2.ID()},
		MetaWrites: []storecontract.MetaWrite{
			{Key: "compact_stub_node/1"},
		},
	}
	got, err := bs.ExactErase(req)
	if err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	if got.NodesRemoved != 1 || got.RelsRemoved != 2 {
		t.Fatalf("ExactErase result = %+v, want 1/2", got)
	}
	if again, err := bs.ExactErase(req); err != nil || again != (storecontract.ExactErasureResult{}) {
		t.Fatalf("idempotent ExactErase = (%+v, %v)", again, err)
	}
	standalone := types.NewRelationship(types.RelID(103), relType, n2, n3)
	if err := bs.PutRelationship(standalone); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelVersion(standalone.ID(), 0, standalone); err != nil {
		t.Fatal(err)
	}
	if got, err := bs.ExactErase(storecontract.ExactErasureRequest{RelIDs: []types.RelID{standalone.ID()}}); err != nil || got.RelsRemoved != 1 {
		t.Fatalf("relationship-only ExactErase = (%+v, %v)", got, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs.Close()
	if _, err := bs.GetNode(n1); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("erased node after restart = %v", err)
	}
	if hist, _ := bs.GetNodeHistory(n1); len(hist) != 0 {
		t.Fatalf("erased node history after restart has %d rows", len(hist))
	}
	for _, rid := range []types.RelID{r1.ID(), r2.ID()} {
		if _, err := bs.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("erased rel %d after restart = %v", rid, err)
		}
		if hist, _ := bs.GetRelHistory(rid); len(hist) != 0 {
			t.Fatalf("erased rel %d history after restart has %d rows", rid, len(hist))
		}
	}
	if _, err := bs.GetRelationship(standalone.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("relationship-only erase after restart = %v", err)
	}
	if hist, _ := bs.GetRelHistory(standalone.ID()); len(hist) != 0 {
		t.Fatalf("relationship-only history after restart has %d rows", len(hist))
	}
	for _, survivor := range []types.NodeID{n2, n3} {
		if _, err := bs.GetNode(survivor); err != nil {
			t.Fatalf("survivor %d after restart: %v", survivor, err)
		}
	}
	if in, err := bs.IncomingRelationships(n2, 0); err != nil || len(in) != 0 {
		t.Fatalf("survivor adjacency after restart = (%d, %v)", len(in), err)
	}
	if v, err := bs.MetaGet("compact_stub_node/1"); err != nil || len(v) != 0 {
		t.Fatalf("erasure metadata after restart = (%q, %v)", v, err)
	}
	stats, err := bs.NodePropertyStats(label, "email")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Count != 2 || stats.Min == "erased-secret" || stats.Max == "erased-secret" {
		t.Fatalf("planner stats retain erased value after restart: %+v", stats)
	}
	if relStats, err := bs.RelPropertyStats(relType, "note"); err != nil || relStats.Count != 0 || relStats.Min != nil || relStats.Max != nil {
		t.Fatalf("relationship planner stats retain erased value after restart = (%+v, %v)", relStats, err)
	}
}

func TestBadgerExactEraseRefusesRetainedLogAfterDisabledReopen(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs, err = New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs.Close()
	_, err = bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}})
	if !errors.Is(err, storecontract.ErrExactErasureChangeLogRetained) {
		t.Fatalf("ExactErase with retained disabled log = %v, want ErrExactErasureChangeLogRetained", err)
	}
	if _, err := bs.GetNode(types.NodeID(1)); err != nil {
		t.Fatalf("refused erasure mutated node: %v", err)
	}
}
