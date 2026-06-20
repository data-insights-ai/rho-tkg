package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredStoreNodeCountByLabelAndPropertyKey exercises the cross-shard
// aggregation: reference-shard, archive-shard, and (warm + hot) event-shard
// counts must all fold into one total. A non-indexable property value and a
// property key absent from a label both report 0.
func TestTieredStoreNodeCountByLabelAndPropertyKey(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	// Reference-class nodes route to the reference shard.
	refWithID := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"id": int64(1)})
	refNameOnly := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"name": "no-id"})
	refArchived := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"id": int64(2)})
	for _, n := range []*types.Node{refWithID, refNameOnly, refArchived} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode ref %d: %v", n.ID(), err)
		}
	}
	// Move one reference node to the archive shard so checkoutArchive contributes.
	if err := ts.ArchiveNode(refArchived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	// Event-class nodes: one before rotation (lands in a warm shard afterwards),
	// one after (hot shard), plus a non-indexable one.
	warmEvt := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"id": int64(3)})
	if err := ts.PutNode(warmEvt); err != nil {
		t.Fatalf("PutNode warm: %v", err)
	}
	forceRotation(t, ts)
	hotEvt := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"id": int64(4)})
	nonIndexableEvt := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"tags": []string{"a", "b"}})
	for _, n := range []*types.Node{hotEvt, nonIndexableEvt} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode event %d: %v", n.ID(), err)
		}
	}

	// Case/id: refWithID (reference shard) + refArchived (archive shard) = 2.
	if got, err := ts.NodeCountByLabelAndPropertyKey(caseTok, "id"); err != nil || got != 2 {
		t.Fatalf("Case/id = (%d, %v), want (2, nil)", got, err)
	}
	// Case/name: only refNameOnly carries it = 1.
	if got, err := ts.NodeCountByLabelAndPropertyKey(caseTok, "name"); err != nil || got != 1 {
		t.Fatalf("Case/name = (%d, %v), want (1, nil)", got, err)
	}
	// Signal/id: warmEvt (warm shard) + hotEvt (hot shard) = 2.
	if got, err := ts.NodeCountByLabelAndPropertyKey(signalTok, "id"); err != nil || got != 2 {
		t.Fatalf("Signal/id = (%d, %v), want (2, nil)", got, err)
	}
	// Signal/tags: a slice value is not indexable and is not counted = 0.
	if got, err := ts.NodeCountByLabelAndPropertyKey(signalTok, "tags"); err != nil || got != 0 {
		t.Fatalf("Signal/tags = (%d, %v), want (0, nil)", got, err)
	}
	// A label with no nodes at all reports 0.
	if got, err := ts.NodeCountByLabelAndPropertyKey(caseTok, "missing"); err != nil || got != 0 {
		t.Fatalf("Case/missing = (%d, %v), want (0, nil)", got, err)
	}
}

func TestTieredStoreNodeCountByLabelAndPropertyKeyErrors(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)

	if _, err := ts.NodeCountByLabelAndPropertyKey(0, "id"); err == nil {
		t.Fatal("token 0 should be rejected")
	}
	if _, err := ts.NodeCountByLabelAndPropertyKey(caseTok, "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ts.NodeCountByLabelAndPropertyKey(caseTok, "id"); err == nil {
		t.Fatal("closed store should error")
	}
}
