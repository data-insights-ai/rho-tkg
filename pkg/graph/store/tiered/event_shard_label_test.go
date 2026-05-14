package tiered

import (
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestTiered_AddNodeLabelToken_EventShard exercises the event-shard branch
// of AddNodeLabelToken — the existing tests run only on refShard. Routing
// goes via shardForNodeIDChecked which returns the timestamp-derived event
// shard for Signal-labeled nodes.
func TestTiered_AddNodeLabelToken_EventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	tagTok, _ := reg.GetOrCreate("Tag")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode signal: %v", err)
	}
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(tagTok)
	if err := ts.AddNodeLabelToken(n.ID(), tagTok, updated); err != nil {
		t.Fatalf("AddNodeLabelToken on event-shard node: %v", err)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after AddLabel: %v", err)
	}
	if !got.HasLabelTokenRaw(tagTok) {
		t.Fatalf("event-shard node missing added label tok %d", tagTok)
	}
}

func TestTiered_RemoveNodeLabelToken_EventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	tagTok, _ := reg.GetOrCreate("Tag")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	n.AddLabelTokenRaw(tagTok)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode signal+tag: %v", err)
	}
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(tagTok)
	if err := ts.RemoveNodeLabelToken(n.ID(), tagTok, updated); err != nil {
		t.Fatalf("RemoveNodeLabelToken on event-shard node: %v", err)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after RemoveLabel: %v", err)
	}
	if got.HasLabelTokenRaw(tagTok) {
		t.Fatalf("event-shard node still has removed label tok %d", tagTok)
	}
}

func TestTiered_AddNodeLabelTokenWithHistory_EventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	tagTok, _ := reg.GetOrCreate("Tag")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(tagTok)
	updated.SetVersion(1)
	if err := ts.AddNodeLabelTokenWithHistory(n.ID(), tagTok, updated, n.Version(), n); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory event-shard: %v", err)
	}
	history, err := ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) == 0 || history[0].Version() != 0 {
		t.Fatalf("history after add-label = %v, want v0 first", history)
	}
}

func TestTiered_RemoveNodeLabelTokenWithHistory_EventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	tagTok, _ := reg.GetOrCreate("Tag")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	n.AddLabelTokenRaw(tagTok)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(tagTok)
	updated.SetVersion(1)
	if err := ts.RemoveNodeLabelTokenWithHistory(n.ID(), tagTok, updated, n.Version(), n); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistory event-shard: %v", err)
	}
	history, err := ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) == 0 || history[0].Version() != 0 {
		t.Fatalf("history after remove-label = %v, want v0 first", history)
	}
}

// TestTiered_ReplaceNodeWithHistory_EventShard exercises the standalone
// Replace path on an event-shard node.
func TestTiered_ReplaceNodeWithHistory_EventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	updated := n.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(updated, n.Version(), n); err != nil {
		t.Fatalf("ReplaceNodeWithHistory event-shard: %v", err)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != 1 {
		t.Fatalf("after Replace version = %d, want 1", got.Version())
	}
}

func TestTiered_ReplaceRelWithHistory_EventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	updated := r.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceRelWithHistory(updated, r.Version(), r); err != nil {
		t.Fatalf("ReplaceRelWithHistory event-shard: %v", err)
	}
	got, err := ts.GetRelationship(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.Version() != 1 {
		t.Fatalf("after Replace version = %d, want 1", got.Version())
	}
}
