package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// containsNodeID reports whether nodes contains a node with id.
func containsNodeID(nodes []*types.Node, id types.NodeID) bool {
	for _, n := range nodes {
		if n.ID() == id {
			return true
		}
	}
	return false
}

func TestBadgerStore_AddNodeLabelToken(t *testing.T) {
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 100, 1, nil)
	id := n.ID().SnowflakeID()
	updated := n.DeepCopy()
	if !updated.AddLabelTokenRaw(2) {
		t.Fatal("AddLabelTokenRaw returned false")
	}

	if err := bs.AddNodeLabelToken(types.NodeID(id), 2, updated); err != nil {
		t.Fatalf("AddNodeLabelToken: %v", err)
	}

	nodes, err := bs.NodesByLabel(2, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if !containsNodeID(nodes, types.NodeID(id)) {
		t.Fatal("node missing from added label index")
	}
	got, err := bs.GetNode(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(2) {
		t.Fatal("persisted node missing added label token")
	}
}

func TestBadgerStore_AddNodeLabelTokenWithHistory(t *testing.T) {
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 101, 1, nil)
	id := n.ID().SnowflakeID()
	prevVersion := n.Version()
	prevState := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(2)
	updated.SetVersion(prevVersion + 1)

	if err := bs.AddNodeLabelTokenWithHistory(types.NodeID(id), 2, updated, prevVersion, prevState); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}

	hist, err := bs.GetNodeVersion(types.NodeID(id), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist.Version() != prevVersion {
		t.Fatalf("history version = %d, want %d", hist.Version(), prevVersion)
	}
	nodes, err := bs.NodesByLabel(2, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if !containsNodeID(nodes, types.NodeID(id)) {
		t.Fatal("node missing from added label index")
	}
}

func TestBadgerStore_AddNodeLabelToken_NotFound(t *testing.T) {
	bs := newTestBadgerStore(t)

	updated := types.NewNode(types.NodeID(snowflake.ID(999)), 1, []uint16{2})
	err := bs.AddNodeLabelToken(types.NodeID(999), 2, updated)
	if err == nil {
		t.Fatal("expected error for missing node")
	}
}
