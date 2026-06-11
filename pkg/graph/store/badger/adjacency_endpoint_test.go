package badger

import (
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachAdjacentEndpoint yields (relID, otherEndpoint) straight from the
// adjacency index — no relationship-row decode. It must produce exactly the
// edges the decoding path would, with the correct OTHER endpoint and type
// filtering, in both directions.
func TestForEachAdjacentEndpoint(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	putTestNode(t, bs, 3, 1, nil)
	putTestRel(t, bs, 100, 5, 1, 2) // 1 -[5]-> 2
	putTestRel(t, bs, 101, 5, 1, 3) // 1 -[5]-> 3
	putTestRel(t, bs, 102, 7, 1, 2) // 1 -[7]-> 2

	collect := func(nid types.NodeID, tok uint16, incoming bool) map[types.RelID]types.NodeID {
		got := map[types.RelID]types.NodeID{}
		if err := bs.ForEachAdjacentEndpoint(nid, tok, incoming, func(rel types.RelID, other types.NodeID) bool {
			got[rel] = other
			return true
		}); err != nil {
			t.Fatalf("ForEachAdjacentEndpoint: %v", err)
		}
		return got
	}

	// Outgoing from 1, type 5: the two KNOWS edges, NOT the LIKES edge.
	if got := collect(types.NodeID(1), 5, false); !reflect.DeepEqual(got, map[types.RelID]types.NodeID{100: 2, 101: 3}) {
		t.Fatalf("outgoing type 5 = %v", got)
	}
	// Outgoing from 1, all types: all three, each to its END node.
	if got := collect(types.NodeID(1), 0, false); !reflect.DeepEqual(got, map[types.RelID]types.NodeID{100: 2, 101: 3, 102: 2}) {
		t.Fatalf("outgoing all = %v", got)
	}
	// Incoming to 2, all types: rels 100 and 102, each from its START node (1).
	if got := collect(types.NodeID(2), 0, true); !reflect.DeepEqual(got, map[types.RelID]types.NodeID{100: 1, 102: 1}) {
		t.Fatalf("incoming to 2 = %v", got)
	}
	// Incoming to 2, type 7: only the LIKES edge.
	if got := collect(types.NodeID(2), 7, true); !reflect.DeepEqual(got, map[types.RelID]types.NodeID{102: 1}) {
		t.Fatalf("incoming type 7 = %v", got)
	}

	// fn returning false stops the scan.
	visited := 0
	if err := bs.ForEachAdjacentEndpoint(types.NodeID(1), 0, false, func(types.RelID, types.NodeID) bool {
		visited++
		return false
	}); err != nil {
		t.Fatalf("early-stop scan: %v", err)
	}
	if visited != 1 {
		t.Fatalf("early stop visited %d, want 1", visited)
	}

	// Unknown node is ErrNodeNotFound, mirroring the other adjacency reads.
	if err := bs.ForEachAdjacentEndpoint(types.NodeID(999), 0, false, func(types.RelID, types.NodeID) bool { return true }); err != ErrNodeNotFound {
		t.Fatalf("unknown node err = %v, want ErrNodeNotFound", err)
	}
}
