package temporal

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (s *temporalOpsSpy) NodesAtTxByIDs(ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error) {
	s.record("NodesAtTxByIDs")
	s.lastNodeIDs, s.lastValidAt, s.lastPin = ids, validAt, txAt
	return s.nodeRows, s.err
}

func (s *temporalOpsSpy) RelsAtTxByIDs(ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error) {
	s.record("RelsAtTxByIDs")
	s.lastRelIDs, s.lastValidAt, s.lastPin = ids, validAt, txAt
	return s.relRows, s.err
}

// TestAtTxByIDsWrappers — rule 1 for the backlog-57 wrappers: a nil API and a
// typed-nil Ops return ErrNilGraph without calling anything; a ready API
// forwards ids, validAt and txAt verbatim (not swapped) and returns the Ops'
// rows (same slice, nil elements kept) and error (errors.Is) unchanged. The
// semantics are tested against NodeAtTx / RelAtTx in internal/core
// (TestNodesAtTxByIDsMatchesNodeAtTx).
func TestAtTxByIDsWrappers(t *testing.T) {
	t.Parallel()
	nodeIDs, relIDs := []types.NodeID{3, 1, 3}, []types.RelID{7, 5}
	for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if _, err := api.NodesAtTxByIDs(nodeIDs, 1, 2); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s NodesAtTxByIDs = %v, want ErrNilGraph", name, err)
		}
		if _, err := api.RelsAtTxByIDs(relIDs, 1, 2); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s RelsAtTxByIDs = %v, want ErrNilGraph", name, err)
		}
	}
	wantErr := errors.New("ops failed")
	n1 := types.NewNode(10, 0, nil)
	r1 := types.NewRelationship(20, 0, 0, 0)
	spy := &temporalOpsSpy{err: wantErr, nodeRows: []*types.Node{n1, nil, n1}, relRows: []*types.Relationship{nil, r1}}
	api := New(spy)

	ns, err := api.NodesAtTxByIDs(nodeIDs, 40, 50)
	if !errors.Is(err, wantErr) || !slices.Equal(ns, spy.nodeRows) || !slices.Equal(spy.lastNodeIDs, nodeIDs) ||
		spy.lastValidAt != 40 || spy.lastPin != 50 {
		t.Fatalf("NodesAtTxByIDs: rows %v err %v, forwarded ids %v validAt %d txAt %d", ns, err, spy.lastNodeIDs, spy.lastValidAt, spy.lastPin)
	}
	rs, err := api.RelsAtTxByIDs(relIDs, 60, 70)
	if !errors.Is(err, wantErr) || !slices.Equal(rs, spy.relRows) || !slices.Equal(spy.lastRelIDs, relIDs) ||
		spy.lastValidAt != 60 || spy.lastPin != 70 {
		t.Fatalf("RelsAtTxByIDs: rows %v err %v, forwarded ids %v validAt %d txAt %d", rs, err, spy.lastRelIDs, spy.lastValidAt, spy.lastPin)
	}
	if !slices.Equal(spy.calls, []string{"NodesAtTxByIDs", "RelsAtTxByIDs"}) {
		t.Fatalf("calls = %v", spy.calls)
	}
}
