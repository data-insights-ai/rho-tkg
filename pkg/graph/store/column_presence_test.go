package store

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestColumnBuildersRefuseTypedRowsBeforeAnyCallback(t *testing.T) {
	nodes := make([]*types.Node, ColumnScanBatchRows+1)
	rels := make([]*types.Relationship, len(nodes))
	for i := range nodes {
		nodes[i] = types.NewNode(types.NodeID(i+1), 1, nil)
		rels[i] = types.NewRelationship(types.RelID(i+1), 1, 1, 2)
	}
	tm := &types.TemporalMetadata{}
	if err := tm.SetValidBounds(types.TemporalRange{From: types.TemporalEndpoint{Value: -1, Finite: true}, To: types.TemporalEndpoint{Finite: true}}); err != nil {
		t.Fatal(err)
	}
	nodes[len(nodes)-1].SetTemporal(tm)
	rels[len(rels)-1].SetTemporal(tm)
	calls := 0
	if err := ScanColumnsFromNodes(nodes, nil, func(*ColumnBatch) bool { calls++; return true }); !errors.Is(err, ErrCapabilityNotSupported) || calls != 0 {
		t.Fatal("node columns partially erased endpoint meaning", calls, err)
	}
	if err := ScanColumnsFromRels(rels, nil, func(*RelColumnBatch) bool { calls++; return true }); !errors.Is(err, ErrCapabilityNotSupported) || calls != 0 {
		t.Fatal("rel columns partially erased endpoint meaning", calls, err)
	}
}
