package tiered

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestTieredStoreNodesByLabelAndPropertyReferenceArchiveDepth(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	archived := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"status": "open"})
	live := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"status": "open"})
	closed := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"status": "closed"})

	for _, n := range []*types.Node{archived, live, closed} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ts.ArchiveNode(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	all, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty DepthAll: %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, all, archived.ID(), live.ID())

	hot, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty DepthHot: %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, hot, live.ID())
}

func TestTieredStoreNodesByLabelAndPropertyEventDepth(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	warm := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"status": "open"})
	if err := ts.PutNode(warm); err != nil {
		t.Fatalf("PutNode warm: %v", err)
	}

	forceRotation(t, ts)

	hot := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"status": "open"})
	closed := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"status": "closed"})
	for _, n := range []*types.Node{hot, closed} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	all, err := ts.NodesByLabelAndProperty(signalTok, "status", "open", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty DepthAll: %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, all, warm.ID(), hot.ID())

	hotOnly, err := ts.NodesByLabelAndProperty(signalTok, "status", "open", QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty DepthHot: %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, hotOnly, hot.ID())
}

func TestTieredStoreLabelReadsIncludeMixedClassExtraLabels(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	refPrimaryEventExtra := tieredLabelPropertyNodeWithExtras(t,
		types.NodeID(gen.Generate()),
		caseTok,
		[]uint16{signalTok},
		map[string]any{"status": "open"},
	)
	eventPrimaryRefExtra := tieredLabelPropertyNodeWithExtras(t,
		types.NodeID(gen.Generate()),
		signalTok,
		[]uint16{caseTok},
		map[string]any{"status": "open"},
	)
	for _, n := range []*types.Node{refPrimaryEventExtra, eventPrimaryRefExtra} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	caseCount, err := ts.NodeCountByLabel(caseTok)
	if err != nil {
		t.Fatalf("NodeCountByLabel(Case): %v", err)
	}
	if caseCount != 2 {
		t.Fatalf("NodeCountByLabel(Case) = %d, want 2", caseCount)
	}
	signalCount, err := ts.NodeCountByLabel(signalTok)
	if err != nil {
		t.Fatalf("NodeCountByLabel(Signal): %v", err)
	}
	if signalCount != 2 {
		t.Fatalf("NodeCountByLabel(Signal) = %d, want 2", signalCount)
	}

	caseNodes, err := ts.NodesByLabel(caseTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(Case): %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, caseNodes, refPrimaryEventExtra.ID(), eventPrimaryRefExtra.ID())

	signalNodes, err := ts.NodesByLabel(signalTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(Signal): %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, signalNodes, refPrimaryEventExtra.ID(), eventPrimaryRefExtra.ID())

	caseByProperty, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty(Case): %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, caseByProperty, refPrimaryEventExtra.ID(), eventPrimaryRefExtra.ID())

	signalByProperty, err := ts.NodesByLabelAndProperty(signalTok, "status", "open", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty(Signal): %v", err)
	}
	requireTieredLabelPropertyNodeIDs(t, signalByProperty, refPrimaryEventExtra.ID(), eventPrimaryRefExtra.ID())
}

func TestTieredStoreNodesByLabelAndPropertyValidationAndClosed(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)

	if _, err := ts.NodesByLabelAndProperty(0, "status", "open", QueryOpts{}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("NodesByLabelAndProperty zero label = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ts.NodesByLabelAndProperty(caseTok, "tkg_hash", "open", QueryOpts{}); !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("NodesByLabelAndProperty reserved key = %v, want ErrReservedPrefix", err)
	}
	if _, err := ts.NodesByLabelAndProperty(caseTok, "status", struct{ Bad int }{Bad: 1}, QueryOpts{}); !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("NodesByLabelAndProperty invalid value = %v, want ErrUnsupportedValueType", err)
	}
	if nodes, err := ts.NodesByLabelAndProperty(caseTok, "status", []string{"valid", "unindexable"}, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabelAndProperty valid unindexable value = (%v, %v), want nil/empty, nil", nodes, err)
	}
	if _, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", QueryOpts{Depth: ShardDepth(99)}); !errors.Is(err, ErrInvalidShardDepth) {
		t.Fatalf("NodesByLabelAndProperty invalid depth = %v, want ErrInvalidShardDepth", err)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", QueryOpts{}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("NodesByLabelAndProperty closed = %v, want ErrStoreClosed", err)
	}
}

func tieredLabelPropertyNode(t *testing.T, id types.NodeID, label uint16, props map[string]any) *types.Node {
	t.Helper()
	return tieredLabelPropertyNodeWithExtras(t, id, label, nil, props)
}

func tieredLabelPropertyNodeWithExtras(t *testing.T, id types.NodeID, label uint16, extraLabels []uint16, props map[string]any) *types.Node {
	t.Helper()
	n := types.NewNode(id, label, extraLabels)
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	if err := n.SetProperties(ps); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	return n
}

func requireTieredLabelPropertyNodeIDs(t *testing.T, nodes []*types.Node, want ...types.NodeID) {
	t.Helper()
	if len(nodes) != len(want) {
		t.Fatalf("node count = %d, want %d; got %v", len(nodes), len(want), tieredLabelPropertyNodeIDs(nodes))
	}

	seen := make(map[types.NodeID]struct{}, len(nodes))
	for _, n := range nodes {
		seen[n.ID()] = struct{}{}
	}
	for _, id := range want {
		if _, ok := seen[id]; !ok {
			t.Fatalf("node IDs = %v, missing %d", tieredLabelPropertyNodeIDs(nodes), id)
		}
	}
}

func tieredLabelPropertyNodeIDs(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID())
	}
	return ids
}
