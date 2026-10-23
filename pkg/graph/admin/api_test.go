package admin

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAPIForwardsEveryMethod(t *testing.T) {
	t.Parallel()

	ops := &adminOpsSpy{}
	api := New(ops)

	if err := api.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if got := api.DecomposeNodeID(types.NodeID(99)); got.NodeID != 7 {
		t.Fatalf("DecomposeNodeID = %+v, want NodeID 7", got)
	}
	if got := api.DecomposeRelID(types.RelID(99)); got.NodeID != 7 {
		t.Fatalf("DecomposeRelID = %+v, want NodeID 7", got)
	}

	if _, err := api.PurgeExpiredNodes(context.Background(), PurgePolicy{Label: "Event", Before: 1}); err != nil {
		t.Fatalf("PurgeExpiredNodes: %v", err)
	}
	if rep, err := api.PurgeExpiredRels(context.Background(), RelPurgePolicy{Type: "EVENT", Before: 1}); err != nil || rep.RelsPurged != 5 {
		t.Fatalf("PurgeExpiredRels = %+v, %v; want the spy's 5", rep, err)
	}
	if _, err := api.ExactErase(context.Background(), ExactErasureRequest{NodeIDs: []types.NodeID{1}}); err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	if _, err := api.ResolveExactErasure(context.Background(), ExactErasureRequest{NodeIDs: []types.NodeID{1}}); err != nil {
		t.Fatalf("ResolveExactErasure: %v", err)
	}

	if err := api.SealRelSegments("HOP"); err != nil {
		t.Fatalf("SealRelSegments: %v", err)
	}
	if st, err := api.RelSegmentStats("HOP"); err != nil || st.Segments != 3 {
		t.Fatalf("RelSegmentStats = %+v, %v; want Segments 3", st, err)
	}
	if rows, err := api.NodeStoredSizes(7); err != nil || len(rows) != 1 || rows[0].Bytes != 70 {
		t.Fatalf("NodeStoredSizes = %+v, %v; want one row of 70 bytes", rows, err)
	}
	var seen []types.NodeID
	if err := api.ForEachNodeStoredSize(context.Background(), "L", store.DepthHot, func(id types.NodeID, rows []StoredRow) bool {
		seen = append(seen, id)
		return len(rows) == 1
	}); err != nil || len(seen) != 1 || seen[0] != 8 || ops.storedLabel != "L" || ops.storedDepth != store.DepthHot {
		t.Fatalf("ForEachNodeStoredSize: %v, saw %v, spy got %q %v", err, seen, ops.storedLabel, ops.storedDepth)
	}

	if err := api.SetUntimedValidStart(context.Background(), UntimedUnbounded); err != nil || ops.untimedMode != UntimedUnbounded {
		t.Fatalf("SetUntimedValidStart: %v, spy got %v", err, ops.untimedMode)
	}
	if axis, err := api.ValidAxis(); err != nil || axis.Literal() {
		t.Fatalf("ValidAxis through the API: %+v %v, want the legacy axis", axis, err)
	}
	if mode, complete, err := api.UntimedValidStart(); err != nil || mode != UntimedUnbounded || !complete {
		t.Fatalf("UntimedValidStart through the API: %v %v %v, want the spy's UntimedUnbounded, true", mode, complete, err)
	}

	if ok, err := api.IsReferenceLabel("Ref"); err != nil || !ok || ops.refLabel != "Ref" {
		t.Fatalf("IsReferenceLabel through the API: %v %v, spy got %q; want true, nil, \"Ref\"", ok, err, ops.refLabel)
	}

	if ops.resetCalls != 1 || ops.decomposeIDCalls != 2 || ops.purgeCalls != 1 || ops.relPurgeCalls != 1 ||
		ops.resolveExactEraseCalls != 1 || ops.exactEraseCalls != 1 || ops.segmentCalls != 2 {
		t.Fatalf("unexpected call counts: %+v", ops)
	}
}

type adminOpsSpy struct {
	resetCalls             int
	decomposeIDCalls       int
	compactCalls           int
	purgeCalls             int
	relPurgeCalls          int
	resolveExactEraseCalls int
	exactEraseCalls        int
	segmentCalls           int
	storedLabel            string
	storedDepth            store.ShardDepth
	untimedMode            UntimedValidStart
	refLabel               string
}

func (s *adminOpsSpy) IsReferenceLabel(label string) (bool, error) {
	s.refLabel = label
	return true, nil
}

func (s *adminOpsSpy) SetUntimedValidStart(_ context.Context, mode UntimedValidStart) error {
	s.untimedMode = mode
	return nil
}

func (s *adminOpsSpy) UntimedValidStart() (UntimedValidStart, bool, error) {
	return s.untimedMode, true, nil
}

func (s *adminOpsSpy) ValidAxis() (types.ValidAxis, error) {
	if s.untimedMode == UntimedWholeAxis {
		return types.LiteralValidAxis, nil
	}
	return types.LegacyValidAxis, nil
}

func (s *adminOpsSpy) SealRelSegments(relType string) error {
	s.segmentCalls++
	return nil
}

func (s *adminOpsSpy) RelSegmentStats(relType string) (RelSegmentStats, error) {
	s.segmentCalls++
	return RelSegmentStats{Segments: 3}, nil
}

func (s *adminOpsSpy) NodeStoredSizes(id types.NodeID) ([]StoredRow, error) {
	return []StoredRow{{Version: uint32(id), Current: true, Bytes: 10 * int(id)}}, nil
}

func (s *adminOpsSpy) ForEachNodeStoredSize(_ context.Context, label string, depth store.ShardDepth, fn func(types.NodeID, []StoredRow) bool) error {
	s.storedLabel, s.storedDepth = label, depth
	fn(8, []StoredRow{{Version: 8}})
	return nil
}

func (s *adminOpsSpy) Reset() error {
	s.resetCalls++
	return nil
}

func (s *adminOpsSpy) DecomposeNodeID(id types.NodeID) IDComponents {
	s.decomposeIDCalls++
	return IDComponents{NodeID: 7}
}

func (s *adminOpsSpy) DecomposeRelID(id types.RelID) IDComponents {
	s.decomposeIDCalls++
	return IDComponents{NodeID: 7}
}

func (s *adminOpsSpy) CompactHistoryNodes(ctx context.Context, policy RetentionPolicy) (CompactReport, error) {
	s.compactCalls++
	return CompactReport{}, nil
}

func (s *adminOpsSpy) CompactHistoryRels(ctx context.Context, policy RetentionPolicy) (CompactReport, error) {
	s.compactCalls++
	return CompactReport{}, nil
}

func (s *adminOpsSpy) PurgeExpiredNodes(ctx context.Context, policy PurgePolicy) (PurgeReport, error) {
	s.purgeCalls++
	return PurgeReport{}, nil
}

func (s *adminOpsSpy) PurgeExpiredRels(ctx context.Context, policy RelPurgePolicy) (RelPurgeReport, error) {
	s.relPurgeCalls++
	return RelPurgeReport{RelsPurged: 5}, nil
}

func (s *adminOpsSpy) ResolveExactErasure(ctx context.Context, request ExactErasureRequest) (ExactErasureResolution, error) {
	s.resolveExactEraseCalls++
	return ExactErasureResolution{Request: request}, nil
}

func (s *adminOpsSpy) ExactErase(ctx context.Context, request ExactErasureRequest) (ExactErasureReceipt, error) {
	s.exactEraseCalls++
	return ExactErasureReceipt{}, nil
}
