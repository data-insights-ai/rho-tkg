package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAPINilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	if err := nilAPI.Reset(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil Reset = %v, want ErrNilGraph", err)
	}
	if got := nilAPI.DecomposeNodeID(1); got != (IDComponents{}) {
		t.Fatalf("nil DecomposeNodeID = %+v, want zero", got)
	}
	if got := nilAPI.DecomposeRelID(1); got != (IDComponents{}) {
		t.Fatalf("nil DecomposeRelID = %+v, want zero", got)
	}
	if _, err := nilAPI.ExactErase(context.Background(), ExactErasureRequest{NodeIDs: []types.NodeID{1}}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ExactErase = %v, want ErrNilGraph", err)
	}
	if _, err := nilAPI.ResolveExactErasure(context.Background(), ExactErasureRequest{NodeIDs: []types.NodeID{1}}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ResolveExactErasure = %v, want ErrNilGraph", err)
	}
	if err := nilAPI.SealRelSegments("HOP"); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil SealRelSegments = %v, want ErrNilGraph", err)
	}
	if _, err := nilAPI.PurgeExpiredRels(context.Background(), RelPurgePolicy{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil PurgeExpiredRels = %v, want ErrNilGraph", err)
	}
	if _, err := nilAPI.RelSegmentStats("HOP"); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil RelSegmentStats = %v, want ErrNilGraph", err)
	}
	if err := nilAPI.SetUntimedValidStart(context.Background(), UntimedUnbounded); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil SetUntimedValidStart = %v, want ErrNilGraph", err)
	}
	if _, err := nilAPI.ValidAxis(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ValidAxis = %v, want ErrNilGraph", err)
	}
	if _, _, err := nilAPI.UntimedValidStart(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil UntimedValidStart = %v, want ErrNilGraph", err)
	}

	api := New((*adminOpsSpy)(nil))
	if err := api.Reset(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Reset = %v, want ErrNilGraph", err)
	}
	if got := api.DecomposeNodeID(1); got != (IDComponents{}) {
		t.Fatalf("typed-nil DecomposeNodeID = %+v, want zero", got)
	}
}

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

	if err := api.SetUntimedValidStart(context.Background(), UntimedUnbounded); err != nil || ops.untimedMode != UntimedUnbounded {
		t.Fatalf("SetUntimedValidStart: %v, spy got %v", err, ops.untimedMode)
	}
	if axis, err := api.ValidAxis(); err != nil || axis.Literal() {
		t.Fatalf("ValidAxis through the API: %+v %v, want the legacy axis", axis, err)
	}
	if mode, complete, err := api.UntimedValidStart(); err != nil || mode != UntimedUnbounded || !complete {
		t.Fatalf("UntimedValidStart through the API: %v %v %v, want the spy's UntimedUnbounded, true", mode, complete, err)
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
	untimedMode            UntimedValidStart
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
