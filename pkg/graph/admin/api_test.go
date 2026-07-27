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
	if _, err := api.ExactErase(context.Background(), ExactErasureRequest{NodeIDs: []types.NodeID{1}}); err != nil {
		t.Fatalf("ExactErase: %v", err)
	}

	if ops.resetCalls != 1 || ops.decomposeIDCalls != 2 || ops.purgeCalls != 1 || ops.exactEraseCalls != 1 {
		t.Fatalf("unexpected call counts: %+v", ops)
	}
}

type adminOpsSpy struct {
	resetCalls       int
	decomposeIDCalls int
	compactCalls     int
	purgeCalls       int
	exactEraseCalls  int
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

func (s *adminOpsSpy) ExactErase(ctx context.Context, request ExactErasureRequest) (ExactErasureReceipt, error) {
	s.exactEraseCalls++
	return ExactErasureReceipt{}, nil
}
