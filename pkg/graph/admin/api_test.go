package admin

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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

	if ops.resetCalls != 1 || ops.decomposeIDCalls != 2 {
		t.Fatalf("unexpected call counts: %+v", ops)
	}
}

type adminOpsSpy struct {
	resetCalls       int
	decomposeIDCalls int
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
