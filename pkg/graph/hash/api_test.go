package hash

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestAPINilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	if got, err := nilAPI.VerifyNodeChain(1); got || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil VerifyNodeChain = (%v, %v), want (false, ErrNilGraph)", got, err)
	}
	if got, err := nilAPI.VerifyRelChain(2); got || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil VerifyRelChain = (%v, %v), want (false, ErrNilGraph)", got, err)
	}

	api := New((*hashOpsSpy)(nil))
	if got, err := api.VerifyNodeChain(1); got || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil VerifyNodeChain = (%v, %v), want (false, ErrNilGraph)", got, err)
	}
	if got, err := api.VerifyRelChain(2); got || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil VerifyRelChain = (%v, %v), want (false, ErrNilGraph)", got, err)
	}
}

func TestAPIForwardsMethodsAndErrors(t *testing.T) {
	t.Parallel()

	nodeErr := errors.New("node verify failed")
	relErr := errors.New("rel verify failed")
	ops := &hashOpsSpy{nodeOK: true, relOK: true, nodeErr: nodeErr, relErr: relErr}
	api := New(ops)

	nodeID := types.NodeID(11)
	relID := types.RelID(12)
	if got, err := api.VerifyNodeChain(nodeID); !got || !errors.Is(err, nodeErr) {
		t.Fatalf("VerifyNodeChain = (%v, %v), want (true, %v)", got, err, nodeErr)
	}
	if got, err := api.VerifyRelChain(relID); !got || !errors.Is(err, relErr) {
		t.Fatalf("VerifyRelChain = (%v, %v), want (true, %v)", got, err, relErr)
	}
	if ops.nodeID != nodeID || ops.relID != relID {
		t.Fatalf("forwarded ids = node %d rel %d", ops.nodeID, ops.relID)
	}
	if ops.nodeCalls != 1 || ops.relCalls != 1 {
		t.Fatalf("call counts = node %d rel %d, want 1/1", ops.nodeCalls, ops.relCalls)
	}
}

type hashOpsSpy struct {
	nodeID types.NodeID
	relID  types.RelID

	nodeOK bool
	relOK  bool

	nodeErr error
	relErr  error

	nodeCalls int
	relCalls  int
}

func (s *hashOpsSpy) VerifyNodeChain(id types.NodeID) (bool, error) {
	s.nodeCalls++
	s.nodeID = id
	return s.nodeOK, s.nodeErr
}

func (s *hashOpsSpy) VerifyRelChain(id types.RelID) (bool, error) {
	s.relCalls++
	s.relID = id
	return s.relOK, s.relErr
}
