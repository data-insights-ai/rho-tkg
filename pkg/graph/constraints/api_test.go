package constraints

import (
	"errors"
	"testing"

	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

func TestAPIForwardsMethodsAndErrors(t *testing.T) {
	t.Parallel()

	setErr := errors.New("set failed")
	addErr := errors.New("add failed")
	wantSet := temporalpkg.NewConstraintSet(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints})
	wantConstraint := temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}
	ops := &constraintsOpsSpy{
		setErr: setErr,
		addErr: addErr,
		getSet: wantSet,
	}
	api := New(ops)

	if err := api.Set(wantSet); !errors.Is(err, setErr) {
		t.Fatalf("Set error = %v, want %v", err, setErr)
	}
	if err := api.Add(wantConstraint); !errors.Is(err, addErr) {
		t.Fatalf("Add error = %v, want %v", err, addErr)
	}
	if got := api.Get(); got.Len() != 1 {
		t.Fatalf("Get length = %d, want 1", got.Len())
	}

	if ops.setArg.Len() != 1 || ops.addArg != wantConstraint {
		t.Fatalf("forwarded args = set len %d add %+v", ops.setArg.Len(), ops.addArg)
	}
	if ops.setCalls != 1 || ops.addCalls != 1 || ops.getCalls != 1 {
		t.Fatalf("call counts = set %d add %d get %d, want 1/1/1", ops.setCalls, ops.addCalls, ops.getCalls)
	}
}

type constraintsOpsSpy struct {
	setArg temporalpkg.ConstraintSet
	addArg temporalpkg.TemporalConstraint
	getSet temporalpkg.ConstraintSet

	setErr error
	addErr error

	setCalls int
	addCalls int
	getCalls int
}

func (s *constraintsOpsSpy) Set(cs temporalpkg.ConstraintSet) error {
	s.setCalls++
	s.setArg = cs
	return s.setErr
}

func (s *constraintsOpsSpy) Add(c temporalpkg.TemporalConstraint) error {
	s.addCalls++
	s.addArg = c
	return s.addErr
}

func (s *constraintsOpsSpy) Get() temporalpkg.ConstraintSet {
	s.getCalls++
	return s.getSet
}
