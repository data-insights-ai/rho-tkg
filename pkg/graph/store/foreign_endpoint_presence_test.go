package store

import (
	"errors"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"testing"
)

func TestForeignIncomingEndpointPresenceValidation(t *testing.T) {
	base := ForeignIncomingEdge{RelID: 1, TypeName: "LINK", StartID: 2, EndID: 3, AttestTx: 1}
	if err := base.Validate(); err != nil {
		t.Fatal("legacydescriptor", err)
	}
	for _, shape := range []types.TemporalRange{{}, {From: types.TemporalEndpoint{Finite: true}, To: types.TemporalEndpoint{Value: 1, Finite: true}}, {From: types.TemporalEndpoint{Value: -1, Finite: true}, To: types.TemporalEndpoint{Finite: true}}} {
		tm := &types.TemporalMetadata{}
		if err := tm.SetValidBounds(shape); err != nil {
			t.Fatal(err)
		}
		e := base
		e.ValidFrom, e.ValidTo, e.ValidEndpoints = tm.ValidFrom, tm.ValidTo, tm.ValidEndpoints
		if err := e.Validate(); err != nil {
			t.Fatal(shape, err)
		}
	}
	bad := base
	bad.ValidEndpoints = 2
	if err := bad.Validate(); !errors.Is(err, ErrInvalidForeignIncoming) || !errors.Is(err, types.ErrInvalidTemporalRange) {
		t.Fatal("invalidflagclass", err)
	}
}
