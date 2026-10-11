package core

import (
	"errors"
	"testing"

	store "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestForeignIncomingPreservesExplicitEndpointMeaning(t *testing.T) {
	shapes := []types.TemporalRange{{From: types.TemporalEndpoint{Value: -10, Finite: true}, To: types.TemporalEndpoint{Finite: true}}, {From: types.TemporalEndpoint{Finite: true}, To: types.TemporalEndpoint{Value: 10, Finite: true}}, {}, {To: types.TemporalEndpoint{Finite: true}}}
	for _, shape := range shapes {
		g, end := newForeignIncomingTestGraph(t, ValidationLimits{})
		e := baseForeignEdge(end)
		tm := &types.TemporalMetadata{}
		if err := tm.SetValidBounds(shape); err != nil {
			t.Fatal(err)
		}
		e.ValidFrom, e.ValidTo, e.ValidEndpoints = tm.ValidFrom, tm.ValidTo, tm.ValidEndpoints
		if err := g.Rels.RecordForeignIncoming(t.Context(), e); err != nil {
			t.Fatal(err)
		}
		rows, err := g.Rels.Incoming(end, "KNOWS")
		if err != nil || len(rows) != 1 {
			t.Fatal(rows, err)
		}
		got, explicit := rows[0].Temporal().ValidBounds()
		if !explicit || got != shape {
			t.Fatal("foreign stub lost finite zero or infinity", got, shape)
		}
		// Foreign IDs intentionally cannot be resolved through the local routed
		// RelAt door; the actual incoming-row boundary must retain their typed
		// meaning for the orchestrator that owns cross-machine resolution.
		for _, at := range []types.Instant{-100, -1, 0, 1, 10} {
			if got.Contains(at) != shape.Contains(at) {
				t.Fatal("foreign range changed interval membership", at, got, shape)
			}
		}

	}
}
func TestForeignIncomingInvalidPresenceRefusesBeforeMutation(t *testing.T) {
	for _, tm := range []types.TemporalMetadata{{ValidEndpoints: 2}, {ValidEndpoints: 129}, {ValidEndpoints: types.EndpointsExplicit, ValidFrom: 1}, {ValidEndpoints: 7, ValidTo: -1}} {
		g, end := newForeignIncomingTestGraph(t, ValidationLimits{})
		e := baseForeignEdge(end)
		e.ValidFrom, e.ValidTo, e.ValidEndpoints = tm.ValidFrom, tm.ValidTo, tm.ValidEndpoints
		previous := baseForeignEdge(end)
		previous.RelID += 128
		if err := g.Rels.RecordForeignIncoming(t.Context(), previous); err != nil {
			t.Fatal(err)
		}
		beforeRows, err := g.Rels.Incoming(end, "KNOWS")
		if err != nil || len(beforeRows) != 1 {
			t.Fatal(beforeRows, err)
		}
		before, err := g.Rels.Count()
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Rels.RecordForeignIncoming(t.Context(), e); !errors.Is(err, store.ErrInvalidForeignIncoming) {
			t.Fatal("invalid foreign presence", err)
		}
		after, err := g.Rels.Count()
		if err != nil || after != before {
			t.Fatal("invalid descriptor wrote an edge", after, before, err)
		}
		rows, err := g.Rels.Incoming(end, "KNOWS")
		if err != nil || len(rows) != 1 || rows[0].ID() != previous.RelID || rows[0].Temporal().TxFrom != beforeRows[0].Temporal().TxFrom {
			t.Fatal("invalid descriptor changed the existing stub", rows, err)
		}
	}
}
