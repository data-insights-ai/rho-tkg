package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

func TestTypedSegmentCorrectionRefusesBeforeAppendingHistory(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{RelSegments: []graphpkg.RelSegmentSpec{{Type: "EPOCH"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	ctx := t.Context()
	a, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(ctx, "EPOCH", a, b, map[string]any{"tkg_valid_from": int64(-1000)})
	if err != nil {
		t.Fatal(err)
	}
	before, err := g.Rels().History(r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetRelVersionBounds(ctx, r.ID(), typedRange(-100, 0), nil); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatal("unsupported segment correction accepted", err)
	}
	after, err := g.Rels().History(r.ID())
	if err != nil || len(after) != len(before) {
		t.Fatal("unsupported correction appended partial history", before, after, err)
	}
	got, err := g.Rels().Get(ctx, r.ID())
	if err != nil || got.Version() != r.Version() || got.Temporal().ValidEndpoints != 0 {
		t.Fatal("unsupported correction changed current slot", got, err)
	}
}
