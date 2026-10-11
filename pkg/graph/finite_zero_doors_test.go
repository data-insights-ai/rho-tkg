package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestFiniteZeroBatchAndIngestCorrectionDoors(t *testing.T) {
	for _, backend := range allStoreBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, mode := range []string{"batch", "strong-sync", "strong-async", "concurrent"} {
				t.Run(mode, func(t *testing.T) {
					g := backend.open(t)
					ctx := t.Context()
					base := types.TemporalRange{From: types.TemporalEndpoint{Value: -1000, Finite: true}}
					n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tag": "old", "tkg_valid_bounds": base})
					if err != nil {
						t.Fatal(err)
					}
					e, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": types.TemporalRange{}})
					if err != nil {
						t.Fatal(err)
					}
					r, err := g.Rels().Add(ctx, "EPOCH", n, e, map[string]any{"tag": "old", "tkg_valid_bounds": base})
					if err != nil {
						t.Fatal(err)
					}
					before := nowTx(t, g)
					bounds := typedRange(-100, 0)
					if mode == "batch" {
						_, err = g.Batch().Run(func(b *graphpkg.BatchBuilder) error {
							if err := b.SetNodeVersionBounds(n.ID(), bounds, map[string]any{"tag": "fixed"}); err != nil {
								return err
							}
							return b.SetRelVersionBounds(r.ID(), bounds, map[string]any{"tag": "fixed"})
						})
					} else {
						for _, m := range ivModes {
							if m.name == mode {
								s, er := g.Ingest().NewSession(m.opts)
								if er != nil {
									t.Fatal(er)
								}
								if er = s.SetNodeVersionBounds(n.ID(), bounds, map[string]any{"tag": "fixed"}); er != nil {
									t.Fatal(er)
								}
								if er = s.SetRelVersionBounds(r.ID(), bounds, map[string]any{"tag": "fixed"}); er != nil {
									t.Fatal(er)
								}
								err = ivSubmit(g, s)
							}
						}
					}
					if err != nil {
						t.Fatal(mode, err)
					}
					for _, tc := range []struct {
						at, pin types.Instant
						tag     string
					}{{-1, before, "old"}, {-1, nowTx(t, g), "fixed"}, {0, nowTx(t, g), "old"}} {
						nd, err := g.Temporal().NodeAtTx(n.ID(), tc.at, tc.pin)
						if err != nil {
							t.Fatal(err)
						}
						if tag, _ := nd.GetProperty("tag"); tag != tc.tag {
							t.Fatal(mode, tc, tag)
						}
						rel, err := g.Temporal().RelAtTx(r.ID(), tc.at, tc.pin)
						if err != nil {
							t.Fatal(err)
						}
						if tag, _ := rel.GetProperty("tag"); tag != tc.tag {
							t.Fatal(mode, tc, tag)
						}
					}
				})
			}
		})
	}
}

func TestTypedBoundsRefusalsDoNotChangeState(t *testing.T) {
	g := allStoreBackends()[0].open(t)
	ctx := t.Context()
	n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": types.TemporalRange{}})
	if err != nil {
		t.Fatal(err)
	}
	invalid := typedRange(0, 0)
	e, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(ctx, "EPOCH", n, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetRelVersionBounds(ctx, r.ID(), invalid, nil); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetNodeVersionBounds(ctx, 0, typedRange(0, 100), nil); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetRelVersionBounds(ctx, 0, typedRange(0, 100), nil); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		if _, err := tx.SetNodeVersionBounds(n.ID(), invalid, nil); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
			t.Fatal(err)
		}
		if _, err := tx.SetRelVersionBounds(r.ID(), invalid, nil); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetNodeVersionBounds(ctx, n.ID(), invalid, nil); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": typedRange(0, 100), "tkg_valid_from": int64(0)}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatal("mixed endpoint representations", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": new(typedRange(0, 100))}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatal("unowned pointer input accepted", err)
	}
	for _, props := range []map[string]any{
		{"tkg_valid_bounds": typedRange(0, 0)},
		{"tkg_valid_bounds": typedRange(0, 100), "tkg_valid_to": int64(0)},
	} {
		if _, err := g.Nodes().Add(ctx, []string{"Ref"}, props); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
			t.Fatal("invalid creation bounds accepted", props, err)
		}
	}
	if _, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": typedRange(0, 100), "tkg_created_at": "unknown"}); err == nil {
		t.Fatal("typed bounds bypassed ordinary temporal input validation")
	}
	var nilAPI *temporal.API
	if _, err := nilAPI.SetNodeVersionBounds(ctx, n.ID(), typedRange(0, 100), nil); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatal(err)
	}
	if _, err := nilAPI.SetRelVersionBounds(ctx, 1, typedRange(0, 100), nil); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetNodeVersionBounds(ctx, n.ID(), typedRange(0, 100), nil); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetRelVersionBounds(ctx, r.ID(), typedRange(0, 100), nil); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatal(err)
	}
}

func TestUnboundedCorrectionPreservesExactLowerInfinity(t *testing.T) {
	g := allStoreBackends()[0].open(t)
	n, err := g.Nodes().Add(t.Context(), []string{"Ref"}, map[string]any{"tag": "old", "tkg_valid_from": int64(-1000)})
	if err != nil {
		t.Fatal(err)
	}
	window := types.TemporalRange{To: types.TemporalEndpoint{Value: 0, Finite: true}}
	if _, err := g.Temporal().SetNodeVersionBounds(t.Context(), n.ID(), window, map[string]any{"tag": "fixed"}); err != nil {
		t.Fatal(err)
	}
	segs, err := g.Temporal().NodeEffectiveTimeline(n.ID(), nowTx(t, g))
	if err != nil || len(segs) != 2 || segs[0].Bounds() != window {
		t.Fatal("unbounded lower endpoint lost", segs, err)
	}
	for _, at := range []types.Instant{-100000, -1000, -1} {
		got, err := g.Temporal().NodeAt(n.ID(), at)
		if err != nil {
			t.Fatal(err)
		}
		if tag, _ := got.GetProperty("tag"); tag != "fixed" {
			t.Fatal(at, tag)
		}
	}
}

func TestTypedRelationshipConstraintChecksExactEpochBounds(t *testing.T) {
	g := allStoreBackends()[0].open(t)
	if err := g.Constraints().Set(temporal.NewConstraintSet(temporal.TemporalConstraint{Kind: temporal.ConstraintRelWithinEndpoints})); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	a, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": typedRange(-100, 0)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_bounds": typedRange(-100, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Add(ctx, "EPOCH", a, b, map[string]any{"tkg_valid_bounds": typedRange(-10, 0)}); err != nil {
		t.Fatal("equal finite0 close rejected", err)
	}
	if _, err := g.Rels().Add(ctx, "EPOCH", a, b, map[string]any{"tkg_valid_bounds": typedRange(0, 10)}); !errors.Is(err, temporal.ErrTemporalConstraint) {
		t.Fatal("relationship started after finite0 endpoint expiry", err)
	}
	if _, err := g.Rels().Add(ctx, "EPOCH", a, b, map[string]any{"tkg_valid_bounds": types.TemporalRange{From: types.TemporalEndpoint{Value: -10, Finite: true}}}); !errors.Is(err, temporal.ErrTemporalConstraint) {
		t.Fatal("unbounded relationship escaped finite0 endpoint", err)
	}
}
