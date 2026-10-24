package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// SAMESTART (sigma, 24 Oct). A node whose rows all start at the same instant
// is closed at 233, re-opened over [92, open) and closed again at 93. The
// close to 93 is the newest belief, so no instant at or after 93 is valid:
// NodeAt(100) and the effective timeline must agree. The fast resolve path
// took the chain as monotonic and returned an older row ([92,233)) that the
// replacing writes had superseded; after an excise of [92,93) it returned the
// resumption [93,233). sigma met it closing session-open facts in place.
func TestSameStartChainCloseIsNotUndone(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cfg := graphpkg.Config{SnowflakeNodeID: 3}
			if backend == "badger" {
				cfg.BadgerDir = t.TempDir()
			}
			g, err := graphpkg.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()

			n, err := g.Nodes().Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(92)})
			if err != nil {
				t.Fatal(err)
			}
			id := n.ID()
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"tkg_valid_to": types.Instant(233)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, id, 92, 0, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"tkg_valid_to": types.Instant(93)}); err != nil {
				t.Fatal(err)
			}

			check := func(when string) {
				t.Helper()
				for _, at := range []types.Instant{92, 93, 100, 232, 300} {
					got, err := g.Temporal().NodeAt(id, at)
					valid := at < 93
					switch {
					case valid && err != nil:
						t.Errorf("%s: NodeAt(%d): %v, want the [92,93) row", when, at, err)
					case !valid && !errors.Is(err, graphpkg.ErrNoVersionValidAt):
						t.Errorf("%s: NodeAt(%d) = %s, want ErrNoVersionValidAt (closed at 93)", when, at, span(got, err))
					}
				}
				pin, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				segs, err := g.Temporal().NodeEffectiveTimeline(id, pin)
				if err != nil {
					t.Fatalf("%s: effective timeline: %v", when, err)
				}
				for _, s := range segs {
					if s.ValidTo == 0 || s.ValidTo > 93 {
						t.Errorf("%s: effective timeline holds [%d,%d), past the close at 93 (all: %s)", when, s.ValidFrom, s.ValidTo, segList(segs))
						break
					}
				}
			}
			check("after the close to 93")
			if err := g.Temporal().ExciseNodeValidity(ctx, id, 92, 93); err != nil {
				t.Fatal(err)
			}
			for _, at := range []types.Instant{92, 100, 232} {
				if got, err := g.Temporal().NodeAt(id, at); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
					t.Errorf("after excising [92,93): NodeAt(%d) = %s, want ErrNoVersionValidAt", at, span(got, err))
				}
			}
		})
	}
}

func span(n *types.Node, err error) string {
	if err != nil {
		return err.Error()
	}
	tm := n.Temporal()
	if tm == nil {
		return "a row without temporal metadata"
	}
	return fmt.Sprintf("the row [%d,%d)", tm.ValidFrom, tm.ValidTo)
}

func segList(segs []temporal.NodeSegment) string {
	out := ""
	for _, s := range segs {
		out += fmt.Sprintf("[%d,%d) ", s.ValidFrom, s.ValidTo)
	}
	return out
}

// TestSameStartRelChainCloseIsNotUndone is the relationship form: RelAt and the
// relationship effective timeline share the resolver's arm choice
// (sortRelChainForResolve).
func TestSameStartRelChainCloseIsNotUndone(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cfg := graphpkg.Config{SnowflakeNodeID: 3}
			if backend == "badger" {
				cfg.BadgerDir = t.TempDir()
			}
			g, err := graphpkg.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"tkg_valid_from": types.Instant(1)})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"tkg_valid_from": types.Instant(1)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().AddByID(ctx, "LINK", a.ID(), b.ID(), map[string]any{"tkg_valid_from": types.Instant(92)})
			if err != nil {
				t.Fatal(err)
			}
			id := r.ID()
			if _, err := g.Rels().Update(ctx, id, map[string]any{"tkg_valid_to": types.Instant(233)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetRelVersionInterval(ctx, id, 92, 0, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(ctx, id, map[string]any{"tkg_valid_to": types.Instant(93)}); err != nil {
				t.Fatal(err)
			}
			for _, at := range []types.Instant{92, 93, 100, 232, 300} {
				got, err := g.Temporal().RelAt(id, at)
				if at < 93 && err != nil {
					t.Errorf("RelAt(%d): %v, want the [92,93) row", at, err)
				}
				if at >= 93 && !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
					state := "a row"
					if err != nil {
						state = err.Error()
					} else if tm := got.Temporal(); tm != nil {
						state = fmt.Sprintf("the row [%d,%d)", tm.ValidFrom, tm.ValidTo)
					}
					t.Errorf("RelAt(%d) = %s, want ErrNoVersionValidAt (closed at 93)", at, state)
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			segs, err := g.Temporal().RelEffectiveTimeline(id, pin)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range segs {
				if s.ValidTo == 0 || s.ValidTo > 93 {
					t.Errorf("relationship effective timeline holds [%d,%d), past the close at 93", s.ValidFrom, s.ValidTo)
				}
			}
			if err := g.Temporal().ExciseRelValidity(ctx, id, 92, 93); err != nil {
				t.Fatal(err)
			}
			for _, at := range []types.Instant{92, 100, 232} {
				if _, err := g.Temporal().RelAt(id, at); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
					t.Errorf("after excising [92,93): RelAt(%d) answers, want ErrNoVersionValidAt", at)
				}
			}
		})
	}
}
