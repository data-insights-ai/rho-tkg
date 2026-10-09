package graphstate

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestCoverageWalkDomainBoundariesAndSingletonGaps(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: profile, Version: 1, Reference: "coverage", CanonicalUnit: "step"}, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		position := func(n int64) temporal.Position {
			if profile == temporal.ProfileIntegerZ {
				p, _ := temporal.IntegerPosition(a, temporal.Int64(n))
				return p
			}
			if profile == temporal.ProfileRationalQ {
				p, _ := temporal.RationalPosition(a, temporal.RationalInt64(n))
				return p
			}
			p, _ := temporal.LexPosition(a, temporal.RationalInt64(0), temporal.Int64(n))
			return p
		}
		span := func(lo, hi int64, lc, uc bool) temporal.Scope {
			lb, _ := temporal.FiniteBound(position(lo), lc)
			hb, _ := temporal.FiniteBound(position(hi), uc)
			s, err := temporal.Span(a, lb, hb, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
		p0, _ := temporal.Point(position(0))
		p1, _ := temporal.Point(position(1))
		whole := span(0, 1, true, true)
		covered, err := coveredByAtoms(whole, []temporal.Scope{p0, p1}, temporal.Limits{})
		if err != nil || covered != (profile != temporal.ProfileRationalQ) {
			t.Fatal(profile, covered, err)
		}
		if covered, err := coveredByAtoms(span(0, 3, true, true), []temporal.Scope{span(0, 1, true, false), span(2, 3, true, true)}, temporal.Limits{}); err != nil || covered {
			t.Fatal("missing interval hidden", profile, covered, err)
		}
		cursor := ownedCursor{parts: span(0, 3, true, true).Parts()}
		if err := cursor.consume(span(0, 1, true, true), temporal.Limits{}); err != nil {
			t.Fatal(err)
		}
		err = cursor.consume(span(1, 3, true, true), temporal.Limits{})
		if !errors.Is(err, ErrContradictoryRead) {
			t.Fatal("overlapping closed boundary accepted", err)
		}
		cursor = ownedCursor{parts: whole.Parts()}
		if err := cursor.consume(whole, temporal.Limits{}); err != nil {
			t.Fatal(err)
		}
		if err := cursor.consume(whole, temporal.Limits{}); !errors.Is(err, ErrContradictoryRead) {
			t.Fatal(err)
		}
	}
}
func TestPagedDataInvalidAxesAndPieceBudgets(t *testing.T) {
	for _, mode := range []string{"invalid state", "foreign data axis", "outside owned", "piece budget", "whole gap"} {
		t.Run(mode, func(t *testing.T) {
			v := newFixtureView(t)
			v.pageHook = func(q ComponentQuery, _ Cursor, p ComponentPage) ComponentPage {
				switch mode {
				case "invalid state":
					p.Data = state.State{}
				case "foreign data axis":
					d := v.axis.Descriptor()
					d.ID = temporal.AxisID{9}
					a, _ := temporal.NewAxis(d, temporal.Limits{})
					p.Data, _ = state.New(a, state.Limits{})
				case "outside owned":
					s, _ := state.New(v.axis, state.Limits{})
					ref, _ := state.NewValueRef(1, 0)
					r, _ := state.NewRevision(1, 0)
					result, _ := s.Set(testSpan(t, v.axis, 0, 20), ref, r, state.Limits{})
					p.Data = result.State()
				case "piece budget":
					s, _ := state.New(v.axis, state.Limits{})
					ref, _ := state.NewValueRef(1, 0)
					for n := int64(0); n < 5; n++ {
						point, _ := temporal.Point(testPosition(t, v.axis, n))
						r, _ := state.NewRevision(uint64(n+1), 0)
						result, _ := s.Set(point, ref, r, state.Limits{})
						s = result.State()
					}
					p.Data = s
				case "whole gap":
					p = clipPage(t, p, testSpan(t, v.axis, 1, 10))
				}
				return p
			}
			r, _ := state.NewRevision(10, 0)
			l := Limits{}
			if mode == "piece budget" {
				l.MaxRows = 4
			}
			_, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 0, 10)}}, r, l)
			if err == nil {
				t.Fatal("reader error accepted")
			}
			if mode == "piece budget" && !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
		})
	}
}
