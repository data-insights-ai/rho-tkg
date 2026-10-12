package temporal

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The valid-end bounded scans (backlog 51) of temporalOpsSpy.
func (s *temporalOpsSpy) ForEachRelEffectiveByTypeFrom(typeName string, pin, validFrom types.Instant, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffectiveByTypeFrom")
	s.lastRelType, s.lastPin, s.lastValidFrom = typeName, pin, validFrom
	return s.spyRelSegs(fn)
}

func (s *temporalOpsSpy) ForEachNodeEffectiveByLabelsFrom(labels []string, pin, validFrom types.Instant, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffectiveByLabelsFrom")
	s.lastLabels, s.lastPin, s.lastValidFrom = labels, pin, validFrom
	return s.spyNodeSegs(fn)
}

// TestEffectiveValidFromWrappers — rule 1 for the two backlog-51 wrappers: a
// nil API and a typed-nil Ops return ErrNilGraph without calling anything; a
// ready API forwards type / labels, pin and validFrom verbatim (they differ,
// so a swap is caught), returns the Ops' error unchanged and hands fn the
// segments until it returns false.
func TestEffectiveValidFromWrappers(t *testing.T) {
	t.Parallel()
	labels := []string{"L", "M"}
	const pin, validFrom = types.Instant(91), types.Instant(17)
	n1, n2 := types.NewNode(10, 0, nil), types.NewNode(11, 0, nil)
	r1, r2 := types.NewRelationship(20, 0, 0, 0), types.NewRelationship(21, 0, 0, 0)
	wantErr := errors.New("ops failed")
	for _, rel := range []bool{true, false} {
		call := func(a *API, fnN func(NodeSegment) bool, fnR func(RelSegment) bool) error {
			if rel {
				return a.ForEachRelEffectiveByTypeFrom("T", pin, validFrom, fnR)
			}
			return a.ForEachNodeEffectiveByLabelsFrom(labels, pin, validFrom, fnN)
		}
		never := func(NodeSegment) bool { t.Fatal("fn called"); return false }
		neverR := func(RelSegment) bool { t.Fatal("fn called"); return false }
		for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
			if err := call(api, never, neverR); !errors.Is(err, grapherr.ErrNilGraph) {
				t.Fatalf("rel=%v %s = %v, want ErrNilGraph", rel, name, err)
			}
		}
		spy := &temporalOpsSpy{
			err:      wantErr,
			nodeSegs: []NodeSegment{{ValidFrom: 5, ValidTo: 7, Node: n1}, {ValidFrom: 7, Node: n2}},
			relSegs:  []RelSegment{{ValidFrom: 3, ValidTo: 4, Rel: r1}, {ValidFrom: 9, Rel: r2}},
		}
		var got []any
		err := call(New(spy), func(s NodeSegment) bool { got = append(got, s.Node); return false },
			func(s RelSegment) bool { got = append(got, s.Rel); return false })
		wantCall, first, args := "ForEachNodeEffectiveByLabelsFrom", any(n1), slices.Equal(spy.lastLabels, labels)
		if rel {
			wantCall, first, args = "ForEachRelEffectiveByTypeFrom", r1, spy.lastRelType == "T"
		}
		if !errors.Is(err, wantErr) || !slices.Equal(spy.calls, []string{wantCall}) || spy.lastPin != pin ||
			spy.lastValidFrom != validFrom || !args || len(got) != 1 || got[0] != first {
			t.Fatalf("%s: err %v, calls %v, pin %v, validFrom %v, args %v, segments %v (want 1: fn stopped)",
				wantCall, err, spy.calls, spy.lastPin, spy.lastValidFrom, args, got)
		}
	}
}
