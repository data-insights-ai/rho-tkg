package importmap

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
)

// Replaying the op stream into a naive valid-time overlay, with Recorded as
// knowledge time, must give v4's effective timelines at every tx pin of the oracle.
func TestReplayMatchesEffectiveTimelines(t *testing.T) {
	for _, n := range names {
		raw, err := os.ReadFile(filepath.Join(fixtureDir, n+".expected.json"))
		if err != nil {
			t.Fatal(err)
		}
		var exp fixtures.Expected
		if err := json.Unmarshal(raw, &exp); err != nil {
			t.Fatal(err)
		}
		res, err := Map(load(t, n))
		if err != nil {
			t.Fatal(err)
		}
		for _, tl := range exp.Timelines {
			for _, set := range []struct {
				kind string
				ents []fixtures.EntityTimeline
			}{{"node", tl.Nodes}, {"rel", tl.Rels}} {
				for _, et := range set.ents {
					var ov overlay
					for _, op := range res.Ops {
						if op.Entity == set.kind && op.ID == et.ID && op.Recorded <= tl.Tx {
							ov.apply(op)
						}
					}
					compareTimeline(t, n, set.kind, et, &ov, tl.Tx)
				}
			}
		}
	}
}

func compareTimeline(t *testing.T, fixture, kind string, et fixtures.EntityTimeline, ov *overlay, tx int64) {
	t.Helper()
	pts := map[int64]bool{0: true}
	add := func(b int64) {
		for _, d := range []int64{-1, 0, 1} {
			if b == math.MinInt64 || b == math.MaxInt64 {
				continue
			}
			pts[b+d] = true
		}
	}
	for _, s := range et.Segments {
		add(s.ValidFrom)
		add(s.ValidTo)
		if s.ValidFrom == math.MinInt64 {
			pts[math.MinInt64+1] = true
		}
	}
	for _, s := range ov.segs {
		add(s.lo)
		add(s.hi)
		if s.lo == math.MinInt64 {
			pts[math.MinInt64+1] = true
		}
	}
	var vs []int64
	for p := range pts {
		vs = append(vs, p)
	}
	sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
	for _, v := range vs {
		var want *fixtures.Row
		for i := range et.Segments {
			s := et.Segments[i]
			if s.ValidFrom <= v && (s.ValidTo == 0 || v < s.ValidTo) {
				want = &et.Segments[i].Row
			}
		}
		got := ov.at(v)
		if (want == nil) != (got == nil) {
			t.Errorf("%s %s %d tx %d valid %d: v4 has state %v, replay has %v", fixture, kind, et.ID, tx, v, want != nil, got != nil)
			continue
		}
		if want == nil {
			continue
		}
		wp := map[string]string{}
		for k, ty := range want.Props {
			if val, bad := MapValue(ty); bad == "" {
				b, _ := json.Marshal(PropValue{val})
				wp[k] = string(b)
			}
		}
		gp := map[string]string{}
		for k, pv := range got.props {
			b, _ := json.Marshal(pv)
			gp[k] = string(b)
		}
		if !sameMap(wp, gp) {
			t.Errorf("%s %s %d tx %d valid %d: props v4 %v replay %v", fixture, kind, et.ID, tx, v, wp, gp)
		}
		if kind == "node" {
			wl, gl := map[string]string{}, map[string]string{}
			for _, l := range want.Labels {
				wl[l] = ""
			}
			for l := range got.labels {
				gl[l] = ""
			}
			if !sameMap(wl, gl) {
				t.Errorf("%s node %d tx %d valid %d: labels v4 %v replay %v", fixture, et.ID, tx, v, want.Labels, gl)
			}
		}
	}
}

func sameMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
