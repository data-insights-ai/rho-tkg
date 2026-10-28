package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PROPDISKLABEL (ai-soc 28 Oct, SCALE): the on-disk property index chosen per
// label. ai-soc's reduced BA k10272220: PropertyIndex.AddKey +1,720 MB of live
// heap from 300 to 900 s, 1.66 GB of it sigma's DerivedPage nodes; all labels
// on disk cost +7.0 % push, mostly another label's lookups.

// TestPropertyIndexOnDiskLabels: on badger and tiered with label D listed in
// PropertyIndexOnDiskLabels, D's property index is on disk (its stated bounds
// decline) and R's, on the same keys, in RAM (they are stated); equality,
// ordered range, prefix and count answer as the memory store does, a node
// carrying both labels included, before and after a reopen (the labels named
// from the persisted registry before the graph installs its own) and after
// writes following it.
func TestPropertyIndexOnDiskLabels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	disk := []string{"D"}
	badgerDir, tieredDir := t.TempDir(), t.TempDir()
	stores := []struct {
		name string
		open func(t *testing.T) *Core
	}{
		{"memory", func(t *testing.T) *Core {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"badger", func(t *testing.T) *Core {
			g, err := New(Config{BadgerDir: badgerDir, PropertyIndexOnDiskLabels: disk})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"tiered", func(t *testing.T) *Core {
			ts, err := tiered.New(tiered.Config{DataDir: tieredDir, ShardWindow: time.Hour, PropertyIndexOnDiskLabels: disk})
			if err != nil {
				t.Fatal(err)
			}
			g, err := New(Config{Store: ts})
			if err != nil {
				_ = ts.Close()
				t.Fatal(err)
			}
			return g
		}},
	}
	type state struct {
		g     *Core
		names map[types.NodeID]string
		ids   map[string]types.NodeID
	}
	read := func(t *testing.T, s *state) map[string]string {
		t.Helper()
		out := map[string]string{}
		render := func(ns []*types.Node) string {
			var names []string
			for _, n := range ns {
				names = append(names, s.names[n.ID()])
			}
			return fmt.Sprint(names)
		}
		for _, label := range []string{"D", "R"} {
			for h := range int64(4) {
				ns, err := s.g.Nodes.ByLabelAndProperty(label, "h", h, storepkg.QueryOpts{})
				if err != nil {
					t.Fatal(err)
				}
				out[fmt.Sprintf("%s h=%d", label, h)] = render(ns)
				if c, exact, err := s.g.Nodes.CountByLabelPropertyValue(label, "h", h, storepkg.QueryOpts{}); err != nil {
					t.Fatal(err)
				} else if exact && int(c) != len(ns) {
					t.Fatalf("%s h=%d: count %d, %d nodes", label, h, c, len(ns))
				}
			}
			var ns []*types.Node
			if err := s.g.Nodes.ForEachByLabelPropertyRangeOrdered(label, "h", 1, 2, true, true, false, storepkg.QueryOpts{}, func(n *types.Node) bool {
				ns = append(ns, n)
				return true
			}); err != nil {
				t.Fatal(err)
			}
			out[label+" h in [1,2]"] = render(ns)
			ns = nil
			if err := s.g.Nodes.ForEachByLabelPropertyPrefix(label, "s", "v1", false, storepkg.QueryOpts{}, func(n *types.Node) bool {
				ns = append(ns, n)
				return true
			}); err != nil {
				t.Fatal(err)
			}
			out[label+" s v1*"] = render(ns)
		}
		return out
	}
	// stated reports which labels' bounds on h are stated: an index kept on
	// disk states none.
	stated := func(t *testing.T, s *state) string {
		t.Helper()
		var out []string
		for _, label := range []string{"D", "R"} {
			_, ok, err := s.g.Stats.PropertyBounds(label, "h", storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				out = append(out, label)
			}
		}
		return fmt.Sprint(out)
	}
	add := func(t *testing.T, s *state, name string, h int64, labels ...string) {
		t.Helper()
		n, err := s.g.Nodes.Add(ctx, labels, map[string]any{"h": h, "s": "v" + fmt.Sprint(len(s.ids))})
		if err != nil {
			t.Fatal(err)
		}
		s.names[n.ID()], s.ids[name] = name, n.ID()
	}
	update := func(t *testing.T, s *state, name string, h int64) {
		t.Helper()
		if _, err := s.g.Nodes.Update(ctx, s.ids[name], map[string]any{"h": h}); err != nil {
			t.Fatal(err)
		}
	}
	runs := make([][]map[string]string, len(stores))
	for si, sc := range stores {
		s := &state{g: sc.open(t), names: map[types.NodeID]string{}, ids: map[string]types.NodeID{}}
		for _, label := range []string{"D", "R"} {
			for _, key := range []string{"h", "s"} {
				if err := s.g.Index.CreateProperty(label, key); err != nil {
					t.Fatalf("%s: %v", sc.name, err)
				}
			}
		}
		for i := range 12 {
			labels := [][]string{{"D"}, {"R"}, {"D", "R"}}[i%3]
			add(t, s, fmt.Sprintf("n%d", i), int64(i%4), labels...)
		}
		update(t, s, "n0", 3)
		update(t, s, "n2", 1)
		if err := s.g.Nodes.Delete(ctx, s.ids["n3"]); err != nil {
			t.Fatal(err)
		}
		runs[si] = append(runs[si], read(t, s))
		if sc.name != "memory" {
			if got := stated(t, s); got != "[R]" {
				t.Fatalf("%s: bounds stated for %s, want [R] (D on disk)", sc.name, got)
			}
			if err := s.g.Close(); err != nil {
				t.Fatal(err)
			}
			s.g = sc.open(t)
			if got := stated(t, s); got != "[R]" {
				t.Fatalf("%s reopened: bounds stated for %s, want [R] (D on disk)", sc.name, got)
			}
		}
		runs[si] = append(runs[si], read(t, s))
		add(t, s, "n12", 2, "D", "R")
		update(t, s, "n5", 0)
		update(t, s, "n8", 2)
		runs[si] = append(runs[si], read(t, s))
		if err := s.g.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for si := 1; si < len(stores); si++ {
		for step, answers := range runs[si] {
			for k, want := range runs[0][step] {
				if got := answers[k]; got != want {
					t.Errorf("%s step %d %s = %s, memory %s", stores[si].name, step, k, got, want)
				}
			}
		}
	}
}
