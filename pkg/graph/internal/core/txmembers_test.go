package core

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TXMEMBERS (ai-soc 27 Oct, SCALE): badger's label/type tx-membership
// sidecars held one RAM entry per entity that ever carried the token, so on a
// tiered reference shard, whose entities never lose a label, they grew with
// every entity ever seen (k10271800: +127 MB of live heap in ten minutes).
// They now hold only the former members; the current ones are the label and
// type index's.

// TestTxMembersHoldOnlyFormerMembers: on badger (label index in RAM and on
// disk) and on tiered with the label on its reference shard and the label
// index on disk (ai-soc's shape), entities written, updated, corrected in
// valid time (the cascade's history-only rows) and read pinned between the
// writes leave the label and type streams empty; a label removal, a node
// delete and a relationship delete put exactly those entities in them; and
// every pinned read (ByLabel, ByLabelAndProperty, ByType, the effective doors
// over the valid-end index) answers as the memory store does at each pin.
func TestTxMembersHoldOnlyFormerMembers(t *testing.T) {
	t.Parallel()
	stores := []struct {
		name string
		open func(t *testing.T) storepkg.MandatoryStore
	}{
		{"memory", func(*testing.T) storepkg.MandatoryStore { return memory.New() }},
		{"badger", func(t *testing.T) storepkg.MandatoryStore {
			bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		}},
		{"badger-labels-on-disk", func(t *testing.T) storepkg.MandatoryStore {
			bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true, LabelIndexOnDisk: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		}},
		{"tiered-ref-labels-on-disk", func(t *testing.T) storepkg.MandatoryStore {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"E", "F"}, ShardWindow: time.Hour, LabelIndexOnDisk: true})
			if err != nil {
				t.Fatal(err)
			}
			return ts
		}},
	}
	type run struct {
		answers []map[string][]string // per pin
		streams map[string][]string
	}
	runs := make([]run, len(stores))
	for si, sc := range stores {
		ctx := context.Background()
		g, err := New(Config{Store: sc.open(t)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatalf("%s: %v", sc.name, err)
			}
		}
		names := map[int64]string{}
		var nodes []types.NodeID
		var rows []*types.Node
		for i := range 12 {
			labels := []string{"E"}
			if i%3 == 0 {
				labels = append(labels, "F")
			}
			n, err := g.Nodes.Add(ctx, labels, map[string]any{"k": int64(i), "tkg_valid_from": types.Instant(1000)})
			must(err)
			names[int64(n.ID())] = fmt.Sprintf("n%d", i)
			nodes, rows = append(nodes, n.ID()), append(rows, n)
		}
		var rels []types.RelID
		for i := 4; i < 11; i++ {
			r, err := g.Rels.Add(ctx, "R", rows[i], rows[i+1], map[string]any{"w": int64(i), "tkg_valid_from": types.Instant(1000)})
			must(err)
			names[int64(r.ID())] = fmt.Sprintf("r%d", i)
			rels = append(rels, r.ID())
		}
		render := func(ids []int64) []string {
			var out []string
			for _, id := range ids {
				out = append(out, names[id])
			}
			slices.Sort(out)
			return out
		}
		// bound renders a valid-time bound; one an update stamped from this
		// graph's clock is "t" (the graphs' clocks differ).
		bound := func(v types.Instant) string {
			if v > 1_000_000 {
				return "t"
			}
			return fmt.Sprint(int64(v))
		}
		read := func(pin types.Instant) map[string][]string {
			out := map[string][]string{}
			opts := storepkg.QueryOpts{TxPin: pin}
			for _, label := range []string{"E", "F"} {
				ns, err := g.Nodes.ByLabel(label, opts)
				must(err)
				var ids []int64
				for _, n := range ns {
					ids = append(ids, int64(n.ID()))
				}
				out["ByLabel "+label] = render(ids)
			}
			ns, err := g.Nodes.ByLabelAndProperty("E", "k", int64(7), opts)
			must(err)
			var ids []int64
			for _, n := range ns {
				ids = append(ids, int64(n.ID()))
			}
			out["ByLabelAndProperty E k=7"] = render(ids)
			rs, err := g.Rels.ByType("R", opts)
			must(err)
			ids = nil
			for _, r := range rs {
				ids = append(ids, int64(r.ID()))
			}
			out["ByType R"] = render(ids)
			var segs []string
			must(g.Temporal.ForEachNodeEffectiveByLabelsFrom([]string{"E"}, pin, 0, func(s temporal.NodeSegment) bool {
				segs = append(segs, fmt.Sprintf("%s[%s,%s)", names[int64(s.Node.ID())], bound(s.ValidFrom), bound(s.ValidTo)))
				return true
			}))
			slices.Sort(segs)
			out["NodeEffectiveByLabelsFrom E"] = segs
			segs = nil
			must(g.Temporal.ForEachRelEffectiveByTypeFrom("R", pin, 0, func(s temporal.RelSegment) bool {
				segs = append(segs, fmt.Sprintf("%s[%s,%s)", names[int64(s.Rel.ID())], bound(s.ValidFrom), bound(s.ValidTo)))
				return true
			}))
			slices.Sort(segs)
			out["RelEffectiveByTypeFrom R"] = segs
			return out
		}
		streams := func() map[string][]string {
			out := map[string][]string{}
			if lc, ok := g.store.(storepkg.LabelTxMembershipCapability); ok {
				tok, _ := g.labels.Lookup("E")
				var ids []int64
				must(lc.ForEachLabelTxMember(tok, func(id types.NodeID, _ types.Instant) bool {
					ids = append(ids, int64(id))
					return true
				}))
				out["E"] = render(ids)
			}
			if rc, ok := g.store.(storepkg.RelTypeTxMembershipCapability); ok {
				tok, _ := g.lookupRelTypeQueryToken("R")
				var ids []int64
				must(rc.ForEachRelTypeTxMember(tok, func(id types.RelID, _ types.Instant) bool {
					ids = append(ids, int64(id))
					return true
				}))
				out["R"] = render(ids)
			}
			return out
		}
		pin := func() types.Instant {
			p, err := g.Temporal.NowTx()
			must(err)
			return p
		}

		var pins []types.Instant
		pins = append(pins, pin())
		read(pins[0]) // builds the sidecars and tracks the valid-end indexes
		// Current rows written after the build: a create and a label add.
		n, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"k": int64(12), "tkg_valid_from": types.Instant(1000)})
		must(err)
		names[int64(n.ID())] = "n12"
		nodes, rows = append(nodes, n.ID()), append(rows, n)
		r, err := g.Rels.Add(ctx, "R", rows[11], n, map[string]any{"w": int64(11), "tkg_valid_from": types.Instant(1000)})
		must(err)
		names[int64(r.ID())] = "r11"
		rels = append(rels, r.ID())
		must(g.Nodes.AddLabel(ctx, nodes[1], "F"))
		for round := range 2 {
			for i, id := range nodes {
				_, err := g.Nodes.Update(ctx, id, map[string]any{"k": int64(i + 100*(round+1))})
				must(err)
				if i%2 == 0 {
					_, err := g.Temporal.SetNodeVersionInterval(ctx, id, 1500, 1600, map[string]any{"k": int64(7)})
					must(err)
				}
			}
			for i, id := range rels {
				_, err := g.Rels.Update(ctx, id, map[string]any{"w": int64(i + 100*(round+1))})
				must(err)
			}
			pins = append(pins, pin())
			read(pins[len(pins)-1])
		}
		if sc.name != "memory" {
			if s := streams(); len(s["E"]) != 0 || len(s["R"]) != 0 {
				t.Fatalf("%s: after updates and corrections the former-member streams are %v, want empty", sc.name, s)
			}
		}
		must(g.Nodes.RemoveLabel(ctx, nodes[3], "E")) // n3 keeps F
		must(g.Nodes.Delete(ctx, nodes[2]))
		must(g.Rels.Delete(ctx, rels[0]))
		pins = append(pins, pin())

		runs[si].streams = streams()
		for _, p := range pins {
			runs[si].answers = append(runs[si].answers, read(p))
		}
	}
	for si := 1; si < len(stores); si++ {
		if got, want := runs[si].streams, (map[string][]string{"E": {"n2", "n3"}, "R": {"r4"}}); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: former-member streams %v, want %v", stores[si].name, got, want)
		}
		for p, answers := range runs[si].answers {
			for k, want := range runs[0].answers[p] {
				if got := answers[k]; !slices.Equal(got, want) {
					t.Errorf("%s pin %d %s = %v, memory %v", stores[si].name, p, k, got, want)
				}
			}
		}
	}
}
