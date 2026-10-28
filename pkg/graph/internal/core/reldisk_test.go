package core

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RELDISK (ai-soc 28 Oct, goal 2): sigma's 2b reads edge source rows back
// through a property index at its read pin. A pinned read takes its candidates
// from the property membership sidecar, a RAM structure of every value a row
// ever carried; on BA the edge rows were the biggest grower (k10281507, NETWORK
// +46 MB in 300 s).

// TestPropertyIndexOnDiskPinnedReads: with relationship type R listed in
// RelPropertyIndexOnDiskTypes and label D in PropertyIndexOnDiskLabels, reads
// at four transaction-time pins (TxPin; TxAt with a valid instant) and current
// reads answer as the memory store does
// while values change between the pins (one changed and changed back), before
// and after a reopen and after writes following it; and no read through an
// on-disk definition builds a membership sidecar: on badger only S's (in RAM)
// is built.
func TestPropertyIndexOnDiskPinnedReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	badgerDir, tieredDir := t.TempDir(), t.TempDir()
	relDisk, nodeDisk := []string{"R"}, []string{"D"}
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
			g, err := New(Config{BadgerDir: badgerDir, RelPropertyIndexOnDiskTypes: relDisk, PropertyIndexOnDiskLabels: nodeDisk})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"tiered", func(t *testing.T) *Core {
			ts, err := tiered.New(tiered.Config{DataDir: tieredDir, ShardWindow: time.Hour,
				RelPropertyIndexOnDiskTypes: relDisk, PropertyIndexOnDiskLabels: nodeDisk})
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
		names map[int64]string
		nodes map[string]*types.Node
		rels  map[string]types.RelID
		pins  []types.Instant
	}
	render := func(s *state, ids []int64) string {
		var names []string
		for _, id := range ids {
			names = append(names, s.names[id])
		}
		slices.Sort(names)
		return fmt.Sprint(names)
	}
	relIDs := func(rs []*types.Relationship) []int64 {
		var ids []int64
		for _, r := range rs {
			ids = append(ids, int64(r.ID()))
		}
		return ids
	}
	nodeIDs := func(ns []*types.Node) []int64 {
		var ids []int64
		for _, n := range ns {
			ids = append(ids, int64(n.ID()))
		}
		return ids
	}
	// read answers every query at every pin so far, and the current ones.
	read := func(t *testing.T, s *state) map[string]string {
		t.Helper()
		out := map[string]string{}
		for _, v := range []string{"x", "y", "z"} {
			for _, typ := range []string{"R", "S"} {
				rs, err := s.g.Rels.ByTypeAndProperty(typ, "k", v, storepkg.QueryOpts{})
				if err != nil {
					t.Fatal(err)
				}
				out[fmt.Sprintf("%s k=%s now", typ, v)] = render(s, relIDs(rs))
			}
			ns, err := s.g.Nodes.ByLabelAndProperty("D", "k", v, storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			out[fmt.Sprintf("D k=%s now", v)] = render(s, nodeIDs(ns))
		}
		for i, pin := range s.pins {
			for _, v := range []string{"x", "y", "z"} {
				for _, typ := range []string{"R", "S"} {
					rs, err := s.g.Rels.ByTypeAndProperty(typ, "k", v, storepkg.QueryOpts{TxPin: pin, TxPinSet: true})
					if err != nil {
						t.Fatal(err)
					}
					out[fmt.Sprintf("%s k=%s pin%d", typ, v, i)] = render(s, relIDs(rs))
					rs, err = s.g.Rels.ByTypeAndProperty(typ, "k", v, storepkg.QueryOpts{TxAt: pin, TxAtSet: true, ValidAt: pin, ValidAtSet: true})
					if err != nil {
						t.Fatal(err)
					}
					out[fmt.Sprintf("%s k=%s txat%d", typ, v, i)] = render(s, relIDs(rs))
				}
				ns, err := s.g.Nodes.ByLabelAndProperty("D", "k", v, storepkg.QueryOpts{TxPin: pin, TxPinSet: true})
				if err != nil {
					t.Fatal(err)
				}
				out[fmt.Sprintf("D k=%s pin%d", v, i)] = render(s, nodeIDs(ns))
				ns, err = s.g.Nodes.ByLabelAndProperty("D", "k", v, storepkg.QueryOpts{TxAt: pin, TxAtSet: true, ValidAt: pin, ValidAtSet: true})
				if err != nil {
					t.Fatal(err)
				}
				out[fmt.Sprintf("D k=%s txat%d", v, i)] = render(s, nodeIDs(ns))
			}
		}
		return out
	}
	pin := func(t *testing.T, s *state) {
		t.Helper()
		p, err := s.g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		s.pins = append(s.pins, p)
	}
	addNode := func(t *testing.T, s *state, name, k string, n int64) {
		t.Helper()
		node, err := s.g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": k, "n": n})
		if err != nil {
			t.Fatal(err)
		}
		s.names[int64(node.ID())], s.nodes[name] = name, node
	}
	addRel := func(t *testing.T, s *state, name, typ, from, to, k string, n int64) {
		t.Helper()
		r, err := s.g.Rels.Add(ctx, typ, s.nodes[from], s.nodes[to], map[string]any{"k": k, "n": n})
		if err != nil {
			t.Fatal(err)
		}
		s.names[int64(r.ID())], s.rels[name] = name, r.ID()
	}
	updateRel := func(t *testing.T, s *state, name string, props map[string]any) {
		t.Helper()
		if _, err := s.g.Rels.Update(ctx, s.rels[name], props); err != nil {
			t.Fatal(err)
		}
	}
	updateNode := func(t *testing.T, s *state, name string, props map[string]any) {
		t.Helper()
		if _, err := s.g.Nodes.Update(ctx, s.nodes[name].ID(), props); err != nil {
			t.Fatal(err)
		}
	}
	runs := make([][]map[string]string, len(stores))
	sidecars := map[string]storepkg.PropertyTxMembershipStats{}
	for si, sc := range stores {
		s := &state{g: sc.open(t), names: map[int64]string{}, nodes: map[string]*types.Node{}, rels: map[string]types.RelID{}}
		for _, typ := range []string{"R", "S"} {
			if err := s.g.Index.CreateRelProperty(typ, "k"); err != nil {
				t.Fatalf("%s: %v", sc.name, err)
			}
		}
		if err := s.g.Index.CreateRelProperty("R", "n"); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"k", "n"} {
			if err := s.g.Index.CreateProperty("D", key); err != nil {
				t.Fatal(err)
			}
		}
		addNode(t, s, "a", "x", 1)
		addNode(t, s, "b", "y", 2)
		addNode(t, s, "c", "x", 3)
		addRel(t, s, "r1", "R", "a", "b", "x", 1)
		addRel(t, s, "r2", "R", "b", "c", "y", 2)
		addRel(t, s, "r3", "R", "a", "c", "x", 3)
		addRel(t, s, "s1", "S", "a", "b", "x", 1)
		pin(t, s)
		updateRel(t, s, "r1", map[string]any{"k": "y", "n": int64(3)})
		updateNode(t, s, "a", map[string]any{"k": "z", "n": int64(3)})
		if err := s.g.Rels.Delete(ctx, s.rels["r2"]); err != nil {
			t.Fatal(err)
		}
		updateRel(t, s, "s1", map[string]any{"k": "y"})
		pin(t, s)
		updateRel(t, s, "r1", map[string]any{"k": "x", "n": int64(1)}) // back to its first value
		updateNode(t, s, "a", map[string]any{"k": "x"})
		addRel(t, s, "r4", "R", "c", "a", "z", 1)
		pin(t, s)
		runs[si] = append(runs[si], read(t, s))
		if sc.name != "memory" {
			if err := s.g.Close(); err != nil {
				t.Fatal(err)
			}
			s.g = sc.open(t)
		}
		runs[si] = append(runs[si], read(t, s))
		updateRel(t, s, "r3", map[string]any{"k": "y", "n": int64(2)})
		updateNode(t, s, "b", map[string]any{"k": "x", "n": int64(1)})
		addRel(t, s, "r5", "R", "b", "a", "x", 2)
		pin(t, s)
		runs[si] = append(runs[si], read(t, s))
		if st, ok := s.g.store.(storepkg.PropertyTxMembershipStatsCapability); ok && sc.name == "badger" {
			stats, err := st.PropertyTxMembershipStats()
			if err != nil {
				t.Fatal(err)
			}
			sidecars[sc.name] = stats
		}
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
	if st := sidecars["badger"]; st.RelSidecars != 1 || st.NodeSidecars != 0 {
		t.Errorf("badger: %d rel and %d node membership sidecars built (%d and %d postings), want 1 (S, in RAM) and 0",
			st.RelSidecars, st.NodeSidecars, st.RelPostings, st.NodePostings)
	}
}

// TestPropertyIndexOnDiskMembershipDoors: every membership door (one value,
// a valid-time window, a page, a numeric range, every value) of a definition
// kept on disk answers as the sidecar it replaces: the same graph is written
// to a badger store with R and D in RAM (the sidecars) and to one with them
// on disk; at every pin the members whose first transaction time is at or
// before it agree by name, a window's members on disk are the value's (a
// superset of the sidecar's), and an empty window answers nothing. Before and
// after a reopen; after one without the option (the definitions in RAM, the
// entries stale) and writes, then one with it again (rebuilt).
func TestPropertyIndexOnDiskMembershipDoors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirs := []string{t.TempDir(), t.TempDir()}
	open := func(t *testing.T, i int, disk bool) *Core {
		t.Helper()
		cfg := Config{BadgerDir: dirs[i]}
		if disk {
			cfg.RelPropertyIndexOnDiskTypes, cfg.PropertyIndexOnDiskLabels = []string{"R"}, []string{"D"}
		}
		g, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	type state struct {
		g     *Core
		names map[int64]string
		nodes map[string]*types.Node
		rels  map[string]types.RelID
		pins  []types.Instant
	}
	type member struct {
		id int64
		tx types.Instant
	}
	collect := func(t *testing.T, door func(fn func(id int64, tx types.Instant) bool) error) []member {
		t.Helper()
		var out []member
		if err := door(func(id int64, tx types.Instant) bool { out = append(out, member{id, tx}); return true }); err != nil {
			t.Fatal(err)
		}
		return out
	}
	// at renders the members known at pin (first transaction time 0 or at or
	// before it) by name.
	at := func(s *state, ms []member, pin types.Instant) string {
		var names []string
		for _, m := range ms {
			if m.tx == 0 || m.tx <= pin {
				names = append(names, s.names[m.id])
			}
		}
		slices.Sort(names)
		return fmt.Sprint(names)
	}
	set := func(s *state, ms []member) string { return at(s, ms, math.MaxInt64) }
	answers := func(t *testing.T, s *state) (map[string]string, map[string]string) {
		t.Helper()
		out, windows := map[string]string{}, map[string]string{}
		rt, _ := s.g.relTypes.Lookup("R")
		lt, _ := s.g.labels.Lookup("D")
		rels := s.g.store.(interface {
			storepkg.RelPropertyTxMembershipCapability
			storepkg.RelPropertyTxMemberWindowCapability
			storepkg.RelPropertyTxMemberPageCapability
			storepkg.RelPropertyTxMemberRangeCapability
			storepkg.RelPropertyTxMemberAnyCapability
		})
		nodes := s.g.store.(interface {
			storepkg.NodePropertyTxMembershipCapability
			storepkg.NodePropertyTxMemberWindowCapability
			storepkg.NodePropertyTxMemberPageCapability
			storepkg.NodePropertyTxMemberRangeCapability
			storepkg.NodePropertyTxMemberAnyCapability
		})
		record := func(name string, ms []member) {
			for i, pin := range s.pins {
				out[fmt.Sprintf("%s pin%d", name, i)] = at(s, ms, pin)
			}
			out[name+" all"] = set(s, ms)
		}
		for _, v := range []string{"x", "y", "z"} {
			vk := types.IndexablePropertyValueKey(v)
			record("R k="+v, collect(t, func(fn func(int64, types.Instant) bool) error {
				return rels.ForEachRelPropertyTxMember(rt, "k", vk, func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
			}))
			record("D k="+v, collect(t, func(fn func(int64, types.Instant) bool) error {
				return nodes.ForEachNodePropertyTxMember(lt, "k", vk, func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
			}))
			for i, pin := range s.pins {
				windows[fmt.Sprintf("R k=%s window%d", v, i)] = set(s, collect(t, func(fn func(int64, types.Instant) bool) error {
					return rels.ForEachRelPropertyTxMemberDuring(rt, "k", vk, pin, pin+1, pin, func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
				}))
				windows[fmt.Sprintf("D k=%s window%d", v, i)] = set(s, collect(t, func(fn func(int64, types.Instant) bool) error {
					return nodes.ForEachNodePropertyTxMemberDuring(lt, "k", vk, pin, pin+1, pin, func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
				}))
			}
			out["R k="+v+" empty window"] = set(s, collect(t, func(fn func(int64, types.Instant) bool) error {
				return rels.ForEachRelPropertyTxMemberDuring(rt, "k", vk, 5, 5, 0, func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
			}))
			// Pages of one, in ID order (the creation order in both stores).
			var pages []string
			after := types.RelID(0)
			for range 10 {
				page := collect(t, func(fn func(int64, types.Instant) bool) error {
					return rels.ForEachRelPropertyTxMemberAfter(rt, "k", vk, after, 1, func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
				})
				if len(page) == 0 {
					break
				}
				if len(page) > 1 {
					t.Fatalf("a page of one held %d members", len(page))
				}
				pages = append(pages, s.names[page[0].id])
				after = types.RelID(page[0].id)
			}
			out["R k="+v+" pages"] = fmt.Sprint(pages)
			var nodePages []string
			nodeAfter := types.NodeID(0)
			for range 10 {
				page := collect(t, func(fn func(int64, types.Instant) bool) error {
					return nodes.ForEachNodePropertyTxMemberAfter(lt, "k", vk, nodeAfter, 1, func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
				})
				if len(page) == 0 {
					break
				}
				if len(page) > 1 {
					t.Fatalf("a page of one held %d members", len(page))
				}
				nodePages = append(nodePages, s.names[page[0].id])
				nodeAfter = types.NodeID(page[0].id)
			}
			out["D k="+v+" pages"] = fmt.Sprint(nodePages)
		}
		record("R n in [1,2]", collect(t, func(fn func(int64, types.Instant) bool) error {
			return rels.ForEachRelPropertyTxMemberInRange(rt, "n", 1, 2, func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
		}))
		record("D n in [1,2]", collect(t, func(fn func(int64, types.Instant) bool) error {
			return nodes.ForEachNodePropertyTxMemberInRange(lt, "n", 1, 2, func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
		}))
		record("R k any", collect(t, func(fn func(int64, types.Instant) bool) error {
			return rels.ForEachRelPropertyTxMemberAny(rt, "k", func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
		}))
		record("D k any", collect(t, func(fn func(int64, types.Instant) bool) error {
			return nodes.ForEachNodePropertyTxMemberAny(lt, "k", func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
		}))
		return out, windows
	}
	pin := func(t *testing.T, s *state) {
		t.Helper()
		p, err := s.g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		s.pins = append(s.pins, p)
	}
	addNode := func(t *testing.T, s *state, name, k string, n float64) {
		t.Helper()
		node, err := s.g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": k, "n": n})
		if err != nil {
			t.Fatal(err)
		}
		s.names[int64(node.ID())], s.nodes[name] = name, node
	}
	addRel := func(t *testing.T, s *state, name, from, to, k string, n float64) {
		t.Helper()
		r, err := s.g.Rels.Add(ctx, "R", s.nodes[from], s.nodes[to], map[string]any{"k": k, "n": n})
		if err != nil {
			t.Fatal(err)
		}
		s.names[int64(r.ID())], s.rels[name] = name, r.ID()
	}
	update := func(t *testing.T, s *state, name string, props map[string]any) {
		t.Helper()
		var err error
		if id, ok := s.rels[name]; ok {
			_, err = s.g.Rels.Update(ctx, id, props)
		} else {
			_, err = s.g.Nodes.Update(ctx, s.nodes[name].ID(), props)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	steps := []func(t *testing.T, s *state){
		func(t *testing.T, s *state) {
			for _, key := range []string{"k", "n"} {
				if err := s.g.Index.CreateRelProperty("R", key); err != nil {
					t.Fatal(err)
				}
				if err := s.g.Index.CreateProperty("D", key); err != nil {
					t.Fatal(err)
				}
			}
			addNode(t, s, "a", "x", 1)
			addNode(t, s, "b", "y", 2)
			addNode(t, s, "c", "x", 3)
			addRel(t, s, "r1", "a", "b", "x", 1)
			addRel(t, s, "r2", "b", "c", "y", 2)
			addRel(t, s, "r3", "a", "c", "x", 2.5)
			pin(t, s)
			update(t, s, "r1", map[string]any{"k": "y", "n": 3.0})
			update(t, s, "a", map[string]any{"k": "z", "n": 2.0})
			update(t, s, "r3", map[string]any{"n": 1.5}) // k unchanged
			if err := s.g.Rels.Delete(ctx, s.rels["r2"]); err != nil {
				t.Fatal(err)
			}
			pin(t, s)
			update(t, s, "r1", map[string]any{"k": "x"}) // back to its first value
			update(t, s, "a", map[string]any{"k": "x"})
			addRel(t, s, "r4", "c", "a", "z", 1)
			pin(t, s)
		},
		func(t *testing.T, s *state) { // written while the on-disk store keeps them in RAM
			update(t, s, "r4", map[string]any{"k": "y"})
			update(t, s, "b", map[string]any{"k": "z", "n": 1.0})
			addRel(t, s, "r5", "b", "a", "z", 2)
			pin(t, s)
		},
		func(t *testing.T, s *state) {
			update(t, s, "r5", map[string]any{"k": "x"})
			addNode(t, s, "d", "y", 1.5)
			pin(t, s)
		},
	}
	states := []*state{
		{g: open(t, 0, false), names: map[int64]string{}, nodes: map[string]*types.Node{}, rels: map[string]types.RelID{}},
		{g: open(t, 1, true), names: map[int64]string{}, nodes: map[string]*types.Node{}, rels: map[string]types.RelID{}},
	}
	compare := func(t *testing.T, phase string) {
		t.Helper()
		ram, ramWindows := answers(t, states[0])
		disk, diskWindows := answers(t, states[1])
		for k, want := range ram {
			if got := disk[k]; got != want {
				t.Errorf("%s: %s = %s on disk, %s in RAM", phase, k, got, want)
			}
		}
		for k, sub := range ramWindows {
			// A window answers a sound superset: on disk, every member of the value.
			value := strings.Split(k, " window")[0]
			if got, all := diskWindows[k], disk[value+" all"]; got != all {
				t.Errorf("%s: %s = %s on disk, want the value's members %s", phase, k, got, all)
			}
			for _, name := range strings.Fields(strings.Trim(sub, "[]")) {
				if !strings.Contains(" "+strings.Trim(diskWindows[k], "[]")+" ", " "+name+" ") {
					t.Errorf("%s: %s on disk %s misses %s (RAM %s)", phase, k, diskWindows[k], name, sub)
				}
			}
		}
		if ram["R k=x empty window"] != "[]" || disk["R k=x empty window"] != "[]" {
			t.Errorf("%s: an empty window answered %s in RAM, %s on disk", phase, ram["R k=x empty window"], disk["R k=x empty window"])
		}
		st, err := states[1].g.store.(storepkg.PropertyTxMembershipStatsCapability).PropertyTxMembershipStats()
		if err != nil {
			t.Fatal(err)
		}
		if st.RelSidecars != 0 || st.NodeSidecars != 0 {
			t.Errorf("%s: on disk %d rel and %d node sidecars built, want none", phase, st.RelSidecars, st.NodeSidecars)
		}
	}
	reopen := func(t *testing.T, disk bool) {
		t.Helper()
		for i, s := range states {
			if err := s.g.Close(); err != nil {
				t.Fatal(err)
			}
			s.g = open(t, i, i == 1 && disk)
		}
	}
	for _, s := range states {
		steps[0](t, s)
	}
	// The sidecar's answers this test compares against, pinned: r2 is deleted
	// and r1 moved x -> y -> x, so both stay members of every value they held.
	ram, _ := answers(t, states[0])
	for k, want := range map[string]string{
		"R k=x pin0": "[r1 r3]", "R k=y pin1": "[r1 r2]", "R k=y all": "[r1 r2]", "R k=z pin1": "[]",
		"R k=x pages": "[r1 r3]", "R n in [1,2] pin1": "[r1 r2 r3]", "D k=z pin1": "[a]", "D k=x all": "[a c]",
	} {
		if ram[k] != want {
			t.Fatalf("in RAM %s = %s, want %s", k, ram[k], want)
		}
	}
	compare(t, "written")
	reopen(t, true)
	compare(t, "reopened")
	reopen(t, false)
	for _, s := range states {
		steps[1](t, s)
	}
	reopen(t, true) // rebuilt: the writes in RAM mode are in the entries
	compare(t, "rebuilt")
	for _, s := range states {
		steps[2](t, s)
	}
	compare(t, "written after the rebuild")
	for _, s := range states {
		if err := s.g.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
