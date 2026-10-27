package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredTxMembershipAndValidEndMatchBadger (backlog 58): the same writes,
// updates, deletes, valid-time closes and history on badger and on a disk tiered
// store (reference shard, event shards, archive, a closed cold shard) must give
// the same tx-membership, property-membership (since v4.58.0 the tiered
// property sidecar is the union of every shard's, so Case members off the
// reference shard — s5 on an event shard, c3 on the archive — are listed as on
// badger) and valid-end capability answers
// (candidates: a superset of what streams) and the same core door answers.
func TestTieredTxMembershipAndValidEndMatchBadger(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	bg, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bg.Close() })
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Case"},
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1, MaxOpenColdShards: 4})
	if err != nil {
		t.Fatal(err)
	}
	tg, err := New(Config{Store: ts})
	if err != nil {
		_ = ts.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tg.Close() })
	graphs := []*Core{bg, tg}
	clk := &switchableClock{}
	for _, g := range graphs {
		g.SetClockForTest(t, clk.Now)
	}
	x0 := etX0()
	x := x0
	tick := func() types.Instant { x += 1000; clk.at.Store(int64(x)); return x }
	tk := func(v types.Instant) string {
		if v == 0 {
			return "-"
		}
		return fmt.Sprint((int64(v) - int64(x0)) / 1000)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// ids[i][name] is the entity's ID on graph i, names[i] the inverse.
	ids := []map[string]int64{{}, {}}
	names := []map[int64]string{{}, {}}
	both := func(f func(i int, g *Core) (string, int64)) {
		tick()
		for i, g := range graphs {
			if name, id := f(i, g); name != "" {
				ids[i][name], names[i][id] = id, name
			}
		}
	}
	addNode := func(name string, labels []string, props map[string]any) {
		both(func(i int, g *Core) (string, int64) {
			p := map[string]any{"name": name, "tkg_valid_from": int64(x)}
			for k, v := range props {
				p[k] = v
			}
			n, err := g.Nodes.Add(ctx, labels, p)
			must(err)
			return name, int64(n.ID())
		})
	}
	addRel := func(name, typ, from, to string) {
		both(func(i int, g *Core) (string, int64) {
			r, err := g.Rels.AddByID(ctx, typ, types.NodeID(ids[i][from]), types.NodeID(ids[i][to]),
				map[string]any{"name": name, "w": 1, "tkg_valid_from": int64(x)})
			must(err)
			return name, int64(r.ID())
		})
	}
	setNode := func(name string, props map[string]any) {
		both(func(i int, g *Core) (string, int64) {
			_, err := g.Nodes.Update(ctx, types.NodeID(ids[i][name]), props)
			must(err)
			return "", 0
		})
	}
	setRel := func(name string, props map[string]any) {
		both(func(i int, g *Core) (string, int64) {
			_, err := g.Rels.Update(ctx, types.RelID(ids[i][name]), props)
			must(err)
			return "", 0
		})
	}
	rotate := func() {
		ts.MuForTest().Lock()
		ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
		ts.MuForTest().Unlock()
		must(ts.CheckRotationForTest())
		time.Sleep(2 * time.Millisecond)
	}
	nameOf := func(i int, id int64) string {
		if n, ok := names[i][id]; ok {
			return n
		}
		return fmt.Sprintf("?%d", id)
	}
	prop := func(get func(string) (any, bool), keys ...string) string {
		var parts []string
		for _, k := range keys {
			if v, ok := get(k); ok {
				parts = append(parts, fmt.Sprintf("%s=%v", k, v))
			}
		}
		return strings.Join(parts, ",")
	}
	sorted := func(ss []string) string { sort.Strings(ss); return strings.Join(ss, "\n  ") }

	// answers renders, for graph i at pin, everything the capabilities drive.
	vfs := func() []types.Instant { return []types.Instant{0, x0 + 4000, x0 + 7000, x} }
	answers := func(i int, pin types.Instant) map[string]string {
		g := graphs[i]
		out := map[string]string{}
		for _, typ := range []string{"LINK", "OWNS"} {
			for _, vf := range vfs() {
				var segs []string
				must(g.Temporal.ForEachRelEffectiveByTypeFrom(typ, pin, vf, func(s temporal.RelSegment) bool {
					segs = append(segs, fmt.Sprintf("%s v%d [%s,%s) %s", nameOf(i, int64(s.Rel.ID())), s.Rel.Version(),
						tk(s.ValidFrom), tk(s.ValidTo), prop(s.Rel.GetProperty, "w")))
					return true
				}))
				out[fmt.Sprintf("RelFrom %s vf=%s", typ, tk(vf))] = sorted(segs)
			}
			rels, err := g.Rels.ByType(typ, storepkg.QueryOpts{TxPin: pin})
			must(err)
			var rs []string
			for _, r := range rels {
				rs = append(rs, nameOf(i, int64(r.ID()))+" "+prop(r.GetProperty, "w"))
			}
			out["ByType "+typ] = sorted(rs)
			count, ok, err := g.Stats.RelHistoryCountByType(typ)
			must(err)
			out["RelHistoryCountByType "+typ] = fmt.Sprint(count, ok)
		}
		for _, labels := range [][]string{{"Sig"}, {"Case"}, {"Sig", "Case"}} {
			for _, vf := range vfs() {
				var segs []string
				must(g.Temporal.ForEachNodeEffectiveByLabelsFrom(labels, pin, vf, func(s temporal.NodeSegment) bool {
					segs = append(segs, fmt.Sprintf("%s v%d [%s,%s) %s", nameOf(i, int64(s.Node.ID())), s.Node.Version(),
						tk(s.ValidFrom), tk(s.ValidTo), prop(s.Node.GetProperty, "status")))
					return true
				}))
				out[fmt.Sprintf("NodeFrom %v vf=%s", labels, tk(vf))] = sorted(segs)
			}
		}
		for _, label := range []string{"Sig", "Case"} {
			nodes, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{TxPin: pin})
			must(err)
			var ns []string
			for _, n := range nodes {
				ns = append(ns, nameOf(i, int64(n.ID()))+" "+prop(n.GetProperty, "status"))
			}
			out["ByLabel "+label] = sorted(ns)
		}
		for _, v := range []string{"open", "closed"} {
			nodes, err := g.Nodes.ByLabelAndProperty("Case", "status", v, storepkg.QueryOpts{TxPin: pin})
			must(err)
			var ns []string
			for _, n := range nodes {
				ns = append(ns, nameOf(i, int64(n.ID())))
			}
			out["ByLabelAndProperty Case status="+v] = sorted(ns)
		}
		return out
	}

	// capabilities renders graph i's capability answers (name -> first tx tick).
	capabilities := func(i int, phase string) map[string]string {
		g := graphs[i]
		out := map[string]string{}
		label := func(name string) uint16 {
			tok, ok := g.labels.Lookup(name)
			if !ok {
				t.Fatalf("label %s not registered", name)
			}
			return tok
		}
		relType := func(name string) uint16 {
			tok, ok := g.lookupRelTypeQueryToken(name)
			if !ok {
				t.Fatalf("type %s not registered", name)
			}
			return tok
		}
		members := func(key string, each func(fn func(id int64, tx types.Instant) bool) error) {
			var ms []string
			seen := map[int64]bool{}
			if err := each(func(id int64, tx types.Instant) bool {
				if seen[id] {
					t.Errorf("[%s] %s: %s listed twice", phase, key, nameOf(i, id))
				}
				seen[id] = true
				ms = append(ms, nameOf(i, id)+"@"+tk(tx))
				return true
			}); err != nil {
				ms = []string{"error: " + err.Error()}
			}
			out[key] = sorted(ms)
		}
		var nodeIDs []types.NodeID
		var relIDs []types.RelID
		for _, id := range ids[i] {
			nodeIDs, relIDs = append(nodeIDs, types.NodeID(id)), append(relIDs, types.RelID(id))
		}
		// Tx membership (TXMEMBERS): the ever-members are the point form's
		// answer over every ID (names only: a first tx time is a lower bound,
		// exact when a shard's sidecar is built after the rows, 0 when a
		// departure recorded it). The stream lists former members, and a
		// store may stream more (tiered: an entity a move took off a shard),
		// so it must stay inside the ever-members.
		names := func(key string) []string {
			var ns []string
			for _, m := range strings.Split(out[key], "\n  ") {
				if at := strings.IndexByte(m, '@'); at >= 0 {
					ns = append(ns, m[:at])
				}
			}
			return ns
		}
		within := func(stream, ever string) {
			for _, n := range names(stream) {
				if !slices.Contains(names(ever), n) {
					t.Errorf("[%s] graph %d %s lists %s, not in %s %v", phase, i, stream, n, ever, out[ever])
				}
			}
			delete(out, stream)
		}
		lc, okL := g.store.(storepkg.LabelTxMembershipCapability)
		lp, okLP := g.store.(storepkg.LabelTxMemberLookupCapability)
		if !okL || !okLP {
			t.Errorf("[%s] graph %d store does not implement the label tx-membership capabilities", phase, i)
		} else {
			for _, l := range []string{"Sig", "Case"} {
				members("LabelTxMembers "+l, func(fn func(int64, types.Instant) bool) error {
					return lc.ForEachLabelTxMember(label(l), func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
				})
				members("LabelTxMembersOf "+l, func(fn func(int64, types.Instant) bool) error {
					return lp.LabelTxMembersOf(label(l), append(nodeIDs, 0), func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
				})
				within("LabelTxMembers "+l, "LabelTxMembersOf "+l)
				out["LabelTxMembersOf "+l] = sorted(names("LabelTxMembersOf " + l))
			}
		}
		rc, okR := g.store.(storepkg.RelTypeTxMembershipCapability)
		rp, okRP := g.store.(storepkg.RelTypeTxMemberLookupCapability)
		if !okR || !okRP {
			t.Errorf("[%s] graph %d store does not implement the rel-type tx-membership capabilities", phase, i)
		} else {
			for _, r := range []string{"LINK", "OWNS"} {
				members("RelTypeTxMembers "+r, func(fn func(int64, types.Instant) bool) error {
					return rc.ForEachRelTypeTxMember(relType(r), func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
				})
				members("RelTypeTxMembersOf "+r, func(fn func(int64, types.Instant) bool) error {
					return rp.RelTypeTxMembersOf(relType(r), relIDs, func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
				})
				within("RelTypeTxMembers "+r, "RelTypeTxMembersOf "+r)
				out["RelTypeTxMembersOf "+r] = sorted(names("RelTypeTxMembersOf " + r))
			}
		}
		if c, ok := g.store.(storepkg.NodePropertyTxMembershipCapability); !ok {
			t.Errorf("[%s] graph %d store does not implement NodePropertyTxMembershipCapability", phase, i)
		} else {
			for _, v := range []string{"open", "closed"} {
				key := "NodePropertyTxMembers Case status=" + v
				members(key, func(fn func(int64, types.Instant) bool) error {
					return c.ForEachNodePropertyTxMember(label("Case"), "status", types.IndexablePropertyValueKey(v),
						func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
				})
			}
		}
		if c, ok := g.store.(storepkg.RelPropertyTxMembershipCapability); !ok {
			t.Errorf("[%s] graph %d store does not implement RelPropertyTxMembershipCapability", phase, i)
		} else if err := c.ForEachRelPropertyTxMember(relType("LINK"), "w", types.IndexablePropertyValueKey(1),
			func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storepkg.ErrIndexNotFound) {
			t.Errorf("[%s] graph %d ForEachRelPropertyTxMember without a declared index: %v, want ErrIndexNotFound", phase, i, err)
		}
		return out
	}

	// validEnds checks graph i's valid-end candidates at (pin, vf) are a superset of what streams.
	validEnds := func(i int, phase string, pin, vf types.Instant) []string {
		g := graphs[i]
		c, ok := g.store.(storepkg.RelValidEndCapability)
		if !ok {
			t.Errorf("[%s] graph %d store does not implement RelValidEndCapability", phase, i)
			return nil
		}
		if _, ok := g.store.(storepkg.NodeValidEndCapability); !ok {
			t.Errorf("[%s] graph %d store does not implement NodeValidEndCapability", phase, i)
		}
		tok, _ := g.lookupRelTypeQueryToken("LINK")
		cands, err := c.RelValidEndCandidates(tok, pin, vf)
		must(err)
		var got []string
		for _, cd := range cands {
			got = append(got, nameOf(i, cd.ID))
		}
		must(g.Temporal.ForEachRelEffectiveByTypeFrom("LINK", pin, vf, func(s temporal.RelSegment) bool {
			if n := nameOf(i, int64(s.Rel.ID())); !slices.Contains(got, n) {
				t.Errorf("[%s] graph %d streams %s at vf=%s but its valid-end candidates %v miss it", phase, i, n, tk(vf), got)
			}
			return true
		}))
		sort.Strings(got)
		return got
	}

	var r2Gone types.Instant // badger's pin after r2's delete
	compare := func(phase string, pin [2]types.Instant) {
		t.Helper()
		for _, side := range []struct {
			what string
			get  func(i int) map[string]string
		}{
			{"core", func(i int) map[string]string { return answers(i, pin[i]) }},
			{"store", func(i int) map[string]string { return capabilities(i, phase) }},
		} {
			want, got := side.get(0), side.get(1)
			keys := make([]string, 0, len(want))
			for k := range want {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if got[k] != want[k] {
					t.Errorf("[%s] %s %s:\n tiered:\n  %s\n badger:\n  %s", phase, side.what, k, got[k], want[k])
				}
			}
		}
		for i := range graphs {
			if c := validEnds(i, phase, pin[i], x); r2Gone != 0 && pin[0] >= r2Gone && slices.Contains(c, "r2") {
				t.Errorf("[%s] graph %d valid-end candidates at vf=now %v keep r2, deleted before it", phase, i, c)
			}
		}
	}
	nowTx := func() [2]types.Instant {
		var p [2]types.Instant
		for i, g := range graphs {
			var err error
			p[i], err = g.Temporal.NowTx()
			must(err)
		}
		return p
	}

	tick()
	for _, g := range graphs {
		must(g.Index.CreateProperty("Case", "status"))
	}
	addNode("c1", []string{"Case"}, map[string]any{"status": "open"})
	addNode("c2", []string{"Case"}, map[string]any{"status": "open"})
	addNode("c3", []string{"Case"}, map[string]any{"status": "closed"})
	addNode("s1", []string{"Sig"}, nil)
	addNode("s2", []string{"Sig"}, nil)
	addRel("r1", "LINK", "s1", "s2")
	addRel("o1", "OWNS", "c1", "s1")
	pinA := nowTx()
	compare("A: one event shard", pinA) // also builds every index the writes below maintain
	setNode("c1", map[string]any{"status": "closed"})
	setRel("r1", map[string]any{"w": 2})
	rotate()
	addNode("s3", []string{"Sig"}, nil)
	addRel("r2", "LINK", "s2", "s3")
	addRel("r3", "LINK", "s3", "s1")
	setRel("r1", map[string]any{"w": 3}) // r1's rows stay on the first event shard
	both(func(i int, g *Core) (string, int64) {
		must(g.Rels.Delete(ctx, types.RelID(ids[i]["r2"])))
		return "", 0
	})
	both(func(i int, g *Core) (string, int64) {
		must(g.Nodes.Delete(ctx, types.NodeID(ids[i]["c2"])))
		return "", 0
	})
	both(func(i int, g *Core) (string, int64) {
		must(g.Rels.CloseVersion(ctx, types.RelID(ids[i]["r3"]), x))
		return "", 0
	})
	pinB := nowTx()
	r2Gone = pinB[0]
	compare("B: two event shards", pinB)
	rotate()
	addNode("s4", []string{"Sig"}, nil)
	addNode("s5", []string{"Sig", "Case"}, map[string]any{"status": "open"}) // a Case member on an event shard
	addRel("r4", "LINK", "s4", "s3")
	addRel("r5", "LINK", "s4", "s5")
	setNode("s5", map[string]any{"status": "closed"})
	both(func(i int, g *Core) (string, int64) {
		must(g.Rels.Delete(ctx, types.RelID(ids[i]["r4"])))
		return "", 0
	})
	tick()
	must(ts.ArchiveNode(types.NodeID(ids[1]["c3"])))
	pinC := nowTx()
	for _, pin := range [][2]types.Instant{pinA, pinB, pinC} {
		compare(fmt.Sprintf("C: three event shards + archive, pin %s", tk(pin[0])), pin)
	}

	// A depth-scoped read keeps its depth: only the hot shard's Sig nodes.
	hot, err := tg.Nodes.ByLabel("Sig", storepkg.QueryOpts{TxPin: pinC[1], Depth: storepkg.DepthHot})
	must(err)
	var hotNames []string
	for _, n := range hot {
		hotNames = append(hotNames, nameOf(1, int64(n.ID())))
	}
	if sort.Strings(hotNames); !slices.Equal(hotNames, []string{"s4", "s5"}) {
		t.Errorf("tiered ByLabel(Sig, DepthHot) = %v, want [s4 s5]", hotNames)
	}
	hotRels, err := tg.Rels.ByType("LINK", storepkg.QueryOpts{TxPin: pinC[1], Depth: storepkg.DepthHot})
	must(err)
	var hotRelNames []string
	for _, r := range hotRels {
		hotRelNames = append(hotRelNames, nameOf(1, int64(r.ID())))
	}
	if sort.Strings(hotRelNames); !slices.Equal(hotRelNames, []string{"r5"}) {
		t.Errorf("tiered ByType(LINK, DepthHot) = %v, want [r5]", hotRelNames)
	}

	// The first event shard goes cold and is closed on disk: the fan-out reopens it.
	var first *tiered.EventShard
	ts.MuForTest().Lock()
	for _, es := range ts.EventShardsForTest() {
		if es != ts.HotShardForTest() && (first == nil || es.Name() < first.Name()) {
			first = es
		}
	}
	first.SetTierForTest(tiered.TierCold)
	ts.CatalogForTest().UpdateShardTier(first.Name(), tiered.TierCold)
	ts.MuForTest().Unlock()
	first.SetLastAccessForTest(0)
	ts.SetIdleTimeoutForTest(time.Nanosecond)
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(time.Hour)
	if first.Store() != nil {
		t.Fatalf("setup: cold shard %s still open", first.Name())
	}
	for _, pin := range [][2]types.Instant{pinA, pinC} {
		compare(fmt.Sprintf("D: cold first shard, pin %s", tk(pin[0])), pin)
	}
}
