package core

// Ledger row 106 (ai-soc R-DIAG1021b, a Flux blocker): on tiered every reopened
// /tkg/asof page (RelsEffectivePage) decoded whole relationship types, because
// tiered served neither the paged property membership
// (store.RelPropertyTxMemberPageCapability and its node twin) nor the member
// IDs (store.MemberIDsCapability), so relPageSources fell back to
// RelationshipsByType per type per page. Tiered now serves both, over the
// reference shard, the archive and every event shard; the page gather stops
// on a cancelled context.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// pageWorld is one graph of the row-106 scenario, entities by name.
type pageWorld struct {
	c     *Core
	ts    *tiered.Store // nil on badger
	nodes map[string]*types.Node
	names map[int64]string
	ids   map[string]int64
	pin1  types.Instant // after phase 1
	pin2  types.Instant // after phase 2
}

func newPageWorld(c *Core, ts *tiered.Store) *pageWorld {
	return &pageWorld{c: c, ts: ts, nodes: map[string]*types.Node{}, names: map[int64]string{}, ids: map[string]int64{}}
}

func (w *pageWorld) name(id int64, name string) {
	w.names[id] = name
	w.ids[name] = id
}

func (w *pageWorld) node(t *testing.T, name, label string, props map[string]any) types.Instant {
	t.Helper()
	n, err := w.c.Nodes.Add(context.Background(), []string{label}, props)
	if err != nil {
		t.Fatalf("add node %s: %v", name, err)
	}
	w.nodes[name] = n
	w.name(int64(n.ID()), name)
	return n.Temporal().TxFrom
}

func (w *pageWorld) rel(t *testing.T, name, typ, from, to, estate string) types.Instant {
	t.Helper()
	r, err := w.c.Rels.Add(context.Background(), typ, w.nodes[from], w.nodes[to], map[string]any{epProp: estate})
	if err != nil {
		t.Fatalf("add rel %s: %v", name, err)
	}
	w.name(int64(r.ID()), name)
	return r.Temporal().TxFrom
}

// build runs the scenario: phase 1 on the reference shard (Case) and the
// first event shard (Signal); after pin1 r4 moves e1 -> e2 and r2 is deleted;
// on tiered the first shard rotates, phase 2 lands on a new hot shard, and the
// first shard is demoted cold and closed.
func (w *pageWorld) build(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, typ := range []string{"HOP", "SEEN"} {
		if err := w.c.Index.CreateRelProperty(typ, epProp); err != nil {
			t.Fatalf("CreateRelProperty(%s): %v", typ, err)
		}
	}
	for _, label := range []string{"Signal", "Case"} {
		if err := w.c.Index.CreateProperty(label, epProp); err != nil {
			t.Fatalf("CreateProperty(%s): %v", label, err)
		}
	}
	var first string
	if w.ts != nil {
		first = hotShardName(w.ts)
	}
	var pin types.Instant
	for _, n := range []struct{ name, label, estate string }{
		{"c1", "Case", "e1"}, {"c2", "Case", "e1"},
		{"s1", "Signal", "e1"}, {"s2", "Signal", "e2"}, {"s3", "Signal", "e1"}, {"s4", "Signal", "e1"},
	} {
		pin = max(pin, w.node(t, n.name, n.label, map[string]any{epProp: n.estate}))
	}
	for _, r := range []struct{ name, typ, from, to, estate string }{
		{"r1", "HOP", "s1", "s2", "e1"},
		{"r2", "SEEN", "s2", "s3", "e1"},
		{"r3", "HOP", "s1", "s3", "e2"},
		{"r4", "HOP", "s3", "s4", "e1"},
		{"rRef", "HOP", "c1", "c2", "e1"},
		{"rRefSeen", "SEEN", "c2", "c1", "e1"},
	} {
		pin = max(pin, w.rel(t, r.name, r.typ, r.from, r.to, r.estate))
	}
	w.pin1 = pin
	if _, err := w.c.Rels.Update(ctx, types.RelID(w.ids["r4"]), map[string]any{epProp: "e2"}); err != nil {
		t.Fatal(err)
	}
	if err := w.c.Rels.Delete(ctx, types.RelID(w.ids["r2"])); err != nil {
		t.Fatal(err)
	}
	if _, err := w.c.Nodes.Update(ctx, w.nodes["s4"].ID(), map[string]any{epProp: "e2"}); err != nil {
		t.Fatal(err)
	}
	if w.ts != nil {
		time.Sleep(2 * time.Millisecond)
		if err := w.ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate: %v", err)
		}
	}
	pin = max(pin, w.node(t, "s5", "Signal", map[string]any{epProp: "e1"}))
	pin = max(pin, w.rel(t, "r5", "HOP", "s4", "s1", "e1"))
	pin = max(pin, w.rel(t, "r6", "SEEN", "s5", "s2", "e1"))
	w.pin2 = pin
	if w.ts != nil {
		demoteToCold(w.ts, first)
		closeEventShardStore(t, w.ts.EventShardsForTest()[first])
	}
}

// relPages walks every page of RelsEffectivePage at pin for estate = value
// and returns the entity names, page by page ("|" between pages).
func (w *pageWorld) relPages(t *testing.T, pin temporal.Pin, value string, limit int) string {
	t.Helper()
	var pages []string
	var after types.RelID
	for range 100 {
		segs, next, err := w.c.Temporal.RelsEffectivePage(context.Background(), pin, temporal.Filter{Prop: epProp, Eq: value}, after, limit)
		if err != nil {
			t.Fatalf("RelsEffectivePage(%+v, %s, after %d): %v", pin, value, after, err)
		}
		var page []string
		for _, s := range segs {
			n := w.names[int64(s.Rel.ID())]
			if len(page) == 0 || page[len(page)-1] != n {
				page = append(page, n)
			}
		}
		pages = append(pages, strings.Join(page, ","))
		if next == 0 {
			return strings.Join(pages, "|")
		}
		after = next
	}
	t.Fatal("RelsEffectivePage did not end")
	return ""
}

// nodePages is relPages for NodesEffectivePage.
func (w *pageWorld) nodePages(t *testing.T, pin temporal.Pin, value string, limit int) string {
	t.Helper()
	var pages []string
	var after types.NodeID
	for range 100 {
		segs, next, err := w.c.Temporal.NodesEffectivePage(context.Background(), pin, temporal.Filter{Prop: epProp, Eq: value}, after, limit)
		if err != nil {
			t.Fatalf("NodesEffectivePage(%+v, %s, after %d): %v", pin, value, after, err)
		}
		var page []string
		for _, s := range segs {
			n := w.names[int64(s.Node.ID())]
			if len(page) == 0 || page[len(page)-1] != n {
				page = append(page, n)
			}
		}
		pages = append(pages, strings.Join(page, ","))
		if next == 0 {
			return strings.Join(pages, "|")
		}
		after = next
	}
	t.Fatal("NodesEffectivePage did not end")
	return ""
}

// TestTieredRelsEffectivePage_NoWholeTypeScan: the ai-soc reopen page path on
// a disk tiered store whose rows span the reference shard, a cold closed event
// shard and a hot shard answers like badger (same entities, same ID order,
// same page cuts) with 0 whole-type scans for pages 1-3, a ValidAt point read,
// a second estate value and the pages after a reopen. Break: a relationship
// type without the property index takes the documented fallback (the type's
// membership IDs, no row decoded: never RelationshipsByType).
func TestTieredRelsEffectivePage_NoWholeTypeScan(t *testing.T) {
	bc, _ := newBadgerCoreForIndexes(t)
	dir := t.TempDir()
	tc, ts := openTieredCoreAt(t, dir, 0, false)
	tcOpen := true
	t.Cleanup(func() {
		if tcOpen {
			_ = tc.Close()
		}
	})
	bw, tw := newPageWorld(bc, nil), newPageWorld(tc, ts)
	bw.build(t)
	tw.build(t)

	type pageCase struct {
		name  string
		pin   func(w *pageWorld) temporal.Pin
		value string
		limit int
		want  string
	}
	at1 := func(w *pageWorld) temporal.Pin { return temporal.Pin{Tx: w.pin1} }
	at2 := func(w *pageWorld) temporal.Pin { return temporal.Pin{Tx: w.pin2} }
	point := func(w *pageWorld) temporal.Pin {
		return temporal.Pin{Tx: w.pin2, ValidAt: w.pin2, ValidAtSet: true}
	}
	cases := []pageCase{
		{"pages 1-3 at pin1", at1, "e1", 2, "r1,r2|r4,rRef|rRefSeen"},
		{"pages at pin2 (r2 deleted, r4 moved: their e1 segments stay)", at2, "e1", 2, "r1,r2|r4,rRef|rRefSeen,r5|r6"},
		{"point read", point, "e1", 10, "r1,rRef,rRefSeen,r5,r6"},
		{"second estate value", at2, "e2", 1, "r3|r4|"},
	}
	var scans []string
	ts.SetTypeScanHookForTest(func(door string) { scans = append(scans, door) })
	check := func(t *testing.T, cases []pageCase) {
		t.Helper()
		for _, pc := range cases {
			want := bw.relPages(t, pc.pin(bw), pc.value, pc.limit)
			if want != pc.want {
				t.Errorf("%s: badger pages %q, scenario expects %q", pc.name, want, pc.want)
			}
			scans = nil
			got := tw.relPages(t, pc.pin(tw), pc.value, pc.limit)
			if got != want {
				t.Errorf("%s: tiered pages %q, badger %q", pc.name, got, want)
			}
			if len(scans) != 0 {
				t.Errorf("%s: %d whole-type scans on tiered %v, want 0", pc.name, len(scans), scans)
			}
		}
	}
	check(t, cases)

	// Reopen (the ai-soc path: every page after a restart).
	if err := tc.Close(); err != nil {
		t.Fatal(err)
	}
	tcOpen = false
	tc2, ts2 := openTieredCoreAt(t, dir, 0, false)
	t.Cleanup(func() { _ = tc2.Close() })
	tw.c, tw.ts = tc2, ts2
	ts2.SetTypeScanHookForTest(func(door string) { scans = append(scans, door) })
	check(t, cases[1:])

	// The node twin: the label pages read the paged sidecars too.
	for _, value := range []string{"e1", "e2"} {
		want := bw.nodePages(t, temporal.Pin{Tx: bw.pin2}, value, 2)
		scans = nil
		if got := tw.nodePages(t, temporal.Pin{Tx: tw.pin2}, value, 2); got != want {
			t.Errorf("node pages %s: tiered %q, badger %q", value, got, want)
		}
		if len(scans) != 0 {
			t.Errorf("node pages %s: %d whole-label scans on tiered %v, want 0", value, len(scans), scans)
		}
	}
	if want := "c1,c2|s1,s3|s4,s5|"; bw.nodePages(t, temporal.Pin{Tx: bw.pin2}, "e1", 2) != want {
		t.Errorf("badger node pages e1 = %q, scenario expects %q", bw.nodePages(t, temporal.Pin{Tx: bw.pin2}, "e1", 2), want)
	}

	// Break: a type without the property index takes the documented fallback.
	for _, w := range []*pageWorld{bw, tw} {
		w.pin2 = max(w.pin2, w.rel(t, "bare", "BARE", "s1", "s5", "e1"))
	}
	scans = nil
	bare := pageCase{"unindexed type", at2, "e1", 10, ""}
	bare.want = bw.relPages(t, bare.pin(bw), "e1", 10)
	if !strings.HasSuffix(bare.want, ",bare") {
		t.Fatalf("badger unindexed type: %q, want bare listed last", bare.want)
	}
	scans = nil
	if got := tw.relPages(t, bare.pin(tw), "e1", 10); got != bare.want {
		t.Errorf("unindexed type: tiered %q, badger %q", got, bare.want)
	}
	if slices.Contains(scans, "RelationshipsByType") {
		t.Errorf("unindexed type: the fallback decoded the whole type (scans %v), want member IDs only", scans)
	}
	if !slices.Contains(scans, "RelIDsByType") || !slices.Contains(scans, "ForEachRelTypeTxMember") {
		t.Errorf("unindexed type: scans %v, want the documented membership fallback seen by the hook", scans)
	}
}

// memberPage walks a paged membership door from after with limit until it
// ends, returning names and checking ascending, unique IDs.
func memberPage(t *testing.T, w *pageWorld, page func(after int64, limit int, fn func(int64, types.Instant) bool) error, after int64, limit int) []string {
	t.Helper()
	var out []string
	last := after
	for range 100 {
		got := 0
		err := page(last, limit, func(id int64, _ types.Instant) bool {
			if id <= last {
				t.Errorf("member %d after %d: not ascending", id, last)
			}
			last = id
			got++
			out = append(out, w.names[id])
			return true
		})
		if err != nil {
			t.Fatalf("page after %d limit %d: %v", last, limit, err)
		}
		if got > limit {
			t.Fatalf("page returned %d members, limit %d", got, limit)
		}
		if got < limit || limit <= 0 {
			return out
		}
	}
	t.Fatal("membership pages did not end")
	return nil
}

// TestTieredMemberPage_EqualsBadger: tiered's paged property membership (rel
// and node twin) lists the same members as badger, in ascending ID order,
// resumed above the cursor, at most limit per page, each entity once though
// its rows sit on several shards (an archived reference node and its
// relationship keep their history on the reference shard). Break: no index
// declared answers store.ErrIndexNotFound before fn; member IDs by type and
// label equal badger's.
func TestTieredMemberPage_EqualsBadger(t *testing.T) {
	bc, _ := newBadgerCoreForIndexes(t)
	tc, ts := openTieredCoreAt(t, t.TempDir(), 0, false)
	t.Cleanup(func() { _ = tc.Close() })
	bw, tw := newPageWorld(bc, nil), newPageWorld(tc, ts)
	bw.build(t)
	tw.build(t)
	ctx := context.Background()
	// Rows on several shards: an update writes history on the reference
	// shard, then the archive takes the current rows of c1 and its rels.
	for _, w := range []*pageWorld{bw, tw} {
		if _, err := w.c.Rels.Update(ctx, types.RelID(w.ids["rRef"]), map[string]any{"note": 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.c.Nodes.Update(ctx, w.nodes["c1"].ID(), map[string]any{"note": 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ts.ArchiveNode(tw.nodes["c1"].ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	type door struct {
		name string
		page func(w *pageWorld) (func(after int64, limit int, fn func(int64, types.Instant) bool) error, error)
	}
	relDoor := func(typ, key, vk string) door {
		return door{fmt.Sprintf("rel %s.%s=%s", typ, key, vk), func(w *pageWorld) (func(int64, int, func(int64, types.Instant) bool) error, error) {
			p, ok := w.c.store.(storepkg.RelPropertyTxMemberPageCapability)
			if !ok {
				return nil, fmt.Errorf("%T does not serve store.RelPropertyTxMemberPageCapability", w.c.store)
			}
			tok, _ := w.c.relTypes.Lookup(typ)
			return func(after int64, limit int, fn func(int64, types.Instant) bool) error {
				return p.ForEachRelPropertyTxMemberAfter(tok, key, vk, types.RelID(after), limit, func(id types.RelID, tx types.Instant) bool { return fn(int64(id), tx) })
			}, nil
		}}
	}
	nodeDoor := func(label, key, vk string) door {
		return door{fmt.Sprintf("node %s.%s=%s", label, key, vk), func(w *pageWorld) (func(int64, int, func(int64, types.Instant) bool) error, error) {
			p, ok := w.c.store.(storepkg.NodePropertyTxMemberPageCapability)
			if !ok {
				return nil, fmt.Errorf("%T does not serve store.NodePropertyTxMemberPageCapability", w.c.store)
			}
			tok, _ := w.c.labels.Lookup(label)
			return func(after int64, limit int, fn func(int64, types.Instant) bool) error {
				return p.ForEachNodePropertyTxMemberAfter(tok, key, vk, types.NodeID(after), limit, func(id types.NodeID, tx types.Instant) bool { return fn(int64(id), tx) })
			}, nil
		}}
	}
	vk := func(s string) string { return types.IndexablePropertyValueKey(s) }
	doors := []door{
		relDoor("HOP", epProp, vk("e1")), relDoor("HOP", epProp, vk("e2")), relDoor("SEEN", epProp, vk("e1")),
		nodeDoor("Signal", epProp, vk("e1")), nodeDoor("Signal", epProp, vk("e2")), nodeDoor("Case", epProp, vk("e1")),
	}
	wantAll := map[string][]string{
		doors[0].name: {"r1", "r4", "rRef", "r5"},
		doors[2].name: {"r2", "rRefSeen", "r6"},
		doors[3].name: {"s1", "s3", "s4", "s5"},
		doors[5].name: {"c1", "c2"},
	}
	for _, d := range doors {
		bp, err := d.page(bw)
		if err != nil {
			t.Fatal(err)
		}
		tp, err := d.page(tw)
		if err != nil {
			t.Fatalf("%s: %v", d.name, err)
		}
		all := memberPage(t, bw, bp, 0, 1000)
		if want, ok := wantAll[d.name]; ok && !slices.Equal(all, want) {
			t.Errorf("%s: badger members %v, scenario expects %v", d.name, all, want)
		}
		for _, limit := range []int{1, 2, 3, 1000} {
			for _, from := range append([]string{""}, all...) {
				after := bw.ids[from]
				tAfter := tw.ids[from]
				want := memberPage(t, bw, bp, after, limit)
				got := memberPage(t, tw, tp, tAfter, limit)
				if !slices.Equal(got, want) {
					t.Errorf("%s after %q limit %d: tiered %v, badger %v", d.name, from, limit, got, want)
				}
			}
		}
		// limit 0 and a negative limit answer what badger answers.
		for _, limit := range []int{0, -1} {
			var bn, tn int
			_ = bp(0, limit, func(int64, types.Instant) bool { bn++; return true })
			_ = tp(0, limit, func(int64, types.Instant) bool { tn++; return true })
			if bn != tn {
				t.Errorf("%s limit %d: tiered %d members, badger %d", d.name, limit, tn, bn)
			}
		}
		// fn returning false stops the page.
		calls := 0
		if err := tp(0, 1000, func(int64, types.Instant) bool { calls++; return false }); err != nil || calls > 1 {
			t.Errorf("%s: fn false: %d calls, err %v; want <= 1, nil", d.name, calls, err)
		}
	}

	// No index declared: ErrIndexNotFound before fn, on both.
	for _, d := range []door{relDoor("HOP", "nokey", vk("e1")), nodeDoor("Signal", "nokey", vk("e1"))} {
		for _, w := range []*pageWorld{bw, tw} {
			p, err := d.page(w)
			if err != nil {
				t.Fatal(err)
			}
			called := false
			if err := p(0, 10, func(int64, types.Instant) bool { called = true; return true }); !errors.Is(err, storepkg.ErrIndexNotFound) || called {
				t.Errorf("%s on %T: err %v, fn called %v; want ErrIndexNotFound, not called", d.name, w.c.store, err, called)
			}
		}
	}

	// Member IDs (store.MemberIDsCapability): current members, ascending,
	// each once, equal to badger's.
	for _, w := range []*pageWorld{bw, tw} {
		if _, ok := w.c.store.(storepkg.MemberIDsCapability); !ok {
			t.Fatalf("%T does not serve store.MemberIDsCapability", w.c.store)
		}
	}
	memberNames := func(w *pageWorld, rel bool, name string) []string {
		m := w.c.store.(storepkg.MemberIDsCapability)
		var ids []int64
		if rel {
			tok, _ := w.c.relTypes.Lookup(name)
			rs, err := m.RelIDsByType(tok)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range rs {
				ids = append(ids, int64(id))
			}
		} else {
			tok, _ := w.c.labels.Lookup(name)
			ns, err := m.NodeIDsByLabel(tok)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range ns {
				ids = append(ids, int64(id))
			}
		}
		if !slices.IsSorted(ids) || len(slices.Compact(slices.Clone(ids))) != len(ids) {
			t.Errorf("%T %s: ids %v not ascending and unique", w.c.store, name, ids)
		}
		out := []string{}
		for _, id := range ids {
			out = append(out, w.names[id])
		}
		return out
	}
	for _, q := range []struct {
		rel  bool
		name string
		want []string
	}{
		{true, "HOP", []string{"r1", "r3", "r4", "rRef", "r5"}},
		{true, "SEEN", []string{"rRefSeen", "r6"}},
		{false, "Signal", []string{"s1", "s2", "s3", "s4", "s5"}},
		{false, "Case", []string{"c1", "c2"}},
	} {
		b, tt := memberNames(bw, q.rel, q.name), memberNames(tw, q.rel, q.name)
		if !slices.Equal(b, q.want) || !slices.Equal(tt, q.want) {
			t.Errorf("member IDs %s: tiered %v, badger %v, want %v", q.name, tt, b, q.want)
		}
	}
	m := tw.c.store.(storepkg.MemberIDsCapability)
	if _, err := m.RelIDsByType(0); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Errorf("RelIDsByType(0) = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := m.NodeIDsByLabel(0); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Errorf("NodeIDsByLabel(0) = %v, want ErrInvalidStoreMutation", err)
	}
}

// TestRelsEffectivePage_CtxCancelStopsGather: a page whose context is
// cancelled while it gathers its sources stops reading: with 20 relationship
// types (labels) without the property index, cancelling at the first type's
// read leaves at most one more read, and the page returns the context error.
func TestRelsEffectivePage_CtxCancelStopsGather(t *testing.T) {
	for _, rel := range []bool{true, false} {
		t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
			c, ts := openTieredCoreAt(t, t.TempDir(), 0, false)
			t.Cleanup(func() { _ = c.Close() })
			bg := context.Background()
			var pinTx types.Instant
			for i := range 20 {
				a, err := c.Nodes.Add(bg, []string{fmt.Sprintf("Sig%d", i)}, map[string]any{epProp: "e1"})
				if err != nil {
					t.Fatal(err)
				}
				b, err := c.Nodes.Add(bg, []string{fmt.Sprintf("Sig%d", i)}, nil)
				if err != nil {
					t.Fatal(err)
				}
				r, err := c.Rels.Add(bg, fmt.Sprintf("T%d", i), a, b, map[string]any{epProp: "e1"})
				if err != nil {
					t.Fatal(err)
				}
				pinTx = max(pinTx, r.Temporal().TxFrom)
			}
			ctx, cancel := context.WithCancel(bg)
			defer cancel()
			reads := 0
			ts.SetTypeScanHookForTest(func(string) {
				reads++
				cancel()
			})
			pin := temporal.Pin{Tx: pinTx}
			f := temporal.Filter{Prop: epProp, Eq: "e1"}
			var err error
			if rel {
				_, _, err = c.Temporal.RelsEffectivePage(ctx, pin, f, 0, 10)
			} else {
				_, _, err = c.Temporal.NodesEffectivePage(ctx, pin, f, 0, 10)
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("page err %v, want context.Canceled", err)
			}
			if reads > 2 {
				t.Errorf("%d whole-type reads after the cancel at the first, want <= 2", reads)
			}
		})
	}
}
