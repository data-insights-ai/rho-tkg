package conformance

import (
	"testing"
)

// Native-profile cases: only semantics v5 keeps. Writes use the graph clock;
// every historical cut is a pin read from CommittedTx right after the write
// that defines it; cursors are followed to their end (next == 0) and never
// interpreted; history is compared by content and order, never by stamp.
func init() {
	register("NativeHistory",
		native("PersistedHistoryAnswersEveryPinAfterReopen", natPersist),
		native("DeleteEndsLifeButKeepsEarlierCuts", natDelete),
		native("RelEarlierCutsSurviveUpdateAndDelete", natRelCuts),
		native("BitemporalPropertyLookupAfterCorrection", natPropertyBitemporal),
	)
	register("NativeVersionInterval",
		native("NodeCorrectionKeepsEarlierCuts", natNodeCorrection),
		native("NodeRefusalsWriteNothing", natNodeRefusals),
		native("RelCorrectionKeepsEarlierCuts", natRelCorrection),
		native("CloseNodeAndRelVersion", natClose),
		native("ExciseNodeAndRelHole", natExcise),
		native("RetractIsNotAValidityEnd", natRetract),
	)
	register("NativeEffectivePages",
		native("RelWalkEqualsTheFilteredSet", natRelWalk),
		native("NodeWalkEqualsTheFilteredSet", natNodeWalk),
		native("PinsAndValidTimeAfterMutation", natPagePins),
		native("CursorSurvivesLaterWrites", natPageStable),
		native("RefusalsReturnNoRows", natPageRefusals),
	)
}

func natPersist(t *testing.T, a Adapter) {
	g, _ := openDir(t, a)
	other := addNode(t, g, NodeSpec{Labels: []string{"Other"}})
	p0 := pinNow(t, g)
	n := addNode(t, g, NodeSpec{Labels: []string{"P", "Q"}, Props: map[string]any{"v": 1}})
	p1 := pinNow(t, g)
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 0)
	ok(t, err)
	p2 := pinNow(t, g)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 3}, 0)
	ok(t, err)
	p3 := pinNow(t, g)
	ok(t, g.Reopen())

	wantProps(t, "current", mustGet(t, g, n.ID).Props, map[string]any{"v": 3})
	if len(mustGet(t, g, n.ID).Labels) != 2 {
		t.Fatalf("labels lost across reopen")
	}
	wantNodeAsOf(t, g, n.ID, p1, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, g, n.ID, p2, map[string]any{"v": 2}, 1)
	wantNodeAsOf(t, g, n.ID, p3, map[string]any{"v": 3}, 2)
	wantNoNodeAsOf(t, g, n.ID, p0)
	_ = other
	h, err := g.NodeHistory(n.ID)
	ok(t, err)
	if len(h) != 2 {
		t.Fatalf("history has %d superseded versions, want 2", len(h))
	}
	wantProps(t, "history[0]", h[0].Props, map[string]any{"v": 1})
	wantProps(t, "history[1]", h[1].Props, map[string]any{"v": 2})
	// A write after the reopen lands after every earlier cut.
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 4}, 0)
	ok(t, err)
	wantNodeAsOf(t, g, n.ID, p3, map[string]any{"v": 3}, 2)
	wantNodeAsOf(t, g, n.ID, pinNow(t, g), map[string]any{"v": 4}, 3)
}

func natDelete(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"P"}, Props: map[string]any{"v": 1}})
	p1 := pinNow(t, g)
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 0)
	ok(t, err)
	p2 := pinNow(t, g)
	ok(t, g.DeleteNode(ctx, n.ID, 0))
	p3 := pinNow(t, g)

	_, err = g.GetNode(ctx, n.ID)
	is(t, "GetNode after delete", err, ErrNotFound)
	wantNodeAsOf(t, g, n.ID, p1, map[string]any{"v": 1}, 0)
	wantNodeAsOf(t, g, n.ID, p2, map[string]any{"v": 2}, 1)
	wantNoNodeAsOf(t, g, n.ID, p3)
	for _, c := range []struct {
		pin  int64
		want []int64
	}{{p2, []int64{n.ID}}, {p3, nil}} {
		ns, err := g.NodesByLabel("P", Query{TxPin: c.pin})
		ok(t, err)
		wantIDs(t, "ByLabel at the pin", ids(ns), c.want)
	}
	is(t, "second delete", g.DeleteNode(ctx, n.ID, 0), ErrNotFound)
	_, err = g.UpdateNode(ctx, n.ID, map[string]any{"v": 3}, 0)
	is(t, "update deleted", err, ErrNotFound)
	wantNodeAsOf(t, g, n.ID, p2, map[string]any{"v": 2}, 1)
}

func natRelCuts(t *testing.T, a Adapter) {
	g := open(t, a, false)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"w": 1}, Valid: Interval{From: 100}})
	p1 := pinNow(t, g)
	_, err := g.UpdateRel(ctx, r.ID, map[string]any{"w": 2}, 0)
	ok(t, err)
	p2 := pinNow(t, g)
	ok(t, g.DeleteRel(ctx, r.ID, 0))
	p3 := pinNow(t, g)
	for _, c := range []struct {
		pin int64
		w   int
	}{{p1, 1}, {p2, 2}} {
		got, err := g.RelAsOf(r.ID, c.pin)
		ok(t, err)
		wantProps(t, "RelAsOf", got.Props, map[string]any{"w": c.w})
		if got.From != x.ID || got.To != y.ID || got.Type != "R" {
			t.Fatalf("RelAsOf = %+v", got)
		}
	}
	_, err = g.RelAsOf(r.ID, p3)
	isAny(t, "RelAsOf after delete", err, ErrNoVersionAsOf, ErrNotFound)
	_, err = g.GetRel(ctx, r.ID)
	is(t, "GetRel after delete", err, ErrNotFound)
	h, err := g.RelHistory(r.ID)
	ok(t, err)
	if len(h) < 1 {
		t.Fatalf("rel history is empty")
	}
	wantProps(t, "oldest rel history row", h[0].Props, map[string]any{"w": 1})
}

func natPropertyBitemporal(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"B"}, Props: map[string]any{"s": "a"}, Valid: Interval{From: 100, To: 200}})
	before := pinNow(t, g)
	_, err := g.SetNodeVersionInterval(ctx, n.ID, 150, 300, map[string]any{"s": "b"}, 0)
	ok(t, err)
	after := pinNow(t, g)
	get := func(v string, q Query) []int64 {
		t.Helper()
		ns, err := g.NodesByLabelAndProperty("B", "s", v, q)
		ok(t, err)
		return ids(ns)
	}
	for _, c := range []struct {
		name  string
		v     string
		valid int64
		tx    int64
		want  bool
	}{
		{"a@120 before", "a", 120, before, true}, {"a@170 before", "a", 170, before, true}, {"b@170 before", "b", 170, before, false},
		{"a@120 after", "a", 120, after, true}, {"a@170 after", "a", 170, after, false},
		{"b@170 after", "b", 170, after, true}, {"b@299 after", "b", 299, after, true}, {"b@300 after", "b", 300, after, false},
	} {
		var want []int64
		if c.want {
			want = []int64{n.ID}
		}
		wantIDs(t, c.name, get(c.v, Query{ValidAt: c.valid, TxAt: c.tx}), want)
	}
}

func natNodeCorrection(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "old"}, Valid: Interval{From: 100, To: 200}})
	before := pinNow(t, g)
	_, err := g.SetNodeVersionInterval(ctx, n.ID, 150, 300, map[string]any{"k": "new"}, 0)
	ok(t, err)
	after := pinNow(t, g)
	for _, c := range []struct {
		valid, tx int64
		want      any
	}{
		{99, before, nil}, {120, before, "old"}, {170, before, "old"}, {199, before, "old"}, {200, before, nil}, {250, before, nil},
		{99, after, nil}, {120, after, "old"}, {149, after, "old"}, {150, after, "new"}, {250, after, "new"}, {299, after, "new"}, {300, after, nil},
	} {
		wantAtTx(t, g, n.ID, c.valid, c.tx, "k", c.want)
	}
	for _, c := range []struct {
		valid int64
		want  []int64
	}{{120, []int64{n.ID}}, {250, []int64{n.ID}}, {300, nil}, {50, nil}} {
		ns, err := g.NodesByLabel("Q", Query{ValidAt: c.valid, TxAt: after})
		ok(t, err)
		wantIDs(t, "ByLabel valid-time", ids(ns), c.want)
	}
	// Open-ended correction, then a second one: every earlier cut stays.
	_, err = g.SetNodeVersionInterval(ctx, n.ID, 400, 0, map[string]any{"k": "open"}, 0)
	ok(t, err)
	third := pinNow(t, g)
	wantAtTx(t, g, n.ID, 5000, third, "k", "open")
	wantAtTx(t, g, n.ID, 5000, after, "k", nil)
	wantAtTx(t, g, n.ID, 250, third, "k", "new")
	wantAtTx(t, g, n.ID, 250, before, "k", nil)
}

func natNodeRefusals(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "old"}, Valid: Interval{From: 100, To: 200}})
	check := historyUnchanged(t, g, n.ID)
	pin := pinNow(t, g)
	for _, c := range []struct {
		name     string
		from, to int64
	}{{"from equals to", 300, 300}, {"from after to", 300, 200}, {"from zero", 0, 200}} {
		_, err := g.SetNodeVersionInterval(ctx, n.ID, c.from, c.to, map[string]any{"k": "bad"}, 0)
		if err == nil {
			t.Fatalf("%s: accepted", c.name)
		}
		is(t, c.name, err, ErrInvalidTimeRange)
	}
	_, err := g.SetNodeVersionInterval(ctx, 987654321, 10, 20, nil, 0)
	is(t, "unknown node", err, ErrNotFound)
	check()
	wantAtTx(t, g, n.ID, 150, pin, "k", "old")
	ok(t, g.DeleteNode(ctx, n.ID, 0))
	_, err = g.SetNodeVersionInterval(ctx, n.ID, 10, 20, nil, 0)
	isAny(t, "deleted node", err, ErrEntityDeleted, ErrNotFound)
}

func natRelCorrection(t *testing.T, a Adapter) {
	g := open(t, a, false)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"w": 1}, Valid: Interval{From: 10, To: 50}})
	before := pinNow(t, g)
	_, err := g.SetRelVersionInterval(ctx, r.ID, 20, 80, map[string]any{"w": 2}, 0)
	ok(t, err)
	after := pinNow(t, g)
	for _, c := range []struct {
		valid, tx int64
		want      int
	}{{15, before, 1}, {15, after, 1}, {19, after, 1}, {20, after, 2}, {79, after, 2}, {70, before, 0}, {80, after, 0}, {9, after, 0}} {
		got, err := g.RelAtTx(r.ID, c.valid, c.tx)
		if c.want == 0 {
			if err == nil {
				t.Fatalf("RelAtTx(valid %d) = %v, want no version", c.valid, got.Props)
			}
			isAny(t, "RelAtTx refusal", err, ErrNoVersionValidAt, ErrNoVersionAsOf)
			continue
		}
		ok(t, err)
		wantProps(t, "RelAtTx", got.Props, map[string]any{"w": c.want})
	}
	for _, c := range []struct {
		name     string
		from, to int64
	}{{"from equals to", 90, 90}, {"from after to", 90, 30}, {"from zero", 0, 30}} {
		_, err := g.SetRelVersionInterval(ctx, r.ID, c.from, c.to, nil, 0)
		is(t, c.name, err, ErrInvalidTimeRange)
	}
	_, err = g.SetRelVersionInterval(ctx, 987654321, 20, 30, nil, 0)
	is(t, "unknown relationship", err, ErrNotFound)
	got, err := g.RelAtTx(r.ID, 25, pinNow(t, g))
	ok(t, err)
	wantProps(t, "rel after refusals", got.Props, map[string]any{"w": 2})
}

func natClose(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"Q"}, Props: map[string]any{"k": "v"}, Valid: Interval{From: 400}})
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Valid: Interval{From: 100}, Props: map[string]any{"w": 1}})
	before := pinNow(t, g)
	check := historyUnchanged(t, g, n.ID)
	is(t, "node close at the start", g.CloseNodeVersion(ctx, n.ID, 400), ErrInvalidTimeRange)
	is(t, "node close before the start", g.CloseNodeVersion(ctx, n.ID, 10), ErrInvalidTimeRange)
	is(t, "rel close at the start", g.CloseRelVersion(ctx, r.ID, 100), ErrInvalidTimeRange)
	is(t, "unknown node", g.CloseNodeVersion(ctx, 987654321, 500), ErrNotFound)
	is(t, "unknown rel", g.CloseRelVersion(ctx, 987654321, 500), ErrNotFound)
	check()

	ok(t, g.CloseNodeVersion(ctx, n.ID, 450))
	ok(t, g.CloseRelVersion(ctx, r.ID, 200))
	after := pinNow(t, g)
	if after <= before {
		t.Fatalf("a close must advance the commit clock")
	}
	cur, err := g.NodeAt(n.ID, 449)
	ok(t, err)
	if cur.ValidFrom != 400 || cur.ValidTo != 450 {
		t.Fatalf("closed node = [%d,%d), want [400,450)", cur.ValidFrom, cur.ValidTo)
	}
	_, err = g.NodeAt(n.ID, 450)
	is(t, "NodeAt at the close", err, ErrNoVersionValidAt)
	wantAtTx(t, g, n.ID, 460, before, "k", "v")
	wantAtTx(t, g, n.ID, 460, after, "k", nil)
	if got, err := g.RelAtTx(r.ID, 199, after); err != nil || got.ValidTo != 200 {
		t.Fatalf("closed rel = %+v, %v", got, err)
	}
	if got, err := g.RelAtTx(r.ID, 500, before); err != nil || got.ValidTo != 0 {
		t.Fatalf("pin before the close must see the open rel: %+v %v", got, err)
	}
	is(t, "second node close", g.CloseNodeVersion(ctx, n.ID, 430), ErrVersionClosed)
	is(t, "second rel close", g.CloseRelVersion(ctx, r.ID, 300), ErrVersionClosed)
}

func natExcise(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"X"}, Props: map[string]any{"k": "a"}, Valid: Interval{From: 100, To: 300}})
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}})
	r := addRel(t, g, RelSpec{Type: "R", From: x.ID, To: y.ID, Props: map[string]any{"est": "e1"}, Valid: Interval{From: 100, To: 300}})
	before := pinNow(t, g)
	ok(t, g.ExciseNodeValidity(ctx, n.ID, 150, 200, 0))
	ok(t, g.ExciseRelValidity(ctx, r.ID, 150, 200, 0))
	after := pinNow(t, g)
	for _, c := range []struct {
		valid, tx int64
		want      any
	}{{100, after, "a"}, {149, after, "a"}, {150, after, nil}, {199, after, nil}, {200, after, "a"}, {299, after, "a"}, {300, after, nil}, {170, before, "a"}} {
		wantAtTx(t, g, n.ID, c.valid, c.tx, "k", c.want)
	}
	if v := voidRows(t, g, n.ID); len(v) != 1 || v[0].ValidFrom != 150 || v[0].ValidTo != 200 {
		t.Fatalf("void rows = %+v, want one covering [150,200)", v)
	}
	for _, pin := range []int64{before, after} {
		got, err := g.NodeAsOf(n.ID, pin)
		ok(t, err)
		if got.Void {
			t.Fatalf("NodeAsOf answered a void row")
		}
	}
	segs, _, err := g.RelsEffectivePage(ctx, Pin{Tx: after}, Filter{Prop: "est", Eq: "e1"}, 0, 10)
	ok(t, err)
	if len(segs) != 2 || segs[0].ValidTo != 150 || segs[1].ValidFrom != 200 {
		t.Fatalf("segments after the excise = %+v, want [100,150) and [200,300)", segs)
	}
	segs, _, err = g.RelsEffectivePage(ctx, Pin{Tx: before}, Filter{Prop: "est", Eq: "e1"}, 0, 10)
	ok(t, err)
	if len(segs) != 1 || segs[0].ValidFrom != 100 || segs[0].ValidTo != 300 {
		t.Fatalf("segments before the excise = %+v, want [100,300)", segs)
	}
	for _, c := range []struct {
		name string
		a, b int64
		want error
	}{{"inverted", 200, 150, ErrInvalidTimeRange}, {"empty", 200, 200, ErrInvalidTimeRange}, {"zero start", 0, 10, ErrInvalidTimeRange}, {"open end", 150, 0, ErrInvalidTimeRange}, {"existing hole", 160, 190, ErrNoVersionValidAt}, {"outside", 500, 600, ErrNoVersionValidAt}} {
		is(t, c.name, g.ExciseNodeValidity(ctx, n.ID, c.a, c.b, 0), c.want)
	}
	is(t, "unknown node", g.ExciseNodeValidity(ctx, 987654321, 150, 160, 0), ErrNotFound)
	_, err = g.SetNodeVersionInterval(ctx, n.ID, 160, 190, map[string]any{"k": "fill"}, 0)
	ok(t, err)
	wantAtTx(t, g, n.ID, 175, pinNow(t, g), "k", "fill")
	wantAtTx(t, g, n.ID, 175, after, "k", nil)
}

func natRetract(t *testing.T, a Adapter) {
	g := open(t, a, false)
	mk := func(label string) Node {
		return addNode(t, g, NodeSpec{Labels: []string{label}, Props: map[string]any{"v": 1}, Valid: Interval{From: 100}})
	}
	deleted, retracted := mk("D"), mk("R")
	for _, n := range []Node{deleted, retracted} {
		_, err := g.UpdateNode(ctx, n.ID, map[string]any{"v": 2}, 0)
		ok(t, err)
	}
	p2 := pinNow(t, g)
	ok(t, g.DeleteNode(ctx, deleted.ID, 0))
	ok(t, g.RetractNode(ctx, retracted.ID, 0))
	p3 := pinNow(t, g)

	for _, n := range []Node{deleted, retracted} {
		wantNodeAsOf(t, g, n.ID, p2, map[string]any{"v": 2}, 1)
		wantNoNodeAsOf(t, g, n.ID, p3)
		_, err := g.GetNode(ctx, n.ID)
		is(t, "GetNode", err, ErrNotFound)
	}
	// Valid instant 150 was inside the validity of both beliefs (the first version). A delete ends
	// validity at its instant, so the later pin still answers; a retraction
	// says the belief never held, so it does not.
	wantAtTx(t, g, deleted.ID, 150, p3, "v", 1)
	wantAtTx(t, g, retracted.ID, 150, p3, "v", nil)
	for _, c := range []struct {
		pin  int64
		want []int64
	}{{p2, []int64{retracted.ID}}, {p3, nil}} {
		ns, err := g.NodesByLabel("R", Query{TxPin: c.pin})
		ok(t, err)
		wantIDs(t, "ByLabel(R) at the pin", ids(ns), c.want)
	}
	h, err := g.NodeHistory(retracted.ID)
	ok(t, err)
	if len(h) < 1 {
		t.Fatalf("retraction erased the history")
	}
	is(t, "second retraction", g.RetractNode(ctx, retracted.ID, 0), ErrNotFound)
	is(t, "retract unknown", g.RetractNode(ctx, 987654321, 0), ErrNotFound)
	is(t, "retract unknown rel", g.RetractRel(ctx, 987654321, 0), ErrNotFound)
	d2 := addNode(t, g, NodeSpec{Labels: []string{"D2"}})
	ok(t, g.DeleteNode(ctx, d2.ID, 0))
	isAny(t, "retract a deleted node", g.RetractNode(ctx, d2.ID, 0), ErrEntityDeleted, ErrNotFound)
}

// --- effective pages, cursor followed to its end ---

func walkRelsOpaque(t *testing.T, g Graph, pin Pin, f Filter, limit int) [][]RelSegment {
	t.Helper()
	var pages [][]RelSegment
	var after int64
	for guard := 0; guard < 100; guard++ {
		segs, next, err := g.RelsEffectivePage(ctx, pin, f, after, limit)
		ok(t, err)
		if n := len(relSegIDs(segs)); n > limit {
			t.Fatalf("page of %d relationships exceeds limit %d", n, limit)
		}
		pages = append(pages, segs)
		if next == 0 {
			return pages
		}
		after = next
	}
	t.Fatalf("cursor never ends")
	return nil
}

func walkNodesOpaque(t *testing.T, g Graph, pin Pin, f Filter, limit int) [][]NodeSegment {
	t.Helper()
	var pages [][]NodeSegment
	var after int64
	for guard := 0; guard < 100; guard++ {
		segs, next, err := g.NodesEffectivePage(ctx, pin, f, after, limit)
		ok(t, err)
		if n := len(nodeSegIDs(segs)); n > limit {
			t.Fatalf("page of %d nodes exceeds limit %d", n, limit)
		}
		pages = append(pages, segs)
		if next == 0 {
			return pages
		}
		after = next
	}
	t.Fatalf("cursor never ends")
	return nil
}

func sameSet(t *testing.T, what string, got, want []int64) {
	t.Helper()
	seen := map[int64]int{}
	for _, id := range got {
		seen[id]++
	}
	for _, id := range want {
		seen[id]--
	}
	for id, n := range seen {
		if n != 0 {
			t.Fatalf("%s: got %v, want the same set as %v (id %d off by %d)", what, got, want, id, n)
		}
	}
}

func natRelWalk(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 9, []string{"e1", "e2", "e3"}, false)
	pin := Pin{Tx: pinNow(t, f.g)}
	for _, limit := range []int{1, 2, 4, 100} {
		pages := walkRelsOpaque(t, f.g, pin, Filter{Prop: "est", Eq: "e1"}, limit)
		sameSet(t, "walk e1", flatten(pages), f.ofEstate("e1"))
		for _, p := range pages {
			for _, s := range p {
				if s.ValidFrom != 100 || s.ValidTo != 200 {
					t.Fatalf("segment [%d,%d), want [100,200)", s.ValidFrom, s.ValidTo)
				}
			}
		}
	}
	segs, next, err := f.g.RelsEffectivePage(ctx, pin, Filter{Prop: "est", Eq: "nobody"}, 0, 10)
	ok(t, err)
	if len(segs) != 0 || next != 0 {
		t.Fatalf("absent value: %d rows next=%d", len(segs), next)
	}
	// A corrected relationship contributes two segments to the same page.
	_, err = f.g.SetRelVersionInterval(ctx, f.rels[0].ID, 150, 300, map[string]any{"est": "e1", "idx": 100}, 0)
	ok(t, err)
	segs, _, err = f.g.RelsEffectivePage(ctx, Pin{Tx: pinNow(t, f.g)}, Filter{Prop: "est", Eq: "e1"}, 0, 1)
	ok(t, err)
	if len(segs) != 2 || segs[0].Rel.ID != f.rels[0].ID || segs[1].Rel.ID != f.rels[0].ID {
		t.Fatalf("a page of limit 1 must hold all segments of its relationship, got %d", len(segs))
	}
	// Every segment equals the per-relationship bitemporal read.
	for _, s := range segs {
		got, err := f.g.RelAtTx(s.Rel.ID, s.ValidFrom, pinNow(t, f.g))
		ok(t, err)
		wantProps(t, "segment vs read", s.Rel.Props, got.Props)
	}
}

func natNodeWalk(t *testing.T, a Adapter) {
	g := open(t, a, false)
	ns := nodeFixture(t, g, 9, []string{"e1", "e2", "e3"}, Interval{From: 100, To: 200})
	_, err := g.SetNodeVersionInterval(ctx, ns[0].ID, 150, 300, map[string]any{"est": "e1", "idx": 100}, 0)
	ok(t, err)
	pin := Pin{Tx: pinNow(t, g)}
	want := nodeIDsOf(ns, 0, 3, 6)
	for _, limit := range []int{1, 2, 3, 100} {
		pages := walkNodesOpaque(t, g, pin, Filter{Prop: "est", Eq: "e1"}, limit)
		sameSet(t, "walk e1", flattenNodes(pages), want)
	}
	segs, _, err := g.NodesEffectivePage(ctx, pin, Filter{Prop: "est", Eq: "e1"}, 0, 1)
	ok(t, err)
	if len(segs) != 2 || segs[0].ValidFrom != 100 || segs[0].ValidTo != 150 || segs[1].ValidFrom != 150 || segs[1].ValidTo != 300 {
		t.Fatalf("corrected node segments = %+v, want [100,150) and [150,300)", segs)
	}
}

func natPagePins(t *testing.T, a Adapter) {
	g := open(t, a, false)
	ns := nodeFixture(t, g, 5, []string{"e1", "e2"}, Interval{From: 100}) // e1: 0,2,4
	p1 := pinNow(t, g)
	_, err := g.UpdateNode(ctx, ns[0].ID, map[string]any{"est": "e2"}, 0)
	ok(t, err)
	p2 := pinNow(t, g)
	moveFrom := mustGet(t, g, ns[0].ID).ValidFrom // 0 when the row carries no explicit start
	_ = moveFrom
	ok(t, g.DeleteNode(ctx, ns[2].ID, 0))
	p3 := pinNow(t, g)
	e1 := Filter{Prop: "est", Eq: "e1"}
	at := func(p Pin, f Filter) []int64 { return flattenNodes(walkNodesOpaque(t, g, p, f, 10)) }

	sameSet(t, "e1 @p1", at(Pin{Tx: p1}, e1), nodeIDsOf(ns, 0, 2, 4))
	// The move is a valid-time change: far in the future node 0 is e2, node 2 (deleted at p3) is gone.
	sameSet(t, "e1 @p2 valid far", at(Pin{Tx: p2, ValidAt: 1 << 50}, e1), nodeIDsOf(ns, 2, 4))
	sameSet(t, "e1 @p3 valid far", at(Pin{Tx: p3, ValidAt: 1 << 50}, e1), nodeIDsOf(ns, 4))
	sameSet(t, "e2 @p2 valid far", at(Pin{Tx: p2, ValidAt: 1 << 50}, Filter{Prop: "est", Eq: "e2"}), nodeIDsOf(ns, 0, 1, 3))
	// Valid 150 predates the move and the delete: every earlier belief still answers it at every pin.
	sameSet(t, "e1 @p3 valid 150", at(Pin{Tx: p3, ValidAt: 150}, e1), nodeIDsOf(ns, 0, 2, 4))
	// Valid-time selectors.
	sameSet(t, "e1 @p1 interval [90,100)", at(Pin{Tx: p1, ValidStart: 90, ValidEnd: 100}, e1), nil)
	sameSet(t, "e1 @p1 interval [90,101)", at(Pin{Tx: p1, ValidStart: 90, ValidEnd: 101}, e1), nodeIDsOf(ns, 0, 2, 4))
	sameSet(t, "e1 @p1 open from 99999", at(Pin{Tx: p1, ValidStart: 99999}, e1), nodeIDsOf(ns, 0, 2, 4))
	sameSet(t, "e1 @p1 valid 99", at(Pin{Tx: p1, ValidAt: 99}, e1), nil)
	_, _, err = g.NodesEffectivePage(ctx, Pin{Tx: p1, ValidAt: 150, ValidStart: 100, ValidEnd: 200}, e1, 0, 10)
	is(t, "ValidAt with an interval", err, ErrInvalidTimeRange)
	// The rel door agrees on the same shape.
	f := newPageFixture(t, a, 4, []string{"e1", "e2"}, true)
	rp1 := pinNow(t, f.g)
	_, err = f.g.UpdateRel(ctx, f.rels[0].ID, map[string]any{"est": "e2"}, 0)
	ok(t, err)
	ok(t, f.g.DeleteRel(ctx, f.rels[2].ID, 0)) // e1: 0 and 2 were e1
	rp2 := pinNow(t, f.g)
	rels := func(p Pin, est string) []int64 {
		return flatten(walkRelsOpaque(t, f.g, p, Filter{Prop: "est", Eq: est}, 10))
	}
	sameSet(t, "rel e1 @rp1", rels(Pin{Tx: rp1}, "e1"), []int64{f.rels[0].ID, f.rels[2].ID})
	sameSet(t, "rel e1 @rp2 valid far", rels(Pin{Tx: rp2, ValidAt: 1 << 50}, "e1"), nil)
	sameSet(t, "rel e1 @rp2 valid 150", rels(Pin{Tx: rp2, ValidAt: 150}, "e1"), []int64{f.rels[0].ID, f.rels[2].ID})
}

func natPageStable(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 6, []string{"e1"}, true)
	pin := Pin{Tx: pinNow(t, f.g)}
	flt := Filter{Prop: "est", Eq: "e1"}
	first, next, err := f.g.RelsEffectivePage(ctx, pin, flt, 0, 3)
	ok(t, err)
	if len(relSegIDs(first)) != 3 || next == 0 {
		t.Fatalf("first page = %d relationships, next=%d", len(relSegIDs(first)), next)
	}
	x := addNode(t, f.g, NodeSpec{Labels: []string{"A"}})
	y := addNode(t, f.g, NodeSpec{Labels: []string{"A"}})
	addRel(t, f.g, RelSpec{Type: "R", From: x.ID, To: y.ID, Valid: Interval{From: 100}, Props: map[string]any{"est": "e1"}})
	_, err = f.g.UpdateRel(ctx, f.rels[4].ID, map[string]any{"est": "e2"}, 0)
	ok(t, err)
	ok(t, f.g.DeleteRel(ctx, f.rels[3].ID, 0))
	// Continue the walk at the old pin from the cursor: exactly the rest of the pinned state.
	var rest []int64
	after := next
	for guard := 0; guard < 20; guard++ {
		segs, n2, err := f.g.RelsEffectivePage(ctx, pin, flt, after, 2)
		ok(t, err)
		rest = append(rest, relSegIDs(segs)...)
		if n2 == 0 {
			break
		}
		after = n2
	}
	sameSet(t, "pages at the old pin", append(relSegIDs(first), rest...), f.ofEstate("e1"))
}

func natPageRefusals(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 3, []string{"e1"}, false)
	nodeFixture(t, f.g, 3, []string{"e1"}, Interval{From: 100, To: 200})
	pin, flt := Pin{Tx: pinNow(t, f.g)}, Filter{Prop: "est", Eq: "e1"}
	for _, c := range []struct {
		name  string
		pin   Pin
		f     Filter
		limit int
	}{
		{"limit zero", pin, flt, 0}, {"limit negative", pin, flt, -1}, {"limit above 10000", pin, flt, 10001},
		{"nil value", pin, Filter{Prop: "est"}, 10}, {"list value", pin, Filter{Prop: "est", Eq: []string{"e1"}}, 10},
		{"empty property", pin, Filter{Eq: "e1"}, 10},
	} {
		rs, rn, rerr := f.g.RelsEffectivePage(ctx, c.pin, c.f, 0, c.limit)
		ns, nn, nerr := f.g.NodesEffectivePage(ctx, c.pin, c.f, 0, c.limit)
		for _, err := range []error{rerr, nerr} {
			if err == nil {
				t.Fatalf("%s: accepted", c.name)
			}
			is(t, c.name, err, ErrInvalidArgument)
		}
		if len(rs)+len(ns) != 0 || rn != 0 || nn != 0 {
			t.Fatalf("%s: a refused page returned rows or a cursor", c.name)
		}
	}
	_, _, err := f.g.RelsEffectivePage(ctx, pin, flt, 0, 10000)
	ok(t, err)
	// A cursor that is not a valid ID is invalid, not "past the end".
	_, _, err = f.g.RelsEffectivePage(ctx, pin, flt, -1, 10)
	is(t, "negative rel cursor", err, ErrInvalidArgument)
	_, _, err = f.g.NodesEffectivePage(ctx, pin, flt, -1, 10)
	is(t, "negative node cursor", err, ErrInvalidArgument)
	// An empty graph answers an empty page.
	g2 := open(t, a, false)
	segs, next, err := g2.RelsEffectivePage(ctx, Pin{Tx: pinNow(t, g2)}, flt, 0, 5)
	if err == nil && (len(segs) != 0 || next != 0) {
		t.Fatalf("empty graph returned rows")
	}
}
