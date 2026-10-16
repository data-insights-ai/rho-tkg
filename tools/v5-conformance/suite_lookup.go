package conformance

import (
	"fmt"
	"reflect"
	"testing"
)

func init() {
	register("Lookup",
		testCase{"GetCurrentAndRefusals", lookupGet},
		testCase{"ByLabelKeysetPagesAreExact", lookupLabelPages},
		testCase{"ByLabelBeforeAndAfterMutationByPin", lookupLabelPins},
		testCase{"ByLabelValidTimeSelectors", lookupLabelValidTime},
		testCase{"ByLabelRefusals", lookupLabelRefusals},
		testCase{"ByLabelAndPropertyIsTypeStrictAndHistorical", lookupProperty},
		testCase{"ByLabelAndPropertyBitemporal", lookupPropertyBitemporal},
	)
}

func lookupGet(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"P", "Q"}, Props: map[string]any{"s": "text", "i": 7, "b": true}, TxFrom: 1000})
	got := mustGet(t, g, n.ID)
	wantProps(t, "Get", got.Props, map[string]any{"s": "text", "i": 7, "b": true})
	if got.ID != n.ID || got.Version != 0 || got.TxFrom != 1000 || got.TxTo != 0 {
		t.Fatalf("Get = %+v", got)
	}
	_, err := g.UpdateNode(ctx, n.ID, map[string]any{"i": 8}, 2000)
	ok(t, err)
	got = mustGet(t, g, n.ID)
	// An update merges into the properties of the current version.
	wantProps(t, "Get after update", got.Props, map[string]any{"s": "text", "i": 8, "b": true})
	if got.Version != 1 || got.TxFrom != 2000 {
		t.Fatalf("Get after update = %+v, want version 1 at 2000", got)
	}
	_, err = g.GetNode(ctx, n.ID+1)
	is(t, "Get of an ID that was never minted", err, ErrNotFound)
	_, err = g.GetRel(ctx, n.ID)
	is(t, "GetRel of a node ID", err, ErrNotFound)
}

func lookupLabelPages(t *testing.T, a Adapter) {
	g := open(t, a, true)
	var all []Node
	for i := 0; i < 7; i++ {
		labels := []string{"L"}
		if i%2 == 0 {
			labels = append(labels, "M")
		}
		all = append(all, addNode(t, g, NodeSpec{Labels: labels, Props: map[string]any{"i": i}, TxFrom: 1000 + int64(i)}))
	}
	want := ids(all)

	whole, err := g.NodesByLabel("L", Query{})
	ok(t, err)
	wantIDs(t, "ByLabel(L)", sortedIDs(whole), want)

	// Walk the keyset: pages of 3 then 3 then 1, each ascending, no overlap, no gap.
	var walked []uint64
	var after uint64
	pageSizes := []int{}
	for {
		page, err := g.NodesByLabel("L", Query{Limit: 3, After: after})
		ok(t, err)
		if len(page) == 0 {
			break
		}
		pageSizes = append(pageSizes, len(page))
		for i, n := range page {
			if i > 0 && page[i-1].ID >= n.ID {
				t.Fatalf("page not ascending: %v", ids(page))
			}
			if n.ID <= after {
				t.Fatalf("page repeats an ID at or below After=%d: %v", after, ids(page))
			}
		}
		walked = append(walked, ids(page)...)
		after = page[len(page)-1].ID
		if len(pageSizes) > 10 {
			t.Fatalf("keyset walk does not terminate: %v", walked)
		}
	}
	if !reflect.DeepEqual(pageSizes, []int{3, 3, 1}) {
		t.Fatalf("page sizes = %v, want [3 3 1]", pageSizes)
	}
	wantIDs(t, "keyset walk", walked, sortedIDs(whole))

	// A node with two labels is listed under both, a node with one only under its own.
	m, err := g.NodesByLabel("M", Query{})
	ok(t, err)
	var wantM []uint64
	for i, n := range all {
		if i%2 == 0 {
			wantM = append(wantM, n.ID)
		}
	}
	wantIDs(t, "ByLabel(M)", sortedIDs(m), wantM)

	// After past the last ID and an unknown label are empty, not errors.
	none, err := g.NodesByLabel("L", Query{After: all[6].ID})
	ok(t, err)
	if len(none) != 0 {
		t.Fatalf("After past the last ID returned %v", ids(none))
	}
	unknown, err := g.NodesByLabel("NoSuchLabel", Query{})
	ok(t, err)
	if len(unknown) != 0 {
		t.Fatalf("unknown label returned %v", ids(unknown))
	}
}

// lookupLabelPins: mutate (update, delete, add), then read the earlier pin and
// the present; the sets must differ exactly by the mutations.
func lookupLabelPins(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x := addNode(t, g, NodeSpec{Labels: []string{"L"}, Props: map[string]any{"v": "x0"}, TxFrom: 1000})
	y := addNode(t, g, NodeSpec{Labels: []string{"L"}, Props: map[string]any{"v": "y0"}, TxFrom: 1000})
	pin, err := g.CommittedTx()
	ok(t, err)

	_, err = g.UpdateNode(ctx, x.ID, map[string]any{"v": "x1"}, 0)
	ok(t, err)
	ok(t, g.DeleteNode(ctx, y.ID, 0))
	z := addNode(t, g, NodeSpec{Labels: []string{"L"}, Props: map[string]any{"v": "z0"}})

	now, err := g.NodesByLabel("L", Query{})
	ok(t, err)
	wantIDs(t, "current set", sortedIDs(now), sortedIDs([]Node{x, z}))
	at, err := g.NodesByLabel("L", Query{TxPin: pin})
	ok(t, err)
	wantIDs(t, "set at the pin", sortedIDs(at), sortedIDs([]Node{x, y}))
	for _, n := range at {
		want := map[uint64]string{x.ID: "x0", y.ID: "y0"}[n.ID]
		wantProps(t, fmt.Sprintf("node %d at the pin", n.ID), n.Props, map[string]any{"v": want})
	}
	for _, n := range now {
		want := map[uint64]string{x.ID: "x1", z.ID: "z0"}[n.ID]
		wantProps(t, fmt.Sprintf("node %d now", n.ID), n.Props, map[string]any{"v": want})
	}
	// Paging composes with the pin.
	page, err := g.NodesByLabel("L", Query{TxPin: pin, Limit: 1, After: x.ID})
	ok(t, err)
	wantIDs(t, "pinned page after x", ids(page), []uint64{y.ID})
}

func lookupLabelValidTime(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n1 := addNode(t, g, NodeSpec{Labels: []string{"V"}, Valid: Interval{From: 100, To: 200}, TxFrom: 1000})
	n2 := addNode(t, g, NodeSpec{Labels: []string{"V"}, Valid: Interval{From: 150, To: 300}, TxFrom: 1000})
	n3 := addNode(t, g, NodeSpec{Labels: []string{"V"}, Valid: Interval{From: 250, To: 400}, TxFrom: 1000})
	n4 := addNode(t, g, NodeSpec{Labels: []string{"V"}, Valid: Interval{From: 500}, TxFrom: 1000})
	for _, c := range []struct {
		name string
		q    Query
		want []Node
	}{
		{"at 99 (before all)", Query{ValidAt: 99}, nil},
		{"at 100 (inclusive start)", Query{ValidAt: 100}, []Node{n1}},
		{"at 175", Query{ValidAt: 175}, []Node{n1, n2}},
		{"at 200 (exclusive end)", Query{ValidAt: 200}, []Node{n2}},
		{"at 260", Query{ValidAt: 260}, []Node{n2, n3}},
		{"at 450 (gap)", Query{ValidAt: 450}, nil},
		{"at 9999 (open end)", Query{ValidAt: 9999}, []Node{n4}},
		{"interval [190,260)", Query{ValidStart: 190, ValidEnd: 260}, []Node{n1, n2, n3}},
		{"interval [200,250) touches n1 end and n3 start only", Query{ValidStart: 200, ValidEnd: 250}, []Node{n2}},
		{"interval [400,500) is the gap", Query{ValidStart: 400, ValidEnd: 500}, nil},
	} {
		got, err := g.NodesByLabel("V", c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		wantIDs(t, c.name, sortedIDs(got), sortedIDs(c.want))
	}
}

func lookupLabelRefusals(t *testing.T, a Adapter) {
	g := open(t, a, true)
	addNode(t, g, NodeSpec{Labels: []string{"L"}, TxFrom: 1000})
	_, err := g.NodesByLabel("L", Query{Limit: -1})
	is(t, "negative limit", err, ErrInvalidArgument)
	_, err = g.NodesByLabel("L", Query{TxPin: 5, ValidAt: 5})
	is(t, "TxPin with ValidAt", err, ErrConflictingOpts)
	_, err = g.NodesByLabel("L", Query{TxPin: 5, TxAt: 5})
	is(t, "TxPin with TxAt", err, ErrConflictingOpts)
}

func lookupProperty(t *testing.T, a Adapter) {
	g := open(t, a, true)
	one := addNode(t, g, NodeSpec{Labels: []string{"E"}, Props: map[string]any{"v": 1}, TxFrom: 1000})
	str := addNode(t, g, NodeSpec{Labels: []string{"E"}, Props: map[string]any{"v": "1"}, TxFrom: 1000})
	two := addNode(t, g, NodeSpec{Labels: []string{"E"}, Props: map[string]any{"v": 2}, TxFrom: 1000})
	noKey := addNode(t, g, NodeSpec{Labels: []string{"E"}, Props: map[string]any{"w": 1}, TxFrom: 1000})
	_ = noKey
	pin, err := g.CommittedTx()
	ok(t, err)

	get := func(label, key string, v any, q Query) []uint64 {
		t.Helper()
		ns, err := g.NodesByLabelAndProperty(label, key, v, q)
		if err != nil {
			t.Fatalf("ByLabelAndProperty(%s,%s,%v): %v", label, key, v, err)
		}
		return sortedIDs(ns)
	}
	wantIDs(t, "v=1 (integer)", get("E", "v", 1, Query{}), []uint64{one.ID})
	wantIDs(t, `v="1" (string)`, get("E", "v", "1", Query{}), []uint64{str.ID})
	wantIDs(t, "v=2", get("E", "v", 2, Query{}), []uint64{two.ID})
	wantIDs(t, "v=3 (absent value)", get("E", "v", 3, Query{}), nil)
	wantIDs(t, "key w holds no v", get("E", "w", 2, Query{}), nil)
	wantIDs(t, "unknown label", get("Nope", "v", 1, Query{}), nil)
	wantIDs(t, "unknown key", get("E", "nokey", 1, Query{}), nil)

	// Mutate: the integer node takes a new value, the string node is deleted.
	_, err = g.UpdateNode(ctx, one.ID, map[string]any{"v": 3}, 0)
	ok(t, err)
	ok(t, g.DeleteNode(ctx, str.ID, 0))

	wantIDs(t, "v=1 now", get("E", "v", 1, Query{}), nil)
	wantIDs(t, "v=3 now", get("E", "v", 3, Query{}), []uint64{one.ID})
	wantIDs(t, `v="1" now (deleted)`, get("E", "v", "1", Query{}), nil)
	// The same questions at the pin before the mutations give the old answers.
	wantIDs(t, "v=1 at the pin", get("E", "v", 1, Query{TxPin: pin}), []uint64{one.ID})
	wantIDs(t, "v=3 at the pin", get("E", "v", 3, Query{TxPin: pin}), nil)
	wantIDs(t, `v="1" at the pin`, get("E", "v", "1", Query{TxPin: pin}), []uint64{str.ID})
}

// lookupPropertyBitemporal: a value valid only in a window, asked at valid
// instants with and without a transaction pin, after a correction.
func lookupPropertyBitemporal(t *testing.T, a Adapter) {
	g := open(t, a, true)
	n := addNode(t, g, NodeSpec{Labels: []string{"B"}, Props: map[string]any{"s": "a"}, Valid: Interval{From: 100, To: 200}, TxFrom: 1000})
	_, err := g.SetNodeVersionInterval(ctx, n.ID, 150, 300, map[string]any{"s": "b"}, 2000)
	ok(t, err)

	get := func(v string, q Query) []uint64 {
		t.Helper()
		ns, err := g.NodesByLabelAndProperty("B", "s", v, q)
		ok(t, err)
		return ids(ns)
	}
	// Before the correction was recorded (TxAt 1500): "a" answers across [100,200).
	wantIDs(t, "a @valid 120 tx 1500", get("a", Query{ValidAt: 120, TxAt: 1500}), []uint64{n.ID})
	wantIDs(t, "a @valid 170 tx 1500", get("a", Query{ValidAt: 170, TxAt: 1500}), []uint64{n.ID})
	wantIDs(t, "b @valid 170 tx 1500", get("b", Query{ValidAt: 170, TxAt: 1500}), nil)
	// After it (TxAt 2500): "a" keeps [100,150), "b" owns [150,300).
	wantIDs(t, "a @valid 120 tx 2500", get("a", Query{ValidAt: 120, TxAt: 2500}), []uint64{n.ID})
	wantIDs(t, "a @valid 170 tx 2500", get("a", Query{ValidAt: 170, TxAt: 2500}), nil)
	wantIDs(t, "b @valid 170 tx 2500", get("b", Query{ValidAt: 170, TxAt: 2500}), []uint64{n.ID})
	wantIDs(t, "b @valid 250 tx 2500", get("b", Query{ValidAt: 250, TxAt: 2500}), []uint64{n.ID})
	wantIDs(t, "b @valid 300 tx 2500 (exclusive end)", get("b", Query{ValidAt: 300, TxAt: 2500}), nil)
}
