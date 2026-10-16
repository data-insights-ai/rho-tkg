package conformance

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

// The numeric rule (René 2026-10-16: one property equality for every equality
// door). Equality is by exact value: a number equals every stored number of the
// same exact value (3 = 3.0), NaN equals nothing, a string "3" equals no number.
// Identity is by kind: what is stored is what is read back, an integer stays an
// integer and a float stays a float.
//
//	native  v5's rule: a key has one kind, its first value's. An exact numeric of
//	        the other numeric kind converts (3.0 into an int key), anything else
//	        (3.5 or "3" into an int key) is refused. Lookup, effective-page
//	        Filter, Count and GetOrCreateByKey agree on 3 vs 3.0; one stored row
//	        per write. Mixed kinds under one key are v4's rule (compat).
//	compat  v4's rule (branch v4-numeric-eq, pkg/types/property_equal.go): every
//	        width of the same exact value (int8, uint, float32, float64 ...), the
//	        count of an equality door equals its rows, no rounding past 2^53.
//	        The suite has no count or statistics door; "count == rows" is checked
//	        as lookup == filtered scan.
//	both    the ID boundaries.
//

func init() {
	register("NumericRule",
		native("LookupAgreesOn3And3Point0", numLookupAgrees),
		native("ScanFilterAgreesOn3And3Point0", numScanAgrees),
		native("GetOrCreateAgreesOn3And3Point0", numGetOrCreateAgrees),
		native("CountAgreesOn3And3Point0", numCountAgrees),
		native("TypedKeyRefusesOtherKinds", numTypedKey),
		native("NaNAndOtherKindsMatchNothing", numNothingMatches),
		compat("EveryWidthOfTheSameValueIsOneRule", numWidths),
		compat("LookupEqualsFilteredScan", numCountEqualsRows),
		compat("NoRoundingPastTwoToThe53", numNoRounding),
		both("IDBoundariesMaxInt64OkZeroAndNegativeRefused", numIDBoundaries),
	)
}

// v4NumericRuleVersion is the first v4 release that carries the numeric rule on
// every equality door. The v4 adapter pins v4.56.1 today, so the cases that need
// the rule skip there until tools/v5-conformance/go.mod pins v4.57.1.
const v4NumericRuleVersion = "v4.57.1"

func isV5(a Adapter) bool {
	typ := reflect.TypeOf(a)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return strings.HasSuffix(typ.PkgPath(), "/v5adapter")
}

// needNumericRule skips a case for the v4 adapter while the module it runs
// against predates the rule. The v5 adapter must satisfy the case, so it is
// never probed; every other adapter (v4, and test wrappers around it) is. The probe is behavioural (a test binary carries no module
// list): an integer 3 and a float 3.0 are stored and one lookup must find both.
func needNumericRule(t *testing.T, a Adapter) {
	t.Helper()
	if isV5(a) {
		return
	}
	g := open(t, a, false)
	for _, v := range []any{3, 3.0} {
		_, err := g.AddNode(ctx, NodeSpec{Labels: []string{"Probe"}, Props: map[string]any{"v": v}})
		ok(t, err)
	}
	ns, err := g.NodesByLabelAndProperty("Probe", "v", 3, Query{})
	ok(t, err)
	if len(ns) < 2 {
		t.Skipf("v4 numeric rule needs rho-tkg %s; the pinned v4 finds %d of 2 rows for 3 vs 3.0", v4NumericRuleVersion, len(ns))
	}
}

// kindOf is the stored kind of a read-back number: "int", "uint", "float" or the
// Go type for anything else. Widths do not matter.
func kindOf(v any) string {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return "int"
	case uint, uint8, uint16, uint32, uint64:
		return "uint"
	case float32, float64:
		return "float"
	}
	return fmt.Sprintf("%T", v)
}

func wantKind(t *testing.T, what string, n Node, key, want string) {
	t.Helper()
	if got := kindOf(n.Props[key]); got != want {
		t.Fatalf("%s: %s = %v (%s), want kind %s", what, key, n.Props[key], got, want)
	}
}

func lookupIDs(t *testing.T, g Graph, label, key string, v any) []int64 {
	t.Helper()
	ns, err := g.NodesByLabelAndProperty(label, key, v, Query{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty(%s,%s,%v): %v", label, key, v, err)
	}
	return sortedIDs(ns)
}

func scanIDs(t *testing.T, g Graph, key string, v any) []int64 {
	t.Helper()
	pin := Pin{Tx: pinNow(t, g)}
	var out []int64
	for _, page := range walkNodesOpaque(t, g, pin, Filter{Prop: key, Eq: v}, 3) {
		out = append(out, nodeSegIDs(page)...)
	}
	return out
}

func numLookupAgrees(t *testing.T, a Adapter) {
	needNumericRule(t, a)
	g := open(t, a, false)
	i3 := addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 3}})
	f3 := addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 3.0}})
	i4 := addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 4}})

	both3 := sortedIDs([]Node{i3, f3})
	wantIDs(t, "lookup 3", lookupIDs(t, g, "N", "v", 3), both3)
	wantIDs(t, "lookup 3.0", lookupIDs(t, g, "N", "v", 3.0), both3)
	wantIDs(t, "lookup int64(3)", lookupIDs(t, g, "N", "v", int64(3)), both3)
	wantIDs(t, "lookup 4", lookupIDs(t, g, "N", "v", 4), []int64{i4.ID})
	wantIDs(t, "lookup 4.0", lookupIDs(t, g, "N", "v", 4.0), []int64{i4.ID})
	wantIDs(t, "lookup 3.5", lookupIDs(t, g, "N", "v", 3.5), nil)
	wantIDs(t, "lookup string 3", lookupIDs(t, g, "N", "v", "3"), nil)
	all, err := g.NodesByLabel("N", Query{})
	ok(t, err)
	if len(all) != 3 {
		t.Fatalf("%d N nodes, want 3 (one stored row per write)", len(all))
	}
}

func numScanAgrees(t *testing.T, a Adapter) {
	needNumericRule(t, a)
	g := open(t, a, false)
	i3 := addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 3}})
	f3 := addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 3.0}})
	i4 := addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 4}})

	both3 := sortedIDs([]Node{i3, f3})
	for _, c := range []struct {
		what string
		eq   any
		want []int64
	}{
		{"filter 3", 3, both3},
		{"filter 3.0", 3.0, both3},
		{"filter 4.0", 4.0, []int64{i4.ID}},
		{"filter 3.5", 3.5, nil},
		{"filter string 3", "3", nil},
	} {
		got := scanIDs(t, g, "v", c.eq)
		wantIDs(t, c.what, got, c.want)
		// The scan and the lookup are one rule.
		wantIDs(t, c.what+" = lookup", got, lookupIDs(t, g, "N", "v", c.eq))
	}
}

func numGetOrCreateAgrees(t *testing.T, a Adapter) {
	if !isV5(a) {
		// v4 keeps GetOrCreateByKey on indexable non-float keys (ErrUniqueUnsupportedType),
		// also under v4.57.1; the 3 vs 3.0 identity is the v5 rule.
		t.Skip("v4 GetOrCreateByKey refuses float keys; 3 vs 3.0 identity is v5-only")
	}
	// A key has one kind (its first value's): an exact numeric of the other kind
	// converts, an inexact one is refused. So 3, 3.0 and int64(3) are one identity
	// and the stored kind is the key's.
	g := open(t, a, false)
	first, created, err := g.GetOrCreateByKey(ctx, "K", "k", 3, nil)
	ok(t, err)
	if !created {
		t.Fatalf("first GetOrCreate(3) did not create")
	}
	wantKind(t, "created", first, "k", "int")
	for _, v := range []any{3, 3.0, int64(3)} {
		again, created, err := g.GetOrCreateByKey(ctx, "K", "k", v, nil)
		ok(t, err)
		if created || again.ID != first.ID {
			t.Fatalf("GetOrCreate(%v): created=%v id=%d, want the existing node %d", v, created, again.ID, first.ID)
		}
		wantKind(t, fmt.Sprintf("GetOrCreate(%v) hit", v), again, "k", "int")
	}
	// 3.5 does not fit an integer key: refused, nothing created.
	_, created, err = g.GetOrCreateByKey(ctx, "K", "k", 3.5, nil)
	if err == nil || created {
		t.Fatalf("GetOrCreate(3.5) on an integer key: created=%v err=%v, want a refusal", created, err)
	}
	is(t, "3.5 on an integer key", err, ErrInvalidArgument)
	ns, err := g.NodesByLabel("K", Query{})
	ok(t, err)
	if len(ns) != 1 {
		t.Fatalf("%d K nodes, want 1", len(ns))
	}
}

func numNothingMatches(t *testing.T, a Adapter) {
	needNumericRule(t, a)
	g := open(t, a, false)
	if isV5(a) {
		// v5 values are finite: a non-finite number is refused at the write, so it
		// can neither be stored nor matched.
		for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
			_, err := g.AddNode(ctx, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": f}})
			is(t, fmt.Sprintf("write %v", f), err, ErrInvalidArgument)
		}
	} else {
		addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": math.NaN()}})
	}
	addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": 3}})
	addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"s": "NaN"}})
	wantIDs(t, "lookup NaN", lookupIDs(t, g, "N", "v", math.NaN()), nil)
	wantIDs(t, "filter NaN", scanIDs(t, g, "v", math.NaN()), nil)
	wantIDs(t, "lookup string 3 finds no number", lookupIDs(t, g, "N", "v", "3"), nil)
	wantIDs(t, "lookup +Inf", lookupIDs(t, g, "N", "v", math.Inf(1)), nil)
}

// numWidths: v4 rule, every width of the same exact value.
func numWidths(t *testing.T, a Adapter) {
	needNumericRule(t, a)
	g := open(t, a, true)
	vals := []any{int8(3), int(3), int64(3), uint(3), uint16(3), float32(3), 3.0}
	var want []int64
	for _, v := range vals {
		n := addNode(t, g, NodeSpec{Labels: []string{"W"}, Props: map[string]any{"v": v}, TxFrom: ts(1000)})
		want = append(want, n.ID)
	}
	other := addNode(t, g, NodeSpec{Labels: []string{"W"}, Props: map[string]any{"v": 4}, TxFrom: ts(1000)})
	negZero := addNode(t, g, NodeSpec{Labels: []string{"W"}, Props: map[string]any{"z": math.Copysign(0, -1)}, TxFrom: ts(1000)})
	zero := addNode(t, g, NodeSpec{Labels: []string{"W"}, Props: map[string]any{"z": 0}, TxFrom: ts(1000)})
	for _, probe := range vals {
		wantIDs(t, fmt.Sprintf("lookup %T(%v)", probe, probe), lookupIDs(t, g, "W", "v", probe), want)
	}
	wantIDs(t, "lookup 4.0", lookupIDs(t, g, "W", "v", 4.0), []int64{other.ID})
	wantIDs(t, "-0.0 equals 0", lookupIDs(t, g, "W", "z", 0), sortedIDs([]Node{negZero, zero}))
	wantIDs(t, "0.0 equals -0.0", lookupIDs(t, g, "W", "z", math.Copysign(0, -1)), sortedIDs([]Node{negZero, zero}))
	// Stored kinds are untouched.
	for i, id := range want {
		wantKind(t, fmt.Sprintf("stored %T", vals[i]), mustGet(t, g, id), "v", kindOf(vals[i]))
	}
}

// numCountEqualsRows: the v4 equality counts equal the rows of the lookup
// (CountByLabelPropertyValue = len(ByLabelAndProperty)); the suite has no count
// door, so the effective-page scan stands in for the second door.
func numCountEqualsRows(t *testing.T, a Adapter) {
	needNumericRule(t, a)
	g := open(t, a, true)
	for _, v := range []any{1, int8(1), 1.0, float32(1), uint(1), 2, 2.5, "1"} {
		addNode(t, g, NodeSpec{Labels: []string{"C"}, Props: map[string]any{"v": v}, TxFrom: ts(1000)})
	}
	for _, probe := range []any{1, int8(1), uint32(1), 1.0, float32(1), 2, 2.0, 2.5, 3, "1", math.NaN()} {
		rows := lookupIDs(t, g, "C", "v", probe)
		scan := scanIDs(t, g, "v", probe)
		if len(rows) != len(scan) {
			t.Fatalf("probe %T(%v): lookup %d rows, filtered scan %d rows", probe, probe, len(rows), len(scan))
		}
		wantIDs(t, fmt.Sprintf("probe %T(%v)", probe, probe), scan, rows)
	}
	if got := len(lookupIDs(t, g, "C", "v", 1)); got != 5 {
		t.Fatalf("lookup 1 = %d rows, want 5 (1, int8(1), 1.0, float32(1), uint(1))", got)
	}
}

// numNoRounding: equality is exact, an integer past 2^53 is not the float it
// rounds to.
func numNoRounding(t *testing.T, a Adapter) {
	needNumericRule(t, a)
	g := open(t, a, true)
	const p = int64(1) << 53
	odd := addNode(t, g, NodeSpec{Labels: []string{"R"}, Props: map[string]any{"v": p + 1}, TxFrom: ts(1000)})
	exact := addNode(t, g, NodeSpec{Labels: []string{"R"}, Props: map[string]any{"v": p}, TxFrom: ts(1000)})
	fl := addNode(t, g, NodeSpec{Labels: []string{"R"}, Props: map[string]any{"v": float64(p)}, TxFrom: ts(1000)})
	wantIDs(t, "lookup 2^53+1", lookupIDs(t, g, "R", "v", p+1), []int64{odd.ID})
	wantIDs(t, "lookup float64(2^53)", lookupIDs(t, g, "R", "v", float64(p)), sortedIDs([]Node{exact, fl}))
	wantIDs(t, "lookup 2^53", lookupIDs(t, g, "R", "v", p), sortedIDs([]Node{exact, fl}))
	wantIDs(t, "lookup MaxInt64", lookupIDs(t, g, "R", "v", int64(math.MaxInt64)), nil)
	wantIDs(t, "lookup float64(2^63)", lookupIDs(t, g, "R", "v", float64(1<<63)), nil)
}

// numIDBoundaries: MaxInt64 is a valid public ID (absent, so not found); 0 and
// negative IDs are refused as invalid arguments, on read and write doors alike.
func numIDBoundaries(t *testing.T, a Adapter) {
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"I"}})
	if n.ID <= 0 {
		t.Fatalf("minted ID %d, want a positive ID", n.ID)
	}
	for _, id := range []int64{math.MaxInt64, math.MaxInt64 - 1} {
		_, err := g.GetNode(ctx, id)
		is(t, fmt.Sprintf("GetNode(%d)", id), err, ErrNotFound)
		_, err = g.UpdateNode(ctx, id, map[string]any{"x": 1}, 0)
		is(t, fmt.Sprintf("UpdateNode(%d)", id), err, ErrNotFound)
		is(t, fmt.Sprintf("DeleteNode(%d)", id), g.DeleteNode(ctx, id, 0), ErrNotFound)
		_, err = g.GetRel(ctx, id)
		is(t, fmt.Sprintf("GetRel(%d)", id), err, ErrNotFound)
	}
	for _, id := range []int64{0, -1, -2, math.MinInt64} {
		_, err := g.GetNode(ctx, id)
		is(t, fmt.Sprintf("GetNode(%d)", id), err, ErrInvalidArgument)
		_, err = g.UpdateNode(ctx, id, map[string]any{"x": 1}, 0)
		is(t, fmt.Sprintf("UpdateNode(%d)", id), err, ErrInvalidArgument)
		is(t, fmt.Sprintf("DeleteNode(%d)", id), g.DeleteNode(ctx, id, 0), ErrInvalidArgument)
		_, err = g.GetRel(ctx, id)
		is(t, fmt.Sprintf("GetRel(%d)", id), err, ErrInvalidArgument)
		_, err = g.AddRel(ctx, RelSpec{Type: "T", From: id, To: n.ID})
		is(t, fmt.Sprintf("AddRel from %d", id), err, ErrInvalidArgument)
	}
	// The refusals changed nothing.
	cur := mustGet(t, g, n.ID)
	if cur.Version != 0 {
		t.Fatalf("refused writes changed node %d: %+v", n.ID, cur)
	}
}

// numCountAgrees: the count door equals the rows of the lookup for 3 vs 3.0.
func numCountAgrees(t *testing.T, a Adapter) {
	needNumericRule(t, a)
	g := open(t, a, false)
	for _, v := range []any{3, 3.0, 4} {
		addNode(t, g, NodeSpec{Labels: []string{"N"}, Props: map[string]any{"v": v}})
	}
	for _, probe := range []any{3, 3.0, 4, 3.5, 3.25, "3"} {
		rows := lookupIDs(t, g, "N", "v", probe)
		c, err := g.CountByLabelPropertyValue("N", "v", probe)
		ok(t, err)
		if c != len(rows) {
			t.Fatalf("probe %T(%v): count %d, lookup %d rows", probe, probe, c, len(rows))
		}
	}
}

// numTypedKey: v5's one-kind-per-key rule. The first value declares the key's
// kind; an exact numeric of the other numeric kind converts and reads back in
// the key's kind; 3.5 or "3" into an int key, a number into a string key are
// refused and change nothing. v4 keys are dynamically typed (compat).
func numTypedKey(t *testing.T, a Adapter) {
	if !isV5(a) {
		t.Skip("v4 property keys are dynamically typed; the typed-key rule is v5-only")
	}
	g := open(t, a, false)
	n := addNode(t, g, NodeSpec{Labels: []string{"T"}, Props: map[string]any{"i": 3, "f": 2.5, "s": "x"}})
	m := addNode(t, g, NodeSpec{Labels: []string{"T"}, Props: map[string]any{"i": 4.0, "f": 2}})
	wantKind(t, "4.0 into an int key", mustGet(t, g, m.ID), "i", "int")
	wantKind(t, "2 into a float key", mustGet(t, g, m.ID), "f", "float")
	for _, c := range []struct {
		key string
		v   any
	}{{"i", 3.5}, {"i", "3"}, {"i", true}, {"f", "2.5"}, {"s", 1}, {"s", 1.5}} {
		_, err := g.AddNode(ctx, NodeSpec{Labels: []string{"T"}, Props: map[string]any{c.key: c.v}})
		is(t, fmt.Sprintf("AddNode %s=%T(%v)", c.key, c.v, c.v), err, ErrInvalidArgument)
		_, err = g.UpdateNode(ctx, n.ID, map[string]any{c.key: c.v}, 0)
		is(t, fmt.Sprintf("UpdateNode %s=%T(%v)", c.key, c.v, c.v), err, ErrInvalidArgument)
	}
	cur := mustGet(t, g, n.ID)
	if cur.Version != 0 || cur.Props["i"] != int64(3) {
		t.Fatalf("refused updates changed node %d: %+v", n.ID, cur)
	}
	ns, err := g.NodesByLabel("T", Query{})
	ok(t, err)
	if len(ns) != 2 {
		t.Fatalf("%d T nodes, want 2: a refused write created a node", len(ns))
	}
}
