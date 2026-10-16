package importmap

import (
	"math"
	"strconv"
	"testing"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func row(id int64, ver uint32, vf, vt, tf int64, props map[string]fixtures.Typed) fixtures.Row {
	return fixtures.Row{ID: id, Labels: []string{"L"}, Props: props, ValidFrom: vf, ValidTo: vt, TxFrom: tf, Version: ver}
}

func src(mode string, nodes ...fixtures.Entity) *fixtures.Source {
	return &fixtures.Source{Config: fixtures.Config{UntimedValidStart: mode}, Nodes: nodes}
}

func p(k, t, v string) map[string]fixtures.Typed { return map[string]fixtures.Typed{k: ty(t, v)} }

func one(t *testing.T, s *fixtures.Source) *Result {
	t.Helper()
	r, err := Map(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Scope: valid_from 0 = unset -> the row's TxFrom; MinInt64 -> -inf; valid_to 0 -> +inf.
// Recorded is the v4 TxFrom, on every op, in both untimed modes.
func TestScopeAndRecordedRules(t *testing.T) {
	cur := row(1, 0, 0, 0, 5000, p("a", "int64", "1"))
	r := one(t, src("tx_from", fixtures.Entity{ID: 1, Current: &cur}))
	if len(r.Ops) != 1 || r.Ops[0].Op != "create_node" {
		t.Fatalf("ops %+v", r.Ops)
	}
	o := r.Ops[0]
	if o.Scope != (Scope{From: At(5000), To: PosInf}) || o.Recorded != 5000 {
		t.Errorf("unset valid_from: scope %+v recorded %d", o.Scope, o.Recorded)
	}

	cases := []struct {
		name      string
		mode      string
		vf, vt    int64
		from, to  Edge
		ambiguity string
	}{
		{"unbounded start", "unbounded", math.MinInt64, 0, NegInf, PosInf, ""},
		{"explicit past", "tx_from", -4_000_000_000_000, 9000, At(-4_000_000_000_000), At(9000), ""},
		{"explicit zero first row in unbounded", "unbounded", 0, 0, At(0), PosInf, ""},
		{"MinInt64 in a tx_from store", "tx_from", math.MinInt64, 0, NegInf, PosInf, "unbounded_start_in_txfrom_store"},
		{"reversed interval", "tx_from", 9000, 8000, Edge{}, Edge{}, "empty_scope"},
		{"empty interval", "tx_from", 9000, 9000, Edge{}, Edge{}, "empty_scope"},
	}
	for _, c := range cases {
		cur := row(1, 0, c.vf, c.vt, 5000, p("a", "int64", "1"))
		r := one(t, src(c.mode, fixtures.Entity{ID: 1, Current: &cur}))
		var kinds []string
		for _, a := range r.Ambiguities {
			kinds = append(kinds, a.Kind)
		}
		if c.ambiguity == "" && len(kinds) != 0 || c.ambiguity != "" && (len(kinds) != 1 || kinds[0] != c.ambiguity) {
			t.Errorf("%s: ambiguities %v want %q", c.name, kinds, c.ambiguity)
		}
		if c.ambiguity == "empty_scope" {
			if len(r.Ops) != 0 {
				t.Errorf("%s: an empty scope must emit no op", c.name)
			}
			continue
		}
		if len(r.Ops) != 1 || r.Ops[0].Scope.From != c.from || r.Ops[0].Scope.To != c.to {
			t.Errorf("%s: ops %+v want %+v..%+v", c.name, r.Ops, c.from, c.to)
		}
	}
}

// Order: rows sort by (TxFrom, Version) whatever order v4 returned them in; a
// current row with a low version but a late TxFrom comes after its history.
func TestChainOrderAndDeleteRetractLife(t *testing.T) {
	h1 := row(1, 1, 0, 0, 7000, p("a", "int64", "2"))
	c0 := row(1, 0, 0, 0, 3000, p("a", "int64", "1"))
	del := row(1, 2, 0, 9000, 8000, p("a", "int64", "3"))
	del.DeletedAt = 9000
	re := row(1, 3, 0, 0, 9500, p("a", "int64", "4"))
	ret := row(1, 4, 0, 9900, 9600, p("a", "int64", "5"))
	ret.DeletedAt, ret.Retracted = 9700, true
	void := row(1, 5, 100, 200, 9800, nil)
	void.Void = true
	r := one(t, src("tx_from", fixtures.Entity{ID: 1, Current: &c0, History: []fixtures.Row{void, ret, re, del, h1}}))
	var got []string
	for _, o := range r.Ops {
		got = append(got, o.Op)
		if o.Recorded == 0 {
			t.Errorf("op %+v has no Recorded", o)
		}
	}
	want := []string{"create_node", "correct", "correct", "end", "create_node", "correct", "retract"}
	if len(got) != len(want) {
		t.Fatalf("ops %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ops %v want %v", got, want)
		}
	}
	if r.Ops[3].Recorded != 9000 || r.Ops[3].Scope.To != At(9000) {
		t.Errorf("end op: %+v", r.Ops[3])
	}
	if r.Ops[6].Recorded != 9700 {
		t.Errorf("retract recorded %d", r.Ops[6].Recorded)
	}
	kinds := map[string]int{}
	for _, a := range r.Ambiguities {
		kinds[a.Kind]++
	}
	if kinds["void_row"] != 1 || kinds["reimport_after_delete"] != 1 || len(kinds) != 2 {
		t.Errorf("ambiguities %v", kinds)
	}
}

// An ambiguous property is dropped from the op and reported with entity, id, version, key and kind;
// it does not turn into an Unset on the next row, and an op left with nothing to set is reported.
func TestAmbiguousPropertyIsDroppedAndReported(t *testing.T) {
	mk := func() map[string]fixtures.Typed {
		return map[string]fixtures.Typed{
			"big": ty("uint64", "18446744073709551615"),
			"l":   {T: "[]int64", V: []fixtures.Typed{ty("int64", "1")}}}
	}
	a := row(1, 0, 0, 0, 3000, mk())
	b := row(1, 1, 0, 0, 4000, mk())
	r := one(t, src("tx_from", fixtures.Entity{ID: 1, Current: &a, History: []fixtures.Row{b}}))
	if len(r.Ops) != 1 || r.Ops[0].Op != "create_node" || len(r.Ops[0].Props) != 0 {
		t.Fatalf("ops %+v", r.Ops)
	}
	var rows []string
	for _, x := range r.Ambiguities {
		rows = append(rows, x.Entity+"/"+strconv.FormatInt(x.ID, 10)+"/v"+strconv.FormatUint(uint64(x.Version), 10)+"/"+x.Key+"/"+x.Kind)
	}
	want := []string{"node/1/v0/big/uint_over_maxint64", "node/1/v0/l/list", "node/1/v1//empty_correction", "node/1/v1/big/uint_over_maxint64", "node/1/v1/l/list"}
	if len(rows) != len(want) {
		t.Fatalf("rows %v want %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("rows %v want %v", rows, want)
		}
	}
	// A key that is still present but ambiguous is not an Unset; a key that is gone is.
	c := row(1, 2, 0, 0, 5000, map[string]fixtures.Typed{"big": ty("uint64", "18446744073709551615")})
	r = one(t, src("tx_from", fixtures.Entity{ID: 1, Current: &a, History: []fixtures.Row{b, c}}))
	if n := len(r.Ops); n != 2 || r.Ops[1].Op != "unset" || r.Ops[1].Key != "l" {
		t.Errorf("ops %+v", r.Ops)
	}
}

func TestMapRefusesBadSource(t *testing.T) {
	if _, err := Map(nil); err == nil {
		t.Error("nil source")
	}
	if _, err := Map(src("later")); err == nil {
		t.Error("unknown untimed mode")
	}
	neg := row(-1, 0, 0, 0, 1, p("a", "int64", "1"))
	if _, err := Map(src("tx_from", fixtures.Entity{ID: -1, Current: &neg})); err == nil {
		t.Error("negative id")
	}
	zero := row(0, 0, 0, 0, 1, p("a", "int64", "1"))
	if _, err := Map(src("tx_from", fixtures.Entity{ID: 0, Current: &zero})); err == nil {
		t.Error("zero id")
	}
	dup := row(1, 0, 0, 0, 1, p("a", "int64", "1"))
	if _, err := Map(src("tx_from", fixtures.Entity{ID: 1, Current: &dup}, fixtures.Entity{ID: 1, Current: &dup})); err == nil {
		t.Error("duplicate entity id")
	}
	if _, err := Map(src("tx_from", fixtures.Entity{ID: 2, Current: &dup})); err == nil {
		t.Error("row id differs from entity id")
	}
}

// Every op of the four fixtures is a valid v5 graph.Op with an explicit scope on the graph's axes.
func TestOpsAreValidGraphOps(t *testing.T) {
	var gid types.GraphID
	gid[0] = 1
	var a1, a2 temporal.AxisID
	a1[0], a2[0] = 1, 2
	l := temporal.Limits{}
	da, err := types.NewDefaultAxisBinding(gid, a1, l)
	if err != nil {
		t.Fatal(err)
	}
	ka, err := types.NewDefaultAxisBinding(gid, a2, l)
	if err != nil {
		t.Fatal(err)
	}
	cfg := graph.Config{Memory: true, GraphID: gid, DefaultAxis: da, KnowledgeAxis: ka, Durability: graph.DurabilityBuffered}
	for _, n := range names {
		r, err := Map(load(t, n))
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Ops) == 0 {
			t.Fatalf("%s: no ops", n)
		}
		for i, o := range r.Ops {
			g, err := o.Graph(cfg)
			if err != nil {
				t.Fatalf("%s op %d %+v: %v", n, i, o, err)
			}
			if err := graph.CheckOp(cfg, g); err != nil {
				t.Errorf("%s op %d (%s %d): %v", n, i, o.Op, o.ID, err)
			}
		}
	}
	bad := Op{Op: "create_node", ID: 1, Scope: Scope{From: At(1), To: At(1)}, Recorded: 1}
	if g, err := bad.Graph(cfg); err == nil {
		if graph.CheckOp(cfg, g) == nil {
			t.Error("an empty scope must not pass CheckOp")
		}
	}
}

// Hashes on or off give the same op stream except the evidence fields.
func TestHashesOnlyAddEvidence(t *testing.T) {
	for _, pair := range [][2]string{{"txfrom_hashed", "txfrom_nohash"}, {"unbounded_hashed", "unbounded_nohash"}} {
		h, _ := Map(load(t, pair[0]))
		n, _ := Map(load(t, pair[1]))
		if len(h.Ops) != len(n.Ops) {
			t.Fatalf("%v: %d vs %d ops", pair, len(h.Ops), len(n.Ops))
		}
		seen := false
		for i := range h.Ops {
			if h.Ops[i].Hash != "" {
				seen = true
			}
			h.Ops[i].Hash, h.Ops[i].PrevHash = "", ""
			if n.Ops[i].Hash != "" {
				t.Errorf("%s: unhashed store carries a hash", pair[1])
			}
		}
		if !seen {
			t.Errorf("%s: no op carries a hash", pair[0])
		}
		if string(h.OpsJSON()) != string(n.OpsJSON()) {
			t.Errorf("%v: op streams differ beyond hashes", pair)
		}
	}
}
