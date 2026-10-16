package importmap

import (
	"encoding/json"
	"testing"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// ai-soc stores []byte fact_wire/occurrence_wire; sigma stores ov_support []int64.
// Both survive the mapper kind-preserving, as ops and as graph.Value.
func TestImportBytesAndListShapesSurviveMapping(t *testing.T) {
	props := map[string]fixtures.Typed{
		"fact_wire":  ty("bytes", "00ff0102"),
		"empty_wire": ty("bytes", ""),
		"ov_support": {T: "[]int64", V: []fixtures.Typed{ty("int64", "3"), ty("int64", "-7")}},
		"names":      {T: "[]string", V: []fixtures.Typed{ty("string", "a")}},
		"any":        {T: "[]interface {}", V: []fixtures.Typed{ty("int64", "1"), ty("string", "x"), ty("bool", "true"), ty("float64", "2.5"), ty("bytes", "01")}},
	}
	cur := row(1, 0, 0, 0, 5000, props)
	r := one(t, src("tx_from", fixtures.Entity{ID: 1, Current: &cur}))
	if len(r.Ambiguities) != 0 {
		t.Fatalf("ambiguities %+v", r.Ambiguities)
	}
	if len(r.Ops) != 1 || len(r.Ops[0].Props) != len(props) {
		t.Fatalf("ops %+v", r.Ops)
	}
	got := r.Ops[0].Props
	if b, ok := got["fact_wire"].AsBytes(); !ok || string(b) != "\x00\xff\x01\x02" {
		t.Errorf("fact_wire %v", got["fact_wire"])
	}
	if b, ok := got["empty_wire"].AsBytes(); !ok || len(b) != 0 {
		t.Errorf("empty_wire %v", got["empty_wire"])
	}
	els, ok := got["ov_support"].AsList()
	if !ok || len(els) != 2 || els[0] != graph.Int(3) || els[1] != graph.Int(-7) {
		t.Errorf("ov_support %v", got["ov_support"])
	}
	if els, ok := got["any"].AsList(); !ok || len(els) != 5 || els[4].Kind() != graph.KindBytes || els[3].Kind() != graph.KindFloat {
		t.Errorf("any %v", got["any"])
	}
	// the op JSON keeps the kind and every element, and is stable.
	a, _ := json.Marshal(r.Ops[0].Props["ov_support"])
	b, _ := json.Marshal(r.Ops[0].Props["ov_support"])
	if string(a) != string(b) || string(a) != `{"k":"list","e":[{"k":"i64","v":"3"},{"k":"i64","v":"-7"}]}` {
		t.Errorf("list json %s", a)
	}
	by, _ := json.Marshal(r.Ops[0].Props["fact_wire"])
	if string(by) != `{"k":"bytes","v":"00ff0102"}` {
		t.Errorf("bytes json %s", by)
	}
}

// A list holding anything unmappable is reported once with the element's reason and dropped whole.
func TestImportListWithUnmappableElementIsReportedWhole(t *testing.T) {
	props := map[string]fixtures.Typed{
		"l": {T: "[]interface {}", V: []fixtures.Typed{ty("int64", "1"), {T: "map", V: map[string]fixtures.Typed{}}}},
		"n": {T: "[][]int64", V: []fixtures.Typed{{T: "[]int64", V: []fixtures.Typed{}}}},
	}
	cur := row(1, 0, 0, 0, 5000, props)
	r := one(t, src("tx_from", fixtures.Entity{ID: 1, Current: &cur}))
	var kinds []string
	for _, x := range r.Ambiguities {
		kinds = append(kinds, x.Key+"/"+x.Kind)
	}
	if len(kinds) != 2 || kinds[0] != "l/map" || kinds[1] != "n/list" {
		t.Fatalf("ambiguities %v", kinds)
	}
}

// JSON-decoded fixtures (expected.json) hold list elements as maps, not Typed.
func TestMapValueAcceptsJSONDecodedLists(t *testing.T) {
	var ty fixtures.Typed
	if err := json.Unmarshal([]byte(`{"t":"[]int64","v":[{"t":"int64","v":"3"},{"t":"int64","v":"4"}]}`), &ty); err != nil {
		t.Fatal(err)
	}
	v, bad := MapValue(ty)
	els, ok := v.AsList()
	if bad != "" || !ok || len(els) != 2 || els[1] != graph.Int(4) {
		t.Fatalf("%q %v", bad, v)
	}
}
