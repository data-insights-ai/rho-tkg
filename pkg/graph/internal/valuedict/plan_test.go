package valuedict

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestNewPlanRefusesBadConfig: every malformed list is refused at open, so a
// typo cannot code the wrong property or none. Catches a check dropped.
func TestNewPlanRefusesBadConfig(t *testing.T) {
	m := func(label, rel, prop string) storepkg.ValueDictionaryMember {
		return storepkg.ValueDictionaryMember{Label: label, RelType: rel, Property: prop}
	}
	ok := storepkg.ValueDictionary{Name: "a", Cap: 1, Members: []storepkg.ValueDictionaryMember{m("", "NETWORK", "action")}}
	for name, cfg := range map[string][]storepkg.ValueDictionary{
		"empty name":         {{Cap: 1, Members: ok.Members}},
		"repeated name":      {ok, ok},
		"zero cap":           {{Name: "a", Members: ok.Members}},
		"no members":         {{Name: "a", Cap: 1}},
		"label and rel type": {{Name: "a", Cap: 1, Members: []storepkg.ValueDictionaryMember{m("Host", "NETWORK", "x")}}},
		"neither":            {{Name: "a", Cap: 1, Members: []storepkg.ValueDictionaryMember{m("", "", "x")}}},
		"no property":        {{Name: "a", Cap: 1, Members: []storepkg.ValueDictionaryMember{m("Host", "", "")}}},
		"pair in two":        {ok, {Name: "b", Cap: 1, Members: ok.Members}},
	} {
		if p, err := NewPlan(cfg); err == nil {
			t.Fatalf("%s: accepted (%+v)", name, p)
		}
	}
	p, err := NewPlan([]storepkg.ValueDictionary{ok, {Name: "b", Cap: 2, Members: []storepkg.ValueDictionaryMember{m("Host", "", "action")}}})
	if err != nil {
		t.Fatal(err)
	}
	if d, found := p.RelDict("NETWORK", "action"); !found || d != 0 {
		t.Fatalf("RelDict = %d %v", d, found)
	}
	if d, found := p.NodeDict("Host", "action"); !found || d != 1 || !p.NodeMembers() || p.Cap(1) != 2 {
		t.Fatalf("NodeDict = %d %v", d, found)
	}
	if _, found := p.NodeDict("NETWORK", "action"); found {
		t.Fatal("a rel-type member matched a node label")
	}
	if p, err := NewPlan(nil); p != nil || err != nil {
		t.Fatalf("NewPlan(nil) = %v, %v; want no plan", p, err)
	}
}
