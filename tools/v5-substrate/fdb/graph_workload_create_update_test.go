package main

import (
	"errors"
	"testing"
)

// Authored guards only; no runtime/SDK or matrix qualification is implied.
func TestGraphCreateUpdateActualBusinessFootprint(t *testing.T) {
	for _, cross := range []uint64{0, 10, 50, 100} {
		spec := graphSelectorsSpec{1, graphCreateUpdateFamily, 40, cross, 8, "kv"}
		c := graphCreateUpdateConfig(spec, "01020304050607080910111213141516")
		if !validGraphCostProfile(c) || c.Cost.Version != 9 || validGraphColumnsConfig(c) || validGraphSelectorsConfig(c) {
			t.Fatal("fresh family/old binding isolation", c)
		}
		h := &harness{config: c}
		var multiple uint64
		for fact := uint64(0); fact < 40; fact++ {
			x := h.graphTransaction("guard", 0, graphCommand{Kind: "edge-create", Edge: c.Cost.edge(fact, 5)}, snapshot{Values: [2]map[string]value{{}, {}}})
			reads, writes, err := graphCreateUpdateFootprint(x)
			if err != nil || reads != writes || !reads[0] || reads[1] != c.Cost.cross(fact) {
				t.Fatal("complete read/write participants", reads, writes, err)
			}
			if reads[1] {
				multiple++
			}
			if !c.Cost.cross(fact) {
				x.Participants = append(x.Participants, participant{Group: 1, Epoch: 11})
				if _, _, err := graphCreateUpdateFootprint(x); !errors.Is(err, errInvalid) {
					t.Fatal("empty control participant became business cross", err)
				}
			}
		}
		if multiple*100 != 40*cross {
			t.Fatal("nominal ratio differs from actual footprint")
		}
	}
	if _, _, err := graphCreateUpdateFootprint(transaction{}); !errors.Is(err, errInvalid) {
		t.Fatal("missing business command", err)
	}
	bad := transaction{GraphChange: new(graphCommand{Kind: "edge-create", Edge: graphEdge{Target: endpoint{Owner: 2}}})}
	if _, _, err := graphCreateUpdateFootprint(bad); !errors.Is(err, errInvalid) {
		t.Fatal("malformed endpoint panicked or passed", err)
	}
	old := graphSelectorsConfig(graphSelectorsSpec{1, graphSelectorsFamily, 40, 0, 8, "kv"}, "01020304050607080910111213141516")
	if validGraphCreateUpdateConfig(old) {
		t.Fatal("old family relabelled")
	}
}
