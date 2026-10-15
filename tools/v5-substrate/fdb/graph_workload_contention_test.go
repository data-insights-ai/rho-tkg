package main

import (
	"bytes"
	"errors"
	"testing"
)

// Authored only. These guards grant no actual concurrent SDK evidence.
func TestGraphContentionFreshProfileAndIncrementGuards(t *testing.T) {
	transport := graphTransportGuardPolicy()
	transport.Spec.Family = graphContentionFamily
	transport.CausalMinimumMillis, _ = graphTransportCausalMinimum(transport.Spec, transport.Transport)
	freshConfig := graphTransportConfig(transport.Spec, "01000000000000000000000000000000")
	ordinary := graphContentionPolicy(freshConfig)
	bound := graphContentionPolicyAdmission(freshConfig, &transport)
	ordinaryBytes, _ := wire(ordinary)
	if bound.Version != 2 || bound.Admission != graphTransportDigest(&transport) || ordinary.Version != 1 || ordinary.Admission != [32]byte{} || bytes.Contains(ordinaryBytes, []byte("Admission")) {
		t.Fatal("ordinary bytes or exact admission digest changed")
	}
	p := graphCostProfile{Version: 2, Facts: 20, CrossPercent: 50, Writers: 8}
	c := graphContentionConfig(p, "kv", "01020304050607080910111213141516")
	if !validGraphCostProfile(c) || c.Limits.Transactions != graphContentionLimit(p).Transactions {
		t.Fatal("fresh quota/profile mismatch")
	}
	legacy := graphCostConfig("graph-cost-v1", graphCostProfile{Version: 1, Facts: 20, CrossPercent: 50, Writers: 1})
	if validGraphContentionConfig(legacy) || graphCostLimit(legacy).Transactions != 43 {
		t.Fatal("legacy config silently widened")
	}
	if graphContentionIncrement(value{Value: 5, Version: 1}, 6) != nil {
		t.Fatal("actual increment rejected")
	}
	for _, next := range []int64{5, 7} {
		if err := graphContentionIncrement(value{Value: 5, Version: 1}, next); !errors.Is(err, errInvalid) {
			t.Fatal("lost increment admitted", next, err)
		}
	}
	if _, err := graphContentionTarget(p, "hot", 20); !errors.Is(err, errInvalid) {
		t.Fatal("foreign logical operation admitted", err)
	}
	if target, err := graphContentionTarget(p, "hot", 1); err != nil || !p.cross(target) {
		t.Fatal("remote hot request redirected locally", target, err)
	}
	if validGraphContentionConfig(graphContentionConfig(p, "kv", "00000000000000000000000000000000")) {
		t.Fatal("fresh run identity missing")
	}
	for _, cross := range []uint64{0, 10, 50, 100} {
		p.CrossPercent = cross
		var hot uint64
		for fact := uint64(0); fact < p.Facts/2; fact++ {
			hot += uint64(graphContentionExpectedValue(p, fact, false) - 5)
		}
		if hot != p.Facts || graphContentionExpectedValue(p, p.Facts/2, true) != 0 {
			t.Fatal("hot mix or old absence", cross, hot)
		}
	}
}
