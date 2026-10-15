package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"
)

// Authored only; the real SDK/concurrent histories and paired native process
// cases are separate from these direct shape/identity refusal guards.
func TestGraphEdgeUniqueFreshIdentityAndOldOwnerCloseFootprints(t *testing.T) {
	p := graphCostProfile{Version: 4, Facts: 20, CrossPercent: 50, Writers: 32}
	c := graphEdgeUniqueConfig(p, "columns", "01020304050607080910111213141516")
	h := &harness{config: c}
	if !validGraphEdgeUniqueConfig(c) || !validGraphCostProfile(c) || c.Limits.Versions != 7+11*p.Facts {
		t.Fatal("fresh family binding/grant mismatch")
	}
	old, err := graphEdgeUniqueEdge(p, 1, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	next, err := graphEdgeUniqueEdge(p, 1, 1, true)
	if err != nil || old.ID == next.ID || old.Value != next.Value || old.Target.Life == next.Target.Life || old.Target.Node != next.Target.Node {
		t.Fatal("ABA candidates reused identity or lost scalar equality", err)
	}
	closeOld := graphCommand{Kind: "edge-close", Edge: old}
	closeOld.Edge.Value = 0
	s := snapshot{Values: [2]map[string]value{{}, {}}, Generations: [2]uint64{11, 13}}
	keys := graphEdgeUniqueReadKeys(closeOld)
	for g := range 2 {
		for _, key := range keys[g] {
			s.Values[g][key] = value{Value: old.Value, Version: 2, Round: 7, TxID: "original-old"}
		}
	}
	uniqueKey := graphEdgeUniqueKey(old.Value)
	s.Values[0][uniqueKey] = value{Value: int64(next.ID), Version: 3, Round: 9, TxID: "new-life-winner"} // #nosec G115 -- fixed test ID13.
	x := h.graphEdgeUniqueTransaction("close-old-after-new", closeOld, s)
	x.Lifecycle = &lifecycleMetadata{Provenance: 17, EdgeLife: old.ID + 1}
	if err := h.validate(x); err != nil {
		t.Fatal("old inactive relation cannot close without deleting new claim", err)
	}
	if slices.ContainsFunc(x.Participants[0].Effects, func(e effect) bool { return e.Key == uniqueKey }) {
		t.Fatal("old close deleted another actual owner's unique head")
	}
	// A pre-ABA frozen close had the exact old head revision and a conditional
	// tombstone. It is a different immutable original; apply must recheck that
	// predicate, never silently rewrite its request body to the later shape.
	s.Values[0][uniqueKey] = value{Value: int64(old.ID), Version: 2, Round: 7, TxID: "original-old"} // #nosec G115 -- fixed test ID11.
	prior := h.graphEdgeUniqueTransaction("close-old-before-new", closeOld, s)
	prior.Lifecycle = x.Lifecycle
	if err := h.validate(prior); err != nil || !slices.ContainsFunc(prior.Participants[0].Effects, func(e effect) bool { return e.Key == uniqueKey && e.Delete }) {
		t.Fatal("exact old-owner release footprint missing", err)
	}
	if digest, err := graphEdgeUniqueOriginalDigest(prior); err != nil || digest == ([32]byte{}) {
		t.Fatal("original body binding missing", err)
	}
	prior.Participants[0].Reads[0].Key = "fake-empty-range"
	if err := h.validate(prior); !errors.Is(err, errInvalid) {
		t.Fatal("invented absence footprint accepted", err)
	}
	if _, _, err := graphEdgeUniqueIdentity(p, graphEdgeUniqueHomeCount(p)); !errors.Is(err, errInvalid) {
		t.Fatal("definition ID accepted as relationship", err)
	}
	legacy := graphCostConfig("graph-cost-v1", graphCostProfile{Version: 1, Facts: 20, CrossPercent: 50, Writers: 1})
	if validGraphEdgeUniqueConfig(legacy) || graphCostLimit(legacy).Transactions != 43 || graphEdgeUniqueEndpoint(c, endpoint{1, old.Target.Node, old.Target.Life + 200}) {
		t.Fatal("fresh endpoint/schema contract leaked into old profile")
	}
	for _, facts := range []uint64{10_000, 100_000, 1_000_000} {
		profile := p
		profile.Facts = facts
		counts, err := graphEdgeUniqueCounts(profile)
		policy, e := graphEdgeUniqueColumnPolicy(profile)
		if err != nil || e != nil || counts.Groups != 3*facts+7 || counts.Versions != 11*facts+7 || counts.Originals != 258*facts+7 || policy.MaxWorkRows != counts.Groups || policy.Version != 1 {
			t.Fatal("complete declared lifecycle not funded", facts, counts, policy, err, e)
		}
	}
	// A complete genuine-envelope shape: close only its old canonical rows.
	// The current new-life claim is deliberately outside this footprint.
	body, err := wire(x)
	if err != nil {
		t.Fatal(err)
	}
	o := outcome{Commit: true, Round: 19, Sequence: 11, Digest: sha256.Sum256(body), ID: x.ID, Request: x.Request, Coordinator: x.Coordinator}
	group := changeGroup{Tx: x, Outcome: o}
	for _, part := range x.Participants {
		for _, effect := range part.Effects {
			before := value{Value: old.Value, Version: 2, Round: 7, TxID: "old-claim"}
			after := value{Value: effect.Value, Version: 3, Round: o.Round, Deleted: effect.Delete, TxID: x.ID}
			group.Writes = append(group.Writes, changedValue{part.Group, effect.Key, &before, after})
		}
	}
	original, err := wire(group)
	if err != nil {
		t.Fatal(err)
	}
	row, err := h.graphColumnRow(original)
	if err != nil || row.Kind != 5 || row.Owner != old.ID || row.TargetLife != old.Target.Life || row.Value != 0 || row.ValueID != 0 {
		t.Fatal("close lost native life/tombstone original", row, err)
	}
	policy, err := graphEdgeUniqueColumnPolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	generation := graphColumnGeneration{Version: 1, ID: "fresh-close", Binding: [32]byte{9}}
	encoded, err := encodeGraphColumnChunk(h, generation, 0, [32]byte{}, []graphColumnRow{row}, policy)
	if err != nil || !bytes.HasPrefix(encoded, []byte("RHOCLM02")) {
		t.Fatal("fresh close chunk", err)
	}
	decoded, err := decodeGraphColumnChunk(h, generation, 0, [32]byte{}, encoded, policy)
	if err != nil || len(decoded) != 1 || !graphColumnSameRow(row, decoded[0]) {
		t.Fatal("close roundtrip", err)
	}
	legacyPolicy := policy
	legacyPolicy.Version = 0
	legacyPolicy.MaxWorkRows = 100
	if _, err := decodeGraphColumnChunk(h, generation, 0, [32]byte{}, encoded, legacyPolicy); !errors.Is(err, errInvalid) {
		t.Fatal("fresh profile accepted legacy policy", err)
	}
	raw, _ := wire(value{Value: 0, Version: 3, Round: 19, Deleted: true, TxID: x.ID})
	physical := graphCostRawRow{Group: 0, Key: h.key(0, "current", edgeKey("edge", closeOld.Edge)), Value: raw}
	if err := h.graphEdgeUniqueValidateRaw(physical, false, 19); err != nil {
		t.Fatal("canonical old tombstone", err)
	}
	physical.Key = h.key(0, "current", graphEdgeUniqueKey(old.Value))
	if err := h.graphEdgeUniqueValidateRaw(physical, false, 19); !errors.Is(err, errInvalid) {
		t.Fatal("phantom deleted unique winner accepted", err)
	}
}
