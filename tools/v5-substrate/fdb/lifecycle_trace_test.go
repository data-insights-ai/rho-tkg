package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestLifecycleProfilePreservesLegacyEncodingAndStructuralEquality(t *testing.T) {
	c := defaults()
	b, err := wire(c)
	want := `{"Format":2,"Graph":"fixture","Topology":1,"Epochs":[7,11],"Limits":{"Bytes":4194304,"Transactions":256,"Versions":4096,"Certificates":256}}`
	if err != nil || string(b) != want {
		t.Fatal("legacy config encoding changed", string(b), err)
	}
	c = lifecycleConfig("profile")
	b, err = wire(c)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decode[config](b)
	if err != nil || decoded != c || !validLifecycleProfile(decoded) {
		t.Fatal("decoded profile uses address equality", decoded, err)
	}
	for _, mutate := range []func(*config){
		func(c *config) { c.Lifecycle.ReopenedLife = c.Lifecycle.Values[1].ID },
		func(c *config) { c.Lifecycle.EdgeLife = c.Nodes[0] },
		func(c *config) { c.Lifecycle.Scope.AxisProfile = "Z" },
		func(c *config) { c.Lifecycle.Scope.Reference = "unknown" },
		func(c *config) { c.Lifecycle.Values[0].Kind = "text" },
		func(c *config) { c.Lifecycle.Version = 2 },
	} {
		bad := c
		mutate(&bad)
		if validLifecycleProfile(bad) {
			t.Fatal("invalid or aliased profile accepted", bad)
		}
	}
	legacy := defaults()
	x := tx(legacy, "legacy", 0, part(legacy, 0, 0, effect{Key: "key", Value: 1}))
	b, err = wire(x)
	if err != nil || bytes.Contains(b, []byte("Lifecycle")) {
		t.Fatal("legacy transaction encoding changed", string(b), err)
	}
	x.Lifecycle = &lifecycleMetadata{Provenance: 1}
	if err := (&harness{config: legacy}).validate(x); !errors.Is(err, errInvalid) {
		t.Fatal("metadata admitted on legacy profile", err)
	}
}

func TestNativeLifecycleTraceRetainsActualHistoryAndRejectsMissingPhysicalEffects(t *testing.T) {
	c := lifecycleConfig(t.Name())
	h := fixture(t, c)
	output, err := os.CreateTemp(t.TempDir(), "trace")
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	defer func() { os.Stdout = previous }()
	os.Stdout = output
	previousArgs := os.Args
	defer func() { os.Args = previousArgs }()
	os.Args = []string{"fdb-spike", "lifecycle-trace"}
	t.Setenv("FDB_LIFECYCLE_GRAPH", c.Graph)
	err = run()
	os.Args = previousArgs
	os.Stdout = previous
	if closeErr := output.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	var trace fdbLifecycleTrace
	if err = json.Unmarshal(raw, &trace); err != nil {
		t.Fatal(err)
	}
	if len(trace.Steps) != 7 || len(trace.OldReads) != 4 {
		t.Fatal("incomplete trace", len(trace.Steps), len(trace.OldReads))
	}
	final := trace.Steps[6]
	edge := final.After.Entities[2]
	if edge.ID != "edge" || edge.Presence.Present || edge.Effective || !edge.Property.Present || edge.Property.I64 != 6 || edge.Property.Reference != "value6" || edge.Property.RevisionOperation != "edge-update" || edge.Property.Provenance != 4 {
		t.Fatal("closed bundled row erased retained property/identity", edge)
	}
	if len(final.After.Lives) != 4 || !reflect.DeepEqual(final.After.CensusIDs, []string{"edge", "node-a", "node-b"}) {
		t.Fatal("declarations omitted", final.After)
	}
	held := trace.OldReads[0]
	if held.View.ObservationScope != "edge-only" || len(held.View.Entities) != 1 || len(held.View.Lives) != 1 || held.View.Entities[0].Property.I64 != 5 || held.View.Lives[0].SourceLife != "life-a1" || held.View.Lives[0].TargetLife != "life-b1" {
		t.Fatal("held old edge/life substituted current", held.View)
	}
	changes := 0
	writes := 0
	for _, s := range trace.Steps {
		changes += len(s.LogicalCDC)
	}
	for _, g := range final.RawObservation.Groups {
		writes += len(g.Writes)
	}
	if changes != 8 || writes != 13 {
		t.Fatal("logical changes hide duplicate physical writes", changes, writes)
	}
	if trace.Steps[5].LogicalCDC[0].Before.RevisionOperation != "edge-create" || trace.Steps[6].LogicalCDC[0].Before.RevisionOperation != "node-close" {
		t.Fatal("physical row revisions replaced component provenance")
	}
	for _, old := range trace.OldReads[1:] {
		for _, s := range trace.Steps {
			if old.Operation == s.Operation && !reflect.DeepEqual(old.View, s.After) {
				t.Fatal("old cut changed", old.Operation)
			}
		}
	}
	// Empty profile declarations do not create observed dictionary membership.
	empty, steps, _, err := h.lifecycleFold(nil, nil)
	if err != nil || len(empty.Entities) != 0 || len(steps) != 0 {
		t.Fatal(empty, steps, err)
	}
	groups := final.RawObservation.Groups
	history := final.RawObservation.History
	for _, mutate := range []func([]changeGroup){
		func(gs []changeGroup) { gs[2].Writes = gs[2].Writes[:2] },
		func(gs []changeGroup) { gs[3].Writes[1].After.Value++ },
		func(gs []changeGroup) { gs[5].Writes[2].After.Deleted = false },
		func(gs []changeGroup) { gs[3].Tx.Lifecycle.Provenance++ },
		func(gs []changeGroup) { gs[3].Tx.Lifecycle.Definition.ID = 5 },
	} {
		bad := cloneDTO(t, groups)
		mutate(bad)
		if _, _, _, err = h.lifecycleFold(history, bad); !errors.Is(err, errInvalid) {
			t.Fatal("corrupt complete group/posting/metadata accepted", err)
		}
	}
	if _, _, _, err = h.lifecycleFold(history[:len(history)-1], groups); !errors.Is(err, errUnavailable) {
		t.Fatal("missing history silently projected", err)
	}
	changed := cloneDTO(t, trace.FinalRawCurrent)
	changed[0].Value = []byte("corrupt")
	if err = h.lifecycleValidateCurrent(changed, groups); !errors.Is(err, errUnavailable) {
		t.Fatal("unvalidated current census", err)
	}
	if err = h.lifecycleValidateCurrent(trace.FinalRawCurrent[:len(trace.FinalRawCurrent)-1], groups); !errors.Is(err, errUnavailable) {
		t.Fatal("partial current census", err)
	}
	// Immutable reference payloads are admitted before commit, not inferred from
	// the step index or invented by the observation converter.
	current, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	x := h.graphTransaction("invalid-reuse", 0, scenarioCommand(4), current)
	x.Lifecycle = &lifecycleMetadata{Provenance: 91, EdgeLife: 4, Definition: new(lifecycleDefinition{5, "i64", 99})}
	x.GraphChange.Edge.Value = 99
	for i := range x.Participants {
		for j := range x.Participants[i].Effects {
			x.Participants[i].Effects[j].Value = 99
		}
	}
	if _, err = h.submit(x); !errors.Is(err, errInvalid) {
		t.Fatal("value reference rebound", err)
	}
	after, err := h.current()
	if err != nil || !reflect.DeepEqual(after, current) {
		t.Fatal("refused metadata changed rows", after, err)
	}
	x.Lifecycle.Definition = new(c.Lifecycle.Values[1])
	x.Lifecycle.Provenance = 0
	if _, err = h.submit(x); !errors.Is(err, errInvalid) {
		t.Fatal("missing caller provenance manufactured", err)
	}
	oldConfig := c
	oldConfig.Lifecycle = lifecycleProfile{}
	if _, err = newHarness(h.db, oldConfig); !errors.Is(err, errStale) {
		t.Fatal("profile silently reused old namespace", err)
	}
}
