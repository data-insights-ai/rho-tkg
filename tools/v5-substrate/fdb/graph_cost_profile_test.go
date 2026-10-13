package main

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestGraphCostProfileExactRoutingAndBoundedShapes(t *testing.T) {
	t.Run("CLIAndSubmissionRefusal", func(t *testing.T) {
		for _, raw := range []string{"", strings.Repeat(" ", 1025), "{", `{"version":1,"facts":2,"crossPercent":0,"writers":1,"unexpected":true}`, `{"version":1,"facts":2,"crossPercent":0,"writers":1} {}`, `{"version":1,"facts":2,"crossPercent":0,"writers":8}`} {
			t.Setenv("RHO_GRAPH_COST_SPEC", raw)
			if _, err := graphCostSpecFromEnv(); !errors.Is(err, errInvalid) {
				t.Fatal("invalid cost CLI admitted", raw, err)
			}
		}
		t.Setenv("RHO_GRAPH_COST_SPEC", `{"version":1,"facts":2,"crossPercent":100,"writers":1}`)
		if p, err := graphCostSpecFromEnv(); err != nil || p != (graphCostProfile{Version: 1, Facts: 2, CrossPercent: 100, Writers: 1}) {
			t.Fatal("exact cost CLI", p, err)
		}
		var absent *harness
		if _, attempts, err := absent.graphCostSubmit(transaction{}); !errors.Is(err, errInvalid) || attempts != 0 {
			t.Fatal("nil producer admitted native work", attempts, err)
		}
		h := &harness{config: graphCostConfig("invalid-input", graphCostProfile{Version: 1, Facts: 2, Writers: 1})}
		if _, attempts, err := h.graphCostSubmit(transaction{}); !errors.Is(err, errInvalid) || attempts != 0 {
			t.Fatal("invalid producer input admitted native work", attempts, err)
		}
	})
	for _, percent := range []uint64{0, 10, 50, 100} {
		p := graphCostProfile{Version: 1, Facts: 10000, CrossPercent: percent, Writers: 1}
		c := graphCostConfig("cost-route", p)
		if !validGraphConfig(c) || !validLifecycleProfile(c) {
			t.Fatal("cost declarations refused")
		}
		h := &harness{config: c}
		var cross uint64
		for i := uint64(0); i < p.Facts; i++ {
			command := graphCommand{Kind: "edge-create", Edge: p.edge(i, 5)}
			keys := graphReadKeys(command)
			s := snapshot{Values: [2]map[string]value{{}, {}}}
			for g := range keys {
				for _, key := range keys[g] {
					if key != edgeKey("edge", command.Edge) {
						s.Values[g][key] = value{Version: 1}
					}
				}
			}
			x := h.graphTransaction("shape", 0, command, s)
			x.Lifecycle = &lifecycleMetadata{Provenance: 4 + i, EdgeLife: command.Edge.ID + 1, Definition: &lifecycleDefinition{ID: 5, Kind: "i64", I64: 5}}
			if err := h.validate(x); err != nil {
				t.Fatal("actual typed cost shape", i, err)
			}
			if p.cross(i) {
				cross++
				if len(x.Participants) != 2 {
					t.Fatal("cross lost participant")
				}
			} else if len(x.Participants) != 1 || len(x.Participants[0].Effects) != 3 {
				t.Fatal("local posting not co-owned")
			}
		}
		if cross != p.Facts*percent/100 {
			t.Fatal("exact cross count", cross, percent)
		}
	}
	legacy := graphConfig("legacy")
	if legacy.Cost != (graphCostProfile{}) || graphCostLimit(legacy) != (limits{16 << 20, 1024, 65536, 1024}) {
		t.Fatal("legacy admission changed")
	}
	raw, err := wire(legacy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decode[config](raw)
	if err != nil || decoded != legacy {
		t.Fatal("legacy canonical config", err)
	}
	c := graphCostConfig("bad", graphCostProfile{Version: 1, Facts: 2, Writers: 1})
	h := &harness{config: c}
	for _, command := range []graphCommand{{Kind: "edge-create", Edge: graphEdge{ID: 7, Source: endpoint{0, 1, 2}, Target: endpoint{2, 3, 4}, Value: 5}}, {Kind: "node-create", Node: endpoint{1, 3, 4}}} {
		x := h.graphTransaction("bad", 0, command, snapshot{})
		if err := h.validate(x); !errors.Is(err, errInvalid) {
			t.Fatal("malformed owner escaped", err)
		}
	}
}
func TestGraphCostProjectionRemembersOldAndRejectsPartialPosting(t *testing.T) {
	p := graphCostProfile{Version: 1, Facts: 2, CrossPercent: 50, Writers: 1}
	h := &harness{config: graphCostConfig("cost-history", p)}
	old := snapshot{Values: [2]map[string]value{{}, {}}}
	for _, node := range p.nodes() {
		old.Values[node.Owner][lifeKey(node)] = value{Value: 1, Version: 1, Round: 1, TxID: "setup"}
	}
	for i := uint64(0); i < p.Facts; i++ {
		for g, effects := range graphEffects(graphCommand{Kind: "edge-create", Edge: p.edge(i, 5)}) {
			for _, effect := range effects {
				old.Values[g][effect.Key] = value{Value: 5, Version: 1, Round: 2 + i, TxID: "create"}
			}
		}
	}
	current := snapshot{Values: [2]map[string]value{{}, {}}}
	for g, rows := range old.Values {
		for key, v := range rows {
			current.Values[g][key] = v
		}
	}
	for i := uint64(0); i < p.Facts; i++ {
		for g, effects := range graphEffects(graphCommand{Kind: "edge-update", Edge: p.edge(i, 6)}) {
			for _, effect := range effects {
				current.Values[g][effect.Key] = value{Value: 6, Version: 2, Round: 4 + i, TxID: "update"}
			}
		}
	}
	for _, read := range []struct {
		s    snapshot
		want int64
	}{{current, 6}, {old, 5}} {
		view, err := h.projectGraph(read.s)
		if err != nil || graphCostCheckView(view, p, read.want) != nil {
			t.Fatal("exact past/current values", read.want, view, err)
		}
	}
	view, err := h.projectGraph(old)
	if err != nil {
		t.Fatal(err)
	}
	missing := view
	missing.VisibleEdges = missing.VisibleEdges[:1]
	if err := graphCostCheckView(missing, p, 5); !errors.Is(err, errMismatch) {
		t.Fatal("oracle omitted fact", err)
	}
	wrongNode := view
	wrongNode.Nodes = slices.Clone(view.Nodes)
	wrongNode.Nodes[0] = endpoint{Owner: 0, Node: 99, Life: 100}
	if err := graphCostCheckView(wrongNode, p, 5); !errors.Is(err, errMismatch) {
		t.Fatal("oracle substituted setup identity", err)
	}
	wrongEdge := view
	wrongEdge.VisibleEdges = slices.Clone(view.VisibleEdges)
	wrongEdge.VisibleEdges[0].Value = 6
	if err := graphCostCheckView(wrongEdge, p, 5); !errors.Is(err, errMismatch) {
		t.Fatal("old oracle substituted current value", err)
	}
	edge := p.edge(1, 6)
	delete(current.Values[edge.Target.Owner], edgeKey("in", edge))
	if _, err := h.projectGraph(current); !errors.Is(err, errInvalid) {
		t.Fatal("missing actual incoming posting hidden", err)
	}
	// Legacy cross routing remains strict: a target cannot be relabeled local.
	legacy := graphConfig("legacy")
	bad := graphEdge{ID: 9, Source: endpoint{0, 1, 11}, Target: endpoint{0, 1, 22}, Value: 5}
	if _, err := parseGraphEdge("edge", edgeKey("edge", bad), 5, legacy); !errors.Is(err, errInvalid) {
		t.Fatal("legacy source reused as local target", err)
	}
}
