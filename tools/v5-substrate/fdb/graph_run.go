package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
)

func graphConfig(graph string) config {
	c := defaults()
	c.Graph, c.Nodes = graph, [2]uint64{1, 2}
	return c
}

func scenarioCommand(stage int) graphCommand {
	a, b := endpoint{0, 1, 11}, endpoint{1, 2, 22}
	e := graphEdge{9, a, b, 5}
	switch stage {
	case 1:
		return graphCommand{Kind: "node-create", Node: a}
	case 2:
		return graphCommand{Kind: "node-create", Node: b}
	case 3:
		return graphCommand{Kind: "edge-create", Edge: e}
	case 4:
		e.Value = 6
		return graphCommand{Kind: "edge-update", Edge: e}
	case 5:
		return graphCommand{Kind: "node-close", Node: a}
	case 6:
		e.Value = 0
		return graphCommand{Kind: "edge-close", Edge: e}
	case 7:
		a.Life = 12
		return graphCommand{Kind: "node-create", Node: a}
	default:
		return graphCommand{}
	}
}

// All observations/output occur outside retryable native transaction callbacks.
func graphScenario(h *harness) (cut, []graphObservation, error) {
	initial, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return cut{}, nil, err
	}
	ids := []string{"node-a", "node-b", "edge-create", "edge-update", "node-close", "edge-close", "node-reopen"}
	coords := []uint8{0, 1, 1, 0, 0, 1, 0}
	var observations []graphObservation
	for i, id := range ids {
		start := int64(i*2 + 1)
		before, err := h.current()
		if err != nil {
			return cut{}, nil, err
		}
		x := h.graphTransaction(id, coords[i], scenarioCommand(i+1), before)
		if i > 0 {
			x.Dependency = observations[i-1].Outcome.Round
		}
		o, err := h.submit(x)
		if err != nil {
			return cut{}, nil, err
		}
		if !o.Commit {
			return cut{}, nil, fmt.Errorf("graph operation aborted: %s", o.Reason)
		}
		c, err := h.fresh([]uint8{0, 1}, &o)
		if err != nil {
			return cut{}, nil, err
		}
		after, err := h.at(c, 1, nil)
		if err != nil {
			return cut{}, nil, err
		}
		view, err := h.graphAt(c)
		if err != nil {
			return cut{}, nil, err
		}
		observations = append(observations, graphObservation{start, start + 1, x, o, c.record, before.Values, after.Values, view})
	}
	if err := checkLiteralHistory(h.config.Graph, observations); err != nil {
		return cut{}, nil, err
	}
	// Query the old graph after correction, endpoint closure, edge closure and ABA.
	old, err := h.at(cut{observations[2].Cut}, 1, nil)
	if err != nil {
		return cut{}, nil, err
	}
	os := observationOutcomes(observations)
	view, err := h.graphAt(cut{observations[2].Cut})
	if err != nil {
		return cut{}, nil, err
	}
	if !reflect.DeepEqual(old.Values, literalRows(os[:3])) || !reflect.DeepEqual(view, literalView(h.config.Graph, 3)) {
		return cut{}, nil, fmt.Errorf("old graph was replaced by current state")
	}
	groups, err := h.changes(initial, cut{observations[6].Cut}, 1)
	if err != nil {
		return cut{}, nil, err
	}
	if err = checkLiteralChanges(os, groups); err != nil {
		return cut{}, nil, err
	}
	return initial, observations, nil
}

func observationOutcomes(observations []graphObservation) []outcome {
	var os []outcome
	for _, r := range observations {
		os = append(os, r.Outcome)
	}
	return os
}

func runGraphSmoke(h *harness) error {
	initial, observations, err := graphScenario(h)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Initial      certificate
		Observations []graphObservation
		Scope        string
	}{initial.record, observations, "fixed sequential graph/CDC functional fixture; no general concurrent serializability or fault/cost acceptance"})
}
