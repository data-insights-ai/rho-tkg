package main

import (
	"fmt"
	"reflect"
	"slices"
)

// This is a literal seven-operation fixture oracle, independent of key builders,
// projection, validation and native transaction code. It is not a general V2
// real-time/concurrent serializability checker.
const (
	oracleA    = "node/0000000000000001/life/000000000000000b"
	oracleB    = "node/0000000000000002/life/0000000000000016"
	oracleA2   = "node/0000000000000001/life/000000000000000c"
	oracleEdge = "edge/0000000000000009/0000000000000001/000000000000000b/0000000000000002/0000000000000016"
	oracleOut  = "out/0000000000000009/0000000000000001/000000000000000b/0000000000000002/0000000000000016"
	oracleIn   = "in/0000000000000009/0000000000000001/000000000000000b/0000000000000002/0000000000000016"
)

type graphObservation struct {
	// Started/Finished are local synchronous observation sequence numbers,
	// not timestamps, durations or a general concurrent real-time history.
	Started, Finished int64
	Tx                transaction
	Outcome           outcome
	Cut               certificate
	Before, After     [2]map[string]value
	View              graphView
}

func literalRows(os []outcome) [2]map[string]value {
	rows := [2]map[string]value{{}, {}}
	if len(os) >= 1 {
		rows[0][oracleA] = value{1, 1, os[0].Round, false, "node-a"}
	}
	if len(os) >= 2 {
		rows[1][oracleB] = value{1, 1, os[1].Round, false, "node-b"}
	}
	if len(os) >= 3 {
		v := value{5, 1, os[2].Round, false, "edge-create"}
		rows[0][oracleEdge], rows[0][oracleOut], rows[1][oracleIn] = v, v, v
	}
	if len(os) >= 4 {
		v := value{6, 2, os[3].Round, false, "edge-update"}
		rows[0][oracleEdge], rows[0][oracleOut], rows[1][oracleIn] = v, v, v
	}
	if len(os) >= 5 {
		delete(rows[0], oracleA)
	}
	if len(os) >= 6 {
		delete(rows[0], oracleEdge)
		delete(rows[0], oracleOut)
		delete(rows[1], oracleIn)
	}
	if len(os) >= 7 {
		rows[0][oracleA2] = value{1, 1, os[6].Round, false, "node-reopen"}
	}
	return rows
}

func literalView(graph string, n int) graphView {
	v := graphView{Graph: graph}
	if n >= 1 && n < 5 {
		v.Nodes = append(v.Nodes, endpoint{0, 1, 11})
	}
	if n >= 7 {
		v.Nodes = append(v.Nodes, endpoint{0, 1, 12})
	}
	if n >= 2 {
		v.Nodes = append(v.Nodes, endpoint{1, 2, 22})
	}
	if n >= 3 && n < 6 {
		e := graphEdge{9, endpoint{0, 1, 11}, endpoint{1, 2, 22}, 5}
		if n >= 4 {
			e.Value = 6
		}
		v.IdentityEdges = []graphEdge{e}
		if n < 5 {
			v.VisibleEdges, v.Outgoing, v.Incoming = []graphEdge{e}, []graphEdge{e}, []graphEdge{e}
		}
	}
	return v
}

func literalWrites(stage int, os []outcome) []changedValue {
	o := os[stage-1]
	before := literalRows(os[:stage-1])
	type literalWrite struct {
		g       uint8
		key     string
		n       int64
		deleted bool
	}
	var keys []literalWrite
	switch stage {
	case 1:
		keys = append(keys, literalWrite{0, oracleA, 1, false})
	case 2:
		keys = append(keys, literalWrite{1, oracleB, 1, false})
	case 3, 4, 6:
		n, deleted := int64(5), false
		if stage == 4 {
			n = 6
		}
		if stage == 6 {
			n, deleted = 0, true
		}
		keys = append(keys, literalWrite{0, oracleEdge, n, deleted}, literalWrite{0, oracleOut, n, deleted}, literalWrite{1, oracleIn, n, deleted})
	case 5:
		keys = append(keys, literalWrite{0, oracleA, 0, true})
	case 7:
		keys = append(keys, literalWrite{0, oracleA2, 1, false})
	}
	var writes []changedValue
	for _, k := range keys {
		old, exists := before[k.g][k.key]
		var prior *value
		if exists {
			prior = new(old)
		}
		writes = append(writes, changedValue{k.g, k.key, prior, value{k.n, old.Version + 1, o.Round, k.deleted, o.ID}})
	}
	return writes
}

func checkLiteralHistory(graph string, observations []graphObservation) error {
	if len(observations) != 7 {
		return fmt.Errorf("literal history: need seven operations")
	}
	return checkLiteralPrefix(graph, observations)
}

func checkLiteralPrefix(graph string, observations []graphObservation) error {
	if len(observations) < 1 || len(observations) > 7 {
		return fmt.Errorf("literal history: invalid prefix")
	}
	ids := []string{"node-a", "node-b", "edge-create", "edge-update", "node-close", "edge-close", "node-reopen"}
	coords := []uint8{0, 1, 1, 0, 0, 1, 0}
	var os []outcome
	var sequence [2]uint64
	for i, r := range observations {
		dep := uint64(0)
		if i > 0 {
			dep = os[i-1].Round
		}
		if r.Started <= 0 || r.Finished <= r.Started || i > 0 && r.Started < observations[i-1].Finished || !r.Outcome.Commit || r.Outcome.ID != ids[i] || r.Outcome.Request != "request/"+ids[i] || r.Outcome.Coordinator != coords[i] || r.Tx.ID != ids[i] || r.Tx.Request != r.Outcome.Request || r.Tx.Coordinator != coords[i] || r.Tx.Graph != graph || r.Tx.Dependency != dep || r.Outcome.Round <= dep {
			return fmt.Errorf("literal history: ordering/binding at %d", i+1)
		}
		sequence[coords[i]]++
		if r.Outcome.Sequence != sequence[coords[i]] || r.Cut.Groups != sequence || r.Cut.Round != r.Outcome.Round || r.Cut.Graph != graph || !slices.Equal(r.Cut.Scope, []uint8{0, 1}) {
			return fmt.Errorf("literal history: watermark at %d", i+1)
		}
		if !reflect.DeepEqual(r.Before, literalRows(os)) {
			return fmt.Errorf("literal history: observed predecessor values at %d", i+1)
		}
		for _, p := range r.Tx.Participants {
			if p.Group > 1 {
				return fmt.Errorf("literal history: invalid read owner")
			}
			for _, read := range p.Reads {
				if read.Version != r.Before[p.Group][read.Key].Version {
					return fmt.Errorf("literal history: read revision at %d", i+1)
				}
			}
		}
		os = append(os, r.Outcome)
		if !reflect.DeepEqual(r.After, literalRows(os)) || !reflect.DeepEqual(r.View, literalView(graph, i+1)) {
			return fmt.Errorf("literal history: observed graph/read values at %d", i+1)
		}
	}
	return nil
}

func checkLiteralChanges(os []outcome, groups []changeGroup) error {
	if len(os) != 7 || len(groups) != 7 {
		return fmt.Errorf("literal feed: incomplete or duplicate group")
	}
	for i, group := range groups {
		if group.Outcome != os[i] || !reflect.DeepEqual(group.Writes, literalWrites(i+1, os)) {
			return fmt.Errorf("literal feed: complete revisions at %d", i+1)
		}
	}
	return nil
}
