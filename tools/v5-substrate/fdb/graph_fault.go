package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Fault records are client-side observations written outside retry callbacks.
// The launcher supplies an external evidence mount; no source file is a receipt.
type graphFaultRecord struct {
	Initial       certificate
	Seed          []graphObservation
	BeforeTx      transaction
	AfterTx       *transaction
	Before, After *outcome
}

func graphFaultConfig() config {
	graph := os.Getenv("FDB_GRAPH_FAULT_GRAPH")
	if graph == "" {
		graph = "graph-process-faults"
	}
	return graphConfig(graph)
}
func graphFaultSave(r graphFaultRecord) error {
	b, err := wire(r)
	if err != nil {
		return err
	}
	return os.WriteFile("/evidence/graph-fault-state.json", b, 0600)
}
func graphFaultLoad() (graphFaultRecord, error) {
	b, err := os.ReadFile("/evidence/graph-fault-state.json")
	if err != nil {
		return graphFaultRecord{}, err
	}
	return decode[graphFaultRecord](b)
}

func graphFaultCheck(h *harness, r graphFaultRecord) error {
	if len(r.Seed) != 3 {
		return errInvalid
	}
	if err := checkLiteralPrefix(h.config.Graph, r.Seed); err != nil {
		return errors.Join(errInvalid, err)
	}
	oldCut := cut{r.Seed[2].Cut}
	old, err := h.at(oldCut, 1, nil)
	if err != nil {
		return err
	}
	oldView, err := h.graphAt(oldCut)
	if err != nil {
		return err
	}
	os := observationOutcomes(r.Seed)
	if !reflect.DeepEqual(old.Values, literalRows(os)) || !reflect.DeepEqual(oldView, literalView(h.config.Graph, 3)) {
		return fmt.Errorf("graph fault: retained endpoint/edge/posting mismatch")
	}
	expected := literalRows(os)
	view := literalView(h.config.Graph, 3)
	var writes [][]changedValue
	for i := range 3 {
		writes = append(writes, literalWrites(i+1, os))
	}
	for i, o := range []*outcome{r.Before, r.After} {
		if o == nil {
			if i == 0 && r.After != nil {
				return errInvalid
			}
			continue
		}
		n := int64(6 + i)
		id := []string{"graph-before", "graph-after"}[i]
		if !o.Commit || o.ID != id || o.Coordinator != 1 {
			return errInvalid
		}
		prior := expected[0][oracleEdge]
		v := value{n, prior.Version + 1, o.Round, false, id}
		writes = append(writes, []changedValue{{0, oracleEdge, new(prior), v}, {0, oracleOut, new(prior), v}, {1, oracleIn, new(prior), v}})
		expected[0][oracleEdge], expected[0][oracleOut], expected[1][oracleIn] = v, v, v
		for _, edges := range [][]graphEdge{view.IdentityEdges, view.VisibleEdges, view.Outgoing, view.Incoming} {
			edges[0].Value = n
		}
		os = append(os, *o)
	}
	current, err := h.current()
	if err != nil {
		return err
	}
	currentView, err := h.projectGraph(current)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current.Values, expected) || !reflect.DeepEqual(currentView, view) {
		return fmt.Errorf("graph fault: mixed native graph values")
	}
	to, err := h.fresh([]uint8{0, 1}, &os[len(os)-1])
	if err != nil {
		return err
	}
	groups, err := h.changes(cut{r.Initial}, to, 1)
	if err != nil {
		return err
	}
	if len(groups) != len(os) {
		return fmt.Errorf("graph fault: missing/duplicate CDC")
	}
	for i, group := range groups {
		if group.Outcome != os[i] || !reflect.DeepEqual(group.Writes, writes[i]) {
			return fmt.Errorf("graph fault: incomplete CDC revisions")
		}
	}
	return nil
}

func runGraphFault(db fdb.Database, mode string) error {
	h, err := newHarness(db, graphFaultConfig())
	if err != nil {
		return err
	}
	if mode == "graph-fault-seed" {
		initial, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		r := graphFaultRecord{Initial: initial.record}
		for i, id := range []string{"node-a", "node-b", "edge-create"} {
			start := int64(i*2 + 1)
			before, err := h.current()
			if err != nil {
				return err
			}
			coord := uint8(1)
			if i == 0 {
				coord = 0
			}
			x := h.graphTransaction(id, coord, scenarioCommand(i+1), before)
			if i > 0 {
				x.Dependency = r.Seed[i-1].Outcome.Round
			}
			o, err := h.submit(x)
			if err != nil {
				return err
			}
			if !o.Commit {
				return errInvalid
			}
			c, err := h.fresh([]uint8{0, 1}, &o)
			if err != nil {
				return err
			}
			after, err := h.at(c, 1, nil)
			if err != nil {
				return err
			}
			view, err := h.graphAt(c)
			if err != nil {
				return err
			}
			r.Seed = append(r.Seed, graphObservation{start, start + 1, x, o, c.record, before.Values, after.Values, view})
		}
		s, err := h.current()
		if err != nil {
			return err
		}
		r.BeforeTx = h.graphTransaction("graph-before", 1, scenarioCommand(4), s)
		r.BeforeTx.Dependency = r.Seed[2].Outcome.Round
		if err = graphFaultCheck(h, r); err != nil {
			return err
		}
		if err = graphFaultSave(r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	r, err := graphFaultLoad()
	if err != nil {
		return err
	}
	switch mode {
	case "graph-fault-before-child":
		if err = h.validate(r.BeforeTx); err != nil {
			return err
		}
		b, err := wire(r.BeforeTx)
		if err != nil {
			return err
		}
		tr, err := db.CreateTransaction()
		if err != nil {
			return err
		}
		o, err := h.execute(tr, r.BeforeTx, sha256.Sum256(b))
		if err != nil {
			return err
		}
		if !o.Commit {
			return errInvalid
		}
		if err = json.NewEncoder(os.Stdout).Encode(map[string]string{"ready": "before-native-commit"}); err != nil {
			return err
		}
		for {
			time.Sleep(time.Hour)
		}
	case "graph-fault-after-child":
		if r.AfterTx == nil {
			return errInvalid
		}
		o, err := h.submit(*r.AfterTx)
		if err != nil {
			return err
		}
		if !o.Commit {
			return errInvalid
		}
		if err = json.NewEncoder(os.Stdout).Encode(map[string]string{"ready": "after-native-commit-before-reply"}); err != nil {
			return err
		}
		for {
			time.Sleep(time.Hour)
		}
	case "graph-fault-recover-before":
		if _, err = h.recover(1, r.BeforeTx.Request); !errors.Is(err, errUnknown) {
			return fmt.Errorf("uncommitted graph outcome: %v", err)
		}
		if err = graphFaultCheck(h, r); err != nil {
			return err
		}
		o, err := h.submit(r.BeforeTx)
		if err != nil {
			return err
		}
		if !o.Commit {
			return errInvalid
		}
		r.Before = new(o)
		s, err := h.current()
		if err != nil {
			return err
		}
		command := scenarioCommand(4)
		command.Edge.Value = 7
		x := h.graphTransaction("graph-after", 1, command, s)
		x.Dependency = o.Round
		r.AfterTx = new(x)
	case "graph-fault-recover-after":
		if r.AfterTx == nil {
			return errInvalid
		}
		recovered, err := h.recover(1, r.AfterTx.Request)
		if err != nil {
			return err
		}
		o, err := h.submit(*r.AfterTx)
		if err != nil {
			return err
		}
		if !o.Commit || o != recovered {
			return errInvalid
		}
		r.After = new(o)
	case "graph-fault-verify":
		if err = graphFaultCheck(h, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]bool{"graph_history_and_complete_cdc_verified": true})
	default:
		return fmt.Errorf("unknown graph fault mode %q", mode)
	}
	if err = graphFaultCheck(h, r); err != nil {
		return err
	}
	if err = graphFaultSave(r); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
