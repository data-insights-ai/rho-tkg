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

type faultRecord struct {
	Initial       outcome
	Cut           certificate
	Before, After *outcome
}

func faultConfig() config {
	c := defaults()
	c.Graph = os.Getenv("FDB_FAULT_GRAPH")
	if c.Graph == "" {
		c.Graph = "process-faults"
	}
	return c
}
func faultTx(id string, gen uint64) transaction {
	c := faultConfig()
	return transaction{Graph: c.Graph, Topology: c.Topology, ID: id, Request: "request/" + id, Coordinator: 1, Participants: []participant{
		{Group: 0, Epoch: c.Epochs[0], Generation: gen, Effects: []effect{{Key: id + "/edge", Value: 9}, {Key: id + "/out", Value: 9}}},
		{Group: 1, Epoch: c.Epochs[1], Generation: gen, Effects: []effect{{Key: id + "/in", Value: 9}}},
	}}
}
func faultSave(r faultRecord) error {
	b, e := wire(r)
	if e != nil {
		return e
	}
	return os.WriteFile("/evidence/fault-state.json", b, 0600)
}
func faultLoad() (faultRecord, error) {
	b, e := os.ReadFile("/evidence/fault-state.json")
	if e != nil {
		return faultRecord{}, e
	}
	return decode[faultRecord](b)
}
func faultExpected(os ...outcome) [2]map[string]value {
	rows := [2]map[string]value{{}, {}}
	for _, o := range os {
		for _, p := range faultTx(o.ID, 0).Participants {
			for _, e := range p.Effects {
				rows[p.Group][e.Key] = value{Value: 9, Version: 1, Round: o.Round, TxID: o.ID}
			}
		}
	}
	return rows
}
func faultCheck(h *harness, r faultRecord) error {
	s, e := h.at(cut{r.Cut}, 1, nil)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(s.Values, faultExpected(r.Initial)) {
		return fmt.Errorf("retained acknowledged cut mismatch: %v", s.Values)
	}
	os := []outcome{r.Initial}
	if r.Before != nil {
		os = append(os, *r.Before)
	}
	if r.After != nil {
		os = append(os, *r.After)
	}
	s, e = h.current()
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(s.Values, faultExpected(os...)) {
		return fmt.Errorf("whole fault state mismatch: %v", s.Values)
	}
	return nil
}
func runFault(db fdb.Database, mode string) error {
	h, e := newHarness(db, faultConfig())
	if e != nil {
		return e
	}
	if mode == "fault-seed" {
		o, e := h.submit(faultTx("initial", 0))
		if e != nil {
			return e
		}
		if !o.Commit {
			return errInvalid
		}
		c, e := h.fresh([]uint8{0, 1}, &o)
		if e != nil {
			return e
		}
		r := faultRecord{Initial: o, Cut: c.record}
		if e = faultSave(r); e != nil {
			return e
		}
		if e = faultCheck(h, r); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(o)
	}
	r, e := faultLoad()
	if e != nil {
		return e
	}
	switch mode {
	case "fault-before-child":
		x := faultTx("before", 1)
		b, e := wire(x)
		if e != nil {
			return e
		}
		tr, e := db.CreateTransaction()
		if e != nil {
			return e
		}
		if _, e = h.execute(tr, x, sha256.Sum256(b)); e != nil {
			return e
		}
		// Explicit real fault seam: native Commit has not been invoked. No receipt.
		if e = json.NewEncoder(os.Stdout).Encode(map[string]string{"ready": "before-native-commit"}); e != nil {
			return e
		}
		for {
			time.Sleep(time.Hour)
		}
	case "fault-after-child":
		o, e := h.submit(faultTx("after", 2))
		if e != nil {
			return e
		}
		if !o.Commit {
			return errInvalid
		}
		// Internal native completion precedes the intentionally suppressed API reply.
		if e = json.NewEncoder(os.Stdout).Encode(map[string]string{"ready": "after-native-commit-before-reply"}); e != nil {
			return e
		}
		for {
			time.Sleep(time.Hour)
		}
	case "fault-recover-before":
		if _, e = h.recover(1, "request/before"); !errors.Is(e, errUnknown) {
			return fmt.Errorf("pre-commit killed child had outcome: %v", e)
		}
		if e = faultCheck(h, r); e != nil {
			return e
		}
		o, e := h.submit(faultTx("before", 1))
		if e != nil {
			return e
		}
		if !o.Commit {
			return errInvalid
		}
		r.Before = &o
	case "fault-recover-after":
		// The killed caller has no receipt. Atomic same-key/digest retry MUST recover
		// its original result, despite the now-stale captured generation.
		recovered, e := h.recover(1, "request/after")
		if e != nil {
			return e
		}
		o, e := h.submit(faultTx("after", 2))
		if e != nil {
			return e
		}
		if !o.Commit || o != recovered {
			return errInvalid
		}
		r.After = &o
	case "fault-verify":
		if e = faultCheck(h, r); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"verified": true, "history": r})
	case "fault-progress":
		o, e := h.submit(faultTx("zone-loss", 3))
		if e != nil {
			return e
		}
		if !o.Commit {
			return errInvalid
		}
		// Verify the complete new transaction and retained prior cut under zone loss.
		c, e := h.fresh([]uint8{0, 1}, &o)
		if e != nil {
			return e
		}
		s, e := h.at(c, 1, nil)
		if e != nil {
			return e
		}
		expected := faultExpected(r.Initial, *r.Before, *r.After, o)
		if !reflect.DeepEqual(s.Values, expected) {
			return fmt.Errorf("one-zone-loss mixed state")
		}
		old, e := h.at(cut{r.Cut}, 1, nil)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(old.Values, faultExpected(r.Initial)) {
			return errInvalid
		}
		b, e := wire(o)
		if e != nil {
			return e
		}
		if e = os.WriteFile("/evidence/progress.json", b, 0600); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(o)
	default:
		return fmt.Errorf("unknown fault mode %q", mode)
	}
	if e = faultSave(r); e != nil {
		return e
	}
	if e = faultCheck(h, r); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
