package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const pairedFaultFixture = "graph-fault-local-v1"

var pairedOperationNames = [6]string{"node-a", "node-b", "edge-create", "precommit-update", "postcommit-update", "zone-loss-update"}

// These are frozen caller inputs, not reconstructed descriptors after a crash.
// BeforeCut is an observation fence, distinct from the transaction dependency.
type pairedOriginal struct {
	Operation string
	Bytes     []byte
	BeforeCut certificate
	Before    traceView
	BeforeRaw traceRawObservation
}
type pairedFaultObservation struct {
	Operation, Observation string
	Cut                    certificate
	Rows                   []traceKV
	Raw                    traceRawObservation
	View                   traceView
	RawCurrent             []traceKV
	Outcome                *outcome `json:",omitempty"`
	ChangedPayload         []byte   `json:",omitempty"`
}
type pairedFaultRecord struct {
	Initial      certificate
	Originals    []pairedOriginal
	Steps        []traceStep
	Observations []pairedFaultObservation
	Faults       []pairedFault
	Held         *traceOldRead `json:",omitempty"`
}
type pairedFaultTrace struct {
	fdbLifecycleTrace
	Steps             []pairedTraceStep        `json:"steps"`
	Originals         []pairedOriginal         `json:"originals"`
	FaultObservations []pairedFaultObservation `json:"faultObservations"`
	Faults            []pairedFault            `json:"faults"`
}
type pairedTraceStep struct {
	traceStep
	OriginalRequest pairedRequest
}
type pairedObservation struct {
	Cut            certificate
	View           traceView
	RawObservation traceRawObservation
	RawRecords     []traceKV
	RawCurrent     []traceKV
}
type pairedRequest struct {
	Coordinator                 uint8
	Graph, Request, Transaction string
	Payload                     []byte
}
type pairedRetry struct {
	Operation     string `json:",omitempty"`
	EventID       int    `json:",omitzero"`
	Payload       []byte
	Result        traceOperation
	RawResult     []byte
	Before, After pairedObservation
	NewRawRecords []traceKV
}
type pairedChanged struct {
	Payload        []byte
	Classification string
	Raw            []byte
	Before, After  pairedObservation
	NewRawRecords  []traceKV
}
type pairedIntermediate struct {
	Classification string
	Raw            []byte
	pairedObservation
}
type pairedFault struct {
	Case, Operation, Marker, CallerClassification string
	MarkerRaw                                     []byte
	CallerRaw                                     []byte
	OriginalRequest                               pairedRequest
	Observer                                      pairedObservation
	Intermediate                                  *pairedIntermediate `json:",omitempty"`
	Retry                                         *pairedRetry        `json:",omitempty"`
	ChangedPayload                                *pairedChanged      `json:",omitempty"`
	ReopenRetries                                 []pairedRetry       `json:",omitempty"`
	// Added from the controller's actual process observations after backend export.
	ProcessEvidence []any
}

func pairedConfig() config {
	graph := os.Getenv("FDB_PAIRED_GRAPH")
	if graph == "" {
		graph = "graph-paired-local-faults"
	}
	return lifecycleConfig(graph)
}
func pairedRecordPath() string {
	if path := os.Getenv("FDB_PAIRED_RECORD"); path != "" {
		return path
	}
	return "/evidence/paired-graph-fault-state.json"
}
func pairedSave(r pairedFaultRecord) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > 2<<20 {
		return errLimit
	}
	path := pairedRecordPath()
	if err = os.WriteFile(path+".tmp", b, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
func pairedLoad() (pairedFaultRecord, error) {
	var r pairedFaultRecord
	b, err := os.ReadFile(pairedRecordPath())
	if err != nil {
		return r, err
	}
	if len(b) == 0 || len(b) > 2<<20 {
		return r, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, errInvalid
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(b, canonical) {
		return r, errInvalid
	}
	return r, nil
}
func pairedCommand(operation int) graphCommand {
	if operation <= 3 {
		return scenarioCommand(operation)
	}
	if operation < 4 || operation > 6 {
		return graphCommand{}
	}
	c := scenarioCommand(4)
	if operation == 5 {
		c.Edge.Value = 5
	}
	return c
}
func (h *harness) pairedCapture(r *pairedFaultRecord, operation int) error {
	if operation < 1 || operation > 6 || len(r.Originals) != operation-1 || len(r.Steps) != operation-1 {
		return errInvalid
	}
	c, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	raw, before, _, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return err
	}
	s, err := h.current()
	if err != nil {
		return err
	}
	coord := uint8(1)
	if operation == 1 {
		coord = 0
	}
	command := pairedCommand(operation)
	x := h.graphTransaction(pairedOperationNames[operation-1], coord, command, s)
	if operation > 1 {
		x.Dependency = r.Steps[operation-2].Cut.Round
	}
	x.Lifecycle = &lifecycleMetadata{Provenance: uint64(operation)}
	if operation >= 3 {
		x.Lifecycle.EdgeLife = h.config.Lifecycle.EdgeLife
		definition := h.config.Lifecycle.Values[0]
		if command.Edge.Value == 6 {
			definition = h.config.Lifecycle.Values[1]
		}
		x.Lifecycle.Definition = new(definition)
	}
	if err = h.validate(x); err != nil {
		return err
	}
	b, err := wire(x)
	if err != nil {
		return err
	}
	r.Originals = append(r.Originals, pairedOriginal{x.ID, b, c.record, before, raw})
	return nil
}
func (h *harness) pairedOriginal(r pairedFaultRecord, operation int) (transaction, error) {
	if operation < 1 || operation > 6 || len(r.Originals) < operation {
		return transaction{}, errInvalid
	}
	original := r.Originals[operation-1]
	x, err := decode[transaction](original.Bytes)
	if err != nil {
		return x, err
	}
	coord := uint8(1)
	if operation == 1 {
		coord = 0
	}
	if original.Operation != pairedOperationNames[operation-1] || x.ID != original.Operation || x.Request != "request/"+x.ID || x.Coordinator != coord || x.GraphChange == nil || *x.GraphChange != pairedCommand(operation) || x.Lifecycle == nil || x.Lifecycle.Provenance != uint64(operation) {
		return x, errInvalid
	}
	if operation > 1 && (len(r.Steps) < operation-1 || x.Dependency != r.Steps[operation-2].Cut.Round) {
		return x, errInvalid
	}
	return x, h.validate(x)
}
func (h *harness) pairedAppend(r *pairedFaultRecord, operation int, o outcome) error {
	x, err := h.pairedOriginal(*r, operation)
	if err != nil {
		return err
	}
	if !o.Commit || o.ID != x.ID || o.Request != x.Request || o.Digest != sha256.Sum256(r.Originals[operation-1].Bytes) {
		return errInvalid
	}
	if len(r.Steps) == operation {
		if len(r.Steps[operation-1].RawObservation.Groups) != operation || r.Steps[operation-1].RawObservation.Groups[operation-1].Outcome != o {
			return errInvalid
		}
		return nil // Same original request replay never changes the captured cut.
	}
	if len(r.Steps) != operation-1 {
		return errInvalid
	}
	c, err := h.fresh([]uint8{0, 1}, &o)
	if err != nil {
		return err
	}
	raw, after, folded, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return err
	}
	if len(folded) != operation || len(raw.Groups) != operation || !reflect.DeepEqual(raw.Groups[operation-1].Tx, x) || raw.Groups[operation-1].Outcome != o {
		return errUnavailable
	}
	step := folded[operation-1]
	step.Before, step.After, step.Cut, step.RawObservation = r.Originals[operation-1].Before, after, c.record, raw
	step.RawRecords, err = h.lifecycleRecordRows(c, raw.Groups[operation-1])
	if err != nil {
		return err
	}
	r.Steps = append(r.Steps, step)
	return nil
}
func (h *harness) pairedCheckHistory(r pairedFaultRecord) error {
	if len(r.Steps) < 3 || len(r.Steps) > 6 || len(r.Originals) < len(r.Steps) || len(r.Originals) > min(6, len(r.Steps)+1) {
		return errInvalid
	}
	for i, original := range r.Originals {
		if _, err := h.pairedOriginal(r, i+1); err != nil {
			return err
		}
		raw, view, _, err := h.lifecycleObserve(cut{r.Initial}, cut{original.BeforeCut}, false)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(raw, original.BeforeRaw) || !reflect.DeepEqual(view, original.Before) {
			return errInvalid
		}
	}
	for i, step := range r.Steps {
		raw, view, folded, err := h.lifecycleObserve(cut{r.Initial}, cut{step.Cut}, false)
		if err != nil {
			return err
		}
		if len(folded) != i+1 || !reflect.DeepEqual(raw, step.RawObservation) || !reflect.DeepEqual(view, step.After) || !reflect.DeepEqual(step.Before, r.Originals[i].Before) {
			return errInvalid
		}
		canonical := folded[i]
		canonical.Before, canonical.After, canonical.Cut, canonical.RawObservation = step.Before, view, step.Cut, raw
		canonical.RawRecords, err = h.lifecycleRecordRows(cut{step.Cut}, raw.Groups[i])
		if err != nil {
			return err
		}
		stored, err := wire(raw.Groups[i].Tx)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(canonical, step) || !bytes.Equal(r.Originals[i].Bytes, stored) {
			return errInvalid
		}
	}
	return nil
}
func (h *harness) pairedCheck(r pairedFaultRecord) error {
	if err := h.pairedCheckHistory(r); err != nil {
		return err
	}
	c, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	raw, view, _, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return err
	}
	if len(raw.Groups) != len(r.Steps) || !reflect.DeepEqual(view, r.Steps[len(r.Steps)-1].After) {
		return errInvalid
	}
	current, err := h.lifecycleRawRows(c, "current")
	if err != nil {
		return err
	}
	return h.lifecycleValidateCurrent(current, raw.Groups)
}
func (h *harness) pairedObserve(r pairedFaultRecord, operation int) (pairedObservation, error) {
	var observation pairedObservation
	if operation < 1 || len(r.Steps) < operation || len(r.Steps[operation-1].RawObservation.Groups) != operation {
		return observation, errInvalid
	}
	o := r.Steps[operation-1].RawObservation.Groups[operation-1].Outcome
	c, err := h.fresh([]uint8{0, 1}, &o)
	if err != nil {
		return observation, err
	}
	raw, view, _, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return observation, err
	}
	if len(raw.Groups) != operation || !reflect.DeepEqual(view, r.Steps[operation-1].After) {
		return observation, errInvalid
	}
	rows, err := h.lifecycleRecordRows(c, raw.Groups[operation-1])
	if err != nil {
		return observation, err
	}
	current, err := h.lifecycleRawRows(c, "current")
	if err != nil {
		return observation, err
	}
	if err = h.lifecycleValidateCurrent(current, raw.Groups); err != nil {
		return observation, err
	}
	return pairedObservation{c.record, view, raw, rows, current}, nil
}
func (h *harness) pairedFaultResult(r *pairedFaultRecord, operation int, name, marker, classification string, retry bool) error {
	x, err := h.pairedOriginal(*r, operation)
	if err != nil {
		return err
	}
	observer, err := h.pairedObserve(*r, operation)
	if err != nil {
		return err
	}
	f := pairedFault{Case: name, Operation: x.ID, Marker: marker, CallerClassification: classification, OriginalRequest: pairedRequest{x.Coordinator, x.Graph, x.Request, x.ID, bytes.Clone(r.Originals[operation-1].Bytes)}, Observer: observer}
	if retry {
		o, err := h.submit(x)
		if err != nil {
			return err
		}
		expected := r.Steps[operation-1].RawObservation.Groups[operation-1].Outcome
		if o != expected {
			return errInvalid
		}
		after, err := h.pairedObserve(*r, operation)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(observer.RawObservation.Groups, after.RawObservation.Groups) || !reflect.DeepEqual(observer.RawRecords, after.RawRecords) || !reflect.DeepEqual(observer.RawCurrent, after.RawCurrent) {
			return errInvalid
		}
		resultBytes, err := wire(o)
		if err != nil {
			return err
		}
		f.Retry = &pairedRetry{Payload: bytes.Clone(f.OriginalRequest.Payload), Result: traceOperation{o.ID, o.Coordinator, o.Request, o.ID, o.Round}, RawResult: resultBytes, Before: observer, After: after, NewRawRecords: []traceKV{}}
	}
	if operation == 4 {
		if len(r.Observations) == 0 {
			return errInvalid
		}
		notFound := r.Observations[len(r.Observations)-1]
		b, err := wire(notFound.Rows)
		if err != nil {
			return err
		}
		f.Intermediate = &pairedIntermediate{Classification: "old-complete", Raw: b, pairedObservation: pairedObservation{Cut: notFound.Cut, View: notFound.View, RawObservation: notFound.Raw, RawRecords: notFound.Rows, RawCurrent: notFound.RawCurrent}}
	}
	r.Faults = append(r.Faults, f)
	return nil
}

// Not-found is observed in a native read transaction that validates this exact
// issued cut. It is not a terminal abort or a claim about the caller's future.
func (h *harness) pairedNotFound(r *pairedFaultRecord) error {
	x, err := h.pairedOriginal(*r, 4)
	if err != nil {
		return err
	}
	c, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	got, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if err := h.verifyCut(tr, c); err != nil {
			return nil, err
		}
		if err := h.checkConfig(tr, x.Coordinator); err != nil {
			return nil, err
		}
		var rows []traceKV
		for _, key := range []fdb.Key{h.key(x.Coordinator, "request", x.Request), h.key(x.Coordinator, "id", x.ID)} {
			b, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			if len(b) != 0 {
				return nil, errInvalid
			}
			rows = append(rows, traceKV{x.Coordinator, bytes.Clone(key), bytes.Clone(b)})
		}
		return rows, nil
	})
	if err != nil {
		return err
	}
	raw, view, _, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return err
	}
	if len(raw.Groups) != 3 || !reflect.DeepEqual(view, r.Steps[2].After) {
		return errInvalid
	}
	current, err := h.lifecycleRawRows(c, "current")
	if err != nil {
		return err
	}
	if err = h.lifecycleValidateCurrent(current, raw.Groups); err != nil {
		return err
	}
	r.Observations = append(r.Observations, pairedFaultObservation{Operation: x.ID, Observation: "not-found-at-observed-cut; original caller unknown", Cut: c.record, Rows: got.([]traceKV), Raw: raw, View: view, RawCurrent: current})
	return nil
}
func (h *harness) pairedMismatch(r *pairedFaultRecord) error {
	x, err := h.pairedOriginal(*r, 5)
	if err != nil {
		return err
	}
	// Valid admitted shape and typed definition, same request/ID, new payload.
	x.GraphChange.Edge.Value = 6
	x.Lifecycle.Definition = new(h.config.Lifecycle.Values[1])
	for i := range x.Participants {
		for j := range x.Participants[i].Effects {
			x.Participants[i].Effects[j].Value = 6
		}
	}
	if err = h.validate(x); err != nil {
		return err
	}
	b, err := wire(x)
	if err != nil {
		return err
	}
	beforeObservation, err := h.pairedObserve(*r, 5)
	if err != nil {
		return err
	}
	before, err := h.current()
	if err != nil {
		return err
	}
	_, refusal := h.submit(x)
	if !errors.Is(refusal, errMismatch) {
		return errors.Join(errInvalid, fmt.Errorf("valid changed original payload: %w", refusal))
	}
	after, err := h.current()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before, after) {
		return errInvalid
	}
	c, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	raw, view, _, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return err
	}
	if len(raw.Groups) != 5 || !reflect.DeepEqual(view, r.Steps[4].After) {
		return errInvalid
	}
	afterObservation, err := h.pairedObserve(*r, 5)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(beforeObservation.RawObservation.Groups, afterObservation.RawObservation.Groups) || !reflect.DeepEqual(beforeObservation.RawRecords, afterObservation.RawRecords) || !reflect.DeepEqual(beforeObservation.RawCurrent, afterObservation.RawCurrent) || len(r.Faults) != 2 {
		return errInvalid
	}
	r.Faults[1].ChangedPayload = &pairedChanged{b, "mismatch", []byte(refusal.Error()), beforeObservation, afterObservation, []traceKV{}}
	r.Observations = append(r.Observations, pairedFaultObservation{Operation: x.ID, Observation: "valid-different-payload-refused-immutable-mismatch", Cut: c.record, Raw: raw, View: view, ChangedPayload: b})
	return nil
}

func (h *harness) pairedReopenRetries(r *pairedFaultRecord) error {
	if len(r.Steps) != 6 || len(r.Faults) != 4 {
		return errInvalid
	}
	for operation := 1; operation <= 6; operation++ {
		x, err := h.pairedOriginal(*r, operation)
		if err != nil {
			return err
		}
		before, err := h.pairedObserve(*r, 6)
		if err != nil {
			return err
		}
		recovered, err := h.recover(x.Coordinator, x.Request)
		if err != nil {
			return err
		}
		o, err := h.submit(x)
		if err != nil {
			return err
		}
		if o != recovered || o != r.Steps[operation-1].RawObservation.Groups[operation-1].Outcome {
			return errInvalid
		}
		after, err := h.pairedObserve(*r, 6)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(before.RawObservation.Groups, after.RawObservation.Groups) || !reflect.DeepEqual(before.RawRecords, after.RawRecords) || !reflect.DeepEqual(before.RawCurrent, after.RawCurrent) {
			return errInvalid
		}
		resultBytes, err := wire(struct{ Recovered, Retried outcome }{recovered, o})
		if err != nil {
			return err
		}
		r.Faults[3].ReopenRetries = append(r.Faults[3].ReopenRetries, pairedRetry{Operation: x.ID, Payload: bytes.Clone(r.Originals[operation-1].Bytes), Result: traceOperation{o.ID, o.Coordinator, o.Request, o.ID, o.Round}, RawResult: resultBytes, Before: before, After: after, NewRawRecords: []traceKV{}})
	}
	return h.pairedCheck(*r)
}

// Written by the actual child after Commit and before its marker. The
// controller preserves these bytes before killing the child, independently of
// the later observer/retry that populates the complete paired trace.
type pairedCommittedMarker struct {
	Outcome  outcome
	Original []byte
	Cut      certificate
	Records  []traceKV
}

func (h *harness) pairedCommitMarker(r pairedFaultRecord, o outcome) (string, error) {
	c, err := h.fresh([]uint8{0, 1}, &o)
	if err != nil {
		return "", err
	}
	groups, err := h.changes(cut{r.Initial}, c, 1)
	if err != nil {
		return "", err
	}
	if len(groups) != 5 || groups[4].Outcome != o {
		return "", errInvalid
	}
	records, err := h.lifecycleRecordRows(c, groups[4])
	if err != nil {
		return "", err
	}
	b, err := wire(pairedCommittedMarker{o, bytes.Clone(r.Originals[4].Bytes), c.record, records})
	if err != nil {
		return "", err
	}
	if err = os.WriteFile(pairedRecordPath()+".after-committed", b, 0600); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}

func runPairedGraphFault(db fdb.Database, mode string) error {
	h, err := newHarness(db, pairedConfig())
	if err != nil {
		return err
	}
	var r pairedFaultRecord
	if mode == "paired-fault-seed" {
		initial, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		r.Initial = initial.record
		for operation := 1; operation <= 3; operation++ {
			if err = h.pairedCapture(&r, operation); err != nil {
				return err
			}
			x, err := h.pairedOriginal(r, operation)
			if err != nil {
				return err
			}
			o, err := h.submit(x)
			if err != nil {
				return err
			}
			if err = h.pairedAppend(&r, operation, o); err != nil {
				return err
			}
		}
		if err = h.pairedCapture(&r, 4); err != nil {
			return err
		}
	} else {
		r, err = pairedLoad()
		if err != nil {
			return err
		}
		// Recover-after legitimately has one committed group beyond the last
		// captured step. Validate retained original observations before indexing;
		// the full current/group check follows after resolving that outcome.
		if err = h.pairedCheckHistory(r); err != nil {
			return err
		}
		switch mode {
		case "paired-fault-before-child", "paired-fault-after-child":
			operation := 4
			if mode == "paired-fault-after-child" {
				operation = 5
			}
			x, err := h.pairedOriginal(r, operation)
			if err != nil {
				return err
			}
			var o outcome
			marker := "after-native-commit-before-reply"
			markerRecord := map[string]string{}
			if operation == 4 {
				tr, err := db.CreateTransaction()
				if err != nil {
					return err
				}
				defer tr.Cancel()
				o, err = h.execute(tr, x, sha256.Sum256(r.Originals[3].Bytes))
				if err != nil {
					return err
				}
				marker = "before-native-commit"
			} else {
				o, err = h.submit(x)
				if err != nil {
					return err
				}
				if !o.Commit {
					return errInvalid
				}
				digest, err := h.pairedCommitMarker(r, o)
				if err != nil {
					return err
				}
				markerRecord["receipt_sha256"] = digest
			}
			if !o.Commit {
				return errInvalid
			}
			markerRecord["ready"] = marker
			if err = json.NewEncoder(os.Stdout).Encode(markerRecord); err != nil {
				return err
			}
			for {
				time.Sleep(time.Hour)
			}
		case "paired-fault-recover-before":
			if len(r.Steps) != 3 {
				return errInvalid
			}
			if err = h.pairedCheck(r); err != nil {
				return err
			}
			if err = h.pairedNotFound(&r); err != nil {
				return err
			}
			x, err := h.pairedOriginal(r, 4)
			if err != nil {
				return err
			}
			if _, err = h.recover(1, x.Request); !errors.Is(err, errUnknown) {
				return errors.Join(errInvalid, err)
			}
			o, err := h.submit(x)
			if err != nil {
				return err
			}
			if err = h.pairedAppend(&r, 4, o); err != nil {
				return err
			}
			if err = h.pairedFaultResult(&r, 4, "precommit-uncertainty", "before-native-commit", "unknown", true); err != nil {
				return err
			}
			if err = h.pairedCapture(&r, 5); err != nil {
				return err
			}
		case "paired-fault-recover-after":
			if len(r.Steps) != 4 {
				return errInvalid
			}
			x, err := h.pairedOriginal(r, 5)
			if err != nil {
				return err
			}
			recovered, err := h.recover(1, x.Request)
			if err != nil {
				return err
			}
			o, err := h.submit(x)
			if err != nil {
				return err
			}
			if o != recovered {
				return errInvalid
			}
			if err = h.pairedAppend(&r, 5, o); err != nil {
				return err
			}
			if err = h.pairedFaultResult(&r, 5, "lost-committed-reply", "after-native-commit-before-reply", "unknown", true); err != nil {
				return err
			}
			r.Observations = append(r.Observations, pairedFaultObservation{Operation: x.ID, Observation: "original-committed-recovery-and-identical-retry", Cut: r.Steps[4].Cut, Outcome: new(o)})
			if err = h.pairedMismatch(&r); err != nil {
				return err
			}
		case "paired-fault-prepare-progress":
			if len(r.Steps) != 5 {
				return errInvalid
			}
			if len(r.Originals) == 5 {
				if err = h.pairedCapture(&r, 6); err != nil {
					return err
				}
			}
		case "paired-fault-progress":
			if len(r.Steps) < 5 {
				return errInvalid
			}
			x, err := h.pairedOriginal(r, 6)
			if err != nil {
				return err
			}
			o, err := h.submit(x)
			if err != nil {
				return err
			}
			if err = h.pairedAppend(&r, 6, o); err != nil {
				return err
			}
			if len(r.Faults) == 2 {
				if err = h.pairedFaultResult(&r, 6, "quorum-preserving-failure", "zone0-stopped-before-progress-commit", "committed", false); err != nil {
					return err
				}
			}
		case "paired-fault-hold-old":
			if len(r.Steps) != 5 || r.Held != nil {
				return errInvalid
			}
			raw, held, _, err := h.lifecycleObserve(cut{r.Initial}, cut{r.Steps[2].Cut}, true)
			if err != nil {
				return err
			}
			r.Held = &traceOldRead{"edge-create", "held-old", r.Steps[2].Cut, held, raw}
		case "paired-fault-reopened":
			if len(r.Steps) != 6 || r.Held == nil || len(r.Faults) != 3 {
				return errInvalid
			}
			if err = h.pairedFaultResult(&r, 6, "same-volume-reopen", "all-four-stopped-before-any-original-volume-restart", "observer-only", false); err != nil {
				return err
			}
			if err = h.pairedReopenRetries(&r); err != nil {
				return err
			}
		case "paired-fault-verify":
			return h.pairedVerifyOutput(r)
		case "paired-fault-terminal":
			if len(r.Steps) != 6 {
				return errInvalid
			}
			return h.pairedTerminal(r)
		default:
			return errInvalid
		}
	}
	if err = h.pairedCheck(r); err != nil {
		return err
	}
	if err = pairedSave(r); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
func (h *harness) pairedVerifyOutput(r pairedFaultRecord) error {
	if err := h.pairedCheck(r); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"verified": true, "completeGroups": len(r.Steps)})
}
func (h *harness) pairedTerminal(r pairedFaultRecord) error {
	if err := h.pairedCheck(r); err != nil {
		return err
	}
	if len(r.Faults) != 4 || len(r.Faults[3].ReopenRetries) != 6 || r.Held == nil {
		return errInvalid
	}
	t := pairedFaultTrace{fdbLifecycleTrace: fdbLifecycleTrace{SchemaVersion: 1, Fixture: pairedFaultFixture, Engine: "foundationdb", IdentityMap: traceIDs(h.config), InitialCut: r.Initial, Steps: r.Steps, Scope: "Paired local functional client/one-zone/all-server process faults only; no independent hosts, power loss, cost matrix or engine selection"}, Originals: r.Originals, FaultObservations: r.Observations, Faults: r.Faults}
	for i, step := range r.Steps {
		o := step.RawObservation.Groups[i].Outcome
		t.Steps = append(t.Steps, pairedTraceStep{step, pairedRequest{o.Coordinator, h.config.Graph, o.Request, o.ID, bytes.Clone(r.Originals[i].Bytes)}})
		t.OperationMap = append(t.OperationMap, traceOperation{step.Operation, o.Coordinator, o.Request, o.ID, o.Round})
		raw, view, _, err := h.lifecycleObserve(cut{r.Initial}, cut{step.Cut}, false)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(view, step.After) {
			return errInvalid
		}
		if i < 5 {
			t.OldReads = append(t.OldReads, traceOldRead{step.Operation, "reopened-at", step.Cut, view, raw})
		}
	}
	raw, held, _, err := h.lifecycleObserve(cut{r.Initial}, cut{r.Steps[2].Cut}, true)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(held, r.Held.View) || !reflect.DeepEqual(raw, r.Held.RawObservation) {
		return errInvalid
	}
	t.OldReads = append(t.OldReads, *r.Held)
	last := r.Steps[5]
	t.FinalRawCurrent, err = h.lifecycleRawRows(cut{last.Cut}, "current")
	if err != nil {
		return err
	}
	if err = h.lifecycleValidateCurrent(t.FinalRawCurrent, last.RawObservation.Groups); err != nil {
		return err
	}
	writes, changes := 0, 0
	for _, group := range last.RawObservation.Groups {
		writes += len(group.Writes)
	}
	for _, step := range t.Steps {
		changes += len(step.LogicalCDC)
	}
	t.RawCounts = map[string]any{"completeGroups": len(t.Steps), "graphEffectWrites": writes, "logicalComponentChanges": changes, "retainedHistoryRows": len(last.RawObservation.History), "exactInstallWriteCount": nil}
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	if len(b) > 2<<20 {
		return errLimit
	}
	_, err = os.Stdout.Write(append(b, '\n'))
	return err
}
