package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphPlatformFormat = "fdb-platform-faults-v1"
const graphPlatformBytes = 2 << 20

// These files record caller intent/observations, never native decision authority.
// Four operations per fresh case: three setup commits and one useful update.
// The Python owner binds them to actual role/zone/process/network observations.
type graphPlatformAttempt struct {
	Classification, Error           string
	Request, Transaction            string
	BodySHA256                      [32]byte
	SubmitInvocations, SDKCallbacks int
	NativeCode                      *int `json:",omitempty"`
	Retryable, MaybeCommitted       bool
	Outcome                         outcome
	Originals                       [2]graphCostStoredRecord
	Original                        graphCostStoredRecord
}

func (a graphPlatformAttempt) unavailable() bool {
	return a.Classification == "native-error" && a.Error != "" && a.NativeCode != nil && (*a.NativeCode == 1031 || a.Retryable || a.MaybeCommitted) && !a.Outcome.Commit
}

type graphPlatformRecord struct {
	Version      uint8
	Format, Case string
	Coordinator  uint8
	Config       config
	Initial      certificate
	Originals    []pairedOriginal
	Steps        []traceStep
	Attempts     []graphPlatformAttempt
}
type graphPlatformTerminal struct {
	Format, Case                                                           string
	Coordinator                                                            uint8
	SetupOperations, UsefulOperations, CompleteGroups, PhysicalHistoryRows int
	Record                                                                 graphPlatformRecord
	Trace                                                                  fdbLifecycleTrace
	Replays                                                                []graphPlatformAttempt
}

func graphPlatformCase(s string) error {
	if !slices.Contains([]string{"transaction", "log", "storage", "one-zone", "two-zones", "network", "commit-issued"}, s) {
		return errInvalid
	}
	return nil
}
func graphPlatformSettings() (config, string, uint8, error) {
	name, graph, coord := os.Getenv("FDB_PLATFORM_CASE"), os.Getenv("FDB_PLATFORM_GRAPH"), os.Getenv("FDB_PLATFORM_COORDINATOR")
	if graphPlatformCase(name) != nil || !validName(graph) || !strings.HasPrefix(graph, "platform-v1-") || coord != "A" && coord != "B" {
		return config{}, "", 0, errInvalid
	}
	return lifecycleConfig(graph), name, uint8(strings.Index("AB", coord)), nil // #nosec G115 -- exact A/B selection.
}
func graphPlatformRoot() (*os.Root, error) {
	p := os.Getenv("FDB_PLATFORM_ARTIFACT")
	if p == "" {
		return nil, errInvalid
	}
	return os.OpenRoot(p)
}
func graphPlatformSave(r graphPlatformRecord) (err error) {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > graphPlatformBytes {
		return errLimit
	}
	root, err := graphPlatformRoot()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	f, err := root.OpenFile("state.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(b)
	closeErr := f.Close()
	if err = errors.Join(writeErr, closeErr); err != nil {
		return errors.Join(err, root.Remove("state.tmp"))
	}
	return root.Rename("state.tmp", "state.json")
}
func graphPlatformLoad() (r graphPlatformRecord, err error) {
	root, err := graphPlatformRoot()
	if err != nil {
		return r, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	f, err := root.Open("state.json")
	if err != nil {
		return r, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, graphPlatformBytes+1))
	if err != nil {
		return r, err
	}
	if len(b) == 0 || len(b) > graphPlatformBytes {
		return r, errLimit
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(canonical, b) {
		return r, errInvalid
	}
	c, name, coord, err := graphPlatformSettings()
	if err != nil || r.Version != 1 || r.Format != graphPlatformFormat || r.Config != c || r.Case != name || r.Coordinator != coord || len(r.Steps) < 3 || len(r.Steps) > 4 || len(r.Originals) != 4 || len(r.Attempts) > 8 {
		return r, errInvalid
	}
	return r, nil
}

var graphPlatformNames = [4]string{"node-a", "node-b", "edge-create", "edge-update"}

func (h *harness) platformOriginal(r graphPlatformRecord, i int) (transaction, error) {
	if h == nil || r.Config != h.config || r.Coordinator > 1 || i < 0 || i >= 4 || i >= len(r.Originals) || len(r.Steps) < i {
		return transaction{}, errInvalid
	}
	p := r.Originals[i]
	x, err := decode[transaction](p.Bytes)
	if err != nil {
		return x, err
	}
	coord := r.Coordinator
	if i == 0 {
		coord = 0
	}
	if i == 1 {
		coord = 1
	}
	dep := uint64(0)
	if i > 0 {
		dep = r.Steps[i-1].Cut.Round
	}
	if p.Operation != graphPlatformNames[i] || x.ID != p.Operation || x.Graph != r.Config.Graph || x.Request != "request/"+x.ID || x.Coordinator != coord || x.Dependency != dep || x.GraphChange == nil || *x.GraphChange != scenarioCommand(i+1) || x.Lifecycle == nil || x.Lifecycle.Provenance != uint64(i+1) {
		return x, errInvalid
	}
	if i >= 2 && (x.Lifecycle.EdgeLife != h.config.Lifecycle.EdgeLife || x.Lifecycle.Definition == nil || *x.Lifecycle.Definition != h.config.Lifecycle.Values[i-2]) {
		return x, errInvalid
	}
	return x, h.validate(x)
}
func (h *harness) platformCapture(r *graphPlatformRecord, i int) error {
	if i < 0 || i >= 4 || len(r.Steps) != i || len(r.Originals) != i {
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
	coord := r.Coordinator
	if i == 0 {
		coord = 0
	}
	if i == 1 {
		coord = 1
	}
	x := h.graphTransaction(graphPlatformNames[i], coord, scenarioCommand(i+1), s)
	if i > 0 {
		x.Dependency = r.Steps[i-1].Cut.Round
	}
	x.Lifecycle = &lifecycleMetadata{Provenance: uint64(i + 1)}
	if i >= 2 {
		x.Lifecycle.EdgeLife = h.config.Lifecycle.EdgeLife
		x.Lifecycle.Definition = new(h.config.Lifecycle.Values[i-2])
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
func (h *harness) platformAppend(r *graphPlatformRecord, i int, o outcome) error {
	x, err := h.platformOriginal(*r, i)
	if err != nil {
		return err
	}
	if !o.Commit || o.ID != x.ID || o.Request != x.Request || o.Coordinator != x.Coordinator || o.Digest != sha256.Sum256(r.Originals[i].Bytes) {
		return errMismatch
	}
	if len(r.Steps) == i+1 {
		if len(r.Steps[i].RawObservation.Groups) != i+1 || r.Steps[i].RawObservation.Groups[i].Outcome != o {
			return errMismatch
		}
		return nil
	}
	if len(r.Steps) != i {
		return errInvalid
	}
	c, err := h.fresh([]uint8{0, 1}, &o)
	if err != nil {
		return err
	}
	raw, v, fold, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return err
	}
	if len(raw.Groups) != i+1 || len(fold) != i+1 || !reflect.DeepEqual(raw.Groups[i].Tx, x) || raw.Groups[i].Outcome != o {
		return errUnavailable
	}
	step := fold[i]
	step.Before, step.After, step.Cut, step.RawObservation = r.Originals[i].Before, v, c.record, raw
	step.RawRecords, err = h.lifecycleRecordRows(c, raw.Groups[i])
	if err != nil {
		return err
	}
	r.Steps = append(r.Steps, step)
	return nil
}
func graphPlatformError(a *graphPlatformAttempt, err error) {
	a.Error = err.Error()
	a.Retryable, a.MaybeCommitted = graphCostParallelError(err)
	if e, ok := errors.AsType[fdb.Error](err); ok {
		a.NativeCode = new(e.Code)
		a.Classification = "native-error"
	} else {
		a.Classification = "caller-error"
	}
}
func (h *harness) platformSubmit(r *graphPlatformRecord) (graphPlatformAttempt, error) {
	x, err := h.platformOriginal(*r, 3)
	if err != nil {
		return graphPlatformAttempt{}, err
	}
	if len(r.Attempts) >= 8 {
		return graphPlatformAttempt{}, errLimit
	}
	a := graphPlatformAttempt{Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(r.Originals[3].Bytes), SubmitInvocations: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	got := h.graphCostParallelSubmit(ctx, x, r.Originals[3].Bytes)
	a.SDKCallbacks = got.Callbacks
	a.Outcome = got.Outcome
	if got.cause != nil {
		graphPlatformError(&a, got.cause)
	} else if got.Outcome.Commit {
		a.Classification = "committed"
	} else {
		a.Classification = "aborted"
	}
	r.Attempts = append(r.Attempts, a)
	// Preserve actual failure before any later observation/certificate can fail.
	if err := graphPlatformSave(*r); err != nil {
		return a, err
	}
	if got.cause == nil && !a.Outcome.Commit {
		return a, errUnavailable
	}
	// No certificate/read is demanded inside the outage. Native commit ACK is
	// retained here; full original and graph/cut checks follow the actual heal.
	return a, nil
}
func (h *harness) platformCheck(r graphPlatformRecord) (fdbLifecycleTrace, error) {
	trace := fdbLifecycleTrace{SchemaVersion: 1, Fixture: "graph-platform-four-v1", Engine: "foundationdb", IdentityMap: traceIDs(h.config), InitialCut: r.Initial, Scope: "fresh four-operation fixed lifecycle; raw real platform faults; no matrix or physical-host/power-loss acceptance"}
	if len(r.Steps) != 4 || len(r.Originals) != 4 {
		return trace, errInvalid
	}
	oslice := make([]outcome, 0, 4)
	for i, step := range r.Steps {
		x, err := h.platformOriginal(r, i)
		if err != nil {
			return trace, err
		}
		c := cut{step.Cut}
		raw, v, fold, err := h.lifecycleObserve(cut{r.Initial}, c, false)
		if err != nil {
			return trace, err
		}
		if len(raw.Groups) != i+1 || len(fold) != i+1 || !reflect.DeepEqual(raw, step.RawObservation) || !reflect.DeepEqual(v, step.After) || !reflect.DeepEqual(raw.Groups[i].Tx, x) || !reflect.DeepEqual(step.Before, r.Originals[i].Before) {
			return trace, errMismatch
		}
		beforeRaw, before, _, err := h.lifecycleObserve(cut{r.Initial}, cut{r.Originals[i].BeforeCut}, false)
		if err != nil {
			return trace, err
		}
		if !reflect.DeepEqual(beforeRaw, r.Originals[i].BeforeRaw) || !reflect.DeepEqual(before, r.Originals[i].Before) {
			return trace, errMismatch
		}
		rows, err := h.lifecycleRecordRows(c, raw.Groups[i])
		if err != nil {
			return trace, err
		}
		if !reflect.DeepEqual(rows, step.RawRecords) {
			return trace, errMismatch
		}
		o := raw.Groups[i].Outcome
		if !o.Commit || o.Digest != sha256.Sum256(r.Originals[i].Bytes) {
			return trace, errMismatch
		}
		oslice = append(oslice, o)
		actual, err := h.at(c, 1, nil)
		if err != nil {
			return trace, err
		}
		view, err := h.graphAt(c)
		if err != nil {
			return trace, err
		}
		if !reflect.DeepEqual(actual.Values, literalRows(oslice)) || !reflect.DeepEqual(view, literalView(h.config.Graph, i+1)) || !reflect.DeepEqual(raw.Groups[i].Writes, literalWrites(i+1, oslice)) {
			return trace, errMismatch
		}
		trace.Steps = append(trace.Steps, step)
		trace.OperationMap = append(trace.OperationMap, traceOperation{x.ID, []string{"A", "B"}[x.Coordinator], x.Request, x.ID, o.Round})
		trace.OldReads = append(trace.OldReads, traceOldRead{x.ID, "reopened-after-real-fault", c.record, v, raw})
	}
	c, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return trace, err
	}
	raw, v, _, err := h.lifecycleObserve(cut{r.Initial}, c, false)
	if err != nil {
		return trace, err
	}
	if !reflect.DeepEqual(raw, r.Steps[3].RawObservation) || !reflect.DeepEqual(v, r.Steps[3].After) || len(raw.History) != 8 || len(raw.Groups) != 4 {
		return trace, errMismatch
	}
	trace.FinalRawCurrent, err = h.lifecycleRawRows(c, "current")
	if err != nil {
		return trace, err
	}
	if err = h.lifecycleValidateCurrent(trace.FinalRawCurrent, raw.Groups); err != nil {
		return trace, err
	}
	if len(trace.FinalRawCurrent) != 5 {
		return trace, errMismatch
	}
	trace.RawCounts = map[string]any{"setupOperations": 3, "usefulOperations": 1, "completeGroups": 4, "graphEffectWrites": 8, "retainedHistoryRows": 8, "currentRows": 5}
	return trace, nil
}
func runGraphPlatformFault(db fdb.Database, mode string) error {
	c, name, coord, err := graphPlatformSettings()
	if err != nil {
		return err
	}
	h := &harness{db: db, config: c}
	if mode == "platform-fault-seed" {
		h, err = newHarness(db, c)
		if err != nil {
			return err
		}
		initial, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		raw, _, _, err := h.lifecycleObserve(initial, initial, false)
		if err != nil {
			return err
		}
		if len(raw.History) != 0 || len(raw.Groups) != 0 {
			return errMismatch
		}
		r := graphPlatformRecord{Version: 1, Format: graphPlatformFormat, Case: name, Coordinator: coord, Config: c, Initial: initial.record}
		for i := range 3 {
			if err = h.platformCapture(&r, i); err != nil {
				return err
			}
			x, err := h.platformOriginal(r, i)
			if err != nil {
				return err
			}
			o, err := h.submit(x)
			if err != nil {
				return err
			}
			if err = h.platformAppend(&r, i, o); err != nil {
				return err
			}
		}
		if err = h.platformCapture(&r, 3); err != nil {
			return err
		}
		if err = graphPlatformSave(r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	r, err := graphPlatformLoad()
	if err != nil {
		return err
	}
	switch mode {
	case "platform-fault-submit":
		a, err := h.platformSubmit(&r)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(a)
	case "platform-fault-commit-child":
		if r.Case != "commit-issued" || len(r.Attempts) != 0 || len(r.Steps) != 3 {
			return errInvalid
		}
		x, err := h.platformOriginal(r, 3)
		if err != nil {
			return err
		}
		tr, err := db.CreateTransaction()
		if err != nil {
			return err
		}
		defer tr.Cancel()
		if err = tr.Options().SetTimeout(10000); err != nil {
			return err
		}
		o, err := h.execute(tr, x, sha256.Sum256(r.Originals[3].Bytes))
		if err != nil || !o.Commit {
			return errors.Join(errUnavailable, err)
		}
		future := tr.Commit() // Actual native invocation; no success inferred from readiness.
		a := graphPlatformAttempt{Classification: "native-commit-issued-unobserved", Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(r.Originals[3].Bytes), SubmitInvocations: 1, SDKCallbacks: 1}
		r.Attempts = append(r.Attempts, a)
		if err = graphPlatformSave(r); err != nil {
			return err
		}
		if err = json.NewEncoder(os.Stdout).Encode(struct {
			Ready, Graph, Request, Transaction string
			BodySHA256                         [32]byte
		}{"native-commit-issued-unobserved", x.Graph, x.Request, x.ID, a.BodySHA256}); err != nil {
			return err
		}
		// A result may arrive internally before kill. Preserve it truthfully, while
		// withholding the public graph response; never synthesize commit_unknown_result.
		err = future.Get()
		if err != nil {
			graphPlatformError(&r.Attempts[0], err)
		} else {
			r.Attempts[0].Classification = "native-commit-returned-public-reply-withheld"
			r.Attempts[0].Outcome = o
		}
		// Preserve a returned native cause in the raw stderr channel. No mutable
		// caller-state rewrite after the marker can strand a killed .tmp writer.
		if err = json.NewEncoder(os.Stderr).Encode(r.Attempts[0]); err != nil {
			return err
		}
		select {}
	case "platform-fault-recover":
		x, err := h.platformOriginal(r, 3)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		proof, callbacks, lookupErr := h.graphCostReadParallelOriginal(ctx, x, r.Originals[3].Bytes)
		a := graphPlatformAttempt{Classification: "original-lookup", Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(r.Originals[3].Bytes), SDKCallbacks: callbacks, Outcome: proof.Outcome, Originals: proof.Pair, Original: proof.Original}
		if lookupErr != nil {
			if !errors.Is(lookupErr, errUnknown) {
				return lookupErr
			}
			// Actual both-absent observation only, never ABORT/replan permission.
			a.Classification = "paired-originals-absent-observed"
			if err = json.NewEncoder(os.Stdout).Encode(a); err != nil {
				return err
			}
			got, err := h.platformSubmit(&r)
			if err != nil {
				return err
			}
			if got.Classification != "committed" {
				return errUnavailable
			}
			// Authenticate the newly completed SAME request from the stored pair/CDC,
			// rather than trusting the returned client capture as decision authority.
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			proof, callbacks, lookupErr = h.graphCostReadParallelOriginal(ctx, x, r.Originals[3].Bytes)
			if lookupErr != nil {
				return lookupErr
			}
			a.Classification = "original-lookup"
			a.SDKCallbacks = callbacks
			a.Outcome = proof.Outcome
			a.Originals = proof.Pair
			a.Original = proof.Original
		}
		if !proof.Outcome.Commit {
			return errUnavailable
		}
		if err = h.platformAppend(&r, 3, proof.Outcome); err != nil {
			return err
		}
		if err = graphPlatformSave(r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(a)
	case "platform-fault-terminal":
		trace, err := h.platformCheck(r)
		if err != nil {
			return err
		}
		terminal := graphPlatformTerminal{Format: graphPlatformFormat, Case: r.Case, Coordinator: r.Coordinator, SetupOperations: 3, UsefulOperations: 1, CompleteGroups: 4, PhysicalHistoryRows: 8, Record: r, Trace: trace}
		for i := range 4 {
			x, err := h.platformOriginal(r, i)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			proof, callbacks, err := h.graphCostReadParallelOriginal(ctx, x, r.Originals[i].Bytes)
			cancel()
			if err != nil {
				return err
			}
			replay, err := h.submit(x)
			if err != nil {
				return err
			}
			if replay != proof.Outcome || replay != r.Steps[i].RawObservation.Groups[i].Outcome {
				return errMismatch
			}
			terminal.Replays = append(terminal.Replays, graphPlatformAttempt{Classification: "same-original-replay", Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(r.Originals[i].Bytes), SDKCallbacks: callbacks, Outcome: proof.Outcome, Originals: proof.Pair, Original: proof.Original})
		}
		after, err := h.platformCheck(r)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(trace, after) {
			return errMismatch
		}
		b, err := json.Marshal(terminal)
		if err != nil {
			return err
		}
		if len(b) > graphPlatformBytes {
			return errLimit
		}
		_, err = os.Stdout.Write(append(b, '\n'))
		return err
	default:
		return fmt.Errorf("unsupported explicit platform mode %q: %w", mode, errInvalid)
	}
}
