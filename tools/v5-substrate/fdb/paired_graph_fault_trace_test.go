package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func pairedTestRun(t *testing.T, mode string) []byte {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "paired-output")
	if err != nil {
		t.Fatal(err)
	}
	oldOutput, oldArgs := os.Stdout, os.Args
	defer func() { os.Stdout, os.Args = oldOutput, oldArgs }()
	os.Stdout, os.Args = f, []string{"fdb-spike", mode}
	err = run()
	os.Stdout, os.Args = oldOutput, oldArgs
	if closeErr := f.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(mode, err)
	}
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Actual private/native transactions and request recovery; marker output errors
// exercise both seams without claiming that these unit tests kill a process.
func TestPairedGraphFaultActualRecoveryRetryMetadataAndHistoricalCuts(t *testing.T) {
	t.Setenv("FDB_PAIRED_GRAPH", t.Name())
	t.Setenv("FDB_PAIRED_RECORD", t.TempDir()+"/record.json")
	h := fixture(t, pairedConfig())
	pairedTestRun(t, "paired-fault-seed")
	r, err := pairedLoad()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Steps) != 3 || len(r.Originals) != 4 {
		t.Fatal("missing frozen seed", r)
	}
	original := bytes.Clone(r.Originals[3].Bytes)
	before, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	err = closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-before-child") })
	if !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	x, err := h.pairedOriginal(r, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.recover(1, x.Request); !errors.Is(err, errUnknown) {
		t.Fatal("private tx published result", err)
	}
	after, err := h.current()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("private tx leaked effects", after, err)
	}
	pairedTestRun(t, "paired-fault-recover-before")
	r, err = pairedLoad()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, r.Originals[3].Bytes) || len(r.Faults) != 1 || r.Faults[0].CallerClassification != "unknown" || r.Faults[0].Intermediate.Classification != "old-complete" || len(r.Faults[0].Intermediate.RawRecords) != 2 || !reflect.DeepEqual(r.Faults[0].Intermediate.View, r.Steps[2].After) {
		t.Fatal("unknown replaced by inferred abort", r.Faults)
	}
	if !reflect.DeepEqual(r.Faults[0].Retry.Before.RawObservation.Groups, r.Faults[0].Retry.After.RawObservation.Groups) || len(r.Faults[0].Retry.NewRawRecords) != 0 {
		t.Fatal("retry duplicated groups")
	}
	known := pairedCopy(t, r)
	if err = h.pairedNotFound(&known); !errors.Is(err, errInvalid) {
		t.Fatal("known original labelled absent", err)
	}
	known.Observations = nil
	if err = h.pairedFaultResult(&known, 4, "precommit-uncertainty", "boundary", "unknown", false); !errors.Is(err, errInvalid) {
		t.Fatal("missing intermediate observation invented", err)
	}
	if err = h.pairedMismatch(&known); !errors.Is(err, errInvalid) {
		t.Fatal("future original observed as complete", err)
	}
	if _, err = h.pairedCommitMarker(r, r.Steps[3].RawObservation.Groups[3].Outcome); !errors.Is(err, errInvalid) {
		t.Fatal("pre-kill receipt accepted incomplete group prefix", err)
	}
	err = closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-after-child") })
	if !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	x, err = h.pairedOriginal(r, 5)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := h.recover(1, x.Request)
	if err != nil || !committed.Commit {
		t.Fatal(committed, err)
	}
	preKillBytes, err := os.ReadFile(pairedRecordPath() + ".after-committed")
	if err != nil {
		t.Fatal(err)
	}
	preKill, err := decode[pairedCommittedMarker](preKillBytes)
	if err != nil || preKill.Outcome != committed || !bytes.Equal(preKill.Original, r.Originals[4].Bytes) || len(preKill.Records) != 3 {
		t.Fatal("pre-marker durable receipt not bound", err)
	}
	pairedTestRun(t, "paired-fault-recover-after")
	r, err = pairedLoad()
	if err != nil {
		t.Fatal(err)
	}
	changed := r.Faults[1].ChangedPayload
	bad, err := decode[transaction](changed.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.validate(bad); err != nil {
		t.Fatal("changed payload did not pass shape", err)
	}
	if _, err = h.submit(bad); !errors.Is(err, errMismatch) {
		t.Fatal("did not reach actual immutable binding", err)
	}
	if !reflect.DeepEqual(changed.Before.RawObservation.Groups, changed.After.RawObservation.Groups) || !reflect.DeepEqual(changed.Before.RawRecords, changed.After.RawRecords) {
		t.Fatal("mismatch duplicated CDC")
	}
	pairedMarkerRefusals(t, h, r)
	pairedTestRun(t, "paired-fault-prepare-progress")
	pairedTestRun(t, "paired-fault-hold-old")
	pairedTestRun(t, "paired-fault-progress")
	// This is exporter/controller plumbing; actual kill/reopen belongs to launcher.
	pairedTestRun(t, "paired-fault-reopened")
	pairedTestRun(t, "paired-fault-progress")
	pairedTestRun(t, "paired-fault-verify")
	data := pairedTestRun(t, "paired-fault-terminal")
	var trace pairedFaultTrace
	if err = json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if len(trace.Steps) != 6 || len(trace.OldReads) != 6 || len(trace.Faults) != 4 {
		t.Fatal("incomplete trace")
	}
	if len(trace.Faults[3].ReopenRetries) != 6 {
		t.Fatal("missing original replay after reopen")
	}
	for i, replay := range trace.Faults[3].ReopenRetries {
		if !bytes.Equal(replay.Payload, trace.Originals[i].Bytes) || replay.Result.Logical != pairedOperationNames[i] || !reflect.DeepEqual(replay.Before.View, trace.Steps[5].After) || !reflect.DeepEqual(replay.Before.RawObservation.Groups, replay.After.RawObservation.Groups) || !reflect.DeepEqual(replay.Before.RawCurrent, replay.After.RawCurrent) || len(replay.NewRawRecords) != 0 {
			t.Fatal("reopen retry changed current/group ledger", i, replay)
		}
	}
	changes, writes, definitions := 0, 0, 0
	for i, step := range trace.Steps {
		changes += len(step.LogicalCDC)
		definitions += len(step.NewValues)
		if step.Operation != pairedOperationNames[i] {
			t.Fatal("wrong operation", step.Operation)
		}
		if i >= 2 {
			want := []int64{5, 6, 5, 6}[i-2]
			edge := step.After.Entities[2]
			if edge.Property.I64 != want || edge.Property.RevisionOperation != step.Operation || edge.Property.Provenance != uint64(i+1) || !edge.Effective || len(step.After.Lives) != 3 || !reflect.DeepEqual(step.After.CensusIDs, []string{"edge", "node-a", "node-b"}) || !reflect.DeepEqual(step.After.PhantomFound, []bool{false, false}) {
				t.Fatal("wrong actual graph/provenance/census", step)
			}
			if !reflect.DeepEqual(step.After.Postings[0].Declared, []string{"edge"}) || !reflect.DeepEqual(step.After.Postings[3].Effective, []string{"edge"}) {
				t.Fatal("lost postings", step.After.Postings)
			}
		}
	}
	for _, group := range trace.Steps[5].RawObservation.Groups {
		writes += len(group.Writes)
	}
	if changes != 7 || writes != 14 || definitions != 2 || len(trace.Steps[4].NewValues) != 0 || len(trace.Steps[5].NewValues) != 0 {
		t.Fatal("reused definitions reborn", changes, writes, definitions)
	}
	for _, old := range trace.OldReads {
		if old.Mode == "held-old" {
			if old.View.Entities[0].Property.I64 != 5 || len(old.View.Lives) != 1 {
				t.Fatal("held edge substituted current", old)
			}
			continue
		}
		for _, step := range trace.Steps {
			if old.Operation == step.Operation && (!reflect.DeepEqual(old.Cut, step.Cut) || !reflect.DeepEqual(old.View, step.After)) {
				t.Fatal("historical cut changed", old)
			}
		}
	}
	r, err = pairedLoad()
	if err != nil {
		t.Fatal(err)
	}
	last := r.Steps[5].RawObservation.Groups[5]
	wrong := last.Writes[2].After
	wrong.Value = 5
	restore := replaceNativeRecord(t, h, h.key(1, "current", oracleIn), wrong)
	if err = h.pairedCheck(r); !errors.Is(err, errUnavailable) {
		t.Fatal("missing exact incoming current revision guard", err)
	}
	restore()
	historyKey := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(1), last.Outcome.Round, []byte(oracleIn)))
	restore = replaceNativeRecord(t, h, historyKey, wrong)
	if err = h.pairedCheck(r); !errors.Is(err, errUnavailable) {
		t.Fatal("historical posting not independently checked", err)
	}
	restore()
	for _, mutate := range []func(*pairedFaultRecord){
		func(r *pairedFaultRecord) { r.Originals[3].Bytes = []byte("corrupt") },
		func(r *pairedFaultRecord) { r.Steps[2].After.Entities[2].Property.I64 = 6 },
		func(r *pairedFaultRecord) { r.Steps[3].RawRecords = nil },
		func(r *pairedFaultRecord) { r.Steps[4].RawObservation.Groups = nil },
	} {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		var bad pairedFaultRecord
		if err = json.Unmarshal(b, &bad); err != nil {
			t.Fatal(err)
		}
		mutate(&bad)
		if err = h.pairedCheck(bad); !errors.Is(err, errInvalid) {
			t.Fatal("corrupt original/observation accepted", err)
		}
	}
	if _, err = h.pairedOriginal(r, 0); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err = h.pairedCapture(&r, 7); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err = runPairedGraphFault(h.db, "paired-fault-invalid"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	pairedControlRefusals(t, h, r)
}

func pairedCopy(t *testing.T, r pairedFaultRecord) pairedFaultRecord {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var copy pairedFaultRecord
	if err = json.Unmarshal(b, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func pairedMarkerRefusals(t *testing.T, h *harness, r pairedFaultRecord) {
	t.Helper()
	wrongCount := pairedCopy(t, r)
	wrongCount.Faults = nil
	if err := h.pairedMismatch(&wrongCount); !errors.Is(err, errInvalid) {
		t.Fatal("mismatch published without owning fault record", err)
	}
	o := r.Steps[4].RawObservation.Groups[4].Outcome
	t.Run("pre-kill-record-owned-IO", func(t *testing.T) {
		t.Setenv("FDB_PAIRED_RECORD", t.TempDir()+"/missing/record")
		if _, err := h.pairedCommitMarker(r, o); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err := pairedSave(r); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	})
	large := pairedCopy(t, r)
	large.Originals[4].Bytes = make([]byte, 64<<10)
	if _, err := h.pairedCommitMarker(large, o); !errors.Is(err, errLimit) {
		t.Fatal("oversize original receipt accepted", err)
	}
	restore := replaceNativeRecord(t, h, h.key(1, "request", o.Request), "corrupt")
	defer restore()
	// Fresh(After) decodes the original outcome before reading its raw witness.
	if _, err := h.pairedCommitMarker(r, o); !errors.Is(err, errInvalid) {
		t.Fatal("pre-kill receipt hid corrupt original", err)
	}
	restore()
}

// Every case below uses the same six committed operations. Refusals must leave
// the actual graph, physical current ledger and complete group inventory intact.
func pairedControlRefusals(t *testing.T, h *harness, original pairedFaultRecord) {
	t.Helper()
	prefix := func(n int, frozen bool) pairedFaultRecord {
		r := pairedCopy(t, original)
		r.Steps = r.Steps[:n]
		count := n
		if frozen {
			count++
		}
		r.Originals = r.Originals[:count]
		return r
	}
	before, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	stale := *h
	stale.config.Limits.Bytes++ // Actual persisted structural config remains unchanged.
	three := prefix(3, true)
	five := prefix(5, false)
	fourOutcome := original.Steps[3].RawObservation.Groups[3].Outcome
	for _, action := range []func() error{
		func() error { r := prefix(3, false); return stale.pairedCapture(&r, 4) },
		func() error { r := pairedCopy(t, three); return stale.pairedAppend(&r, 4, fourOutcome) },
		func() error { _, err := stale.pairedObserve(original, 6); return err },
		func() error {
			r := pairedCopy(t, original)
			return stale.pairedFaultResult(&r, 6, "case", "boundary", "committed", false)
		},
		func() error { r := pairedCopy(t, three); return stale.pairedNotFound(&r) },
		func() error { r := pairedCopy(t, five); return stale.pairedMismatch(&r) },
		func() error { r := pairedCopy(t, original); return stale.pairedReopenRetries(&r) },
		func() error { _, err := stale.pairedCommitMarker(original, fourOutcome); return err },
	} {
		if err = action(); !errors.Is(err, errStale) {
			t.Fatal("stale persisted config accepted", err)
		}
	}
	badOperation := pairedCopy(t, original)
	badOperation.Originals[3].Operation = "foreign"
	badDependency := pairedCopy(t, original)
	x, err := decode[transaction](badDependency.Originals[3].Bytes)
	if err != nil {
		t.Fatal(err)
	}
	x.Dependency++
	badDependency.Originals[3].Bytes, err = wire(x)
	if err != nil {
		t.Fatal(err)
	}
	wrongBefore := pairedCopy(t, original)
	wrongBefore.Originals[0].BeforeRaw.Config = nil
	wrongHeld := pairedCopy(t, original)
	wrongHeld.Held.View.Entities[0].Property.I64 = 6
	wrongOutcome := fourOutcome
	wrongOutcome.Commit = false
	wrongReplay := original.Steps[5].RawObservation.Groups[5].Outcome
	wrongReplay.Round++
	for _, action := range []func() error{
		func() error { _, err := h.pairedOriginal(badOperation, 4); return err },
		func() error { _, err := h.pairedOriginal(badDependency, 4); return err },
		func() error { r := pairedCopy(t, original); return h.pairedAppend(&r, 0, fourOutcome) },
		func() error { r := pairedCopy(t, original); return h.pairedAppend(&r, 4, wrongOutcome) },
		func() error { r := pairedCopy(t, original); return h.pairedAppend(&r, 6, wrongReplay) },
		func() error { r := pairedCopy(t, original); return h.pairedAppend(&r, 4, fourOutcome) },
		func() error { return h.pairedCheckHistory(pairedFaultRecord{}) },
		func() error { return h.pairedCheckHistory(wrongBefore) },
		func() error { _, err := h.pairedObserve(original, 0); return err },
		func() error { _, err := h.pairedObserve(original, 5); return err },
		func() error {
			r := pairedCopy(t, original)
			return h.pairedFaultResult(&r, 0, "case", "boundary", "unknown", false)
		},
		func() error {
			r := pairedCopy(t, original)
			return h.pairedFaultResult(&r, 5, "case", "boundary", "unknown", false)
		},
		func() error { r := pairedFaultRecord{}; return h.pairedNotFound(&r) },
		func() error { r := pairedFaultRecord{}; return h.pairedMismatch(&r) },
		func() error { r := pairedFaultRecord{}; return h.pairedReopenRetries(&r) },
		func() error { return h.pairedVerifyOutput(wrongBefore) },
		func() error { return h.pairedTerminal(wrongBefore) },
		func() error { r := pairedCopy(t, original); r.Faults = nil; return h.pairedTerminal(r) },
		func() error { return h.pairedTerminal(wrongHeld) },
	} {
		if err = closedOutput(t, action); !errors.Is(err, errInvalid) {
			t.Fatal("invalid original/observation/control accepted", err)
		}
	}
	badCut := pairedCopy(t, original)
	badCut.Initial.Graph = "unavailable-origin"
	// Closed-cut CDC rejects unequal graph frames before authority lookup.
	if err = h.pairedCheckHistory(badCut); !errors.Is(err, errInvalid) {
		t.Fatal("foreign origin cut accepted", err)
	}
	badCut.Steps, badCut.Originals = badCut.Steps[:3], badCut.Originals[:3]
	if err = h.pairedCapture(&badCut, 4); !errors.Is(err, errInvalid) {
		t.Fatal("capture hid unavailable origin", err)
	}
	large := pairedCopy(t, original)
	large.Faults[0].CallerRaw = make([]byte, 2<<20)
	if err = h.pairedTerminal(large); !errors.Is(err, errLimit) {
		t.Fatal("oversize trace accepted", err)
	}
	t.Run("default-namespace-and-invalid-framing", func(t *testing.T) {
		t.Setenv("FDB_PAIRED_GRAPH", "")
		t.Setenv("FDB_PAIRED_RECORD", "")
		if pairedConfig().Graph != "graph-paired-local-faults" || pairedRecordPath() != "/evidence/paired-graph-fault-state.json" || pairedCommand(8) != (graphCommand{}) {
			t.Fatal("default/fixed mode contract")
		}
		t.Setenv("FDB_PAIRED_GRAPH", strings.Repeat("x", 129))
		if err := runPairedGraphFault(h.db, "paired-fault-seed"); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	})
	for _, mode := range []string{"paired-fault-recover-before", "paired-fault-recover-after", "paired-fault-prepare-progress", "paired-fault-hold-old", "paired-fault-reopened"} {
		if err = pairedSave(original); err != nil {
			t.Fatal(err)
		}
		if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, mode) }); !errors.Is(err, errInvalid) {
			t.Fatal(mode, err)
		}
	}
	for _, mode := range []string{"paired-fault-progress", "paired-fault-terminal", "paired-fault-before-child", "paired-fault-after-child"} {
		if err = pairedSave(prefix(3, false)); err != nil {
			t.Fatal(err)
		}
		if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, mode) }); !errors.Is(err, errInvalid) {
			t.Fatal(mode, err)
		}
	}
	if err = pairedSave(five); err != nil {
		t.Fatal(err)
	}
	if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-progress") }); !errors.Is(err, errInvalid) {
		t.Fatal("missing frozen progress original accepted", err)
	}
	if err = pairedSave(three); err != nil {
		t.Fatal(err)
	}
	if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-recover-before") }); !errors.Is(err, errInvalid) {
		t.Fatal("nonold intermediate accepted", err)
	}
	if err = pairedSave(prefix(4, true)); err != nil {
		t.Fatal(err)
	}
	if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-recover-after") }); !errors.Is(err, errUnavailable) {
		t.Fatal("future group substituted recovered cut", err)
	}
	corrupt := pairedCopy(t, original)
	corrupt.Originals[0].Bytes = nil
	if err = pairedSave(corrupt); err != nil {
		t.Fatal(err)
	}
	if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-verify") }); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	t.Run("missing-owned-record", func(t *testing.T) {
		t.Setenv("FDB_PAIRED_RECORD", t.TempDir()+"/absent")
		if err := runPairedGraphFault(h.db, "paired-fault-verify"); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	})
	// A new initial cut excludes the existing complete groups, so their retained
	// history has no matching prefix and the projection refuses availability.
	if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-seed") }); !errors.Is(err, errUnavailable) {
		t.Fatal("nonfresh graph replayed with new captures", err)
	}
	// Corrupt actual original-record witnesses one at a time, never the expected
	// graph fixture. Every mutation is restored before another action runs.
	for _, operation := range []int{1, 4, 5, 6} {
		o := original.Steps[operation-1].RawObservation.Groups[operation-1].Outcome
		key := h.key(o.Coordinator, "request", o.Request)
		restore := replaceNativeRecord(t, h, key, "corrupt")
		defer restore()
		if operation == 1 {
			if err = h.pairedCheckHistory(original); !errors.Is(err, errUnavailable) {
				t.Fatal(err)
			}
			r := pairedCopy(t, original)
			if err = h.pairedReopenRetries(&r); !errors.Is(err, errInvalid) {
				t.Fatal("corrupt recovered original accepted", err)
			}
		} else {
			r := prefix(operation-1, true)
			if err = pairedSave(r); err != nil {
				t.Fatal(err)
			}
			mode := map[int]string{4: "paired-fault-before-child", 5: "paired-fault-after-child", 6: "paired-fault-progress"}[operation]
			if err = closedOutput(t, func() error { return runPairedGraphFault(h.db, mode) }); !errors.Is(err, errInvalid) {
				t.Fatal(mode, err)
			}
			if operation == 5 && !errors.Is(closedOutput(t, func() error { return runPairedGraphFault(h.db, "paired-fault-recover-after") }), errInvalid) {
				t.Fatal("corrupt recovery result accepted")
			}
			if operation == 6 {
				if _, err = h.pairedObserve(original, 6); !errors.Is(err, errInvalid) {
					t.Fatal(err)
				}
			}
		}
		restore()
	}
	o := original.Steps[0].RawObservation.Groups[0].Outcome
	wrong := o
	wrong.Round++
	restore := replaceNativeRecord(t, h, h.key(o.Coordinator, "request", o.Request), wrong)
	r := pairedCopy(t, original)
	if err = h.pairedReopenRetries(&r); !errors.Is(err, errMismatch) {
		t.Fatal("retry rebound corrupted original", err)
	}
	restore()
	r = pairedCopy(t, original)
	r.Steps[0].RawObservation.Groups[0].Outcome.Round++
	if err = h.pairedReopenRetries(&r); !errors.Is(err, errInvalid) {
		t.Fatal("reopen result differed from original receipt", err)
	}
	if err = pairedSave(original); err != nil {
		t.Fatal(err)
	}
	if err = h.pairedCheck(original); err != nil {
		t.Fatal(err)
	}
	after, err := h.current()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("refusals altered actual fixture/current ledger", err)
	}
}

func TestPairedRecordRefusesNoncanonicalAndOversizeWithoutReplacingEvidence(t *testing.T) {
	path := t.TempDir() + "/record.json"
	t.Setenv("FDB_PAIRED_RECORD", path)
	if _, err := pairedLoad(); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	r := pairedFaultRecord{}
	if err := pairedSave(r); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r.Originals = []pairedOriginal{{Bytes: make([]byte, 2<<20)}}
	if err = pairedSave(r); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	kept, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, kept) {
		t.Fatal("oversize overwrote evidence", err)
	}
	for _, data := range [][]byte{nil, []byte("{} "), []byte(`{"Unknown":1}`), make([]byte, 2<<20+1)} {
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = pairedLoad(); !errors.Is(err, errInvalid) {
			t.Fatal("invalid record accepted", err)
		}
	}
}
