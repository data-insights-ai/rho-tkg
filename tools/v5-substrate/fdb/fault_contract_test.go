package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// These tests intentionally do not run in parallel: stdout, argv, environment
// and the owned evidence record are process-wide interfaces.
func closedOutput(t *testing.T, action func() error) error {
	t.Helper()
	previous := os.Stdout
	file, err := os.CreateTemp(t.TempDir(), "closed-output")
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = file
	t.Cleanup(func() { os.Stdout = previous })
	defer func() { os.Stdout = previous }()
	return action()
}

func faultFixture(t *testing.T) (*harness, graphFaultRecord) {
	t.Helper()
	t.Setenv("FDB_GRAPH_FAULT_GRAPH", t.Name())
	db, err := openDB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err = runGraphFault(db, "graph-fault-seed"); err != nil {
		t.Fatal(err)
	}
	r, err := graphFaultLoad()
	if err != nil {
		t.Fatal(err)
	}
	h, err := newHarness(db, graphFaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return h, r
}

func mustSaveFault(t *testing.T, r graphFaultRecord) {
	t.Helper()
	if err := graphFaultSave(r); err != nil {
		t.Fatal(err)
	}
}

func TestNativeFaultMarkerWriteFailureSeparatesCommitBoundaries(t *testing.T) {
	h, r := faultFixture(t)
	before, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	// This is an actual native precommit preparation with a failed reply writer,
	// not a child kill. No native Commit is called before this output boundary.
	err = closedOutput(t, func() error { return runGraphFault(h.db, "graph-fault-before-child") })
	if !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	if _, err = h.recover(1, r.BeforeTx.Request); !errors.Is(err, errUnknown) {
		t.Fatal("failed precommit marker created outcome", err)
	}
	after, err := h.current()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("precommit output error mutated state", after, err)
	}
	if err = graphFaultCheck(h, r); err != nil {
		t.Fatal(err)
	}
	if err = runGraphFault(h.db, "graph-fault-recover-before"); err != nil {
		t.Fatal(err)
	}
	r, err = graphFaultLoad()
	if err != nil || r.AfterTx == nil {
		t.Fatal(r, err)
	}
	// A failed reply after native Commit must recover the same result/groups.
	err = closedOutput(t, func() error { return runGraphFault(h.db, "graph-fault-after-child") })
	if !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	committed, err := h.recover(1, r.AfterTx.Request)
	if err != nil || !committed.Commit {
		t.Fatal(committed, err)
	}
	if err = runGraphFault(h.db, "graph-fault-recover-after"); err != nil {
		t.Fatal(err)
	}
	r, err = graphFaultLoad()
	if err != nil || r.After == nil || *r.After != committed {
		t.Fatal(r, err)
	}
	if err = graphFaultCheck(h, r); err != nil {
		t.Fatal(err)
	}
	current, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := h.submit(*r.AfterTx); err != nil || retry != committed {
		t.Fatal(retry, err)
	}
	unchanged, err := h.current()
	if err != nil || !reflect.DeepEqual(unchanged, current) {
		t.Fatal("retry duplicated effects", unchanged, err)
	}
}

func TestNativeFaultModesRefuseMalformedOrWrongOutcomeRecords(t *testing.T) {
	h, r := faultFixture(t)
	base, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"graph-fault-after-child", "graph-fault-recover-after"} {
		if err = closedOutput(t, func() error { return runGraphFault(h.db, mode) }); !errors.Is(err, errInvalid) {
			t.Fatal(mode, err)
		}
	}
	bad := cloneDTO(t, r)
	bad.BeforeTx.ID = ""
	mustSaveFault(t, bad)
	if err = closedOutput(t, func() error { return runGraphFault(h.db, "graph-fault-before-child") }); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	bad = cloneDTO(t, r)
	bad.BeforeTx.ID, bad.BeforeTx.Request = "aborted-before", "request/aborted-before"
	bad.BeforeTx.Participants[0].Generation++
	mustSaveFault(t, bad)
	if err = closedOutput(t, func() error { return runGraphFault(h.db, "graph-fault-before-child") }); !errors.Is(err, errInvalid) {
		t.Fatal("aborted native preparation emitted ready", err)
	}
	actual, err := h.current()
	if err != nil || !reflect.DeepEqual(actual, base) {
		t.Fatal("refused child changed values", actual, err)
	}
	mustSaveFault(t, r)
	// A known result cannot be classified as a killed-before-commit UNKNOWN.
	committed := mustSubmit(t, h, r.BeforeTx)
	if err = runGraphFault(h.db, "graph-fault-recover-before"); err == nil {
		t.Fatal("known outcome classified as precommit UNKNOWN")
	}
	// The after mode must reject an admitted-but-aborted descriptor too.
	bad = cloneDTO(t, r)
	bad.AfterTx = new(r.BeforeTx)
	bad.AfterTx.ID = "bad-after"
	bad.AfterTx.Request = "request/bad-after"
	bad.AfterTx.Participants[0].Generation++
	mustSaveFault(t, bad)
	if err = closedOutput(t, func() error { return runGraphFault(h.db, "graph-fault-after-child") }); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	bad = cloneDTO(t, r)
	bad.AfterTx = new(r.BeforeTx)
	bad.AfterTx.ID = "unknown-after"
	bad.AfterTx.Request = "request/unknown-after"
	mustSaveFault(t, bad)
	if err = runGraphFault(h.db, "graph-fault-recover-after"); !errors.Is(err, errUnknown) {
		t.Fatal("unknown after inferred committed", err)
	}
	for _, mutate := range []func(*graphFaultRecord){
		func(r *graphFaultRecord) { r.Seed = nil },
		func(r *graphFaultRecord) { r.Before = nil; r.After = new(committed) },
		func(r *graphFaultRecord) { r.Before = new(committed); r.Before.Commit = false },
		func(r *graphFaultRecord) { r.Before = new(committed); r.Before.Coordinator = 0 },
	} {
		bad = cloneDTO(t, r)
		mutate(&bad)
		if err = graphFaultCheck(h, bad); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
}

func TestFaultRecordMissingOversizeAndConfigErrorsPreserveCallerData(t *testing.T) {
	h, r := faultFixture(t)
	original, err := os.ReadFile("/evidence/graph-fault-state.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile("/evidence/graph-fault-state.json", original, 0600); err != nil {
			t.Error(err)
		}
	})
	bad := cloneDTO(t, r)
	bad.Initial.Graph = strings.Repeat("x", 64<<10)
	if err = graphFaultSave(bad); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	unchanged, err := os.ReadFile("/evidence/graph-fault-state.json")
	if err != nil || !bytes.Equal(unchanged, original) {
		t.Fatal("oversize record overwrote prior file", err)
	}
	if err = os.Remove("/evidence/graph-fault-state.json"); err != nil {
		t.Fatal(err)
	}
	if _, err = graphFaultLoad(); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err = runGraphFault(h.db, "graph-fault-verify"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	t.Setenv("FDB_GRAPH_FAULT_GRAPH", strings.Repeat("x", 129))
	if err = runGraphFault(h.db, "graph-fault-seed"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	t.Setenv("FDB_GRAPH_FAULT_GRAPH", "")
	if got := graphFaultConfig().Graph; got != "graph-process-faults" {
		t.Fatal(got)
	}
}

func replaceNativeRecord(t *testing.T, h *harness, key fdb.Key, replacement any) func() {
	t.Helper()
	result, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { return tr.Get(key).Get() })
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(result.([]byte))
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) { return nil, put(tr, key, replacement) })
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		_, err := h.db.Transact(func(tr fdb.Transaction) (any, error) {
			if original == nil {
				tr.Clear(key)
			} else {
				tr.Set(key, original)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeFaultVerifierRejectsObservedReadAndCompleteGroupMutants(t *testing.T) {
	h, r := faultFixture(t)
	// The observations remain untouched. Mutate real stored values one at a
	// time so the verifier must inspect history/current/postings/complete CDC.
	history := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(0), r.Seed[2].Outcome.Round, []byte(oracleEdge)))
	old := value{5, 1, r.Seed[2].Outcome.Round, false, "edge-create"}
	wrong := old
	wrong.Value = 8
	var historyRestores []func()
	for _, row := range []struct {
		group uint8
		key   string
	}{{0, oracleEdge}, {0, oracleOut}, {1, oracleIn}} {
		key := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(row.group), r.Seed[2].Outcome.Round, []byte(row.key)))
		historyRestores = append(historyRestores, replaceNativeRecord(t, h, key, wrong))
	}
	if err := graphFaultCheck(h, r); err == nil {
		t.Fatal("retained wrong value passed")
	}
	for _, restore := range historyRestores {
		restore()
	}
	wrong = old
	wrong.Version = 0
	restore := replaceNativeRecord(t, h, history, wrong)
	if err := graphFaultCheck(h, r); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	restore()
	wrong = old
	wrong.Version = 0
	restore = replaceNativeRecord(t, h, h.key(0, "current", oracleEdge), wrong)
	if err := graphFaultCheck(h, r); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	restore()
	wrong = old
	wrong.Value = 8
	restore = replaceNativeRecord(t, h, h.key(1, "current", oracleIn), wrong)
	if err := graphFaultCheck(h, r); !errors.Is(err, errInvalid) {
		t.Fatal("incoming mutation passed", err)
	}
	restore()
	var restores []func()
	for _, key := range []struct {
		g uint8
		k string
	}{{0, oracleEdge}, {0, oracleOut}, {1, oracleIn}} {
		restores = append(restores, replaceNativeRecord(t, h, h.key(key.g, "current", key.k), wrong))
	}
	if err := graphFaultCheck(h, r); err == nil {
		t.Fatal("coherent current substitution passed")
	}
	for _, restore := range restores {
		restore()
	}
	groupKey := h.changeKey(1, r.Seed[2].Outcome.Sequence, r.Seed[2].Outcome.Request)
	result, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { return tr.Get(groupKey).Get() })
	if err != nil {
		t.Fatal(err)
	}
	group, err := decode[changeGroup](result.([]byte))
	if err != nil {
		t.Fatal(err)
	}
	group.Writes[0].Before = new(value{7, 1, 1, false, "foreign"})
	group.Writes[0].After.Version = 2
	// Structurally canonical before/after revisions must still match the literal
	// observation, even if the generic complete-group validator accepts shape.
	restore = replaceNativeRecord(t, h, groupKey, group)
	if err := graphFaultCheck(h, r); err == nil {
		t.Fatal("wrong complete revision passed")
	}
	restore()
	// Extra read-only transactions change no values but are complete CDC groups.
	// The fixed serial history must detect the extra group rather than use counts
	// of edge/postings alone to declare the whole history equivalent.
	current, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, h, tx(h.config, "extra-read-only", 0, part(h.config, 0, current.Generations[0])))
	if err := graphFaultCheck(h, r); err == nil {
		t.Fatal("extra complete group passed")
	}
}

func TestNativeGraphScenarioRefusalsAndDirectCLIEntry(t *testing.T) {
	if scenarioCommand(0) != (graphCommand{}) {
		t.Fatal("unknown stage")
	}
	if _, _, err := graphScenario(nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := runGraphSmoke(nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	h := graphFixture(t)
	if _, _, err := graphScenario(h); err != nil {
		t.Fatal(err)
	}
	before, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := graphScenario(h); !errors.Is(err, errMismatch) {
		t.Fatal("nonfresh namespace silently succeeded", err)
	}
	after, err := h.current()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("failed scenario changed state", after, err)
	}
	args := slices.Clone(os.Args)
	t.Cleanup(func() { os.Args = args })
	os.Args = []string{"fdb-spike", "graph-smoke"}
	t.Setenv("FDB_GRAPH_FIXTURE", "")
	main() // Actual public executable wrapper's success path, no exit seam.
	t.Setenv("FDB_GRAPH_FIXTURE", strings.Repeat("x", 129))
	if err := run(); !errors.Is(err, errInvalid) {
		t.Fatal("invalid CLI graph", err)
	}
	t.Setenv("FDB_CONNECTION_STRING", "")
	if err := run(); err == nil {
		t.Fatal("missing CLI connection")
	}
}
