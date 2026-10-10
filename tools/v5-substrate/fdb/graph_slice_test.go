package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"maps"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func cloneRows(rows [2]map[string]value) [2]map[string]value {
	return [2]map[string]value{maps.Clone(rows[0]), maps.Clone(rows[1])}
}
func cloneDTO[T any](t *testing.T, v T) T {
	t.Helper()
	b, err := wire(v)
	if err != nil {
		t.Fatal(err)
	}
	x, err := decode[T](b)
	if err != nil {
		t.Fatal(err)
	}
	return x
}
func graphFixture(t *testing.T) *harness { return fixture(t, graphConfig(t.Name())) }

func TestGraphShapeAndProjectionContracts(t *testing.T) {
	h := &harness{config: graphConfig("shape")}
	os := []outcome{{Round: 2}, {Round: 3}, {Round: 4}, {Round: 5}}
	before := snapshot{Values: literalRows(os[:2])}
	x := h.graphTransaction("edge", 1, scenarioCommand(3), before)
	if err := h.validate(x); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*transaction){
		func(x *transaction) { x.Participants = x.Participants[:1] },
		func(x *transaction) { x.Participants[1].Effects = nil },
		func(x *transaction) { x.Participants[0].Effects[1].Value++ },
		func(x *transaction) { x.GraphChange.Edge.Target.Life = 23 },
		func(x *transaction) { x.GraphChange.Edge.Source.Owner = 1 },
		func(x *transaction) { x.GraphChange.Kind = "unknown" },
		func(x *transaction) { x.GraphChange.Edge.ID = 0 },
		func(x *transaction) { x.Participants[0].Reads[0].Version = 1 },
		func(x *transaction) { x.Participants[0].Reads = nil },
	} {
		bad := cloneDTO(t, x)
		mutate(&bad)
		if err := h.validate(bad); !errors.Is(err, errInvalid) {
			t.Fatal(bad, err)
		}
	}
	for stage := 1; stage <= 7; stage++ {
		outcomes := []outcome{{Round: 2}, {Round: 3}, {Round: 4}, {Round: 5}, {Round: 6}, {Round: 7}, {Round: 8}}
		view, err := h.projectGraph(snapshot{Values: literalRows(outcomes[:stage])})
		if err != nil || !reflect.DeepEqual(view, literalView("shape", stage)) {
			t.Fatal(stage, view, err)
		}
	}
	for _, mutate := range []func(*snapshot){
		func(s *snapshot) { delete(s.Values[1], oracleIn) },
		func(s *snapshot) { v := s.Values[1][oracleIn]; v.Value++; s.Values[1][oracleIn] = v },
		func(s *snapshot) { v := s.Values[0][oracleA]; v.Round = 0; s.Values[0][oracleA] = v },
		func(s *snapshot) { v := s.Values[0][oracleA]; v.Deleted = true; s.Values[0][oracleA] = v },
		func(s *snapshot) { s.Values[0][oracleA2] = value{1, 1, 5, false, "another-life"} },
		func(s *snapshot) {
			v := s.Values[0][oracleEdge]
			s.Values[1][oracleEdge] = v
			delete(s.Values[0], oracleEdge)
		},
		func(s *snapshot) { s.Values[0]["node/invalid/life/invalid"] = s.Values[0][oracleA] },
	} {
		s := snapshot{Values: literalRows(os[:3])}
		mutate(&s)
		if _, err := h.projectGraph(s); !errors.Is(err, errInvalid) {
			t.Fatal(s, err)
		}
	}
	if _, err := (*harness)(nil).projectGraph(snapshot{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.graphAt(cut{record: certificate{Scope: []uint8{0}}}); !errors.Is(err, errUnavailable) {
		t.Fatal(err)
	}
	if err := h.validate(h.graphTransaction("invalid-owner", 0, graphCommand{Kind: "node-create", Node: endpoint{2, 1, 1}}, before)); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if validGraphConfig(config{Nodes: [2]uint64{1, 1}}) || validGraphConfig(config{Nodes: [2]uint64{1, 0}}) {
		t.Fatal("invalid node declarations")
	}
}

func literalObservations() []graphObservation {
	h := &harness{config: graphConfig("oracle")}
	var observations []graphObservation
	ids := []string{"node-a", "node-b", "edge-create", "edge-update", "node-close", "edge-close", "node-reopen"}
	coords := []uint8{0, 1, 1, 0, 0, 1, 0}
	var os []outcome
	var seq [2]uint64
	for i, id := range ids {
		before := literalRows(os)
		x := h.graphTransaction(id, coords[i], scenarioCommand(i+1), snapshot{Values: before})
		if i > 0 {
			x.Dependency = os[i-1].Round
		}
		seq[coords[i]]++
		o := outcome{Commit: true, Round: uint64(i + 2), ID: id, Request: "request/" + id, Coordinator: coords[i], Sequence: seq[coords[i]]}
		os = append(os, o)
		observations = append(observations, graphObservation{int64(i*2 + 1), int64(i*2 + 2), x, o, certificate{Graph: "oracle", Topology: 1, Epochs: [2]uint64{7, 11}, Scope: []uint8{0, 1}, Round: o.Round, Groups: seq}, before, literalRows(os), literalView("oracle", i+1)})
	}
	return observations
}

func TestLiteralObservedHistoryRejectsAdversarialMutants(t *testing.T) {
	good := literalObservations()
	if err := checkLiteralHistory("oracle", good); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]graphObservation){
		func(rs []graphObservation) { delete(rs[2].After[1], oracleIn) },
		func(rs []graphObservation) { rs[2].After = cloneRows(rs[6].After) },
		func(rs []graphObservation) { rs[4].View.VisibleEdges = slices.Clone(rs[4].View.IdentityEdges) },
		func(rs []graphObservation) { rs[3].Before = cloneRows(rs[3].After) },
		func(rs []graphObservation) { rs[3].Tx.Dependency = 0 },
		func(rs []graphObservation) { rs[3].Started = rs[2].Started },
		func(rs []graphObservation) { rs[3].Tx.Participants[0].Reads[0].Version++ },
		func(rs []graphObservation) { rs[2].View.IdentityEdges[0].Source.Life = 12 },
		func(rs []graphObservation) { rs[2].Cut.Groups[1]-- },
	} {
		bad := cloneDTO(t, good)
		mutate(bad)
		if err := checkLiteralHistory("oracle", bad); err == nil {
			t.Fatal("mutant passed", bad)
		}
	}
	os := observationOutcomes(good)
	var groups []changeGroup
	for i, r := range good {
		groups = append(groups, changeGroup{r.Tx, r.Outcome, literalWrites(i+1, os)})
	}
	if err := checkLiteralChanges(os, groups); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]changeGroup) []changeGroup{
		func(gs []changeGroup) []changeGroup { gs[2].Writes = gs[2].Writes[:2]; return gs },
		func(gs []changeGroup) []changeGroup { return gs[:6] },
		func(gs []changeGroup) []changeGroup { gs[3] = gs[2]; return gs },
		func(gs []changeGroup) []changeGroup { gs[3].Writes[0].Before.Value++; return gs },
	} {
		if err := checkLiteralChanges(os, mutate(cloneDTO(t, groups))); err == nil {
			t.Fatal("feed mutant passed")
		}
	}
}

func TestNativeGraphLifecycleAndCompleteCDC(t *testing.T) {
	h := graphFixture(t)
	initial, observations, err := graphScenario(h)
	if err != nil {
		t.Fatal(err)
	}
	os := observationOutcomes(observations)
	for i, r := range observations {
		actual, err := h.at(cut{r.Cut}, 1, nil)
		if err != nil || !reflect.DeepEqual(actual.Values, literalRows(os[:i+1])) {
			t.Fatal(i, actual, err)
		}
		view, err := h.graphAt(cut{r.Cut})
		if err != nil || !reflect.DeepEqual(view, literalView(h.config.Graph, i+1)) {
			t.Fatal(i, view, err)
		}
	}
	// Closing the endpoint precedes closing the old identity: no live-life read is
	// required for cleanup. All three native current rows are exact tombstones.
	_, err = h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		for _, row := range []struct {
			g   uint8
			key string
		}{{0, oracleEdge}, {0, oracleOut}, {1, oracleIn}} {
			v, ok, err := get[value](tr, h.key(row.g, "current", row.key))
			if err != nil {
				return nil, err
			}
			if !ok || v != (value{0, 3, os[5].Round, true, "edge-close"}) {
				return nil, errInvalid
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if o, err := h.submit(observations[2].Tx); err != nil || o != os[2] {
		t.Fatal("retry changed result", o, err)
	}
	s, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	e := graphEdge{9, endpoint{0, 1, 12}, endpoint{1, 2, 22}, 8}
	o, err := h.submit(h.graphTransaction("rebind", 1, graphCommand{Kind: "edge-create", Edge: e}, s))
	if err != nil || o.Commit || o.Reason != "edge-identity" {
		t.Fatal("identity rebound", o, err)
	}
	stale := cloneDTO(t, observations[3].Tx)
	stale.ID, stale.Request = "stale-life", "request/stale-life"
	o, err = h.submit(stale)
	if err != nil || o.Commit || o.Reason != "endpoint-life" {
		t.Fatal("old LifeID became live again", o, err)
	}
	to := mustCut(t, h, []uint8{0, 1}, &os[6])
	groups, err := h.changes(initial, to, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = checkLiteralChanges(os, groups); err != nil {
		t.Fatal(err)
	}
	// Retained application history crosses the native MVCC window. Every page is
	// a new native read transaction; old endpoint and edge values remain exact.
	delayed := false
	old, err := h.at(cut{observations[2].Cut}, 1, func() {
		if !delayed {
			delayed = true
			time.Sleep(6 * time.Second)
		}
	})
	if err != nil || !delayed || !reflect.DeepEqual(old.Values, literalRows(os[:3])) {
		t.Fatal(old, err)
	}
}

func TestNativeScopedFeedReadsRemoteCoordinatorAndRejectsMissingGroups(t *testing.T) {
	h := graphFixture(t)
	initial := mustCut(t, h, []uint8{0}, nil)
	var last outcome
	for i, id := range []string{"node-a", "node-b", "edge-create"} {
		s, err := h.current()
		if err != nil {
			t.Fatal(err)
		}
		coord := uint8(1)
		if i == 0 {
			coord = 0
		}
		x := h.graphTransaction(id, coord, scenarioCommand(i+1), s)
		x.Dependency = last.Round
		last = mustSubmit(t, h, x)
	}
	to := mustCut(t, h, []uint8{0}, &last)
	groups, err := h.changes(initial, to, 1)
	if err != nil || len(groups) != 2 || groups[0].Outcome.ID != "node-a" || groups[1].Outcome != last || len(groups[1].Writes) != 3 {
		t.Fatal(groups, err)
	}
	// The complete cross-owner transaction is returned even though its
	// coordinator and incoming posting reside outside the requested scope.
	k := h.changeKey(1, last.Sequence, last.Request)
	var original []byte
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) {
		b, err := tr.Get(k).Get()
		if err != nil {
			return nil, err
		}
		original = b
		tr.Clear(k)
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.changes(initial, to, 1); !errors.Is(err, errUnavailable) {
		t.Fatal("missing group looked empty", err)
	}
	s, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	// Replay the exact committed request from the retained immutable group DTO.
	group, err := decode[changeGroup](original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.submit(group.Tx); !errors.Is(err, errUnavailable) {
		t.Fatal("retry ignored missing group", err, s)
	}
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) { tr.Set(k, original); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	bad := cloneDTO(t, group)
	bad.Writes = bad.Writes[:2]
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) { return nil, put(tr, k, bad) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.changes(initial, to, 1); !errors.Is(err, errInvalid) {
		t.Fatal("partial group accepted", err)
	}
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) { tr.Set(k, original); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	forged := to
	forged.record.Groups[1]++
	if _, err = h.changes(initial, forged, 1); !errors.Is(err, errUnavailable) {
		t.Fatal("forged watermark", err)
	}
	if _, err = h.changes(initial, to, 0); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err = h.graphAt(to); !errors.Is(err, errUnavailable) {
		t.Fatal("remote endpoint proof missing", err)
	}
}

func TestNativeActualAbsenceRangeConflictsWithoutMetadataWrites(t *testing.T) {
	h := graphFixture(t)
	tr, err := h.db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Cancel()
	rows, err := tr.GetRange(h.logicalRange(0, "edge/"), fdb.RangeOptions{}).GetSliceWithError()
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	_, err = h.db.Transact(func(other fdb.Transaction) (any, error) {
		other.Set(h.key(0, "current", oracleEdge), []byte("conflicting raw insertion"))
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.Set(fdb.Key(h.prefix(0)+"range-control"), []byte("must not commit"))
	err = tr.Commit().Get()
	native, ok := errors.AsType[fdb.Error](err)
	if !ok || native.Code != 1020 {
		t.Fatal("absence range did not conflict", err)
	}
	_, err = h.db.ReadTransact(func(read fdb.ReadTransaction) (any, error) {
		b, err := read.Get(fdb.Key(h.prefix(0) + "range-control")).Get()
		if err != nil {
			return nil, err
		}
		if b != nil {
			return nil, errInvalid
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNativeEndpointABAMasksRetainedIdentityBeforeCleanup(t *testing.T) {
	h := graphFixture(t)
	var os []outcome
	for i, id := range []string{"node-a", "node-b", "edge-create"} {
		s, err := h.current()
		if err != nil {
			t.Fatal(err)
		}
		coord := uint8(1)
		if i == 0 {
			coord = 0
		}
		x := h.graphTransaction(id, coord, scenarioCommand(i+1), s)
		if i > 0 {
			x.Dependency = os[i-1].Round
		}
		os = append(os, mustSubmit(t, h, x))
	}
	old := mustCut(t, h, []uint8{0, 1}, &os[2])
	for _, step := range []struct {
		id      string
		command graphCommand
	}{{"close-a", scenarioCommand(5)}, {"new-life", scenarioCommand(7)}} {
		s, err := h.current()
		if err != nil {
			t.Fatal(err)
		}
		mustSubmit(t, h, h.graphTransaction(step.id, 0, step.command, s))
	}
	s, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	// Literal expectations preserve the old edge and both raw postings while
	// changing only the endpoint incarnation. A NodeID-only rebinding fails.
	want := literalRows(os)
	delete(want[0], oracleA)
	newLife, exists := s.Values[0][oracleA2]
	if !exists || newLife.Value != 1 || newLife.Version != 1 || newLife.TxID != "new-life" {
		t.Fatal("missing new life", s)
	}
	want[0][oracleA2] = newLife
	if !reflect.DeepEqual(s.Values, want) {
		t.Fatal("lost/rebound identity postings", s.Values, want)
	}
	view, err := h.projectGraph(s)
	wantView := literalView(h.config.Graph, 3)
	wantView.Nodes = []endpoint{{0, 1, 12}, {1, 2, 22}}
	wantView.VisibleEdges, wantView.Outgoing, wantView.Incoming = nil, nil, nil
	if err != nil || !reflect.DeepEqual(view, wantView) {
		t.Fatal("old edge became live via NodeID", view, err)
	}
	oldRaw, err := h.at(old, 1, nil)
	oldView, viewErr := h.graphAt(old)
	if err != nil || viewErr != nil || !reflect.DeepEqual(oldRaw.Values, literalRows(os)) || !reflect.DeepEqual(oldView, literalView(h.config.Graph, 3)) {
		t.Fatal("old graph overwritten", oldRaw, oldView, err, viewErr)
	}
	closed := mustSubmit(t, h, h.graphTransaction("cleanup", 1, scenarioCommand(6), s))
	current, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	delete(want[0], oracleEdge)
	delete(want[0], oracleOut)
	delete(want[1], oracleIn)
	if !reflect.DeepEqual(current.Values, want) {
		t.Fatal("cleanup partial", current, closed)
	}
}

func TestChangeGroupValidationContracts(t *testing.T) {
	h := &harness{config: graphConfig("oracle")}
	rs := literalObservations()
	os := observationOutcomes(rs)
	group := changeGroup{rs[2].Tx, rs[2].Outcome, literalWrites(3, os)}
	b, err := wire(group.Tx)
	if err != nil {
		t.Fatal(err)
	}
	group.Outcome.Digest = sha256.Sum256(b)
	if err = h.validateChange(group); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*changeGroup){
		func(g *changeGroup) { g.Outcome.Sequence = 0 }, func(g *changeGroup) { g.Outcome.Commit = false },
		func(g *changeGroup) { g.Outcome.Digest[0]++ }, func(g *changeGroup) { g.Outcome.Coordinator = 0 },
		func(g *changeGroup) { g.Writes[0].After.TxID = "wrong" }, func(g *changeGroup) { g.Writes[0].After.Version = 2 },
		func(g *changeGroup) { g.Writes = append(g.Writes, g.Writes[0]) },
		func(g *changeGroup) { g.Writes[0].Before = new(value{Version: 1}) },
	} {
		bad := cloneDTO(t, group)
		mutate(&bad)
		if err = h.validateChange(bad); !errors.Is(err, errInvalid) {
			t.Fatal(bad, err)
		}
	}
	if !touchesScope(group.Tx, []uint8{0}) || touchesScope(transaction{Participants: []participant{{Group: 1}}}, []uint8{0}) {
		t.Fatal("scope classification")
	}
	if bytes.Equal(h.changeKey(0, 1, "request/a"), h.changeKey(1, 1, "request/a")) {
		t.Fatal("coordinator key collision")
	}
}

func TestNativeGraphRefusesIncompleteDescriptorsAndOrphanPosting(t *testing.T) {
	h := graphFixture(t)
	for i, id := range []string{"node-a", "node-b"} {
		s, err := h.current()
		if err != nil {
			t.Fatal(err)
		}
		mustSubmit(t, h, h.graphTransaction(id, uint8(i), scenarioCommand(i+1), s))
	}
	s, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	x := h.graphTransaction("create", 1, scenarioCommand(3), s)
	for _, mutate := range []func(*transaction){
		func(x *transaction) { x.Participants[1].Effects = nil },
		func(x *transaction) { x.GraphChange = nil },
		func(x *transaction) { x.Participants[0].Effects[1].Value = 7 },
	} {
		bad := cloneDTO(t, x)
		mutate(&bad)
		if _, err = h.submit(bad); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
		if _, err = h.recover(1, x.Request); !errors.Is(err, errUnknown) {
			t.Fatal("invalid descriptor mutated request table", err)
		}
		actual, err := h.current()
		if err != nil || !reflect.DeepEqual(actual, s) {
			t.Fatal("invalid descriptor mutated state", actual, err)
		}
	}
	// An orphan incoming posting may not be overwritten into a graph whose
	// three revision streams differ. Absence ranges see the actual native row.
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) {
		return nil, put(tr, h.key(1, "current", oracleIn), value{5, 1, 2, false, "orphan"})
	})
	if err != nil {
		t.Fatal(err)
	}
	o, err := h.submit(x)
	if err != nil || o.Commit || o.Reason != "graph-shape" {
		t.Fatal(o, err)
	}
	_, err = h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if b, err := tr.Get(h.key(0, "current", oracleEdge)).Get(); err != nil || b != nil {
			return nil, errors.Join(errInvalid, err)
		}
		v, ok, err := get[value](tr, h.key(1, "current", oracleIn))
		if err != nil {
			return nil, err
		}
		if !ok || v != (value{5, 1, 2, false, "orphan"}) {
			return nil, errInvalid
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNativeGraphCLIAndFaultRecoveryHelpers(t *testing.T) {
	args := slices.Clone(os.Args)
	t.Cleanup(func() { os.Args = args })
	t.Setenv("FDB_GRAPH_FIXTURE", t.Name()+"-smoke")
	os.Args = []string{"fdb-spike", "graph-smoke"}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FDB_GRAPH_FAULT_GRAPH", t.Name()+"-fault")
	db, err := openDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// These direct helper tests do not claim child SIGKILL. The separate native
	// graph-fault child modes supply those real before/after commit seams.
	for _, mode := range []string{"graph-fault-seed", "graph-fault-verify", "graph-fault-recover-before"} {
		if err = runGraphFault(db, mode); err != nil {
			t.Fatal(mode, err)
		}
	}
	r, err := graphFaultLoad()
	if err != nil {
		t.Fatal(err)
	}
	h, err := newHarness(db, graphFaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if r.AfterTx == nil {
		t.Fatal("missing captured after transaction")
	}
	committed := mustSubmit(t, h, *r.AfterTx)
	if err = runGraphFault(db, "graph-fault-recover-after"); err != nil {
		t.Fatal(err)
	}
	r, err = graphFaultLoad()
	if err != nil || r.After == nil || *r.After != committed {
		t.Fatal(r, err)
	}
	os.Args = []string{"fdb-spike", "graph-fault-verify"}
	if err = run(); err != nil {
		t.Fatal(err)
	}
	bad := cloneDTO(t, r)
	bad.Seed[2].Cut.Round++
	if err = graphFaultCheck(h, bad); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err = runGraphFault(db, "graph-fault-unknown"); err == nil {
		t.Fatal("unknown mode accepted")
	}
	b, err := os.ReadFile("/evidence/graph-fault-state.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("/evidence/graph-fault-state.json", []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = graphFaultLoad(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err = os.WriteFile("/evidence/graph-fault-state.json", b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestLiteralFeedReconstructsExactStateFromSnapshot(t *testing.T) {
	rs := literalObservations()
	os := observationOutcomes(rs)
	// Retain tombstones in the reconstruction's revision ledger, but return only
	// live rows. This independent fold never calls production projection/at.
	ledger := cloneRows(literalRows(os[:2]))
	for stage := 3; stage <= 7; stage++ {
		for _, write := range literalWrites(stage, os) {
			prior, exists := ledger[write.Group][write.Key]
			if exists != (write.Before != nil) || exists && prior != *write.Before {
				t.Fatal("snapshot/feed gap", stage, write)
			}
			ledger[write.Group][write.Key] = write.After
		}
		live := [2]map[string]value{{}, {}}
		for g, rows := range ledger {
			for key, v := range rows {
				if !v.Deleted {
					live[g][key] = v
				}
			}
		}
		if !reflect.DeepEqual(live, literalRows(os[:stage])) {
			t.Fatal(stage, live)
		}
	}
}
