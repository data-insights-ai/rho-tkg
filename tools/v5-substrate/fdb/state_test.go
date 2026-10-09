package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func fixture(t *testing.T, c config) *harness {
	t.Helper()
	db, e := openDB()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(db.Close)
	h, e := newHarness(db, c)
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func testConfig(t *testing.T) config { c := defaults(); c.Graph = t.Name(); return c }
func tx(c config, id string, coord uint8, ps ...participant) transaction {
	return transaction{Graph: c.Graph, Topology: c.Topology, ID: id, Request: "request/" + id, Coordinator: coord, Participants: ps}
}
func part(c config, g uint8, gen uint64, es ...effect) participant {
	return participant{Group: g, Epoch: c.Epochs[g], Generation: gen, Effects: es}
}
func mustSubmit(t *testing.T, h *harness, x transaction) outcome {
	t.Helper()
	o, e := h.submit(x)
	if e != nil || !o.Commit {
		t.Fatalf("submit %+v: %+v %v", x, o, e)
	}
	return o
}
func mustCut(t *testing.T, h *harness, scope []uint8, after *outcome) cut {
	t.Helper()
	c, e := h.fresh(scope, after)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func exact(t *testing.T, h *harness, c cut, want [2]map[string]value) {
	t.Helper()
	s, e := h.at(c, 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(s.Values, want) {
		t.Fatalf("exact whole state got=%v want=%v", s.Values, want)
	}
}

// This oracle never calls harness validation/snapshot/round code. It owns only
// scalar revisions, declared whole effects and generation read dependencies.
type whole struct {
	Tx    transaction
	Round uint64
}
type model struct {
	Rows [2]map[string]value
	Gen  [2]uint64
}

func emptyModel() model { return model{Rows: [2]map[string]value{{}, {}}} }
func modelValid(m model, x transaction) bool {
	for _, p := range x.Participants {
		if m.Gen[p.Group] != p.Generation {
			return false
		}
		for _, r := range p.Reads {
			if m.Rows[p.Group][r.Key].Version != r.Version {
				return false
			}
		}
	}
	return true
}
func modelApply(m *model, w whole) {
	for _, p := range w.Tx.Participants {
		for _, e := range p.Effects {
			v := m.Rows[p.Group][e.Key]
			m.Rows[p.Group][e.Key] = value{e.Value, v.Version + 1, w.Round, e.Delete, w.Tx.ID}
		}
		if len(p.Effects) > 0 {
			m.Gen[p.Group]++
		}
	}
}
func serial(ws []whole) bool {
	var visit func(model, []whole) bool
	visit = func(m model, left []whole) bool {
		if len(left) == 0 {
			return true
		}
		for i, w := range left {
			if !modelValid(m, w.Tx) {
				continue
			}
			n := model{Rows: [2]map[string]value{maps.Clone(m.Rows[0]), maps.Clone(m.Rows[1])}, Gen: m.Gen}
			modelApply(&n, w)
			tail := append(slices.Clone(left[:i]), left[i+1:]...)
			if visit(n, tail) {
				return true
			}
		}
		return false
	}
	return visit(emptyModel(), ws)
}
func modelAt(ws []whole, round uint64) [2]map[string]value {
	ordered := slices.Clone(ws)
	slices.SortFunc(ordered, func(a, b whole) int {
		if a.Round < b.Round {
			return -1
		}
		if a.Round > b.Round {
			return 1
		}
		if a.Tx.ID < b.Tx.ID {
			return -1
		}
		if a.Tx.ID > b.Tx.ID {
			return 1
		}
		return 0
	})
	m := emptyModel()
	for _, w := range ordered {
		if w.Round <= round {
			modelApply(&m, w)
		}
	}
	for _, rows := range m.Rows {
		maps.DeleteFunc(rows, func(_ string, v value) bool { return v.Deleted })
	}
	return m.Rows
}

func TestWholeTransactionHistoryAndPagedFence(t *testing.T) {
	c := testConfig(t)
	h := fixture(t, c)
	edge := tx(c, "edge", 1, part(c, 0, 0, effect{Key: "edge/e", Value: 5}, effect{Key: "out/e", Value: 5}), part(c, 1, 0, effect{Key: "in/e", Value: 5}))
	first := mustSubmit(t, h, edge)
	history := []whole{{edge, first.Round}}
	old := mustCut(t, h, []uint8{0, 1}, &first)
	correction := tx(c, "correction", 1, part(c, 0, 1, effect{Key: "edge/e", Value: 6}, effect{Key: "out/e", Delete: true}, effect{Key: "out/new/e", Value: 6}), part(c, 1, 1, effect{Key: "in/e", Delete: true}, effect{Key: "in/new/e", Value: 6}))
	pages := 0
	var second outcome
	s, e := h.at(old, 1, func() {
		pages++
		if pages == 1 {
			// Cross the native MVCC window between page transactions. A version pin
			// substituted for retained history fails this adversarial test.
			time.Sleep(6 * time.Second)
			second = mustSubmit(t, h, correction)
		}
	})
	if e != nil || pages < 2 {
		t.Fatal(pages, e)
	}
	if second.Round <= old.record.Round {
		t.Fatal("post-fence history at/below cut", second)
	}
	history = append(history, whole{correction, second.Round})
	if !serial(history) {
		t.Fatal("nonserializable history")
	}
	if !reflect.DeepEqual(s.Values, modelAt(history, first.Round)) {
		t.Fatal("paged old cut contains later correction", s)
	}
	current := mustCut(t, h, []uint8{0, 1}, &second)
	exact(t, h, current, modelAt(history, second.Round))
	exact(t, h, old, modelAt(history, first.Round))
	del := tx(c, "delete", 1, part(c, 0, 2, effect{Key: "edge/e", Delete: true}, effect{Key: "out/new/e", Delete: true}), part(c, 1, 2, effect{Key: "in/new/e", Delete: true}))
	third := mustSubmit(t, h, del)
	history = append(history, whole{del, third.Round})
	exact(t, h, mustCut(t, h, []uint8{0, 1}, &third), modelAt(history, third.Round))
	exact(t, h, old, modelAt(history, first.Round))
	// Owned reads cannot mutate stored values; reopen another native handle.
	s.Values[0]["edge/e"] = value{Value: 999}
	again := fixture(t, c)
	exact(t, again, old, modelAt(history, first.Round))
	// Independent oracle mutants: half effects and current-state substitution.
	half := modelAt(history, first.Round)
	delete(half[1], "in/e")
	if reflect.DeepEqual(half, modelAt(history, first.Round)) {
		t.Fatal("oracle missed half edge")
	}
	if reflect.DeepEqual(modelAt(history, third.Round), modelAt(history, first.Round)) {
		t.Fatal("oracle missed current substitution")
	}
	mustSubmit(t, h, tx(c, "scope-round", 0, part(c, 0, 3)))
	only := mustCut(t, h, []uint8{0}, nil)
	if _, e = h.at(cut{certificate{c.Graph, c.Topology, c.Epochs, []uint8{0, 1}, only.record.Round}}, 1, nil); !errors.Is(e, errUnavailable) {
		t.Fatal("expanded certificate accepted", e)
	}
}

func TestConflictGenerationAbsenceAndABA(t *testing.T) {
	c := testConfig(t)
	h := fixture(t, c)
	a := tx(c, "a", 0, part(c, 0, 0, effect{Key: "doctor/a", Value: 1}), part(c, 1, 0))
	b := tx(c, "b", 1, part(c, 0, 0), part(c, 1, 0, effect{Key: "doctor/b", Value: 1}))
	var wg sync.WaitGroup
	results := make([]outcome, 2)
	errs := make([]error, 2)
	for i, x := range []transaction{a, b} {
		wg.Go(func() { results[i], errs[i] = h.submit(x) })
	}
	wg.Wait()
	committed := []whole{}
	for i, x := range []transaction{a, b} {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if results[i].Commit {
			committed = append(committed, whole{x, results[i].Round})
		}
	}
	if len(committed) != 1 || !serial(committed) {
		t.Fatal("write skew", results)
	}
	exact(t, h, mustCut(t, h, []uint8{0, 1}, nil), modelAt(committed, results[0].Round+results[1].Round))
	if serial([]whole{{a, 1}, {b, 2}}) {
		t.Fatal("oracle accepted write-skew mutant")
	}
	// Capture empty predicate generation, insert a phantom, reject old generation.
	s, e := h.current()
	if e != nil {
		t.Fatal(e)
	}
	g := s.Generations[0]
	mustSubmit(t, h, tx(c, "insert", 0, part(c, 0, g, effect{Key: "phantom", Value: 8})))
	p := part(c, 0, g, effect{Key: "dependent", Value: 9})
	p.Reads = []read{{"missing", 0}}
	rejected, e := h.submit(tx(c, "absence", 0, p))
	if e != nil || rejected.Commit || rejected.Reason != "generation" {
		t.Fatal(rejected, e)
	}
	s, e = h.current()
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := s.Values[0]["dependent"]; ok {
		t.Fatal("phantom dependent exists")
	}
	for i, v := range []int64{1, 2, 1} {
		s, e = h.current()
		if e != nil {
			t.Fatal(e)
		}
		mustSubmit(t, h, tx(c, fmt.Sprintf("aba%d", i), 1, part(c, 1, s.Generations[1], effect{Key: "life", Value: v})))
	}
	s, e = h.current()
	if e != nil {
		t.Fatal(e)
	}
	p = part(c, 1, s.Generations[1], effect{Key: "derived", Value: 9})
	p.Reads = []read{{"life", 1}}
	rejected, e = h.submit(tx(c, "historical", 1, p))
	if e != nil || rejected.Commit || rejected.Reason != "revision" {
		t.Fatal("ABA", rejected, e)
	}
	replay, e := h.submit(tx(c, "historical", 1, p))
	if e != nil || replay != rejected {
		t.Fatal("terminal abort changed", replay, e)
	}
}

func TestRequestScopeDedupAndAmbiguousNativeCancel(t *testing.T) {
	c := testConfig(t)
	h := fixture(t, c)
	x := tx(c, "one", 0, part(c, 0, 0, effect{Key: "key", Value: 1}))
	first := mustSubmit(t, h, x)
	for range 3 {
		o, e := h.submit(x)
		if e != nil || o != first {
			t.Fatal("retry changed", o, e)
		}
	}
	recovered, e := h.recover(0, x.Request)
	if e != nil || recovered != first {
		t.Fatal(recovered, e)
	}
	if _, e = h.recover(1, x.Request); !errors.Is(e, errUnknown) {
		t.Fatal("request scope widened", e)
	}
	changed := x
	changed.Participants = slices.Clone(x.Participants)
	changed.Participants[0].Effects = []effect{{Key: "key", Value: 2}}
	if _, e = h.submit(changed); !errors.Is(e, errMismatch) {
		t.Fatal(e)
	}
	changed = x
	changed.ID = "reuse"
	if _, e = h.submit(changed); !errors.Is(e, errMismatch) {
		t.Fatal(e)
	}
	changed = x
	changed.Request = "different"
	if _, e = h.submit(changed); !errors.Is(e, errMismatch) {
		t.Fatal(e)
	}
	// Equivalent prototype scope: same request/TxID in another coordinator is
	// a separate binding, explicitly NOT a graph-wide allocator/dedup claim.
	other := tx(c, "one", 1, part(c, 1, 0, effect{Key: "key", Value: 2}))
	other.Request = x.Request
	mustSubmit(t, h, other)
	if _, e = h.recover(0, "absent"); !errors.Is(e, errUnknown) {
		t.Fatal("absence claimed abort", e)
	}
	// Genuine native cancellation after Commit: outcome remains ambiguous; retry
	// races safely with any still-in-flight attempt via the same request binding.
	uncertain := tx(c, "cancelled", 0, part(c, 0, 1, effect{Key: "unknown", Value: 3}), part(c, 1, 1, effect{Key: "unknown", Value: 3}))
	b, e := wire(uncertain)
	if e != nil {
		t.Fatal(e)
	}
	tr, e := h.db.CreateTransaction()
	if e != nil {
		t.Fatal(e)
	}
	o, e := h.execute(tr, uncertain, sha256.Sum256(b))
	if e != nil {
		t.Fatal(e)
	}
	future := tr.Commit()
	future.Cancel()
	nativeErr := future.Get()
	t.Logf("actual cancelled native commit result: %v; recovery never infers abort from absence", nativeErr)
	retry := mustSubmit(t, h, uncertain)
	if retry.Digest != o.Digest {
		t.Fatal("retry binding changed")
	}
	s, e := h.current()
	if e != nil {
		t.Fatal(e)
	}
	for g := range 2 {
		v := s.Values[g]["unknown"]
		if v.Value != 3 || v.Version != 1 {
			t.Fatal("unknown retry duplicated or omitted effect", s)
		}
	}
}

func TestScopesQuotasAndStrictInputs(t *testing.T) {
	c := testConfig(t)
	c.Limits.Versions = 1
	c.Limits.Certificates = 1
	h := fixture(t, c)
	x := tx(c, "first", 0, part(c, 0, 0, effect{Key: "k", Value: 4}))
	first := mustSubmit(t, h, x)
	old := mustCut(t, h, []uint8{0, 1}, nil)
	if _, e := h.submit(tx(c, "full", 0, part(c, 0, 1, effect{Key: "new", Value: 5}))); !errors.Is(e, errLimit) {
		t.Fatal(e)
	}
	if _, e := h.submit(x); e != nil {
		t.Fatal("quota evicted dedup", e)
	}
	exact(t, h, old, modelAt([]whole{{x, first.Round}}, first.Round))
	mustSubmit(t, h, tx(c, "no-effects", 0, part(c, 0, 1)))
	if _, e := h.fresh([]uint8{0, 1}, nil); !errors.Is(e, errLimit) {
		t.Fatal("certificate quota", e)
	}
	if _, e := h.fresh([]uint8{1, 0}, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := h.at(cut{}, 1, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	corrupt := old
	corrupt.record.Topology++
	if _, e := h.at(corrupt, 1, nil); !errors.Is(e, errStale) {
		t.Fatal(e)
	}
	corrupt = old
	corrupt.record.Round++
	if _, e := h.at(corrupt, 1, nil); !errors.Is(e, errUnavailable) {
		t.Fatal(e)
	}
	if _, e := h.at(old, 0, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	after := first
	after.Digest[0]++
	if _, e := h.fresh([]uint8{0}, &after); !errors.Is(e, errUnknown) {
		t.Fatal(e)
	}
	for _, change := range []func(*transaction){func(x *transaction) { x.Participants[0].Epoch++ }, func(x *transaction) { x.Graph = "other" }, func(x *transaction) { x.Topology++ }} {
		bad := x
		b, _ := wire(bad)
		bad, _ = decode[transaction](b)
		change(&bad)
		if _, e := h.submit(bad); !errors.Is(e, errStale) {
			t.Fatal(e)
		}
	}
	for _, b := range [][]byte{nil, []byte("{} "), []byte(`{"Unknown":1}`), []byte(`{"Graph":"a","Graph":"b"}`), bytesRepeated('[', 17)} {
		if _, e := decode[config](b); !errors.Is(e, errInvalid) {
			t.Fatal(string(b), e)
		}
	}
	var nilH *harness
	if _, e := nilH.submit(x); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := nilH.current(); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := nilH.recover(0, "a"); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := nilH.fresh([]uint8{0}, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := nilH.at(old, 1, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
}
func bytesRepeated(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestNativeConflictRangeBeforeWrites(t *testing.T) {
	c := testConfig(t)
	h := fixture(t, c)
	x := tx(c, "slow", 0, part(c, 0, 0, effect{Key: "slow", Value: 1}))
	tr, e := h.db.CreateTransaction()
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(x)
	if _, e = h.execute(tr, x, sha256.Sum256(b)); e != nil {
		t.Fatal(e)
	}
	mustSubmit(t, h, tx(c, "fast", 0, part(c, 0, 0, effect{Key: "fast", Value: 2})))
	e = tr.Commit().Get()
	native, ok := errors.AsType[fdb.Error](e)
	if !ok || native.Code != 1020 {
		t.Fatal("native conflict was not detected", e)
	}
	o, e := h.submit(x)
	if e != nil || o.Commit {
		t.Fatal("stale generation retry committed", o, e)
	}
}

func TestNativeFenceRejectsInFlightHistoryBelowClosedCut(t *testing.T) {
	c := testConfig(t)
	h := fixture(t, c)
	delayed := tx(c, "delayed", 0, part(c, 0, 0, effect{Key: "late", Value: 1}))
	tr, e := h.db.CreateTransaction()
	if e != nil {
		t.Fatal(e)
	}
	b, _ := wire(delayed)
	tentative, e := h.execute(tr, delayed, sha256.Sum256(b))
	if e != nil {
		t.Fatal(e)
	}
	closed := mustCut(t, h, []uint8{0, 1}, nil)
	if tentative.Round > closed.record.Round {
		t.Fatal("not a below-cut in-flight test")
	}
	nativeErr := tr.Commit().Get()
	native, ok := errors.AsType[fdb.Error](nativeErr)
	if !ok || native.Code != 1020 {
		t.Fatal("fence did not conflict pre-cut native transaction", nativeErr)
	}
	o := mustSubmit(t, h, delayed)
	if o.Round <= closed.record.Round {
		t.Fatal("retry history escaped native fence", o)
	}
	exact(t, h, closed, [2]map[string]value{{}, {}})
	exact(t, h, mustCut(t, h, []uint8{0, 1}, &o), modelAt([]whole{{delayed, o.Round}}, o.Round))
}

func TestMetadataReservationMaximumWidthsAndCorruptQuotaCounters(t *testing.T) {
	c := defaults()
	c.Graph = string(make([]byte, 128))
	c.Topology = ^uint64(0)
	c.Epochs = [2]uint64{^uint64(0), ^uint64(0)}
	c.Limits.Bytes = 8192
	h := fixture(t, c)
	if h.metadataReservation(0) <= 1024 || h.metadataReservation(0) >= 8192 {
		t.Fatal("escaped/worst-width reservation", h.metadataReservation(0))
	}
	_, e := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		rows, e := tr.GetRange(prefixRange(h.prefix(0)+"meta/"), fdb.RangeOptions{}).GetSliceWithError()
		if e != nil {
			return nil, e
		}
		actual := uint64(0)
		for _, row := range rows {
			actual += uint64(len(row.Key) + len(row.Value))
		}
		m, e := h.meta(tr, 0)
		if e != nil {
			return nil, e
		}
		if m.Bytes < actual {
			return nil, fmt.Errorf("metadata reservation undercharges %d < %d", m.Bytes, actual)
		}
		return nil, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	x := tx(c, "max-width", 0, part(c, 0, 0, effect{Key: "key", Value: -1}))
	o := mustSubmit(t, h, x)
	old := mustCut(t, h, []uint8{0, 1}, &o)
	exact(t, h, old, modelAt([]whole{{x, o.Round}}, o.Round))
	var saved metadata
	_, e = h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { var e error; saved, e = h.meta(tr, 0); return nil, e })
	if e != nil {
		t.Fatal(e)
	}
	for _, bad := range []metadata{{Bytes: 0}, {Bytes: c.Limits.Bytes + 1}, {Bytes: saved.Bytes, Versions: c.Limits.Versions + 1}, {Bytes: saved.Bytes, Transactions: c.Limits.Transactions + 1}, {Bytes: saved.Bytes, Certificates: c.Limits.Certificates + 1}} {
		_, e = h.db.Transact(func(tr fdb.Transaction) (any, error) { return nil, put(tr, h.key(0, "meta", "state"), bad) })
		if e != nil {
			t.Fatal(e)
		}
		if _, e = h.submit(tx(c, "corrupt-meta", 0, part(c, 0, 1))); !errors.Is(e, errInvalid) {
			t.Fatal("unsigned quota underflow accepted", bad, e)
		}
	}
	saved.Floor = ^uint64(0)
	_, e = h.db.Transact(func(tr fdb.Transaction) (any, error) { return nil, put(tr, h.key(0, "meta", "state"), saved) })
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.submit(tx(c, "overflow", 0, part(c, 0, 1))); !errors.Is(e, errLimit) {
		t.Fatal("round overflow", e)
	}
	// A mismatching immutable config is rejected on reopen, without partial reset.
	mismatch := c
	mismatch.Epochs[0]--
	if _, e = newHarness(h.db, mismatch); !errors.Is(e, errStale) {
		t.Fatal(e)
	}
}

func TestInvalidTransactionShapesAndFiniteByteQuota(t *testing.T) {
	c := testConfig(t)
	c.Limits.Bytes = 8192
	h := fixture(t, c)
	base := tx(c, "valid", 0, part(c, 0, 0, effect{Key: "k", Value: 1}))
	for _, mutate := range []func(*transaction){
		func(x *transaction) { x.ID = "" }, func(x *transaction) { x.Request = "" }, func(x *transaction) { x.Coordinator = 2 }, func(x *transaction) { x.Coordinator = 1 }, func(x *transaction) { x.Participants = nil },
		func(x *transaction) { x.Participants[0].Group = 2 }, func(x *transaction) { x.Participants[0].Epoch = 0 }, func(x *transaction) { x.Participants = append(x.Participants, x.Participants[0]) },
		func(x *transaction) { x.Participants[0].Reads = []read{{Key: ""}} }, func(x *transaction) { x.Participants[0].Reads = []read{{Key: "z"}, {Key: "a"}} },
		func(x *transaction) { x.Participants[0].Effects = []effect{{Key: "k", Value: 1, Delete: true}} }, func(x *transaction) { x.Participants[0].Effects = []effect{{Key: ""}} }, func(x *transaction) {
			x.Participants[0].Effects = append(x.Participants[0].Effects, x.Participants[0].Effects[0])
		},
	} {
		b, _ := wire(base)
		bad, _ := decode[transaction](b)
		mutate(&bad)
		if _, e := h.submit(bad); !errors.Is(e, errInvalid) {
			t.Fatal(bad, e)
		}
	}
	x := base
	for i := range 128 {
		x.Participants[0].Effects = append(x.Participants[0].Effects, effect{Key: fmt.Sprintf("%03d%s", i, string(make([]byte, 125)))})
	}
	if _, e := h.submit(x); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	x.Participants[0].Effects = x.Participants[0].Effects[1:]
	if _, e := h.submit(x); !errors.Is(e, errLimit) {
		t.Fatal("wire cap", e)
	}
	for i := range 30 {
		x = tx(c, fmt.Sprintf("byte%d", i), 0, part(c, 0, uint64(i), effect{Key: "key", Value: int64(i)}))
		o, e := h.submit(x)
		if errors.Is(e, errLimit) {
			s, e := h.current()
			if e != nil {
				t.Fatal(e)
			}
			if s.Values[0]["key"].Version != uint64(i) {
				t.Fatal("failed admission wrote partial state", s)
			}
			return
		}
		if e != nil || !o.Commit {
			t.Fatal(o, e)
		}
	}
	t.Fatal("finite encoded KV-byte quota did not exhaust")
}

func TestSmokeMainAndFaultRecoveryHelpers(t *testing.T) {
	oldArgs := slices.Clone(os.Args)
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"fdb-spike"}
	if e := run(); e == nil {
		t.Fatal("invalid usage")
	}
	os.Args = []string{"fdb-spike", "smoke"}
	if e := run(); e != nil {
		t.Fatal(e)
	}
	connection := os.Getenv("FDB_CONNECTION_STRING")
	t.Setenv("FDB_CONNECTION_STRING", "")
	if _, e := openDB(); e == nil {
		t.Fatal("missing connection")
	}
	t.Setenv("FDB_CONNECTION_STRING", connection)
	t.Setenv("FDB_FAULT_GRAPH", t.Name())
	db, e := openDB()
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	for _, mode := range []string{"fault-seed", "fault-verify", "fault-recover-before"} {
		if e := runFault(db, mode); e != nil {
			t.Fatal(mode, e)
		}
	}
	h, e := newHarness(db, faultConfig())
	if e != nil {
		t.Fatal(e)
	}
	mustSubmit(t, h, faultTx("after", 2))
	for _, mode := range []string{"fault-recover-after", "fault-verify", "fault-progress"} {
		if e := runFault(db, mode); e != nil {
			t.Fatal(mode, e)
		}
	}
	if e := runFault(db, "invalid"); e == nil {
		t.Fatal("unknown mode")
	}
	r, e := faultLoad()
	if e != nil {
		t.Fatal(e)
	}
	if e := faultCheck(h, r); e == nil {
		t.Fatal("extra effect missed")
	}
	os.Args = []string{"fdb-spike", "fault-verify"}
	if e := run(); e == nil {
		t.Fatal("fault state mismatch ignored")
	}
	b, e := os.ReadFile("/evidence/fault-state.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile("/evidence/fault-state.json", []byte("bad"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = faultLoad(); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if e = os.WriteFile("/evidence/fault-state.json", b, 0600); e != nil {
		t.Fatal(e)
	}
}

func TestCorruptStoredRowsFailClosed(t *testing.T) {
	c := testConfig(t)
	h := fixture(t, c)
	x := tx(c, "initial", 0, part(c, 0, 0, effect{Key: "key", Value: 1}))
	o := mustSubmit(t, h, x)
	old := mustCut(t, h, []uint8{0, 1}, &o)
	// Both current and immutable-history read doors reject malformed persisted
	// values; a corrupt stored record cannot silently become empty state.
	currentKey := h.key(0, "current", "key")
	historyKey := fdb.Key(fmt.Sprintf("%shistory/%016x/%s", h.prefix(0), o.Round, hex.EncodeToString([]byte("key"))))
	for _, k := range []fdb.Key{currentKey, historyKey} {
		var original []byte
		_, e := h.db.Transact(func(tr fdb.Transaction) (any, error) {
			var e error
			original, e = tr.Get(k).Get()
			if e != nil {
				return nil, e
			}
			tr.Set(k, []byte("not canonical JSON"))
			return nil, nil
		})
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Equal(k, currentKey) {
			if _, e := h.current(); !errors.Is(e, errInvalid) {
				t.Fatal("current corruption", e)
			}
		} else {
			if _, e := h.at(old, 1, nil); !errors.Is(e, errInvalid) {
				t.Fatal("history corruption", e)
			}
		}
		_, e = h.db.Transact(func(tr fdb.Transaction) (any, error) { tr.Set(k, original); return nil, nil })
		if e != nil {
			t.Fatal(e)
		}
	}
	// Canonical but invalid value-level state is also rejected.
	bad := value{Value: 1, Round: o.Round, TxID: "initial"}
	_, e := h.db.Transact(func(tr fdb.Transaction) (any, error) { return nil, put(tr, historyKey, bad) })
	if e != nil {
		t.Fatal(e)
	}
	if _, e := h.at(old, 1, nil); !errors.Is(e, errInvalid) {
		t.Fatal("zero revision", e)
	}
	_, e = h.db.Transact(func(tr fdb.Transaction) (any, error) { return nil, put(tr, currentKey, bad) })
	if e != nil {
		t.Fatal(e)
	}
	if _, e := h.current(); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	// Unknown config/metadata disappearance fails closed rather than reset.
	_, e = h.db.Transact(func(tr fdb.Transaction) (any, error) { tr.Clear(h.key(0, "meta", "state")); return nil, nil })
	if e != nil {
		t.Fatal(e)
	}
	if _, e := newHarness(h.db, c); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := h.current(); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
}

func TestFaultRecordMissingAndCertificateMismatch(t *testing.T) {
	t.Setenv("FDB_FAULT_GRAPH", t.Name())
	db, e := openDB()
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e := runFault(db, "fault-seed"); e != nil {
		t.Fatal(e)
	}
	r, e := faultLoad()
	if e != nil {
		t.Fatal(e)
	}
	h, e := newHarness(db, faultConfig())
	if e != nil {
		t.Fatal(e)
	}
	bad := r
	bad.Cut.Round++
	if e := faultCheck(h, bad); !errors.Is(e, errUnavailable) {
		t.Fatal(e)
	}
	bad = r
	bad.Cut.Graph = "another"
	if e := faultCheck(h, bad); !errors.Is(e, errStale) {
		t.Fatal(e)
	}
	if e = os.Remove("/evidence/fault-state.json"); e != nil {
		t.Fatal(e)
	}
	if _, e = faultLoad(); e == nil {
		t.Fatal("missing fault record")
	}
	if e = runFault(db, "fault-verify"); e == nil {
		t.Fatal(e)
	}
	r.Cut.Graph = string(make([]byte, 64<<10))
	if e = faultSave(r); !errors.Is(e, errLimit) {
		t.Fatal(e)
	}
}
