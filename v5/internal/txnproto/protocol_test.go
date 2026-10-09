package txnproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func config(g uint8) Config {
	return Config{Graph: "fixture", Topology: 1, Group: g, Epochs: [2]uint64{7, 11}, Limits: DefaultLimits()}
}
func machine(t *testing.T, g uint8) *Machine {
	t.Helper()
	m, e := New(config(g))
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func must(t *testing.T, p Proposal, e error) Proposal {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func apply(t *testing.T, m *Machine, p Proposal) error {
	t.Helper()
	b, e := json.Marshal(p.command)
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Apply(replica.Entry{Index: m.applied + 1, Term: 1, Data: b}); e != nil {
		t.Fatal(e)
	}
	return m.last
}
func submit(t *testing.T, m *Machine, p Proposal) {
	t.Helper()
	if e := apply(t, m, p); e != nil {
		t.Fatal(e)
	}
}

// Unit reducer proofs below are intentionally tentative fixtures. Only the
// separate Host integration tests create production-authoritative Proof values.
func proof(t *testing.T, m *Machine, kind, id string, round uint64) Proof {
	t.Helper()
	v, e := m.answer(Query{kind, id, round}, max(m.applied, 1))
	if e != nil {
		t.Fatal(e)
	}
	return Proof{v}
}
func tx(id string, coord uint8, ps ...Participant) Tx {
	return Tx{Graph: "fixture", Topology: 1, ID: id, Request: "request/" + id, Coordinator: coord, Participants: ps}
}
func participant(g uint8, gen uint64, es ...Effect) Participant {
	return Participant{Group: g, Epoch: config(g).Epochs[g], Generation: gen, Effects: es}
}
func reg(t *testing.T, m *Machine, x Tx) Proof {
	t.Helper()
	p, e := Register(x)
	submit(t, m, must(t, p, e))
	return proof(t, m, Registration, x.ID, 0)
}
func prep(t *testing.T, m *Machine, r, prev Proof) Proof {
	t.Helper()
	p, e := Prepare(r, prev)
	submit(t, m, must(t, p, e))
	return proof(t, m, Prepared, r.view.Tx.ID, 0)
}
func vote(t *testing.T, m *Machine, v Proof) {
	t.Helper()
	p, e := RecordVote(v)
	submit(t, m, must(t, p, e))
}
func decide(t *testing.T, m *Machine, r Proof, commit bool) Proof {
	t.Helper()
	p, e := Decide(r, commit)
	submit(t, m, must(t, p, e))
	return proof(t, m, Decided, r.view.Tx.ID, 0)
}
func resolve(t *testing.T, m *Machine, d Proof) {
	t.Helper()
	p, e := Resolve(d)
	submit(t, m, must(t, p, e))
}
func cut(t *testing.T, round uint64, ms ...*Machine) Cut {
	t.Helper()
	var ps []Proof
	var scope []uint8
	for _, m := range ms {
		p, e := Fence(round)
		submit(t, m, must(t, p, e))
		p, e = Certify(round)
		submit(t, m, must(t, p, e))
		ps = append(ps, proof(t, m, Certified, "", round))
		scope = append(scope, m.config.Group)
	}
	c, e := AssembleCut(scope, ps)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func local(t *testing.T, m *Machine, x Tx) Proof {
	t.Helper()
	p, e := Local(x)
	submit(t, m, must(t, p, e))
	return proof(t, m, Decided, x.ID, 0)
}
func checkpoint(t *testing.T, m *Machine) *Machine {
	t.Helper()
	b, e := m.Checkpoint(16 << 20)
	if e != nil {
		t.Fatal(e)
	}
	n, e := New(m.config)
	if e != nil {
		t.Fatal(e)
	}
	if e = n.Restore(m.applied+3, b); e != nil {
		t.Fatal(e)
	}
	return n
}
func exact(t *testing.T, got Snapshot, want map[string]int64) {
	t.Helper()
	x := map[string]int64{}
	for k, v := range got.Values {
		x[k] = v.Value
	}
	if !reflect.DeepEqual(x, want) {
		t.Fatalf("exact state got=%v want=%v", x, want)
	}
}

func TestE15HalfInstallationAndCoordinatorOutsideScope(t *testing.T) {
	a, b := machine(t, 0), machine(t, 1)
	x := tx("edge", 1, participant(0, 0, Effect{Key: "edge/e", Value: 9}, Effect{Key: "out/n1/e", Value: 9}), participant(1, 0, Effect{Key: "in/n2/e", Value: 9}))
	r := reg(t, b, x)
	av := prep(t, a, r, Proof{})
	bv := prep(t, b, r, av)
	vote(t, b, av)
	vote(t, b, bv)
	d := decide(t, b, r, true)
	resolve(t, a, d)
	collections := []Proof{proof(t, a, Collection, "", 0), proof(t, b, Collection, "", 0)}
	if _, e := ChooseRound(collections, nil, 0); !errors.Is(e, ErrUnavailable) {
		t.Fatal("ignored unavailable decision", e)
	}
	// A-only fresh collection must see B's acknowledged decision before A apply.
	before := machine(t, 0)
	ar := reg(t, machine(t, 1), tx("remote", 1, participant(0, 0, Effect{Key: "k", Value: 1}), participant(1, 0)))
	_ = prep(t, before, ar, Proof{})
	if _, e := ChooseRound([]Proof{proof(t, before, Collection, "", 0)}, nil, 0); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	round, e := ChooseRound(collections, []Proof{d}, 0)
	if e != nil || round != d.view.Decision.Round {
		t.Fatal(round, e)
	}
	for _, m := range []*Machine{a, b} {
		p, e := Fence(round)
		submit(t, m, must(t, p, e))
	}
	p, e := Certify(round)
	if e = apply(t, b, must(t, p, e)); !errors.Is(e, ErrPending) {
		t.Fatal("half transaction certified", e)
	}
	resolve(t, b, d)
	c := cut(t, round, a, b)
	as, e := a.At(c)
	if e != nil {
		t.Fatal(e)
	}
	bs, e := b.At(c)
	if e != nil {
		t.Fatal(e)
	}
	exact(t, as, map[string]int64{"edge/e": 9, "out/n1/e": 9})
	exact(t, bs, map[string]int64{"in/n2/e": 9})
	if _, e := AssembleCut([]uint8{0, 1}, []Proof{proof(t, a, Certified, "", round)}); !errors.Is(e, ErrInvalid) {
		t.Fatal("missing scope accepted", e)
	}
	only, e := AssembleCut([]uint8{0}, []Proof{proof(t, a, Certified, "", round)})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.At(only); !errors.Is(e, ErrUnavailable) {
		t.Fatal("scope expanded", e)
	}
}

func TestE16OlderPrepareBelowObservedRoundAndDelayedPostFence(t *testing.T) {
	a, b := machine(t, 0), machine(t, 1)
	// Floors 89 yield old COMMIT 90 at coordinator B while A has observed 100.
	for _, m := range []*Machine{a, b} {
		p, e := Fence(89)
		submit(t, m, must(t, p, e))
	}
	x := tx("old", 1, participant(0, 0, Effect{Key: "a", Value: 1}), participant(1, 0, Effect{Key: "b", Value: 2}))
	r := reg(t, b, x)
	av := prep(t, a, r, Proof{})
	bv := prep(t, b, r, av)
	vote(t, b, av)
	vote(t, b, bv)
	p, e := Fence(100)
	submit(t, a, must(t, p, e))
	a = checkpoint(t, a)
	p, e = Certify(100)
	if e = apply(t, a, must(t, p, e)); !errors.Is(e, ErrPending) {
		t.Fatal("max observed certified unresolved old intent", e)
	}
	d := decide(t, b, r, true)
	if d.view.Decision.Round != 90 {
		t.Fatal(d.view.Decision)
	}
	resolve(t, a, d)
	resolve(t, b, d)
	c := cut(t, 100, a, b)
	s, e := a.At(c)
	if e != nil {
		t.Fatal(e)
	}
	exact(t, s, map[string]int64{"a": 1})
	// Delayed proposal is applied after fence, so its vote cannot retain floor 89.
	x = tx("late", 1, participant(0, 1, Effect{Key: "a", Value: 3}), participant(1, 1, Effect{Key: "b", Value: 4}))
	r = reg(t, b, x)
	pp, e := Prepare(r, Proof{})
	p, e2 := Fence(200)
	submit(t, a, must(t, p, e2))
	submit(t, a, must(t, pp, e))
	av = proof(t, a, Prepared, x.ID, 0)
	if av.view.Vote.Floor < 200 {
		t.Fatal("proposal floor sampled before fence")
	}
	bv = prep(t, b, r, av)
	vote(t, b, av)
	vote(t, b, bv)
	d = decide(t, b, r, true)
	if d.view.Decision.Round <= 200 {
		t.Fatal("delayed vote escaped fence")
	}
	resolve(t, a, d)
	resolve(t, b, d)
	// Earlier cut must still return version one, despite mutation after t0.
	s, e = a.At(c)
	if e != nil {
		t.Fatal(e)
	}
	exact(t, s, map[string]int64{"a": 1})
}

func TestE18MovedIndexAndDeletedHistory(t *testing.T) {
	a, b := machine(t, 0), machine(t, 1)
	d := local(t, a, tx("initial", 0, participant(0, 0, Effect{Key: "idx/old/e", Value: 1}, Effect{Key: "owner/e", Value: 1})))
	old := cut(t, d.view.Decision.Round, a, b)
	local(t, a, tx("correct", 0, participant(0, 1, Effect{Key: "idx/new/e", Value: 1}, Effect{Key: "idx/old/e", Delete: true}, Effect{Key: "owner/e", Value: 2})))
	a = checkpoint(t, a)
	s, e := a.At(old)
	if e != nil {
		t.Fatal(e)
	}
	exact(t, s, map[string]int64{"idx/old/e": 1, "owner/e": 1})
	now := proof(t, a, Current, "", 0).View()
	exact(t, *now.State, map[string]int64{"idx/new/e": 1, "owner/e": 2})
	now.State.Values["owner/e"] = Value{Value: 999}
	exact(t, *proof(t, a, Current, "", 0).view.State, map[string]int64{"idx/new/e": 1, "owner/e": 2})
}

func TestImmutableVotesRequestsDecisionsAndAbortBeforePrepare(t *testing.T) {
	a, b := machine(t, 0), machine(t, 1)
	x := tx("first", 1, participant(0, 0, Effect{Key: "x", Value: 1}), participant(1, 0))
	r := reg(t, b, x)
	av := prep(t, a, r, Proof{})
	a = checkpoint(t, a)
	// Coarse read/write lock survives restore; conflicting Tx records terminal NO.
	y := tx("conflict", 1, participant(0, 0, Effect{Key: "y", Value: 1}), participant(1, 0))
	yr := reg(t, b, y)
	yv := prep(t, a, yr, Proof{})
	if yv.view.Vote.Yes || yv.view.Vote.Reason != ErrRetry.Error() {
		t.Fatal("lock lost on restore", yv.View())
	}
	a = checkpoint(t, a)
	vote(t, b, yv)
	yd := decide(t, b, yr, false)
	resolve(t, a, yd)
	bv := prep(t, b, r, av)
	vote(t, b, av)
	vote(t, b, bv)
	d := decide(t, b, r, true)
	// Lost decision response: identical registration and decision retry recover it.
	reg(t, b, x)
	dp, e := Decide(r, true)
	submit(t, b, must(t, dp, e))
	if !reflect.DeepEqual(d.View(), proof(t, b, Decided, x.ID, 0).View()) { // read indices may differ
		got := proof(t, b, Decided, x.ID, 0)
		if digest(got.view.Decision) != digest(d.view.Decision) {
			t.Fatal("decision changed")
		}
	}
	resolve(t, a, d)
	resolve(t, b, d)
	a = checkpoint(t, a)
	b = checkpoint(t, b)
	// No vote cannot become yes after blocker has disappeared; installed yes cannot
	// reacquire locks after duplicate/delayed prepare or resolve.
	again := prep(t, a, yr, Proof{})
	if again.view.Vote.Yes {
		t.Fatal("NO became YES")
	}
	prep(t, a, r, Proof{})
	resolve(t, a, d)
	if a.state.Lock != "" || a.state.Versions != 1 {
		t.Fatal("terminal prepare reexecuted")
	}
	conflict := x
	conflict.Participants = slices.Clone(x.Participants)
	conflict.Participants[0].Effects = []Effect{{Key: "x", Value: 9}}
	p, e := Register(conflict)
	if e = apply(t, b, must(t, p, e)); !errors.Is(e, ErrMismatch) {
		t.Fatal(e)
	}
	p, e = Decide(r, false)
	if e = apply(t, b, must(t, p, e)); !errors.Is(e, ErrMismatch) {
		t.Fatal("commit became abort", e)
	}
	x2 := x
	x2.ID = "reuse"
	p, e = Register(x2)
	if e = apply(t, b, must(t, p, e)); !errors.Is(e, ErrMismatch) {
		t.Fatal("request reused", e)
	}
	z := tx("abortFirst", 1, participant(0, 1, Effect{Key: "z", Value: 7}), participant(1, 0))
	zr := reg(t, b, z)
	zd := decide(t, b, zr, false)
	resolve(t, a, zd)
	prep(t, a, zr, Proof{})
	if a.state.Lock != "" || a.state.Intents[z.ID].Vote.Yes {
		t.Fatal("delayed prepare escaped terminal abort")
	}
	// Participant B cannot acquire before A's immutable yes vote.
	zz := tx("unordered", 1, participant(0, 1), participant(1, 0))
	rr := reg(t, b, zz)
	p, e = Prepare(rr, Proof{})
	if e = apply(t, b, must(t, p, e)); !errors.Is(e, ErrStale) {
		t.Fatal("unordered acquire", e)
	}
}

func TestWriteSkewEmptyPredicateHistoricalABA(t *testing.T) {
	a, b := machine(t, 0), machine(t, 1)
	x := tx("x", 1, participant(0, 0, Effect{Key: "doctor/a", Value: 0}), participant(1, 0))
	y := tx("y", 0, participant(0, 0), participant(1, 0, Effect{Key: "doctor/b", Value: 0}))
	xr := reg(t, b, x)
	yr := reg(t, a, y)
	xv := prep(t, a, xr, Proof{})
	yv := prep(t, a, yr, Proof{})
	if !xv.view.Vote.Yes || yv.view.Vote.Yes {
		t.Fatal("write skew allowed both")
	}
	vote(t, a, yv)
	yd := decide(t, a, yr, false)
	resolve(t, a, yd)
	bv := prep(t, b, xr, xv)
	vote(t, b, xv)
	vote(t, b, bv)
	xd := decide(t, b, xr, true)
	resolve(t, a, xd)
	resolve(t, b, xd)
	// Empty predicate was read at generation 0; insertion must cause validation NO.
	z := tx("phantom", 0, participant(0, 0, Effect{Key: "reservation", Value: 1}))
	zr := reg(t, a, z)
	zv := prep(t, a, zr, Proof{})
	if zv.view.Vote.Yes {
		t.Fatal("empty predicate not validated")
	}
	// Value equality after ABA must not validate an old revision/generation.
	local(t, b, tx("aba1", 1, participant(1, 0, Effect{Key: "life", Value: 1})))
	local(t, b, tx("aba2", 1, participant(1, 1, Effect{Key: "life", Value: 2})))
	local(t, b, tx("aba3", 1, participant(1, 2, Effect{Key: "life", Value: 1})))
	old := participant(1, 3, Effect{Key: "derived", Value: 9})
	old.Reads = []Read{{"life", 1}}
	d := local(t, b, tx("historical", 1, old))
	if d.view.Decision.Commit {
		t.Fatal("version ABA accepted")
	}
}

func TestRecoveryReservationsAndExhaustion(t *testing.T) {
	a, b := machine(t, 0), machine(t, 1)
	l := DefaultLimits()
	l.Transitions = 10
	l.Transactions = 4
	l.Versions = 1
	for _, m := range []*Machine{a, b} {
		m.config.Limits = l
	}
	x := tx("reserved", 1, participant(0, 0, Effect{Key: "a", Value: 1}), participant(1, 0, Effect{Key: "b", Value: 1}))
	r := reg(t, b, x)
	av := prep(t, a, r, Proof{})
	bv := prep(t, b, r, av)
	// Fill all unreserved transition capacity. Decision/installation still fit.
	for _, m := range []*Machine{a, b} {
		for round := uint64(1); round < 20; round++ {
			p, e := Fence(round)
			err := apply(t, m, must(t, p, e))
			if errors.Is(err, ErrLimit) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	vote(t, b, av)
	vote(t, b, bv)
	d := decide(t, b, r, true)
	resolve(t, a, d)
	resolve(t, b, d)
	exact(t, *proof(t, a, Current, "", 0).view.State, map[string]int64{"a": 1})
	// Version/history quota rejects before new yes-vote and never evicts old dedup.
	y := tx("full", 0, participant(0, 1, Effect{Key: "c", Value: 1}))
	p, e := Local(y)
	if err := apply(t, a, must(t, p, e)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	p, e = Fence(math.MaxUint64)
	_ = apply(t, a, must(t, p, e))
	m := machine(t, 0)
	p, e = Fence(math.MaxUint64)
	submit(t, m, must(t, p, e))
	p, e = Local(tx("overflow", 0, participant(0, 0)))
	if e = apply(t, m, must(t, p, e)); !errors.Is(e, ErrLimit) {
		t.Fatal("round overflow", e)
	}
}

func TestStrictWireCheckpointAndStaleEpoch(t *testing.T) {
	m := machine(t, 0)
	for _, b := range [][]byte{[]byte("{}"), []byte(`{"Version":1,"Version":1}`), []byte(`{"Version":2}`), bytes.Repeat([]byte("["), 17), []byte(`{"Version":1,"Kind":"unknown","Tx":null,"Remote":null,"Prior":null,"Commit":false,"Round":0}`)} {
		if e := m.Apply(replica.Entry{Index: 1, Term: 1, Data: b}); !errors.Is(e, ErrInvalid) {
			t.Fatal(string(b), e)
		}
	}
	if e := m.Apply(replica.Entry{Index: 0, Term: 1}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e := m.Apply(replica.Entry{Index: 1, Term: 0}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e := m.Apply(replica.Entry{Index: 4, Term: 1}); e != nil {
		t.Fatal(e)
	} // configuration gaps legal
	x := tx("stale", 0, participant(0, 0))
	x.Participants[0].Epoch++
	p, e := Register(x)
	if e = apply(t, m, must(t, p, e)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	local(t, m, tx("ok", 0, participant(0, 0, Effect{Key: "k", Value: 2})))
	b, e := m.Checkpoint(16 << 20)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = m.Checkpoint(1); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	before := clone(m.state)
	for _, bad := range [][]byte{append(bytes.Clone(b), ' '), []byte(`{"Version":2}`), b[:len(b)-1]} {
		if e = m.Restore(100, bad); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(before, m.state) {
			t.Fatal("partial restore")
		}
	}
	if e = m.Restore(0, b); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e = m.Restore(100, b); e != nil {
		t.Fatal(e)
	}
	if e = m.Restore(0, nil); e != nil {
		t.Fatal(e)
	}
	if e = m.Restore(2, nil); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	var nilM *Machine
	if _, e = nilM.Checkpoint(1); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e = nilM.Restore(0, nil); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e = nilM.Apply(replica.Entry{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e = nilM.At(Cut{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}

func TestInvalidUTF8AndProposalValidation(t *testing.T) {
	invalid := string([]byte{'k', 0xff})
	valid := tx("valid", 0, participant(0, 0, Effect{Key: "key", Value: 1}))
	for _, change := range []func(*Tx){func(x *Tx) { x.Graph = invalid }, func(x *Tx) { x.ID = invalid }, func(x *Tx) { x.Request = invalid }, func(x *Tx) { x.Participants[0].Effects[0].Key = invalid }, func(x *Tx) { x.Participants[0].Reads = []Read{{invalid, 0}} }, func(x *Tx) {
		x.Participants[0].Effects = append(x.Participants[0].Effects, x.Participants[0].Effects[0])
	}, func(x *Tx) { x.Coordinator = 1 }} {
		x := clone(valid)
		change(&x)
		if _, e := Register(x); !errors.Is(e, ErrInvalid) {
			t.Fatal(x, e)
		}
		if _, e := Local(x); !errors.Is(e, ErrInvalid) {
			t.Fatal(x, e)
		}
	}
	c := config(0)
	c.Graph = invalid
	if _, e := New(c); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	c = config(0)
	c.Limits = Limits{}
	if _, e := New(c); e != nil {
		t.Fatal(e)
	}
	if _, e := New(Config{}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := Local(tx("cross", 0, participant(0, 0), participant(1, 0))); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := RecordVote(Proof{}); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e := Decide(Proof{}, true); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e := Fence(0); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := Certify(0); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	for _, q := range []Query{{Kind: Registration, TxID: invalid}, {Kind: Prepared}, {Kind: Decided, TxID: "id", Round: 1}, {Kind: Certified}, {Kind: Collection, TxID: "id"}, {Kind: Current, Round: 1}} {
		if e := checkQuery(q); !errors.Is(e, ErrInvalid) {
			t.Fatal(q, e)
		}
	}
}

func TestRecoveryByteReservationWithMaximumScalarPayload(t *testing.T) {
	// Maximum effect count, long keys and read footprints drive proof framing
	// near its command limit. Quotas fill with unrelated fences AFTER the yes-vote.
	effects := make([]Effect, 128)
	reads := make([]Read, 128)
	for j := range effects {
		key := fmt.Sprintf("%03d/%s", j, strings.Repeat("k", 20))
		effects[j] = Effect{Key: key, Value: int64(j)}
		reads[j] = Read{Key: key}
	}
	x := tx("wide", 1, participant(0, 0, effects...), participant(1, 0, effects...))
	x.Participants[0].Reads = reads
	x.Participants[1].Reads = reads
	encoded, _ := json.Marshal(x)
	coordBytes, intentBytes, err := recoveryBudgets(x, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	regView := View{Graph: x.Graph, Topology: x.Topology, Group: 1, Epoch: config(1).Epochs[1], Index: math.MaxUint64, Kind: Registration, Tx: &x}
	firstView := View{Graph: x.Graph, Topology: x.Topology, Group: 0, Epoch: config(0).Epochs[0], Index: math.MaxUint64, Kind: Prepared, Tx: &x, Vote: &Vote{Group: 0, Epoch: config(0).Epochs[0], Digest: digest(x), Effects: digest(x.Participants[0]), Yes: true}}
	rp, e := Register(x)
	if e != nil {
		t.Fatal(e)
	}
	regWire, _ := json.Marshal(rp.command)
	pa, e := Prepare(Proof{regView}, Proof{})
	if e != nil {
		t.Fatal(e)
	}
	paWire, _ := json.Marshal(pa.command)
	pb, e := Prepare(Proof{regView}, Proof{firstView})
	if e != nil {
		t.Fatal(e)
	}
	pbWire, _ := json.Marshal(pb.command)
	ca, cb := config(0), config(1)
	ca.Limits.Versions = 256
	cb.Limits.Versions = 256
	ca.Limits.JournalBytes = len(paWire) + 32 + intentBytes + 512
	cb.Limits.JournalBytes = len(regWire) + 32 + len(pbWire) + 32 + coordBytes + intentBytes + 512
	a, e := New(ca)
	if e != nil {
		t.Fatal(e)
	}
	b, e := New(cb)
	if e != nil {
		t.Fatal(e)
	}
	r := reg(t, b, x)
	av := prep(t, a, r, Proof{})
	bv := prep(t, b, r, av)
	for _, m := range []*Machine{a, b} {
		for round := uint64(1); round <= 4096; round++ {
			p, e := Fence(round)
			err := apply(t, m, must(t, p, e))
			if errors.Is(err, ErrLimit) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if round == 4096 {
				t.Fatal("quota did not exhaust")
			}
		}
	}
	vote(t, b, av)
	vote(t, b, bv)
	d := decide(t, b, r, true)
	resolve(t, a, d)
	resolve(t, b, d)
	if a.state.Versions != 128 || b.state.Versions != 128 {
		t.Fatal("reserved installation omitted effects")
	}
	a = checkpoint(t, a)
	b = checkpoint(t, b)
	if a.state.Lock != "" || b.state.Lock != "" {
		t.Fatal("resource pressure stranded locks")
	}
	// A smaller participant command limit cannot accept a yes-vote whose later
	// decision framing would not fit. The guard runs at authoritative application.
	small := config(0)
	small.Limits.CommandBytes = len(encoded) + 1024
	if small.Limits.CommandBytes > 64<<10 {
		t.Fatal("fixture too big")
	}
	m, e := New(small)
	if e != nil {
		t.Fatal(e)
	}
	p, e := Prepare(r, Proof{})
	if err := apply(t, m, must(t, p, e)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if m.state.Lock != "" {
		t.Fatal("undersized recovery frame accepted yes")
	}
}

func FuzzStrictApplicationAndCheckpoint(f *testing.F) {
	f.Add([]byte("{}"))
	f.Add([]byte(`{"Version":1,"Version":1}`))
	f.Add([]byte("[[[[[[[[[[[[[[[[["))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 64<<10 {
			t.Skip()
		}
		m := machine(t, 0)
		_ = m.Apply(replica.Entry{Index: 1, Term: 1, Data: b})
		_ = m.Restore(100, b)
	})
}

func TestFutureEnvelopeAdmissionEscapingAndUint64Widths(t *testing.T) {
	// The active command bound must cover future proofs, not just today's entry.
	// Config's smallest legal 1024-byte bound can fit registration but cannot fit
	// a full decision; no request binding or yes-vote may be acknowledged there.
	tiny := config(0)
	tiny.Limits.CommandBytes = 1024
	m, e := New(tiny)
	if e != nil {
		t.Fatal(e)
	}
	x := tx("tiny", 0, participant(0, 0, Effect{Key: "k", Value: 1}))
	p, e := Register(x)
	b, _ := json.Marshal(p.command)
	if len(b) > 1024 {
		t.Fatal("not a future-envelope test")
	}
	if err := apply(t, m, must(t, p, e)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if len(m.state.Coordinators) > 0 || len(m.state.Requests) > 0 {
		t.Fatal("undersized recovery protocol was registered")
	}
	// Worst string escaping and integer widths change wire bytes materially.
	graph := strings.Repeat("\x00", 128)
	id := strings.Repeat("\n", 128)
	request := strings.Repeat("\t", 128)
	x = Tx{Graph: graph, Topology: math.MaxUint64, ID: id, Request: request, Coordinator: 1, Dependency: math.MaxUint64 - 2, Participants: []Participant{{Group: 0, Epoch: math.MaxUint64, Effects: []Effect{{Key: strings.Repeat("\r", 128), Value: math.MinInt64}}}, {Group: 1, Epoch: math.MaxUint64, Effects: []Effect{{Key: strings.Repeat("\f", 128), Value: math.MaxInt64}}}}}
	lo, hi := 1024, 64<<10
	for lo < hi {
		mid := lo + (hi-lo)/2
		if _, _, err := recoveryBudgets(x, mid); err == nil {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	limit := lo
	if _, _, err := recoveryBudgets(x, limit); err != nil {
		t.Fatal("fixture does not fit finite envelope", err)
	}
	cfg := Config{Graph: graph, Topology: math.MaxUint64, Epochs: [2]uint64{math.MaxUint64, math.MaxUint64}, Limits: DefaultLimits()}
	cfg.Limits.CommandBytes = limit
	cfg.Group = 0
	a, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	cfg.Group = 1
	coord, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	r := reg(t, coord, x)
	r.view.Index = math.MaxUint64
	// One byte below the computed worst-case bound rejects before admission.
	small := cfg
	small.Limits.CommandBytes = limit - 1
	reject, e := New(small)
	if e != nil {
		t.Fatal(e)
	}
	p, e = Register(x)
	if err := apply(t, reject, must(t, p, e)); !errors.Is(err, ErrLimit) {
		t.Fatal("future envelope ignored", err)
	}
	small.Group = 0
	reject, e = New(small)
	if e != nil {
		t.Fatal(e)
	}
	p, e = Prepare(r, Proof{})
	if err := apply(t, reject, must(t, p, e)); !errors.Is(err, ErrLimit) {
		t.Fatal("prepared unresolvable yes", err)
	}
	if reject.state.Lock != "" || len(reject.state.Intents) > 0 {
		t.Fatal("declined prepare mutated ownership")
	}
	av := prep(t, a, r, Proof{})
	av.view.Index = math.MaxUint64
	bv := prep(t, coord, r, av)
	bv.view.Index = math.MaxUint64
	vote(t, coord, av)
	vote(t, coord, bv)
	d := decide(t, coord, r, true)
	d.view.Index = math.MaxUint64
	if d.view.Decision.Round != math.MaxUint64-1 {
		t.Fatal("dependency/max width round changed")
	}
	resolve(t, a, d)
	resolve(t, coord, d)
	if a.state.Lock != "" || coord.state.Lock != "" {
		t.Fatal("preflighted framing failed recovery")
	}
}

func TestDelayedPrepareCannotReacquireReadOnlyLocalTerminal(t *testing.T) {
	m := machine(t, 0)
	x := tx("readOnlyLocal", 0, participant(0, 0))
	d := local(t, m, x)
	m = checkpoint(t, m)
	r := proof(t, m, Registration, x.ID, 0)
	p, e := Prepare(r, Proof{})
	if err := apply(t, m, must(t, p, e)); !errors.Is(err, ErrMismatch) {
		t.Fatal("terminal local reacquired", err)
	}
	if m.state.Lock != "" || len(m.state.Intents) != 0 {
		t.Fatal("local terminal gained prepared state")
	}
	// An unnecessary duplicated resolve of the already-atomic local decision is
	// harmless; it cannot install again or create a late intent.
	p, e = Resolve(d)
	submit(t, m, must(t, p, e))
	if m.state.Lock != "" || len(m.state.Intents) != 0 {
		t.Fatal("local resolve created an intent")
	}
	// Known but irrelevant nested fields are invalid, not ignored on replay.
	bad := r.View()
	bad.State = &Snapshot{Values: map[string]Value{"phantom": {Value: 1}}}
	p, e = Prepare(Proof{bad}, Proof{})
	if err := apply(t, m, must(t, p, e)); !errors.Is(err, ErrInvalid) {
		t.Fatal("ignored nested proof payload", err)
	}
}
