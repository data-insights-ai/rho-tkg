package txnproto

import (
	"cmp"
	"maps"
	"reflect"
	"slices"
	"testing"
)

// This model knows only whole transactions, scalar revisions and partition
// predicate generations. It does NOT call reducer validation, vote checks,
// locks, floor/fence code, certificate eligibility, or implementation snapshots.
type whole struct {
	tx    Tx
	round uint64
}
type model struct {
	rows        [2]map[string]Value
	generations [2]uint64
}

func emptyModel() model { return model{rows: [2]map[string]Value{{}, {}}} }
func modelClone(m model) model {
	return model{rows: [2]map[string]Value{maps.Clone(m.rows[0]), maps.Clone(m.rows[1])}, generations: m.generations}
}
func modelValid(m model, t Tx) bool {
	for _, p := range t.Participants {
		if m.generations[p.Group] != p.Generation {
			return false
		}
		for _, r := range p.Reads {
			if m.rows[p.Group][r.Key].Version != r.Version {
				return false
			}
		}
	}
	return true
}
func modelWrite(m *model, w whole) {
	for _, p := range w.tx.Participants {
		for _, e := range p.Effects {
			old := m.rows[p.Group][e.Key]
			m.rows[p.Group][e.Key] = Value{e.Value, old.Version + 1, w.round, e.Delete, w.tx.ID}
		}
		if len(p.Effects) > 0 {
			m.generations[p.Group]++
		}
	}
}

// Exhaustive serial-order search accepts no implementation guard as proof.
func serialOrder(ws []whole) ([]whole, bool) {
	var visit func(model, []whole, []whole) ([]whole, bool)
	visit = func(m model, left, path []whole) ([]whole, bool) {
		if len(left) == 0 {
			return path, true
		}
		for j, w := range left {
			if !modelValid(m, w.tx) {
				continue
			}
			next := modelClone(m)
			modelWrite(&next, w)
			tail := append(slices.Clone(left[:j]), left[j+1:]...)
			if result, ok := visit(next, tail, append(slices.Clone(path), w)); ok {
				return result, true
			}
		}
		return nil, false
	}
	return visit(emptyModel(), ws, nil)
}
func modelAt(ws []whole, round uint64) [2]map[string]Value {
	ordered := slices.Clone(ws)
	slices.SortFunc(ordered, func(a, b whole) int {
		if n := cmp.Compare(a.round, b.round); n != 0 {
			return n
		}
		return cmp.Compare(a.tx.ID, b.tx.ID)
	})
	m := emptyModel()
	for _, w := range ordered {
		if w.round <= round {
			modelWrite(&m, w)
		}
	}
	for _, rows := range m.rows {
		for k, v := range rows {
			if v.Deleted {
				delete(rows, k)
			}
		}
	}
	return m.rows
}
func compareOracle(t *testing.T, ms [2]*Machine, c Cut, history []whole, round uint64) {
	t.Helper()
	if _, ok := serialOrder(history); !ok {
		t.Fatal("committed history is nonserializable")
	}
	want := modelAt(history, round)
	for g, m := range ms {
		s, e := m.At(c)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(s.Values, want[g]) {
			t.Fatalf("whole transaction cut group%d: got%v want%v", g, s.Values, want[g])
		}
	}
}

func TestIndependentWholeTransactionOracleAndMutants(t *testing.T) {
	a, b := machine(t, 0), machine(t, 1)
	ms := [2]*Machine{a, b}
	edge := tx("edge", 1, participant(0, 0, Effect{Key: "edge/e", Value: 5}, Effect{Key: "out/e", Value: 5}), participant(1, 0, Effect{Key: "in/e", Value: 5}))
	r := reg(t, b, edge)
	av := prep(t, a, r, Proof{})
	bv := prep(t, b, r, av)
	vote(t, b, av)
	vote(t, b, bv)
	d := decide(t, b, r, true)
	history := []whole{{edge, d.view.Decision.Round}}
	resolve(t, a, d)
	// The independent whole-effect model detects half installation directly.
	broken := [2]map[string]Value{snapshot(a.state, mathMaxRound).Values, snapshot(b.state, mathMaxRound).Values}
	if reflect.DeepEqual(broken, modelAt(history, d.view.Decision.Round)) {
		t.Fatal("oracle missed half installation")
	}
	resolve(t, b, d)
	old := cut(t, d.view.Decision.Round, a, b)
	compareOracle(t, ms, old, history, d.view.Decision.Round)
	correction := tx("correction", 1, participant(0, 1, Effect{Key: "edge/e", Value: 6}, Effect{Key: "out/e", Delete: true}, Effect{Key: "out/new/e", Value: 6}), participant(1, 1, Effect{Key: "in/e", Delete: true}, Effect{Key: "in/new/e", Value: 6}))
	r = reg(t, b, correction)
	av = prep(t, a, r, Proof{})
	bv = prep(t, b, r, av)
	vote(t, b, av)
	vote(t, b, bv)
	d = decide(t, b, r, true)
	resolve(t, b, d)
	resolve(t, a, d)
	history = append(history, whole{correction, d.view.Decision.Round})
	fresh := cut(t, d.view.Decision.Round, a, b)
	compareOracle(t, ms, fresh, history, d.view.Decision.Round)
	compareOracle(t, ms, old, history, history[0].round)
	// Current-state substitution at an old cut must fail independently.
	current := [2]map[string]Value{snapshot(a.state, mathMaxRound).Values, snapshot(b.state, mathMaxRound).Values}
	if reflect.DeepEqual(current, modelAt(history, history[0].round)) {
		t.Fatal("oracle missed current substitution")
	}
	ms = [2]*Machine{checkpoint(t, a), checkpoint(t, b)}
	compareOracle(t, ms, old, history, history[0].round)
	// Mutants remain atomic yet are not serializable: both branches read the
	// original empty predicate / doctors state and independently mutate a partition.
	skewA := tx("skewA", 0, participant(0, 0, Effect{Key: "a", Value: 1}), participant(1, 0))
	skewB := tx("skewB", 1, participant(0, 0), participant(1, 0, Effect{Key: "b", Value: 1}))
	if _, ok := serialOrder([]whole{{skewA, 1}, {skewB, 2}}); ok {
		t.Fatal("serial oracle accepted atomic write skew")
	}
	// No-op restoration of a value does not restore revision identity.
	one := tx("one", 0, participant(0, 0, Effect{Key: "life", Value: 1}))
	two := tx("two", 0, participant(0, 1, Effect{Key: "life", Value: 2}))
	three := tx("three", 0, participant(0, 2, Effect{Key: "life", Value: 1}))
	p := participant(0, 3, Effect{Key: "dependent", Value: 1})
	p.Reads = []Read{{"life", 1}}
	stale := tx("stale", 0, p)
	if _, ok := serialOrder([]whole{{one, 1}, {two, 2}, {three, 3}, {stale, 4}}); ok {
		t.Fatal("serial oracle accepted version ABA")
	}
}
