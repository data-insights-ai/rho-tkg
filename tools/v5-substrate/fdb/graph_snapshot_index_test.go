package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Authored only. This direct API parent uses the genuine SDK fixture when the
// coordinated implementation checkpoint is later allowed to execute it.
func TestGraphSnapshotActualRetainedIndexScan(t *testing.T) {
	p := graphCostProfile{1, 20, 50, 1}
	h := fixture(t, graphCostConfig("index-direct-"+rand.Text(), p))
	t.Cleanup(func() {
		_, err := h.db.Transact(func(tr fdb.Transaction) (any, error) {
			for g := uint8(0); g < 2; g++ {
				tr.ClearRange(prefixRange(h.prefix(g)))
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
		}
	})
	initial := mustCut(t, h, []uint8{0, 1}, nil)
	source, err := newGraphHistoryRetention(h, initial, graphSelectorsPolicy(h.config).History)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(phase string, fact uint64) {
		t.Helper()
		if err := h.graphSelectorsApply(t.Context(), phase, fact, 0, func(graphCostParallelEvent) {}); err != nil {
			t.Fatal(err)
		}
	}
	for fact := uint64(0); fact < 3; fact++ {
		apply("setup", fact)
	}
	apply("create", 0)
	apply("create", 1)
	open := func() *graphSnapshot {
		t.Helper()
		r, err := source.OpenGraphSnapshot(t.Context(), graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}}, graphSelectorsPolicy(h.config).Read)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		})
		return r
	}
	old := open()
	apply("update", 0) // The unrelated second edge genuinely remains5.
	current := open()
	predicates := []graphIndexPredicate{{"out", 5}, {"out", 6}, {"out", 7}, {"in", 5}, {"in", 6}, {"in", 7}}
	check := func(r *graphSnapshot, corrected bool) {
		t.Helper()
		remaining := map[string]bool{}
		for fact := uint64(0); fact < 2; fact++ {
			value := int64(5)
			if corrected && fact == 0 {
				value = 6
			}
			edge := p.edge(fact, value)
			out, in := 0, 3
			if value == 6 {
				out, in = 1, 4
			}
			remaining[fmt.Sprintf("%d/0/%s", out, edgeKey("out", edge))] = true
			remaining[fmt.Sprintf("%d/%d/%s", in, edge.Target.Owner, edgeKey("in", edge))] = true
		}
		end, err := r.IndexScan(t.Context(), predicates, func(match graphIndexMatch) error {
			key := fmt.Sprintf("%d/%d/%s", match.Selector, match.Row.Group, match.Row.Key)
			if !remaining[key] || match.Row.Value.Value != predicates[match.Selector].Equals {
				return errMismatch
			}
			delete(remaining, key)
			return nil
		})
		var count uint64
		for _, groups := range end.Matches {
			count += groups[0] + groups[1]
		}
		if err != nil || len(remaining) != 0 || count != 4 || end.Selectors != 6 || end.Source.Cut.Round == 0 || end.Matches[2] != [2]uint64{} || end.Matches[5] != [2]uint64{} {
			t.Fatal("old/current/phantom exact index sets", end, remaining, err)
		}
	}
	check(old, false)
	check(current, true)
	stop := errors.New("index consumer stopped")
	if end, err := current.IndexScan(t.Context(), predicates, func(graphIndexMatch) error { return stop }); !errors.Is(err, stop) || end.Source.Cut.Round != 0 {
		t.Fatal("partial matching prefix published EOF", end, err)
	}
}

func TestGraphSnapshotIndexOwnedPredicateGuards(t *testing.T) {
	if _, err := (*graphSnapshot)(nil).IndexScan(t.Context(), nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, bad := range [][]graphIndexPredicate{nil, {{"edge", 5}}, {{"out", 5}, {"out", 5}}, make([]graphIndexPredicate, 9)} {
		if _, err := graphIndexPredicates(bad); !errors.Is(err, errInvalid) {
			t.Fatal("invalid index selector", bad, err)
		}
	}
	input := []graphIndexPredicate{{"out", 5}, {"in", 7}}
	owned, err := graphIndexPredicates(input)
	input[0].Equals = 6
	if err != nil || owned[0].Equals != 5 {
		t.Fatal("caller mutation changed owned predicate", owned, err)
	}
}
