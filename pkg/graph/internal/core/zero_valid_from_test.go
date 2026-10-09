package core

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestZeroValidFromIsUnsetInEveryDoor (sigma-tkgd question, 2026-10-09): is
// tkg_valid_from = 0 a legal explicit valid-from? The documented rule is
// "0 = unset; the effective start is the ID's mint instant" (pkg/types,
// storeutil.EntityValidFrom). An entity written with an explicit 0 through
// Add / AddByID, AddWithTx and Import must read exactly like one written
// without the key, in every door: absent before the mint instant (1, mint-1),
// present from it (mint, mint+1, far), pinned or not; the record doors
// (NodeAsOf, TxPin) list it at a pin after the write; the timeline is
// [mint, 0). SetVersionInterval with validFrom 0 is refused
// (ErrInvalidTimeRange). Catches a door that reads a stored 0 as "valid since
// the epoch" (eternal) while another derives the mint instant. Four backends,
// node and rel.
func TestZeroValidFromIsUnsetInEveryDoor(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		type made struct {
			name string
			id   int64
		}
		var ents []made
		zero := map[string]any{"tkg_valid_from": types.Instant(0), "x": int64(0)}
		for _, how := range []string{"add", "add-without-key", "addWithTx", "import"} {
			var id int64
			props := map[string]any{}
			for k, v := range zero {
				props[k] = v
			}
			if how == "add-without-key" {
				delete(props, "tkg_valid_from")
			}
			switch {
			case how == "addWithTx" && e.rel:
				s, _ := e.g.Nodes.Get(e.ctx, e.start)
				en, _ := e.g.Nodes.Get(e.ctx, e.end)
				r, err := e.g.Rels.AddWithTx(e.ctx, ccType, s, en, props, e.pin()-60_000)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				id = int64(r.ID())
			case how == "addWithTx":
				n, err := e.g.Nodes.AddWithTx(e.ctx, []string{ccLabel}, props, e.pin()-60_000)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				id = int64(n.ID())
			case how == "import" && e.rel:
				s, _ := e.g.Nodes.Get(e.ctx, e.start)
				en, _ := e.g.Nodes.Get(e.ctx, e.end)
				newID := types.RelID(e.add("tmp", 1000, nil) + 1<<20)
				r, err := e.g.Rels.Import(e.ctx, newID, ccType, s, en, props)
				if err != nil {
					t.Fatalf("Rels.Import: %v", err)
				}
				id = int64(r.ID())
			case how == "import":
				newID := types.NodeID(e.add("tmp", 1000, nil) + 1<<20)
				n, err := e.g.Nodes.Import(e.ctx, newID, []string{ccLabel}, props)
				if err != nil {
					t.Fatalf("Nodes.Import: %v", err)
				}
				id = int64(n.ID())
			default:
				id = e.addProps(props)
			}
			e.names[id] = how
			ents = append(ents, made{how, id})
		}
		pin := e.pin()
		for _, m := range ents {
			if err := e.cascade(m.id, 0, 5000, map[string]any{"x": int64(1)}); !errors.Is(err, ErrInvalidTimeRange) {
				t.Fatalf("%s: SetVersionInterval(0, 5000) = %v, want ErrInvalidTimeRange", m.name, err)
			}
			mint := e.mint(m.id)
			var bad []string
			for _, va := range []types.Instant{1, mint - 1, mint, mint + 1, mint + 1_000_000_000} {
				want := va >= mint
				for door, got := range e.zeroDoors(m.id, va, pin) {
					if got != want {
						bad = append(bad, fmt.Sprintf("%s(valid %d) present=%v, want %v", door, va, got, want))
					}
				}
			}
			for door, got := range e.zeroRecordDoors(m.id, pin) {
				if !got {
					bad = append(bad, door+" absent at a pin after the write")
				}
			}
			if got, want := e.mustTimeline(m.id, pin), fmt.Sprintf("[%d,0) v0", mint); got != want {
				bad = append(bad, fmt.Sprintf("timeline %q, want %q", got, want))
			}
			if len(bad) > 0 {
				t.Fatalf("%s %s (mint %d):\n  %s", e.kind(), m.name, mint, strings.Join(bad, "\n  "))
			}
		}
	})
}

// addProps creates a tracked entity with exactly props.
func (e *ccEnt) addProps(props map[string]any) int64 {
	e.t.Helper()
	if e.rel {
		r, err := e.g.Rels.AddByID(e.ctx, ccType, e.start, e.end, props)
		if err != nil {
			e.t.Fatalf("AddByID: %v", err)
		}
		return int64(r.ID())
	}
	n, err := e.g.Nodes.Add(e.ctx, []string{ccLabel}, props)
	if err != nil {
		e.t.Fatalf("Add: %v", err)
	}
	return int64(n.ID())
}

// zeroDoors: presence of id at valid instant va through every valid-time door,
// unpinned and pinned.
func (e *ccEnt) zeroDoors(id int64, va, pin types.Instant) map[string]bool {
	e.t.Helper()
	out := e.scanDoorsPresent(id, va, pin)
	out["AtTx(pin)"] = e.atTxPresent(id, va, pin)
	out["At"] = e.atTxPresent(id, va, 0)
	contains := func(ids []int64) bool {
		for _, x := range ids {
			if x == id {
				return true
			}
		}
		return false
	}
	if e.rel {
		rs, err := e.g.Rels.ByType(ccType, storepkg.QueryOpts{ValidAt: va})
		if err != nil {
			e.t.Fatalf("ByType: %v", err)
		}
		var ids []int64
		for _, r := range rs {
			ids = append(ids, int64(r.ID()))
		}
		out["ByType{ValidAt}"] = contains(ids)
		rs, err = e.g.Temporal.RelsAt(va)
		if err != nil {
			e.t.Fatalf("RelsAt: %v", err)
		}
		ids = ids[:0]
		for _, r := range rs {
			ids = append(ids, int64(r.ID()))
		}
		out["RelsAt"] = contains(ids)
		r, err := e.g.Rels.Get(e.ctx, types.RelID(id))
		if err != nil {
			e.t.Fatalf("Get: %v", err)
		}
		out["RelMatchesValidTime"] = e.g.Temporal.RelMatchesValidTime(r, storepkg.QueryOpts{ValidAt: va})
		return out
	}
	ns, err := e.g.Nodes.ByLabel(ccLabel, storepkg.QueryOpts{ValidAt: va})
	if err != nil {
		e.t.Fatalf("ByLabel: %v", err)
	}
	var ids []int64
	for _, n := range ns {
		ids = append(ids, int64(n.ID()))
	}
	out["ByLabel{ValidAt}"] = contains(ids)
	ns, err = e.g.Temporal.NodesAt(va)
	if err != nil {
		e.t.Fatalf("NodesAt: %v", err)
	}
	ids = ids[:0]
	for _, n := range ns {
		ids = append(ids, int64(n.ID()))
	}
	out["NodesAt"] = contains(ids)
	n, err := e.g.Nodes.Get(e.ctx, types.NodeID(id))
	if err != nil {
		e.t.Fatalf("Get: %v", err)
	}
	out["NodeMatchesValidTime"] = e.g.Temporal.NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: va})
	return out
}

// zeroRecordDoors: the record doors (no valid-time filter) at pin.
func (e *ccEnt) zeroRecordDoors(id int64, pin types.Instant) map[string]bool {
	e.t.Helper()
	out := map[string]bool{}
	if e.rel {
		_, err := e.g.Temporal.RelAsOf(types.RelID(id), pin)
		out["RelAsOf"] = err == nil
		rs, err := e.g.Rels.ByType(ccType, storepkg.QueryOpts{TxPin: pin})
		if err != nil {
			e.t.Fatalf("ByType TxPin: %v", err)
		}
		for _, r := range rs {
			out["ByType{TxPin}"] = out["ByType{TxPin}"] || int64(r.ID()) == id
		}
		return out
	}
	_, err := e.g.Temporal.NodeAsOf(types.NodeID(id), pin)
	out["NodeAsOf"] = err == nil
	ns, err := e.g.Nodes.ByLabel(ccLabel, storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		e.t.Fatalf("ByLabel TxPin: %v", err)
	}
	for _, n := range ns {
		out["ByLabel{TxPin}"] = out["ByLabel{TxPin}"] || int64(n.ID()) == id
	}
	return out
}
