package core

import (
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Consumer reports (sigma-tkgd, effective-timeline stream, against v4.44.1 /
// v4.45.0): the point door answers an entity's row beyond the end of its life —
// (1) beyond a CloseVersion end, (2) beyond the delete instant — at a pin after
// the close / delete, which breaks "the timeline is NodeAtTx pointwise". Each
// shape below writes T (valid from 1000) and a bystander B, then asserts, at a
// pin after the last write and without a pin, presence of T at every probe
// instant against the windows the shape's writes leave valid (written down from
// the writes, not read back). Catches: a resolver that drops the close's
// ValidTo on a chain with history (positional tiling from an older row), a
// delete that caps only the stamped row, a cascade over the close that reopens
// the closed tail, and a backend whose fast path (current-row shortcut,
// skeletons) answers past the end.

// lifeWindow is one half-open valid window [from, to) in which T must answer;
// to == 0 on a deleting shape means "up to the delete instant D".
type lifeWindow struct{ from, to types.Instant }

type lifeShape struct {
	name   string
	apply  func(e *ccEnt, id int64)
	want   []lifeWindow
	delete bool
}

func (e *ccEnt) mustClose(id int64, at types.Instant) {
	e.t.Helper()
	if err := e.closeAt(id, at); err != nil {
		e.t.Fatalf("%s CloseVersion(%d, %d): %v", e.kind(), id, at, err)
	}
}

func lifeCloseShapes() []lifeShape {
	return []lifeShape{
		{name: "close", apply: func(e *ccEnt, id int64) { e.mustClose(id, 4000) },
			want: []lifeWindow{{1000, 4000}}},
		{name: "update-then-close", apply: func(e *ccEnt, id int64) {
			e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(2)})
			e.mustClose(id, 4000)
		}, want: []lifeWindow{{1000, 4000}}},
		{name: "open-cascade-then-close", apply: func(e *ccEnt, id int64) {
			e.mustCascade(id, 2000, 0, map[string]any{"x": int64(1)})
			e.mustClose(id, 4000)
		}, want: []lifeWindow{{1000, 4000}}},
		{name: "bounded-cascade-then-close", apply: func(e *ccEnt, id int64) {
			e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
			e.mustClose(id, 4000)
		}, want: []lifeWindow{{1000, 4000}}},
		{name: "close-then-bounded-cascade-inside", apply: func(e *ccEnt, id int64) {
			e.mustClose(id, 4000)
			e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 4000}}},
		{name: "close-then-gap-piece", apply: func(e *ccEnt, id int64) {
			e.mustClose(id, 4000)
			e.mustCascade(id, 5000, 6000, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 4000}, {5000, 6000}}},
		{name: "close-then-cascade-across-the-end", apply: func(e *ccEnt, id int64) {
			e.mustClose(id, 4000)
			e.mustCascade(id, 3000, 5000, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 5000}}},
		{name: "update-with-end-then-bounded-cascade", apply: func(e *ccEnt, id int64) {
			e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "tkg_valid_to": types.Instant(4000), "x": int64(2)})
			e.mustCascade(id, 2500, 3000, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 4000}}},
		{name: "close-then-one-tick-at-the-end", apply: func(e *ccEnt, id int64) {
			e.mustClose(id, 4000)
			e.mustCascade(id, 4000, 4001, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 4001}}},
	}
}

func lifeDeleteShapes() []lifeShape {
	return []lifeShape{
		{name: "delete", apply: func(e *ccEnt, id int64) {}, want: []lifeWindow{{1000, 0}}, delete: true},
		{name: "update-then-delete", apply: func(e *ccEnt, id int64) {
			e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(2)})
		}, want: []lifeWindow{{1000, 0}}, delete: true},
		{name: "open-cascade-then-delete", apply: func(e *ccEnt, id int64) {
			e.mustCascade(id, 2000, 0, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 0}}, delete: true},
		{name: "bounded-cascade-then-delete", apply: func(e *ccEnt, id int64) {
			e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 0}}, delete: true},
		{name: "close-then-delete", apply: func(e *ccEnt, id int64) { e.mustClose(id, 4000) },
			want: []lifeWindow{{1000, 4000}}, delete: true},
		{name: "close-bounded-inside-then-delete", apply: func(e *ccEnt, id int64) {
			e.mustClose(id, 4000)
			e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 4000}}, delete: true},
		{name: "close-gap-then-delete", apply: func(e *ccEnt, id int64) {
			e.mustClose(id, 4000)
			e.mustCascade(id, 5000, 6000, map[string]any{"x": int64(1)})
		}, want: []lifeWindow{{1000, 4000}, {5000, 6000}}, delete: true},
	}
}

// lifeProbes are the valid instants probed for T: the windows' ends ±1, far
// beyond every write, and D-1, D, D+1 for a deleted T.
func lifeProbes(want []lifeWindow, d types.Instant) []types.Instant {
	ps := []types.Instant{1, 999, 1000, 1500, 2500, 3999, 4000, 4001, 4500, 5000, 5999, 6000, 6001, 1_000_000_000}
	for _, w := range want {
		ps = append(ps, w.from-1, w.from, w.to-1, w.to, w.to+1)
	}
	if d != 0 {
		ps = append(ps, d-1, d, d+1, d+1_000_000)
	}
	var out []types.Instant
	for _, p := range ps {
		if p > 0 {
			out = append(out, p)
		}
	}
	return out
}

func lifeIn(want []lifeWindow, d, t types.Instant) bool {
	for _, w := range want {
		to := w.to
		if to == 0 {
			to = d
		}
		if t >= w.from && (to == 0 || t < to) {
			return true
		}
	}
	return false
}

// atTxPresent reports whether T answers NodeAtTx / RelAtTx(t, pin); any error
// but ErrNoVersionValidAt fails.
func (e *ccEnt) atTxPresent(id int64, t, pin types.Instant) bool {
	e.t.Helper()
	var err error
	if e.rel {
		_, err = e.g.Temporal.RelAtTx(types.RelID(id), t, pin)
	} else {
		_, err = e.g.Temporal.NodeAtTx(types.NodeID(id), t, pin)
	}
	if errors.Is(err, storepkg.ErrNoVersionValidAt) {
		return false
	}
	if err != nil {
		e.t.Fatalf("%s AtTx(%d, %d, %d): %v", e.kind(), id, t, pin, err)
	}
	return true
}

// duringTxPresent reports whether the interval door NodesDuringTx /
// RelsDuringTx over the one-tick window [t, t+1) at pin lists T: the interval
// family must agree with the point door (rule 17).
func (e *ccEnt) duringTxPresent(id int64, t, pin types.Instant) bool {
	e.t.Helper()
	if e.rel {
		rs, err := e.g.Temporal.RelsDuringTx(t, t+1, pin)
		if err != nil {
			e.t.Fatalf("RelsDuringTx(%d, %d, %d): %v", t, t+1, pin, err)
		}
		for _, r := range rs {
			if int64(r.ID()) == id {
				return true
			}
		}
		return false
	}
	ns, err := e.g.Temporal.NodesDuringTx(t, t+1, pin)
	if err != nil {
		e.t.Fatalf("NodesDuringTx(%d, %d, %d): %v", t, t+1, pin, err)
	}
	for _, n := range ns {
		if int64(n.ID()) == id {
			return true
		}
	}
	return false
}

func runLifeShapes(t *testing.T, shapes []lifeShape, doors []ccDeleteDoor) {
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, sh := range shapes {
			for _, door := range doors {
				t.Run(sh.name+"/"+door.name, func(t *testing.T) {
					e := mk(t)
					b := e.add("B", 1000, nil)
					id := e.add("T", 1000, nil)
					sh.apply(e, id)
					var d types.Instant
					if sh.delete {
						if err := door.del(e, id); err != nil {
							t.Fatalf("delete via %s: %v", door.name, err)
						}
						for _, r := range e.chain(id) {
							d = max(d, r.tm.DeletedAt)
						}
						if d == 0 {
							t.Fatalf("no tombstone:%s", e.chainString(id))
						}
					}
					pin := e.pin()
					var bad []string
					for _, va := range lifeProbes(sh.want, d) {
						want := lifeIn(sh.want, d, va)
						if got := e.atTxPresent(id, va, pin); got != want {
							bad = append(bad, fmt.Sprintf("AtTx(valid %d, pin after): present=%v, want %v", va, got, want))
						}
						if got := e.atTxPresent(id, va, 0); got != want {
							bad = append(bad, fmt.Sprintf("At(valid %d): present=%v, want %v", va, got, want))
						}
						if got := e.duringTxPresent(id, va, pin); got != want {
							bad = append(bad, fmt.Sprintf("DuringTx([%d,%d), pin after): present=%v, want %v", va, va+1, got, want))
						}
						if !e.atTxPresent(b, max(va, 1000), pin) {
							bad = append(bad, fmt.Sprintf("bystander absent at %d", max(va, 1000)))
						}
					}
					if len(bad) > 0 {
						t.Fatalf("%s %s (D=%d):\n  %v\nchain:%s", e.kind(), sh.name, d, bad, e.chainString(id))
					}
				})
			}
		}
	})
}

// TestAtTxEndsAtClose — consumer report (1): after CloseVersion(id, 4000) the
// point door at a pin after the close answers nothing at or after 4000 (and
// exactly the pieces a later cascade asserts), every backend, node and rel.
func TestAtTxEndsAtClose(t *testing.T) {
	t.Parallel()
	runLifeShapes(t, lifeCloseShapes(), ccDeleteDoors()[:1])
}

// TestAtTxEndsAtDelete — consumer report (2): on a deleted chain the point
// door at a pin after the delete answers nothing at or after the delete
// instant D, through every delete door, every backend, node and rel.
func TestAtTxEndsAtDelete(t *testing.T) {
	t.Parallel()
	runLifeShapes(t, lifeDeleteShapes(), ccDeleteDoors())
}
