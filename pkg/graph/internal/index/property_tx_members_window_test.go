package index

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ledger row 62: the per-value valid-time window of the membership sidecar.

const inf types.Instant = math.MaxInt64

func windowIDs(t *testing.T, m *PropertyTxMembers[int64], vk string, a, b, pin types.Instant) []int64 {
	t.Helper()
	got, ok := m.MembersDuring(vk, a, b, pin)
	if !ok {
		m.OpenWindow(vk)
		if got, ok = m.MembersDuring(vk, a, b, pin); !ok {
			t.Fatalf("MembersDuring(%q) not ok after OpenWindow", vk)
		}
	}
	ids := make([]int64, 0, len(got))
	for _, mb := range got {
		ids = append(ids, mb.ID)
	}
	slices.Sort(ids)
	if len(slices.Compact(slices.Clone(ids))) != len(ids) {
		t.Fatalf("duplicate members %v", ids)
	}
	return ids
}

func row(v uint32, txFrom, txTo, from, to types.Instant) RowInfo {
	return RowInfo{Version: v, TxFrom: txFrom, TxTo: txTo, Span: Span{From: from, To: to}}
}

// TestPropertyTxMembersWindowBoundaries — half-open overlap at both ends,
// open rows, a = b and a > b (empty), an unknown value (empty, ok), an
// UnboundedValidFrom start, a nil sidecar, and a Record (unknown span)
// member that meets every window.
func TestPropertyTxMembersWindowBoundaries(t *testing.T) {
	t.Parallel()
	m := NewPropertyTxMembers[int64]()
	m.RecordRow("v", 1, row(1, 5, 0, 10, 20), true)
	m.RecordRow("v", 2, row(1, 5, 0, 20, inf), true)
	m.RecordRow("v", 3, row(1, 5, 0, types.UnboundedValidFrom, -100), false)
	m.RecordRow("w", 4, row(1, 5, 0, 10, 20), true)
	cases := []struct {
		a, b types.Instant
		want []int64
	}{
		{0, 10, nil},
		{0, 11, []int64{1}},
		{19, 20, []int64{1}},
		{20, 21, []int64{2}},
		{19, 21, []int64{1, 2}},
		{1 << 60, inf, []int64{2}},
		{types.UnboundedValidFrom, -100, []int64{3}},
		{-101, -100, []int64{3}},
		{-100, 10, nil},
		{15, 15, nil},
		{16, 15, nil},
		{types.UnboundedValidFrom, inf, []int64{1, 2, 3}},
	}
	for _, c := range cases {
		for _, pin := range []types.Instant{0, 4, 5, 1 << 40} {
			if got := windowIDs(t, m, "v", c.a, c.b, pin); !slices.Equal(got, c.want) {
				t.Errorf("[%d, %d) pin %d: got %v want %v", c.a, c.b, pin, got, c.want)
			}
		}
	}
	if got, ok := m.MembersDuring("nope", 0, inf, 0); !ok || len(got) != 0 {
		t.Errorf("unknown value: %v %v", got, ok)
	}
	m.Record("v", 9, 5) // unknown span
	if got := windowIDs(t, m, "v", -100, 10, 0); !slices.Equal(got, []int64{9}) {
		t.Errorf("wild member: got %v", got)
	}
	var nilM *PropertyTxMembers[int64]
	if got, ok := nilM.MembersDuring("v", 0, 1, 0); !ok || got != nil {
		t.Errorf("nil sidecar: %v %v", got, ok)
	}
}

// TestPropertyTxMembersWindowReplacedRowEnds — a row created open and
// replaced (RecordReplaced: its copy with TxTo T, the replacing row recorded
// at T, a higher version, starting no earlier) no longer meets a window after
// the replacing row's start at pins >= T, whatever value the replacing row
// carries, but still does at a pin before T. A successor recorded at another
// instant or an earlier-starting one leaves the row's own span, and so does a
// current row replaced without RecordReplaced (an in-place rewrite). The view
// opened before or after the writes answers the same.
func TestPropertyTxMembersWindowReplacedRowEnds(t *testing.T) {
	t.Parallel()
	late := func(m *PropertyTxMembers[int64], pin types.Instant) []int64 {
		return windowIDs(t, m, "v", 500, inf, pin)
	}
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprint("viewFirst=", early), func(t *testing.T) {
			m := NewPropertyTxMembers[int64]()
			if early {
				m.RecordRow("v", 99, row(1, 1, 0, 0, 1), false)
				m.OpenWindow("v")
			}
			// 1: replaced at 20 by a closed row starting at 110.
			m.RecordRow("v", 1, row(1, 10, 0, 100, inf), true)
			m.RecordReplaced("v", 1, row(1, 10, 20, 100, inf), row(2, 20, 0, 110, 150))
			m.RecordRow("v", 1, row(2, 20, 0, 110, 150), true)
			// 2: the successor is recorded at another instant: own span.
			m.RecordRow("v", 2, row(1, 10, 0, 100, inf), true)
			m.RecordReplaced("v", 2, row(1, 10, 20, 100, inf), row(2, 21, 0, 110, 150))
			m.RecordRow("v", 2, row(2, 21, 0, 110, 150), true)
			// 3: the successor starts earlier (a correction): own span.
			m.RecordRow("v", 3, row(1, 10, 0, 100, inf), true)
			m.RecordReplaced("v", 3, row(1, 10, 20, 100, inf), row(2, 20, 0, 90, 150))
			m.RecordRow("v", 3, row(2, 20, 0, 90, 150), true)
			// 4: CloseVersion: same start, closed: ends at its own start.
			m.RecordRow("v", 4, row(1, 10, 0, 100, inf), true)
			m.RecordReplaced("v", 4, row(1, 10, 20, 100, inf), row(2, 20, 0, 100, 150))
			m.RecordRow("v", 4, row(2, 20, 0, 100, 150), true)
			// 5: the replacement carries another value.
			m.RecordRow("v", 5, row(1, 10, 0, 100, inf), true)
			m.RecordReplaced("v", 5, row(1, 10, 20, 100, inf), row(2, 20, 0, 110, 150))
			m.RecordRow("w", 5, row(2, 20, 0, 110, 150), true)
			// 6: replaced in place (no RecordReplaced): the old span stays.
			m.RecordRow("v", 6, row(1, 10, 0, 100, inf), true)
			m.RecordRow("v", 6, row(2, 20, 0, 110, 150), true)
			if got := late(m, 0); !slices.Equal(got, []int64{2, 3, 6}) {
				t.Fatalf("no pin: %v want [2 3 6]", got)
			}
			if got := late(m, 20); !slices.Equal(got, []int64{2, 3, 6}) {
				t.Fatalf("pin 20: %v want [2 3 6]", got)
			}
			if got := late(m, 19); !slices.Equal(got, []int64{1, 2, 3, 4, 5, 6}) {
				t.Fatalf("pin 19 (before the replacement): %v", got)
			}
			if got := windowIDs(t, m, "v", 105, 106, 0); !slices.Equal(got, []int64{1, 2, 3, 4, 5, 6}) {
				t.Fatalf("history slot: %v", got)
			}
			got, _ := m.MembersDuring("v", 105, 106, 0)
			for _, mb := range got {
				if mb.FirstTx != 10 {
					t.Fatalf("first tx: %+v", mb)
				}
			}
		})
	}
}

// TestPropertyTxMembersWindowDelete — a delete records its tombstone (closed
// at the delete) then forgets the current row: at pins at or after the
// delete the member meets only the tombstone's span, before it the reopened
// row (ValidTo == DeletedAt reads open there).
func TestPropertyTxMembersWindowDelete(t *testing.T) {
	t.Parallel()
	m := NewPropertyTxMembers[int64]()
	m.RecordRow("v", 1, row(1, 10, 0, 100, inf), true)
	tomb := row(1, 10, 30, 100, 300)
	tomb.DeletedAt = 300
	m.RecordRow("v", 1, tomb, false)
	m.ForgetCurrent("v", 1)
	m.ForgetCurrent("v", 1)  // idempotent
	m.ForgetCurrent("v", 77) // unknown member
	m.ForgetCurrent("x", 1)  // unknown value
	if got := windowIDs(t, m, "v", 400, inf, 0); len(got) != 0 {
		t.Fatalf("after the delete: %v", got)
	}
	if got := windowIDs(t, m, "v", 400, inf, 299); !slices.Equal(got, []int64{1}) {
		t.Fatalf("pin before the delete: %v", got)
	}
	if got := windowIDs(t, m, "v", 200, 201, 0); !slices.Equal(got, []int64{1}) {
		t.Fatalf("inside the tombstone: %v", got)
	}
}

// TestPropertyTxMembersWindowEquivalence — random rows (replacing chains,
// unrelated history rows, deletes, wild records) on a few values with many
// changes per member (pending folds into the trees many times). A view
// opened before every write, one opened after, an image round trip
// (ForEachValueSpans / Restore) and a Merge into an empty sidecar answer
// every window at every pin alike; and every answer holds every member whose
// any-pin envelope (late envelope, at pins >= last) meets the window, and no
// other.
func TestPropertyTxMembersWindowEquivalence(t *testing.T) {
	t.Parallel()
	for seed := uint64(0); seed < 6; seed++ {
		rng := rand.New(rand.NewPCG(seed, 62))
		early := NewPropertyTxMembers[int64]()
		lateOpen := NewPropertyTxMembers[int64]()
		vks := []string{"a", "b", "c"}
		for _, vk := range vks {
			early.Record(vk, 1_000_000, 1) // a member so the view opens
			lateOpen.Record(vk, 1_000_000, 1)
			early.OpenWindow(vk)
		}
		type ent struct {
			ver  uint32
			tx   types.Instant
			span Span
			vk   string
		}
		live := map[int64]*ent{}
		tx := types.Instant(100)
		at := func() types.Instant { return types.Instant(rng.IntN(2000) - 500) }
		span := func(from types.Instant) Span {
			if rng.IntN(3) == 0 {
				return Span{From: from, To: inf}
			}
			return Span{From: from, To: from + 1 + types.Instant(rng.IntN(300))}
		}
		both := func(fn func(m *PropertyTxMembers[int64])) { fn(early); fn(lateOpen) }
		for step := 0; step < 4000; step++ {
			tx += types.Instant(1 + rng.IntN(3))
			id := int64(rng.IntN(300))
			e := live[id]
			switch r := rng.IntN(10); {
			case e == nil || r < 2: // create
				e = &ent{ver: 1, tx: tx, span: span(at()), vk: vks[rng.IntN(len(vks))]}
				live[id] = e
				ri := RowInfo{Version: e.ver, TxFrom: tx, Span: e.span}
				both(func(m *PropertyTxMembers[int64]) { m.RecordRow(e.vk, id, ri, true) })
			case r < 7: // replace: the copy ended by the next current row
				old := RowInfo{Version: e.ver, TxFrom: e.tx, TxTo: tx, Span: e.span}
				oldVK := e.vk
				e.ver++
				e.tx = tx
				e.span = span(e.span.From + types.Instant(rng.IntN(50)))
				if rng.IntN(4) == 0 {
					e.vk = vks[rng.IntN(len(vks))]
				}
				ri := RowInfo{Version: e.ver, TxFrom: tx, Span: e.span}
				both(func(m *PropertyTxMembers[int64]) {
					m.RecordReplaced(oldVK, id, old, ri)
					m.RecordRow(e.vk, id, ri, true)
				})
			case r < 8: // an unrelated history row (a correction)
				ri := RowInfo{Version: e.ver + 100, TxFrom: tx, Span: span(at())}
				both(func(m *PropertyTxMembers[int64]) { m.RecordRow(e.vk, id, ri, false) })
			case r < 9: // delete
				tomb := RowInfo{Version: e.ver, TxFrom: e.tx, TxTo: tx, DeletedAt: tx, Span: e.span}
				if tomb.Span.To == inf {
					tomb.Span.To = tx
				}
				both(func(m *PropertyTxMembers[int64]) { m.RecordRow(e.vk, id, tomb, false); m.ForgetCurrent(e.vk, id) })
				delete(live, id)
			default:
				if rng.IntN(20) == 0 {
					both(func(m *PropertyTxMembers[int64]) { m.Record(e.vk, id, tx) })
				}
			}
		}
		img := NewPropertyTxMembers[int64]()
		early.ForEachValueSpans(func(vk string, members []SpanMember[int64]) {
			for _, mb := range members {
				img.Restore(vk, mb)
			}
		})
		merged := NewPropertyTxMembers[int64]()
		merged.Merge(early)
		for _, vk := range vks {
			for i := 0; i < 200; i++ {
				a := at()
				b := a + types.Instant(rng.IntN(400))
				if rng.IntN(4) == 0 {
					b = inf
				}
				pin := []types.Instant{0, 150, tx / 2, tx - 5, tx}[rng.IntN(5)]
				want := windowIDs(t, early, vk, a, b, pin)
				for name, other := range map[string]*PropertyTxMembers[int64]{"opened late": lateOpen, "image": img, "merged": merged} {
					if got := windowIDs(t, other, vk, a, b, pin); !slices.Equal(got, want) {
						t.Fatalf("seed %d %s %s [%d,%d) pin %d:\n got  %v\n want %v", seed, name, vk, a, b, pin, got, want)
					}
				}
				// Exactly the members whose envelope for this pin meets.
				var brute []int64
				ps := early.values[vk]
				check := func(id int64, p txPosting) {
					env, ok := p.envLate()
					if pin != 0 && p.last > pin {
						env, ok = p.envAll()
					}
					if ok && a < b && env.meets(a, b) {
						brute = append(brute, id)
					}
				}
				check(ps.id, ps.p)
				for id, p := range ps.more {
					check(id, p)
				}
				slices.Sort(brute)
				if !slices.Equal(brute, want) {
					t.Fatalf("seed %d %s [%d,%d) pin %d: view %v brute %v", seed, vk, a, b, pin, want, brute)
				}
			}
		}
	}
}

// TestRowScanEndsReplacedRows — a build scan of the stored rows (each
// replaced row's copy in history, the current row) ends a replaced row at
// its replacing row like the write path; a replacing row without the value
// still ends it; rows past the search window keep their own span.
func TestRowScanEndsReplacedRows(t *testing.T) {
	t.Parallel()
	var s RowScan[int64]
	s.Add(1, row(1, 10, 20, 100, inf), false, []string{"v"})
	s.Add(1, row(2, 20, 0, 110, 150), true, []string{"v"})
	s.Add(2, row(1, 10, 20, 100, inf), false, []string{"v"})
	s.Add(2, row(2, 20, 0, 110, 150), true, nil) // the value changed
	s.Add(3, row(1, 10, 20, 100, inf), false, []string{"v"})
	for v := uint32(2); v < 2+replacingRowWindow+1; v++ {
		s.Add(3, row(v, 15, 15, 101, 102), false, nil) // unrelated rows first
	}
	s.Add(3, row(50, 20, 0, 110, 150), true, nil)
	m := NewPropertyTxMembers[int64]()
	s.Into(m)
	if got := windowIDs(t, m, "v", 500, inf, 0); !slices.Equal(got, []int64{3}) {
		t.Fatalf("late window: %v want [3]", got)
	}
	if got := windowIDs(t, m, "v", 500, inf, 19); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("pin before the replacement: %v", got)
	}
	if s.rows != nil || s.reps != nil {
		t.Fatal("Into keeps the rows")
	}
}

// TestPropertyTxMembersWindowMergeUnitesCurrent — Merge unites current spans
// (a build scan and a concurrent record commute) instead of replacing, and a
// united current row is never ended by a later replacing row.
func TestPropertyTxMembersWindowMergeUnitesCurrent(t *testing.T) {
	t.Parallel()
	m := NewPropertyTxMembers[int64]()
	m.RecordRow("v", 1, row(2, 5, 0, 100, 200), true)
	scan := NewPropertyTxMembers[int64]()
	scan.RecordRow("v", 1, row(1, 3, 0, 0, inf), true)
	m.Merge(scan)
	for _, w := range [][2]types.Instant{{10, 11}, {150, 151}, {500, 501}} {
		if got := windowIDs(t, m, "v", w[0], w[1], 0); !slices.Equal(got, []int64{1}) {
			t.Fatalf("[%d,%d): %v", w[0], w[1], got)
		}
	}
	m.RecordRow("v", 1, row(2, 5, 9, 100, 200), false)
	m.RecordRow("v", 1, row(3, 9, 0, 300, 400), true)
	if got := windowIDs(t, m, "v", 500, 501, 0); !slices.Equal(got, []int64{1}) {
		t.Fatalf("united current row ended: %v", got)
	}
	if got, _ := m.MembersDuring("v", 10, 11, 0); got[0].FirstTx != 3 {
		t.Fatalf("first tx %d", got[0].FirstTx)
	}
}
