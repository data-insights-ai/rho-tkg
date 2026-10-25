package core

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RESUME (sigma): a valid-time write that ends exactly where the row valid
// there starts needs no resumption row — the appended pieces cover nothing
// from that start on, so the rows covering any later instant are unchanged —
// yet a copy of that row was appended (one wasted row per rewritten step).

// w is the entity's "w" at valid instant at (-1: nothing valid).
func (e appendEntity) w(tb testing.TB, g *Core, at types.Instant) int64 {
	tb.Helper()
	var v any
	if e.rel {
		r, err := g.Temporal.RelAt(e.rid, at)
		if err != nil || r == nil {
			return -1
		}
		v, _ = r.GetProperty("w")
	} else {
		n, err := g.Temporal.NodeAt(e.nid, at)
		if err != nil || n == nil {
			return -1
		}
		v, _ = n.GetProperty("w")
	}
	i, _ := v.(int64)
	return i
}

// TestResume_NoCopyWhenWriteEndsAtRowStart: (a) a write [500, 1000) before
// a genesis [1000, open) and (b) a rewritten step [1000, 1100) before an
// appended [1100, open) each append only their piece, and every valid
// instant reads as before plus the piece. Catches the resumption copy of the
// row starting at the write's end (memory and badger, nodes and
// relationships).
func TestResume_NoCopyWhenWriteEndsAtRowStart(t *testing.T) {
	for _, be := range r10Backends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				g, err := New(be.cfg())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = g.Close() }()

				a := newAppendEntity(t, g, rel) // [1000, open) w=0
				if err := a.set(g, 500, 1000, 7); err != nil {
					t.Fatalf("(a) set: %v", err)
				}
				if got := a.rows(t, g); got != 2 {
					t.Fatalf("(a) rows = %d, want 2 (genesis + the piece, no resumption copy)", got)
				}
				for at, want := range map[types.Instant]int64{499: -1, 500: 7, 999: 7, 1000: 0, 5000: 0} {
					if got := a.w(t, g, at); got != want {
						t.Fatalf("(a) w at %d = %d, want %d", at, got, want)
					}
				}

				b := newAppendEntity(t, g, rel) // [1000, open) w=0
				if err := b.set(g, 1100, 0, 1); err != nil {
					t.Fatalf("(b) append: %v", err)
				}
				before := b.rows(t, g)
				if err := b.set(g, 1000, 1100, 5); err != nil {
					t.Fatalf("(b) rewrite: %v", err)
				}
				if got := b.rows(t, g); got != before+1 {
					t.Fatalf("(b) rows %d -> %d, want one more (the rewritten step only)", before, got)
				}
				for at, want := range map[types.Instant]int64{999: -1, 1000: 5, 1099: 5, 1100: 1, 5000: 1} {
					if got := b.w(t, g, at); got != want {
						t.Fatalf("(b) w at %d = %d, want %d", at, got, want)
					}
				}
			})
		}
	}
}
