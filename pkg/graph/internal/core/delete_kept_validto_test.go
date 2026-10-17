package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// keptFix is one entity (a node, or a relationship between two nodes) valid
// from 1000 whose current version carries a scheduled close.
type keptFix struct {
	g   *Core
	rel bool
	nid types.NodeID
	rid types.RelID
}

func newKeptFix(t *testing.T, g *Core, rel bool) keptFix {
	t.Helper()
	ctx := context.Background()
	f := keptFix{g: g, rel: rel}
	props := map[string]any{"tkg_valid_from": int64(1000)}
	if rel {
		s, err := g.Nodes.Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		d, err := g.Nodes.Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add(ctx, "R", s, d, props)
		if err != nil {
			t.Fatal(err)
		}
		f.rid = r.ID()
		return f
	}
	n, err := g.Nodes.Add(ctx, []string{"A"}, props)
	if err != nil {
		t.Fatal(err)
	}
	f.nid = n.ID()
	return f
}

func (f keptFix) close(t *testing.T, at types.Instant) {
	t.Helper()
	var err error
	if f.rel {
		err = f.g.Rels.CloseVersion(context.Background(), f.rid, at)
	} else {
		err = f.g.Nodes.CloseVersion(context.Background(), f.nid, at)
	}
	if err != nil {
		t.Fatalf("CloseVersion(%d): %v", at, err)
	}
}

// at reports whether the entity answers at valid instant v as believed at pin.
func (f keptFix) at(v, pin types.Instant) bool {
	if f.rel {
		_, err := f.g.Temporal.RelAtTx(f.rid, v, pin)
		return err == nil
	}
	_, err := f.g.Temporal.NodeAtTx(f.nid, v, pin)
	return err == nil
}

// asOf is the as-of row's (ValidTo, DeletedAt) at pin.
func (f keptFix) asOf(t *testing.T, pin types.Instant) (types.Instant, types.Instant) {
	t.Helper()
	var tm *types.TemporalMetadata
	if f.rel {
		r, err := f.g.Temporal.RelAsOf(f.rid, pin)
		if err != nil {
			t.Fatalf("RelAsOf(%d): %v", pin, err)
		}
		tm = r.Temporal()
	} else {
		n, err := f.g.Temporal.NodeAsOf(f.nid, pin)
		if err != nil {
			t.Fatalf("NodeAsOf(%d): %v", pin, err)
		}
		tm = n.Temporal()
	}
	return tm.ValidTo, tm.DeletedAt
}

// answers is the point answer at every probe as believed at pin.
func (f keptFix) answers(pin types.Instant, probes ...types.Instant) string {
	out := ""
	for _, v := range probes {
		out += fmt.Sprintf("%d:%v ", v, f.at(v, pin))
	}
	return out
}

// Backlog 25: CloseVersion(far-1), a pin, then a plain Delete. The delete
// clamped the scheduled close to its own instant and the as-of rewind
// reopened it, so the pin before the delete answered [1000, inf) and the
// entity at far, where it was believed closed. The answers at the pin must not
// change; after the delete the entity answers only before the delete instant.
// Break cases: a close far ahead and one just past the delete (now+1), node
// and relationship.
func TestDeleteKeepsScheduledClose25(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(clock types.Instant) types.Instant
	}{
		{"far", func(c types.Instant) types.Instant { return c + 1_000_000 - 1 }},
		{"boundary now+1", func(c types.Instant) types.Instant { return c + 1 }},
	} {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", tc.name, rel), func(t *testing.T) {
				g, err := New(Config{})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = g.Close() }()
				wall := time.Now().Add(time.Second)
				g.SetClockForTest(t, func() time.Time { return wall })
				f := newKeptFix(t, g, rel)
				end := tc.close(g.peekNow() + 1)
				f.close(t, end)
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatal(err)
				}
				probes := []types.Instant{1000, end - 1, end, end + 1}
				before := f.answers(pin, probes...)
				if vt, _ := f.asOf(t, pin); vt != end {
					t.Fatalf("pre-delete AsOf ValidTo = %d, want %d", vt, end)
				}

				if rel {
					err = g.Rels.Delete(context.Background(), f.rid)
				} else {
					err = g.Nodes.Delete(context.Background(), f.nid)
				}
				if err != nil {
					t.Fatalf("Delete: %v", err)
				}

				if vt, da := f.asOf(t, pin); vt != end || da != 0 {
					t.Errorf("AsOf(pin) after the delete: ValidTo %d DeletedAt %d, want %d and 0", vt, da, end)
				}
				if after := f.answers(pin, probes...); after != before {
					t.Errorf("answers at the pre-delete pin changed:\nbefore %s\nafter  %s", before, after)
				}
				post, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatal(err)
				}
				if !f.at(1000, post) {
					t.Error("after the delete the valid past [1000, delete) no longer answers")
				}
				if f.at(end-1, post) && end-1 >= post {
					t.Errorf("after the delete the entity answers at %d, past the delete", end-1)
				}
				if f.at(post+1, post) {
					t.Errorf("after the delete the entity answers at %d", post+1)
				}
			})
		}
	}
}

// Backlog 7: DeleteWithTx(id, t) of a row whose recorded close lies at or
// after t was refused with ErrTxOrder, since one tombstone could not end
// belief at t and keep the close for pins before t. It now succeeds: a pin
// before t keeps the believed close, a pin at t sees validity end at
// min(close, t). Break cases: the close after t, exactly at t, node and
// relationship.
func TestDeleteWithTxKeepsScheduledClose7(t *testing.T) {
	for _, tc := range []struct {
		name string
		t    func(x, close types.Instant) types.Instant
	}{
		{"close after t", func(x, _ types.Instant) types.Instant { return x + 1 }},
		{"close at t", func(_, c types.Instant) types.Instant { return c }},
	} {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", tc.name, rel), func(t *testing.T) {
				g, err := New(Config{AllowTxBackfill: true})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = g.Close() }()
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				x := txbWall() + txbHour
				clk.at.Store(int64(x - 10))
				f := newKeptFix(t, g, rel)
				clk.at.Store(int64(x))
				end := x + 500
				f.close(t, end)
				clk.at.Store(int64(x + 1000))
				at := tc.t(x, end)

				if rel {
					err = g.Rels.DeleteWithTx(context.Background(), f.rid, at)
				} else {
					err = g.Nodes.DeleteWithTx(context.Background(), f.nid, at)
				}
				if err != nil {
					t.Fatalf("DeleteWithTx(%d): %v", at, err)
				}
				if vt, da := f.asOf(t, at-1); vt != end || da != 0 {
					t.Errorf("AsOf(t-1): ValidTo %d DeletedAt %d, want the believed close %d and 0", vt, da, end)
				}
				for _, v := range []types.Instant{1000, at - 1, at, end - 1, end} {
					if got, want := f.at(v, at-1), v < end; got != want {
						t.Errorf("AtTx(%d, t-1) = %v, want %v", v, got, want)
					}
					if got, want := f.at(v, at), v < min(end, at); got != want {
						t.Errorf("AtTx(%d, t) = %v, want %v", v, got, want)
					}
				}
			})
		}
	}
}
