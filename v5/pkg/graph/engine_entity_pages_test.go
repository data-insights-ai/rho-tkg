package graph

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// TestUnindexedScanWithHugeSeedPagesStoredEntities: a Scan with no index must
// cost the stored entities, not the ID range below the seed (snowflake IDs).
func TestUnindexedScanWithHugeSeedPagesStoredEntities(t *testing.T) {
	const seed = uint64(800_000_000_000_000_000)
	g, _ := openSeeded(t, seed)
	all := scopeAll(t)
	ids := []types.NodeID{7, types.NodeID(seed - 5), types.NodeID(seed)}
	var minted types.NodeID
	update(t, g, func(tx Tx) (err error) {
		for _, id := range ids {
			if _, err = tx.CreateNode(CreateNode{ID: id, Props: map[string]Value{"n": Int(1)}, Scope: all}); err != nil {
				return
			}
		}
		minted, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(2)}, Scope: all})
		return
	})
	want := append(slices.Clone(ids), minted)
	run := func(sel Selector, rows int) ([]types.NodeID, error) {
		type res struct {
			ids []types.NodeID
			err error
		}
		done := make(chan res, 1)
		go func() {
			s, err := g.OpenSnapshot(context.Background(), Fresh())
			if err != nil {
				done <- res{nil, err}
				return
			}
			defer s.Close()
			var out []types.NodeID
			lim := PageLimits{Rows: rows, MaxBytes: 1 << 20}
			p, err := s.Scan(context.Background(), ScanRequest{Selector: sel, Paging: lim})
			for guard := 0; err == nil && guard < 20; guard++ {
				for _, r := range p.Rows {
					out = append(out, r.Ref.Node())
				}
				if p.Next.IsZero() {
					break
				}
				p, err = s.Resume(context.Background(), p.Next, lim)
			}
			if err != nil {
				done <- res{nil, err}
				return
			}
			done <- res{out, nil}
		}()
		select {
		case r := <-done:
			return r.ids, r.err
		case <-time.After(10 * time.Second):
			t.Fatal("unindexed Scan walks the ID range: no answer in 10s")
			return nil, nil
		}
	}
	sel := Selector{View: ViewDeclared, Valid: all, Known: knownAll(t)}
	for _, rows := range []int{100, 2, 1} {
		got, err := run(sel, rows)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("rows=%d: got %v err=%v, want %v", rows, got, err, want)
		}
	}
}

// TestEndShortensEarlierLifeAfterReopen: End names its life by the point p and
// clips its tail at the next life's start, so it can shorten an earlier life
// while a later one stands (committed lives, after a store reopen, and lives
// made in the same Update).
func TestEndShortensEarlierLifeAfterReopen(t *testing.T) {
	g, cfg := openSeeded(t, 0)
	var a types.NodeID
	update(t, g, func(tx Tx) (err error) {
		a, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: spanMS(t, 0, 1000)})
		return
	})
	ra, _ := NodeRef(a)
	update(t, g, func(tx Tx) error { return tx.End(End{Target: ra, Scope: spanMS(t, 0, 100)}) })
	update(t, g, func(tx Tx) error {
		if err := tx.Reopen(Reopen{Target: ra, Scope: spanMS(t, 200, 300)}); err != nil {
			return err
		}
		return tx.Set(Set{Target: ra, Key: "n", Value: Int(2), Scope: spanMS(t, 200, 300)})
	})
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	all := scopeAll(t)
	// Life 1 ends at 50 although life 2 lies after it; life 2 is untouched.
	update(t, g2, func(tx Tx) error { return tx.End(End{Target: ra, Scope: spanMS(t, 0, 50)}) })
	got := rowsView(t, g2, ViewDeclared, []types.NodeID{a}, nil, all)
	if len(got) != 2 || !sameScope(t, got[0].Valid, spanMS(t, 0, 50)) || !sameScope(t, got[1].Valid, spanMS(t, 200, 300)) || got[1].Props["n"] != Int(2) {
		t.Fatalf("after End at 50: %d rows", len(got))
	}
	// An End inside life 2 shortens life 2 only.
	update(t, g2, func(tx Tx) error { return tx.End(End{Target: ra, Scope: spanMS(t, 200, 250)}) })
	got = rowsView(t, g2, ViewDeclared, []types.NodeID{a}, nil, all)
	if len(got) != 2 || !sameScope(t, got[0].Valid, spanMS(t, 0, 50)) || !sameScope(t, got[1].Valid, spanMS(t, 200, 250)) {
		t.Fatalf("after End at 250: %d rows", len(got))
	}
	// Same Update: a fresh node with two lives, End of the first after the Reopen.
	var b types.NodeID
	update(t, g2, func(tx Tx) (err error) {
		if b, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: spanMS(t, 0, 100)}); err != nil {
			return
		}
		rb, _ := NodeRef(b)
		if err = tx.Reopen(Reopen{Target: rb, Scope: spanMS(t, 200, 300)}); err != nil {
			return
		}
		return tx.End(End{Target: rb, Scope: spanMS(t, 0, 60)})
	})
	got = rowsView(t, g2, ViewDeclared, []types.NodeID{b}, nil, all)
	if len(got) != 2 || !sameScope(t, got[0].Valid, spanMS(t, 0, 60)) || !sameScope(t, got[1].Valid, spanMS(t, 200, 300)) {
		t.Fatalf("same-Update End: %d rows", len(got))
	}
}
