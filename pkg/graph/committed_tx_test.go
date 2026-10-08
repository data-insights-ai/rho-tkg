package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// personValues reads the Person nodes at opts as "i" values, sorted.
func personValues(t *testing.T, g *graphpkg.Graph, opts graphpkg.QueryOpts) []int64 {
	t.Helper()
	nodes, err := g.Nodes().ByLabel("Person", opts)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]int64, 0, len(nodes))
	for _, n := range nodes {
		v, _ := n.GetProperty("i")
		out = append(out, v.(int64))
	}
	slices.Sort(out)
	return out
}

// While a transaction is open, CommittedTx is its start instant, and a read
// pinned there sees the committed state only: not the transaction's creates,
// its update, its delete or its relationship, which a read of the current
// state does see (writes are write-through). Two-phase: after Commit (and
// after Rollback) the read at that pin is unchanged; a new CommittedTx sees
// what committed.
func TestCommittedTxExcludesAnOpenTransaction(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		var base []types.NodeID
		for i := range 3 {
			n, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			base = append(base, n.ID())
		}
		if _, err := g.Rels().AddByID(ctx, "KNOWS", base[0], base[1], nil); err != nil {
			t.Fatal(err)
		}
		committed := []int64{0, 1, 2}
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("commit=%v", commit), func(t *testing.T) {
				tx, err := g.Tx().Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback() }() // a failure below must not leave it open (ErrTxDone after an end)
				if _, err := tx.AddNode([]string{"Person"}, map[string]any{"i": int64(10)}); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.UpdateNode(base[2], map[string]any{"i": int64(20)}); err != nil {
					t.Fatal(err)
				}
				extra, err := tx.AddNode([]string{"Person"}, map[string]any{"i": int64(11)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.AddRelationshipByID("KNOWS", base[1], extra.ID(), nil); err != nil {
					t.Fatal(err)
				}
				pin, err := g.Temporal().CommittedTx()
				if err != nil {
					t.Fatal(err)
				}
				if pin != tx.StartInstant() {
					t.Fatalf("CommittedTx = %d, the open transaction started at %d", pin, tx.StartInstant())
				}
				at := graphpkg.QueryOpts{TxPin: pin}
				if got := personValues(t, g, at); !slices.Equal(got, committed) {
					t.Fatalf("at the pin with the transaction open: %v, want %v", got, committed)
				}
				if got := personValues(t, g, graphpkg.QueryOpts{}); slices.Equal(got, committed) {
					t.Fatalf("the current state hides the open transaction's writes: %v", got)
				}
				if n, err := g.Rels().CountByTypeAt("KNOWS", at); err != nil || n != 1 {
					t.Fatalf("relationships at the pin = %d, %v; want 1", n, err)
				}
				if commit {
					err = tx.Commit()
				} else {
					err = tx.Rollback()
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := personValues(t, g, at); !slices.Equal(got, committed) {
					t.Fatalf("at the pin after the transaction ended: %v, want %v", got, committed)
				}
				after, err := g.Temporal().CommittedTx()
				if err != nil {
					t.Fatal(err)
				}
				if after <= pin {
					t.Fatalf("CommittedTx after the transaction %d <= %d", after, pin)
				}
				want := committed
				if commit {
					want = []int64{0, 1, 10, 11, 20}
				}
				if got := personValues(t, g, graphpkg.QueryOpts{TxPin: after}); !slices.Equal(got, want) {
					t.Fatalf("at a pin after the transaction: %v, want %v", got, want)
				}
				committed = want
			})
		}
	})
}

// A reader that pins every read at CommittedTx never sees part of a
// transaction while a writer commits and rolls back transactions of five
// nodes each: every count at a pin is a multiple of five, and a second read at
// the same pin gives the same count. (A read of the current state, or one
// pinned at NowTx, can see a transaction's first nodes before its last.)
func TestCommittedTxNeverSeesAPartialTransaction(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		const txs, per = 40, 5
		var wg sync.WaitGroup
		stop := make(chan struct{})  // closed by the writer when it is done
		abort := make(chan struct{}) // closed by the reader on a failure
		writerErr := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(stop)
			for k := range txs {
				select {
				case <-abort:
					writerErr <- nil
					return
				default:
				}
				tx, err := g.Tx().Begin()
				if err != nil {
					writerErr <- err
					return
				}
				for i := range per {
					if _, err := tx.AddNode([]string{"Person"}, map[string]any{"i": int64(k*per + i)}); err != nil {
						_ = tx.Rollback()
						writerErr <- err
						return
					}
				}
				if k%3 == 2 {
					err = tx.Rollback()
				} else {
					err = tx.Commit()
				}
				if err != nil {
					writerErr <- err
					return
				}
			}
			writerErr <- nil
		}()
		// read returns a failure message, or "" for a sound read.
		read := func(reads int) string {
			pin, err := g.Temporal().CommittedTx()
			if err != nil {
				return err.Error()
			}
			at := graphpkg.QueryOpts{TxPin: pin}
			n, err := g.Nodes().CountByLabelAt("Person", at)
			if err != nil {
				return err.Error()
			}
			if n%per != 0 {
				return fmt.Sprintf("read %d at pin %d saw %d nodes: part of a transaction", reads, pin, n)
			}
			nodes, err := g.Nodes().ByLabel("Person", at)
			if err != nil {
				return err.Error()
			}
			if len(nodes) != n {
				return fmt.Sprintf("read %d: a second read at pin %d saw %d nodes, the first %d", reads, pin, len(nodes), n)
			}
			return ""
		}
		failure := ""
		for reads, done := 0, false; !done && failure == ""; reads++ {
			select {
			case <-stop:
				done = true
			default:
			}
			failure = read(reads)
		}
		if failure != "" {
			close(abort)
		}
		wg.Wait()
		if err := <-writerErr; err != nil {
			t.Fatal(err)
		}
		if failure != "" {
			t.Fatal(failure)
		}
		pin, err := g.Temporal().CommittedTx()
		if err != nil {
			t.Fatal(err)
		}
		committedTxs := txs - txs/3
		if n, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{TxPin: pin}); err != nil || n != committedTxs*per {
			t.Fatalf("after the writer: %d, %v; want %d", n, err, committedTxs*per)
		}
	})
}

// CommittedTx on a closed graph fails; without an open transaction it is a
// fresh NowTx (strictly after an earlier NowTx).
func TestCommittedTxWithoutATransaction(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal().CommittedTx()
	if err != nil || pin <= now {
		t.Fatalf("CommittedTx = %d, %v; NowTx was %d", pin, err, now)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().CommittedTx(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed CommittedTx = %v", err)
	}
}
