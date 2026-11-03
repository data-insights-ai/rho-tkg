package core

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BATCHADD (ai-soc R3): GraphTx.AddNodes keeps its one batch store write when
// unique constraints exist, refuses the whole call on a violation (nothing
// stored, no claim left), and racing calls on one value leave one winner.

type addNodesUniqueBackend struct {
	name string
	open func(t *testing.T) *Core
}

// "Ref" is unique-capable on every backend (tiered: a reference label).
var addNodesUniqueBackends = []addNodesUniqueBackend{
	{"badger", newBadgerCore},
	{"tiered", newTieredTestCore},
}

func addNodesKeyed(prefix string, n int) []map[string]any {
	props := make([]map[string]any, n)
	for i := range props {
		props[i] = map[string]any{"k": fmt.Sprintf("%s%d", prefix, i)}
	}
	return props
}

// addNodesInTx runs AddNodes in its own transaction: committed on success,
// rolled back on an error. Never calls t.Fatal (it runs in goroutines and must
// not leave a transaction open); a refusal that returns nodes is an error.
func addNodesInTx(t *testing.T, g *Core, labels []string, props []map[string]any) error {
	t.Helper()
	tx, err := g.BeginTx()
	if err != nil {
		return err
	}
	nodes, err := tx.AddNodes(labels, props)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		if nodes != nil {
			return fmt.Errorf("AddNodes returned %d nodes with %v (not all or nothing)", len(nodes), err)
		}
		return err
	}
	if len(nodes) != len(props) {
		_ = tx.Rollback()
		return fmt.Errorf("%d nodes, want %d", len(nodes), len(props))
	}
	return tx.Commit()
}

func labelCount(t *testing.T, g *Core, label string) int {
	t.Helper()
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return 0
	}
	ns, err := g.store.NodesByLabel(tok, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	return len(ns)
}

// 1,000 constrained creates: one batch put, no single put, one lookup each.
// An unconstrained label (beside a constrained one in the registry) takes the
// batch with no lookup at all; a constrained label beside an unconstrained one
// in the same call is checked.
func TestAddNodesUniqueKeepsBatch(t *testing.T) {
	t.Parallel()
	for _, b := range addNodesUniqueBackends {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			seam := &nodePutCounts{}
			g.nodePutSeam = seam
			if err := g.Constraints.CreateUnique(context.Background(), "Ref", "k"); err != nil {
				t.Fatal(err)
			}
			check := func(what string, labels []string, props []map[string]any, lookups int64) {
				t.Helper()
				seam.single.Store(0)
				seam.batch.Store(0)
				seam.lookups.Store(0)
				if err := addNodesInTx(t, g, labels, props); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if s, bt, l := seam.single.Load(), seam.batch.Load(), seam.lookups.Load(); s != 0 || bt != 1 || l != lookups {
					t.Fatalf("%s: %d single puts, %d batch puts, %d lookups; want 0, 1, %d", what, s, bt, l, lookups)
				}
			}
			check("constrained", []string{"Ref"}, addNodesKeyed("a", 1000), 1000)
			check("unconstrained label", []string{"Plain"}, addNodesKeyed("a", 1000), 0)
			mixed := addNodesKeyed("m", 10)
			mixed[3] = map[string]any{"other": int64(1)} // binds no constrained value
			check("constrained beside unconstrained", []string{"Ref", "Extra"}, mixed, 9)
			if n := labelCount(t, g, "Ref"); n != 1010 {
				t.Fatalf("%d Ref nodes, want 1010", n)
			}
		})
	}
}

// An in-batch twin and a committed holder each refuse the whole call: no node
// stored, no UniqueForever claim left, so the same values succeed afterwards.
func TestAddNodesUniqueRefusesWholeCall(t *testing.T) {
	t.Parallel()
	for _, b := range addNodesUniqueBackends {
		for _, forever := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/forever=%v", b.name, forever), func(t *testing.T) {
				g := b.open(t)
				ctx := context.Background()
				create := g.Constraints.CreateUnique
				if forever {
					create = g.Constraints.CreateUniqueForever
				}
				if err := create(ctx, "Ref", "k"); err != nil {
					t.Fatal(err)
				}
				if err := addNodesInTx(t, g, []string{"Ref"}, []map[string]any{{"k": "held"}}); err != nil {
					t.Fatal(err)
				}
				owners := func() int {
					g.uniqueMu.RLock()
					defer g.uniqueMu.RUnlock()
					return len(g.uniqueOwners)
				}
				ownersBefore := owners()
				for _, tc := range []struct {
					name  string
					props []map[string]any
				}{
					{"in-batch twin", []map[string]any{{"k": "a"}, {"k": "b"}, {"k": "a"}}},
					{"committed holder", []map[string]any{{"k": "a"}, {"k": "b"}, {"k": "held"}}},
				} {
					err := addNodesInTx(t, g, []string{"Ref"}, tc.props)
					if !errors.Is(err, ErrUniqueViolation) {
						t.Fatalf("%s: err = %v, want ErrUniqueViolation", tc.name, err)
					}
					if n := labelCount(t, g, "Ref"); n != 1 {
						t.Fatalf("%s: %d Ref nodes, want 1", tc.name, n)
					}
					if o := owners(); o != ownersBefore {
						t.Fatalf("%s: %d forever owners, want %d (a claim left behind)", tc.name, o, ownersBefore)
					}
				}
				if err := addNodesInTx(t, g, []string{"Ref"}, []map[string]any{{"k": "a"}, {"k": "b"}}); err != nil {
					t.Fatalf("values of the refused calls: %v", err)
				}
			})
		}
	}
}

// failBatchBadger fails PutNodesBatch after writing the batch when fail is set,
// and counts persists of the UniqueForever owner registry.
type failBatchBadger struct {
	*badger.Store
	fail        atomic.Bool
	ownerWrites atomic.Int64
}

func (s *failBatchBadger) MetaSet(key string, value []byte) error {
	if key == uniqueForeverOwnersMeta {
		s.ownerWrites.Add(1)
	}
	return s.Store.MetaSet(key, value)
}

// NodesByLabelAndProperty is declared here so the graph accepts the wrapper's
// property index (an embedded native capability is refused otherwise).
func (s *failBatchBadger) NodesByLabelAndProperty(l uint16, k string, v any, o storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(l, k, v, o)
}

var errInjectedBatch = errors.New("injected batch failure")

func (s *failBatchBadger) PutNodesBatch(nodes []*types.Node) error {
	if err := s.Store.PutNodesBatch(nodes); err != nil {
		return err
	}
	if s.fail.Load() {
		return errInjectedBatch
	}
	return nil
}

// A failed batch store write withdraws the UniqueForever claims it made.
func TestAddNodesUniqueStoreFailureWithdrawsClaims(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	fs := &failBatchBadger{Store: bs}
	g := newTxTestGraphStore(t, fs)
	if err := g.Constraints.CreateUniqueForever(context.Background(), "Ref", "k"); err != nil {
		t.Fatal(err)
	}
	fs.fail.Store(true)
	if err := addNodesInTx(t, g, []string{"Ref"}, addNodesKeyed("f", 3)); !errors.Is(err, errInjectedBatch) {
		t.Fatalf("err = %v, want the injected error", err)
	}
	fs.fail.Store(false)
	g.uniqueMu.RLock()
	left := len(g.uniqueOwners)
	g.uniqueMu.RUnlock()
	if left != 0 {
		t.Fatalf("%d forever claims left by the failed write", left)
	}
	if n := labelCount(t, g, "Ref"); n != 0 {
		t.Fatalf("%d Ref nodes after the failed write", n)
	}
	if err := addNodesInTx(t, g, []string{"Ref"}, addNodesKeyed("f", 3)); err != nil {
		t.Fatalf("values of the failed call: %v", err)
	}
}

// A UniqueForever value whose owner no longer holds it (the owner deleted, or
// its value changed) is refused in AddNodes' read-only pass: no current row
// holds it, so only the owner registry refuses, and it must do so before any
// value of the call is claimed. A claim made and withdrawn is two registry
// persists, and a crash between them leaves the claim.
func TestAddNodesUniqueForeverOwnerGone(t *testing.T) {
	t.Parallel()
	for _, gone := range []string{"deleted", "value changed"} {
		t.Run(gone, func(t *testing.T) {
			ctx := context.Background()
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			fs := &failBatchBadger{Store: bs}
			g := newTxTestGraphStore(t, fs)
			if err := g.Constraints.CreateUniqueForever(ctx, "Ref", "k"); err != nil {
				t.Fatal(err)
			}
			owner, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "owned"})
			if err != nil {
				t.Fatal(err)
			}
			if gone == "deleted" {
				err = g.Nodes.Delete(ctx, owner.ID())
			} else {
				_, err = g.Nodes.Update(ctx, owner.ID(), map[string]any{"k": "other"})
			}
			if err != nil {
				t.Fatal(err)
			}
			before := fs.ownerWrites.Load()
			props := []map[string]any{{"k": "fresh0"}, {"k": "fresh1"}, {"k": "owned"}}
			if err := addNodesInTx(t, g, []string{"Ref"}, props); !errors.Is(err, ErrUniqueViolation) {
				t.Fatalf("reuse of an owned value = %v, want ErrUniqueViolation", err)
			}
			if w := fs.ownerWrites.Load() - before; w != 0 {
				t.Fatalf("the refused call persisted the owner registry %d times, want 0", w)
			}
			g.uniqueMu.RLock()
			_, fresh := g.uniqueOwners[foreverOwnerKey(mustLabel(t, g, "Ref"), "k", types.IndexablePropertyValueKey("fresh0"))]
			g.uniqueMu.RUnlock()
			if fresh {
				t.Fatal("the refused call left a claim on fresh0")
			}
			if err := addNodesInTx(t, g, []string{"Ref"}, props[:2]); err != nil {
				t.Fatalf("the call's other values: %v", err)
			}
		})
	}
}

func mustLabel(t *testing.T, g *Core, label string) uint16 {
	t.Helper()
	tok, ok := g.labels.Lookup(label)
	if !ok {
		t.Fatalf("label %q not registered", label)
	}
	return tok
}

// A unique key on a tiered EVENT label is kept by the store's owner index
// (UNIQIDX). AddNodes claims there only after every value passed: an in-call
// twin and a committed holder (created by AddNodes, so found only through its
// claim) refuse the call with no node stored and no owner claim made; a failed
// batch write stores nothing and its values stay free.
func TestAddNodesUniqueTieredEventLabel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := newTieredTestCore(t)
	seam := &nodePutCounts{}
	g.nodePutSeam = seam
	if err := g.Constraints.CreateUnique(ctx, "Ev", "k"); err != nil {
		t.Fatal(err)
	}
	tok := mustLabel(t, g, "Ev")
	oi, ok := g.ownerIndexFor(tok, "k")
	if !ok {
		t.Fatal("no owner index declared for the event label")
	}
	owned := func(v string) bool {
		t.Helper()
		_, ok, err := oi.UniqueOwner(tok, "k", types.CanonicalIndexKey(types.IndexablePropertyValueKey(v)))
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if err := addNodesInTx(t, g, []string{"Ev"}, []map[string]any{{"k": "held"}}); err != nil {
		t.Fatal(err)
	}
	if !owned("held") {
		t.Fatal("AddNodes made no owner claim for its value")
	}
	for _, tc := range []struct {
		name  string
		props []map[string]any
	}{
		{"in-batch twin", []map[string]any{{"k": "a"}, {"k": "b"}, {"k": "a"}}},
		{"committed holder", []map[string]any{{"k": "a"}, {"k": "b"}, {"k": "held"}}},
	} {
		if err := addNodesInTx(t, g, []string{"Ev"}, tc.props); !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("%s: err = %v, want ErrUniqueViolation", tc.name, err)
		}
		if n := labelCount(t, g, "Ev"); n != 1 {
			t.Fatalf("%s: %d Ev nodes, want 1", tc.name, n)
		}
		if owned("a") || owned("b") {
			t.Fatalf("%s: the refused call made owner claims", tc.name)
		}
	}
	seam.failBatch = errInjectedBatch
	err := addNodesInTx(t, g, []string{"Ev"}, []map[string]any{{"k": "a"}, {"k": "b"}})
	seam.failBatch = nil
	if !errors.Is(err, errInjectedBatch) {
		t.Fatalf("err = %v, want the injected error", err)
	}
	if n := labelCount(t, g, "Ev"); n != 1 {
		t.Fatalf("%d Ev nodes after the failed write, want 1", n)
	}
	if found, err := g.Nodes.ByLabelAndProperty("Ev", "k", "a", storepkg.QueryOpts{}); err != nil || len(found) != 0 {
		t.Fatalf("lookup of a failed write's value = %d nodes, %v; want none", len(found), err)
	}
	if err := addNodesInTx(t, g, []string{"Ev"}, []map[string]any{{"k": "a"}, {"k": "b"}}); err != nil {
		t.Fatalf("values of the failed call: %v", err)
	}
}

// AddNodes racing standalone creates of its unique value: exactly one wins.
// Transactions serialize with each other (BeginTx), but a transaction on the
// shared lock (memory) runs beside standalone writers, so only the value
// stripes, held from the first lookup to the batch write, keep one winner.
// The raced value is the call's first node, and the standalone writers start
// once its lookup is done, long before the batch write of all 500.
func TestAddNodesUniqueConcurrentOneWinner(t *testing.T) {
	t.Parallel()
	for _, b := range []addNodesUniqueBackend{
		{"memory", func(t *testing.T) *Core { return newTxTestGraphStore(t, memory.New()) }},
		{"badger", newBadgerCore},
	} {
		t.Run(b.name, func(t *testing.T) {
			rounds := 60
			if testing.Short() {
				rounds = 20
			}
			ctx := context.Background()
			for r := 0; r < rounds; r++ {
				g := b.open(t)
				if err := g.Constraints.CreateUnique(ctx, "Ref", "k"); err != nil {
					t.Fatal(err)
				}
				seam := &nodePutCounts{}
				g.nodePutSeam = seam
				const standalone = 4
				var wg sync.WaitGroup
				var wins atomic.Int32
				start := make(chan struct{})
				wg.Add(1 + standalone)
				go func() {
					defer wg.Done()
					props := addNodesKeyed("tx-", 500)
					props[0] = map[string]any{"k": "same"}
					<-start
					err := addNodesInTx(t, g, []string{"Ref"}, props)
					if err != nil && !errors.Is(err, ErrUniqueViolation) {
						t.Errorf("AddNodes: %v", err)
					}
					if err == nil {
						wins.Add(1)
					}
				}()
				for w := 0; w < standalone; w++ {
					go func(w int) {
						defer wg.Done()
						<-start
						// Start once AddNodes has looked the raced value up
						// (bounded wait: a path without the lookup seam).
						for deadline := time.Now().Add(2 * time.Second); seam.lookups.Load() < 2 && time.Now().Before(deadline); {
							runtime.Gosched()
						}
						_, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "same"})
						if err != nil && !errors.Is(err, ErrUniqueViolation) {
							t.Errorf("standalone %d: %v", w, err)
						}
						if err == nil {
							wins.Add(1)
						}
					}(w)
				}
				close(start)
				wg.Wait()
				if n := wins.Load(); n != 1 {
					t.Fatalf("round %d: %d winners, want 1", r, n)
				}
				_ = g.Close()
			}
		})
	}
}

func newTxTestGraphStore(t *testing.T, s storepkg.Store) *Core {
	t.Helper()
	g, err := New(Config{Store: s})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// BenchmarkAddNodesUnique: one AddNodes of 1,000 nodes on a label with a
// unique key, committed (BATCHADD, ai-soc R3).
func BenchmarkAddNodesUnique(b *testing.B) {
	for _, be := range []struct {
		name string
		open func() (storepkg.Store, error)
	}{
		{"badger", func() (storepkg.Store, error) { return badger.New(badger.Config{InMemory: true}) }},
		{"tiered", func() (storepkg.Store, error) {
			return tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
		}},
	} {
		b.Run(be.name, func(b *testing.B) {
			s, err := be.open()
			if err != nil {
				b.Fatal(err)
			}
			g, err := New(Config{Store: s})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			if err := g.Constraints.CreateUnique(context.Background(), "Ref", "k"); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			round := 0
			for b.Loop() {
				round++
				props := addNodesKeyed(fmt.Sprintf("r%d-", round), 1000)
				tx, err := g.BeginTx()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := tx.AddNodes([]string{"Ref"}, props); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
