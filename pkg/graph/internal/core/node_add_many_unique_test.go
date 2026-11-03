package core

import (
	"context"
	"errors"
	"fmt"
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

// failBatchBadger fails PutNodesBatch after writing the batch when fail is set.
type failBatchBadger struct {
	*badger.Store
	fail atomic.Bool
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

// Concurrent AddNodes on one unique value: exactly one call wins. On memory a
// transaction holds the shared lock, so only the value stripes serialize.
func TestAddNodesUniqueConcurrentOneWinner(t *testing.T) {
	t.Parallel()
	for _, b := range []addNodesUniqueBackend{
		{"memory", func(t *testing.T) *Core { return newTxTestGraphStore(t, memory.New()) }},
		{"badger", newBadgerCore},
	} {
		t.Run(b.name, func(t *testing.T) {
			rounds := 100
			if testing.Short() {
				rounds = 30
			}
			for r := 0; r < rounds; r++ {
				g := b.open(t)
				if err := g.Constraints.CreateUnique(context.Background(), "Ref", "k"); err != nil {
					t.Fatal(err)
				}
				const writers = 6
				var wg sync.WaitGroup
				var mu sync.Mutex
				wins := 0
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						props := addNodesKeyed(fmt.Sprintf("w%d-", w), 20)
						props[w%len(props)] = map[string]any{"k": "same"}
						err := addNodesInTx(t, g, []string{"Ref"}, props)
						if err != nil && !errors.Is(err, ErrUniqueViolation) {
							t.Errorf("writer %d: %v", w, err)
							return
						}
						if err == nil {
							mu.Lock()
							wins++
							mu.Unlock()
						}
					}(w)
				}
				wg.Wait()
				if wins != 1 {
					t.Fatalf("round %d: %d winners, want 1", r, wins)
				}
				if n := labelCount(t, g, "Ref"); n != 20 {
					t.Fatalf("round %d: %d Ref nodes, want 20", r, n)
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
