package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 42: LatestStamps reads the current-row half through
// store.CurrentStampsCapability (the current row's stamps without the full
// row: badger decodes only the temporal block on an entity-cache miss). The
// public answer must stay byte-identical to the implementation it replaces —
// the lent (or fetched) current row's stamps folded with the history sidecar —
// for every input.

// oldNodeLatestStamps is LatestStamps as v4.49.0 computed it, over the store
// directly: the current row read through LendNode (GetNode where the store
// does not lend), its stamps folded with NodeHistoryStamps.
func oldNodeLatestStamps(st storepkg.Store, id types.NodeID) (types.Instant, types.Instant, bool, error) {
	var cur *types.Node
	var err error
	if l, ok := st.(storepkg.EntityLendCapability); ok {
		cur, err = l.LendNode(id)
	} else {
		cur, err = st.GetNode(id)
	}
	live := err == nil
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return 0, 0, false, err
	}
	var from, to types.Instant
	if live {
		f, t, d := cur.TxStamps()
		from, to = storepkg.FoldTxStamps(0, 0, f, t, d)
	}
	hf, ht, has, err := st.(storepkg.HistoryStampsCapability).NodeHistoryStamps(id)
	if err != nil {
		return 0, 0, false, err
	}
	if !live && !has {
		return 0, 0, false, storepkg.ErrNodeNotFound
	}
	from, to = storepkg.FoldTxStamps(from, to, hf, ht, 0)
	return from, to, !live, nil
}

// oldRelLatestStamps is oldNodeLatestStamps for relationships.
func oldRelLatestStamps(st storepkg.Store, id types.RelID) (types.Instant, types.Instant, bool, error) {
	var cur *types.Relationship
	var err error
	if l, ok := st.(storepkg.EntityLendCapability); ok {
		cur, err = l.LendRelationship(id)
	} else {
		cur, err = st.GetRelationship(id)
	}
	live := err == nil
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return 0, 0, false, err
	}
	var from, to types.Instant
	if live {
		f, t, d := cur.TxStamps()
		from, to = storepkg.FoldTxStamps(0, 0, f, t, d)
	}
	hf, ht, has, err := st.(storepkg.HistoryStampsCapability).RelHistoryStamps(id)
	if err != nil {
		return 0, 0, false, err
	}
	if !live && !has {
		return 0, 0, false, storepkg.ErrRelNotFound
	}
	from, to = storepkg.FoldTxStamps(from, to, hf, ht, 0)
	return from, to, !live, nil
}

// sameAnswer compares two LatestStamps answers: equal values, and on an error
// the same sentinel class.
func sameAnswer(f1, t1 types.Instant, d1 bool, e1 error, f2, t2 types.Instant, d2 bool, e2 error) bool {
	if (e1 == nil) != (e2 == nil) {
		return false
	}
	if e1 != nil {
		return errors.Is(e1, storepkg.ErrNodeNotFound) == errors.Is(e2, storepkg.ErrNodeNotFound) &&
			errors.Is(e1, storepkg.ErrRelNotFound) == errors.Is(e2, storepkg.ErrRelNotFound) &&
			errors.Is(e1, storepkg.ErrInvalidStoreMutation) == errors.Is(e2, storepkg.ErrInvalidStoreMutation)
	}
	return f1 == f2 && t1 == t2 && d1 == d2
}

// assertCurrentStampsAgree checks, for every ID, the door against the old
// implementation (byte-identical), and the store capability against the
// current row the Get door returns (or its not-found sentinel).
func assertCurrentStampsAgree(t *testing.T, step string, g *graphpkg.Graph, st storepkg.Store, nodes []types.NodeID, rels []types.RelID) {
	t.Helper()
	caps, ok := st.(storepkg.CurrentStampsCapability)
	if !ok {
		t.Fatalf("%s: store %T lacks CurrentStampsCapability", step, st)
	}
	for _, id := range nodes {
		f, to, d, err := g.Nodes().LatestStamps(id)
		of, oto, od, oerr := oldNodeLatestStamps(st, id)
		if !sameAnswer(f, to, d, err, of, oto, od, oerr) {
			t.Fatalf("%s: node %d LatestStamps = (%d, %d, %v, %v), the v4.49.0 implementation answers (%d, %d, %v, %v)", step, id, f, to, d, err, of, oto, od, oerr)
		}
		got, cerr := caps.NodeCurrentStamps(id)
		row, gerr := st.GetNode(id)
		switch {
		case gerr != nil && errors.Is(gerr, storepkg.ErrNodeNotFound):
			if !errors.Is(cerr, storepkg.ErrNodeNotFound) {
				t.Fatalf("%s: node %d has no current row: NodeCurrentStamps = (%+v, %v)", step, id, got, cerr)
			}
		case gerr != nil:
			if cerr == nil {
				t.Fatalf("%s: node %d: GetNode %v, NodeCurrentStamps succeeded with %+v", step, id, gerr, got)
			}
		case cerr != nil || got != storepkg.NodeRowStamps(row):
			t.Fatalf("%s: node %d NodeCurrentStamps = (%+v, %v), the current row is %+v", step, id, got, cerr, storepkg.NodeRowStamps(row))
		}
	}
	for _, id := range rels {
		f, to, d, err := g.Rels().LatestStamps(id)
		of, oto, od, oerr := oldRelLatestStamps(st, id)
		if !sameAnswer(f, to, d, err, of, oto, od, oerr) {
			t.Fatalf("%s: rel %d LatestStamps = (%d, %d, %v, %v), the v4.49.0 implementation answers (%d, %d, %v, %v)", step, id, f, to, d, err, of, oto, od, oerr)
		}
		got, cerr := caps.RelCurrentStamps(id)
		row, gerr := st.GetRelationship(id)
		switch {
		case gerr != nil && errors.Is(gerr, storepkg.ErrRelNotFound):
			if !errors.Is(cerr, storepkg.ErrRelNotFound) {
				t.Fatalf("%s: rel %d has no current row: RelCurrentStamps = (%+v, %v)", step, id, got, cerr)
			}
		case gerr != nil:
			if cerr == nil {
				t.Fatalf("%s: rel %d: GetRelationship %v, RelCurrentStamps succeeded with %+v", step, id, gerr, got)
			}
		case cerr != nil || got != storepkg.RelRowStamps(row):
			t.Fatalf("%s: rel %d RelCurrentStamps = (%+v, %v), the current row is %+v", step, id, got, cerr, storepkg.RelRowStamps(row))
		}
	}
}

// currentStampsBackends is latestStampsBackends plus badger, tiered and
// sharded with a one-entry entity cache: after every flush the current rows
// are evicted, so the capability answers from the stored temporal block (the
// cache-miss path) while the other backends answer from the cache.
func currentStampsBackends() []hasHistoryBackend {
	return append(latestStampsBackends(),
		hasHistoryBackend{name: "badger-tinycache-disk", reopen: true, open: func(t *testing.T, dir string) (storepkg.Store, func() error) {
			st, err := badger.New(badger.Config{Dir: dir, FlushInterval: time.Hour, CacheCapacity: 1})
			if err != nil {
				t.Fatal(err)
			}
			return st, st.Flush
		}},
		hasHistoryBackend{name: "tiered-tinycache-disk", reopen: true, tieredRf: true, open: func(t *testing.T, dir string) (storepkg.Store, func() error) {
			st, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: time.Hour, CacheCapacity: 1})
			if err != nil {
				t.Fatal(err)
			}
			return st, st.Flush
		}},
		hasHistoryBackend{name: "sharded-tinycache-disk", reopen: true, open: func(t *testing.T, dir string) (storepkg.Store, func() error) {
			st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2, CacheCapacity: 1})
			if err != nil {
				t.Fatal(err)
			}
			return st, st.Flush
		}},
	)
}

// currentStampsSeeds is the seed count of TestCurrentStampsDifferential:
// CURRENT_STAMPS_SEEDS, else 48 (24 in -short, at 100 steps instead of 300).
func currentStampsSeeds() int {
	if s := os.Getenv("CURRENT_STAMPS_SEEDS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	if testing.Short() {
		return 24
	}
	return 48
}

// TestCurrentStampsDifferential runs the LatestStamps differential's
// randomized write sequence (create, AddWithTx backfill, update, UpdateInPlace,
// UpdateWithTx, cascade, close, label, delete, DeleteWithTx, re-import of a
// deleted ID, GraphTx commit and rollback, compaction, purge, erasure, Clear,
// flush, reopen) for CURRENT_STAMPS_SEEDS seeds x 300 steps on every backend,
// the one-entry-cache variants included, and after EVERY step compares the
// door with the v4.49.0 implementation and the capability with the current row
// (assertCurrentStampsAgree, called from runLatestStampsDifferential). Faults
// it names: the door answering the history half only, the capability stale
// after a plain ReplaceNode (UpdateInPlace, rollback restore), a deleted
// current row answered with stamps, zero stamps from a stored row form, a
// cache miss that disagrees with the hit, a router that asks the wrong shard.
func TestCurrentStampsDifferential(t *testing.T) {
	steps := 300
	if testing.Short() {
		steps = 100
	}
	seeds := currentStampsSeeds()
	for _, b := range currentStampsBackends() {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			total := map[string]int{}
			cov := stampsCoverage{}
			t.Run("seeds", func(t *testing.T) {
				for seed := int64(1); seed <= int64(seeds); seed++ {
					t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
						t.Parallel()
						local := &stampsCoverage{}
						ok := runLatestStampsDifferential(t, b, 1000+seed, steps, local)
						mu.Lock()
						defer mu.Unlock()
						for k, v := range ok {
							total[k] += v
						}
						cov.aboveHead += local.aboveHead
						cov.deleted += local.deleted
						cov.revived += local.revived
						cov.histTo += local.histTo
					})
				}
			})
			if t.Failed() {
				return
			}
			for _, must := range []string{"updateNode", "updateRel", "cascadeNode", "deleteNode", "deleteRel", "reimport", "txRollback", "updateWithTx", "deleteWithTx", "addNodeWithTx", "updateInPlace"} {
				if total[must] == 0 {
					t.Errorf("%s: op %s never succeeded over %d seeds", b.name, must, seeds)
				}
			}
			if cov.aboveHead == 0 || cov.deleted == 0 || cov.revived == 0 {
				t.Errorf("%s: coverage %+v: a stamp above the head, a deleted entity or a revived ID never reached", b.name, cov)
			}
		})
	}
}

// currentStampsCounting declares its own capability methods (a third-party
// store's implementation): the core must use it, not fall back.
type currentStampsCounting struct {
	*memory.Store
	calls atomic.Int64
}

func (s *currentStampsCounting) NodeCurrentStamps(id types.NodeID) (storepkg.RowStamps, error) {
	s.calls.Add(1)
	return s.Store.NodeCurrentStamps(id)
}

func (s *currentStampsCounting) RelCurrentStamps(id types.RelID) (storepkg.RowStamps, error) {
	s.calls.Add(1)
	return s.Store.RelCurrentStamps(id)
}

// currentStampsGetOverride embeds a native store, so the capability methods
// are only PROMOTED, and overrides the current-row reads (fault injection, a
// policy check): the core must not route around the override through the
// promoted capability.
type currentStampsGetOverride struct {
	*memory.Store
	failNode atomic.Int64
	failRel  atomic.Int64
}

var errInjectedGet = errors.New("injected current-row read failure")

func (s *currentStampsGetOverride) GetNode(id types.NodeID) (*types.Node, error) {
	if int64(id) == s.failNode.Load() {
		return nil, errInjectedGet
	}
	return s.Store.GetNode(id)
}

func (s *currentStampsGetOverride) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if int64(id) == s.failRel.Load() {
		return nil, errInjectedGet
	}
	return s.Store.GetRelationship(id)
}

// TestCurrentStampsCapabilityAdmission: a store without the capability (the
// wrapper hides every optional capability) answers through the current-row
// read with the same values; a wrapper that only inherits the capability from
// an embedded native store keeps its GetNode / GetRelationship override
// visible (an injected failure reaches the caller, errors.Is); a store that
// declares the methods itself is asked (its calls are counted) and answers the
// oracle.
func TestCurrentStampsCapabilityAdmission(t *testing.T) {
	ctx := context.Background()
	scenario := func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.RelID) {
		t.Helper()
		a, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		z, err := g.Nodes().Add(ctx, []string{"Event"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().AddByID(ctx, "LINK", a.ID(), z.ID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().UpdateInPlace(ctx, a.ID(), map[string]any{"v": int64(2)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(3)}); err != nil {
			t.Fatal(err)
		}
		return a.ID(), r.ID()
	}
	agree := func(t *testing.T, g *graphpkg.Graph, nid types.NodeID, rid types.RelID) {
		t.Helper()
		want, _, err := nodeStampsOracle(g, nid)
		if err != nil {
			t.Fatal(err)
		}
		if f, to, d, err := g.Nodes().LatestStamps(nid); err != nil || f != want.from || to != want.to || d != want.deleted {
			t.Fatalf("node = (%d, %d, %v, %v), want %+v", f, to, d, err, want)
		}
		rwant, _, err := relStampsOracle(g, rid)
		if err != nil {
			t.Fatal(err)
		}
		if f, to, d, err := g.Rels().LatestStamps(rid); err != nil || f != rwant.from || to != rwant.to || d != rwant.deleted {
			t.Fatalf("rel = (%d, %d, %v, %v), want %+v", f, to, d, err, rwant)
		}
	}
	t.Run("without the capability", func(t *testing.T) {
		st := historyOnlyStore{memory.New()}
		if _, ok := any(st).(storepkg.CurrentStampsCapability); ok {
			t.Fatal("the wrapper must hide the capability")
		}
		g, err := graphpkg.New(graphpkg.Config{Store: st})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		nid, rid := scenario(t, g)
		agree(t, g, nid, rid)
	})
	t.Run("inherited capability keeps the Get override visible", func(t *testing.T) {
		st := &currentStampsGetOverride{Store: memory.New()}
		if _, ok := any(st).(storepkg.CurrentStampsCapability); !ok {
			t.Fatal("setup: the embedded store must promote the capability")
		}
		g, err := graphpkg.New(graphpkg.Config{Store: st})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		nid, rid := scenario(t, g)
		agree(t, g, nid, rid)
		st.failNode.Store(int64(nid))
		if _, _, _, err := g.Nodes().LatestStamps(nid); !errors.Is(err, errInjectedGet) {
			t.Fatalf("node with a failing GetNode override = %v, want the injected error", err)
		}
		st.failRel.Store(int64(rid))
		if _, _, _, err := g.Rels().LatestStamps(rid); !errors.Is(err, errInjectedGet) {
			t.Fatalf("rel with a failing GetRelationship override = %v, want the injected error", err)
		}
	})
	t.Run("declared capability is used", func(t *testing.T) {
		st := &currentStampsCounting{Store: memory.New()}
		g, err := graphpkg.New(graphpkg.Config{Store: st})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		nid, rid := scenario(t, g)
		before := st.calls.Load()
		agree(t, g, nid, rid)
		if st.calls.Load()-before < 2 {
			t.Fatalf("LatestStamps asked the declared capability %d times, want one per call", st.calls.Load()-before)
		}
	})
}

// TestCurrentStampsConcurrentWriters (-race): writers update, cascade and
// UpdateInPlace a set of entities while readers call LatestStamps on every
// backend, with a small entity cache and a fast flush on badger, so current
// rows are read from the cache, from the write buffer and from badger while
// they change. A reader's answer for one ID never goes down (the writes only
// add rows and every UpdateInPlace keeps the TxFrom), and after the writers stop
// every answer equals the oracle.
func TestCurrentStampsConcurrentWriters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(t *testing.T) storepkg.Store
	}{
		{"memory", func(*testing.T) storepkg.Store { return memory.New() }},
		{"badger", func(t *testing.T) storepkg.Store {
			st, err := badger.New(badger.Config{InMemory: true, FlushInterval: 2 * time.Millisecond, CacheCapacity: 4})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
		{"tiered", func(t *testing.T) storepkg.Store {
			st, err := tiered.New(tiered.Config{InMemory: true, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 2 * time.Millisecond, CacheCapacity: 4})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
		{"sharded", func(t *testing.T) storepkg.Store {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, CacheCapacity: 4})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			g, err := graphpkg.New(graphpkg.Config{Store: tc.store(t)})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			var nodes []types.NodeID
			var rels []types.RelID
			for i := range 12 {
				n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n.ID())
				if i > 0 {
					r, err := g.Rels().AddByID(ctx, "LINK", nodes[i-1], n.ID(), nil)
					if err != nil {
						t.Fatal(err)
					}
					rels = append(rels, r.ID())
				}
			}
			var stop atomic.Bool
			var wg sync.WaitGroup
			for w := range 4 {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(w))) // #nosec G404 -- deterministic test load
					for i := 0; i < 120; i++ {
						switch r.Intn(5) {
						case 0:
							_, _ = g.Nodes().Update(ctx, nodes[r.Intn(len(nodes))], map[string]any{"v": int64(i)})
						case 1:
							_, _ = g.Rels().Update(ctx, rels[r.Intn(len(rels))], map[string]any{"w": int64(i)})
						case 2:
							now, _ := g.Temporal().NowTx()
							_, _ = g.Temporal().SetNodeVersionInterval(ctx, nodes[r.Intn(len(nodes))], now+10, now+20, map[string]any{"v": int64(-i)})
						case 3:
							_, _ = g.Nodes().UpdateInPlace(ctx, nodes[r.Intn(len(nodes))], map[string]any{"v": int64(i)})
						default:
							now, _ := g.Temporal().NowTx()
							_, _ = g.Temporal().SetRelVersionInterval(ctx, rels[r.Intn(len(rels))], now+10, now+20, map[string]any{"w": int64(-i)})
						}
					}
				}(w)
			}
			errs := make(chan error, 8)
			var readers sync.WaitGroup
			for range 3 {
				readers.Add(1)
				go func() {
					defer readers.Done()
					lastN := make([][2]types.Instant, len(nodes))
					lastR := make([][2]types.Instant, len(rels))
					for !stop.Load() {
						for i, id := range nodes {
							from, to, deleted, err := g.Nodes().LatestStamps(id)
							if err != nil || deleted || from < lastN[i][0] || to < lastN[i][1] {
								errs <- fmt.Errorf("node %d: (%d, %d, %v, %v) after (%d, %d)", id, from, to, deleted, err, lastN[i][0], lastN[i][1])
								return
							}
							lastN[i] = [2]types.Instant{from, to}
						}
						for i, id := range rels {
							from, to, deleted, err := g.Rels().LatestStamps(id)
							if err != nil || deleted || from < lastR[i][0] || to < lastR[i][1] {
								errs <- fmt.Errorf("rel %d: (%d, %d, %v, %v) after (%d, %d)", id, from, to, deleted, err, lastR[i][0], lastR[i][1])
								return
							}
							lastR[i] = [2]types.Instant{from, to}
						}
					}
				}()
			}
			wg.Wait()
			stop.Store(true)
			readers.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			for _, id := range nodes {
				want, _, err := nodeStampsOracle(g, id)
				if err != nil {
					t.Fatal(err)
				}
				from, to, deleted, err := g.Nodes().LatestStamps(id)
				if err != nil || from != want.from || to != want.to || deleted != want.deleted {
					t.Fatalf("final node %d = (%d, %d, %v, %v), want %+v", id, from, to, deleted, err, want)
				}
			}
			for _, id := range rels {
				want, _, err := relStampsOracle(g, id)
				if err != nil {
					t.Fatal(err)
				}
				from, to, deleted, err := g.Rels().LatestStamps(id)
				if err != nil || from != want.from || to != want.to || deleted != want.deleted {
					t.Fatalf("final rel %d = (%d, %d, %v, %v), want %+v", id, from, to, deleted, err, want)
				}
			}
		})
	}
}
