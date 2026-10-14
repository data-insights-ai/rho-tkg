package graph_test

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
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
// CURRENT_STAMPS_SEEDS, else 48 (24 in -short, at 100 steps instead of 300;
// 8 in -short under -race, the 4-vCPU CI race run).
func currentStampsSeeds() int {
	if s := os.Getenv("CURRENT_STAMPS_SEEDS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	if testing.Short() && isRaceEnabled() {
		return 8
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

// shardedGetOverride is currentStampsGetOverride over a sharded store, also
// overriding the label scan the effective scans' gather reads.
type shardedGetOverride struct {
	*sharded.Store
	failNode atomic.Int64
	failScan atomic.Bool
}

func (s *shardedGetOverride) GetNode(id types.NodeID) (*types.Node, error) {
	if int64(id) == s.failNode.Load() {
		return nil, errInjectedGet
	}
	return s.Store.GetNode(id)
}

func (s *shardedGetOverride) NodesByLabel(tok uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if s.failScan.Load() {
		return nil, errInjectedGet
	}
	return s.Store.NodesByLabel(tok, opts)
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
	t.Run("inherited from sharded keeps the overrides visible", func(t *testing.T) {
		inner, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
		if err != nil {
			t.Fatal(err)
		}
		st := &shardedGetOverride{Store: inner}
		if _, ok := any(st).(storepkg.CurrentStampsCapability); !ok {
			t.Fatal("setup: the embedded sharded store must promote CurrentStampsCapability")
		}
		if _, ok := any(st).(storepkg.MemberIDsCapability); !ok {
			t.Fatal("setup: the embedded sharded store must promote MemberIDsCapability")
		}
		g, err := graphpkg.New(graphpkg.Config{Store: st})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		nid, _ := scenario(t, g)
		pin, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		st.failNode.Store(int64(nid))
		if _, _, _, err := g.Nodes().LatestStamps(nid); !errors.Is(err, errInjectedGet) {
			t.Fatalf("LatestStamps with a failing GetNode override = %v, want the injected error", err)
		}
		st.failScan.Store(true)
		if err := g.Temporal().ForEachNodeEffectiveByLabel("Event", pin, func(temporal.NodeSegment) bool { return true }); !errors.Is(err, errInjectedGet) {
			t.Fatalf("effective scan with a failing NodesByLabel override = %v, want the injected error", err)
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
