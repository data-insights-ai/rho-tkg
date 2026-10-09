package core

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Shared fixtures for the caller-instant delete/update tests (DeleteWithTx,
// UpdateWithTx; tasks/handover-tx-backfill-delete-update-20261009.md §5).
// Every test runs over memory, badger (in-memory), sharded and tiered; on
// tiered the relationship is cross-shard ("Ref" endpoint on the reference
// shard, "Ev" endpoint on the hot event shard).

type txbBackend struct {
	name string
	open func(t *testing.T, allowBackfill bool) *Core
}

func txbBackends() []txbBackend {
	newCore := func(t *testing.T, cfg Config) *Core {
		t.Helper()
		g, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}
	return []txbBackend{
		{"memory", func(t *testing.T, allow bool) *Core {
			return newCore(t, Config{AllowTxBackfill: allow})
		}},
		{"badger", func(t *testing.T, allow bool) *Core {
			return newCore(t, Config{BadgerInMemory: true, AllowTxBackfill: allow})
		}},
		{"sharded", func(t *testing.T, allow bool) *Core {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return newCore(t, Config{Store: st, AllowTxBackfill: allow})
		}},
		{"tiered", func(t *testing.T, allow bool) *Core {
			ts, err := tiered.New(tiered.Config{
				InMemory:      true,
				RefLabels:     []string{"Ref"},
				ShardWindow:   7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			t.Cleanup(func() { _ = ts.Close() })
			return newCore(t, Config{Store: ts, AllowTxBackfill: allow})
		}},
	}
}

// txbEndpoints adds a "Ref" start node and an "Ev" end node (cross-shard on
// tiered).
func txbEndpoints(t *testing.T, g *Core) (types.NodeID, types.NodeID) {
	t.Helper()
	ctx := context.Background()
	s, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "s"})
	if err != nil {
		t.Fatalf("Add(Ref): %v", err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": "e"})
	if err != nil {
		t.Fatalf("Add(Ev): %v", err)
	}
	return s.ID(), e.ID()
}

// txbPlainRel adds a LINK relationship through the plain door (system TxFrom).
func txbPlainRel(t *testing.T, g *Core, props map[string]any) *types.Relationship {
	t.Helper()
	s, e := txbEndpoints(t, g)
	r, err := g.Rels.AddByID(context.Background(), "LINK", s, e, props)
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	return r
}

// txbWall is the wall clock in Unix milliseconds.
func txbWall() types.Instant { return types.Instant(time.Now().UnixMilli()) }

// relSnap is everything a refused caller-instant door must leave untouched.
type relSnap struct {
	cur    *types.Relationship
	curErr error
	hist   []*types.Relationship
}

func snapRel(t *testing.T, g *Core, id types.RelID) relSnap {
	t.Helper()
	cur, curErr := g.Rels.Get(context.Background(), id)
	if curErr != nil && !errors.Is(curErr, storepkg.ErrRelNotFound) {
		t.Fatalf("Rels.Get(%v): %v", id, curErr)
	}
	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("Rels.History(%v): %v", id, err)
	}
	hist = append([]*types.Relationship(nil), hist...)
	sort.Slice(hist, func(i, j int) bool { return hist[i].Version() < hist[j].Version() })
	return relSnap{cur: cur, curErr: curErr, hist: hist}
}

func relTemporalCopy(r *types.Relationship) types.TemporalMetadata {
	if tm := r.Temporal(); tm != nil {
		return *tm
	}
	return types.TemporalMetadata{}
}

func relPropsMap(r *types.Relationship) map[string]any {
	out := map[string]any{}
	for _, p := range r.Properties() {
		out[p.Key] = p.Value
	}
	return out
}

// assertRelUnchanged fails unless after equals before: same current row
// (version, every temporal stamp, properties), same history length and the same
// stamps on every history row.
func assertRelUnchanged(t *testing.T, phase string, before, after relSnap) {
	t.Helper()
	if (before.curErr == nil) != (after.curErr == nil) {
		t.Fatalf("[%s] current row presence changed: before err=%v, after err=%v", phase, before.curErr, after.curErr)
	}
	if before.cur != nil {
		if before.cur.Version() != after.cur.Version() {
			t.Fatalf("[%s] current version %d -> %d", phase, before.cur.Version(), after.cur.Version())
		}
		if b, a := relTemporalCopy(before.cur), relTemporalCopy(after.cur); !reflect.DeepEqual(b, a) {
			t.Fatalf("[%s] current temporal changed:\n before %+v\n after  %+v", phase, b, a)
		}
		if b, a := relPropsMap(before.cur), relPropsMap(after.cur); !reflect.DeepEqual(b, a) {
			t.Fatalf("[%s] current properties changed: %v -> %v", phase, b, a)
		}
	}
	if len(before.hist) != len(after.hist) {
		t.Fatalf("[%s] history length %d -> %d", phase, len(before.hist), len(after.hist))
	}
	for i := range before.hist {
		b, a := before.hist[i], after.hist[i]
		if b.Version() != a.Version() || !reflect.DeepEqual(relTemporalCopy(b), relTemporalCopy(a)) {
			t.Fatalf("[%s] history row %d changed:\n before v%d %+v\n after  v%d %+v",
				phase, i, b.Version(), relTemporalCopy(b), a.Version(), relTemporalCopy(a))
		}
	}
}

// txbChain returns history plus the current row (if any), ordered by version.
func txbChain(t *testing.T, g *Core, id types.RelID) []*types.Relationship {
	t.Helper()
	s := snapRel(t, g, id)
	chain := append([]*types.Relationship(nil), s.hist...)
	if s.cur != nil {
		chain = append(chain, s.cur)
	}
	sort.Slice(chain, func(i, j int) bool { return chain[i].Version() < chain[j].Version() })
	return chain
}

// txbTxDo runs do inside a committed GraphTx.
func txbTxDo(t *testing.T, g *Core, do func(tx *GraphTx) error) error {
	t.Helper()
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := do(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
