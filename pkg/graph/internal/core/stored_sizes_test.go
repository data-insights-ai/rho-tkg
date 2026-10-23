package core

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TestNodeStoredSizesMatchHistory pins row 121's stored-size read on badger,
// sharded and tiered (delta-encoded history, a reference and an event
// label): for nodes never changed, updated, rewritten in valid time, excised
// and deleted, NodeStoredSizes lists the current row (if any) with the
// current version, then exactly the versions History reads, ascending, each
// with its excise mark, every row with a positive size; ForEachNodeStoredSize
// hands fn the same rows for every current member, ascending. Mutation: a
// void flag never set, a history row skipped, or the current row listed for a
// deleted node fails a comparison.
func TestNodeStoredSizesMatchHistory(t *testing.T) {
	t.Parallel()
	stores := map[string]func(t *testing.T) storepkg.MandatoryStore{
		"badger":  pageDoorStores["badger"],
		"sharded": pageDoorStores["sharded"],
		"tiered": func(t *testing.T) storepkg.MandatoryStore {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"D"}, ShardWindow: time.Hour, HistoryDeltaEncoding: true})
			if err != nil {
				t.Fatal(err)
			}
			return ts
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: open(t)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			var ids []types.NodeID
			for i := range 24 {
				label := "D"
				if i%3 == 0 {
					label = "Ev"
				}
				n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"k": i, "pad": "abcdefghijklmnopqrstuvwxyz", "tkg_valid_from": types.Instant(1000)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			for i, id := range ids {
				var err error
				switch i % 6 {
				case 1:
					for u := range 4 {
						if _, err = g.Nodes.Update(ctx, id, map[string]any{"k": 100*u + i}); err != nil {
							break
						}
					}
				case 2:
					_, err = g.Temporal.SetNodeVersionInterval(ctx, id, 2000, 3000, map[string]any{"k": -i})
				case 3:
					err = g.Temporal.ExciseNodeValidity(ctx, id, 1500, 1600)
				case 4:
					err = g.Nodes.Delete(ctx, id)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			voids := 0
			for _, id := range ids {
				rows, err := g.Admin.NodeStoredSizes(id)
				if err != nil {
					t.Fatal(err)
				}
				hist, err := g.Nodes.History(id)
				if err != nil {
					t.Fatal(err)
				}
				cur, curErr := g.Nodes.Get(ctx, id)
				if curErr == nil {
					if len(rows) == 0 || !rows[0].Current || rows[0].Version != cur.Version() {
						t.Fatalf("node %d: rows %+v, want the current row v%d first", id, rows, cur.Version())
					}
					rows = rows[1:]
				}
				if len(rows) != len(hist) {
					t.Fatalf("node %d: %d history rows stored, History reads %d", id, len(rows), len(hist))
				}
				for i, r := range rows {
					tm := hist[i].Temporal()
					if r.Current || r.Version != hist[i].Version() || r.Void != (tm != nil && tm.Void) || r.Bytes <= 0 {
						t.Fatalf("node %d row %d: %+v, History v%d void=%v", id, i, r, hist[i].Version(), tm != nil && tm.Void)
					}
					if r.Void {
						voids++
					}
				}
			}
			if voids == 0 {
				t.Fatal("fixture wrote no excise piece")
			}
			for _, label := range []string{"D", "Ev"} {
				var seen []types.NodeID
				if err := g.Admin.ForEachNodeStoredSize(ctx, label, storepkg.DepthAll, func(id types.NodeID, rows []storepkg.StoredRow) bool {
					want, err := g.Admin.NodeStoredSizes(id)
					if err != nil || !slices.Equal(rows, want) {
						t.Fatalf("%s node %d: ForEach rows %+v, NodeStoredSizes %+v (%v)", label, id, rows, want, err)
					}
					seen = append(seen, id)
					return true
				}); err != nil {
					t.Fatal(err)
				}
				members, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{})
				if err != nil {
					t.Fatal(err)
				}
				if len(seen) != len(members) || !slices.IsSorted(seen) {
					t.Fatalf("%s: ForEach saw %d nodes (sorted %v), the label has %d", label, len(seen), slices.IsSorted(seen), len(members))
				}
			}
		})
	}
}

// TestNodeStoredSizesContract pins the edges: memory declines with
// ErrCapabilityNotSupported; fn returning false stops; a nil fn, a cancelled
// ctx and an unknown label.
func TestNodeStoredSizesContract(t *testing.T) {
	t.Parallel()
	mem, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mem.Close() })
	if _, err := mem.Admin.NodeStoredSizes(types.NodeID(5)); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("memory NodeStoredSizes = %v, want ErrCapabilityNotSupported", err)
	}
	g, _ := pageDoorFixture(t, pageDoorStores["badger"](t), false)
	ctx := context.Background()
	calls := 0
	if err := g.Admin.ForEachNodeStoredSize(ctx, "D", storepkg.DepthAll, func(types.NodeID, []storepkg.StoredRow) bool {
		calls++
		return calls < 2
	}); err != nil || calls != 2 {
		t.Fatalf("fn false at the 2nd node: %v, %d calls", err, calls)
	}
	never := func(types.NodeID, []storepkg.StoredRow) bool { t.Fatal("fn called"); return false }
	if err := g.Admin.ForEachNodeStoredSize(ctx, "D", storepkg.DepthAll, nil); !errors.Is(err, grapherr.ErrNilCallback) {
		t.Fatalf("nil fn = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := g.Admin.ForEachNodeStoredSize(cancelled, "D", storepkg.DepthAll, never); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx = %v", err)
	}
	if err := g.Admin.ForEachNodeStoredSize(ctx, "Unknown", storepkg.DepthAll, never); err != nil {
		t.Fatalf("unknown label = %v", err)
	}
}

// TestNodeStoredSizesBytesAreTheStoredValues pins Bytes and Delta on badger
// with delta-encoded history: after a flush, every row's Bytes is the stored
// value's length (the raw key-value size), history rows between anchors are
// deltas, and the same rows are reported while they are still in the write
// buffer. Mutation: a size taken from the decoded row, or a delta flag never
// set, fails a comparison.
func TestNodeStoredSizesBytesAreTheStoredValues(t *testing.T) {
	t.Parallel()
	bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true, HistoryDeltaEncoding: true, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": 0, "pad": "abcdefghijklmnopqrstuvwxyz0123456789"})
	if err != nil {
		t.Fatal(err)
	}
	for u := 1; u <= 9; u++ {
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"k": u}); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := g.Admin.NodeStoredSizes(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	rows, err := g.Admin.NodeStoredSizes(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pending, rows) {
		t.Fatalf("pending rows %+v, flushed %+v", pending, rows)
	}
	raw := map[string]int{}
	if err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			raw[string(it.Item().KeyCopy(nil))] = int(it.Item().ValueSize())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	id := n.ID().SnowflakeID()
	deltas := 0
	for _, r := range rows {
		key := storeutil.NodeKey(id)
		if !r.Current {
			key = storeutil.HistNodeKey(id, uint64(r.Version))
		}
		if want, ok := raw[string(key)]; !ok || r.Bytes != want {
			t.Fatalf("row %+v: stored value %d bytes (present %v)", r, want, ok)
		}
		if r.Delta {
			deltas++
		}
	}
	if len(rows) != 10 || deltas == 0 {
		t.Fatalf("%d rows, %d deltas; want 10 rows (current + 9 history), some deltas", len(rows), deltas)
	}
}
