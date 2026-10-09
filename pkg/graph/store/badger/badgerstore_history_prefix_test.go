package badger

import (
	"reflect"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// History reads for ONE entity must touch only that entity's rows
// (backlog 20, handover 1a). The four history iterators used
// DefaultIteratorOptions (PrefetchValues = true) without opts.Prefix, so a
// lookup for an entity WITHOUT history prefetched the values of the next 100
// keys, i.e. other entities' history. The behaviour tests below pin the
// answer (exact rows, neighbours hold history); TestHistoryScanAllocGate pins
// the cost.

// histRow is the (id, version) pair both node and rel history answers reduce to.
type histRow struct {
	id  int64
	ver uint32
}

// histKind adapts the node and the rel history doors to one shape so every
// scenario runs for both (node/rel parity).
type histKind struct {
	name string
	// put writes one history version row for id through PutNodeVersion/PutRelVersion.
	put func(t testing.TB, bs *Store, id int64, ver uint32)
	// get is GetNodeHistory/GetRelHistory.
	get func(bs *Store, id int64) ([]histRow, error)
	// page is NodeHistoryVersionsFrom/RelHistoryVersionsFrom.
	page func(bs *Store, id int64, start uint32, limit int) ([]histRow, error)
	// truncate is TruncateNodeHistory/TruncateRelHistory.
	truncate func(bs *Store, id int64, keep int) error
}

var histKinds = []histKind{
	{
		name: "node",
		put: func(t testing.TB, bs *Store, id int64, ver uint32) {
			t.Helper()
			n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
			n.SetVersion(ver)
			if err := bs.PutNodeVersion(types.NodeID(snowflake.ID(id)), ver, n); err != nil {
				t.Fatalf("PutNodeVersion(%d, v%d): %v", id, ver, err)
			}
		},
		get: func(bs *Store, id int64) ([]histRow, error) {
			ns, err := bs.GetNodeHistory(types.NodeID(snowflake.ID(id)))
			rows := make([]histRow, 0, len(ns))
			for _, n := range ns {
				rows = append(rows, histRow{int64(n.ID()), n.Version()})
			}
			return rows, err
		},
		page: func(bs *Store, id int64, start uint32, limit int) ([]histRow, error) {
			ns, err := bs.NodeHistoryVersionsFrom(types.NodeID(snowflake.ID(id)), start, limit)
			rows := make([]histRow, 0, len(ns))
			for _, n := range ns {
				rows = append(rows, histRow{int64(n.ID()), n.Version()})
			}
			return rows, err
		},
		truncate: func(bs *Store, id int64, keep int) error {
			return bs.TruncateNodeHistory(types.NodeID(snowflake.ID(id)), keep)
		},
	},
	{
		name: "rel",
		put: func(t testing.TB, bs *Store, id int64, ver uint32) {
			t.Helper()
			r := types.NewRelationship(types.RelID(snowflake.ID(id)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
			r.SetVersion(ver)
			if err := bs.PutRelVersion(types.RelID(snowflake.ID(id)), ver, r); err != nil {
				t.Fatalf("PutRelVersion(%d, v%d): %v", id, ver, err)
			}
		},
		get: func(bs *Store, id int64) ([]histRow, error) {
			rs, err := bs.GetRelHistory(types.RelID(snowflake.ID(id)))
			rows := make([]histRow, 0, len(rs))
			for _, r := range rs {
				rows = append(rows, histRow{int64(r.ID()), r.Version()})
			}
			return rows, err
		},
		page: func(bs *Store, id int64, start uint32, limit int) ([]histRow, error) {
			rs, err := bs.RelHistoryVersionsFrom(types.RelID(snowflake.ID(id)), start, limit)
			rows := make([]histRow, 0, len(rs))
			for _, r := range rs {
				rows = append(rows, histRow{int64(r.ID()), r.Version()})
			}
			return rows, err
		},
		truncate: func(bs *Store, id int64, keep int) error {
			return bs.TruncateRelHistory(types.RelID(snowflake.ID(id)), keep)
		},
	},
}

const histTarget = int64(1000)

// histNeighbours are IDs whose history rows sit right below, right above and
// far from histTarget in key order: ids that differ only in the last byte
// (999, 1001), the second byte (744, 1256), higher bytes, and extremes.
var histNeighbours = []int64{1, 744, 999, 1001, 1256, 1000 + 1<<16, 1000 + 1<<40, 1000 + 1<<56}

// seedNeighbours gives every neighbour three history versions.
func seedNeighbours(t testing.TB, k histKind, bs *Store) {
	t.Helper()
	for _, id := range histNeighbours {
		for ver := uint32(0); ver < 3; ver++ {
			k.put(t, bs, id, ver)
		}
	}
}

func wantRows(id int64, vers ...uint32) []histRow {
	out := make([]histRow, 0, len(vers))
	for _, v := range vers {
		out = append(out, histRow{id, v})
	}
	return out
}

// histMode builds the store state the scenario runs against. seed receives the
// store and writes the rows; the mode decides where they live afterwards.
type histMode struct {
	name string
	// run seeds via seed(bs) and returns the store to read from.
	run func(t *testing.T, seed func(bs *Store)) *Store
}

var histModes = []histMode{
	{"pending", func(t *testing.T, seed func(bs *Store)) *Store {
		// FlushInterval = 1h: every row stays in the write-buffer overlay.
		bs := newSlowFlushBadgerStore(t)
		seed(bs)
		return bs
	}},
	{"flushed", func(t *testing.T, seed func(bs *Store)) *Store {
		bs := newTestBadgerStore(t)
		seed(bs)
		if err := bs.flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		return bs
	}},
	{"reopened", func(t *testing.T, seed func(bs *Store)) *Store {
		dir := t.TempDir()
		bs, err := New(Config{Dir: dir})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		seed(bs)
		if err := bs.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		bs2, err := New(Config{Dir: dir, FlushInterval: time.Hour})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		t.Cleanup(func() { _ = bs2.Close() })
		return bs2
	}},
}

// TestHistoryPrefix_ExactRowsAmongNeighbours: x's history is exactly x's rows,
// in ascending version order, while lower, higher and last-byte-adjacent
// neighbours hold history too. Rows are written out of order.
func TestHistoryPrefix_ExactRowsAmongNeighbours(t *testing.T) {
	t.Parallel()
	for _, k := range histKinds {
		for _, m := range histModes {
			t.Run(k.name+"/"+m.name, func(t *testing.T) {
				t.Parallel()
				bs := m.run(t, func(bs *Store) {
					seedNeighbours(t, k, bs)
					for _, ver := range []uint32{3, 0, 2, 1} {
						k.put(t, bs, histTarget, ver)
					}
				})
				got, err := k.get(bs, histTarget)
				if err != nil {
					t.Fatalf("history: %v", err)
				}
				if want := wantRows(histTarget, 0, 1, 2, 3); !reflect.DeepEqual(got, want) {
					t.Fatalf("history = %v, want %v", got, want)
				}
				// Every neighbour keeps exactly its own three rows.
				for _, id := range histNeighbours {
					g, err := k.get(bs, id)
					if err != nil {
						t.Fatalf("history(%d): %v", id, err)
					}
					if want := wantRows(id, 0, 1, 2); !reflect.DeepEqual(g, want) {
						t.Fatalf("neighbour %d history = %v, want %v", id, g, want)
					}
				}
			})
		}
	}
}

// TestHistoryPrefix_PlainEntityEmpty: an entity WITHOUT history, with history
// on both sides of it in key order, answers empty and a nil error.
func TestHistoryPrefix_PlainEntityEmpty(t *testing.T) {
	t.Parallel()
	for _, k := range histKinds {
		for _, m := range histModes {
			t.Run(k.name+"/"+m.name, func(t *testing.T) {
				t.Parallel()
				bs := m.run(t, func(bs *Store) { seedNeighbours(t, k, bs) })
				for _, id := range []int64{histTarget, 2, 100000, 1 << 62} {
					got, err := k.get(bs, id)
					if err != nil || len(got) != 0 {
						t.Fatalf("history(%d) = %v, %v; want empty, nil", id, got, err)
					}
					page, err := k.page(bs, id, 0, 0)
					if err != nil || len(page) != 0 {
						t.Fatalf("page(%d) = %v, %v; want empty, nil", id, page, err)
					}
				}
			})
		}
	}
}

// TestHistoryPrefix_PagedDoor: the paged door (NodeHistoryVersionsFrom /
// RelHistoryVersionsFrom, history_*.go :877 and :464) returns exactly x's rows
// from startVersion on, honours the limit, and never leaks a neighbour row.
func TestHistoryPrefix_PagedDoor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		start uint32
		limit int
		want  []uint32
	}{
		{"all", 0, 0, []uint32{0, 1, 2, 3}},
		{"from2", 2, 0, []uint32{2, 3}},
		{"limit2", 0, 2, []uint32{0, 1}},
		{"from1limit2", 1, 2, []uint32{1, 2}},
		{"pastEnd", 4, 0, nil},
		{"fromLast", 3, 5, []uint32{3}},
	}
	for _, k := range histKinds {
		for _, m := range histModes {
			for _, c := range cases {
				t.Run(k.name+"/"+m.name+"/"+c.name, func(t *testing.T) {
					t.Parallel()
					bs := m.run(t, func(bs *Store) {
						seedNeighbours(t, k, bs)
						for _, ver := range []uint32{3, 0, 2, 1} {
							k.put(t, bs, histTarget, ver)
						}
					})
					got, err := k.page(bs, histTarget, c.start, c.limit)
					if err != nil {
						t.Fatalf("page: %v", err)
					}
					if want := wantRows(histTarget, c.want...); !reflect.DeepEqual(got, want) {
						t.Fatalf("page(start=%d, limit=%d) = %v, want %v", c.start, c.limit, got, want)
					}
				})
			}
		}
	}
}

// TestHistoryPrefix_MixedFlushedAndPending: neighbours and the early rows of x
// are committed to Badger, the later rows of x sit in the write buffer. The
// scan and the overlay merge into x's rows only.
func TestHistoryPrefix_MixedFlushedAndPending(t *testing.T) {
	t.Parallel()
	for _, k := range histKinds {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			bs := newSlowFlushBadgerStore(t)
			seedNeighbours(t, k, bs)
			k.put(t, bs, histTarget, 0)
			k.put(t, bs, histTarget, 1)
			if err := bs.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			k.put(t, bs, histTarget, 2) // pending only
			k.put(t, bs, 1001, 3)       // pending row for a neighbour
			got, err := k.get(bs, histTarget)
			if err != nil {
				t.Fatalf("history: %v", err)
			}
			if want := wantRows(histTarget, 0, 1, 2); !reflect.DeepEqual(got, want) {
				t.Fatalf("history = %v, want %v", got, want)
			}
			page, err := k.page(bs, histTarget, 1, 0)
			if err != nil {
				t.Fatalf("page: %v", err)
			}
			if want := wantRows(histTarget, 1, 2); !reflect.DeepEqual(page, want) {
				t.Fatalf("page = %v, want %v", page, want)
			}
			if g, _ := k.get(bs, 1001); !reflect.DeepEqual(g, wantRows(1001, 0, 1, 2, 3)) {
				t.Fatalf("neighbour 1001 = %v, want v0..v3", g)
			}
		})
	}
}

// TestHistoryPrefix_TruncateOverlayDelete: a pending truncate masks committed
// rows of x (overlay delete) without touching neighbours; same after flush and
// after reopen.
func TestHistoryPrefix_TruncateOverlayDelete(t *testing.T) {
	t.Parallel()
	for _, k := range histKinds {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir, FlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			seedNeighbours(t, k, bs)
			for ver := uint32(0); ver < 4; ver++ {
				k.put(t, bs, histTarget, ver)
			}
			if err := bs.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if err := k.truncate(bs, histTarget, 1); err != nil { // keeps v3 only; deletes are pending
				t.Fatalf("truncate: %v", err)
			}
			check := func(stage string, bs *Store) {
				t.Helper()
				got, err := k.get(bs, histTarget)
				if err != nil {
					t.Fatalf("%s history: %v", stage, err)
				}
				if want := wantRows(histTarget, 3); !reflect.DeepEqual(got, want) {
					t.Fatalf("%s history = %v, want %v", stage, got, want)
				}
				page, err := k.page(bs, histTarget, 0, 0)
				if err != nil || !reflect.DeepEqual(page, wantRows(histTarget, 3)) {
					t.Fatalf("%s page = %v, %v; want v3 only", stage, page, err)
				}
				for _, id := range []int64{999, 1001} {
					if g, _ := k.get(bs, id); !reflect.DeepEqual(g, wantRows(id, 0, 1, 2)) {
						t.Fatalf("%s neighbour %d = %v, want v0..v2", stage, id, g)
					}
				}
			}
			check("pending-delete", bs)
			if err := bs.flush(); err != nil {
				t.Fatalf("flush 2: %v", err)
			}
			check("flushed", bs)
			if err := bs.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			bs2, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer bs2.Close()
			check("reopened", bs2)
		})
	}
}

// TestHistoryPrefix_DeleteWithHistoryTombstone: delete-with-history writes one
// tombstone row under the deleted entity's prefix; the entity's history is that
// row alone, neighbours are unchanged.
func TestHistoryPrefix_DeleteWithHistoryTombstone(t *testing.T) {
	t.Parallel()
	t.Run("rel", func(t *testing.T) {
		t.Parallel()
		k := histKinds[1]
		bs := newTestBadgerStore(t)
		seedNeighbours(t, k, bs)
		putTestNode(t, bs, 10, 1, nil)
		putTestNode(t, bs, 20, 1, nil)
		rel := putTestRel(t, bs, histTarget, 5, 10, 20)
		if err := bs.DeleteRelWithHistory(rel.ID(), rel.Version(), rel.DeepCopy()); err != nil {
			t.Fatalf("DeleteRelWithHistory: %v", err)
		}
		for _, stage := range []string{"pending", "flushed"} {
			got, err := k.get(bs, histTarget)
			if err != nil || !reflect.DeepEqual(got, wantRows(histTarget, 0)) {
				t.Fatalf("%s history = %v, %v; want only the tombstone row", stage, got, err)
			}
			if g, _ := k.get(bs, 999); !reflect.DeepEqual(g, wantRows(999, 0, 1, 2)) {
				t.Fatalf("%s neighbour 999 = %v", stage, g)
			}
			if err := bs.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	})
	t.Run("node", func(t *testing.T) {
		t.Parallel()
		k := histKinds[0]
		bs := newTestBadgerStore(t)
		seedNeighbours(t, k, bs)
		n := putTestNode(t, bs, histTarget, 1, nil)
		if err := bs.DeleteNodeWithHistory(n.ID(), n.Version(), n.DeepCopy(), nil); err != nil {
			t.Fatalf("DeleteNodeWithHistory: %v", err)
		}
		for _, stage := range []string{"pending", "flushed"} {
			got, err := k.get(bs, histTarget)
			if err != nil || !reflect.DeepEqual(got, wantRows(histTarget, 0)) {
				t.Fatalf("%s history = %v, %v; want only the tombstone row", stage, got, err)
			}
			if g, _ := k.get(bs, 1001); !reflect.DeepEqual(g, wantRows(1001, 0, 1, 2)) {
				t.Fatalf("%s neighbour 1001 = %v", stage, g)
			}
			if err := bs.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	})
}

// TestHistoryScanAllocGate is the deterministic cost gate. The iterator used to
// prefetch the values of the next 100 keys after the Seek whether or not they
// shared the prefix, so a plain entity (no history) in a store with dense
// neighbouring history allocated per neighbour row. With opts.Prefix the Seek
// ends at the prefix boundary and the cost no longer depends on what the
// neighbours hold. The reference is a plain entity ABOVE every history key
// (the Seek lands past the end, so no neighbour can be prefetched); the
// plain entity in the middle of dense history must cost the same allocations.
func TestHistoryScanAllocGate(t *testing.T) {
	// Not parallel: AllocsPerRun measures the whole process allocator.
	const rows = 600 // > the 100-item prefetch window, history on both sides
	for _, k := range histKinds {
		t.Run(k.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			for i := int64(1); i <= rows; i++ {
				id := 1000 + 2*i // even offsets hold history, odd ones are plain
				k.put(t, bs, id, 0)
				k.put(t, bs, id, 1)
			}
			if err := bs.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			bs, err = New(Config{Dir: dir, FlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = bs.Close() })

			plainMiddle := int64(1000 + 2*rows/2 + 1) // between two history owners
			plainAbove := int64(1 << 40)              // past the last history key
			if got, _ := k.get(bs, plainMiddle); len(got) != 0 {
				t.Fatalf("plainMiddle %d has history %v", plainMiddle, got)
			}

			measure := func(name string, fn func() error) float64 {
				a := testing.AllocsPerRun(50, func() {
					if err := fn(); err != nil {
						t.Fatalf("%s: %v", name, err)
					}
				})
				t.Logf("%s: %.1f allocs/op", name, a)
				return a
			}
			doors := []struct {
				name string
				fn   func(id int64) func() error
			}{
				{"history", func(id int64) func() error {
					return func() error { _, err := k.get(bs, id); return err }
				}},
				{"paged", func(id int64) func() error {
					return func() error { _, err := k.page(bs, id, 0, 0); return err }
				}},
			}
			for _, d := range doors {
				ref := measure(d.name+"/aboveAll", d.fn(plainAbove))
				mid := measure(d.name+"/amongDense", d.fn(plainMiddle))
				if mid > ref+4 {
					t.Errorf("%s: plain entity among dense neighbour history allocates %.1f/op vs %.1f/op past the last key: "+
						"the iterator reads other entities' rows (missing opts.Prefix)", d.name, mid, ref)
				}
			}
		})
	}
}
