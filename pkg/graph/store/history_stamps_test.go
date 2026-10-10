package store_test

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 30: store.HistoryStampsCapability, the history half of
// LatestStamps. On every backend NodeHistoryStamps(id) / RelHistoryStamps(id)
// must equal, at every moment, the fold of GetNodeHistory(id) /
// GetRelHistory(id): the largest TxFrom, the largest TxTo or DeletedAt, and
// whether there is a row. Driven through the store doors that add, overwrite
// and remove history rows, with distinct stamps per row so a fold that skips a
// row, ignores DeletedAt, keeps a value an overwrite or a delete removed, or
// survives Clear answers a different number.

type stampsBackend struct {
	name string
	open func(t *testing.T) storecontract.Store
}

func stampsBackends() []stampsBackend {
	return []stampsBackend{
		{name: "memory", open: func(t *testing.T) storecontract.Store { return memory.New() }},
		{name: "badger", open: func(t *testing.T) storecontract.Store {
			bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return bs
		}},
		{name: "badger-delta", open: func(t *testing.T) storecontract.Store {
			bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: time.Hour, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return bs
		}},
		{name: "sharded", open: func(t *testing.T) storecontract.Store {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return st
		}},
		{name: "tiered", open: func(t *testing.T) storecontract.Store {
			st, err := tiered.New(tiered.Config{InMemory: true, ShardWindow: 7 * 24 * time.Hour, FlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			return st
		}},
	}
}

// stampsKind adapts node and relationship doors (testing rule 2).
type stampsKind struct {
	name     string
	put      func(st storecontract.Store, id int64, ver uint32, tf, tt, da types.Instant) error
	trimFrom func(st storecontract.Store, id int64, minVer uint32) error
	truncate func(st storecontract.Store, id int64, keep int) error
	stamps   func(st storecontract.Store, id int64) (types.Instant, types.Instant, bool, error)
	history  func(st storecontract.Store, id int64) ([]*types.TemporalMetadata, error)
}

// stampsIDBase puts the test IDs one second past the snowflake epoch: tiered
// routes a row by its ID's timestamp, and an ID of timestamp 0 maps to no
// event shard.
const stampsIDBase = int64(1000) << 22

func stampsKinds() []stampsKind {
	tm := func(tf, tt, da types.Instant) *types.TemporalMetadata {
		return &types.TemporalMetadata{TxFrom: tf, TxTo: tt, DeletedAt: da}
	}
	return []stampsKind{
		{
			name: "node",
			put: func(st storecontract.Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				n := types.NewNode(types.NodeID(snowflake.ID(stampsIDBase+id)), 1, nil)
				n.SetVersion(ver)
				n.SetTemporal(tm(tf, tt, da))
				return st.PutNodeVersion(n.ID(), ver, n)
			},
			trimFrom: func(st storecontract.Store, id int64, minVer uint32) error {
				nid := types.NodeID(snowflake.ID(stampsIDBase + id))
				if c, ok := st.(storecontract.HistoryRollbackTrimCapability); ok {
					return c.TrimNodeHistoryFrom(nid, minVer)
				}
				// No trim door (sharded, tiered): drop every row, put back the kept ones.
				rows, err := st.GetNodeHistory(nid)
				if err != nil {
					return err
				}
				if err := st.TruncateNodeHistory(nid, 0); err != nil {
					return err
				}
				for _, r := range rows {
					if r.Version() < minVer {
						if err := st.PutNodeVersion(nid, r.Version(), r); err != nil {
							return err
						}
					}
				}
				return nil
			},
			truncate: func(st storecontract.Store, id int64, keep int) error {
				return st.TruncateNodeHistory(types.NodeID(snowflake.ID(stampsIDBase+id)), keep)
			},
			stamps: func(st storecontract.Store, id int64) (types.Instant, types.Instant, bool, error) {
				return st.(storecontract.HistoryStampsCapability).NodeHistoryStamps(types.NodeID(snowflake.ID(stampsIDBase + id)))
			},
			history: func(st storecontract.Store, id int64) ([]*types.TemporalMetadata, error) {
				h, err := st.GetNodeHistory(types.NodeID(snowflake.ID(stampsIDBase + id)))
				out := make([]*types.TemporalMetadata, 0, len(h))
				for _, n := range h {
					out = append(out, n.Temporal())
				}
				return out, err
			},
		},
		{
			name: "rel",
			put: func(st storecontract.Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				r := types.NewRelationship(types.RelID(snowflake.ID(stampsIDBase+id)), 5, types.NodeID(stampsIDBase+1), types.NodeID(stampsIDBase+2))
				r.SetVersion(ver)
				r.SetTemporal(tm(tf, tt, da))
				return st.PutRelVersion(r.ID(), ver, r)
			},
			trimFrom: func(st storecontract.Store, id int64, minVer uint32) error {
				rid := types.RelID(snowflake.ID(stampsIDBase + id))
				if c, ok := st.(storecontract.HistoryRollbackTrimCapability); ok {
					return c.TrimRelHistoryFrom(rid, minVer)
				}
				rows, err := st.GetRelHistory(rid)
				if err != nil {
					return err
				}
				if err := st.TruncateRelHistory(rid, 0); err != nil {
					return err
				}
				for _, r := range rows {
					if r.Version() < minVer {
						if err := st.PutRelVersion(rid, r.Version(), r); err != nil {
							return err
						}
					}
				}
				return nil
			},
			truncate: func(st storecontract.Store, id int64, keep int) error {
				return st.TruncateRelHistory(types.RelID(snowflake.ID(stampsIDBase+id)), keep)
			},
			stamps: func(st storecontract.Store, id int64) (types.Instant, types.Instant, bool, error) {
				return st.(storecontract.HistoryStampsCapability).RelHistoryStamps(types.RelID(snowflake.ID(stampsIDBase + id)))
			},
			history: func(st storecontract.Store, id int64) ([]*types.TemporalMetadata, error) {
				h, err := st.GetRelHistory(types.RelID(snowflake.ID(stampsIDBase + id)))
				out := make([]*types.TemporalMetadata, 0, len(h))
				for _, r := range h {
					out = append(out, r.Temporal())
				}
				return out, err
			},
		},
	}
}

// expectStamps asserts the store answer equals both the fold of the history
// door and the value the scenario expects (so the scenario cannot drift into
// asserting nothing).
func expectStamps(t *testing.T, k stampsKind, st storecontract.Store, step string, id int64, wantFrom, wantTo types.Instant, wantHas bool) {
	t.Helper()
	rows, err := k.history(st, id)
	if err != nil {
		t.Fatalf("%s %s: history(%d): %v", k.name, step, id, err)
	}
	var from, to types.Instant
	for _, tm := range rows {
		if tm != nil {
			from = max(from, tm.TxFrom)
			to = max(to, tm.TxTo, tm.DeletedAt)
		}
	}
	if from != wantFrom || to != wantTo || (len(rows) > 0) != wantHas {
		t.Fatalf("%s %s: scenario wrong: history(%d) folds to (%d, %d, %v), scenario expects (%d, %d, %v)", k.name, step, id, from, to, len(rows) > 0, wantFrom, wantTo, wantHas)
	}
	gf, gt, has, err := k.stamps(st, id)
	if err != nil || gf != wantFrom || gt != wantTo || has != wantHas {
		t.Fatalf("%s %s: stamps(%d) = (%d, %d, %v, %v), history folds to (%d, %d, %v)", k.name, step, id, gf, gt, has, err, wantFrom, wantTo, wantHas)
	}
}

// stampsScenario drives one ID through every maintenance case; check runs
// between steps (nil: only at the end, so the first call builds over the
// whole sequence in the write buffer).
func stampsScenario(t *testing.T, k stampsKind, st storecontract.Store, check func(step string, from, to types.Instant, has bool)) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	const a = 10
	must(k.put(st, a, 0, 100, 200, 0))
	must(k.put(st, a, 1, 300, 0, 0))
	check("two rows", 300, 200, true)
	// A row whose DeletedAt lies past its TxTo: the delete stamp counts.
	must(k.put(st, a, 2, 250, 0, 900))
	check("delete stamp", 300, 900, true)
	// Overwrite the row holding the max TxFrom with lower stamps: the max
	// must come down to the next row (a running max cannot).
	must(k.put(st, a, 1, 50, 60, 0))
	check("overwrite lowers TxFrom", 250, 900, true)
	must(k.put(st, a, 2, 260, 0, 0))
	check("overwrite drops the delete stamp", 260, 200, true)
	// A new top version raises.
	must(k.put(st, a, 3, 400, 500, 0))
	check("new top", 400, 500, true)
	// Trims (rollback) and truncation (compaction) remove the max rows.
	must(k.trimFrom(st, a, 2))
	check("trim from 2", 100, 200, true)
	must(k.truncate(st, a, 1))
	check("truncate keep 1", 50, 60, true)
	must(k.trimFrom(st, a, 0))
	check("trim all", 0, 0, false)
	must(k.put(st, a, 7, 10, 20, 0))
	check("first row after empty", 10, 20, true)
	must(k.truncate(st, a, 0))
	check("truncate to zero", 0, 0, false)
}

func TestHistoryStampsMatchHistoryFold(t *testing.T) {
	for _, b := range stampsBackends() {
		for _, k := range stampsKinds() {
			t.Run(b.name+"/"+k.name+"/checked", func(t *testing.T) {
				st := b.open(t)
				t.Cleanup(func() { _ = st.Close() })
				const nb = 11
				if err := k.put(st, nb, 0, 7, 8, 0); err != nil {
					t.Fatal(err)
				}
				expectStamps(t, k, st, "unknown id", 12, 0, 0, false)
				stampsScenario(t, k, st, func(step string, from, to types.Instant, has bool) {
					expectStamps(t, k, st, step, 10, from, to, has)
					expectStamps(t, k, st, step+" neighbour", nb, 7, 8, true)
					if f, ok := st.(interface{ Flush() error }); ok && step == "delete stamp" {
						if err := f.Flush(); err != nil {
							t.Fatal(err)
						}
					}
				})
				// Clear drops everything; an ID's stamps must not survive it.
				if err := st.Clear(); err != nil {
					t.Fatal(err)
				}
				expectStamps(t, k, st, "cleared neighbour", nb, 0, 0, false)
				if err := k.put(st, nb, 3, 1, 2, 0); err != nil {
					t.Fatal(err)
				}
				expectStamps(t, k, st, "after clear", nb, 1, 2, true)
			})
			t.Run(b.name+"/"+k.name+"/first-call-at-end", func(t *testing.T) {
				st := b.open(t)
				t.Cleanup(func() { _ = st.Close() })
				stampsScenario(t, k, st, func(string, types.Instant, types.Instant, bool) {})
				if err := k.put(st, 10, 9, 600, 0, 700); err != nil {
					t.Fatal(err)
				}
				expectStamps(t, k, st, "first call after the sequence", 10, 600, 700, true)
			})
		}
	}
}

// Invalid IDs fail like the history doors (ErrInvalidStoreMutation), a
// closed store fails, and a nil concrete store returns ErrNilStore where its
// history doors do.
func TestHistoryStampsErrors(t *testing.T) {
	for _, b := range stampsBackends() {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			caps, ok := st.(storecontract.HistoryStampsCapability)
			if !ok {
				t.Fatalf("%T lacks HistoryStampsCapability", st)
			}
			sameClass := func(a, b error) bool {
				return a != nil && b != nil &&
					errors.Is(a, storecontract.ErrInvalidStoreMutation) == errors.Is(b, storecontract.ErrInvalidStoreMutation) &&
					errors.Is(a, storecontract.ErrSlotNotLocal) == errors.Is(b, storecontract.ErrSlotNotLocal)
			}
			for _, bad := range []int64{0, -1} {
				_, herr := st.GetNodeHistory(types.NodeID(bad))
				if _, _, _, err := caps.NodeHistoryStamps(types.NodeID(bad)); !sameClass(err, herr) {
					t.Fatalf("NodeHistoryStamps(%d) = %v, GetNodeHistory %v", bad, err, herr)
				}
				_, herr = st.GetRelHistory(types.RelID(bad))
				if _, _, _, err := caps.RelHistoryStamps(types.RelID(bad)); !sameClass(err, herr) {
					t.Fatalf("RelHistoryStamps(%d) = %v, GetRelHistory %v", bad, err, herr)
				}
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := caps.NodeHistoryStamps(types.NodeID(stampsIDBase + 1)); err == nil {
				t.Fatal("closed store NodeHistoryStamps succeeded")
			}
			if _, _, _, err := caps.RelHistoryStamps(types.RelID(stampsIDBase + 1)); err == nil {
				t.Fatal("closed store RelHistoryStamps succeeded")
			}
		})
	}
	var nm *memory.Store
	if _, _, _, err := nm.NodeHistoryStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil memory store: %v", err)
	}
	if _, _, _, err := nm.RelHistoryStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil memory store rel: %v", err)
	}
	var nb *badger.Store
	if _, _, _, err := nb.NodeHistoryStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil badger store: %v", err)
	}
	if _, _, _, err := nb.RelHistoryStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil badger store rel: %v", err)
	}
}

// FoldTxStamps is the one definition every backend and the core fallback use.
func TestFoldTxStamps(t *testing.T) {
	for _, tc := range []struct {
		name                string
		from, to            types.Instant
		txFrom, txTo, delAt types.Instant
		wantFrom, wantTo    types.Instant
	}{
		{"empty", 0, 0, 0, 0, 0, 0, 0},
		{"first row", 0, 0, 5, 0, 0, 5, 0},
		{"lower row keeps max", 9, 8, 5, 4, 0, 9, 8},
		{"higher TxFrom raises", 9, 8, 10, 0, 0, 10, 8},
		{"TxTo raises", 9, 8, 1, 20, 0, 9, 20},
		{"DeletedAt past TxTo counts", 9, 8, 1, 2, 30, 9, 30},
		{"DeletedAt below TxTo ignored", 9, 8, 1, 40, 30, 9, 40},
	} {
		f, to := storecontract.FoldTxStamps(tc.from, tc.to, tc.txFrom, tc.txTo, tc.delAt)
		if f != tc.wantFrom || to != tc.wantTo {
			t.Errorf("%s: FoldTxStamps = (%d, %d), want (%d, %d)", tc.name, f, to, tc.wantFrom, tc.wantTo)
		}
	}
}
