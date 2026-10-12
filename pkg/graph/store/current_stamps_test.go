package store_test

import (
	"errors"
	"strings"
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

// Backlog 42: store.CurrentStampsCapability, the current-row half of
// LatestStamps. On every backend NodeCurrentStamps(id) / RelCurrentStamps(id)
// must equal, at every moment, the stamps and version of the row GetNode(id) /
// GetRelationship(id) returns, and fail with its not-found sentinel where
// that door does. Each step writes distinct stamps, so a backend that keeps a
// running max (stale after a plain Replace that LOWERS the stamps), answers a
// deleted current row with its last stamps, reads zero stamps from a row's
// stored form, or reads a row the entity cache has already replaced answers a
// different number. On the badger-based backends every check is made twice:
// with the row as the write left it in the entity cache, then after a flush
// and an eviction, when the row is read from badger (the hit and the miss must
// agree).

// currentKind adapts the node and relationship doors (testing rule 2).
type currentKind struct {
	name    string
	put     func(st storecontract.Store, id int64, row rowSpec) error
	replace func(st storecontract.Store, id int64, row rowSpec) error
	del     func(st storecontract.Store, id int64) error
	get     func(st storecontract.Store, id int64) (storecontract.RowStamps, error)
	stamps  func(st storecontract.Store, id int64) (storecontract.RowStamps, error)
	evict   func(t *testing.T, st storecontract.Store, id int64)
	missing error
}

// rowSpec is one current row's stamps; hashed adds an integrity hash, so the
// stores cache the row in the compact form (TxTo, DeletedAt, CreatedAt zero).
type rowSpec struct {
	ver        uint32
	tf, tt, da types.Instant
	hashed     bool
}

func (s rowSpec) want() storecontract.RowStamps {
	return storecontract.RowStamps{TxFrom: s.tf, TxTo: s.tt, DeletedAt: s.da, Version: s.ver}
}

func currentNodeID(id int64) types.NodeID { return types.NodeID(snowflake.ID(stampsIDBase + id)) }
func currentRelID(id int64) types.RelID   { return types.RelID(snowflake.ID(stampsIDBase + id)) }

// The relationship endpoints the scenario's relationships hang between.
const currentStart, currentEnd = 1, 2

func currentNodeRow(id int64, s rowSpec) *types.Node {
	n := types.NewNode(currentNodeID(id), 1, nil)
	n.SetVersion(s.ver)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 5, TxFrom: s.tf, TxTo: s.tt, DeletedAt: s.da})
	if s.hashed {
		n.SetIntegrity(&types.NodeIntegrity{Hash: strings.Repeat("ab", 32)})
	}
	return n
}

func currentRelRow(id int64, s rowSpec) *types.Relationship {
	r := types.NewRelationship(currentRelID(id), 5, currentNodeID(currentStart), currentNodeID(currentEnd))
	r.SetVersion(s.ver)
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: 5, TxFrom: s.tf, TxTo: s.tt, DeletedAt: s.da})
	if s.hashed {
		r.SetIntegrity(&types.RelIntegrity{Hash: strings.Repeat("cd", 32)})
	}
	return r
}

// evictCached flushes st and drops the entity-cache entry of the row the ID
// routes to, so the next read decodes the stored row (badger-based backends;
// memory holds its rows and has no cache).
func evictCached(t *testing.T, st storecontract.Store, node bool, sid snowflake.ID) {
	t.Helper()
	if f, ok := st.(interface{ Flush() error }); ok {
		if err := f.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	var shard *badger.Store
	switch s := st.(type) {
	case *badger.Store:
		shard = s
	case *sharded.Store:
		var err error
		if shard, err = s.ShardForIDForTest(sid); err != nil {
			t.Fatalf("sharded shard: %v", err)
		}
	case *tiered.Store:
		var err error
		if node {
			shard, err = s.ShardForNodeIDForTest(types.NodeID(sid))
		} else {
			shard, err = s.ShardForRelIDForTest(types.RelID(sid))
		}
		if err != nil {
			t.Fatalf("tiered shard: %v", err)
		}
		if err := shard.Flush(); err != nil {
			t.Fatalf("tiered shard flush: %v", err)
		}
	default:
		return
	}
	if node {
		shard.NodeCacheForTest().EvictForTest(sid)
	} else {
		shard.RelCacheForTest().EvictForTest(sid)
	}
}

func currentKinds() []currentKind {
	nodeStamps := func(st storecontract.Store, id int64) (storecontract.RowStamps, error) {
		return st.(storecontract.CurrentStampsCapability).NodeCurrentStamps(currentNodeID(id))
	}
	relStamps := func(st storecontract.Store, id int64) (storecontract.RowStamps, error) {
		return st.(storecontract.CurrentStampsCapability).RelCurrentStamps(currentRelID(id))
	}
	return []currentKind{
		{
			name:    "node",
			put:     func(st storecontract.Store, id int64, s rowSpec) error { return st.PutNode(currentNodeRow(id, s)) },
			replace: func(st storecontract.Store, id int64, s rowSpec) error { return st.ReplaceNode(currentNodeRow(id, s)) },
			del:     func(st storecontract.Store, id int64) error { return st.DeleteNode(currentNodeID(id)) },
			get: func(st storecontract.Store, id int64) (storecontract.RowStamps, error) {
				n, err := st.GetNode(currentNodeID(id))
				if err != nil {
					return storecontract.RowStamps{}, err
				}
				return storecontract.NodeRowStamps(n), nil
			},
			stamps: nodeStamps,
			evict: func(t *testing.T, st storecontract.Store, id int64) {
				evictCached(t, st, true, snowflake.ID(stampsIDBase+id))
			},
			missing: storecontract.ErrNodeNotFound,
		},
		{
			name: "rel",
			put: func(st storecontract.Store, id int64, s rowSpec) error {
				return st.PutRelationship(currentRelRow(id, s))
			},
			replace: func(st storecontract.Store, id int64, s rowSpec) error {
				return st.ReplaceRelationship(currentRelRow(id, s))
			},
			del: func(st storecontract.Store, id int64) error { return st.DeleteRelationship(currentRelID(id)) },
			get: func(st storecontract.Store, id int64) (storecontract.RowStamps, error) {
				r, err := st.GetRelationship(currentRelID(id))
				if err != nil {
					return storecontract.RowStamps{}, err
				}
				return storecontract.RelRowStamps(r), nil
			},
			stamps: relStamps,
			evict: func(t *testing.T, st storecontract.Store, id int64) {
				evictCached(t, st, false, snowflake.ID(stampsIDBase+id))
			},
			missing: storecontract.ErrRelNotFound,
		},
	}
}

// expectCurrent asserts the capability answers want (or the kind's not-found
// sentinel when found is false), equal to the row the Get door returns, from
// the cache as the write left it and again after a flush and an eviction.
func expectCurrent(t *testing.T, k currentKind, st storecontract.Store, step string, id int64, want storecontract.RowStamps, found bool) {
	t.Helper()
	check := func(phase string) {
		t.Helper()
		got, err := k.stamps(st, id)
		row, gerr := k.get(st, id)
		if !found {
			if !errors.Is(err, k.missing) || !errors.Is(gerr, k.missing) {
				t.Fatalf("%s %s (%s): stamps = (%+v, %v), Get %v; want %v from both", k.name, step, phase, got, err, gerr, k.missing)
			}
			return
		}
		if gerr != nil || row != want {
			t.Fatalf("%s %s (%s): scenario wrong: Get = (%+v, %v), want %+v", k.name, step, phase, row, gerr, want)
		}
		if err != nil || got != want {
			t.Fatalf("%s %s (%s): stamps = (%+v, %v), the current row is %+v", k.name, step, phase, got, err, want)
		}
	}
	check("as written")
	k.evict(t, st, id)
	got, err := k.stamps(st, id) // before Get fills the cache again
	if found && (err != nil || got != want) {
		t.Fatalf("%s %s (evicted): stamps = (%+v, %v), the current row is %+v (a cache hit and a miss disagree)", k.name, step, got, err, want)
	}
	if !found && !errors.Is(err, k.missing) {
		t.Fatalf("%s %s (evicted): stamps = (%+v, %v), want %v", k.name, step, got, err, k.missing)
	}
	check("evicted")
}

// TestCurrentStampsMatchCurrentRow drives one ID per kind through create,
// a raising Replace, a LOWERING Replace (UpdateInPlace, rollback restore and
// replica apply write one), the compact cached form, a delete and a re-create,
// beside a neighbour that keeps its row, on every backend. Faults it names: a
// running max over the writes (stale after the lowering Replace), the deleted
// row's last stamps, zero stamps from the compact form, an evicted row
// answered from anything but its stored bytes, a router that asks the wrong
// shard.
func TestCurrentStampsMatchCurrentRow(t *testing.T) {
	for _, b := range currentBackends() {
		for _, k := range currentKinds() {
			t.Run(b.name+"/"+k.name, func(t *testing.T) {
				st := b.open(t)
				t.Cleanup(func() { _ = st.Close() })
				for _, id := range []int64{currentStart, currentEnd} {
					if err := st.PutNode(currentNodeRow(id, rowSpec{tf: 1})); err != nil {
						t.Fatalf("endpoint %d: %v", id, err)
					}
				}
				const a, nb = 10, 11
				must := func(err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
				}
				expectCurrent(t, k, st, "unknown id", a, storecontract.RowStamps{}, false)
				neighbour := rowSpec{ver: 4, tf: 70, tt: 71, da: 72}
				must(k.put(st, nb, neighbour))
				steps := []struct {
					name string
					do   func() error
					row  rowSpec
				}{
					{"create", func() error { return k.put(st, a, rowSpec{ver: 0, tf: 100, tt: 200, da: 300}) }, rowSpec{ver: 0, tf: 100, tt: 200, da: 300}},
					{"replace raises", nil, rowSpec{ver: 1, tf: 400, tt: 0, da: 500}},
					{"replace lowers", nil, rowSpec{ver: 2, tf: 50, tt: 60, da: 0}},
					{"compact cached form", nil, rowSpec{ver: 3, tf: 600, hashed: true}},
					{"replace with a version gap", nil, rowSpec{ver: 9, tf: 30, tt: 0, da: 0}},
				}
				for _, s := range steps {
					if s.do != nil {
						must(s.do())
					} else {
						must(k.replace(st, a, s.row))
					}
					expectCurrent(t, k, st, s.name, a, s.row.want(), true)
					expectCurrent(t, k, st, s.name+" neighbour", nb, neighbour.want(), true)
				}
				must(k.del(st, a))
				expectCurrent(t, k, st, "deleted", a, storecontract.RowStamps{}, false)
				expectCurrent(t, k, st, "deleted neighbour", nb, neighbour.want(), true)
				again := rowSpec{ver: 12, tf: 900, tt: 0, da: 0}
				must(k.put(st, a, again))
				expectCurrent(t, k, st, "re-created", a, again.want(), true)
				// Clear removes every row; no answer may survive it.
				must(st.Clear())
				expectCurrent(t, k, st, "cleared", a, storecontract.RowStamps{}, false)
				expectCurrent(t, k, st, "cleared neighbour", nb, storecontract.RowStamps{}, false)
			})
		}
	}
}

// currentBackends is stampsBackends plus badger on disk: an evicted row is
// read from the value log and the LSM rather than from the memtable alone.
func currentBackends() []stampsBackend {
	return append(stampsBackends(), stampsBackend{name: "badger-disk", open: func(t *testing.T) storecontract.Store {
		bs, err := badger.New(badger.Config{Dir: t.TempDir(), FlushInterval: time.Hour})
		if err != nil {
			t.Fatalf("badger.New: %v", err)
		}
		return bs
	}})
}

// Invalid IDs fail like GetNode / GetRelationship (ErrInvalidStoreMutation;
// sharded: a foreign slot ErrSlotNotLocal), a closed store fails with
// ErrStoreClosed, and a nil concrete store returns ErrNilStore.
func TestCurrentStampsErrors(t *testing.T) {
	for _, b := range currentBackends() {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			caps, ok := st.(storecontract.CurrentStampsCapability)
			if !ok {
				t.Fatalf("%T lacks CurrentStampsCapability", st)
			}
			for _, bad := range []int64{0, -1} {
				_, gerr := st.GetNode(types.NodeID(bad))
				if _, err := caps.NodeCurrentStamps(types.NodeID(bad)); !errors.Is(err, storecontract.ErrInvalidStoreMutation) || !errors.Is(gerr, storecontract.ErrInvalidStoreMutation) {
					t.Fatalf("NodeCurrentStamps(%d) = %v, GetNode %v; want ErrInvalidStoreMutation from both", bad, err, gerr)
				}
				_, gerr = st.GetRelationship(types.RelID(bad))
				if _, err := caps.RelCurrentStamps(types.RelID(bad)); !errors.Is(err, storecontract.ErrInvalidStoreMutation) || !errors.Is(gerr, storecontract.ErrInvalidStoreMutation) {
					t.Fatalf("RelCurrentStamps(%d) = %v, GetRelationship %v; want ErrInvalidStoreMutation from both", bad, err, gerr)
				}
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := caps.NodeCurrentStamps(currentNodeID(1)); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed NodeCurrentStamps = %v, want ErrStoreClosed", err)
			}
			if _, err := caps.RelCurrentStamps(currentRelID(1)); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed RelCurrentStamps = %v, want ErrStoreClosed", err)
			}
		})
	}
	var nm *memory.Store
	if _, err := nm.NodeCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil memory store: %v", err)
	}
	if _, err := nm.RelCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil memory store rel: %v", err)
	}
	var nb *badger.Store
	if _, err := nb.NodeCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil badger store: %v", err)
	}
	if _, err := nb.RelCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil badger store rel: %v", err)
	}
	var ns *sharded.Store
	if _, err := ns.NodeCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil sharded store: %v", err)
	}
	if _, err := ns.RelCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil sharded store rel: %v", err)
	}
	var nt *tiered.Store
	if _, err := nt.NodeCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil tiered store: %v", err)
	}
	if _, err := nt.RelCurrentStamps(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil tiered store rel: %v", err)
	}
}

// NodeRowStamps / RelRowStamps read a row's stamps and version: distinct
// values per field catch a helper that reads the wrong one; the compact form
// (no TxTo, no DeletedAt) and a row without temporal metadata read zeros where
// the row has none; a nil row reads zero.
func TestRowStampsOf(t *testing.T) {
	full := rowSpec{ver: 7, tf: 30, tt: 40, da: 50}
	if got := storecontract.NodeRowStamps(currentNodeRow(1, full)); got != full.want() {
		t.Fatalf("NodeRowStamps = %+v, want %+v", got, full.want())
	}
	if got := storecontract.RelRowStamps(currentRelRow(1, full)); got != full.want() {
		t.Fatalf("RelRowStamps = %+v, want %+v", got, full.want())
	}
	compact := rowSpec{ver: 3, tf: 60, hashed: true}
	n := currentNodeRow(2, compact).CompactFrozenCopy()
	if got := storecontract.NodeRowStamps(n); got != compact.want() {
		t.Fatalf("NodeRowStamps(compact) = %+v, want %+v", got, compact.want())
	}
	r := currentRelRow(2, compact).CompactFrozenCopy()
	if got := storecontract.RelRowStamps(r); got != compact.want() {
		t.Fatalf("RelRowStamps(compact) = %+v, want %+v", got, compact.want())
	}
	bare := types.NewNode(currentNodeID(3), 1, nil)
	bare.SetVersion(2)
	if got := storecontract.NodeRowStamps(bare); got != (storecontract.RowStamps{Version: 2}) {
		t.Fatalf("NodeRowStamps(no temporal) = %+v", got)
	}
	bareRel := types.NewRelationship(currentRelID(3), 5, currentNodeID(1), currentNodeID(2))
	bareRel.SetVersion(2)
	if got := storecontract.RelRowStamps(bareRel); got != (storecontract.RowStamps{Version: 2}) {
		t.Fatalf("RelRowStamps(no temporal) = %+v", got)
	}
	if got := storecontract.NodeRowStamps(nil); got != (storecontract.RowStamps{}) {
		t.Fatalf("NodeRowStamps(nil) = %+v", got)
	}
	if got := storecontract.RelRowStamps(nil); got != (storecontract.RowStamps{}) {
		t.Fatalf("RelRowStamps(nil) = %+v", got)
	}
}
