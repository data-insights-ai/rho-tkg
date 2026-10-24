package badger

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Backlog 42: badger NodeCurrentStamps / RelCurrentStamps read the entity
// cache first and, on a miss, only the stored row's temporal block. Every test
// compares the answer with the row GetNode / GetRelationship returns.

const curID = int64(1000) << 22

func curNode(id int64, ver uint32, tf, tt, da types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(curID+id)), 1, nil)
	n.SetVersion(ver)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: tf, TxTo: tt, DeletedAt: da})
	return n
}

func curRel(id int64, ver uint32, tf, tt, da types.Instant) *types.Relationship {
	r := types.NewRelationship(types.RelID(snowflake.ID(curID+id)), 1, types.NodeID(snowflake.ID(curID+1)), types.NodeID(snowflake.ID(curID+2)))
	r.SetVersion(ver)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: tf, TxTo: tt, DeletedAt: da})
	return r
}

// curKind adapts the node and relationship doors (testing rule 2).
type curKind struct {
	name    string
	put     func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error
	replace func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error
	del     func(bs *Store, id int64) error
	stamps  func(bs *Store, id int64) (storecontract.RowStamps, error)
	get     func(bs *Store, id int64) (storecontract.RowStamps, error)
	evict   func(bs *Store, id int64)
	key     func(id int64) []byte
	missing error
}

func curKinds() []curKind {
	return []curKind{
		{
			name: "node",
			put: func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				return bs.PutNode(curNode(id, ver, tf, tt, da))
			},
			replace: func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				return bs.ReplaceNode(curNode(id, ver, tf, tt, da))
			},
			del: func(bs *Store, id int64) error { return bs.DeleteNode(types.NodeID(snowflake.ID(curID + id))) },
			stamps: func(bs *Store, id int64) (storecontract.RowStamps, error) {
				return bs.NodeCurrentStamps(types.NodeID(snowflake.ID(curID + id)))
			},
			get: func(bs *Store, id int64) (storecontract.RowStamps, error) {
				n, err := bs.GetNode(types.NodeID(snowflake.ID(curID + id)))
				return storecontract.NodeRowStamps(n), err
			},
			evict:   func(bs *Store, id int64) { bs.nodeCache.EvictForTest(snowflake.ID(curID + id)) },
			key:     func(id int64) []byte { return storeutil.NodeKey(snowflake.ID(curID + id)) },
			missing: ErrNodeNotFound,
		},
		{
			name: "rel",
			put: func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				return bs.PutRelationship(curRel(id, ver, tf, tt, da))
			},
			replace: func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				return bs.ReplaceRelationship(curRel(id, ver, tf, tt, da))
			},
			del: func(bs *Store, id int64) error { return bs.DeleteRelationship(types.RelID(snowflake.ID(curID + id))) },
			stamps: func(bs *Store, id int64) (storecontract.RowStamps, error) {
				return bs.RelCurrentStamps(types.RelID(snowflake.ID(curID + id)))
			},
			get: func(bs *Store, id int64) (storecontract.RowStamps, error) {
				r, err := bs.GetRelationship(types.RelID(snowflake.ID(curID + id)))
				return storecontract.RelRowStamps(r), err
			},
			evict:   func(bs *Store, id int64) { bs.relCache.EvictForTest(snowflake.ID(curID + id)) },
			key:     func(id int64) []byte { return storeutil.RelKey(snowflake.ID(curID + id)) },
			missing: ErrRelNotFound,
		},
	}
}

func newCurStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	cfg.InMemory = true
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for _, id := range []int64{1, 2} {
		if err := bs.PutNode(curNode(id, 0, 1, 0, 0)); err != nil {
			t.Fatal(err)
		}
	}
	return bs
}

func wantCur(t *testing.T, k curKind, bs *Store, step string, id int64, want storecontract.RowStamps) {
	t.Helper()
	got, err := k.stamps(bs, id)
	if err != nil || got != want {
		t.Fatalf("%s %s: stamps = (%+v, %v), want %+v", k.name, step, got, err, want)
	}
	if row, err := k.get(bs, id); err != nil || row != want {
		t.Fatalf("%s %s: scenario wrong: the current row is (%+v, %v), want %+v", k.name, step, row, err, want)
	}
}

func wantCurMissing(t *testing.T, k curKind, bs *Store, step string, id int64) {
	t.Helper()
	if got, err := k.stamps(bs, id); !errors.Is(err, k.missing) {
		t.Fatalf("%s %s: stamps = (%+v, %v), want %v (GetNode has no row)", k.name, step, got, err, k.missing)
	}
	if _, err := k.get(bs, id); !errors.Is(err, k.missing) {
		t.Fatalf("%s %s: scenario wrong: Get = %v", k.name, step, err)
	}
}

// The cache answers before the committed row: a write the flush has not
// committed yet (a lowering Replace, a delete whose tombstone sits in the
// cache) must win over the older committed row; a faulty read that asks
// badger first, or skips the cache's tombstone, answers the committed row.
// After the flush the evicted row is read from badger and equals the cached
// answer.
func TestCurrentStamps_CacheBeforeCommittedRow(t *testing.T) {
	for _, k := range curKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newCurStore(t, Config{FlushInterval: time.Hour})
			const a = 10
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(k.put(bs, a, 0, 100, 200, 300))
			must(bs.Flush())
			k.evict(bs, a)
			wantCur(t, k, bs, "committed, evicted", a, storecontract.RowStamps{TxFrom: 100, TxTo: 200, DeletedAt: 300})
			must(k.replace(bs, a, 1, 50, 0, 0)) // unflushed: the committed row still says 100
			wantCur(t, k, bs, "lowering replace in the buffer", a, storecontract.RowStamps{TxFrom: 50, Version: 1})
			must(k.del(bs, a)) // unflushed: the committed row is still there
			wantCurMissing(t, k, bs, "delete in the buffer", a)
			must(bs.Flush())
			k.evict(bs, a)
			wantCurMissing(t, k, bs, "delete committed", a)
			must(k.put(bs, a, 4, 700, 0, 0))
			wantCur(t, k, bs, "re-created in the buffer", a, storecontract.RowStamps{TxFrom: 700, Version: 4})
			must(bs.Flush())
			k.evict(bs, a)
			wantCur(t, k, bs, "re-created, committed, evicted", a, storecontract.RowStamps{TxFrom: 700, Version: 4})
		})
	}
}

// writeRawRow replaces the committed row of id with raw and evicts it, so the
// next read decodes raw (the row stays a live ID in the store's index).
func writeRawRow(t *testing.T, k curKind, bs *Store, id int64, raw []byte) {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(k.key(id), raw) }); err != nil {
		t.Fatal(err)
	}
	k.evict(bs, id)
}

// The stored forms a current row takes: a row without a temporal block (zero
// stamps, the version still read), a row whose stamps live only in the
// patched v2 tail (the ingest applier's pre-encoded put), and rows that fail
// the full decode — a newer format version and corrupt bytes — which must fail
// here too instead of answering zero stamps.
func TestCurrentStamps_StoredRowForms(t *testing.T) {
	for _, k := range curKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newCurStore(t, Config{FlushInterval: time.Hour})
			const a = 10
			if err := k.put(bs, a, 2, 100, 0, 0); err != nil {
				t.Fatal(err)
			}
			var bare []byte
			var err error
			if k.name == "node" {
				n := types.NewNode(types.NodeID(snowflake.ID(curID+a)), 1, nil)
				n.SetVersion(6)
				bare, err = storeutil.MarshalNodeWire(n)
			} else {
				r := types.NewRelationship(types.RelID(snowflake.ID(curID+a)), 1, types.NodeID(snowflake.ID(curID+1)), types.NodeID(snowflake.ID(curID+2)))
				r.SetVersion(6)
				bare, err = storeutil.MarshalRelWire(r)
			}
			if err != nil {
				t.Fatal(err)
			}
			writeRawRow(t, k, bs, a, bare)
			wantCur(t, k, bs, "no temporal block", a, storecontract.RowStamps{Version: 6})

			var newer []byte
			if k.name == "node" {
				w := storeutil.NodeToWire(curNode(a, 3, 5, 0, 0))
				w.FormatVersion = storeutil.OpenVoidWireFormatVersion + 1
				newer, err = storeutil.MarshalNodeWireStruct(w)
			} else {
				w := storeutil.RelToWire(curRel(a, 3, 5, 0, 0))
				w.FormatVersion = storeutil.OpenVoidWireFormatVersion + 1
				newer, err = storeutil.MarshalRelWireStruct(w)
			}
			if err != nil {
				t.Fatal(err)
			}
			writeRawRow(t, k, bs, a, newer)
			_, gerr := k.get(bs, a)
			k.evict(bs, a)
			if got, err := k.stamps(bs, a); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) || !errors.Is(gerr, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("newer format: stamps = (%+v, %v), Get %v; want ErrWireFormatVersionUnsupported from both", got, err, gerr)
			}
			// A row whose temporal block decodes but which the full decode
			// rejects (keyed to the wrong ID; a zero label / type token): the
			// stamps answer from the temporal block while Get fails. Documented
			// asymmetry (store.CurrentStampsCapability); a faulty "fail closed"
			// change or a full decode on the miss path flips it.
			for _, bad := range []struct {
				name string
				wire func() ([]byte, error)
			}{
				{"keyed to the wrong ID", func() ([]byte, error) {
					if k.name == "node" {
						return storeutil.MarshalNodeWireStruct(storeutil.NodeToWire(curNode(a+1, 7, 70, 71, 72)))
					}
					return storeutil.MarshalRelWireStruct(storeutil.RelToWire(curRel(a+1, 7, 70, 71, 72)))
				}},
				{"zero label or type token", func() ([]byte, error) {
					if k.name == "node" {
						w := storeutil.NodeToWire(curNode(a, 7, 70, 71, 72))
						w.PrimaryLabel = 0
						return storeutil.MarshalNodeWireStruct(w)
					}
					w := storeutil.RelToWire(curRel(a, 7, 70, 71, 72))
					w.RelType = 0
					return storeutil.MarshalRelWireStruct(w)
				}},
			} {
				raw, err := bad.wire()
				if err != nil {
					t.Fatal(err)
				}
				writeRawRow(t, k, bs, a, raw)
				_, gerr := k.get(bs, a)
				k.evict(bs, a)
				got, err := k.stamps(bs, a)
				if gerr == nil || err != nil || got != (storecontract.RowStamps{TxFrom: 70, TxTo: 71, DeletedAt: 72, Version: 7}) {
					t.Fatalf("%s: stamps = (%+v, %v), Get %v; want the temporal block's stamps while Get fails", bad.name, got, err, gerr)
				}
			}
			writeRawRow(t, k, bs, a, []byte{0x85, 0xc1})
			_, gerr = k.get(bs, a)
			k.evict(bs, a)
			if got, err := k.stamps(bs, a); err == nil || gerr == nil {
				t.Fatalf("corrupt row: stamps = (%+v, %v), Get %v; want errors from both", got, err, gerr)
			}
		})
	}
	t.Run("node/pre-encoded patched tail", func(t *testing.T) {
		bs := newCurStore(t, Config{FlushInterval: time.Hour})
		reg := registrypkg.NewPropertyKeyRegistry()
		bs.SetPropertyKeyRegistry(reg)
		n := curNode(20, 0, 900, 950, 0)
		buf, err := storeutil.PreEncodeNodeWireV2WithKeys(n, reg)
		if err != nil {
			t.Fatal(err)
		}
		if err := storeutil.PatchWireTemporalTail(buf, 900, 950); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNodesBatchPreEncoded([]*types.Node{n}, [][]byte{buf}); err != nil {
			t.Fatal(err)
		}
		k := curKinds()[0]
		wantCur(t, k, bs, "pre-encoded, cached", 20, storecontract.RowStamps{TxFrom: 900, TxTo: 950})
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		k.evict(bs, 20)
		got, err := k.stamps(bs, 20)
		if err != nil || got != (storecontract.RowStamps{TxFrom: 900, TxTo: 950}) {
			t.Fatalf("pre-encoded, evicted: stamps = (%+v, %v), want TxFrom 900, TxTo 950 from the patched tail", got, err)
		}
	})
}

// A cache miss reads one key and decodes the temporal block only: a handful
// of allocations (the badger read transaction), not the full row decode's 20+
// (properties, labels, hashes). The bound is the backlog-42 target for the
// whole LatestStamps call.
func TestCurrentStamps_MissReadsTheTemporalBlockOnly(t *testing.T) {
	for _, k := range curKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newCurStore(t, Config{FlushInterval: time.Hour})
			const a = 10
			if k.name == "node" {
				n := curNode(a, 3, 100, 0, 0)
				n.SetProperties(mustPropertySlice(t, map[string]any{"name": "a fairly long string value", "n": int64(7), "tags": []any{"x", "y", "z"}}))
				n.SetIntegrity(&types.NodeIntegrity{Hash: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", PrevHash: "p"})
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			} else {
				r := curRel(a, 3, 100, 0, 0)
				r.SetProperties(mustPropertySlice(t, map[string]any{"name": "a fairly long string value", "n": int64(7), "tags": []any{"x", "y", "z"}}))
				r.SetIntegrity(&types.RelIntegrity{Hash: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", PrevHash: "p"})
				if err := bs.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			var got storecontract.RowStamps
			var err error
			allocs := testing.AllocsPerRun(200, func() {
				k.evict(bs, a)
				got, err = k.stamps(bs, a)
			})
			if err != nil || got != (storecontract.RowStamps{TxFrom: 100, Version: 3}) {
				t.Fatalf("miss = (%+v, %v)", got, err)
			}
			if allocs > 8 {
				t.Fatalf("a cache miss allocates %.0f per call, want <= 8 (temporal block only)", allocs)
			}
			if _, err := k.get(bs, a); err != nil { // GetNode fills the cache; the miss path does not
				t.Fatal(err)
			}
			if hit := testing.AllocsPerRun(200, func() { got, err = k.stamps(bs, a) }); hit != 0 {
				t.Fatalf("a cache hit allocates %.0f per call, want 0", hit)
			}
		})
	}
}

// Concurrent writers replace the rows (version and TxFrom rising together:
// TxFrom = 1000 + version) while a small cache evicts and the flush loop
// commits underneath; readers' answers must always be a row the writer wrote
// (the stamps match the version) and never go back to an older version. After
// the writers stop every answer equals Get. Run with -race.
func TestCurrentStamps_ConcurrentWriters(t *testing.T) {
	for _, k := range curKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newCurStore(t, Config{FlushInterval: time.Millisecond, CacheCapacity: 2})
			const ids = 6
			for id := int64(10); id < 10+ids; id++ {
				if err := k.put(bs, id, 0, 1000, 0, 0); err != nil {
					t.Fatal(err)
				}
			}
			var stop atomic.Bool
			var wg sync.WaitGroup
			for w := int64(0); w < ids; w++ {
				wg.Add(1)
				go func(id int64) {
					defer wg.Done()
					for v := uint32(1); v <= 150; v++ {
						if err := k.replace(bs, id, v, 1000+types.Instant(v), 0, 0); err != nil {
							t.Errorf("replace: %v", err)
							return
						}
					}
				}(10 + w)
			}
			errs := make(chan error, 4)
			var readers sync.WaitGroup
			for range 3 {
				readers.Add(1)
				go func() {
					defer readers.Done()
					last := make([]uint32, ids)
					for !stop.Load() {
						for i := int64(0); i < ids; i++ {
							s, err := k.stamps(bs, 10+i)
							if err != nil || s.TxFrom != 1000+types.Instant(s.Version) || s.Version < last[i] {
								errs <- errors.New(k.name + ": torn or stale answer")
								return
							}
							last[i] = s.Version
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
			for id := int64(10); id < 10+ids; id++ {
				wantCur(t, k, bs, "final", id, storecontract.RowStamps{TxFrom: 1150, Version: 150})
			}
		})
	}
}

// Errors: an invalid ID, a closed store, a store mid-close.
func TestCurrentStamps_BadgerErrors(t *testing.T) {
	bs := newCurStore(t, Config{FlushInterval: time.Hour})
	for _, k := range curKinds() {
		if _, err := k.stamps(bs, -curID); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("%s zero id = %v, want ErrInvalidStoreMutation", k.name, err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	for _, k := range curKinds() {
		if _, err := k.stamps(bs, 1); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("%s closed = %v, want ErrStoreClosed", k.name, err)
		}
	}
	var zero Store
	if _, err := zero.NodeCurrentStamps(1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("zero-value store = %v, want ErrStoreClosed", err)
	}
	if _, err := zero.RelCurrentStamps(1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("zero-value store rel = %v, want ErrStoreClosed", err)
	}
}
