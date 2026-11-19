package badger

import (
	"errors"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func asOfIndexDiskSize(t *testing.T, bs *Store) (entries, bytes int) {
	t.Helper()
	if err := bs.view(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyHistoryAsOf}})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			entries++
			bytes += len(it.Item().Key()) + int(it.Item().ValueSize())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return entries, bytes
}

func TestAsOfIndexDiskCostAndCleanup(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(3) {
				if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			check := func(wantEntries, wantBytes int) {
				t.Helper()
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				entries, bytes := asOfIndexDiskSize(t, bs)
				if entries != wantEntries || bytes != wantBytes {
					t.Fatalf("disk entries/bytes %d/%d, want %d/%d", entries, bytes, wantEntries, wantBytes)
				}
			}
			const rowBytes = storepkg.HistoryAsOfSeekSize + storepkg.HistoryAsOfAliasSize + 8
			check(7, 3*rowBytes+storepkg.HistoryAsOfProofSize+asOfProofSize)
			t.Logf("steady state: 2 entries/%d logical bytes per history row, 1 entry/%d logical bytes per entity proof; no per-entity RAM mirror", rowBytes, storepkg.HistoryAsOfProofSize+asOfProofSize)
			for range 20 {
				if err := k.put(bs, 100, 1, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
				check(5, 2*rowBytes+storepkg.HistoryAsOfProofSize+1)
			}
			var err error
			if k.name == "node" {
				err = bs.TruncateNodeHistory(100, 0)
			} else {
				err = bs.TruncateRelHistory(100, 0)
			}
			if err != nil {
				t.Fatal(err)
			}
			check(0, 0) // a complete history purge leaves no accelerator residue
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			check(0, 0)
		})
	}
}

func TestAsOfIndexExactEraseLeavesNoResidue(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		for _, trimmed := range []bool{false, true} {
			t.Run(k.name+map[bool]string{false: "/history", true: "/already-trimmed"}[trimmed], func(t *testing.T) {
				bs := newFlushParkStore(t, nil)
				first := uint32(0)
				if trimmed {
					first = 1
				}
				for v := first; v < first+2; v++ {
					if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
				if trimmed {
					var err error
					if k.name == "node" {
						err = bs.TrimNodeHistoryFrom(100, 1)
					} else {
						err = bs.TrimRelHistoryFrom(100, 1)
					}
					if err != nil {
						t.Fatal(err)
					}
					if err := bs.Flush(); err != nil {
						t.Fatal(err)
					}
					if entries, _ := asOfIndexDiskSize(t, bs); entries != 1 {
						t.Fatalf("empty-history fixture needs its invalid proof, got %d keys", entries)
					}
				}
				req := storecontract.ExactErasureRequest{Bounds: badgerExactErasureBounds}
				if k.name == "node" {
					req.NodeIDs = []types.NodeID{100}
				} else {
					req.RelIDs = []types.RelID{100}
				}
				if _, err := bs.ExactErase(req); err != nil {
					t.Fatal(err)
				}
				if entries, bytes := asOfIndexDiskSize(t, bs); entries != 0 || bytes != 0 {
					t.Fatalf("exact erase left %d entries / %d logical bytes", entries, bytes)
				}
			})
		}
	}
}

func TestAsOfIndexEraseOrphanAfterPartialApply(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for _, id := range []int64{100, 101} {
				for v := range uint32(2) {
					if err := k.put(bs, id, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if v, ok, err := k.read(bs, 100, 250, nil); err != nil || !ok || v != 1 {
				t.Fatalf("control v%d %v %v", v, ok, err)
			}
			if err := bs.update(func(txn *badgerv4.Txn) error {
				key := storepkg.HistNodeKey(100, 0)
				key[0] = asOfIndexKind(k)
				if err := txn.Delete(key); err != nil {
					return err
				}
				return invalidateAsOfGeneration(txn)
			}); err != nil {
				t.Fatal(err)
			}
			req := storecontract.ExactErasureRequest{Bounds: badgerExactErasureBounds}
			if k.name == "node" {
				req.NodeIDs = []types.NodeID{100}
			} else {
				req.RelIDs = []types.RelID{100}
			}
			if _, err := bs.ExactErase(req); err != nil {
				t.Fatal(err)
			}
			const foreignBytes = 2*(storepkg.HistoryAsOfSeekSize+storepkg.HistoryAsOfAliasSize+8) + storepkg.HistoryAsOfProofSize + asOfProofSize
			if entries, bytes := asOfIndexDiskSize(t, bs); entries != 5 || bytes != foreignBytes {
				t.Fatalf("expected only untouched foreign accelerator: %d entries/%d bytes", entries, bytes)
			}
			if err := bs.view(func(txn *badgerv4.Txn) error {
				it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyHistoryAsOf}})
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					if id := storepkg.ParseIDFromKey(it.Item().Key(), 2); id != 101 {
						t.Fatalf("erased identity %d retained index key %x", id, it.Item().Key())
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if v, ok, err := k.read(bs, 101, 250, nil); err != nil || !ok || v != 1 {
				t.Fatalf("foreign control v%d %v %v", v, ok, err)
			}
		})
	}
}

func TestAsOfIndexOrphanRelationshipPurge(t *testing.T) {
	for _, mode := range []string{"descriptor", "adjacent", "label"} {
		t.Run(mode, func(t *testing.T) {
			relTypes := registrypkg.NewRelTypeRegistry()
			for _, name := range []string{"A", "B", "C", "D", "R"} {
				if _, err := relTypes.GetOrCreate(name); err != nil {
					t.Fatal(err)
				}
			}
			keys := registrypkg.NewPropertyKeyRegistry()
			bs := newFlushParkStore(t, func(c *Config) {
				c.PropertyKeyRegistry = keys
				c.RelPropertyIndexOnDiskTypes, c.RelTypeNames = []string{"R"}, relTypes
			})
			if err := bs.CreateRelPropertyIndex(5, "payload"); err != nil {
				t.Fatal(err)
			}
			k := asOfBudgetKinds()[1]
			for _, id := range []int64{100, 101} {
				for v := range uint32(2) {
					if err := k.put(bs, id, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := bs.PutNode(types.NewNode(1, 7, nil)); err != nil {
				t.Fatal(err)
			}
			// A torn apply can leave adjacency and accelerator keys without
			// the current relationship row; this is the orphan cleanup arm.
			bs.outIdx[1] = map[types.RelID]types.NodeID{100: 2}
			bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.OutKey(1, 5, 2, 100)})
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			tok, _ := keys.GetOrCreate("payload")
			head := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelSpan, 5, tok)
			if ids := entityIDs(t, bs, storepkg.EntityIndexRelSpan, head); ids != "[100 100 101 101]" {
				t.Fatalf("disk member control %s", ids)
			}
			if v, ok, err := k.read(bs, 100, 250, nil); err != nil || !ok || v != 1 {
				t.Fatalf("control v%d %v %v", v, ok, err)
			}
			if err := bs.ForEachRelTypeTxMember(5, func(types.RelID, types.Instant) bool { return true }); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mode {
			case "descriptor":
				err = bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 100, TypeToken: 5, StartID: 1, EndID: 2})
			case "adjacent":
				_, err = bs.PurgeAdjacentRelsForNode(1)
			case "label":
				_, err = bs.PurgeNodesByLabelBefore(7, 1<<50, 8)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if h, err := bs.GetRelHistory(100); err != nil || len(h) != 0 {
				t.Errorf("orphan purge left %d history versions: %v", len(h), err)
			}
			if _, ok, err := k.read(bs, 100, 250, nil); ok || !errors.Is(err, ErrVersionNotFound) {
				t.Errorf("orphan purge still answers historical relationship: %v %v", ok, err)
			}
			if v, ok, err := k.read(bs, 101, 250, nil); err != nil || !ok || v != 1 {
				t.Fatalf("foreign history changed: v%d %v %v", v, ok, err)
			}
			if entries, bytes := asOfIndexDiskSize(t, bs); entries != 5 || bytes != 155 {
				t.Fatalf("expected only foreign index after orphan purge: %d entries/%d bytes", entries, bytes)
			}
			members := make(map[types.RelID]bool)
			if err := bs.ForEachRelTypeTxMember(5, func(id types.RelID, _ types.Instant) bool { members[id] = true; return true }); err != nil {
				t.Fatal(err)
			}
			if len(members) != 1 || !members[101] {
				t.Fatalf("orphan history still resident in membership sidecar: %v", members)
			}
			if ids := entityIDs(t, bs, storepkg.EntityIndexRelSpan, head); ids != "[101 101]" {
				t.Fatalf("disk members after orphan purge %s", ids)
			}
		})
	}
}

func TestAsOfIndexCorruptionFailsClosed(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		for _, damage := range []string{"proof", "generation", "missing first", "missing middle", "missing row", "malformed seek", "below first", "wrong max"} {
			t.Run(k.name+"/"+damage, func(t *testing.T) {
				bs := newFlushParkStore(t, nil)
				for v := range uint32(3) {
					if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				pin := types.Instant(250)
				kind := asOfIndexKind(k)
				if err := bs.update(func(txn *badgerv4.Txn) error {
					switch damage {
					case "proof":
						return txn.Set(storepkg.HistoryAsOfProofKey(kind, 100), []byte{1})
					case "generation":
						return txn.Set(storepkg.HistoryAsOfGenerationKey, []byte{1})
					case "missing first":
						pin = 150
						return txn.Delete(storepkg.HistoryAsOfSeekKey(kind, 100, 100, 0))
					case "missing middle":
						return txn.Delete(storepkg.HistoryAsOfSeekKey(kind, 100, 200, 1))
					case "missing row":
						key := storepkg.HistNodeKey(100, 1)
						key[0] = kind
						return txn.Delete(key)
					case "malformed seek":
						key := storepkg.HistoryAsOfSeekKey(kind, 100, 225, 1)
						return txn.Set(key[:len(key)-1], nil)
					case "below first":
						pin = 75
						return txn.Set(storepkg.HistoryAsOfSeekKey(kind, 100, 50, 0), nil)
					case "wrong max":
						return txn.Set(storepkg.HistoryAsOfSeekKey(kind, 100, 225, 2), nil)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				_, ok, err := k.read(bs, 100, pin, m.Tally("damaged index"))
				if ok || !errors.Is(err, storecontract.ErrCorruptWire) {
					t.Fatalf("damaged index returned row %v err %v", ok, err)
				}
			})
		}
	}
}

func TestAsOfIndexCumulativeKeysAndStores(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			stores := []*Store{newFlushParkStore(t, nil), newFlushParkStore(t, nil)}
			for _, bs := range stores {
				for _, id := range []int64{100, 101} {
					for v := range uint32(2) {
						if err := k.put(bs, id, v, strings.Repeat("x", 8192)); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			var per uint64
			for _, bs := range stores {
				for _, id := range []int64{100, 101} {
					_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
					v, ok, err := k.read(bs, id, 150, m.Tally("independent control"))
					if err != nil || !ok || v != 0 || (per != 0 && m.Work().HistoryExamined != per) {
						t.Fatalf("independent exact control %d %v %+v %v", v, ok, m.Work(), err)
					}
					per = m.Work().HistoryExamined
				}
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{HistoryExamined: 4*per - 1})
			before := stores[0].HistoryRowDecodesForTest() + stores[1].HistoryRowDecodesForTest()
			calls := 0
			for _, bs := range stores {
				for _, id := range []int64{100, 101} {
					calls++
					v, ok, err := k.read(bs, id, 150, m.Tally("shared two shards"))
					if calls < 4 && (err != nil || !ok || v != 0) {
						t.Fatalf("admitted prefix %d %v %v", v, ok, err)
					}
					if calls == 4 && (ok || !errors.Is(err, storecontract.ErrReadBudget)) {
						t.Fatalf("fourth read %v %v", ok, err)
					}
				}
			}
			if got := stores[0].HistoryRowDecodesForTest() + stores[1].HistoryRowDecodesForTest() - before; got != 3 {
				t.Fatalf("native winner decodes %d, want 3 before fourth refusal", got)
			}
		})
	}
}

func TestAsOfIndexPendingAndFailedFlush(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(2) {
				if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			assertAsOfIndexOracle(t, bs, k)
			parkPendingIntoFlushing(t, bs)
			assertAsOfIndexOracle(t, bs, k)
			// A newer append arrives while the older coherent operation batch
			// is parked; restoring failed ops must retain its newer proof.
			if err := k.put(bs, 100, 2, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err)
			}
			bs.wbMu.Lock()
			failed := bs.flushing
			bs.wbMu.Unlock()
			bs.requeueOps(failed)
			bs.wbMu.Lock()
			bs.dropFlushingLocked()
			bs.wbMu.Unlock()
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexOracle(t, bs, k)
			finish := observeAsOfIndexRead(bs)
			v, ok, err := k.read(bs, 100, 150, nil)
			work := finish()
			if err != nil || !ok || v != 0 || work.visits != 1 || work.rows != 1 {
				t.Fatalf("requeue lost indexed answer v%d %v %+v %v", v, ok, work, err)
			}
		})
	}
}

func TestAsOfIndexUnknownLegacyChain(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 100, 0, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := bs.db.DropPrefix([]byte{storepkg.KeyHistoryAsOf}); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 100, 1, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			// The append marks the chain invalid (one 11-byte key) and certifies
			// nothing: no seek key, no alias.
			if entries, _ := asOfIndexDiskSize(t, bs); entries != 1 {
				t.Fatalf("legacy history: %d index entries, want 1 (the invalid marker)", entries)
			}
			if err := bs.view(func(txn *badgerv4.Txn) error {
				raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfProofKey(asOfIndexKind(k), 100))
				if err == nil && (len(raw) != 1 || raw[0] != 0) {
					t.Errorf("legacy history proof %x, want the invalid marker 00", raw)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexOracle(t, bs, k)
		})
	}
}

func TestAsOfIndexGenerationDeclinesOldProof(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(2) {
				if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := bs.update(invalidateAsOfGeneration); err != nil {
				t.Fatal(err)
			}
			reloadAsOfView(bs) // what the open after that write does
			assertAsOfIndexOracle(t, bs, k)
			if err := k.put(bs, 100, 2, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			// No old-generation alias may be extended into a new valid proof.
			if err := bs.view(func(txn *badgerv4.Txn) error {
				raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfProofKey(asOfIndexKind(k), 100))
				if err != nil {
					return err
				}
				_, valid, err := decodeAsOfProof(raw)
				if valid {
					t.Error("append re-certified an old-generation proof")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
