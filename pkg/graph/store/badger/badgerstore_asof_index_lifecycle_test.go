package badger

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestAsOfIndexCleanReopenPreservesWork(t *testing.T) {
	testtier.Slow(t)
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), FlushInterval: time.Hour, HistoryDeltaEncoding: true, HistoryAnchorInterval: 8}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for v := range uint32(8) {
				if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			var base storecontract.ReadWork
			var nativeBase asOfIndexNativeWork
			for round := range 4 {
				if round != 0 {
					cfg.ReadOnly = round == 1
					_, openMeter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
					cfg.OpenReadTally = openMeter.Tally("clean indexed reopen")
					bs, err = New(cfg)
					if err != nil {
						t.Fatal(err)
					}
				}
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				finish := observeAsOfIndexRead(bs)
				v, ok, err := k.read(bs, 100, 250, m.Tally("cold old pin"))
				native := finish()
				if err != nil || !ok || v != 1 {
					t.Fatalf("round %d: v%d exact payload %v err %v", round, v, ok, err)
				}
				if round == 0 {
					base, nativeBase = m.Work(), native
				} else if m.Work() != base || native != nativeBase {
					t.Fatalf("round %d: work %+v native %+v; warm %+v native %+v", round, m.Work(), native, base, nativeBase)
				}
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
			}
			oldMax := maxReadableStoreFormatVersion
			maxReadableStoreFormatVersion = storepkg.HistoryAsOfStoreFormatVersion - 1
			defer func() { maxReadableStoreFormatVersion = oldMax }()
			cfg.OpenReadTally = nil
			older, err := New(cfg)
			if older != nil {
				_ = older.Close()
			}
			if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("older writer opened indexed format: %v", err)
			}
		})
	}
}

func TestAsOfIndexDeltaAnchorUsesPinnedView(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding = true; c.HistoryAnchorInterval = 8 })
			for v := range uint32(2) {
				if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			raw, err := k.raw(bs, 100, 1)
			if err != nil || storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
				t.Fatalf("delta control: %v", err)
			}
			read := func() (storecontract.ReadWork, asOfIndexNativeWork) {
				t.Helper()
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				finish := observeAsOfIndexRead(bs)
				v, ok, err := k.read(bs, 100, 250, m.Tally("pinned anchor"))
				native := finish()
				if err != nil || !ok || v != 1 {
					t.Fatalf("v%d exact original payload %v err %v", v, ok, err)
				}
				return m.Work(), native
			}
			base, nativeBase := read()
			called := false
			asOfIndexedBeforeDecodeTestHook = func() {
				called = true
				asOfIndexedBeforeDecodeTestHook = nil
				if err := k.put(bs, 100, 0, strings.Repeat("y", 8192)); err != nil {
					t.Fatal(err)
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			defer func() { asOfIndexedBeforeDecodeTestHook = nil }()
			got, native := read()
			if !called || got != base || native != nativeBase || native.anchors == 0 {
				t.Fatalf("pinned read work %+v native %+v; want %+v %+v; hook %v", got, native, base, nativeBase, called)
			}
			// The replacement really committed: a later full anchor read sees y.
			if _, original, err := k.read(bs, 100, 150, nil); err != nil || original {
				t.Fatalf("replacement anchor not visible after pinned read: original %v err %v", original, err)
			}
		})
	}
}

func TestAsOfIndexStagedRecoveryGeneration(t *testing.T) {
	testtier.Slow(t)
	for _, k := range asOfBudgetKinds() {
		for _, mode := range []string{"replay", "discard", "unmarked"} {
			t.Run(k.name+"/"+mode, func(t *testing.T) {
				cfg := Config{Dir: t.TempDir(), FlushInterval: time.Hour}
				bs, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				for v := range uint32(2) {
					if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				if v, ok, err := k.read(bs, 100, 250, nil); err != nil || !ok || v != 1 {
					t.Fatalf("control v%d %v %v", v, ok, err)
				}
				if err := k.put(bs, 100, 2, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
				parkPendingIntoFlushing(t, bs)
				var entries []flushEntry
				for _, op := range bs.flushing {
					entries = append(entries, flushEntry{key: op.key, value: op.value, del: op.opType == writeOpDelete})
				}
				step := stagedStepMarked
				if mode == "unmarked" {
					step = stagedStepStaged
				}
				injected := failAt(bs, step)
				if _, err := bs.commitStaged(entries); !errors.Is(err, injected) {
					t.Fatalf("stage fault: %v", err)
				}
				// Only the history row survives an unsynced partial apply. The
				// old proof and seek entries alone cannot describe that row.
				key := storepkg.HistNodeKey(100, 2)
				key[0] = asOfIndexKind(k)
				if err := bs.update(func(txn *badgerv4.Txn) error { return txn.Set(key, bs.flushing[string(key)].value) }); err != nil {
					t.Fatal(err)
				}
				crashStop(bs)
				if mode == "discard" {
					if dropped, err := DiscardStagedFlush(cfg); err != nil || !dropped {
						t.Fatalf("discard %v %v", dropped, err)
					}
					db, err := badgerv4.Open(buildBadgerOptions(cfg))
					if err != nil {
						t.Fatal(err)
					}
					if err := db.View(func(txn *badgerv4.Txn) error {
						raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfGenerationKey)
						if err != nil {
							return err
						}
						gen, err := asOfGeneration(raw)
						if gen == 0 {
							t.Error("discard left old generation trusted")
						}
						return err
					}); err != nil {
						t.Fatal(err)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
				}
				bs, err = New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer bs.Close()
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				v, ok, err := k.read(bs, 100, 350, m.Tally("recovered indexed history"))
				if err != nil || !ok || v != 2 {
					t.Fatalf("recovered v%d exact payload %v work %+v err %v", v, ok, m.Work(), err)
				}
				if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
					t.Fatalf("%d staging keys survived", n)
				}
				if marker, err := readStageMarker(bs.db); err != nil || marker != nil {
					t.Fatalf("marker survived: %x %v", marker, err)
				}
			})
		}
	}
}
