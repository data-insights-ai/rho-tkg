package badger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func asOfIndexNextRaw(t *testing.T, bs *Store, k asOfBudgetKind) []byte {
	t.Helper()
	raw, err := k.raw(bs, 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if k.name == "node" {
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			t.Fatal(err)
		}
		w.Version, w.TxFrom, w.TxTo = 2, 300, 400
		raw, err = storepkg.MarshalNodeWireStruct(w)
	} else {
		var w storepkg.RelWire
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			t.Fatal(err)
		}
		w.Version, w.TxFrom, w.TxTo = 2, 300, 400
		raw, err = storepkg.MarshalRelWireStruct(w)
	}
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// A crash can leave an applied history row while neither a stage marker nor
// staging keys survive. Only the existing clean-close header can vouch for
// the proof's complete state; a read-only/budgeted open must not repair it.
func TestAsOfIndexUncleanOpenDeclinesBeforeWrite(t *testing.T) {
	testtier.Slow(t)
	for _, k := range asOfBudgetKinds() {
		for _, format := range []uint16{18, 17, 0, 3} {
			t.Run(fmt.Sprintf("%s/format%d", k.name, format), func(t *testing.T) {
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
				raw := asOfIndexNextRaw(t, bs, k)
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				db, err := badgerv4.Open(buildBadgerOptions(cfg))
				if err != nil {
					t.Fatal(err)
				}
				key := storepkg.HistNodeKey(100, 2)
				key[0] = asOfIndexKind(k)
				if err := db.Update(func(txn *badgerv4.Txn) error {
					if err := txn.Set(key, raw); err != nil {
						return err
					}
					if format == 0 {
						return txn.Delete(storepkg.WireFormatVersionKey)
					}
					if format < 18 {
						if err := txn.Set(storepkg.WireFormatVersionKey, formatMarkerValue(format)); err != nil {
							return err
						}
						if format == 17 {
							header, err := asOfStoredValue(txn, openImageHeaderKey)
							if err != nil {
								return err
							}
							// An older writer can clean-close after a lost marker;
							// its newest header cannot certify this index format.
							return txn.Set(openImageHeaderKey, header)
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				before := db.MaxVersion()
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				for _, budgeted := range []bool{false, true} {
					probe := cfg
					probe.ReadOnly = !budgeted
					if budgeted {
						_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
						probe.OpenReadTally = m.Tally("unclean indexed shard")
					}
					opened, err := New(probe)
					if budgeted && !errors.Is(err, storecontract.ErrReadBudget) {
						t.Fatalf("budgeted open err %v, want %v", err, storecontract.ErrReadBudget)
					}
					if !budgeted {
						// The read-only handle opens and answers through the
						// admitted scan; it writes no recovery generation.
						if err != nil {
							t.Fatalf("read-only open: %v", err)
						}
						_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
						v, ok, err := k.read(opened, 100, 350, m.Tally("read-only unclean"))
						if err != nil || !ok || v != 2 {
							t.Fatalf("read-only row v%d exact payload %v err %v", v, ok, err)
						}
					}
					if opened != nil {
						_ = opened.Close()
					}
				}
				db, err = badgerv4.Open(buildBadgerOptions(cfg))
				if err != nil {
					t.Fatal(err)
				}
				if got := db.MaxVersion(); got != before {
					t.Fatalf("declined open wrote version %d, before %d", got, before)
				}
				_ = db.Close()
				bs, err = New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer bs.Close()
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				v, ok, err := k.read(bs, 100, 350, m.Tally("recovered old proof"))
				if err != nil || !ok || v != 2 {
					t.Fatalf("recovered row v%d exact payload %v err %v", v, ok, err)
				}
				if err := bs.view(func(txn *badgerv4.Txn) error {
					raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfGenerationKey)
					if err != nil {
						return err
					}
					generation, err := asOfGeneration(raw)
					if err == nil && generation != 1 {
						t.Errorf("recovery generation %d, want 1", generation)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestAsOfIndexCloseAfterCurrentRead(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 100, 0, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			bs.asOfAfterCurrentTestHook = func() { _ = bs.Close() }
			done := make(chan error, 1)
			go func() {
				_, _, err := k.read(bs, 100, 150, nil)
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, ErrStoreClosed) {
					t.Fatalf("closed reader returned %v", err)
				}
			case <-time.After(250 * time.Millisecond):
				t.Fatal("as-of opened a transaction on a closed database")
			}
		})
	}
}

func TestAsOfIndexWALRemnantOverridesCloseHeader(t *testing.T) {
	testtier.Slow(t)
	for _, k := range asOfBudgetKinds() {
		for _, migrate := range []bool{false, true} {
			t.Run(k.name+map[bool]string{false: "/open", true: "/migration-before-open"}[migrate], func(t *testing.T) {
				cfg := Config{Dir: t.TempDir()}
				bs, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				for v := range uint32(2) {
					if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
				next := asOfIndexNextRaw(t, bs, k)
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				db, err := badgerv4.Open(buildBadgerOptions(cfg))
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Update(func(txn *badgerv4.Txn) error {
					header, err := asOfStoredValue(txn, openImageHeaderKey)
					if err != nil {
						return err
					}
					key := storepkg.HistNodeKey(100, 2)
					key[0] = asOfIndexKind(k)
					if err := txn.Set(key, next); err != nil {
						return err
					}
					return txn.Set(openImageHeaderKey, header) // newest header survived a partial apply
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				// An empty WAL header is still evidence of an interrupted close.
				// Badger can remove it during open before the guard runs.
				if err := os.WriteFile(filepath.Join(cfg.Dir, "00999.mem"), make([]byte, 20), 0o600); err != nil {
					t.Fatal(err)
				}
				if migrate {
					cfg.MemTableSize, cfg.WALReplayMemTableSize = 8<<20, 64<<20
					if err := MigrateOversizedWAL(cfg); err != nil {
						t.Fatal(err)
					}
				}
				bs, err = New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer bs.Close()
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				v, ok, err := k.read(bs, 100, 350, m.Tally("partial close recovery"))
				if err != nil || !ok || v != 2 {
					t.Fatalf("partial-close answer v%d exact payload %v err %v", v, ok, err)
				}
				if err := bs.view(func(txn *badgerv4.Txn) error {
					raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfGenerationKey)
					if err != nil {
						return err
					}
					generation, err := asOfGeneration(raw)
					if generation == 0 {
						t.Error("WAL remnant did not invalidate a newest close-header proof")
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestAsOfIndexZeroWALBudgetRefusesBeforeOpen(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	if err := os.WriteFile(filepath.Join(cfg.Dir, "00001.mem"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
	cfg.OpenReadTally = m.Tally("zero WAL")
	bs, err := New(cfg)
	if bs != nil {
		_ = bs.Close()
	}
	if !errors.Is(err, storecontract.ErrReadBudget) {
		t.Fatalf("zero WAL opened: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Dir, badgerv4.ManifestFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("declined WAL probe touched database: %v", err)
	}
}

func TestAsOfIndexRecoverySyncBarrier(t *testing.T) {
	for _, discard := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "discard"}[discard], func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := asOfBudgetKinds()[0].put(bs, 100, 0, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("recovery sync refused")
			calls := 0
			asOfRecoverySyncTestHook = func() error {
				calls++
				if err := bs.view(func(txn *badgerv4.Txn) error {
					raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfGenerationKey)
					if err != nil {
						return err
					}
					gen, err := asOfGeneration(raw)
					if gen != 1 {
						t.Errorf("sync preceded generation update: %d", gen)
					}
					return err
				}); err != nil {
					t.Error(err)
				}
				return injected
			}
			defer func() { asOfRecoverySyncTestHook = nil }()
			var err error
			if discard {
				err = discardStagedFlush(bs.db)
			} else {
				_, _, err = prepareAsOfIndexOpen(bs.db, storepkg.HistoryAsOfStoreFormatVersion, false, true, nil)
			}
			if !errors.Is(err, injected) || calls != 1 {
				t.Fatalf("recovery accepted before durable generation: calls %d err %v", calls, err)
			}
		})
	}
}

func TestAsOfIndexWritableReadOpenRequiresWriterStamps(t *testing.T) {
	testtier.Slow(t)
	for _, anchor := range []bool{false, true} {
		t.Run(map[bool]string{false: "wire", true: "anchor"}[anchor], func(t *testing.T) {
			cfg := Config{Dir: t.TempDir()}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(types.NewNode(100, 1, nil)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := badgerv4.Open(buildBadgerOptions(cfg))
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(txn *badgerv4.Txn) error {
				header, err := asOfStoredValue(txn, openImageHeaderKey)
				if err != nil {
					return err
				}
				if !anchor {
					if err := txn.Set(storepkg.WireFormatVersionKey, formatMarkerValue(3)); err != nil {
						return err
					}
				}
				return txn.Set(openImageHeaderKey, header)
			}); err != nil {
				t.Fatal(err)
			}
			before := db.MaxVersion()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			cfg.OpenReadTally, cfg.HistoryDeltaEncoding = m.Tally("retained writable shard"), anchor
			opened, err := New(cfg)
			if opened != nil {
				_ = opened.Close()
			}
			if !errors.Is(err, storecontract.ErrReadBudget) {
				t.Fatalf("writable handle opened without writer stamp: %v", err)
			}
			db, err = badgerv4.Open(buildBadgerOptions(cfg))
			if err != nil {
				t.Fatal(err)
			}
			if db.MaxVersion() != before {
				t.Error("declined open wrote metadata")
			}
			_ = db.Close()
			cfg.OpenReadTally = nil
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := bs.GetNode(100); err != nil || n.ID() != 100 {
				t.Fatalf("ordinary repair lost control node: %v", err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A read-only open cannot write the recovery generation after an unclean
// close. It opens anyway and its handle never reads a proof: every as-of read
// takes the admitted scan. One writable open and a clean close later a
// read-only handle uses the index again.
func TestAsOfIndexReadOnlyAfterUncleanCloseDeclines(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), FlushInterval: time.Hour}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for v := range uint32(4) {
				if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			bs.openImageSuppressed.Store(true) // no close header: an unclean close
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			readOnlyProbes := func(when string) int64 {
				t.Helper()
				ro := cfg
				ro.ReadOnly = true
				opened, err := New(ro)
				if err != nil {
					t.Fatalf("%s: read-only open: %v", when, err)
				}
				defer opened.Close()
				probes, stop := CountAsOfIndexProbesForTest()
				defer stop()
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				v, ok, err := k.read(opened, 100, 250, m.Tally(when))
				if err != nil || !ok || v != 1 {
					t.Fatalf("%s: as-of answer v%d exact payload %v err %v, want v1", when, v, ok, err)
				}
				return probes()
			}
			if got := readOnlyProbes("after unclean close"); got != 0 {
				t.Fatalf("read-only handle after an unclean close probed %d proofs, want 0", got)
			}
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if got := readOnlyProbes("after writable recovery"); got == 0 {
				t.Fatal("read-only handle after a clean close probed no proof: the index is not used")
			}
		})
	}
}
