package badger

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestHistoryBoundsCapability(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	s := bs.NewReadSnapshot()
	defer s.Close()
	if _, ok := s.(storecontract.ReadSnapshotHistoryBoundsCapability); !ok {
		t.Fatal("native snapshot does not expose admitted history bounds")
	}
}

type historyBoundsAnswer struct {
	version uint32
	newest  types.Instant
	known   bool
}

func historyBoundsRead(t *testing.T, bs *Store, k asOfBudgetKind, id int64, tally *storecontract.ReadTally) (historyBoundsAnswer, error) {
	t.Helper()
	s := bs.NewReadSnapshot()
	defer s.Close()
	s.(storecontract.ReadSnapshotMeterCapability).MeterReads(tally)
	b, ok := s.(storecontract.ReadSnapshotHistoryBoundsCapability)
	if !ok {
		t.Fatal("native snapshot does not expose admitted history bounds")
	}
	var a historyBoundsAnswer
	var err error
	if k.name == "node" {
		a.version, a.newest, a.known, err = b.NodeHistoryBounds(types.NodeID(id))
	} else {
		a.version, a.newest, a.known, err = b.RelHistoryBounds(types.RelID(id))
	}
	return a, err
}

func putBoundsHistory(t *testing.T, bs *Store, k asOfBudgetKind, id int64, from, to uint32) {
	t.Helper()
	for v := from; v < to; v++ {
		if err := k.put(bs, id, v, strings.Repeat("x", 8192)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
}

// The same finite allowance admits two and eight versions. Independent native
// counters prove that no history entries, temporal skeletons or rows are read.
func TestHistoryBoundsGrowthAndAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		for _, generation := range []bool{false, true} {
			t.Run(k.name+map[bool]string{false: "/initial", true: "/generation"}[generation], func(t *testing.T) {
				bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding = true; c.HistoryAnchorInterval = 8 })
				bytes, decodes := uint64(asOfProofSize), int64(1)
				if generation {
					if err := bs.update(invalidateAsOfGeneration); err != nil {
						t.Fatal(err)
					}
					reloadAsOfView(bs) // what the open after that write does
					bytes, decodes = bytes+8, decodes+1
				}
				read := func(b storecontract.ReadBudget) (historyBoundsAnswer, storecontract.ReadWork, asOfIndexNativeWork, error) {
					_, m := storecontract.WithReadBudget(t.Context(), b)
					finish := observeAsOfIndexRead(bs)
					a, err := historyBoundsRead(t, bs, k, 100, m.Tally("history bounds"))
					return a, m.Work(), finish(), err
				}
				wantWork := storecontract.ReadWork{Candidates: 2, BufferBytes: bytes, DecodedBytes: bytes}
				wantNative := asOfIndexNativeWork{probes: 2, indexDecodes: decodes, indexCopies: int64(bytes)} // #nosec G115 -- at most49 bytes
				exact := storecontract.ReadBudget{Candidates: 2, BufferBytes: bytes, DecodedBytes: bytes, HistoryExamined: 2}
				for _, n := range []uint32{2, 8} {
					from := uint32(0)
					if n == 8 {
						from = 2
					}
					putBoundsHistory(t, bs, k, 100, from, n)
					want := historyBoundsAnswer{n - 1, types.Instant(n+1) * 100, true}
					if got, err := historyBoundsRead(t, bs, k, 100, nil); err != nil || got != want {
						t.Fatalf("plain %d versions: %+v, %v; want %+v", n, got, err, want)
					}
					for _, b := range []storecontract.ReadBudget{{}, exact} {
						got, work, native, err := read(b)
						if err != nil || got != want || work != wantWork || native != wantNative {
							t.Fatalf("%d versions: %+v, work %+v, native %+v, %v; want %+v, %+v, %+v", n, got, work, native, err, want, wantWork, wantNative)
						}
					}
				}
				beforeCopy := asOfIndexNativeWork{probes: 1}
				beforeDecode := asOfIndexNativeWork{probes: 1, indexCopies: asOfProofSize}
				if generation {
					beforeCopy = asOfIndexNativeWork{probes: 2, indexCopies: asOfProofSize, indexDecodes: 1}
					beforeDecode = asOfIndexNativeWork{probes: 2, indexCopies: asOfProofSize + 8, indexDecodes: 1}
				}
				for _, tc := range []struct {
					name string
					b    storecontract.ReadBudget
					want asOfIndexNativeWork
				}{
					{"candidates", storecontract.ReadBudget{Candidates: 1}, asOfIndexNativeWork{probes: 1, indexCopies: asOfProofSize, indexDecodes: 1}},
					{"buffer bytes", storecontract.ReadBudget{BufferBytes: bytes - 1}, beforeCopy},
					{"decoded bytes", storecontract.ReadBudget{DecodedBytes: bytes - 1}, beforeDecode},
				} {
					a, _, native, err := read(tc.b)
					var budgetErr *storecontract.ReadBudgetError
					if !errors.As(err, &budgetErr) || budgetErr.Limit != tc.name || a != (historyBoundsAnswer{}) || native != tc.want {
						t.Fatalf("one-under %s: %+v, native %+v, %v; want no proof and %+v", tc.name, a, native, err, tc.want)
					}
				}
			})
		}
	}
}

func TestHistoryBoundsCumulativeKeysAndStores(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			stores := []*Store{newFlushParkStore(t, nil), newFlushParkStore(t, nil)}
			for i, bs := range stores {
				putBoundsHistory(t, bs, k, int64(100+i), 0, 2)
			}
			putBoundsHistory(t, stores[0], k, 101, 0, 2)
			for _, second := range stores {
				for _, tc := range []struct {
					name string
					b    storecontract.ReadBudget
					want asOfIndexNativeWork
				}{
					{"candidates", storecontract.ReadBudget{Candidates: 2}, asOfIndexNativeWork{probes: 2, indexCopies: 41, indexDecodes: 1}},
					{"buffer bytes", storecontract.ReadBudget{BufferBytes: 41}, asOfIndexNativeWork{probes: 3, indexCopies: 41, indexDecodes: 1}},
					{"decoded bytes", storecontract.ReadBudget{DecodedBytes: 41}, asOfIndexNativeWork{probes: 3, indexCopies: 82, indexDecodes: 1}},
				} {
					for i, bs := range []*Store{stores[0], second} {
						_, m := storecontract.WithReadBudget(t.Context(), tc.b)
						if a, err := historyBoundsRead(t, bs, k, int64(100+i), m.Tally("bounds")); err != nil || a != (historyBoundsAnswer{1, 300, true}) {
							t.Fatalf("individual %s: %+v, %v", tc.name, a, err)
						}
					}
					_, m := storecontract.WithReadBudget(t.Context(), tc.b)
					finish := observeAsOfIndexRead(stores[0])
					metas, rows := second.HistoryMetaDecodesForTest(), second.HistoryRowDecodesForTest()
					first, firstErr := historyBoundsRead(t, stores[0], k, 100, m.Tally("bounds"))
					last, err := historyBoundsRead(t, second, k, 101, m.Tally("bounds"))
					native := finish()
					if second != stores[0] {
						native.metas += second.HistoryMetaDecodesForTest() - metas
						native.rows += second.HistoryRowDecodesForTest() - rows
					}
					if firstErr != nil || first != (historyBoundsAnswer{1, 300, true}) || !errors.Is(err, storecontract.ErrReadBudget) || last != (historyBoundsAnswer{}) || native != tc.want {
						t.Fatalf("shared %s: first %+v/%v, last %+v/%v, native %+v; want %+v", tc.name, first, firstErr, last, err, native, tc.want)
					}
				}
			}
		})
	}
}

func TestHistoryBoundsDeclinesUnsafeStates(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		for _, state := range []string{"missing", "invalid", "generation", "pending", "flushing", "unrelated pending", "malformed proof", "malformed generation"} {
			t.Run(k.name+"/"+state, func(t *testing.T) {
				bs := newFlushParkStore(t, nil)
				putBoundsHistory(t, bs, k, 100, 0, 2)
				proofKey := storepkg.HistoryAsOfProofKey(asOfIndexKind(k), 100)
				wantErr := false
				want := historyBoundsAnswer{}
				wantNative := asOfIndexNativeWork{}
				switch state {
				case "pending", "flushing", "unrelated pending":
					id := int64(100)
					if state == "unrelated pending" {
						id = 101
						want = historyBoundsAnswer{1, 300, true}
						wantNative = asOfIndexNativeWork{probes: 2, indexCopies: 41, indexDecodes: 1}
					}
					if err := k.put(bs, id, 2, "pending"); err != nil {
						t.Fatal(err)
					}
					if state == "flushing" {
						parkPendingIntoFlushing(t, bs)
					}
				default:
					if err := bs.update(func(txn *badgerv4.Txn) error {
						switch state {
						case "missing":
							wantNative.probes = 1
							return txn.Delete(proofKey)
						case "invalid":
							wantNative = asOfIndexNativeWork{probes: 1, indexCopies: 1, indexDecodes: 1}
							return txn.Set(proofKey, []byte{0})
						case "generation":
							wantNative = asOfIndexNativeWork{probes: 2, indexCopies: 49, indexDecodes: 2}
							return invalidateAsOfGeneration(txn)
						case "malformed proof":
							wantErr = true
							wantNative = asOfIndexNativeWork{probes: 1, indexCopies: 1, indexDecodes: 1}
							return txn.Set(proofKey, []byte{1})
						default:
							wantErr = true
							wantNative = asOfIndexNativeWork{probes: 2, indexCopies: 42, indexDecodes: 2}
							return txn.Set(storepkg.HistoryAsOfGenerationKey, []byte{1})
						}
					}); err != nil {
						t.Fatal(err)
					}
				}
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{HistoryExamined: 2})
				finish := observeAsOfIndexRead(bs)
				a, err := historyBoundsRead(t, bs, k, 100, m.Tally("bounds"))
				native := finish()
				if a != want || native != wantNative || errors.Is(err, storecontract.ErrCorruptWire) != wantErr || !wantErr && err != nil || m.Work().HistoryExamined != 0 {
					t.Fatalf("%s: %+v, native %+v, work %+v, %v; want %+v, %+v, corrupt %v", state, a, native, m.Work(), err, want, wantNative, wantErr)
				}
			})
		}
	}
}

func TestHistoryBoundsCleanAndRecoveredReopen(t *testing.T) {
	testtier.Slow(t)
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), FlushInterval: time.Hour}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			putBoundsHistory(t, bs, k, 100, 0, 8)
			for _, readOnly := range []bool{true, false} {
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				cfg.ReadOnly = readOnly
				bs, err = New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 2, HistoryExamined: 2, BufferBytes: 41, DecodedBytes: 41})
				finish := observeAsOfIndexRead(bs)
				a, err := historyBoundsRead(t, bs, k, 100, m.Tally("cold bounds"))
				native := finish()
				if err != nil || a != (historyBoundsAnswer{7, 900, true}) || native != (asOfIndexNativeWork{probes: 2, indexCopies: 41, indexDecodes: 1}) {
					t.Fatalf("clean reopen: %+v, native %+v, %v", a, native, err)
				}
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			// A write after the latest close header makes recovery invalidate the
			// older proof even when the raw Badger close subsequently removes WALs.
			db, err := badgerv4.Open(badgerv4.DefaultOptions(cfg.Dir).WithLogger(nil))
			if err != nil {
				t.Fatal(err)
			}
			var stamp [8]byte
			binary.BigEndian.PutUint64(stamp[:], 123)
			if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("dirty bounds fixture"), stamp[:]) }); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = bs.Close() }()
			finish := observeAsOfIndexRead(bs)
			a, err := historyBoundsRead(t, bs, k, 100, nil)
			native := finish()
			if err != nil || a != (historyBoundsAnswer{}) || native != (asOfIndexNativeWork{probes: 2, indexCopies: 49, indexDecodes: 2}) {
				t.Fatalf("recovered proof: %+v, native %+v, %v", a, native, err)
			}
		})
	}
}

func TestHistoryBoundsProofAndGenerationUseOneView(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			putBoundsHistory(t, bs, k, 100, 0, 2)
			prefix := storepkg.HistNodePrefix(100)
			prefix[0] = asOfIndexKind(k)
			view, pin, err := bs.pinAsOfView(prefix)
			if err != nil || view == nil {
				t.Fatalf("pin proof view: %v, %v", view, err)
			}
			defer bs.unpinAsOfView(view, pin)
			txn := view.txn
			// This write lands after the view was pinned (update then installs
			// a newer one). Reading either proof field from another view would
			// mix recovery generations.
			if err := bs.update(invalidateAsOfGeneration); err != nil {
				t.Fatal(err)
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 2, BufferBytes: 41, DecodedBytes: 41, HistoryExamined: 2})
			finish := observeAsOfIndexRead(bs)
			proof, known, err := admittedAsOfProof(txn, asOfIndexKind(k), 100, m.Tally("pinned bounds"))
			native := finish()
			if err != nil || !known || proof.version != 1 || proof.lastTxTo != 300 || proof.generation != 0 || native != (asOfIndexNativeWork{probes: 2, indexCopies: 41, indexDecodes: 1}) {
				t.Fatalf("pinned proof: %+v, known %v, native %+v, %v", proof, known, native, err)
			}
			if got, err := historyBoundsRead(t, bs, k, 100, nil); err != nil || got != (historyBoundsAnswer{}) {
				t.Fatalf("fresh call must decline recovered proof: %+v, %v", got, err)
			}
		})
	}
}

func TestHistoryBoundsSnapshotFollowsWrites(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			putBoundsHistory(t, bs, k, 100, 0, 2)
			s := bs.NewReadSnapshot()
			defer s.Close()
			b := s.(storecontract.ReadSnapshotHistoryBoundsCapability)
			read := func() (historyBoundsAnswer, error) {
				var a historyBoundsAnswer
				var err error
				if k.name == "node" {
					a.version, a.newest, a.known, err = b.NodeHistoryBounds(100)
				} else {
					a.version, a.newest, a.known, err = b.RelHistoryBounds(100)
				}
				return a, err
			}
			if a, err := read(); err != nil || a != (historyBoundsAnswer{1, 300, true}) {
				t.Fatalf("initial proof: %+v, %v", a, err)
			}
			if err := k.put(bs, 100, 2, "next"); err != nil {
				t.Fatal(err)
			}
			if a, err := read(); err != nil || a != (historyBoundsAnswer{}) {
				t.Fatalf("pending append: %+v, %v", a, err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if a, err := read(); err != nil || a != (historyBoundsAnswer{2, 400, true}) {
				t.Fatalf("committed append: %+v, %v", a, err)
			}
			if err := k.put(bs, 100, 1, "rewrite"); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if a, err := read(); err != nil || a != (historyBoundsAnswer{}) {
				t.Fatalf("committed rewrite: %+v, %v", a, err)
			}
		})
	}
}

func TestHistoryBoundsErrorsBeforeWork(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			putBoundsHistory(t, bs, k, 100, 0, 2)
			for _, id := range []int64{0, -1} {
				finish := observeAsOfIndexRead(bs)
				a, err := historyBoundsRead(t, bs, k, id, nil)
				if native := finish(); !errors.Is(err, storecontract.ErrInvalidStoreMutation) || a != (historyBoundsAnswer{}) || native != (asOfIndexNativeWork{}) {
					t.Fatalf("invalid id %d: %+v, native %+v, %v", id, a, native, err)
				}
			}
			// A prior refusal must propagate even when a pending row means the
			// proof would otherwise decline before issuing a native probe.
			if err := k.put(bs, 100, 2, "pending"); err != nil {
				t.Fatal(err)
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 1})
			tally := m.Tally("bounds")
			refusal := tally.Candidates(2)
			finish := observeAsOfIndexRead(bs)
			a, err := historyBoundsRead(t, bs, k, 100, tally)
			if native := finish(); !errors.Is(err, storecontract.ErrReadBudget) || err != refusal || a != (historyBoundsAnswer{}) || native != (asOfIndexNativeWork{}) {
				t.Fatalf("sticky refusal: %+v, native %+v, %v; want %v", a, native, err, refusal)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			finish = observeAsOfIndexRead(bs)
			a, err = historyBoundsRead(t, bs, k, 100, nil)
			if native := finish(); !errors.Is(err, ErrStoreClosed) || a != (historyBoundsAnswer{}) || native != (asOfIndexNativeWork{}) {
				t.Fatalf("closed store: %+v, native %+v, %v", a, native, err)
			}
		})
	}
}
