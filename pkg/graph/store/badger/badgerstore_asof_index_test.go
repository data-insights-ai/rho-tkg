package badger

import (
	"errors"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Appending later publications must not make an immutable old pin revisit
// those later rows. The native counters are independent of the read meter.
func TestAsOfIndexOldPinGrowth(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			payload := strings.Repeat("x", 8192)
			put := func(from, to uint32) {
				t.Helper()
				for v := from; v < to; v++ {
					if err := k.put(bs, 100, v, payload); err != nil {
						t.Fatal(err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			read := func(b storecontract.ReadBudget) (storecontract.ReadWork, int64, int64) {
				t.Helper()
				_, m := storecontract.WithReadBudget(t.Context(), b)
				before := bs.HistoryRowDecodesForTest()
				steps, stop := CountIndexEntriesVisitedForTest()
				v, ok, err := k.read(bs, 100, 150, m.Tally("old publication"))
				nativeSteps := steps()
				stop()
				if err != nil || !ok || v != 0 {
					t.Fatalf("old pin: v%d exact payload %v, work %+v, err %v", v, ok, m.Work(), err)
				}
				return m.Work(), bs.HistoryRowDecodesForTest() - before, nativeSteps
			}
			put(0, 2)
			base, decodes, steps := read(storecontract.ReadBudget{})
			put(2, 8)
			grown, grownDecodes, grownSteps := read(storecontract.ReadBudget{})
			if grown != base || grownDecodes != decodes || grownSteps != steps {
				t.Fatalf("old pin grew: 2 cuts work %+v decodes %d visits %d; 8 cuts work %+v decodes %d visits %d", base, decodes, steps, grown, grownDecodes, grownSteps)
			}
			read(storecontract.ReadBudget{Candidates: base.Candidates, HistoryExamined: base.HistoryExamined, DecodedBytes: base.DecodedBytes, BufferBytes: base.BufferBytes})
		})
	}
}

type asOfIndexNativeWork struct {
	probes, indexDecodes, indexCopies, visits, historyCopies, anchors, metas, rows int64
}

func observeAsOfIndexRead(bs *Store) func() asOfIndexNativeWork {
	probes, stopProbes := CountAsOfIndexProbesForTest()
	decodes, stopDecodes := CountAsOfIndexDecodesForTest()
	copies, stopCopies := CountAsOfIndexBytesCopiedForTest()
	visits, stopVisits := CountIndexEntriesVisitedForTest()
	history, stopHistory := CountHistoryBytesCopiedForTest()
	anchors, stopAnchors := CountHistoryAnchorBytesCopiedForTest()
	metas, rows := bs.HistoryMetaDecodesForTest(), bs.HistoryRowDecodesForTest()
	return func() asOfIndexNativeWork {
		w := asOfIndexNativeWork{probes(), decodes(), copies(), visits(), history(), anchors(), bs.HistoryMetaDecodesForTest() - metas, bs.HistoryRowDecodesForTest() - rows}
		stopProbes()
		stopDecodes()
		stopCopies()
		stopVisits()
		stopHistory()
		stopAnchors()
		return w
	}
}

func TestAsOfIndexAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		for _, delta := range []bool{false, true} {
			t.Run(k.name+map[bool]string{false: "/full", true: "/delta"}[delta], func(t *testing.T) {
				bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding = delta; c.HistoryAnchorInterval = 8 })
				for v := range uint32(2) {
					if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				raw, err := k.raw(bs, 100, 1)
				if err != nil {
					t.Fatal(err)
				}
				anchorLen := 0
				if delta {
					anchor, err := k.raw(bs, 100, 0)
					if err != nil || storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
						t.Fatalf("delta fixture: %v", err)
					}
					anchorLen = len(anchor)
				}
				read := func(b storecontract.ReadBudget) (storecontract.ReadWork, asOfIndexNativeWork, error) {
					_, m := storecontract.WithReadBudget(t.Context(), b)
					finish := observeAsOfIndexRead(bs)
					v, ok, err := k.read(bs, 100, 250, m.Tally("indexed as-of"))
					native := finish()
					if err == nil && (!ok || v != 1) {
						t.Fatalf("wrong unlimited answer v%d, payload %v", v, ok)
					}
					if err != nil && ok {
						t.Fatal("refusal published a row")
					}
					return m.Work(), native, err
				}
				w, native, err := read(storecontract.ReadBudget{})
				want := storecontract.ReadWork{Candidates: 4, HistoryExamined: 3, DecodedBytes: uint64(asOfProofSize + 16 + 2*len(raw) + anchorLen), BufferBytes: uint64(asOfProofSize + storepkg.HistoryAsOfSeekSize + len(raw) + anchorLen)}
				if delta {
					want.HistoryExamined++
				}
				wantNative := asOfIndexNativeWork{probes: 2, indexDecodes: 2, indexCopies: asOfProofSize, visits: 1, historyCopies: int64(len(raw) + anchorLen), anchors: int64(anchorLen), metas: 1, rows: 1}
				if err != nil || w != want || native != wantNative {
					t.Fatalf("control work %+v want %+v; native %+v want %+v; err %v", w, want, native, wantNative, err)
				}
				for _, tc := range []struct {
					name string
					b    storecontract.ReadBudget
					want asOfIndexNativeWork
				}{
					{"before proof probe", storecontract.ReadBudget{Candidates: 1}, asOfIndexNativeWork{}},
					{"before generation probe", storecontract.ReadBudget{Candidates: 2}, asOfIndexNativeWork{probes: 1, indexCopies: asOfProofSize, indexDecodes: 1}},
					{"before seek", storecontract.ReadBudget{Candidates: 3}, asOfIndexNativeWork{probes: 2, indexCopies: asOfProofSize, indexDecodes: 1}},
					{"before proof copy", storecontract.ReadBudget{BufferBytes: 1}, asOfIndexNativeWork{probes: 1}},
					{"before proof decode", storecontract.ReadBudget{DecodedBytes: 1}, asOfIndexNativeWork{probes: 1, indexCopies: asOfProofSize}},
					{"before history copy", storecontract.ReadBudget{BufferBytes: uint64(asOfProofSize + storepkg.HistoryAsOfSeekSize + len(raw) - 1)}, asOfIndexNativeWork{probes: 2, indexCopies: asOfProofSize, indexDecodes: 2, visits: 1}},
					{"before history meta", storecontract.ReadBudget{HistoryExamined: 1}, asOfIndexNativeWork{probes: 2, indexCopies: asOfProofSize, indexDecodes: 2, visits: 1, historyCopies: int64(len(raw))}},
					{"before history meta bytes", storecontract.ReadBudget{DecodedBytes: uint64(asOfProofSize + 16 + len(raw) - 1)}, asOfIndexNativeWork{probes: 2, indexCopies: asOfProofSize, indexDecodes: 2, visits: 1, historyCopies: int64(len(raw))}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						_, got, err := read(tc.b)
						if !errors.Is(err, storecontract.ErrReadBudget) || got != tc.want {
							t.Fatalf("native %+v want %+v; err %v", got, tc.want, err)
						}
					})
				}
				if delta {
					_, got, err := read(storecontract.ReadBudget{BufferBytes: want.BufferBytes - 1})
					if !errors.Is(err, storecontract.ErrReadBudget) || got.anchors != 0 || got.rows != 0 || got.historyCopies != int64(len(raw)) || got.metas != 1 {
						t.Fatalf("anchor copied after refusal: %+v %v", got, err)
					}
				}
			})
		}
	}
}

func asOfIndexKind(k asOfBudgetKind) byte {
	if k.name == "rel" {
		return storepkg.KeyHistRel
	}
	return storepkg.KeyHistNode
}

func asOfIndexPut(t *testing.T, bs *Store, k asOfBudgetKind, version uint32, tm *types.TemporalMetadata) {
	t.Helper()
	var err error
	if k.name == "node" {
		n := types.NewNode(100, 1, nil)
		n.SetVersion(version)
		n.SetTemporal(tm)
		_ = n.SetProperty("payload", strings.Repeat("x", 8192))
		err = bs.PutNodeVersion(n.ID(), version, n)
	} else {
		r := types.NewRelationship(100, 5, 1, 2)
		r.SetVersion(version)
		r.SetTemporal(tm)
		_ = r.SetProperty("payload", strings.Repeat("x", 8192))
		err = bs.PutRelVersion(r.ID(), version, r)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func asOfIndexOracle(t *testing.T, bs *Store, k asOfBudgetKind, pin types.Instant) (uint32, bool) {
	t.Helper()
	if k.name == "node" {
		history, err := bs.GetNodeHistory(100)
		if err != nil {
			t.Fatal(err)
		}
		row, ok := storepkg.SelectAsOf(bs.validAxis.Get(), history, pin)
		if ok {
			return row.Version(), true
		}
		return 0, false
	}
	history, err := bs.GetRelHistory(100)
	if err != nil {
		t.Fatal(err)
	}
	row, ok := storepkg.SelectAsOf(bs.validAxis.Get(), history, pin)
	if ok {
		return row.Version(), true
	}
	return 0, false
}

func assertAsOfIndexOracle(t *testing.T, bs *Store, k asOfBudgetKind) {
	t.Helper()
	for _, pin := range []types.Instant{1, 99, 100, 150, 199, 200, 250, 275, 300, 350, 400, 450, 500} {
		want, wantOK := asOfIndexOracle(t, bs, k, pin)
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
		got, ok, err := k.read(bs, 100, pin, m.Tally("mutation oracle"))
		if wantOK && (err != nil || !ok || got != want) {
			t.Fatalf("pin %d: got v%d ok %v err %v; want v%d", pin, got, ok, err, want)
		}
		if !wantOK && (ok || !errors.Is(err, ErrVersionNotFound)) {
			t.Fatalf("pin %d: got v%d ok %v err %v; want absent", pin, got, ok, err)
		}
	}
}

func TestAsOfIndexMutationFallback(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		for _, mutation := range []string{"rewrite", "backdated", "same stamp", "gap", "tombstone", "void", "trim", "truncate"} {
			t.Run(k.name+"/"+mutation, func(t *testing.T) {
				bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding = true })
				for v := range uint32(3) {
					if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
						t.Fatal(err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				assertAsOfIndexOracle(t, bs, k)
				var err error
				switch mutation {
				case "rewrite":
					err = k.put(bs, 100, 1, strings.Repeat("x", 8192))
				case "backdated":
					asOfIndexPut(t, bs, k, 3, &types.TemporalMetadata{TxFrom: 250, TxTo: 350, ValidFrom: 1})
				case "same stamp":
					asOfIndexPut(t, bs, k, 3, &types.TemporalMetadata{TxFrom: 300, TxTo: 400, ValidFrom: 1})
				case "gap":
					asOfIndexPut(t, bs, k, 3, &types.TemporalMetadata{TxFrom: 450, TxTo: 500, ValidFrom: 1})
				case "tombstone":
					asOfIndexPut(t, bs, k, 1, &types.TemporalMetadata{TxFrom: 200, TxTo: 250, DeletedAt: 250, ValidFrom: 1, ValidTo: 250})
				case "void":
					asOfIndexPut(t, bs, k, 3, &types.TemporalMetadata{TxFrom: 400, ValidFrom: 450, ValidTo: 500, Void: true})
				case "trim":
					if k.name == "node" {
						err = bs.TrimNodeHistoryFrom(100, 1)
					} else {
						err = bs.TrimRelHistoryFrom(100, 1)
					}
				case "truncate":
					if k.name == "node" {
						err = bs.TruncateNodeHistory(100, 2)
					} else {
						err = bs.TruncateRelHistory(100, 2)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				assertAsOfIndexOracle(t, bs, k) // pending state must retain the admitted fallback
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				assertAsOfIndexOracle(t, bs, k)
				var valid bool
				if err := bs.view(func(txn *badgerv4.Txn) error {
					raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfProofKey(asOfIndexKind(k), 100))
					if err != nil {
						return err
					}
					_, valid, err = decodeAsOfProof(raw)
					return err
				}); err != nil || valid {
					t.Fatalf("mutation kept proof valid %v: %v", valid, err)
				}
			})
		}
	}
}
