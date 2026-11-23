package badger

import (
	"errors"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type asOfBudgetKind struct {
	name string
	put  func(*Store, int64, uint32, string) error
	read func(*Store, int64, types.Instant, *storecontract.ReadTally) (uint32, bool, error)
	raw  func(*Store, int64, uint64) ([]byte, error)
}

func asOfBudgetKinds() []asOfBudgetKind {
	return []asOfBudgetKind{
		{"node", func(bs *Store, id int64, v uint32, payload string) error {
			n := types.NewNode(types.NodeID(id), 1, nil)
			n.SetVersion(v)
			n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(v+1) * 100, TxTo: types.Instant(v+2) * 100, ValidFrom: 1})
			if err := n.SetProperty("payload", payload); err != nil {
				return err
			}
			return bs.PutNodeVersion(n.ID(), v, n)
		}, func(bs *Store, id int64, pin types.Instant, t *storecontract.ReadTally) (uint32, bool, error) {
			n, err := bs.NodeAsOfMetered(types.NodeID(id), pin, t)
			if n == nil {
				return 0, false, err
			}
			return n.Version(), n.ID() == types.NodeID(id) && n.PropertiesMap()["payload"] == strings.Repeat("x", 8192), err
		}, func(bs *Store, id int64, v uint64) ([]byte, error) {
			return bs.readHistoryRaw(storepkg.HistNodeKey(types.NodeID(id).SnowflakeID(), v))
		}},
		{"rel", func(bs *Store, id int64, v uint32, payload string) error {
			r := types.NewRelationship(types.RelID(id), 5, 1, 2)
			r.SetVersion(v)
			r.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(v+1) * 100, TxTo: types.Instant(v+2) * 100, ValidFrom: 1})
			if err := r.SetProperty("payload", payload); err != nil {
				return err
			}
			return bs.PutRelVersion(r.ID(), v, r)
		}, func(bs *Store, id int64, pin types.Instant, t *storecontract.ReadTally) (uint32, bool, error) {
			r, err := bs.RelAsOfMetered(types.RelID(id), pin, t)
			if r == nil {
				return 0, false, err
			}
			return r.Version(), r.ID() == types.RelID(id) && r.PropertiesMap()["payload"] == strings.Repeat("x", 8192), err
		}, func(bs *Store, id int64, v uint64) ([]byte, error) {
			return bs.readHistoryRaw(storepkg.HistRelKey(types.RelID(id).SnowflakeID(), v))
		}},
	}
}

func asOfBudgetFixture(t *testing.T, k asOfBudgetKind, flushed bool) *Store {
	t.Helper()
	bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding = true; c.HistoryAnchorInterval = 8 })
	for _, id := range []int64{100, 101} {
		for v := range uint32(2) {
			if err := k.put(bs, id, v, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err)
			}
		}
		// This suite owns the general resolver's native boundaries. A rewrite
		// keeps the exact rows while invalidating the ordinary-chain index;
		// the indexed path has its own exact native admission suite.
		if err := k.put(bs, id, 1, strings.Repeat("x", 8192)); err != nil {
			t.Fatal(err)
		}
	}
	if flushed {
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := k.raw(bs, 100, 1)
	if err != nil || storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
		t.Fatalf("delta fixture: %v", err)
	}
	return bs
}

func TestAsOfNativeAnchorBufferAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		for _, flushed := range []bool{false, true} {
			t.Run(k.name+map[bool]string{false: "/pending", true: "/flushed"}[flushed], func(t *testing.T) {
				bs := asOfBudgetFixture(t, k, flushed)
				anchor, err := k.raw(bs, 100, 0)
				if err != nil {
					t.Fatal(err)
				}
				delta, err := k.raw(bs, 100, 1)
				if err != nil {
					t.Fatal(err)
				}
				// One exact control establishes all phases. The final copied buffer is
				// the anchor: one byte less must refuse before that native copy.
				read := func(b storecontract.ReadBudget) (uint32, bool, storecontract.ReadWork, int64, error) {
					_, m := storecontract.WithReadBudget(t.Context(), b)
					copies, stop := CountHistoryAnchorBytesCopiedForTest()
					defer stop()
					v, ok, err := k.read(bs, 100, 250, m.Tally("asof"))
					return v, ok, m.Work(), copies(), err
				}
				before := bs.HistoryRowDecodesForTest()
				steps, stopSteps := CountIndexEntriesVisitedForTest()
				v, ok, total, copies, err := read(storecontract.ReadBudget{})
				nativeSteps := steps()
				stopSteps()
				wantSteps := int64(0)
				if flushed {
					wantSteps = 3
				} // one scoped presence key and two reverse rows
				if nativeSteps != wantSteps || bs.HistoryRowDecodesForTest()-before != 3 {
					t.Fatalf("native control: key visits %d (want %d), row/metadata decodes %d (want 3)", nativeSteps, wantSteps, bs.HistoryRowDecodesForTest()-before)
				}
				if err != nil || !ok || v != 1 || copies != int64(len(anchor)) {
					t.Fatalf("control: v%d ok %v work %+v anchor bytes %d err %v", v, ok, total, copies, err)
				}
				// Cold presence reads keys only. The reverse scan examines two
				// history rows; delta window, run answer and winner each decode
				// the delta; the lower full row decodes its fixed temporal tail,
				// then the final anchor lookup/decode reconstructs the winner.
				wantHistory := uint64(7)
				wantBuffer := uint64(100 + len(delta) + len(anchor)) // one invalid-proof byte before fallback
				if !flushed {
					wantHistory += 2                                                                 // capture each buffered history value
					wantBuffer = uint64(225 + 2*storepkg.SizeHistKey + 2*len(delta) + 2*len(anchor)) // a captured row is priced with its key, as a committed one
				}
				wantDecoded := uint64(3*len(delta) + 2*len(anchor))
				if flushed {
					wantDecoded++
				} // decode the one-byte invalid proof
				if total.HistoryExamined != wantHistory || total.BufferBytes != wantBuffer || total.DecodedBytes != wantDecoded {
					t.Fatalf("actual phases %+v; want history %d buffer %d decoded %d", total, wantHistory, wantBuffer, wantDecoded)
				}
				v, ok, w, copies, err := read(storecontract.ReadBudget{BufferBytes: total.BufferBytes - 1})
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || ok || copies != 0 {
					t.Fatalf("limited: v%d ok %v work %+v anchor bytes %d err %v", v, ok, w, copies, err)
				}
			})
		}
	}
}

func TestAsOfNativePendingAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := asOfBudgetFixture(t, k, false)
			// Warm only the presence table to isolate the raw overlay admission.
			if _, err := bs.hasHistory(&bs.histNodePresence, storepkg.KeyHistNode, 100, bs.probeHistoryPresence); err != nil {
				t.Fatal(err)
			}
			if _, err := bs.hasHistory(&bs.histRelPresence, storepkg.KeyHistRel, 100, bs.probeHistoryPresence); err != nil {
				t.Fatal(err)
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{HistoryExamined: 1})
			visits, stop := CountPendingKeysVisitedForTest()
			defer stop()
			copies, end := CountHistoryBytesCopiedForTest()
			defer end()
			before := bs.HistoryRowDecodesForTest()
			_, ok, err := k.read(bs, 100, 250, m.Tally("asof"))
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "history examined" || ok || visits() != 2 || bs.HistoryRowDecodesForTest() != before {
				t.Fatalf("limited: ok %v visits %d copies %d decodes %d work %+v err %v", ok, visits(), copies(), bs.HistoryRowDecodesForTest()-before, m.Work(), err)
			}
		})
	}
}

func TestAsOfNativeColdPresenceScope(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := asOfBudgetFixture(t, k, false)
			for id := int64(200); id < 260; id++ {
				if err := k.put(bs, id, 0, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 8})
			visits, stop := CountPendingKeysVisitedForTest()
			defer stop()
			v, ok, err := k.read(bs, 100, 250, m.Tally("asof"))
			stats := bs.HistoryPresenceStats()
			if err != nil || !ok || v != 1 || visits() != 4 || stats.NodesBuilt || stats.RelsBuilt {
				t.Fatalf("cold scoped control: v%d ok %v visits %d stats %+v work %+v err %v", v, ok, visits(), stats, m.Work(), err)
			}
		})
	}
}

// No metadata decode may run before its own byte admission, even though the
// selector needs that metadata before it knows the winning version.
func TestAsOfNativeDecodeAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := asOfBudgetFixture(t, k, true)
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{DecodedBytes: 1})
			before := bs.HistoryRowDecodesForTest()
			copied, stop := CountHistoryAnchorBytesCopiedForTest()
			defer stop()
			_, ok, err := k.read(bs, 100, 250, m.Tally("asof"))
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || ok || bs.HistoryRowDecodesForTest() != before || copied() != 0 {
				t.Fatalf("metadata admission: row %v decodes %d anchor bytes %d work %+v err %v", ok, bs.HistoryRowDecodesForTest()-before, copied(), m.Work(), err)
			}
		})
	}
}

// The scan owns the retained write-run bytes, independently of the anchor.
func TestAsOfNativeRunBufferAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := asOfBudgetFixture(t, k, true)
			raw, err := k.raw(bs, 100, 1)
			if err != nil {
				t.Fatal(err)
			}
			// Reverse seek key + first iterator key + one retained run slot/value.
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{BufferBytes: uint64(1 + 17 + 17 + 48 + len(raw) - 1)})
			copied, stop := CountHistoryBytesCopiedForTest()
			defer stop()
			_, ok, err := k.read(bs, 100, 250, m.Tally("asof"))
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || ok || copied() != 0 {
				t.Fatalf("run admission: row %v native raw bytes %d work %+v err %v", ok, copied(), m.Work(), err)
			}
		})
	}
}

// A tally spans IDs and physical stores: each read fits on its own, while a
// second ID or a second shard's store must refuse before another anchor copy.
func TestAsOfNativeCumulativeAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			a, b := asOfBudgetFixture(t, k, true), asOfBudgetFixture(t, k, true)
			var cost uint64
			for _, c := range []struct {
				bs *Store
				id int64
			}{{a, 100}, {a, 101}, {b, 100}} {
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				v, ok, err := k.read(c.bs, c.id, 250, m.Tally("asof"))
				if err != nil || !ok || v != 1 {
					t.Fatalf("exact per-store ID control: v%d ok %v err %v", v, ok, err)
				}
				if m.Work().HistoryExamined > cost {
					cost = m.Work().HistoryExamined
				}
			}
			for _, c := range []struct {
				name string
				bs   *Store
				id   int64
			}{{"IDs", a, 101}, {"shards", b, 100}} {
				t.Run(c.name, func(t *testing.T) {
					_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{HistoryExamined: cost})
					v, ok, err := k.read(a, 100, 250, m.Tally("asof"))
					if err != nil || !ok || v != 1 {
						t.Fatalf("first admitted row: v%d ok %v err %v", v, ok, err)
					}
					copies, stop := CountHistoryAnchorBytesCopiedForTest()
					defer stop()
					before := c.bs.HistoryRowDecodesForTest()
					_, ok, err = k.read(c.bs, c.id, 250, m.Tally("asof"))
					var refusal *storecontract.ReadBudgetError
					if !errors.As(err, &refusal) || refusal.Limit != "history examined" || ok || copies() != 0 || c.bs.HistoryRowDecodesForTest() != before {
						t.Fatalf("cumulative refusal: row %v anchor bytes %d decodes %d work %+v err %v", ok, copies(), c.bs.HistoryRowDecodesForTest()-before, m.Work(), err)
					}
				})
			}
		})
	}
}

// A presence refusal must propagate even when the current row's maximum
// version would let the selector answer without consulting historyAbove.
func TestAsOfNativePresenceRefusalCannotReturnCurrent(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := asOfBudgetFixture(t, k, false)
			const maxVersion = ^uint32(0)
			if k.name == "node" {
				n := types.NewNode(100, 1, nil)
				n.SetVersion(maxVersion)
				n.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 1})
				if err := n.SetProperty("payload", strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			} else {
				putTestNode(t, bs, 1, 10, nil)
				putTestNode(t, bs, 2, 10, nil)
				r := types.NewRelationship(100, 5, 1, 2)
				r.SetVersion(maxVersion)
				r.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 1})
				if err := r.SetProperty("payload", strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
				if err := bs.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 1})
			copies, stop := CountHistoryBytesCopiedForTest()
			defer stop()
			_, ok, err := k.read(bs, 100, 250, m.Tally("asof"))
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "candidates" || ok || copies() != 0 {
				t.Fatalf("presence refusal: row %v raw copies %d work %+v err %v", ok, copies(), m.Work(), err)
			}
		})
	}
}

// The fixed-width v2 tail still decodes two stored scalars. Admission belongs
// before that fast path, even though no full message-pack row is decoded.
func TestAsOfNativeTailDecodeAdmission(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(2) {
				if err := k.put(bs, 100, v, strings.Repeat("x", 8192)); err != nil {
					t.Fatal(err)
				}
			}
			if err := k.put(bs, 100, 1, strings.Repeat("x", 8192)); err != nil {
				t.Fatal(err) // retain the rewritten-chain fallback's tail decoder boundary
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			raw0, err := k.raw(bs, 100, 0)
			if err != nil {
				t.Fatal(err)
			}
			raw1, err := k.raw(bs, 100, 1)
			if err != nil {
				t.Fatal(err)
			}
			read := func(b storecontract.ReadBudget) (uint32, bool, storecontract.ReadWork, int64, error) {
				_, m := storecontract.WithReadBudget(t.Context(), b)
				before := bs.HistoryMetaDecodesForTest()
				v, ok, err := k.read(bs, 100, 250, m.Tally("asof"))
				return v, ok, m.Work(), bs.HistoryMetaDecodesForTest() - before, err
			}
			_, ok, w, decodes, err := read(storecontract.ReadBudget{DecodedBytes: 1})
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || ok || decodes != 0 {
				t.Fatalf("tail refusal: row %v native tail decodes %d work %+v err %v", ok, decodes, w, err)
			}
			v, ok, w, decodes, err := read(storecontract.ReadBudget{})
			if err != nil || !ok || v != 1 || decodes != 2 || w.DecodedBytes != uint64(1+len(raw0)+3*len(raw1)) || w.HistoryExamined != 6 {
				t.Fatalf("exact full-row control: v%d row %v native tail decodes %d work %+v err %v", v, ok, decodes, w, err)
			}
		})
	}
}
