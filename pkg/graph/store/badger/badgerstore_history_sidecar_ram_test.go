package badger

import (
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 41: the RAM the history-presence set and the history-stamps sidecar
// retain per ID, against the v4.51.4 shapes they replaced (one Go map per kind:
// map[ID]uint32 for presence, map[ID]{from, to Instant; top uint32} for
// stamps, measured the same way in the same process). Gate per size, right
// after the build and again after writes added half as many IDs again between
// the built ones and raised the top of a quarter of them:
//
//   - HasHistory-only (the presence set alone): <= the v4.51.4 presence map;
//   - LatestStamps-only (the stamps sidecar alone) and both doors: lower than
//     the v4.51.4 maps by more than sidecarRAMNoise.
//
// The 1 M size runs without -short.
//
//	go test ./pkg/graph/store/badger/ -run TestHistorySidecarRAMPerID -v
func TestHistorySidecarRAMPerID(t *testing.T) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		t.Run(fmt.Sprintf("ids=%d", n), func(t *testing.T) {
			if n > 100_000 && testing.Short() {
				t.Skip("1 M IDs skipped under -short")
			}
			bs := newFlushParkStore(t, nil)
			ids := sidecarRAMIDs(n)
			if err := writeSidecarRAMRows(bs, ids, 0); err != nil {
				t.Fatal(err)
			}
			for _, phase := range []string{"built", "after writes"} {
				if phase == "after writes" {
					// Interleaved new IDs (inserts between built ones) and a
					// raised top on every fourth built ID (in-place updates).
					added := make([]int64, 0, n/2)
					raised := make([]int64, 0, n/4)
					for i, id := range ids {
						if i%2 == 0 {
							added = append(added, id+sidecarRAMIDGap/2)
						}
						if i%4 == 0 {
							raised = append(raised, id)
						}
					}
					if err := writeSidecarRAMRows(bs, added, 0); err != nil {
						t.Fatal(err)
					}
					if err := writeSidecarRAMRows(bs, raised, 1); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, added...)
				} else {
					buildSidecars(t, bs, ids[0])
				}
				st := bs.HistoryPresenceStats()
				ss := bs.HistoryStampsStats()
				if !st.NodesBuilt || !ss.NodesBuilt || st.NodeIDs != len(ids) || ss.NodeIDs != len(ids) {
					t.Fatalf("%s: presence %+v stamps %+v, want both built over %d IDs", phase, st, ss, len(ids))
				}
				per := func(b int64) float64 { return float64(b) / float64(len(ids)) }
				// One reading each: the drop rebuilds the sidecar fresh, so a
				// repeat would no longer see the one the writes maintained.
				pres := per(sidecarBytes(t, bs, true))
				stamps := per(sidecarBytes(t, bs, false))
				refPres := per(medianBytes(func() int64 { return refMapBytes(ids, true) }))
				refStamps := per(medianBytes(func() int64 { return refMapBytes(ids, false) }))
				t.Logf("ids=%d %s: HasHistory-only %.1f B/ID (v4.51.4 %.1f), LatestStamps-only %.1f B/ID (v4.51.4 %.1f), both %.1f B/ID (v4.51.4 %.1f)",
					len(ids), phase, pres, refPres, stamps, refStamps, pres+stamps, refPres+refStamps)
				if pres > refPres {
					t.Errorf("ids=%d %s: HasHistory-only pays %.1f B/ID, more than v4.51.4's %.1f", len(ids), phase, pres, refPres)
				}
				if stamps >= refStamps*(1-sidecarRAMNoise) {
					t.Errorf("ids=%d %s: LatestStamps-only pays %.1f B/ID, not lower than v4.51.4's %.1f", len(ids), phase, stamps, refStamps)
				}
				if pres+stamps >= (refPres+refStamps)*(1-sidecarRAMNoise) {
					t.Errorf("ids=%d %s: both doors pay %.1f B/ID, not lower than v4.51.4's %.1f", len(ids), phase, pres+stamps, refPres+refStamps)
				}
				// Answers after the writes (the gate is about RAM; a set that
				// lost entries would be smaller too).
				if phase == "after writes" {
					for _, id := range []int64{ids[0], ids[n-1], ids[n], ids[len(ids)-1], ids[0] + 1} {
						expectSidecarAgree(t, bs, id)
					}
				}
			}
		})
	}
}

// sidecarRAMNoise is the measurement slack of a strict "lower" gate: two live
// heap readings around a drop agree within about 1 %.
const sidecarRAMNoise = 0.05

// sidecarRAMIDGap spreads the IDs like snowflakes minted apart, leaving room
// for the IDs the "after writes" phase inserts between them.
const sidecarRAMIDGap = 4096

func sidecarRAMIDs(n int) []int64 {
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = 1 + int64(i)*sidecarRAMIDGap
	}
	return ids
}

// writeSidecarRAMRows writes one stamped node history row at version ver for
// every ID, flushing in batches.
func writeSidecarRAMRows(bs *Store, ids []int64, ver uint32) error {
	for i, id := range ids {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
		n.SetVersion(ver)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, TxFrom: types.Instant(id + int64(ver)), TxTo: types.Instant(id + int64(ver) + 1)})
		if err := bs.PutNodeVersion(n.ID(), ver, n); err != nil {
			return err
		}
		if i%50_000 == 49_999 {
			if err := bs.Flush(); err != nil {
				return err
			}
		}
	}
	return bs.Flush()
}

func buildSidecars(t *testing.T, bs *Store, id int64) {
	t.Helper()
	if has, err := bs.HasNodeHistory(types.NodeID(snowflake.ID(id))); err != nil || !has {
		t.Fatalf("HasNodeHistory(%d) = %v, %v", id, has, err)
	}
	if _, _, has, err := bs.NodeHistoryStamps(types.NodeID(snowflake.ID(id))); err != nil || !has {
		t.Fatalf("NodeHistoryStamps(%d) = %v, %v", id, has, err)
	}
}

func expectSidecarAgree(t *testing.T, bs *Store, id int64) {
	t.Helper()
	nid := types.NodeID(snowflake.ID(id))
	rows, err := bs.GetNodeHistory(nid)
	if err != nil {
		t.Fatal(err)
	}
	var from, to types.Instant
	for _, r := range rows {
		from, to = max(from, r.Temporal().TxFrom), max(to, r.Temporal().TxTo)
	}
	has, err := bs.HasNodeHistory(nid)
	if err != nil || has != (len(rows) > 0) {
		t.Fatalf("HasNodeHistory(%d) = %v, %v; history has %d rows", id, has, err, len(rows))
	}
	f, tt, sh, err := bs.NodeHistoryStamps(nid)
	if err != nil || sh != (len(rows) > 0) || f != from || tt != to {
		t.Fatalf("NodeHistoryStamps(%d) = %d, %d, %v, %v; history folds to %d, %d", id, f, tt, sh, err, from, to)
	}
}

// sidecarBytes is the live heap one built node sidecar retains: the heap with
// it, minus the heap after dropping it, which then is rebuilt.
func sidecarBytes(t *testing.T, bs *Store, presence bool) int64 {
	t.Helper()
	with := liveHeap()
	if presence {
		resetHistoryPresenceForBench(bs, &bs.histNodePresence)
	} else {
		resetHistoryStampsForBench(bs, &bs.histNodeStamps)
	}
	without := liveHeap()
	buildSidecars(t, bs, 1)
	return int64(with) - int64(without)
}

// sidecarRefStamp is the v4.51.4 stamps map value.
type sidecarRefStamp struct {
	from, to types.Instant
	top      uint32
}

// refMapBytes is the live heap of the v4.51.4 map shape over ids.
func refMapBytes(ids []int64, presence bool) int64 {
	before := liveHeap()
	var keep any
	if presence {
		m := make(map[snowflake.ID]uint32)
		for _, id := range ids {
			m[snowflake.ID(id)] = 1
		}
		keep = m
	} else {
		m := make(map[snowflake.ID]sidecarRefStamp)
		for _, id := range ids {
			m[snowflake.ID(id)] = sidecarRefStamp{from: 1, to: 2, top: 1}
		}
		keep = m
	}
	after := liveHeap()
	runtime.KeepAlive(keep)
	return int64(after) - int64(before)
}

func liveHeap() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func medianBytes(measure func() int64) int64 {
	s := []int64{measure(), measure(), measure()}
	slices.Sort(s)
	return s[1]
}

// BenchmarkHistorySidecarLookup measures one HasNodeHistory and one
// NodeHistoryStamps call on built sidecars over 10 K / 100 K / 1 M IDs with
// history (hit) and IDs without (miss): the per-ID lookup cost the backlog 41
// representation must not regress.
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench BenchmarkHistorySidecarLookup -benchmem
func BenchmarkHistorySidecarLookup(b *testing.B) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("ids=%d", n), func(b *testing.B) {
			bs, err := New(Config{InMemory: true, FlushInterval: time.Hour})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = bs.Close() })
			ids := sidecarRAMIDs(n)
			if err := writeSidecarRAMRows(bs, ids, 0); err != nil {
				b.Fatal(err)
			}
			probe := make([]types.NodeID, 4096)
			for i := range probe {
				probe[i] = types.NodeID(snowflake.ID(ids[(i*7919)%n]))
			}
			for _, door := range []string{"HasHistory", "LatestStamps"} {
				for _, miss := range []int64{0, 1} {
					name := door + "/hit"
					if miss == 1 {
						name = door + "/miss"
					}
					b.Run(name, func(b *testing.B) {
						call := func(id types.NodeID) (bool, error) {
							if door == "HasHistory" {
								return bs.HasNodeHistory(id)
							}
							_, _, has, err := bs.NodeHistoryStamps(id)
							return has, err
						}
						if _, err := call(probe[0]); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							id := probe[i%len(probe)] + types.NodeID(miss)
							if has, err := call(id); err != nil || has == (miss == 1) {
								b.Fatalf("%d: %v, %v", id, has, err)
							}
						}
					})
				}
			}
		})
	}
}
