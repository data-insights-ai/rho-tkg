//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): live heap after a GC moves
// with other tests, badger's background work and host load, so these bounds
// run by hand, not in a gate (MEMGATE). The sidecars' answers after the
// writes gate in TestHistorySidecarsAfterWrites.

package badger

import (
	"fmt"
	"runtime"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestHistorySidecarRAMPerID is backlog 41's bound: the RAM the
// history-presence set and the history-stamps sidecar retain per ID, against
// the v4.51.4 shapes they replaced (one Go map per kind: map[ID]uint32 for
// presence, map[ID]{from, to Instant; top uint32} for stamps, measured the
// same way in the same process). Per size, right after the build and again
// after writes added half as many IDs again between the built ones and raised
// the top of a quarter of them (sidecarAddWrites):
//
//   - HasHistory-only (the presence set alone): <= the v4.51.4 presence map;
//   - LatestStamps-only (the stamps sidecar alone) and both doors: lower than
//     the v4.51.4 maps by more than sidecarRAMNoise.
//
// The 1 M size runs without -short.
//
//	go test -tags measure ./pkg/graph/store/badger/ -run TestHistorySidecarRAMPerID -v
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
					ids = sidecarAddWrites(t, bs, ids)
				} else {
					buildSidecars(t, bs, ids[0])
				}
				expectSidecarsBuilt(t, bs, phase, len(ids))
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
				// Answers after the writes (the bound is about RAM; a set that
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

// sidecarRAMNoise is the measurement slack of a strict "lower" bound: two
// live heap readings around a drop agree within about 1 %.
const sidecarRAMNoise = 0.05

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
