package badger

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 41: the history-presence set and the history-stamps sidecar replace
// one Go map per kind. TestHistorySidecarsAfterWrites pins their sizes and
// answers after a build and after writes between the built IDs; the RAM they
// retain per ID against the v4.51.4 maps is TestHistorySidecarRAMPerID, at
// the measurement level (badgerstore_history_sidecar_ram_measure_test.go,
// MEMGATE).
func TestHistorySidecarsAfterWrites(t *testing.T) {
	const n = 10_000
	bs := newFlushParkStore(t, nil)
	ids := sidecarRAMIDs(n)
	if err := writeSidecarRAMRows(bs, ids, 0); err != nil {
		t.Fatal(err)
	}
	buildSidecars(t, bs, ids[0])
	expectSidecarsBuilt(t, bs, "built", len(ids))
	ids = sidecarAddWrites(t, bs, ids)
	expectSidecarsBuilt(t, bs, "after writes", len(ids))
	for _, id := range []int64{ids[0], ids[n-1], ids[n], ids[len(ids)-1], ids[0] + 1} {
		expectSidecarAgree(t, bs, id)
	}
}

// sidecarAddWrites writes, over built sidecars, a new ID between every second
// pair of built ones (inserts) and a raised top on every fourth built ID
// (in-place updates), and returns ids with the new IDs appended.
func sidecarAddWrites(t *testing.T, bs *Store, ids []int64) []int64 {
	t.Helper()
	added := make([]int64, 0, len(ids)/2)
	raised := make([]int64, 0, len(ids)/4)
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
	return append(ids, added...)
}

// expectSidecarsBuilt fails unless both node sidecars are built over want IDs.
func expectSidecarsBuilt(t *testing.T, bs *Store, phase string, want int) {
	t.Helper()
	st := bs.HistoryPresenceStats()
	ss := bs.HistoryStampsStats()
	if !st.NodesBuilt || !ss.NodesBuilt || st.NodeIDs != want || ss.NodeIDs != want {
		t.Fatalf("%s: presence %+v stamps %+v, want both built over %d IDs", phase, st, ss, want)
	}
}

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

// BenchmarkHistorySidecarAtScale measures the sidecars over 1 M rel IDs with
// one history row each: the first call's build (scan plus install), a lookup
// on a built sidecar (an ID with history, one without), and a history write
// of a new random ID noted into both built sidecars (the maintenance every
// write pays under the write-buffer lock).
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench BenchmarkHistorySidecarAtScale
func BenchmarkHistorySidecarAtScale(b *testing.B) {
	const n = 1_000_000
	bs, err := New(Config{InMemory: true, FlushInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bs.Close() })
	if err := bs.PutNode(types.NewNode(1, 1, nil)); err != nil {
		b.Fatal(err)
	}
	ids := sidecarRAMIDs(n)
	for i, id := range ids {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), 5, 1, 1)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, TxFrom: types.Instant(id), TxTo: types.Instant(id + 1)})
		if err := bs.PutRelVersion(r.ID(), 0, r); err != nil {
			b.Fatal(err)
		}
		if i%50_000 == 49_999 {
			if err := bs.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := bs.Flush(); err != nil {
		b.Fatal(err)
	}
	has := func(id int64) bool {
		ok, err := bs.HasRelHistory(types.RelID(snowflake.ID(id)))
		if err != nil {
			b.Fatal(err)
		}
		return ok
	}
	stamps := func(id int64) bool {
		_, _, ok, err := bs.RelHistoryStamps(types.RelID(snowflake.ID(id)))
		if err != nil {
			b.Fatal(err)
		}
		return ok
	}
	b.Run("build/HasHistory", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			resetHistoryPresenceForBench(bs, &bs.histRelPresence)
			if !has(ids[0]) {
				b.Fatal("miss")
			}
		}
	})
	b.Run("build/LatestStamps", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			resetHistoryStampsForBench(bs, &bs.histRelStamps)
			if !stamps(ids[0]) {
				b.Fatal("miss")
			}
		}
	})
	probe := make([]int64, 4096)
	for i := range probe {
		probe[i] = ids[(i*7919)%n]
	}
	for _, d := range []struct {
		name string
		fn   func(int64) bool
	}{{"HasHistory", has}, {"LatestStamps", stamps}} {
		for _, miss := range []int64{0, 1} {
			b.Run(fmt.Sprintf("lookup/%s/miss=%v", d.name, miss == 1), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if d.fn(probe[i&4095]+miss) != (miss == 0) {
						b.Fatal("wrong answer")
					}
				}
			})
		}
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(2)), 5, 1, 1)
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, TxFrom: 1_000, TxTo: 2_000})
	value, err := storepkg.MarshalRelWire(r)
	if err != nil {
		b.Fatal(err)
	}
	rng := rand.New(rand.NewSource(41)) // #nosec G404 -- deterministic benchmark data
	b.Run("insert/random-new-ID", func(b *testing.B) {
		keys := make([][]byte, b.N)
		for i := range keys {
			keys[i] = storepkg.HistRelKey(snowflake.ID(2+2*rng.Int63n(1<<40)), 0) // even: never a seeded ID
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bs.wbMu.Lock()
			bs.noteHistoryKey(writeOp{opType: writeOpSet, key: keys[i], value: value})
			bs.wbMu.Unlock()
		}
	})
}
