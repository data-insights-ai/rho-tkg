package badger

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelValidStamp_LazyBuild pins that the stamp index is NOT maintained until
// the first temporal traversal (so non-temporal / tiered workloads pay nothing),
// and that the lazy build captures the existing rels correctly. A non-temporal
// scan must never trigger the build.
func TestRelValidStamp_LazyBuild(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	if err := bs.PutRelationship(newTemporalRel(100, 0, 1, 100, 200, 1)); err != nil {
		t.Fatalf("put 100: %v", err)
	}
	if err := bs.PutRelationship(newTemporalRel(101, 0, 1, 100, 0, 1)); err != nil {
		t.Fatalf("put 101: %v", err)
	}

	// No temporal traversal yet → the index must be unbuilt (zero memory).
	if bs.relValidIdx != nil || bs.relValidIdxBuilt.Load() {
		t.Fatal("relValidIdx must be nil/unbuilt before any temporal traversal (lazy)")
	}

	// A NON-temporal endpoint scan must not build it either.
	if err := bs.ForEachAdjacentEndpointAt(types.NodeID(snowflake.ID(1)), 0, false, QueryOpts{}, func(types.RelID, types.NodeID) bool {
		return true
	}); err != nil {
		t.Fatalf("non-temporal scan: %v", err)
	}
	if bs.relValidIdx != nil || bs.relValidIdxBuilt.Load() {
		t.Fatal("a non-temporal scan must not build the stamp index")
	}

	// The first TEMPORAL traversal builds it AND returns the correct edges.
	got := map[int64]bool{}
	if err := bs.ForEachAdjacentEndpointAt(types.NodeID(snowflake.ID(1)), 0, false, QueryOpts{ValidAt: 150},
		func(rel types.RelID, _ types.NodeID) bool {
			got[int64(rel)] = true
			return true
		}); err != nil {
		t.Fatalf("temporal scan: %v", err)
	}
	if bs.relValidIdx == nil || !bs.relValidIdxBuilt.Load() {
		t.Fatal("first temporal traversal must build the stamp index")
	}
	// At t=150 both rels are valid ([100,200) and [100,open)).
	if len(got) != 2 || !got[100] || !got[101] {
		t.Fatalf("temporal result = %v, want {100,101} — lazy build missed an existing rel", got)
	}
}

// OPT15 divergence gate. The inline-stamp temporal traversal
// (ForEachAdjacentEndpointAt) must return EXACTLY the set the decode path
// returns, at every query time, after every create / version-close / delete.
// The only way the inline path can diverge is a lifecycle site that forgot to
// seed, refresh, or drop a stamp — so this fuzz, by replaying random mutation
// sequences and comparing the two paths plus an independent oracle after each
// step, is the safety that makes the decode-skipping fast path shippable.
//
// Three views are cross-checked after every mutation:
//   - oracle:    expected (rel,endpoint) set computed from the TEST's own
//                bookkeeping of each rel's intended interval.
//   - decodeSet: ForEachOutgoingRel / ForEachIncomingRel + the same predicate
//                applied to the freshly DECODED row (validates that storage and
//                bookkeeping agree — catches a wrong ReplaceRelationship).
//   - fastSet:   ForEachAdjacentEndpointAt (the inline-stamp path under test).
// A stale stamp shows up as fastSet != oracle while decodeSet == oracle.

// relRec is the test's authoritative bookkeeping for one relationship.
type relRec struct {
	rid        int64
	start, end int // node index (0..numNodes-1)
	vf, vt     int64
	alive      bool
}

// passPoint reproduces MatchesPointInTime for vf != 0 (the test always sets an
// explicit ValidFrom, so the snowflake fallback never applies): from <= t AND
// (to == 0 OR to > t).
func passPoint(vf, vt, t int64) bool {
	if vf > t {
		return false
	}
	return vt == 0 || vt > t
}

// passInterval reproduces MatchesInterval: from < end AND (to == 0 OR to > start).
func passInterval(vf, vt, start, end int64) bool {
	if vf >= end {
		return false
	}
	return vt == 0 || vt > start
}

func newTemporalRel(rid int64, start, end, vf, vt int64, nodeBase int64) *types.Relationship {
	r := types.NewRelationship(
		types.RelID(snowflake.ID(rid)), 1,
		types.NodeID(snowflake.ID(nodeBase+start)),
		types.NodeID(snowflake.ID(nodeBase+end)),
	)
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(vf), ValidTo: types.Instant(vt)})
	return r
}

func TestRelValidStamp_DivergenceVsDecode(t *testing.T) {
	t.Parallel()
	const (
		numNodes = 6
		nodeBase = int64(1000)
	)
	rng := rand.New(rand.NewSource(0x0157A))
	// NB: 0 is the "no filter" sentinel in MatchesTemporalFilter (ValidAt==0),
	// so real query times start at 1. The no-filter passthrough is exercised by
	// ForEachAdjacentEndpoint's own tests.
	queryTimes := []int64{1, 50, 100, 150, 200, 250, 300, 400, 500, 700, 900}

	for trial := 0; trial < 50; trial++ {
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			bs := newTestBadgerStore(t)
			for i := int64(0); i < numNodes; i++ {
				putTestNode(t, bs, nodeBase+i, 1, nil)
			}

			var rels []*relRec
			nextRID := int64(50000) + int64(trial)*1000

			liveOpenRels := func() []*relRec {
				var out []*relRec
				for _, r := range rels {
					if r.alive && r.vt == 0 {
						out = append(out, r)
					}
				}
				return out
			}
			liveRels := func() []*relRec {
				var out []*relRec
				for _, r := range rels {
					if r.alive {
						out = append(out, r)
					}
				}
				return out
			}

			ops := 25 + rng.Intn(35)
			for op := 0; op < ops; op++ {
				switch rng.Intn(4) {
				case 0, 1: // create
					s, e := rng.Intn(numNodes), rng.Intn(numNodes)
					vf := int64(50 + rng.Intn(500))
					vt := int64(0)
					if rng.Intn(2) == 0 {
						vt = vf + int64(1+rng.Intn(500))
					}
					rid := nextRID
					nextRID++
					if err := bs.PutRelationship(newTemporalRel(rid, int64(s), int64(e), vf, vt, nodeBase)); err != nil {
						t.Fatalf("PutRelationship(%d): %v", rid, err)
					}
					rels = append(rels, &relRec{rid: rid, start: s, end: e, vf: vf, vt: vt, alive: true})

				case 2: // version-close: ReplaceRelationship moving valid_to (the easy-to-miss mutate site)
					open := liveOpenRels()
					if len(open) == 0 {
						continue
					}
					rec := open[rng.Intn(len(open))]
					newVT := rec.vf + int64(1+rng.Intn(800))
					if err := bs.ReplaceRelationship(newTemporalRel(rec.rid, int64(rec.start), int64(rec.end), rec.vf, newVT, nodeBase)); err != nil {
						t.Fatalf("ReplaceRelationship(%d): %v", rec.rid, err)
					}
					rec.vt = newVT

				case 3: // delete
					live := liveRels()
					if len(live) == 0 {
						continue
					}
					rec := live[rng.Intn(len(live))]
					if err := bs.DeleteRelationship(types.RelID(snowflake.ID(rec.rid))); err != nil {
						t.Fatalf("DeleteRelationship(%d): %v", rec.rid, err)
					}
					rec.alive = false
				}

				// Cross-check both directions at every query time after each op.
				for _, qt := range queryTimes {
					verifyPoint(t, bs, rels, nodeBase, numNodes, qt)
				}
				// And a couple of interval filters (ValidStart must be > 0, else
				// it is the no-filter sentinel).
				verifyInterval(t, bs, rels, nodeBase, numNodes, 100, 300)
				verifyInterval(t, bs, rels, nodeBase, numNodes, 1, 1000)
			}
		})
	}
}

func verifyPoint(t *testing.T, bs *Store, rels []*relRec, nodeBase int64, numNodes int, qt int64) {
	t.Helper()
	opts := QueryOpts{ValidAt: types.Instant(qt)}
	for ni := 0; ni < numNodes; ni++ {
		nid := types.NodeID(snowflake.ID(nodeBase + int64(ni)))
		for _, incoming := range []bool{false, true} {
			// Oracle from bookkeeping.
			oracle := map[int64]int64{}
			for _, rec := range rels {
				if !rec.alive {
					continue
				}
				match := (!incoming && rec.start == ni) || (incoming && rec.end == ni)
				if match && passPoint(rec.vf, rec.vt, qt) {
					// endpoint stored by the fast path is always the OTHER node;
					// for outgoing that is end, for incoming that is start.
					if incoming {
						oracle[rec.rid] = nodeBase + int64(rec.start)
					} else {
						oracle[rec.rid] = nodeBase + int64(rec.end)
					}
				}
			}

			decodeSet := map[int64]int64{}
			scan := bs.ForEachOutgoingRel
			if incoming {
				scan = bs.ForEachIncomingRel
			}
			if err := scan(nid, 0, func(r *types.Relationship) bool {
				if passPoint(int64(r.Temporal().ValidFrom), int64(r.Temporal().ValidTo), qt) {
					if incoming {
						decodeSet[int64(r.ID())] = int64(r.StartNodeID())
					} else {
						decodeSet[int64(r.ID())] = int64(r.EndNodeID())
					}
				}
				return true
			}); err != nil {
				t.Fatalf("decode scan node %d incoming=%v: %v", ni, incoming, err)
			}

			fastSet := map[int64]int64{}
			if err := bs.ForEachAdjacentEndpointAt(nid, 0, incoming, opts, func(rel types.RelID, other types.NodeID) bool {
				fastSet[int64(rel)] = int64(other)
				return true
			}); err != nil {
				t.Fatalf("fast scan node %d incoming=%v: %v", ni, incoming, err)
			}

			// Decode-arm fast path: ForEachAdjacentRelAt yields decoded rels but
			// skips decoding the inline-stamp-rejected ones — its rel set must
			// still equal the oracle.
			relScanSet := map[int64]int64{}
			if err := bs.ForEachAdjacentRelAt(nid, 0, incoming, opts, func(r *types.Relationship) bool {
				if incoming {
					relScanSet[int64(r.ID())] = int64(r.StartNodeID())
				} else {
					relScanSet[int64(r.ID())] = int64(r.EndNodeID())
				}
				return true
			}); err != nil {
				t.Fatalf("rel scan node %d incoming=%v: %v", ni, incoming, err)
			}

			assertSetEqual(t, oracle, decodeSet, fmt.Sprintf("decode vs oracle node=%d incoming=%v t=%d", ni, incoming, qt))
			assertSetEqual(t, oracle, fastSet, fmt.Sprintf("FAST endpoint vs oracle node=%d incoming=%v t=%d", ni, incoming, qt))
			assertSetEqual(t, oracle, relScanSet, fmt.Sprintf("FAST relscan vs oracle node=%d incoming=%v t=%d", ni, incoming, qt))
		}
	}
}

func verifyInterval(t *testing.T, bs *Store, rels []*relRec, nodeBase int64, numNodes int, start, end int64) {
	t.Helper()
	opts := QueryOpts{ValidStart: types.Instant(start), ValidEnd: types.Instant(end)}
	for ni := 0; ni < numNodes; ni++ {
		nid := types.NodeID(snowflake.ID(nodeBase + int64(ni)))
		oracle := map[int64]int64{}
		for _, rec := range rels {
			if rec.alive && rec.start == ni && passInterval(rec.vf, rec.vt, start, end) {
				oracle[rec.rid] = nodeBase + int64(rec.end)
			}
		}
		fastSet := map[int64]int64{}
		if err := bs.ForEachAdjacentEndpointAt(nid, 0, false, opts, func(rel types.RelID, other types.NodeID) bool {
			fastSet[int64(rel)] = int64(other)
			return true
		}); err != nil {
			t.Fatalf("fast interval scan node %d: %v", ni, err)
		}
		assertSetEqual(t, oracle, fastSet, fmt.Sprintf("FAST interval vs oracle node=%d [%d,%d)", ni, start, end))
	}
}

func assertSetEqual(t *testing.T, want, got map[int64]int64, ctx string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: size mismatch want=%d got=%d\nwant=%s\ngot=%s", ctx, len(want), len(got), dumpSet(want), dumpSet(got))
	}
	for k, v := range want {
		if gv, ok := got[k]; !ok || gv != v {
			t.Fatalf("%s: rel %d want endpoint %d got %d (present=%v)\nwant=%s\ngot=%s", ctx, k, v, gv, ok, dumpSet(want), dumpSet(got))
		}
	}
}

func dumpSet(m map[int64]int64) string {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	s := "{"
	for _, k := range keys {
		s += fmt.Sprintf("%d->%d ", k, m[k])
	}
	return s + "}"
}

// TestRelValidStamp_ReplaceWithHistoryRefreshes is the targeted gate for the
// OTHER in-place mutate site: ReplaceRelWithHistory closes valid_to on a version
// bump while leaving endpoints/type (and adjacency) untouched. A missed stamp
// refresh there would make the inline path report the OLD interval.
func TestRelValidStamp_ReplaceWithHistoryRefreshes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)

	const rid = int64(7777)
	// nodeBase=1: index 0 -> node 1 (start), index 1 -> node 2 (end).
	// v1: open-ended from t=100.
	v1 := newTemporalRel(rid, 0, 1, 100, 0, 1)
	v1.SetVersion(1)
	if err := bs.PutRelationship(v1); err != nil {
		t.Fatalf("Put v1: %v", err)
	}

	queryAt := func(qt int64) bool {
		seen := false
		opts := QueryOpts{ValidAt: types.Instant(qt)}
		if err := bs.ForEachAdjacentEndpointAt(types.NodeID(snowflake.ID(1)), 0, false, opts, func(rel types.RelID, _ types.NodeID) bool {
			if int64(rel) == rid {
				seen = true
			}
			return true
		}); err != nil {
			t.Fatalf("fast scan: %v", err)
		}
		return seen
	}

	if !queryAt(500) {
		t.Fatal("open rel should be visible at t=500 before close")
	}

	// Close it at t=300 via the history path: current=v2 (valid_to=300), prevState=v1.
	prev := newTemporalRel(rid, 0, 1, 100, 0, 1)
	prev.SetVersion(1)
	cur := newTemporalRel(rid, 0, 1, 100, 300, 1)
	cur.SetVersion(2)
	if err := bs.ReplaceRelWithHistory(cur, 1, prev); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}

	if queryAt(500) {
		t.Fatal("after close at t=300 the rel must NOT be visible at t=500 — stale inline stamp (ReplaceRelWithHistory missed the refresh)")
	}
	if !queryAt(200) {
		t.Fatal("rel must still be visible at t=200 (inside [100,300))")
	}
}
