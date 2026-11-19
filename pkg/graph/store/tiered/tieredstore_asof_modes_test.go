package tiered

import (
	"fmt"
	"strings"
	"testing"
	"time"

	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredAsOfAnswersEqualOnOffInvalidated: on the reference shard and the
// hot event shard, the snapshot's as-of walk answers every pin of node and
// relationship chains the same with each shard's as-of index used, declined
// and invalidated (a moved recovery generation), before and after more
// appends, and every history version answers at its own pins.
func TestTieredAsOfAnswersEqualOnOffInvalidated(t *testing.T) {
	ts := newTestTieredStore(t)
	caseTok, _, eventTok := installDefaultTestLabelRegistry(t, ts)
	type chain struct {
		n *types.Node
		r *types.Relationship
	}
	var chains []chain
	tm := func(v uint32) *types.TemporalMetadata {
		return &types.TemporalMetadata{TxFrom: types.Instant(v+1) * 100, TxTo: types.Instant(v+2) * 100, ValidFrom: 1}
	}
	putVersions := func(c chain, from, to uint32) {
		t.Helper()
		for v := from; v < to; v++ {
			nh, rh := c.n.DeepCopy(), c.r.DeepCopy()
			nh.SetVersion(v)
			nh.SetTemporal(tm(v))
			rh.SetVersion(v)
			rh.SetTemporal(tm(v))
			if err := ts.PutNodeVersion(c.n.ID(), v, nh); err != nil {
				t.Fatal(err)
			}
			if err := ts.PutRelVersion(c.r.ID(), v, rh); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, tok := range []uint16{caseTok, eventTok} {
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
		n.SetVersion(12)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 1300, ValidFrom: 1})
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
		r.SetVersion(12)
		r.SetTemporal(n.Temporal())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		c := chain{n, r}
		chains = append(chains, c)
		putVersions(c, 0, 4)
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, c := range chains {
		putVersions(c, 4, 8) // buffered
	}
	stores := []*BadgerStore{ts.refShard, ts.hotShard.store}
	read := func() map[string]string {
		t.Helper()
		snap := ts.NewReadSnapshot()
		defer snap.Close()
		walk := snap.(snapshotAsOfWalk)
		out := map[string]string{}
		for i, c := range chains {
			for pin := types.Instant(0); pin <= 1500; pin += 50 {
				n, nh, nerr := walk.NodeAsOfWalk(c.n.ID(), pin)
				r, rh, rerr := walk.RelAsOfWalk(c.r.ID(), pin)
				nv, rv := -1, -1
				if n != nil {
					nv = int(n.Version())
				}
				if r != nil {
					rv = int(r.Version())
				}
				out[fmt.Sprintf("%d/%d", i, pin)] = fmt.Sprintf("node v%d %v %v; rel v%d %v %v", nv, nh, nerr, rv, rh, rerr)
			}
		}
		return out
	}
	// probes counts the native proof reads of the index's read side: none
	// when every shard declines it. seeks counts the as-of seek keys the
	// reads visited, one per answer the index gave: a read that probes the
	// proof and then takes the scan visits none. visits counts every index
	// entry visited, the walks of other indexes the reads take included.
	type work struct{ probes, seeks, visits int64 }
	probed := func() (map[string]string, work) {
		p, stopProbes := badgerstore.CountAsOfIndexProbesForTest()
		defer stopProbes()
		s, stopSeeks := badgerstore.CountAsOfIndexSeekEntriesForTest()
		defer stopSeeks()
		v, stopVisits := badgerstore.CountIndexEntriesVisitedForTest()
		defer stopVisits()
		out := read()
		return out, work{p(), s(), v()}
	}
	compare := func(stage string) map[string]string {
		t.Helper()
		on, onWork := probed()
		for _, bs := range stores {
			bs.SetAsOfIndexDeclinedForTest(true)
		}
		off, offWork := probed()
		for _, bs := range stores {
			bs.SetAsOfIndexDeclinedForTest(false)
		}
		if (onWork.probes == 0) != strings.HasSuffix(stage, "buffered") || offWork.probes != 0 {
			t.Fatalf("%s: proof probes %d with the index used (want > 0 once flushed, 0 while every chain is buffered), %d declined (want 0)", stage, onWork.probes, offWork.probes)
		}
		if stage == "flushed" && (onWork.seeks == 0 || onWork.visits == 0 || offWork.seeks != 0) {
			t.Fatalf("%s: index used: %+v (want seek keys and index entries visited: it answered); declined: %+v (want no seek key)", stage, onWork, offWork)
		}
		for k, a := range on {
			if off[k] != a {
				t.Fatalf("%s %s: index used %q, declined %q", stage, k, a, off[k])
			}
		}
		return on
	}
	compare("buffered")
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	before := compare("flushed")
	for i := range chains {
		for v := range 8 {
			key := fmt.Sprintf("%d/%d", i, (v+1)*100+50)
			if want := fmt.Sprintf("node v%d true <nil>; rel v%d true <nil>", v, v); before[key] != want {
				t.Fatalf("control %s: %q, want %q", key, before[key], want)
			}
		}
	}
	for _, bs := range stores {
		if err := bs.InvalidateAsOfGenerationForTest(); err != nil {
			t.Fatal(err)
		}
	}
	for k, a := range read() {
		if before[k] != a {
			t.Fatalf("%s: index used %q, invalidated %q", k, before[k], a)
		}
	}
	for _, c := range chains {
		putVersions(c, 8, 12)
	}
	compare("appended after invalidation, buffered")
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	compare("appended after invalidation, flushed")
}

// TestTieredAsOfQuietChainsReadIndexBesideOtherFlushes runs on ai-soc's
// store settings (on disk, SyncWrites: a flush per mutation and no flush
// loop; delta history, no compression). Node and relationship chains on the
// reference shard and on the hot event shard are flushed and then left
// alone. While the snapshot's as-of walk reads every pin of them, each shard
// store meets, at both seams of every indexed read, one more row of another
// chain of that shard appended and flushed and one meta key committed (the
// commit floor an event flush writes into the reference shard). The index
// answers exactly the reads it answers on the quiet store, and every answer
// equals the declined run's. Before, a flush between a read's two lock
// sections made it decline, so the seek keys visited fell with the load.
func TestTieredAsOfQuietChainsReadIndexBesideOtherFlushes(t *testing.T) {
	ts, err := New(Config{
		DataDir: t.TempDir(), RefLabels: []string{"Case", "User"}, ShardWindow: 7 * 24 * time.Hour,
		SyncWrites: true, HistoryDeltaEncoding: true, NoCompression: true, BlockCacheSize: 64 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	caseTok, _, eventTok := installDefaultTestLabelRegistry(t, ts)
	tm := func(v uint32) *types.TemporalMetadata {
		return &types.TemporalMetadata{TxFrom: types.Instant(v+1) * 100, TxTo: types.Instant(v+2) * 100, ValidFrom: 1}
	}
	type chain struct {
		n *types.Node
		r *types.Relationship
	}
	// Two chains a shard: the first is read, the second is the other chain
	// the seams append to.
	var quiet, other []chain
	for _, tok := range []uint16{caseTok, eventTok} {
		for i := range 2 {
			n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
			n.SetVersion(8)
			n.SetTemporal(&types.TemporalMetadata{TxFrom: 900, ValidFrom: 1})
			if err := ts.PutNode(n); err != nil {
				t.Fatal(err)
			}
			r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
			r.SetVersion(8)
			r.SetTemporal(n.Temporal())
			if err := ts.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
			c := chain{n, r}
			if i == 0 {
				quiet = append(quiet, c)
				for v := range uint32(8) {
					nh, rh := n.DeepCopy(), r.DeepCopy()
					nh.SetVersion(v)
					nh.SetTemporal(tm(v))
					rh.SetVersion(v)
					rh.SetTemporal(tm(v))
					if err := ts.PutNodeVersion(n.ID(), v, nh); err != nil {
						t.Fatal(err)
					}
					if err := ts.PutRelVersion(r.ID(), v, rh); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				other = append(other, c)
			}
		}
	}
	if err := ts.DurableFlush(); err != nil { // the commit group's flush
		t.Fatal(err)
	}
	stores := []*BadgerStore{ts.refShard, ts.hotShard.store}
	read := func() (map[string]string, int64) {
		t.Helper()
		seeks, stop := badgerstore.CountAsOfIndexSeekEntriesForTest()
		defer stop()
		snap := ts.NewReadSnapshot()
		defer snap.Close()
		walk := snap.(snapshotAsOfWalk)
		out := map[string]string{}
		for i, c := range quiet {
			for pin := types.Instant(50); pin <= 950; pin += 100 { // before the chain, each history version, the current row
				n, nh, nerr := walk.NodeAsOfWalk(c.n.ID(), pin)
				r, rh, rerr := walk.RelAsOfWalk(c.r.ID(), pin)
				nv, rv := -1, -1
				if n != nil {
					nv = int(n.Version())
				}
				if r != nil {
					rv = int(r.Version())
				}
				out[fmt.Sprintf("%d/%d", i, pin)] = fmt.Sprintf("node v%d %v %v; rel v%d %v %v", nv, nh, nerr, rv, rh, rerr)
			}
		}
		return out, seeks()
	}
	calm, calmSeeks := read()
	for i := range quiet {
		for v := range 8 {
			key := fmt.Sprintf("%d/%d", i, (v+1)*100+50)
			if want := fmt.Sprintf("node v%d true <nil>; rel v%d true <nil>", v, v); calm[key] != want {
				t.Fatalf("control %s: %q, want %q", key, calm[key], want)
			}
		}
	}
	// 2 shards x (node + rel) x one pin inside each of the 8 history versions.
	if calmSeeks != 32 {
		t.Fatalf("control: %d as-of seek keys visited on the quiet store, want 32 (every history pin through the index)", calmSeeks)
	}
	for _, bs := range stores {
		bs.SetAsOfIndexDeclinedForTest(true)
	}
	declined, declinedSeeks := read()
	for _, bs := range stores {
		bs.SetAsOfIndexDeclinedForTest(false)
	}
	if declinedSeeks != 0 {
		t.Fatalf("declined run visited %d as-of seek keys, want 0", declinedSeeks)
	}
	seams := make([]int, len(stores))
	next := make([]uint32, len(stores))
	for i, bs := range stores {
		bs.SetAsOfViewPinHookForTest(func(bool) {
			seams[i]++
			v := next[i]
			next[i]++
			// Straight into this shard's store: with SyncWrites the append is
			// a flush. Node and relationship rows take turns.
			var err error
			if v%2 == 0 {
				nh := other[i].n.DeepCopy()
				nh.SetVersion(v / 2)
				nh.SetTemporal(tm(v / 2))
				err = bs.PutNodeVersion(nh.ID(), v/2, nh)
			} else {
				rh := other[i].r.DeepCopy()
				rh.SetVersion(v / 2)
				rh.SetTemporal(tm(v / 2))
				err = bs.PutRelVersion(rh.ID(), v/2, rh)
			}
			if err == nil {
				err = bs.MetaSet("floor", []byte{byte(v)})
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	busy, busySeeks := read()
	for _, bs := range stores {
		bs.SetAsOfViewPinHookForTest(nil)
	}
	for i, n := range seams {
		if n == 0 {
			t.Fatalf("shard store %d: no indexed read reached a seam: the test proves nothing there", i)
		}
	}
	if busySeeks != calmSeeks {
		t.Fatalf("beside other chains' flushes the index answered %d reads, on the quiet store %d", busySeeks, calmSeeks)
	}
	for k, a := range calm {
		if busy[k] != a || declined[k] != a {
			t.Fatalf("%s: quiet %q, beside other chains' flushes %q, declined %q", k, a, busy[k], declined[k])
		}
	}
}
