package badger

import (
	"errors"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// buildRelTypeTemporalIndexPerRel is the pre-10(a) build, kept as the oracle: a
// point read plus a history prefix scan per relationship. fold, when set, receives
// every folded row: (id, version), -1 for the current row.
func buildRelTypeTemporalIndexPerRel(bs *Store, rids []types.RelID, fold func(snowflake.ID, int64)) (*index.TemporalIndex, error) {
	if fold == nil {
		fold = func(snowflake.ID, int64) {}
	}
	ti := index.NewTemporalIndex()
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, err
		}
		rawID := rid.SnowflakeID()
		from, to := index.RelTemporalBounds(types.LegacyValidAxis, rawID, r.Temporal())
		ti.Extend(rawID, from, to)
		fold(rawID, -1)
		hist, err := bs.GetRelHistory(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, err
		}
		for _, hv := range hist {
			if hv == nil {
				continue
			}
			hf, ht := index.RelTemporalBounds(types.LegacyValidAxis, rawID, hv.Temporal())
			ti.Extend(rawID, hf, ht)
			fold(rawID, int64(hv.Version()))
		}
	}
	return ti, nil
}

const (
	rebuildTypeA = uint16(7)
	rebuildTypeB = uint16(8)
)

func rebuildRel(id types.RelID, relType uint16, version uint32, from, to types.Instant) *types.Relationship {
	r := types.NewRelationship(id, relType, 1, 2)
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	r.SetVersion(version)
	return r
}

// seedRelTemporalRebuild writes n rels of type A interleaved by ID with n of type
// B, then diverging lifecycles on A (updates with history, deletes, re-imports, a
// history-only row above the current version). B has history too, so the history
// range holds foreign rows.
func seedRelTemporalRebuild(t testing.TB, bs *Store, n int) {
	t.Helper()
	for _, id := range []types.NodeID{1, 2} {
		if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		base := types.Instant(1000 + 100*i)
		a := types.RelID(10_000 + 2*i)
		if i >= n/2 {
			a += 1000
		}
		if i == n/2 { // 100 type-B relationships with history in a gap of type A
			for g := range types.RelID(100) {
				gap := rebuildRel(10_000+types.RelID(2*n)+g, rebuildTypeB, 0, base, base+50)
				if err := bs.PutRelationship(gap); err != nil {
					t.Fatal(err)
				}
				if err := bs.ReplaceRelWithHistory(rebuildRel(gap.ID(), rebuildTypeB, 1, base, 0), 0, gap); err != nil {
					t.Fatal(err)
				}
			}
		}
		b := a + 1
		cur := rebuildRel(a, rebuildTypeA, 0, base, base+50)
		if err := bs.PutRelationship(cur); err != nil {
			t.Fatal(err)
		}
		other := rebuildRel(b, rebuildTypeB, 0, base, base+50)
		if err := bs.PutRelationship(other); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 { // B history between A's history rows
			next := rebuildRel(b, rebuildTypeB, 1, base-500, base)
			if err := bs.ReplaceRelWithHistory(next, 0, other); err != nil {
				t.Fatal(err)
			}
		}
		switch {
		case i%3 == 0: // updated twice: the envelope spans every version
			for v := uint32(1); v <= 2; v++ {
				next := rebuildRel(a, rebuildTypeA, v, base+types.Instant(200*v), 0)
				if v == 1 {
					next.SetTemporal(&types.TemporalMetadata{ValidFrom: base - 300, ValidTo: base + 10})
				}
				if err := bs.ReplaceRelWithHistory(next, v-1, cur); err != nil {
					t.Fatal(err)
				}
				cur = next
			}
		case i%5 == 0: // deleted before the snapshot
			if err := bs.DeleteRelationship(a); err != nil {
				t.Fatal(err)
			}
		case i%7 == 0: // deleted, then re-imported under the same ID
			if err := bs.DeleteRelationship(a); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutRelationship(rebuildRel(a, rebuildTypeA, 5, base+5000, base+6000)); err != nil {
				t.Fatal(err)
			}
		case i%11 == 0: // a history-only row above the current version
			if err := bs.PutRelVersion(a, 9, rebuildRel(a, rebuildTypeA, 9, base-9000, base-8000)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestRelTemporalIndexRebuildSequential pins that the sequential build (backlog
// 10(a)) equals the per-relationship oracle on every envelope, in one transaction.
func TestRelTemporalIndexRebuildSequential(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	const n = 120
	for _, tc := range []struct {
		name          string
		delta, reopen bool
	}{
		{"pending-full", false, false},
		{"pending-delta", true, false},
		{"reopened-full", false, true},
		{"reopened-delta", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *Store {
				bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: tc.delta, HistoryAnchorInterval: 2, FlushInterval: 1<<63 - 1, CacheCapacity: 8})
				if err != nil {
					t.Fatal(err)
				}
				return bs
			}
			bs := open()
			seedRelTemporalRebuild(t, bs, n)
			if !tc.reopen {
				// Committed rows the cache released (the row walk answers), and
				// buffered history rows shadowing committed ones.
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				for _, i := range []int{11, 22, 44, 88} {
					a := types.RelID(10_000 + 2*i)
					if i >= n/2 {
						a += 1000
					}
					base := types.Instant(1000 + 100*i)
					if err := bs.PutRelVersion(a, 9, rebuildRel(a, rebuildTypeA, 9, base-20_000, base-19_000)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.reopen {
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				bs = open()
			}
			t.Cleanup(func() { _ = bs.Close() })

			bs.idxMu.RLock()
			rids := bs.relTypeRelIDsSnapshotLocked(rebuildTypeA)
			bs.idxMu.RUnlock()
			// Deleted between the snapshot and the build: skipped by both.
			for _, gone := range []types.RelID{10_000 + 2*1, 10_000 + 2*3} {
				if err := bs.DeleteRelationship(gone); err != nil {
					t.Fatal(err)
				}
			}

			wantRows := make(map[[2]int64]bool)
			want, err := buildRelTypeTemporalIndexPerRel(bs, rids, func(id snowflake.ID, v int64) { wantRows[[2]int64{int64(id), v}] = true })
			if err != nil {
				t.Fatal(err)
			}
			gotRows := make(map[[2]int64]bool)
			relTemporalBuildFoldHook = func(id snowflake.ID, v int64) { gotRows[[2]int64{int64(id), v}] = true }
			defer func() { relTemporalBuildFoldHook = nil }()
			txns, seeks := readTxnsTotal.Load(), relTemporalBuildSeeks.Load()
			got, err := bs.buildRelTypeTemporalIndex(rids)
			if err != nil {
				t.Fatal(err)
			}
			relTemporalBuildFoldHook = nil
			// Every row the oracle folds, inner history rows included, and no other.
			for k := range wantRows {
				if !gotRows[k] {
					t.Errorf("row %v (id, version; -1 = current) not folded", k)
				}
			}
			for k := range gotRows {
				if !wantRows[k] {
					t.Errorf("row %v folded, the oracle has no such row", k)
				}
			}
			txns, seeks = readTxnsTotal.Load()-txns, relTemporalBuildSeeks.Load()-seeks
			t.Logf("%d snapshot ids, %d indexed: %d read txns, %d seeks", len(rids), want.Len(), txns, seeks)

			if got.Len() != want.Len() {
				t.Errorf("index holds %d relationships, oracle %d", got.Len(), want.Len())
			}
			var diffs []string
			for _, rid := range rids {
				id := rid.SnowflakeID()
				gf, gt, gok := got.EnvelopeOf(id)
				wf, wt, wok := want.EnvelopeOf(id)
				if gf != wf || gt != wt || gok != wok {
					diffs = append(diffs, fmt.Sprintf("%d: got [%d,%d) %v, want [%d,%d) %v", id, gf, gt, gok, wf, wt, wok))
				}
			}
			if len(diffs) > 0 {
				t.Errorf("%d envelopes differ from the oracle, first: %v", len(diffs), diffs[:min(3, len(diffs))])
			}
			// The oracle must see the shapes the test claims to cover.
			if want.Len() < n/2 || want.Len() >= len(rids) {
				t.Fatalf("oracle holds %d of %d snapshot ids: the scenario lost its deleted or kept rows", want.Len(), len(rids))
			}
			if txns > 1 {
				t.Errorf("build opened %d read transactions for %d relationships, want 1 (no per-relationship read)", txns, len(rids))
			}
			// One seek per range, plus one per range across the 100-key gap.
			if seeks > 4 {
				t.Errorf("build did %d iterator seeks for %d relationships, want <= 4", seeks, len(rids))
			}
		})
	}
}

// BenchmarkRelTemporalIndexRebuild: per-relationship vs sequential build on a
// reopened store of 200,000 rels with one history row each. Reports rels/s.
func BenchmarkRelTemporalIndexRebuild(b *testing.B) {
	if testing.Short() {
		b.Skip("seeds 400,000 relationships")
	}
	const n = 200_000
	dir := b.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	for _, id := range []types.NodeID{1, 2} {
		if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
			b.Fatal(err)
		}
	}
	for i := range n {
		base := types.Instant(1000 + 100*i)
		a := types.RelID(10_000 + 2*i)
		cur := rebuildRel(a, rebuildTypeA, 0, base, base+50)
		if err := bs.PutRelationship(cur); err != nil {
			b.Fatal(err)
		}
		if err := bs.PutRelationship(rebuildRel(a+1, rebuildTypeB, 0, base, base+50)); err != nil {
			b.Fatal(err)
		}
		if err := bs.ReplaceRelWithHistory(rebuildRel(a, rebuildTypeA, 1, base-300, 0), 0, cur); err != nil {
			b.Fatal(err)
		}
	}
	if err := bs.Close(); err != nil {
		b.Fatal(err)
	}
	bs, err = New(Config{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bs.Close() })
	bs.idxMu.RLock()
	rids := bs.relTypeRelIDsSnapshotLocked(rebuildTypeA)
	bs.idxMu.RUnlock()

	for _, bc := range []struct {
		name  string
		build func([]types.RelID) (*index.TemporalIndex, error)
	}{
		{"per-rel", func(r []types.RelID) (*index.TemporalIndex, error) {
			return buildRelTypeTemporalIndexPerRel(bs, r, nil)
		}},
		{"sequential", bs.buildRelTypeTemporalIndex},
	} {
		b.Run(bc.name, func(b *testing.B) {
			for b.Loop() {
				ti, err := bc.build(rids)
				if err != nil || ti.Len() != n {
					b.Fatalf("build: %d relationships, %v; want %d", ti.Len(), err, n)
				}
			}
			b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "rels/s")
		})
	}
}

type relGetHook struct {
	index.EntityCache[*types.Relationship]
	nth, seen int
	fire      func()
}

func (c *relGetHook) GetNoPromote(id snowflake.ID) (*types.Relationship, index.CacheStatus) {
	c.seen++
	if c.seen == c.nth {
		c.fire()
	}
	return c.EntityCache.GetNoPromote(id)
}

// Dirty rows at build start, a flush (commit + evict) lands mid-build.
func TestRelTemporalIndexRebuild_FlushEvictDropsLiveRel(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	putTestNode(t, bs, 1, 7, nil)
	putTestNode(t, bs, 2, 7, nil)
	const n = 64
	for i := 0; i < n; i++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(5000+i)), 9, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(100 + i), ValidTo: types.Instant(200 + i)})
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	bs.idxMu.RLock()
	rids := bs.relTypeRelIDsSnapshotLocked(9)
	bs.idxMu.RUnlock()
	fired := false
	bs.relCache = &relGetHook{EntityCache: bs.relCache, nth: 2, fire: func() {
		fired = true
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}}
	got, err := bs.buildRelTypeTemporalIndex(rids)
	if err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("hook never fired")
	}
	if got.Len() != n {
		t.Fatalf("index holds %d of %d live relationships (flush-evict mid-build dropped rows)", got.Len(), n)
	}
}

// Same window, rel updated (with history) before the build: the new current
// version is lost, the old one is read from the stale snapshot.
func TestRelTemporalIndexRebuild_FlushEvictStaleVersion(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	putTestNode(t, bs, 1, 7, nil)
	putTestNode(t, bs, 2, 7, nil)
	const n = 16
	var olds []*types.Relationship
	for i := 0; i < n; i++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(6000+i)), 9, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		olds = append(olds, r)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, old := range olds {
		next := types.NewRelationship(old.ID(), 9, 1, 2)
		next.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 6000})
		next.SetVersion(1)
		if err := bs.ReplaceRelWithHistory(next, 0, old); err != nil {
			t.Fatal(err)
		}
	}
	bs.idxMu.RLock()
	rids := bs.relTypeRelIDsSnapshotLocked(9)
	bs.idxMu.RUnlock()
	bs.relCache = &relGetHook{EntityCache: bs.relCache, nth: 2, fire: func() {
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}}
	got, err := bs.buildRelTypeTemporalIndex(rids)
	if err != nil {
		t.Fatal(err)
	}
	bad := 0
	for _, rid := range rids {
		_, to, ok := got.EnvelopeOf(rid.SnowflakeID())
		if !ok || to < 6000 {
			bad++
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d envelopes miss the current version [5000,6000)", bad, n)
	}
}

// An update lands after the build's pending snapshot, before its cache lookup:
// the prior (pre-build) version's interval must stay in the envelope.
func TestRelTemporalIndexRebuild_UpdateMidBuildKeepsPriorVersion(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 7, nil)
	putTestNode(t, bs, 2, 7, nil)
	const n = 4
	var olds []*types.Relationship
	for i := 0; i < n; i++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(7000+i)), 9, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		olds = append(olds, r)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.idxMu.RLock()
	rids := bs.relTypeRelIDsSnapshotLocked(9)
	bs.idxMu.RUnlock()
	inner := bs.relCache
	bs.relCache = &relGetHook{EntityCache: inner, nth: 1, fire: func() {
		for _, old := range olds {
			next := types.NewRelationship(old.ID(), 9, 1, 2)
			next.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 6000})
			next.SetVersion(1)
			if err := bs.ReplaceRelWithHistory(next, 0, old); err != nil {
				t.Fatal(err)
			}
		}
	}}
	got, err := bs.buildRelTypeTemporalIndex(rids)
	if err != nil {
		t.Fatal(err)
	}
	bad := 0
	for _, rid := range rids {
		from, _, ok := got.EnvelopeOf(rid.SnowflakeID())
		if !ok || from > 100 {
			bad++
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d envelopes lost the pre-build version [100,200)", bad, n)
	}
}

// relGetSeq runs fire(k) at the k-th cache lookup; lookups made while fire
// runs (the writes it does) are not counted.
type relGetSeq struct {
	index.EntityCache[*types.Relationship]
	seen   int
	inFire bool
	fire   func(k int)
}

func (c *relGetSeq) GetNoPromote(id snowflake.ID) (*types.Relationship, index.CacheStatus) {
	if !c.inFire {
		c.seen++
		c.inFire = true
		c.fire(c.seen)
		c.inFire = false
	}
	return c.EntityCache.GetNoPromote(id)
}

// seedRace22 writes four flushed relationships of type 9, valid [100,200).
func seedRace22(t *testing.T) (*Store, []*types.Relationship) {
	t.Helper()
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 7, nil)
	putTestNode(t, bs, 2, 7, nil)
	var rels []*types.Relationship
	for i := range 4 {
		r := types.NewRelationship(types.RelID(snowflake.ID(7000+i)), 9, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return bs, rels
}

func race22Update(t *testing.T, bs *Store, old *types.Relationship) {
	t.Helper()
	next := types.NewRelationship(old.ID(), 9, 1, 2)
	next.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 6000})
	next.SetVersion(1)
	if err := bs.ReplaceRelWithHistory(next, 0, old); err != nil {
		t.Fatal(err)
	}
}

// race22MustKeep asserts that pruning at each instant keeps id: id holds a row
// valid there, so dropping it is an unsound prune.
func race22MustKeep(t *testing.T, bs *Store, id types.RelID, at ...types.Instant) {
	t.Helper()
	for _, v := range at {
		kept, ok := bs.PruneRelTypeTemporalCandidates(9, []types.RelID{id}, QueryOpts{ValidAt: v})
		if !ok || len(kept) != 1 {
			t.Errorf("prune at %d kept %v (ok=%v), want [%d]: it holds a row valid there", v, kept, ok, id)
		}
	}
}

// Backlog 22: a relationship updated after the build read it, before the
// install. Its new row [5000,6000) must be in the installed envelope.
func TestCreateRelTemporalIndex_WriteBetweenSnapshotAndInstall(t *testing.T) {
	bs, rels := seedRace22(t)
	bs.relCache = &relGetSeq{EntityCache: bs.relCache, fire: func(k int) {
		if k == 2 { // relationship 7000 is read; update it
			race22Update(t, bs, rels[0])
		}
	}}
	if err := bs.CreateRelTemporalIndex(9); err != nil {
		t.Fatal(err)
	}
	race22MustKeep(t, bs, rels[0].ID(), 150, 5500)
}

// Updated before the build reaches it, its history row flushed after the
// build read its history and before the second buffer read: the pre-build row
// [100,200) must stay in the envelope.
func TestCreateRelTemporalIndex_UpdateMidBuildHistoryFlushedLate(t *testing.T) {
	bs, rels := seedRace22(t)
	bs.relCache = &relGetSeq{EntityCache: bs.relCache, fire: func(k int) {
		switch k {
		case 1: // before relationship 7002 is reached
			race22Update(t, bs, rels[2])
		case 4: // 7002 is done
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}}
	if err := bs.CreateRelTemporalIndex(9); err != nil {
		t.Fatal(err)
	}
	race22MustKeep(t, bs, rels[2].ID(), 150, 5500)
}
