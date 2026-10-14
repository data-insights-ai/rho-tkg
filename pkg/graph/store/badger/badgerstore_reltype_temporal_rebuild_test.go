package badger

import (
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// buildRelTypeTemporalIndexPerRel is the pre-10(a) build, kept as the oracle:
// one current-row point read plus one history prefix scan per relationship.
func buildRelTypeTemporalIndexPerRel(bs *Store, rids []types.RelID) (*index.TemporalIndex, error) {
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
		from, to := index.RelTemporalBounds(rawID, r.Temporal())
		ti.Extend(rawID, from, to)
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
			hf, ht := index.RelTemporalBounds(rawID, hv.Temporal())
			ti.Extend(rawID, hf, ht)
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

// seedRelTemporalRebuild writes n relationships of type A interleaved by ID
// with n of type B, then diverging lifecycles on type A: updates with history
// (the envelope moves earlier and later), deletes, re-imports of a deleted ID,
// and a history-only row written above the current version. Type B gets
// history too, so the history range holds foreign rows between A's.
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

// TestRelTemporalIndexRebuildSequential: the rel-type temporal index build
// (backlog 10(a)) equals the per-relationship oracle on every envelope, and
// reads the store in one transaction instead of one or two per relationship.
func TestRelTemporalIndexRebuildSequential(t *testing.T) {
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
				bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: tc.delta, HistoryAnchorInterval: 2, FlushInterval: 1<<63 - 1})
				if err != nil {
					t.Fatal(err)
				}
				return bs
			}
			bs := open()
			seedRelTemporalRebuild(t, bs, n)
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

			want, err := buildRelTypeTemporalIndexPerRel(bs, rids)
			if err != nil {
				t.Fatal(err)
			}
			txns, seeks := readTxnsTotal.Load(), relTemporalBuildSeeks.Load()
			got, err := bs.buildRelTypeTemporalIndex(rids)
			if err != nil {
				t.Fatal(err)
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

// BenchmarkRelTemporalIndexRebuild: the per-relationship build (before 10(a))
// against the sequential one, on a reopened store of 200,000 relationships of
// the indexed type, each with one history row, interleaved with 200,000 of
// another type. Reports rels/s.
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
		{"per-rel", func(r []types.RelID) (*index.TemporalIndex, error) { return buildRelTypeTemporalIndexPerRel(bs, r) }},
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
