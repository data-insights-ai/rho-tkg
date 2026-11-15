package tiered

import (
	"errors"
	"strings"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type historyMetaBudgetFixture struct {
	ts     *Store
	shards []*BadgerStore
	node   *types.Node
	rel    *types.Relationship
	want   []storecontract.VersionTemporalMeta
}

func newHistoryMetaBudgetFixture(t *testing.T, split bool) historyMetaBudgetFixture {
	t.Helper()
	ts := newTestTieredStore(t)
	label, _, _ := installDefaultTestLabelRegistry(t, ts)
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), label, nil)
	n.SetVersion(3)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: 40, ValidFrom: 4})
	if err := n.SetProperty("payload", strings.Repeat("n", 256)); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
	r.SetVersion(3)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 40, ValidFrom: 4})
	if err := r.SetProperty("payload", strings.Repeat("r", 256)); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	f := historyMetaBudgetFixture{ts: ts, shards: []*BadgerStore{ts.refShard}, node: n, rel: r}
	put := func(bs *BadgerStore, v uint32, from types.Instant) {
		if err := bs.PutNodeVersion(n.ID(), v, stampedNodeRow(n, v, from, from+10, 0)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelVersion(r.ID(), v, stampedRelRow(r, v, from, from+10, 0)); err != nil {
			t.Fatal(err)
		}
	}
	put(ts.refShard, 0, 10)
	f.want = append(f.want, storecontract.VersionTemporalMeta{Version: 0, Temporal: &types.TemporalMetadata{TxFrom: 10, TxTo: 20}})
	if split {
		archive := mustArchiveStore(t, ts)
		f.shards = append(f.shards, archive)
		put(archive, 1, 20)
		put(ts.refShard, 2, 30)
		put(archive, 2, 90) // the owner's same-version entry must win
		f.want = append(f.want,
			storecontract.VersionTemporalMeta{Version: 1, Temporal: &types.TemporalMetadata{TxFrom: 20, TxTo: 30}},
			storecontract.VersionTemporalMeta{Version: 2, Temporal: &types.TemporalMetadata{TxFrom: 30, TxTo: 40}})
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f historyMetaBudgetFixture) read(s storecontract.ReadSnapshot, rel bool) ([]storecontract.VersionTemporalMeta, error) {
	if rel {
		return s.RelHistoryTemporalMeta(f.rel.ID())
	}
	return s.NodeHistoryTemporalMeta(f.node.ID())
}

func (f historyMetaBudgetFixture) check(t *testing.T, got []storecontract.VersionTemporalMeta) {
	t.Helper()
	if len(got) != len(f.want) {
		t.Fatalf("metadata length %d, want %d: %+v", len(got), len(f.want), got)
	}
	for i, want := range f.want {
		if got[i].Version != want.Version || got[i].Temporal == nil || *got[i].Temporal != *want.Temporal {
			t.Fatalf("metadata[%d] = v%d %+v, want v%d %+v", i, got[i].Version, got[i].Temporal, want.Version, want.Temporal)
		}
	}
}

func (f historyMetaBudgetFixture) metaDecodes() int64 {
	var n int64
	for _, bs := range f.shards {
		n += bs.HistoryMetaDecodesForTest()
	}
	return n
}

// A cold current-row probe precedes the historical list. Its native decoder
// must stop at the byte limit, and its ID probe must use the shared allowance.
func TestHistoryMetaBudgetCurrentProbe(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			f := newHistoryMetaBudgetFixture(t, false)
			read := func(b storecontract.ReadBudget) ([]storecontract.VersionTemporalMeta, int64, int64, error) {
				f.ts.refShard.NodeCacheForTest().ResetForTest()
				f.ts.refShard.RelCacheForTest().ResetForTest()
				s := f.ts.NewReadSnapshot()
				defer s.Close()
				_, meter := storecontract.WithReadBudget(t.Context(), b)
				s.(storecontract.ReadSnapshotMeterCapability).MeterReads(meter.Tally("metadata"))
				before := f.metaDecodes()
				var got []storecontract.VersionTemporalMeta
				var err error
				decoded := badgerstore.CountRowDecodesForTest(func() { got, err = f.read(s, rel) })
				return got, decoded, f.metaDecodes() - before, err
			}
			got, decoded, metas, err := read(storecontract.ReadBudget{})
			if err != nil || decoded != 1 || metas != 1 {
				t.Fatalf("control: %d row/%d meta decodes, %v", decoded, metas, err)
			}
			f.check(t, got)
			got, decoded, metas, err = read(storecontract.ReadBudget{DecodedBytes: 1})
			if !errors.Is(err, storecontract.ErrReadBudget) || got != nil || decoded != 0 || metas != 0 {
				t.Fatalf("refused current decode: result %v, %d row/%d meta decodes, %v", got, decoded, metas, err)
			}
			got, decoded, metas, err = read(storecontract.ReadBudget{Candidates: 1})
			wantDecodes := int64(0)
			if rel {
				wantDecodes = 1 // relationship routing validates its first row probe
			}
			if !errors.Is(err, storecontract.ErrReadBudget) || got != nil || decoded != wantDecodes || metas != 0 {
				t.Fatalf("refused second probe: result %v, %d row/%d meta decodes, %v", got, decoded, metas, err)
			}
		})
	}
}

// Both shards contain real history and each fits alone. The tiered result
// retains both lists and copies four entries before discarding the duplicate.
// Native copy counters remain independent of the meter being tested.
func TestHistoryMetaBudgetCrossShardBuffers(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			f := newHistoryMetaBudgetFixture(t, true)
			plain := f.ts.NewReadSnapshot()
			got, err := f.read(plain, rel)
			plain.Close()
			if err != nil {
				t.Fatal(err)
			}
			f.check(t, got)
			var nativeBytes [2]uint64
			for i, bs := range f.shards {
				s := bs.NewReadSnapshot()
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				s.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("native metadata"))
				got, err := f.read(s, rel)
				s.Close()
				if err != nil || len(got) != 2 || got[0].Version != uint32(i) || got[1].Version != 2 {
					t.Fatalf("shard %d control: %+v, %v", i, got, err)
				}
				nativeBytes[i] = m.Work().BufferBytes
			}
			read := func(b storecontract.ReadBudget) ([]storecontract.VersionTemporalMeta, storecontract.ReadWork, *historyMetaCopyCounts, int64, error) {
				counts := new(historyMetaCopyCounts)
				historyMetaCopies.Store(counts)
				defer historyMetaCopies.Store(nil)
				s := f.ts.NewReadSnapshot()
				defer s.Close()
				_, m := storecontract.WithReadBudget(t.Context(), b)
				s.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("metadata"))
				before := f.metaDecodes()
				got, err := f.read(s, rel)
				return got, m.Work(), counts, f.metaDecodes() - before, err
			}
			wantBytes := nativeBytes[0] + nativeBytes[1] + 2*24 + 4*16
			for _, b := range []storecontract.ReadBudget{{}, {BufferBytes: wantBytes}} {
				got, work, copies, decoded, err := read(b)
				if err != nil || work.BufferBytes != wantBytes || copies.parts.Load() != 2 || copies.rows.Load() != 4 || decoded != 4 {
					t.Fatalf("control %+v: work %+v, %d parts/%d copies/%d meta decodes, %v; want %d bytes", b, work, copies.parts.Load(), copies.rows.Load(), decoded, err, wantBytes)
				}
				f.check(t, got)
			}
			got, work, copies, decoded, err := read(storecontract.ReadBudget{BufferBytes: wantBytes - 1})
			if !errors.Is(err, storecontract.ErrReadBudget) || got != nil || copies.parts.Load() != 2 || copies.rows.Load() != 0 || decoded != 4 {
				t.Fatalf("merge one-under: %v, %+v, %d parts/%d copies/%d decodes, %v", got, work, copies.parts.Load(), copies.rows.Load(), decoded, err)
			}
			got, work, copies, decoded, err = read(storecontract.ReadBudget{BufferBytes: nativeBytes[0] + 23})
			if !errors.Is(err, storecontract.ErrReadBudget) || got != nil || copies.parts.Load() != 0 || copies.rows.Load() != 0 || decoded != 2 {
				t.Fatalf("part one-under: %v, %+v, %d parts/%d copies/%d decodes, %v", got, work, copies.parts.Load(), copies.rows.Load(), decoded, err)
			}
			limit := max(nativeBytes[0], nativeBytes[1]) + 24
			for i, bs := range f.shards {
				s := bs.NewReadSnapshot()
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{BufferBytes: limit})
				s.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("native metadata"))
				rows, err := f.read(s, rel)
				s.Close()
				if err != nil || len(rows) != 2 {
					t.Fatalf("shard %d fits %d alone: %v, %v", i, limit, rows, err)
				}
			}
			got, _, copies, decoded, err = read(storecontract.ReadBudget{BufferBytes: limit})
			if !errors.Is(err, storecontract.ErrReadBudget) || got != nil || copies.parts.Load() != 1 || copies.rows.Load() != 0 || decoded != 2 {
				t.Fatalf("cumulative shards: %v, %d parts/%d copies/%d decodes, %v", got, copies.parts.Load(), copies.rows.Load(), decoded, err)
			}
		})
	}
}

func TestHistoryMetaBudgetSharesAllowanceAcrossKeys(t *testing.T) {
	f := newHistoryMetaBudgetFixture(t, true)
	var limits [2]uint64
	for i, rel := range []bool{false, true} {
		s := f.ts.NewReadSnapshot()
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
		s.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("metadata"))
		got, err := f.read(s, rel)
		s.Close()
		if err != nil {
			t.Fatal(err)
		}
		f.check(t, got)
		limits[i] = m.Work().BufferBytes
	}
	limit := max(limits[0], limits[1])
	for _, rel := range []bool{false, true} {
		s := f.ts.NewReadSnapshot()
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{BufferBytes: limit})
		s.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("metadata"))
		got, err := f.read(s, rel)
		s.Close()
		if err != nil {
			t.Fatal(err)
		}
		f.check(t, got)
	}
	counts := new(historyMetaCopyCounts)
	historyMetaCopies.Store(counts)
	defer historyMetaCopies.Store(nil)
	s := f.ts.NewReadSnapshot()
	defer s.Close()
	_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{BufferBytes: limit})
	s.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("metadata"))
	before := f.metaDecodes()
	got, err := f.read(s, false)
	if err != nil {
		t.Fatal(err)
	}
	f.check(t, got)
	got, err = f.read(s, true)
	if !errors.Is(err, storecontract.ErrReadBudget) || got != nil || f.metaDecodes()-before != 4 || counts.parts.Load() != 2 || counts.rows.Load() != 4 {
		t.Fatalf("cumulative entity keys: %v, %d parts/%d copies/%d meta decodes, %v", got, counts.parts.Load(), counts.rows.Load(), f.metaDecodes()-before, err)
	}
}

func TestHistoryMetaBudgetShardSnapshot(t *testing.T) {
	ts := newTestTieredStore(t)
	const pointerBytes = 8
	want := int64(len(ts.eventShards))
	if want == 0 {
		t.Fatal("fixture needs an event shard")
	}
	for _, b := range []uint64{0, uint64(want * pointerBytes), uint64(want*pointerBytes - 1)} {
		counts := new(historyMetaCopyCounts)
		historyMetaCopies.Store(counts)
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{BufferBytes: b})
		err := ts.forEachHistoryShardKeep(nil, nil, m.Tally("metadata"), nil, func(*BadgerStore) (bool, error) { return false, nil })
		historyMetaCopies.Store(nil)
		if b != 0 && b < uint64(want*pointerBytes) {
			if !errors.Is(err, storecontract.ErrReadBudget) || counts.shards.Load() != 0 {
				t.Fatalf("refused shard snapshot: %d copies, %v", counts.shards.Load(), err)
			}
		} else if err != nil || counts.shards.Load() != want || m.Work().BufferBytes != uint64(want*pointerBytes) {
			t.Fatalf("shard snapshot: %d copies, %+v, %v", counts.shards.Load(), m.Work(), err)
		}
	}
}
