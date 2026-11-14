package tiered

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type snapshotAsOfWalk interface {
	NodeAsOfWalk(types.NodeID, types.Instant) (*types.Node, bool, error)
	RelAsOfWalk(types.RelID, types.Instant) (*types.Relationship, bool, error)
}

// Two live owners exercise one cumulative allowance, including rejected
// reference probes on an event read. The native owner hook is independent of
// ReadMeter: a refusal must stop before another owner's current-row read.
func TestTieredSnapshotAsOfUsesAdmittedOwner(t *testing.T) {
	ts := newTestTieredStore(t)
	caseTok, _, eventTok := installDefaultTestLabelRegistry(t, ts)
	var nodes []*types.Node
	var rels []*types.Relationship
	for _, tok := range []uint16{caseTok, eventTok} {
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 1})
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
		r.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 1})
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		nodes, rels = append(nodes, n), append(rels, r)
	}
	calls := 0
	for _, bs := range []*BadgerStore{ts.refShard, ts.hotShard.store} {
		bs.SetAsOfAfterCurrentTestHookForTest(func() { calls++ })
		defer bs.SetAsOfAfterCurrentTestHookForTest(nil)
	}
	snap := ts.NewReadSnapshot()
	defer snap.Close()
	walk, ok := snap.(snapshotAsOfWalk)
	if !ok {
		t.Fatal("snapshot has no admitted owner as-of walk")
	}
	for _, rel := range []bool{false, true} {
		read := func(i int) error {
			if rel {
				r, handled, err := walk.RelAsOfWalk(rels[i].ID(), 20)
				if err == nil && (!handled || r == nil || r.ID() != rels[i].ID() || r.Version() != 0 || r.Temporal().TxFrom != 10) {
					t.Fatalf("owner %d: rel %v, handled %v", i, r, handled)
				}
				return err
			}
			n, handled, err := walk.NodeAsOfWalk(nodes[i].ID(), 20)
			if err == nil && (!handled || n == nil || n.ID() != nodes[i].ID() || n.Version() != 0 || n.Temporal().TxFrom != 10) {
				t.Fatalf("owner %d: node %v, handled %v", i, n, handled)
			}
			return err
		}
		var perOwner [2]uint64
		for i := range 2 {
			_, full := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(full.Tally("as-of"))
			calls = 0
			if err := read(i); err != nil || calls != 1 {
				t.Fatalf("control rel=%v owner=%d: %d native reads, err %v", rel, i, calls, err)
			}
			perOwner[i] = full.Work().Candidates
		}
		want := [2]uint64{5, 5} // route, initial live, owner current, proof, final live
		if rel {
			want = [2]uint64{4, 5} // ref hit; event also rejects the reference probe
		}
		if perOwner != want {
			t.Fatalf("rel=%v native probe costs %v, want %v", rel, perOwner, want)
		}
		limit := max(perOwner[0], perOwner[1])
		for i := range 2 {
			_, one := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: limit})
			snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(one.Tally("as-of"))
			if err := read(i); err != nil {
				t.Fatalf("each owner alone fits %d: %v", limit, err)
			}
		}
		_, limited := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: limit})
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(limited.Tally("as-of"))
		calls = 0
		if err := read(0); err != nil {
			t.Fatal(err)
		}
		if err := read(1); !errors.Is(err, storecontract.ErrReadBudget) || calls != 1 || limited.Work().Candidates != limit+1 {
			t.Fatalf("two owners rel=%v: %d native reads, work %+v, err %v; want refusal before second owner read", rel, calls, limited.Work(), err)
		}
	}
}

func TestTieredSnapshotAsOfChargesArchiveEligibilityOpen(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			cfg := volumeCfg(t.TempDir(), "", "")
			ts, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			caseTok, _, _ := installDefaultTestLabelRegistry(t, ts)
			var ids []types.NodeID
			for range 2 {
				n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
				n.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 1})
				if err := ts.PutNode(n); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, ids[0], ids[0])
			r.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 1})
			if err := ts.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
			if err := ts.ArchiveNode(ids[1]); err != nil {
				t.Fatal(err)
			}
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}
			ts, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ts.Close() }()
			if !ts.HasArchiveShardForTest() || ts.refArchive.Load() != nil {
				t.Fatal("fixture: archive must be catalogued and closed")
			}
			calls := 0
			ts.refShard.SetAsOfAfterCurrentTestHookForTest(func() { calls++ })
			defer ts.refShard.SetAsOfAfterCurrentTestHookForTest(nil)
			read := func(m *storecontract.ReadMeter) error {
				snap := ts.NewReadSnapshot()
				defer snap.Close()
				snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("as-of"))
				walk := snap.(snapshotAsOfWalk)
				if rel {
					got, ok, err := walk.RelAsOfWalk(r.ID(), 20)
					if err == nil && (!ok || got == nil || got.ID() != r.ID()) {
						t.Fatalf("rel control: %v, %v", got, ok)
					}
					return err
				}
				got, ok, err := walk.NodeAsOfWalk(ids[0], 20)
				if err == nil && (!ok || got == nil || got.ID() != ids[0]) {
					t.Fatalf("node control: %v, %v", got, ok)
				}
				return err
			}
			_, spent := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{ShardOpens: 1})
			if err := spent.AddShardOpens("earlier", 1); err != nil {
				t.Fatal(err)
			}
			if err := read(spent); !errors.Is(err, storecontract.ErrReadBudget) || ts.refArchive.Load() != nil || calls != 0 {
				t.Fatalf("spent opens: err %v, archive open %v, native owner calls %d", err, ts.refArchive.Load() != nil, calls)
			}
			_, full := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			if err := read(full); err != nil || calls != 1 || ts.refArchive.Load() == nil || full.Work().ShardOpens != 2 {
				t.Fatalf("control: err %v, native owner calls %d, work %+v", err, calls, full.Work())
			}
		})
	}
}

func TestTieredSnapshotAsOfRefusalSkipsPostWalk(t *testing.T) {
	ts := newTestTieredStore(t)
	_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 1})
	tally := meter.Tally("as-of")
	if err := tally.Candidates(1); err != nil {
		t.Fatal(err)
	}
	post := 0
	n, handled, err := asOfOwnerWalk(ts, ts.hotShard.store, tally,
		func(storecontract.ReadSnapshot) (bool, error) { t.Fatal("unexpected archive probe"); return false, nil },
		func() (*types.Node, error) { return nil, tally.Candidates(1) },
		func() (bool, error) { post++; return true, nil })
	if !errors.Is(err, storecontract.ErrReadBudget) || n != nil || !handled || post != 0 {
		t.Fatalf("owner refusal: row %v, handled %v, err %v, post probes %d", n, handled, err, post)
	}
}

func TestTieredSnapshotAsOfAdmitsCrossShardRouting(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, eventTok := installDefaultTestLabelRegistry(t, ts)
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), eventTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	owner := ts.hotShard.store
	rotate(t, ts)
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 1})
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if !owner.HasRelID(r.ID().SnowflakeID()) || ts.hotShard.store.HasRelID(r.ID().SnowflakeID()) {
		t.Fatal("fixture: current mint must differ from the start-node owner")
	}
	calls := 0
	owner.SetAsOfAfterCurrentTestHookForTest(func() { calls++ })
	defer owner.SetAsOfAfterCurrentTestHookForTest(nil)
	read := func(budget storecontract.ReadBudget) (bool, *storecontract.ReadMeter, error) {
		_, m := storecontract.WithReadBudget(t.Context(), budget)
		snap := ts.NewReadSnapshot()
		defer snap.Close()
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("as-of"))
		got, ok, err := snap.(snapshotAsOfWalk).RelAsOfWalk(r.ID(), 20)
		if err == nil && (!ok || got == nil || got.ID() != r.ID() || got.Temporal().TxFrom != 10) {
			t.Fatalf("nonempty cross-shard control: row %v, handled %v", got, ok)
		}
		return ok, m, err
	}
	if _, m, err := read(storecontract.ReadBudget{}); err != nil || calls != 1 || m.Work().Candidates != 8 || m.Work().BufferBytes != 24 {
		t.Fatalf("cross-shard control: owner reads %d, work %+v, err %v; want ref/mint misses, two shard entries, owner/current/proof/post probes, vector16 + copy8", calls, m.Work(), err)
	}
	for _, budget := range []storecontract.ReadBudget{{BufferBytes: 15}, {Candidates: 3}} {
		calls = 0
		if ok, m, err := read(budget); !errors.Is(err, storecontract.ErrReadBudget) || ok || calls != 0 {
			t.Fatalf("routing refusal: handled %v, owner reads %d, work %+v, err %v", ok, calls, m.Work(), err)
		}
	}
}
