package tiered

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func boundsOwnerFixture(t *testing.T, ts *Store, token uint16) (*types.Node, *types.Relationship) {
	t.Helper()
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), token, nil)
	n.SetVersion(2)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: 30, ValidFrom: 1})
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
	r.SetVersion(2)
	r.SetTemporal(n.Temporal())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	for v := range uint32(2) {
		tm := &types.TemporalMetadata{TxFrom: types.Instant(10 + v*10), TxTo: types.Instant(20 + v*10), ValidFrom: 1}
		nh, rh := n.DeepCopy(), r.DeepCopy()
		nh.SetVersion(v)
		nh.SetTemporal(tm)
		rh.SetVersion(v)
		rh.SetTemporal(tm)
		if err := ts.PutNodeVersion(n.ID(), v, nh); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutRelVersion(r.ID(), v, rh); err != nil {
			t.Fatal(err)
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	return n, r
}

// A route can be handled even when its native proof is unknown. Conversely,
// a native proof cannot certify history left on another owner or a route that
// changed during the read. Exercise both ends of that boundary with real rows.
func TestTieredSnapshotHistoryBoundsEligibility(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, state := range []string{"pending", "invalid", "split", "split gap", "archive history", "archived", "archive gap"} {
			t.Run(map[bool]string{false: "node/", true: "rel/"}[rel]+state, func(t *testing.T) {
				ts := newTestTieredStore(t)
				ref, _, event := installDefaultTestLabelRegistry(t, ts)
				token := ref
				if state == "split" || state == "split gap" {
					token = event
				}
				n, r := boundsOwnerFixture(t, ts, token)
				snap := ts.NewReadSnapshot()
				defer snap.Close()
				bounds, ok := snap.(storecontract.ReadSnapshotHistoryBoundsCapability)
				if !ok {
					t.Fatal("snapshot has no complete history bounds")
				}
				read := func() (uint32, types.Instant, bool, error) {
					if rel {
						return bounds.RelHistoryBounds(r.ID())
					}
					return bounds.NodeHistoryBounds(n.ID())
				}
				_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("bounds"))
				probes, stop := badgerstore.CountAsOfIndexProbesForTest()
				version, newest, known, err := read()
				p := probes()
				stop()
				if err != nil || !known || version != 1 || newest != 30 || p != 2 || m.Work().HistoryExamined != 0 {
					t.Fatalf("complete owner control: v%d newest %d known %v probes %d work %+v err %v", version, newest, known, p, m.Work(), err)
				}
				split := func() {
					if rel {
						ts.relLives.add(r.ID().SnowflakeID())
					} else {
						ts.nodeLives.add(n.ID().SnowflakeID())
					}
				}
				switch state {
				case "pending", "invalid":
					if rel {
						h, e := ts.GetRelVersion(r.ID(), 1)
						if e != nil {
							t.Fatal(e)
						}
						err = ts.PutRelVersion(r.ID(), 1, h)
					} else {
						h, e := ts.GetNodeVersion(n.ID(), 1)
						if e != nil {
							t.Fatal(e)
						}
						err = ts.PutNodeVersion(n.ID(), 1, h)
					}
					if state == "invalid" && err == nil {
						err = ts.Flush()
					}
				case "split":
					split()
				case "split gap":
					ts.asOfWalkGapHook = split
				case "archive history":
					archive := mustArchiveStore(t, ts)
					if rel {
						h := r.DeepCopy()
						h.SetVersion(3)
						h.SetTemporal(&types.TemporalMetadata{TxFrom: 30, TxTo: 40, ValidFrom: 1})
						err = archive.PutRelVersion(r.ID(), 3, h)
					} else {
						h := n.DeepCopy()
						h.SetVersion(3)
						h.SetTemporal(&types.TemporalMetadata{TxFrom: 30, TxTo: 40, ValidFrom: 1})
						err = archive.PutNodeVersion(n.ID(), 3, h)
					}
				case "archived":
					err = ts.ArchiveNode(n.ID())
				case "archive gap":
					ts.asOfWalkGapHook = func() {
						ts.asOfWalkGapHook = nil
						if e := ts.ArchiveNode(n.ID()); e != nil {
							t.Fatal(e)
						}
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				_, m = storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("bounds"))
				probes, stop = badgerstore.CountAsOfIndexProbesForTest()
				_, _, known, err = read()
				p = probes()
				stop()
				ts.asOfWalkGapHook = nil
				if err != nil || known {
					t.Fatalf("unsafe owner claimed complete history: known %v err %v", known, err)
				}
				wantProbes := int64(0)
				switch state {
				case "invalid":
					wantProbes = 1
				case "split gap", "archive gap":
					wantProbes = 2
				}
				if p != wantProbes {
					t.Fatalf("native proof probes %d, want %d", p, wantProbes)
				}
				before := m.Work()
				var rows []storecontract.VersionTemporalMeta
				if rel {
					rows, err = snap.RelHistoryTemporalMeta(r.ID())
				} else {
					rows, err = snap.NodeHistoryTemporalMeta(n.ID())
				}
				wantRows := 2
				if state == "archive history" {
					wantRows = 3
				}
				if err != nil || len(rows) != wantRows || m.Work().HistoryExamined <= before.HistoryExamined {
					t.Fatalf("same-tally fallback: %d rows, before %+v after %+v err %v", len(rows), before, m.Work(), err)
				}
			})
		}
	}
}

func TestTieredSnapshotHistoryBoundsRefusal(t *testing.T) {
	ts := newTestTieredStore(t)
	ref, _, _ := installDefaultTestLabelRegistry(t, ts)
	n, r := boundsOwnerFixture(t, ts, ref)
	for _, rel := range []bool{false, true} {
		snap := ts.NewReadSnapshot()
		defer snap.Close()
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{BufferBytes: 40})
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("bounds"))
		proof := snap.(storecontract.ReadSnapshotHistoryBoundsCapability)
		probes, stopProbes := badgerstore.CountAsOfIndexProbesForTest()
		copies, stopCopies := badgerstore.CountAsOfIndexBytesCopiedForTest()
		var known bool
		var err error
		if rel {
			_, _, known, err = proof.RelHistoryBounds(r.ID())
		} else {
			_, _, known, err = proof.NodeHistoryBounds(n.ID())
		}
		p, copied := probes(), copies()
		stopProbes()
		stopCopies()
		if !errors.Is(err, storecontract.ErrReadBudget) || known || p != 1 || copied != 0 || m.Work().HistoryExamined != 0 {
			t.Fatalf("proof copy refusal: known %v probes %d copied %d work %+v err %v", known, p, copied, m.Work(), err)
		}
	}
}
