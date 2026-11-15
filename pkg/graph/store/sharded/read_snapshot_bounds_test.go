package sharded

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestShardedSnapshotHistoryBoundsSharesAdmission(t *testing.T) {
	s := newMemStore(t, 0, 2)
	for slot := uint8(0); slot < 2; slot++ {
		for v := range uint32(2) {
			tm := &types.TemporalMetadata{TxFrom: types.Instant(10 + v*10), TxTo: types.Instant(20 + v*10), ValidFrom: 1}
			n := types.NewNode(mkNodeID(slot, 1), 1, nil)
			n.SetVersion(v)
			n.SetTemporal(tm)
			if err := s.PutNodeVersion(n.ID(), v, n); err != nil {
				t.Fatal(err)
			}
			r := types.NewRelationship(mkRelID(slot, 2), 1, n.ID(), n.ID())
			r.SetVersion(v)
			r.SetTemporal(tm)
			if err := s.PutRelVersion(r.ID(), v, r); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			read := func(b storecontract.ReadBudget, slots ...uint8) (storecontract.ReadWork, int64, int64, error) {
				_, m := storecontract.WithReadBudget(t.Context(), b)
				snap := s.NewReadSnapshot()
				defer snap.Close()
				snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.Tally("bounds"))
				bounds, ok := snap.(storecontract.ReadSnapshotHistoryBoundsCapability)
				if !ok {
					t.Fatal("snapshot has no complete history bounds")
				}
				probes, stopProbes := badgerstore.CountAsOfIndexProbesForTest()
				defer stopProbes()
				copies, stopCopies := badgerstore.CountAsOfIndexBytesCopiedForTest()
				defer stopCopies()
				for _, slot := range slots {
					var version uint32
					var newest types.Instant
					var known bool
					var err error
					if rel {
						version, newest, known, err = bounds.RelHistoryBounds(mkRelID(slot, 2))
					} else {
						version, newest, known, err = bounds.NodeHistoryBounds(mkNodeID(slot, 1))
					}
					if err != nil {
						if known {
							t.Fatal("refused proof reported known bounds")
						}
						return m.Work(), probes(), copies(), err
					}
					if !known || version != 1 || newest != 30 {
						t.Fatalf("slot %d: version %d newest %d known %v", slot, version, newest, known)
					}
				}
				return m.Work(), probes(), copies(), nil
			}
			w, probes, copies, err := read(storecontract.ReadBudget{}, 0, 1)
			want := storecontract.ReadWork{Candidates: 4, BufferBytes: 82, DecodedBytes: 82}
			if err != nil || w != want || probes != 4 || copies != 82 {
				t.Fatalf("two owners: work %+v, probes %d copied %d err %v", w, probes, copies, err)
			}
			if got, _, _, err := read(storecontract.ReadBudget{Candidates: 4, HistoryExamined: 2, BufferBytes: 82, DecodedBytes: 82}, 0, 1); err != nil || got != want {
				t.Fatalf("exact cumulative allowance: %+v, %v", got, err)
			}
			for _, b := range []storecontract.ReadBudget{{Candidates: 2}, {BufferBytes: 41}, {DecodedBytes: 41}} {
				for _, slot := range []uint8{0, 1} {
					if _, _, _, err := read(b, slot); err != nil {
						t.Fatalf("owner %d alone must fit %+v: %v", slot, b, err)
					}
				}
				_, probes, copies, err := read(b, 0, 1)
				wantProbes, wantCopies := int64(3), int64(41)
				if b.Candidates != 0 {
					wantProbes = 2
				}
				if b.DecodedBytes != 0 {
					wantCopies = 82
				}
				if !errors.Is(err, storecontract.ErrReadBudget) || probes != wantProbes || copies != wantCopies {
					t.Fatalf("cumulative %+v: probes %d copied %d err %v", b, probes, copies, err)
				}
			}
		})
	}
}
