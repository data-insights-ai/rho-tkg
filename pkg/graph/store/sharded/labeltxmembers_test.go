package sharded

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestShardedTxMembersNativeAdmission(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "nodes", true: "rels"}[rel], func(t *testing.T) {
			s := newMemStore(t, 0, 2)
			want := map[uint16][]int64{}
			for tok := uint16(1); tok <= 2; tok++ {
				for slot := uint8(0); slot < 2; slot++ {
					for i := int64(1); i <= 3; i++ {
						seq := int64(tok)*10 + i
						tm := &types.TemporalMetadata{TxFrom: types.Instant(10 + tok), ValidFrom: 1}
						var err error
						if rel {
							r := types.NewRelationship(mkRelID(slot, seq), tok, mkNodeID(slot, 1), mkNodeID(slot, 2))
							r.SetTemporal(tm)
							err = s.PutRelVersion(r.ID(), 0, r)
							want[tok] = append(want[tok], int64(r.ID()))
						} else {
							n := types.NewNode(mkNodeID(slot, seq), tok, nil)
							n.SetTemporal(tm)
							err = s.PutNodeVersion(n.ID(), 0, n)
							want[tok] = append(want[tok], int64(n.ID()))
						}
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				slices.Sort(want[tok])
			}
			cap, ok := any(s).(storecontract.TxMemberGatherMeteredCapability)
			if !ok {
				t.Fatal("native label/type ever-membership admission unavailable")
			}
			copied, stopCopy := badger.CountTxMembersCopiedForTest()
			defer stopCopy()
			decoded, stopDecode := badger.CountMembershipDecodesForTest()
			defer stopDecode()
			read := func(tokens []uint16, budget storecontract.ReadBudget, stop bool) ([]int64, int64, int64, storecontract.ReadWork, error) {
				_, m := storecontract.WithReadBudget(t.Context(), budget)
				c0, d0 := copied(), decoded()
				var out []int64
				for _, tok := range tokens {
					var err error
					visit := func(id int64, tx types.Instant) bool {
						if tx != types.Instant(10+tok) {
							t.Errorf("ID %d: first tx %d; want %d", id, tx, 10+tok)
						}
						out = append(out, id)
						return !stop
					}
					if rel {
						err = cap.ForEachRelTypeTxMemberMetered(tok, m.Tally("members"), func(id types.RelID, tx types.Instant) bool { return visit(int64(id), tx) })
					} else {
						err = cap.ForEachLabelTxMemberMetered(tok, m.Tally("members"), func(id types.NodeID, tx types.Instant) bool { return visit(int64(id), tx) })
					}
					if err != nil {
						return out, copied() - c0, decoded() - d0, m.Work(), err
					}
				}
				slices.Sort(out)
				return out, copied() - c0, decoded() - d0, m.Work(), nil
			}
			got, copies, decodes, work, err := read([]uint16{1}, storecontract.ReadBudget{DecodedBytes: 1}, false)
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || len(got) != 0 || copies != 0 || decodes != 0 {
				t.Fatalf("cold: ids %v, copies %d, decodes %d, work %+v, err %v; want refusal before a first-build decode", got, copies, decodes, work, err)
			}
			// Build both sidecars using their own admission; the following exact
			// copy controls isolate warm fanout from first-build row work.
			allWant := append(slices.Clone(want[1]), want[2]...)
			got, copies, decodes, _, err = read([]uint16{1, 2}, storecontract.ReadBudget{}, false)
			if err != nil || !slices.Equal(got, allWant) || copies != 12 || decodes != 12 {
				t.Fatalf("cold control: ids %v, copies %d, decodes %d, err %v; want all twelve rows built and copied", got, copies, decodes, err)
			}
			for _, tok := range []uint16{1, 2} {
				got, copies, decodes, work, err = read([]uint16{tok}, storecontract.ReadBudget{Candidates: 6}, false)
				if err != nil || !slices.Equal(got, want[tok]) || copies != 6 || decodes != 0 || work.Candidates != 6 || work.BufferBytes != 6*storecontract.TxMemberCopyBytes {
					t.Errorf("key %d: ids %v, copies %d, decodes %d, work %+v, err %v", tok, got, copies, decodes, work, err)
				}
			}
			got, copies, _, work, err = read([]uint16{1}, storecontract.ReadBudget{Candidates: 3}, false)
			if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 3 || copies != 3 || work.Candidates != 6 {
				t.Errorf("two shards: ids %v, copies %d, work %+v, err %v; want only the first shard's three copies", got, copies, work, err)
			}
			got, copies, _, work, err = read([]uint16{1, 2}, storecontract.ReadBudget{Candidates: 7}, false)
			if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 6 || copies != 6 || work.Candidates != 9 {
				t.Errorf("two keys: ids %v, copies %d, work %+v, err %v; want refusal before the second key's first three-copy snapshot", got, copies, work, err)
			}
			got, copies, _, work, err = read([]uint16{1}, storecontract.ReadBudget{BufferBytes: 1}, false)
			if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(got) != 0 || copies != 0 {
				t.Errorf("buffer: ids %v, copies %d, work %+v, err %v; want refusal before copying", got, copies, work, err)
			}
			got, copies, _, work, err = read([]uint16{1}, storecontract.ReadBudget{}, true)
			if err != nil || len(got) != 1 || copies != 3 || work.Candidates != 3 {
				t.Errorf("stop: ids %v, copies %d, work %+v, err %v; want one callback and no second shard read", got, copies, work, err)
			}
		})
	}
}
