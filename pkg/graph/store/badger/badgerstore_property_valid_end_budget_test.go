package badger

import (
	"errors"
	"slices"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A trusted valid-end definition must use the same admitted native key walk
// as the ordinary equality query, including pending entries and bound values.
func TestPropertyValidEndNativeAdmission(t *testing.T) {
	for _, flushed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "flushed"}[flushed], func(t *testing.T) {
			names := registrypkg.NewLabelRegistry()
			tok, _ := names.GetOrCreate("D")
			bs, err := New(Config{InMemory: true, LabelNames: names, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1,
				PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), PropertyIndexOnDisk: true,
				PropertyIndexValidEnd: []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(bs.CreatePropertyIndex(tok, "ov_k"))
			var want []types.NodeID
			for ki, key := range []string{"a", "b", "unrelated"} {
				count := 6
				if ki == 2 {
					count = 64
				}
				for i := range count {
					id := types.NodeID(100 + ki*100 + i)
					n := types.NewNode(id, tok, nil)
					must(n.SetProperty("ov_k", key))
					must(bs.PutNode(n))
					if ki < 2 {
						want = append(want, id)
					}
				}
			}
			keys := []string{types.IndexablePropertyValueKey("a"), types.IndexablePropertyValueKey("b")}
			opts := storecontract.QueryOpts{ValidEndAfter: 50, ValidEndAfterSet: true}
			rows, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(tok, "ov_k", keys, opts)
			must(err)
			if len(rows) != 12 || len(probes) != 12 {
				t.Fatalf("plain control: rows %d, probes %d; want twelve", len(rows), len(probes))
			}
			for i, n := range rows {
				bs.RefineNodePropertyValidEnd(n, probes[i], 100)
			}
			if flushed {
				must(bs.Flush())
			}
			decoded, stopDecoded := CountPropertyValidEndDecodesForTest()
			defer stopDecoded()
			read := func(keys []string, budget storecontract.ReadBudget) ([]types.NodeID, int64, storecontract.ReadWork, error) {
				_, m := storecontract.WithReadBudget(t.Context(), budget)
				pending, stopPending := CountPendingKeysVisitedForTest()
				defer stopPending()
				committed, stopCommitted := CountIndexEntriesVisitedForTest()
				defer stopCommitted()
				nodes, _, err := bs.NodesByLabelAndPropertyKeysValidEnd(tok, "ov_k", keys, opts.WithReadMeter(m, "valid-end"))
				var got []types.NodeID
				for _, n := range nodes {
					got = append(got, n.ID())
				}
				return got, pending() + committed(), m.Work(), err
			}
			for i, key := range keys {
				got, visits, work, err := read([]string{key}, storecontract.ReadBudget{Candidates: 6})
				if err != nil || !slices.Equal(got, want[i*6:(i+1)*6]) || visits != 6 || work.Candidates != 6 {
					t.Errorf("single key %d: ids %v, visits %d, work %+v, err %v; want six native admissions", i, got, visits, work, err)
				}
			}
			beforeDecode := decoded()
			got, visits, work, err := read(keys, storecontract.ReadBudget{})
			if err != nil || !slices.Equal(got, want) || visits != 12 || work.Candidates != 12 || work.DecodedBytes != 12*8 || decoded()-beforeDecode != 12 {
				t.Errorf("control: ids %v, visits %d, work %+v, err %v; want twelve IDs, visits and eight-byte bound decodes", got, visits, work, err)
			}
			got, visits, work, err = read(keys, storecontract.ReadBudget{Candidates: 7})
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "candidates" || len(got) != 0 || visits != 8 || work.Candidates != 8 {
				t.Errorf("cumulative: ids %v, visits %d, work %+v, err %v; want refusal at native visit eight", got, visits, work, err)
			}
			beforeDecode = decoded()
			got, visits, work, err = read(keys, storecontract.ReadBudget{DecodedBytes: 1})
			if !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || len(got) != 0 || visits != 1 || work.DecodedBytes != 8 || decoded() != beforeDecode {
				t.Errorf("bound decode: ids %v, visits %d, work %+v, err %v; want refusal before first eight-byte bound decode", got, visits, work, err)
			}
			got, visits, _, err = read(keys, storecontract.ReadBudget{BufferBytes: 1})
			if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(got) != 0 || visits != 0 {
				t.Errorf("buffer: ids %v, visits %d, err %v; want refusal before prefix allocation", got, visits, err)
			}
		})
	}
}
