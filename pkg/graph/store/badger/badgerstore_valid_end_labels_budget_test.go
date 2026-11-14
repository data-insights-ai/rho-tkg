package badger

import (
	"context"
	"slices"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestValidEndFirstCall_TwoLabelsChargeEachLabelOnce (R2 late counts, review of
// lane C, finding 2): with LabelIndexOnDisk a label's members are listed from
// the keyspace, and the first call's attribution pass (which tells each entity
// read which tracked labels it carries) took the charging members closure too, so
// a second label's first call charged the first label's members again, and its
// own once more. The track listing is the only charge of a label's walk. Label 1
// has 20 nodes and label 2 has 3; the first call of label 2 counts its listing (3
// on disk, none with the label index in RAM, where the walk costs nothing), one
// candidate per entity it reads (3) and, on disk, one probe per entity and
// tracked label (R2-2 VEATTRIB: 3 alone, 6 after label 1), never label 1's 20
// members; the native counter (rows the bulk read decoded) is 3 either way.
// Buffered writes and flushed ones.
func TestValidEndFirstCall_TwoLabelsChargeEachLabelOnce(t *testing.T) {
	t.Parallel()
	const a, b = 20, 3
	for _, disk := range []bool{true, false} {
		for _, flushed := range []bool{true, false} {
			name := map[bool]string{true: "disk", false: "ram"}[disk] + "/" + map[bool]string{true: "flushed", false: "pending"}[flushed]
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				open := func() *Store {
					bs, err := New(Config{Dir: t.TempDir(), LabelIndexOnDisk: disk, FlushInterval: time.Hour})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = bs.Close() })
					for i := int64(1); i <= a; i++ {
						if err := bs.PutNode(veNode(i, 0, 10, 0, 0)); err != nil { // label 1
							t.Fatal(err)
						}
					}
					for i := int64(a + 1); i <= a+b; i++ {
						if err := bs.PutNode(types.NewNode(types.NodeID(i), 2, nil)); err != nil {
							t.Fatal(err)
						}
					}
					if flushed {
						if err := bs.Flush(); err != nil {
							t.Fatal(err)
						}
					}
					return bs
				}
				call := func(bs *Store, label uint16, budget storecontract.ReadBudget) (*storecontract.ReadMeter, []int64, error) {
					_, m := storecontract.WithReadBudget(context.Background(), budget)
					cs, err := bs.NodeValidEndCandidatesMetered([]uint16{label}, 1_000, 0, m.Tally("probe"))
					var ids []int64
					for _, c := range cs {
						ids = append(ids, c.ID)
					}
					slices.Sort(ids)
					return m, ids, err
				}

				// Label 2 alone: its own listing and entities.
				alone := open()
				m, want, err := call(alone, 2, storecontract.ReadBudget{})
				if err != nil || len(want) != b {
					t.Fatalf("label 2 alone = %d candidates, %v; want %d", len(want), err, b)
				}
				listing, probes := uint64(0), func(uint64) uint64 { return 0 }
				if disk {
					listing = b // the keyspace walk; the RAM index lists nothing
					// R2-2 VEATTRIB: the attribution probes each entity's key of each
					// tracked label, one candidate a probe; the RAM maps answer free.
					probes = func(tracked uint64) uint64 { return b * tracked }
				}
				if got := m.Work().Candidates; got != listing+probes(1)+b {
					t.Fatalf("label 2 alone counted %d candidates, want %d (%d listed, %d probes, %d entities)", got, listing+probes(1)+b, listing, probes(1), b)
				}

				// Label 1 tracked first: label 2's count must not move.
				both := open()
				if _, got, err := call(both, 1, storecontract.ReadBudget{}); err != nil || len(got) != a {
					t.Fatalf("label 1 = %d candidates, %v; want %d", len(got), err, a)
				}
				decodes := both.ValidEndRowDecodesForTest()
				m, got, err := call(both, 2, storecontract.ReadBudget{})
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("label 2 after label 1 = %v, %v; want %v", got, err, want)
				}
				if counted, read := m.Work().Candidates, both.ValidEndRowDecodesForTest()-decodes; counted != listing+probes(2)+b || (flushed && read != b) {
					t.Fatalf("label 2 after label 1 counted %d candidates, %d rows decoded; want %d (label 1's %d members not charged again, a probe per entity and tracked label) and, flushed, %d decoded", counted, read, listing+probes(2)+b, a, b)
				}

				// The room of its own count answers; one under it refuses.
				bounded := open()
				if _, _, err := call(bounded, 1, storecontract.ReadBudget{}); err != nil {
					t.Fatal(err)
				}
				if _, got, err := call(bounded, 2, storecontract.ReadBudget{Candidates: listing + probes(2) + b}); err != nil || !slices.Equal(got, want) {
					t.Fatalf("label 2 under its own count %d = %v, %v; want %v", listing+probes(2)+b, got, err, want)
				}
				one := open()
				if _, _, err := call(one, 1, storecontract.ReadBudget{}); err != nil {
					t.Fatal(err)
				}
				if _, got, err := call(one, 2, storecontract.ReadBudget{Candidates: listing + probes(2) + b - 1}); err == nil || len(got) != 0 {
					t.Fatalf("label 2 one under its count = %v, %v; want ErrReadBudget and no candidates", got, err)
				}
			})
		}
	}
}
