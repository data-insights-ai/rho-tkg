package badger

import (
	"context"
	"slices"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestValidEndFirstCall_WalksNoOtherLabel (R2-2 VEATTRIB): with
// LabelIndexOnDisk the valid-end index's first call for a label tells each
// entity it reads which tracked labels it carries, and it did so by listing
// every tracked label's keyspace again, uncounted: a label of 3 nodes read
// after a label of 200 walked the 200 keys (and its own 3 twice). It now asks
// the label index once per entity and tracked label, a point probe charged as
// one candidate before it is made. The native counter is the label index
// entries the store visits (an iterator step or a probe), which the meter does
// not keep: label 2's first call visits its own committed listing (3, none
// while the writes are buffered) and makes one probe per entity and tracked
// label (3 x 2), whatever label 1's size; it counts its listing (3), the
// probes (6) and one candidate per entity read (3). Buffered and flushed writes.
// Not parallel: the visit counter is the process's.
func TestValidEndFirstCall_WalksNoOtherLabel(t *testing.T) {
	const b = 3
	for _, a := range []int64{20, 200} {
		for _, flushed := range []bool{true, false} {
			name := map[bool]string{true: "flushed", false: "pending"}[flushed]
			t.Run(name, func(t *testing.T) {
				bs, err := New(Config{Dir: t.TempDir(), LabelIndexOnDisk: true, FlushInterval: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = bs.Close() }()
				for i := int64(1); i <= a; i++ {
					if err := bs.PutNode(veNode(i, 0, 10, 0, 0)); err != nil { // label 1
						t.Fatal(err)
					}
				}
				for i := a + 1; i <= a+b; i++ {
					if err := bs.PutNode(types.NewNode(types.NodeID(i), 2, nil)); err != nil {
						t.Fatal(err)
					}
				}
				if flushed {
					if err := bs.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				call := func(label uint16) (*storecontract.ReadMeter, []int64, error) {
					_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
					cs, err := bs.NodeValidEndCandidatesMetered([]uint16{label}, 1_000, 0, m.Tally("probe"))
					var ids []int64
					for _, c := range cs {
						ids = append(ids, c.ID)
					}
					slices.Sort(ids)
					return m, ids, err
				}
				if _, got, err := call(1); err != nil || int64(len(got)) != a {
					t.Fatalf("label 1 = %d candidates, %v; want %d", len(got), err, a)
				}

				visited, stop := CountIndexEntriesVisitedForTest()
				defer stop()
				m, got, err := call(2)
				want := []int64{a + 1, a + 2, a + 3}
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("label 2 = %v, %v; want %v", got, err, want)
				}
				// The listing walks label 2's committed keys (none while they are
				// buffered: it reads the write buffer), then one probe per entity
				// and tracked label, answered by the buffer or the index.
				listing := int64(0)
				if flushed {
					listing = b
				}
				if v := visited(); v != listing+b*2 {
					t.Fatalf("label 2's first call visited %d label index entries after a label of %d; want %d (its listing %d and %d probes)", v, a, listing+b*2, listing, b*2)
				}
				if c, want := m.Work().Candidates, uint64(b+b*2+b); c != want {
					t.Fatalf("label 2's first call counted %d candidates; want %d (%d listed, %d probes, %d entities read)", c, want, b, b*2, b)
				}
			})
		}
	}
}
