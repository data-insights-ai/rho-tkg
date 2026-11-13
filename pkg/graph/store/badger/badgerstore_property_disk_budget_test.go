package badger

import (
	"context"
	"errors"
	"slices"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyDiskLookupAdmitsCandidatesAsProduced (R2, gap A): the equality
// lookup of the on-disk property index admits each candidate under the read
// budget as the walk produces it, before it collects, sorts or compacts the
// rest of the key. A Candidates budget of 1 over 64 matches refuses at the
// second entry (the typed error, no rows) with the walk's own visit count at
// the budget plus the one that passed it, whether the entries are on disk or in
// the write buffer; a budget as wide as the key answers the unbudgeted rows and
// counts each match once; one less refuses. The visit count is the store's
// property-index hook, never time.
func TestPropertyDiskLookupAdmitsCandidatesAsProduced(t *testing.T) {
	const matches = 64
	for _, flushed := range []bool{false, true} {
		name := map[bool]string{false: "pending", true: "flushed"}[flushed]
		t.Run(name, func(t *testing.T) {
			bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			if err = bs.CreatePropertyIndex(1, "k"); err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= matches; i++ {
				n := types.NewNode(types.NodeID(existMapBase+int64(i)), 1, []uint16{1})
				_ = n.SetProperty("k", "v")
				if err = bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			if flushed {
				if err = bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			visits := 0
			propertyDiskVisitTestHook = func() { visits++ }
			defer func() { propertyDiskVisitTestHook = nil }()
			read := func(candidates uint64) ([]*types.Node, *storecontract.ReadMeter, error) {
				_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: candidates})
				rows, err := bs.NodesByLabelAndProperty(1, "k", "v", storecontract.QueryOpts{}.WithReadMeter(m, "lookup"))
				return rows, m, err
			}
			want, err := bs.NodesByLabelAndProperty(1, "k", "v", storecontract.QueryOpts{})
			if err != nil || len(want) != matches {
				t.Fatalf("no budget = %d rows, %v; want %d", len(want), err, matches)
			}

			visits = 0
			rows, meter, err := read(1)
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "candidates" || rb.Door != "lookup" || rows != nil || rb.Work != meter.Work() || meter.Work().Candidates != 2 {
				t.Fatalf("budget 1 = %d rows, %v (meter %+v); want ErrReadBudget at candidates after 2 counted, no rows", len(rows), err, meter.Work())
			}
			if flushed && visits > 3 {
				t.Fatalf("budget 1 visited %d index entries of %d: the walk collected the key before it admitted one", visits, matches)
			}

			rows, meter, err = read(matches)
			if err != nil || meter.Work().Candidates != matches {
				t.Fatalf("budget %d = %d rows, %v, counted %d; want every match counted once", matches, len(rows), err, meter.Work().Candidates)
			}
			if !slices.EqualFunc(rows, want, func(a, b *types.Node) bool { return a.ID() == b.ID() }) {
				t.Fatalf("budget %d answered other rows than no budget", matches)
			}
			if rows, _, err = read(matches - 1); !errors.Is(err, storecontract.ErrReadBudget) || rows != nil {
				t.Fatalf("budget %d = %d rows, %v; want ErrReadBudget", matches-1, len(rows), err)
			}
		})
	}
}
