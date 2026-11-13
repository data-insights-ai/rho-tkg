package badger

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodesByLabel_ParallelPathCountsBeforeDecode (RH list R2, READBUDGET
// criterion): the parallel decode path of an unbounded label scan counts each
// row's stored bytes in the serial staging pass, before the row is copied and
// handed to a worker. A budget of 1 byte stops the scan at the first row
// (nothing is copied or decoded after it, so no worker runs); unlimited, the
// scan counts every row and answers as without a budget.
func TestNodesByLabel_ParallelPathCountsBeforeDecode(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)
	gen := newTestGen(t, 0)
	const label = uint16(1)
	n := parallelDecodeMinIDs + 50
	for i := 0; i < n; i++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(gen.Generate()), label, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	bs.nodeCache.ResetForTest()

	want, err := bs.NodesByLabel(label, store.QueryOpts{})
	if err != nil || len(want) != n {
		t.Fatalf("no budget = %d rows, %v; want %d", len(want), err, n)
	}

	bs.nodeCache.ResetForTest()
	_, meter := store.WithReadBudget(context.Background(), store.ReadBudget{})
	got, err := bs.NodesByLabel(label, store.QueryOpts{}.WithReadMeter(meter, "scan"))
	total := meter.Work().DecodedBytes
	if err != nil || len(got) != n || total < uint64(n) {
		t.Fatalf("metered scan = %d rows, %v, %d decoded bytes; want %d rows and a count", len(got), err, total, n)
	}

	bs.nodeCache.ResetForTest()
	_, probe := store.WithReadBudget(context.Background(), store.ReadBudget{DecodedBytes: 1})
	rows, err := bs.NodesByLabel(label, store.QueryOpts{}.WithReadMeter(probe, "scan"))
	var be *store.ReadBudgetError
	if !errors.As(err, &be) || be.Limit != "decoded bytes" || be.Door != "scan" || rows != nil {
		t.Fatalf("budget 1 = %d rows, %v; want ErrReadBudget from scan and no rows", len(rows), err)
	}
	if w := probe.Work(); w != be.Work || w.DecodedBytes > 2*(total/uint64(n)) {
		t.Fatalf("budget 1 counted %+v after the refusal (one row is about %d bytes); want one row's bytes", w, total/uint64(n))
	}
}
