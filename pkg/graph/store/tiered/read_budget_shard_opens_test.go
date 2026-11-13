package tiered

import (
	"context"
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_SnapshotChargesClosedArchiveOpen (READBUDGET step 6): after a
// reopen the archive is closed until a read needs it; a metered read
// snapshot routing a node there charges the open before it, so a spent
// ShardOpens budget refuses with the archive still closed, the meter reports
// the refusal (Exceeded), and with room the open counts once.
func TestReadBudget_SnapshotChargesClosedArchiveOpen(t *testing.T) {
	t.Parallel()
	cfg := volumeCfg(t.TempDir(), "", "")
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Case")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
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
	if ts.RefArchiveForTest().Load() != nil || !ts.HasArchiveShardForTest() {
		t.Fatal("fixture: the archive is open after the reopen")
	}

	read := func(meter *storecontract.ReadMeter) (*types.Node, error) {
		snap := ts.NewReadSnapshot()
		defer snap.Close()
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(meter.Tally("Door"))
		return snap.GetNode(n.ID())
	}

	ctx, spent := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 1})
	if err := storecontract.ReadMeterFrom(ctx).AddShardOpens("earlier", 1); err != nil {
		t.Fatal(err)
	}
	got, err := read(spent)
	var be *storecontract.ReadBudgetError
	if got != nil || !errors.As(err, &be) || be.Limit != "shard opens" || be.Door != "Door" {
		t.Fatalf("spent budget = %v, %v; want a shard-opens refusal", got, err)
	}
	if ts.RefArchiveForTest().Load() != nil {
		t.Fatal("a refused read opened the archive")
	}
	if !errors.Is(errors.Join(spent.Exceeded(), err), storecontract.ErrReadBudget) || spent.Exceeded() == nil {
		t.Fatalf("Exceeded = %v after the refusal", spent.Exceeded())
	}

	_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 1})
	for range 2 { // the second read finds the archive open: no charge
		if got, err := read(meter); err != nil || got == nil || got.ID() != n.ID() {
			t.Fatalf("read = %v, %v", got, err)
		}
	}
	if w := meter.Work(); w.ShardOpens != 1 || meter.Exceeded() != nil {
		t.Fatalf("work %+v, Exceeded %v; want 1 open, no refusal", w, meter.Exceeded())
	}
}
