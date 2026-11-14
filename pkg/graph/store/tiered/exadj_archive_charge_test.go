package tiered

import (
	"context"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestExAdjacencyMetered_ChargesTheArchiveOpen (R2-SCANS review finding 1):
// with relationships placed by mint, ForEachExAdjacentRelIDMetered first asks
// whether the node lives on the reference side, which reads the archive. An
// archive closed since New must be opened only after its open is charged:
// under ShardOpens 0 the read refuses and the archive stays closed; under an
// open budget the open counts. It opened the archive uncharged before.
func TestExAdjacencyMetered_ChargesTheArchiveOpen(t *testing.T) {
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	dir := t.TempDir()
	ts := openMintStore(t, mintCfg(dir, true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	c := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	putMintRel(t, ts, types.RelID(rg.Generate()), 1, a, b)
	if err := ts.ArchiveNode(c); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts = openMintStore(t, mintCfg(dir, true))
	if !ts.HasArchiveShardForTest() || ts.RefArchiveForTest().Load() != nil {
		t.Fatal("fixture: want a catalogued archive, closed since New")
	}
	_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 1})
	if err := meter.AddShardOpens("prior", 1); err != nil { // the room is used up: the next open refuses
		t.Fatal(err)
	}
	err := ts.ForEachExAdjacentRelIDMetered(b, false, meter.Tally("exadj"), func(types.RelID) bool { return true })
	var rb *storecontract.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "shard opens" || ts.RefArchiveForTest().Load() != nil {
		t.Fatalf("no room left: err %v, work %+v, archive open %t; want the shard-opens refusal with the archive closed", err, meter.Work(), ts.RefArchiveForTest().Load() != nil)
	}
	_, meter = storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 10})
	if err := ts.ForEachExAdjacentRelIDMetered(b, false, meter.Tally("exadj"), func(types.RelID) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if ts.RefArchiveForTest().Load() == nil || meter.Work().ShardOpens < 1 {
		t.Fatalf("open budget: work %+v, archive open %t; want the archive's open counted", meter.Work(), ts.RefArchiveForTest().Load() != nil)
	}
}
