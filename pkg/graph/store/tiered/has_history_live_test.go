package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// HasNodeHistory reads the current row to decide liveness, so a corrupt row fails
// as GetNodeHistory does. Deciding liveness from the ID index alone would save
// that read but answer false with no error: no history row, nothing else read.
func TestHasNodeHistory_CorruptCurrentRow(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Case")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	corruptTieredNodeRowAfterFlush(t, ts.RefShardForTest(), n.ID())
	if has, err := ts.HasNodeHistory(n.ID()); err == nil {
		t.Fatalf("corrupt current row answered %v without error", has)
	}
}
