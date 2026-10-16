package graphapply

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Before the setup fix this refuses at homeCount > MaxBlockSize. Passing this
// guard permits setup only; it proves no million-fact runtime or RSS bound.
func TestNativeGraphCostMillionFactProfileUsesBoundedGrantBlocks(t *testing.T) {
	t.Setenv("RHO_GRAPH_COST_BLOCK_SIZE", "")
	s := nativeGraphCostSpec{Version: 1, Facts: 1_000_000, Writers: 1}
	var init declaredInitCommand
	l := raftlog.DefaultLimits()
	p := raftlog.DefaultApplicationPolicy(1)
	g := raftlog.ApplicationGenerationLimits{}
	if err := nativeGraphCostProfile(&s, &init, &l, &p, &g); err != nil {
		t.Fatal("multiple grants must fund the declared ID interval", err)
	}
	if init.maxBlock != idalloc.MaxBlockSize || l.MaxRetainedEntries != 1_000_000 || p.RetainedApplicationRecords != 65536+s.Facts*512 {
		t.Fatal("block reservation changed unrelated retained quotas", init.maxBlock, l, p)
	}
	first, count, err := nativeGraphCostGrantPlan(s, init.maxBlock, 3, 2)
	if err != nil || first != 1_048_577 || count != 951_430 {
		t.Fatal("exact second HOME range", first, count, err)
	}
	first, count, err = nativeGraphCostGrantPlan(s, init.maxBlock, 8, 1)
	if err != nil || first != 2_000_007 || count != 2 {
		t.Fatal("remote range overlaps HOME", first, count, err)
	}
	for _, raw := range []string{"bad", "0", "1", "1048577", "100"} {
		t.Setenv("RHO_GRAPH_COST_BLOCK_SIZE", raw)
		if _, err := nativeGraphCostBlockSize(s); !errors.Is(err, errInvalid) && !errors.Is(err, errLimit) {
			t.Fatal("invalid/too-small block accepted", raw, err)
		}
	}
	if _, _, err := nativeGraphCostGrantPlan(s, idalloc.MaxBlockSize, 8, 2); !errors.Is(err, errInvalid) {
		t.Fatal("remote extra grant accepted", err)
	}
}

// The same production Reserve/read/install and original graph/history/CDC
// runner crosses the HOME grant boundary, then admits the remote endpoint.
func TestNativeGraphCostMultipleGrantsSmallActualProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, cell := range []struct {
		name  string
		cross uint64
	}{{"local", 0}, {"cross", 100}} {
		t.Run(cell.name, func(t *testing.T) {
			runNativeGraphCostWithBlock(t, nativeGraphCostSpec{Version: 1, Facts: 17, Writers: 1, CrossPercent: cell.cross}, 32)
		})
	}
}
