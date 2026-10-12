package graphstore

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// GUARD under the independently accepted E17 capability RED. Planning carries
// fixed source bindings only; it cannot retain a whole range or confer authority.
func TestRangeMovePlanBindsNativeRootsWithoutRetainingRange(t *testing.T) {
	sources, declaration, binding, cfg, _ := transactionViewFixture(t)
	publication, from, _, err := LookupPublishedRange(t.Context(), sources[0], Namespace{declaration.Graph(), 3}, 1, cfg, transactionTestBudget())
	if err != nil {
		t.Fatal(err)
	}
	to := from
	to.Partition, to.Epoch = 8, from.Epoch+1
	descriptor, _, err := PlanRangeMove(t.Context(), sources[0], sources[1], declaration, binding, cfg, RangeMoveSpec{publication, from, to}, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil || descriptor.Sealed || descriptor.Rows != 0 || descriptor.Bytes != 0 || descriptor.Spec.To != to || descriptor.Source.Binding.Identity.Partition != 3 || descriptor.Target.Binding.Identity.Partition != 8 {
		t.Fatal("planning materialized range or changed captured bindings", descriptor, err)
	}
	state, _, err := BeginRangeMoveImport(t.Context(), sources[1], descriptor, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if !errors.Is(err, ErrInvalid) || state.Sequence != 0 || state.Sealed {
		t.Fatal("unsealed inspection descriptor became an import", state, err)
	}
	var cursor RangeMoveCursor
	page, _, err := ExportRangeMovePage(t.Context(), sources[0], descriptor, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if err != nil || page.Next.Sequence != 1 {
		t.Fatal("bounded actual source export failed", page, err)
	}
}
