package graphapply

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Authored guards only until the implementation-first source checkpoint is
// reviewed. The actual workload parent supplies the real frozen/native traces.
func TestNativeGraphEdgeUniqueProfileAndFreshColumnAllowance(t *testing.T) {
	if _, err := nativeGraphEdgeUniqueColumnPolicy(nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, _, err := nativeGraphEdgeUniqueGrantPlan(nativeGraphCostSpec{}, 2, 3, 1); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, facts := range []uint64{10_000, 100_000, 1_000_000} {
		for _, writers := range []int{1, 8, 32} {
			spec := nativeGraphCostSpec{Version: 4, Facts: facts, CrossPercent: 100, Writers: writers}
			l := raftlog.DefaultLimits()
			p := raftlog.DefaultApplicationPolicy(1)
			p.MaxImageBytes, p.MaxInstallWrites = 172, 256
			g := raftlog.ApplicationGenerationLimits{}
			init := declaredInitCommand{}
			if err := nativeGraphEdgeUniqueProfile(spec, &init, &l, &p, &g); err != nil || l.Validate() != nil || p.Validate(l) != nil {
				t.Fatal(facts, writers, err)
			}
			block, err := nativeGraphCostBlockSize(spec)
			if err != nil {
				t.Fatal(err)
			}
			first, count, err := nativeGraphEdgeUniqueGrantPlan(spec, block, 3, (spec.homeCount()+block-1)/block)
			if err != nil || first+count-1 != 5*facts+7 {
				t.Fatal(first, count, err)
			}
			first, count, err = nativeGraphEdgeUniqueGrantPlan(spec, block, 8, 1)
			if err != nil || first != 5*facts+8 || count != 3 {
				t.Fatal(first, count, err)
			}
		}
	}
	legacy := nativeColumnGuardPolicy()
	before, err := json.Marshal(legacy)
	if err != nil || bytes.Contains(before, []byte("Version")) || bytes.Contains(before, []byte("Admission")) {
		t.Fatal("zero profile changed encoded bytes", err)
	}
	fresh := legacy
	fresh.Version = 1
	fresh.ChunkRecords = 16
	fresh.ChunkRows = 16
	application := raftlog.DefaultApplicationPolicy(1)
	limits := defaultMaterializerLimits()
	_, oldGrant, err := nativeColumnLimitAllowance(application, limits, legacy)
	if err != nil {
		t.Fatal(err)
	}
	_, freshGrant, err := nativeColumnLimitAllowance(application, limits, fresh)
	if err != nil {
		t.Fatal(err)
	}
	legacy.Version = 1
	_, oneGrant, err := nativeColumnLimitAllowance(application, limits, legacy)
	if err != nil || oneGrant.Bytes > oldGrant.Bytes || freshGrant.Bytes == 0 {
		t.Fatal("fresh aggregate-row allowance", err)
	}
	defaults := graphstore.DefaultLimits()
	catalogCalls := uint64(3*(fresh.ChunkRecords+1) + 10*fresh.ChunkRows)
	if freshGrant.Records < catalogCalls*uint64(max(limits.catalog.MaxReadRows, defaults.MaxReadRows)) {
		t.Fatal("lookahead/over-row native catalog calls unfunded")
	}
	fresh.Admission = [32]byte{1}
	if fresh.validate() != nil {
		t.Fatal("fresh authenticated workload policy refused")
	}
	legacy.Version = 0
	legacy.Admission = fresh.Admission
	if !errors.Is(legacy.validate(), errInvalid) {
		t.Fatal("legacy acquired fresh admission bytes")
	}
	fresh.Version = 2
	if !errors.Is(fresh.validate(), errInvalid) {
		t.Fatal("unknown allowance version")
	}
	if nativeGraphEdgeUniquePlanShape(partitionGraphCommand{}) {
		t.Fatal("empty recipe accepted")
	}
	if nativeGraphEdgeUniquePlanShape(partitionGraphCommand{request: graphRequest{operations: []graphstate.Operation{{Kind: graphstate.Set}}}}) {
		t.Fatal("arbitrary operation accepted as fixture recipe")
	}
}
