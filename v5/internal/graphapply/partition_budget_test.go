package graphapply

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestPartitionAggregateOutputRefusalPrecedes128OperationInputClones(t *testing.T) {
	r := partitionCodecRequest(t, 0, defaultMaterializerLimits())
	n := newPartitionWriteNetworkWithRouting(t, r, partitionWriteSchemas())
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 16)
	h := n.hosts[0][0]
	initial, claims := partitionNineOperations(t, 1)
	for i := range claims {
		claims[i].grant = grantReference{session: handle.session, sequence: 1}
	}
	p, err := h.PreparePartitionGraphOperations(n.requestID(), initial, 23, claims)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, p, reasonNone)
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	// 128 valid typed operations are admitted by the existing operation policy.
	// Their portable fixed input slots alone require81920B, beyond this legitimate
	// 8192B common output allowance. The standalone opener demonstrably fits;
	// aggregate opening may itself refuse earlier, before input cloning.
	ops := make([]graphstate.Operation, 128)
	for i := range ops {
		ops[i] = graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "old", Scope: scope}
	}
	gl := h.machine.limits.graph
	gl.MaxOutputBytes = 8192
	budget := graphstore.OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 8192}
	revision, err := state.NewRevision(2, 24)
	if err != nil {
		t.Fatal(err)
	}
	var openingWork, failedWork graphstore.PageWork
	opening := testing.AllocsPerRun(3, func() {
		v, work, e := graphstore.OpenPartitionGraphReadView(t.Context(), view, n.d, graphGenesisBinding(t), proof.observation.configuration, h.machine.limits.catalog, gl, budget)
		if e != nil {
			t.Fatal("bounded opening must fit", work, e)
		}
		openingWork = work
		if e := v.Close(); e != nil {
			t.Fatal(e)
		}
	})
	stage := testing.AllocsPerRun(3, func() {
		effects, work, e := graphstore.StagePartitionOperations(t.Context(), view, n.d, graphGenesisBinding(t), proof.observation.configuration, ops, revision, h.machine.limits.catalog, gl, budget)
		if !errors.Is(e, graphstore.ErrResourceLimit) || len(effects.Writes)+len(effects.Groups)+len(effects.Delta.Patches) != 0 {
			t.Fatal("wrong atomic output refusal", effects, work, e)
		}
		failedWork = work
	})
	if stage > opening+16 || failedWork.Records > openingWork.Records || failedWork.Bytes > openingWork.Bytes+1024 {
		t.Fatalf("aggregate refusal allocated owned input before admission: open %.0fallocs work%+v; stage %.0fallocs work%+v", opening, openingWork, stage, failedWork)
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("borrow poisoned by output admission", err)
	}
}
