package graphapply

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

// Diagnostic only: measure the accepted materializer's real same-view prelude.
// It never stages graph operations on GR3 or promotes its local catalog to Full.
func TestPartitionStageCheckedGenesisPreludeReportsActualConsumedWork(t *testing.T) {
	n := newGraphGenesisNetwork(t)
	home := n.hosts[0][0]
	binding := graphGenesisBinding(t)
	first, err := home.PrepareGraphInitialization(3, bootstrapAttemptID{1}, nil, 16, binding, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, first)
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	second, err := home.PrepareGraphInitialization(8, bootstrapAttemptID{2}, nil, 16, binding, proof)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(1, second)
	for group := range 2 {
		h := n.hosts[group][0]
		m := h.machine
		view, err := m.store.ApplicationView(h.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		base, err := view.RootBounded(t.Context(), 172)
		if err != nil {
			t.Fatal(err)
		}
		limits := m.limits.allocation
		limits.readRows = min(limits.readRows, m.limits.sourceRows)
		limits.readBytes = min(limits.readBytes, m.limits.sourceBytes)
		q := reader{ctx: t.Context(), view: view, ns: m.ns, base: base, limits: limits, bytes: cap(base.Image)}
		info, err := m.readOwnership(t.Context(), view, &q)
		if err != nil {
			t.Fatal(err)
		}
		ownership := graphstore.PageWork{Records: q.rows, Bytes: q.bytes}
		cfg, err := m.initialized(&q)
		if err != nil || cfg.defaultAxis != binding || info.Root().SemanticEpoch() == 0 {
			t.Fatal(cfg, err)
		}
		t.Logf("partition=%d source-rows=%d source-bytes=%d after-ownership=%+v after-initialized=%+v; catalog rows/bytes=%d/%d; default=%+v", m.ns.partition, q.limits.readRows, q.limits.readBytes, ownership, graphstore.PageWork{Records: q.rows, Bytes: q.bytes}, m.limits.catalog.MaxReadRows, m.limits.catalog.MaxReadBytes, cfg.defaultAxis.Axis().Descriptor())
		if err := errors.Join(err, view.Close()); err != nil {
			t.Fatal(err)
		}
	}
}
