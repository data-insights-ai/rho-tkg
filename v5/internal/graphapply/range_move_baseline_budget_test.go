package graphapply

import (
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestRangeMoveCatalogBaselineChargesParentBeforeOpeningRetainedOwner(t *testing.T) {
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 3}
	transports[8] = gate
	c, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, c, intent)
	if !gate.stopped || pending.committed || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
		t.Fatal("requires actual staged target", err)
	}
	h := n.hosts[1][0]
	m := h.machine
	q, cfg := rangeMoveBaselineGuardReader(t, m, h.driver.Applied())
	before := rangeMoveBaselineGuardUsage(t, m.store)
	trial := *q
	trial.arena, err = graphstate.NewOutputBudget(m.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	view, err := m.rangeMoveCatalogBaseline(&trial, cfg)
	if err != nil || view == nil {
		t.Fatal("funded actual baseline", err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	rows, owned := trial.rows-q.rows, trial.arena.Used()
	if rows < 1 || owned < 2048 {
		t.Fatal("missing actual baseline work and ownership admission", rows, owned)
	}
	// The exact funded counterpart establishes the one-short parent boundary.
	// Both refusals must occur before acquiring the separately owned C handle.
	shortRows := *q
	shortRows.limits.readRows = q.rows + rows - 1
	shortRows.arena, _ = graphstate.NewOutputBudget(m.limits.outputBytes)
	if view, err := m.rangeMoveCatalogBaseline(&shortRows, cfg); view != nil || !errors.Is(err, errLimit) {
		if view != nil {
			_ = view.Close()
		}
		t.Fatal("one-short source parent acquired baseline", err)
	}
	shortOutput := *q
	shortOutput.arena, _ = graphstate.NewOutputBudget(owned - 1)
	if view, err := m.rangeMoveCatalogBaseline(&shortOutput, cfg); view != nil || !errors.Is(err, errLimit) || !errors.Is(err, graphstate.ErrResourceLimit) {
		if view != nil {
			_ = view.Close()
		}
		t.Fatal("one-short ownership parent acquired baseline", err)
	}
	if after := rangeMoveBaselineGuardUsage(t, m.store); after != before {
		t.Fatal("failed admission retained baseline ownership", before, after)
	}
	// Native handle exhaustion is a separate resource refusal. Existing borrowed
	// current data stays healthy and every successfully acquired owner is closed.
	var held []*raftlog.ApplicationView
	defer func() {
		for _, v := range held {
			if err := v.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for range m.store.ApplicationLimits().MaxViews + 1 {
		v, e := m.store.ApplicationView(q.base.Index)
		if errors.Is(e, raftlog.ErrLimit) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, v)
	}
	saturated := rangeMoveBaselineGuardUsage(t, m.store)
	quota := *q
	quota.arena, _ = graphstate.NewOutputBudget(m.limits.outputBytes)
	if view, err := m.rangeMoveCatalogBaseline(&quota, cfg); view != nil || !errors.Is(err, raftlog.ErrLimit) {
		if view != nil {
			_ = view.Close()
		}
		t.Fatal("native quota refusal opened/substituted another baseline", err)
	}
	if after := rangeMoveBaselineGuardUsage(t, m.store); after != saturated {
		t.Fatal("native quota refusal leaked another handle", saturated, after)
	}
	if root, err := q.view.RootBounded(t.Context(), 172); err != nil || root.Index != q.base.Index {
		t.Fatal("refusal closed borrowed current owner", err)
	}
}
