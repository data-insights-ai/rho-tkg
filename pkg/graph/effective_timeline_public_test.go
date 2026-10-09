package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEffectiveTimeline_PublicFacade drives the four effective-timeline doors
// through g.Temporal() on a badger graph on disk, reopened: a relationship and
// a node corrected by a bounded SetVersionInterval and then deleted read as
// three segments ending at the delete instant, the scan forms list them after
// the delete, and the public sentinels match with errors.Is
// (graph.ErrTxPinTooNew, graph.ErrInvalidTimeRange, graph.ErrRelNotFound,
// graph.ErrGraphClosed). Catches a façade that loses the error identity or a
// door that reads the current state instead of the pinned one after reopen.
func TestEffectiveTimeline_PublicFacade(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, BadgerDir: dir})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	r, err := g.Rels().AddByID(ctx, "LINK", a.ID(), b.ID(), map[string]any{"tkg_valid_from": types.Instant(1000), "w": int64(0)})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	if _, err := g.Temporal().SetRelVersionInterval(ctx, r.ID(), 2000, 3000, map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("cascade: %v", err)
	}
	if _, err := g.Temporal().SetNodeVersionInterval(ctx, a.ID(), 2000, 3000, map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("cascade node: %v", err)
	}
	if err := g.Rels().Delete(ctx, r.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	g, err = graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	rs, err := g.Temporal().RelEffectiveTimeline(r.ID(), pin)
	if err != nil || len(rs) != 3 || rs[0].ValidFrom != 1000 || rs[0].ValidTo != 2000 || rs[1].ValidTo != 3000 || rs[2].ValidTo <= 3000 {
		t.Fatalf("RelEffectiveTimeline = %+v, %v; want [1000,2000) [2000,3000) [3000,D)", rs, err)
	}
	if w, _ := rs[1].Rel.Properties().Get("w"); w != int64(1) {
		t.Fatalf("correction segment w = %v", w)
	}
	if !rs[0].Rel.IsFrozen() {
		t.Fatal("segment row is not frozen")
	}
	ns, err := g.Temporal().NodeEffectiveTimeline(a.ID(), pin)
	if err != nil || len(ns) != 3 || ns[2].ValidTo != 0 {
		t.Fatalf("NodeEffectiveTimeline = %+v, %v; want three segments, the last open", ns, err)
	}
	listed := 0
	if err := g.Temporal().ForEachRelEffectiveByType("LINK", pin, func(s temporal.RelSegment) bool {
		if s.Rel.ID() == r.ID() {
			listed++
		}
		return true
	}); err != nil || listed != 3 {
		t.Fatalf("ForEachRelEffectiveByType: %v, %d segments of the deleted rel, want 3", err, listed)
	}
	nodes := map[types.NodeID]int{}
	if err := g.Temporal().ForEachNodeEffectiveByLabel("Host", pin, func(s temporal.NodeSegment) bool {
		nodes[s.Node.ID()]++
		return true
	}); err != nil || len(nodes) != 2 || nodes[a.ID()] != 3 || nodes[b.ID()] != 1 {
		t.Fatalf("ForEachNodeEffectiveByLabel: %v, %v", err, nodes)
	}

	peek, _ := g.Temporal().PeekTx()
	if _, err := g.Temporal().RelEffectiveTimeline(r.ID(), peek+3_600_000); !errors.Is(err, graphpkg.ErrTxPinTooNew) {
		t.Fatalf("future pin: %v, want graph.ErrTxPinTooNew", err)
	}
	if _, err := g.Temporal().NodeEffectiveTimeline(a.ID(), 0); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatalf("pin 0: %v, want graph.ErrInvalidTimeRange", err)
	}
	if _, err := g.Temporal().RelEffectiveTimeline(r.ID()+1<<30, pin); !errors.Is(err, graphpkg.ErrRelNotFound) {
		t.Fatalf("unknown rel: %v, want graph.ErrRelNotFound", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := g.Temporal().NodeEffectiveTimeline(a.ID(), pin); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed: %v, want graph.ErrGraphClosed", err)
	}
	if err := g.Temporal().ForEachRelEffectiveByType("LINK", pin, func(temporal.RelSegment) bool { return true }); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed scan: %v, want graph.ErrGraphClosed", err)
	}
}
