package graphstore

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func TestFullInitializerValidPagePolicySurvivesRemainingSourceCap(t *testing.T) {
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	_ = bootstrapRoot(t, s)
	c := openCatalog(t, s, 1, Limits{})
	defer c.view.Close()
	baseline, err := InitializeGraphIndexes(t.Context(), c, nil, GraphLimits{})
	if err != nil || baseline.Work.Bytes >= DefaultPageLimits().MaxCheckpointBytes+128 {
		t.Fatal("fixture must reach valid small remaining allowance", baseline.Work, err)
	}
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int{-1, 0, 1} {
		out, err := InitializeGraphIndexes(t.Context(), c, nil, GraphLimits{MaxSourceBytes: baseline.Work.Bytes + delta})
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrInvalid) || !reflect.DeepEqual(out, GraphEffects{}) {
				t.Fatal("valid operation budget became invalid page policy", delta, out.Work, err)
			}
		} else if err != nil || out.Root != baseline.Root || out.Work != baseline.Work || !sameWrites(out.Writes, baseline.Writes) {
			t.Fatal("exact actual source allowance refused or changed physical effects", delta, out.Work, err)
		}
		if c.stages != 0 || c.records != 0 || c.stageBytes != 0 {
			t.Fatal("budget attempt leaked private staging")
		}
		if usage, err := s.ApplicationUsage(); err != nil || usage != before {
			t.Fatal("budget attempt changed durable state", usage, before, err)
		}
	}
	if _, err := c.Root(); err != nil {
		t.Fatal("resource refusal poisoned borrow", err)
	}
}

func TestFullStageValidPagePolicySurvivesRemainingSourceCap(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	revision, err := state.NewRevision(100, 0)
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateRelationship, Owner: 2, Life: 21, Scope: scope, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: graphstate.IdentityReference}}}
	baseline, err := StageOperations(t.Context(), c, ops, revision, GraphLimits{})
	if err != nil || baseline.Work.Bytes >= DefaultPageLimits().MaxCheckpointBytes+128 {
		t.Fatal("fixture must reach real own-presence maintenance below policy floor", baseline.Work, err)
	}
	before, err := f.db.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int{-1, 0, 1} {
		out, used, err := stageOperations(t.Context(), c, ops, revision, GraphLimits{MaxSourceBytes: baseline.Work.Bytes + delta}, nil)
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrInvalid) || !reflect.DeepEqual(out, GraphEffects{}) || used.Bytes > baseline.Work.Bytes+delta {
				t.Fatal("valid aggregate budget became invalid CP page policy", delta, used, err)
			}
		} else if err != nil || out.Root != baseline.Root || out.Work != baseline.Work || !sameWrites(out.Writes, baseline.Writes) {
			t.Fatal("exact operation source allowance refused or changed effects", delta, used, err)
		}
		if c.fullViews != 0 || c.fullViewBytes != 0 || c.stages != 0 || c.records != 0 || c.stageBytes != 0 {
			t.Fatal("budget attempt leaked private staging/read view")
		}
		if usage, err := f.db.ApplicationUsage(); err != nil || usage != before {
			t.Fatal("budget attempt installed partial relationship", usage, before, err)
		}
	}
	if _, found, err := c.Entity(t.Context(), EntityRef{f.root.namespace.Graph, 2}); err != nil || found {
		t.Fatal("attempted relationship escaped private effects", found, err)
	}
}

func TestFullInitializerSharedStageAdmissionKeepsHeldState(t *testing.T) {
	s := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	_ = bootstrapRoot(t, s)
	c := openCatalog(t, s, 1, Limits{MaxStages: 1})
	defer c.view.Close()
	pages, err := (PageLimits{}).resolve()
	if err != nil {
		t.Fatal(err)
	}
	// Hold the same real private admission used by a concurrent initializer.
	// Its metadata is valid; it need not manufacture a partial index descriptor.
	held, err := allocateFullStage(c, fullStageState{root: c.root, pages: pages})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	before, err := held.Writes()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	out, err := InitializeGraphIndexes(t.Context(), c, nil, GraphLimits{})
	after, afterErr := held.Writes()
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) || afterErr != nil || !sameWrites(before, after) || c.stages != 1 || c.stageBytes != fullStageBaseBytes || c.records != 0 {
		t.Fatal("initializer changed held admission", out, err, afterErr)
	}
	if got, err := s.ApplicationUsage(); err != nil || got != usage {
		t.Fatal("refused initializer changed persistent state", got, usage, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := InitializeGraphIndexes(t.Context(), c, nil, GraphLimits{}); err != nil || out.Root.next != 6 || c.stages != 0 || c.stageBytes != 0 {
		t.Fatal("released admission not reusable", out.Root, err)
	}
}
