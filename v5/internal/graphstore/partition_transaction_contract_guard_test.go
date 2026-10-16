package graphstore

import (
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// GUARD: a remote entity's nondefault axis is registered at the declared axis
// authority, and a later conflicting authority cannot rewrite a retained view.
func TestPartitionTransactionRemoteAxisAuthorityRegistrationAndRetainedConflict(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	axis := testAxis(t, 41, temporal.ProfileRationalQ)
	all, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	original, _ := state.NewRevision(100, 9)
	template, _, err := PlanPartitionTransaction(t.Context(), v, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}}, original)
	if err != nil {
		t.Fatal(err)
	}
	current := transactionInstallTemplate(t, sources, stores, template, original)
	for i, source := range current {
		row, found, err := source.Get(t.Context(), AxisRegistrationKey(Namespace{d.Graph(), d.entries[i].Partition}, axis.Descriptor().ID), 1<<20)
		if err != nil || found != (i == 0) {
			t.Fatal("axis registration escaped its authority", i, row, found, err)
		}
	}
	retained, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	desc := axis.Descriptor()
	desc.Reference += ":conflicting"
	conflicting, err := temporal.NewAxis(desc, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	conflictScope, err := temporal.All(conflicting)
	if err != nil {
		t.Fatal(err)
	}
	next, _ := state.NewRevision(200, 9)
	before, err := stores[0].ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if out, _, err := PlanPartitionTransaction(t.Context(), retained, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 18, Life: 2, Scope: conflictScope}}, next); out != nil || !errors.Is(err, ErrRebinding) {
		t.Fatal("same AxisID admitted a new definition", out, err)
	}
	after, err := stores[0].ApplicationUsage()
	if err != nil || before != after {
		t.Fatal("axis refusal changed durable state", before, after, err)
	}
	// An independently stored conflicting authority is corruption at read time;
	// the exact old captured authority still validates the remote entity state.
	n := Namespace{d.Graph(), 3}
	wire, err := encodeAxis(n, conflicting, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	base, err := current[0].Root()
	if err != nil {
		t.Fatal(err)
	}
	changed := ownershipView(t, stores[0], ownershipInstallRaw(t, stores[0], base.Image, []raftlog.KV{{Key: AxisRegistrationKey(n, desc.ID), Value: wire}}))
	fresh, _, err := OpenPartitionTransactionView(t.Context(), []*raftlog.ApplicationView{changed, current[1]}, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 17, Kind: graphstate.Presence}, Window: all}
	if page, err := fresh.ComponentPage(t.Context(), query, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20}); !errors.Is(err, ErrCorrupt) || page.View != (graphstate.ViewID{}) {
		t.Fatal("conflicting current authority produced state", page, err)
	}
	page, err := retained.ComponentPage(t.Context(), query, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
	if err != nil || !page.Complete || page.Data.Usage().Pieces() != 1 || page.Data.Pieces()[0].Cell().Revision() != original {
		t.Fatal("current authority changed retained state", page, err)
	}
}

// GUARD: a correction can retain one complete own-presence atom while inserting
// another. Both endpoint owners keep the exact old/new footprint independently.
func TestPartitionTransactionPresenceCorrectionKeepsUnchangedAtomsAtBothEndpoints(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	points := []temporal.Scope{pagePoint(t, binding.Axis(), -3), pagePoint(t, binding.Axis(), 0), pagePoint(t, binding.Axis(), 3)}
	initial, err := points[0].Union(points[2], temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	correction, err := points[1].Union(points[2], temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	original, _ := state.NewRevision(100, 9)
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 1, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}, {Kind: graphstate.CreateRelationship, Owner: 2, Life: 1, Scope: initial, Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 1, TargetLife: 1}}}
	template, _, err := PlanPartitionTransaction(t.Context(), v, ops, original)
	if err != nil {
		t.Fatal(err)
	}
	current := transactionInstallTemplate(t, sources, stores, template, original)
	old, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	next, _ := state.NewRevision(200, 9)
	template, _, err = PlanPartitionTransaction(t.Context(), old, []graphstate.Operation{{Kind: graphstate.Correct, Owner: 2, Life: 1, Scope: correction, Present: true}}, next)
	if err != nil {
		t.Fatal(err)
	}
	installed := transactionInstallTemplate(t, current, stores, template, next)
	fresh, _, err := OpenPartitionTransactionView(t.Context(), installed, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	for i := range 2 {
		if old.views[i].descriptor.own.count != 2 || fresh.views[i].descriptor.own.count != 3 {
			t.Fatal("endpoint atom omission or duplication", i, old.views[i].descriptor.own, fresh.views[i].descriptor.own)
		}
	}
	for i, point := range points {
		query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 2, Kind: graphstate.Presence}, Window: point}
		beforePieces := 1
		if i == 1 {
			beforePieces = 0
		}
		before, err := old.ComponentPage(t.Context(), query, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if err != nil || !before.Complete || before.Data.Usage().Pieces() != beforePieces {
			t.Fatal("retained support changed", i, before, err)
		}
		after, err := fresh.ComponentPage(t.Context(), query, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
		want := original
		if i != 0 {
			want = next
		}
		if err != nil || !after.Complete || after.Data.Usage().Pieces() != 1 || !after.Data.Pieces()[0].Cell().Present() || after.Data.Pieces()[0].Cell().Revision() != want {
			t.Fatal("incorrect corrected support or provenance", i, after, err)
		}
	}
}

// GUARD: invalid owned replacement support never leaks partial changes into a
// preexisting private stage, and valid staging remains usable after each refusal.
func TestPartitionTransactionPatchSupportRefusalsPreservePrivateStage(t *testing.T) {
	for _, owner := range []graphstate.EntityID{1, 2} {
		t.Run(map[graphstate.EntityID]string{1: "node", 2: "relationship"}[owner], func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
			f.key.Owner = owner
			c := openCatalog(t, f.db, f.index, Limits{})
			s := stage(t, c)
			if err := s.Property(t.Context(), graphstate.PropertyDefinition{Name: "retained", Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}); err != nil {
				t.Fatal(err)
			}
			before, err := s.Writes()
			if err != nil {
				t.Fatal(err)
			}
			window := pagePoint(t, f.axis, 5)
			other := pagePoint(t, f.axis, 6)
			first := pagePatch(t, f.key, emptyPageState(t, f.axis), window, presentLife(t), 1, false)
			outside := pagePatch(t, f.key, emptyPageState(t, f.axis), other, presentLife(t), 1, false)
			empty, err := temporal.Empty(f.axis)
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name  string
				patch graphstate.ComponentPatch
			}{
				{"empty-owned", graphstate.ComponentPatch{Key: first.Key, Owned: empty, State: first.State, Changes: first.Changes}},
				{"state-outside-owned", graphstate.ComponentPatch{Key: first.Key, Owned: window, State: outside.State, Changes: first.Changes}},
				{"change-outside-owned", graphstate.ComponentPatch{Key: first.Key, Owned: window, State: first.State, Changes: outside.Changes}},
				{"unannounced-state", graphstate.ComponentPatch{Key: first.Key, Owned: window, State: first.State}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{tc.patch}, f.limits)
					if !errors.Is(err, ErrInvalid) || !reflect.ValueOf(result).IsZero() {
						t.Fatal("invalid support yielded partial result", result, err)
					}
					if tc.name == "unannounced-state" && !errors.Is(err, ErrPatchConflict) {
						t.Fatal("lost before-image conflict", err)
					}
					after, err := s.Writes()
					if err != nil || !sameWrites(before, after) {
						t.Fatal("refusal changed preexisting bytes", before, after, err)
					}
				})
			}
			if result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{first}, f.limits); err != nil || len(result.Groups) != 1 {
				t.Fatal("refusal poisoned later valid staging", result, err)
			}
		})
	}
}

// GUARD: an inclusive rational upper endpoint belongs to both routed own
// postings. Closing exactly that point changes current support and pruning
// flags, while retained pages still contain the original complete closed run.
func TestPartitionTransactionClosedUpperEndpointPresenceHistory(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	lo, err := temporal.RationalPosition(binding.Axis(), temporal.RationalInt64(0))
	if err != nil {
		t.Fatal(err)
	}
	hi, err := temporal.RationalPosition(binding.Axis(), temporal.RationalInt64(1))
	if err != nil {
		t.Fatal(err)
	}
	lower, err := temporal.FiniteBound(lo, true)
	if err != nil {
		t.Fatal(err)
	}
	upper, err := temporal.FiniteBound(hi, true)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := temporal.Span(binding.Axis(), lower, upper, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	point, err := temporal.Point(hi)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	original, _ := state.NewRevision(100, 9)
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 1, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}, {Kind: graphstate.CreateRelationship, Owner: 2, Life: 1, Scope: closed, Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 1, TargetLife: 1}}}
	template, _, err := PlanPartitionTransaction(t.Context(), v, ops, original)
	if err != nil {
		t.Fatal(err)
	}
	current := transactionInstallTemplate(t, sources, stores, template, original)
	old, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	next, _ := state.NewRevision(200, 9)
	template, _, err = PlanPartitionTransaction(t.Context(), old, []graphstate.Operation{{Kind: graphstate.Close, Owner: 2, Life: 1, Scope: point}}, next)
	if err != nil {
		t.Fatal(err)
	}
	installed := transactionInstallTemplate(t, current, stores, template, next)
	fresh, _, err := OpenPartitionTransactionView(t.Context(), installed, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	for phase, view := range []*PartitionTransactionView{old, fresh} {
		query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 2, Kind: graphstate.Presence}, Window: point}
		page, err := view.ComponentPage(t.Context(), query, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if err != nil || !page.Complete || page.Data.Usage().Pieces() != 1 || page.Data.Pieces()[0].Cell().Present() != (phase == 0) {
			t.Fatal("upper endpoint history substituted", phase, page, err)
		}
		for i, source := range view.views {
			tree := source.descriptor.own
			row, found, err := source.c.view.Get(t.Context(), physicalKey(source.c.root.namespace, currentPresenceRecord, tree.id), 1<<20)
			if err != nil || !found || tree.count != 1 || tree.level != 0 {
				t.Fatal("missing exact endpoint footprint", phase, i, tree, err)
			}
			p, _, err := decodeCurrentPresencePage(t.Context(), source.c.root.namespace, tree.id, tree.digest, row.Value, defaultCurrentPresenceLimits())
			if err != nil || len(p.rows) != 1 || len(p.groups) != 1 {
				t.Fatal(phase, i, p, err)
			}
			role, endpoint := cpSource, graphstate.EntityID(1)
			if i == 1 {
				role, endpoint = cpTarget, 17
			}
			flags := role | cpLowerClosed
			if phase == 0 {
				flags |= cpUpperClosed
			}
			if p.rows[0].relationship != 2 || p.rows[0].life != 1 || p.rows[0].flags != flags || p.groups[0].endpoint != endpoint || p.groups[0].bound != 1 {
				t.Fatal("incorrect retained/current endpoint atom", phase, i, p.rows, p.groups)
			}
		}
	}
}
