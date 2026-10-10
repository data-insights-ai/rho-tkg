package graphstore

import (
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func fullOwnCount(t *testing.T, f *fullFixture) uint64 {
	t.Helper()
	c := f.catalog(t, f.index)
	q, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	d, found, err := q.fullDescriptor(c.root)
	if err != nil || !found {
		t.Fatal(err)
	}
	return d.own.count
}
func TestFullPresenceInteriorSplitRecoalesceAndProvenance(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: whole})
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	old := f.index
	if n := fullOwnCount(t, f); n != 2 {
		t.Fatal("complete dual-role atom missing", n)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: f.span(t, 40, 60)})
	if n := fullOwnCount(t, f); n != 4 {
		t.Fatal("old complete atom was not split into two dual-role tails", n)
	}
	for _, at := range []int64{39, 40, 50, 60} {
		p, err := temporal.IntegerPosition(f.axis, temporal.Int64(at))
		if err != nil {
			t.Fatal(err)
		}
		query := IncidentAtQuery{Endpoint: 1, At: p, Direction: IncidentBoth, Visible: graphstate.Declared}
		want := []IncidentAtCandidate{{3, 31, IncidentSource}}
		if at >= 40 && at < 60 {
			want = nil
		}
		v := f.view(t, f.index)
		exactIncidentAt(t, collectIncidentAt(t, v, query), want)
		v.Close()
		v = f.view(t, old)
		exactIncidentAt(t, collectIncidentAt(t, v, query), []IncidentAtCandidate{{3, 31, IncidentSource}})
		v.Close()
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 3, Life: 31, Scope: f.span(t, 40, 60), Present: true})
	if n := fullOwnCount(t, f); n != 2 {
		t.Fatal("same-life repair did not recoalesce complete runs", n)
	}
	effects := f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 3, Life: 31, Scope: f.span(t, 10, 90), Present: true})
	for _, row := range effects.Writes {
		if len(row.Key) > 0 && row.Key[0] == byte(currentPresenceRecord) {
			t.Fatal("provenance-only correction rewrote own-presence atoms")
		}
	}
	if n := fullOwnCount(t, f); n != 2 {
		t.Fatal(n)
	}
}

func TestFullPresenceDisconnectedWindowsRecoverCompleteRuns(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: whole})
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: f.span(t, 20, 40)})
	old := f.index
	windows, err := f.span(t, 10, 15).Union(f.span(t, 60, 70), temporal.Limits{})
	if err != nil || windows.Kind() != temporal.ScopeRegion {
		t.Fatal(windows, err)
	}
	c := f.catalog(t, old)
	defer c.view.Close()
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	q, err := v.begin(t.Context(), graphstate.ReadBudget{})
	if err != nil {
		t.Fatal(err)
	}
	runs, envelope, err := readPresenceRuns(q, graphstate.ComponentKey{Owner: 3, Kind: graphstate.Presence}, windows, true)
	if err != nil || len(runs) != 2 {
		t.Fatal("disconnected complete run recovery", runs, err)
	}
	for i, want := range []temporal.Scope{f.span(t, 0, 20), f.span(t, 40, 100)} {
		same, e := runs[i].scope.SameSupport(want, temporal.Limits{})
		if e != nil || !same || runs[i].life != 31 {
			t.Fatal("incomplete old atom", i, runs[i], e)
		}
	}
	if envelope.Kind() != temporal.ScopeRegion {
		t.Fatal("repair bridged unrelated gap", envelope)
	}
	if err := v.finish(q, nil); err != nil {
		t.Fatal(err)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: windows})
	if count := fullOwnCount(t, f); count != 8 {
		t.Fatal("disconnected split lost dual-role runs", count)
	}
	for _, at := range []int64{0, 10, 14, 15, 20, 39, 40, 60, 69, 70, 99} {
		p, _ := temporal.IntegerPosition(f.axis, temporal.Int64(at))
		query := IncidentAtQuery{Endpoint: 1, At: p, Direction: IncidentBoth, Visible: graphstate.Declared}
		wanted := []IncidentAtCandidate{{3, 31, IncidentSource}}
		if at >= 20 && at < 40 || at >= 10 && at < 15 || at >= 60 && at < 70 {
			wanted = nil
		}
		current := f.view(t, f.index)
		exactIncidentAt(t, collectIncidentAt(t, current, query), wanted)
		_ = current.Close()
		_ = current.c.view.Close()
		wanted = []IncidentAtCandidate{{3, 31, IncidentSource}}
		if at >= 20 && at < 40 {
			wanted = nil
		}
		retained := f.view(t, old)
		exactIncidentAt(t, collectIncidentAt(t, retained, query), wanted)
		_ = retained.Close()
		_ = retained.c.view.Close()
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 3, Life: 31, Scope: windows, Present: true})
	if count := fullOwnCount(t, f); count != 4 {
		t.Fatal("disconnected correction did not restore both complete runs", count)
	}
}

func TestFullPresencePointHolesDenseAndLexicographicRetainedLives(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(cpProfileName(profile), func(t *testing.T) {
			f := newFullFixture(t, GraphLimits{})
			f.axis = testAxis(t, byte(profile), profile)
			position := func(model temporal.Rational, micro int64) temporal.Position {
				var p temporal.Position
				var err error
				if profile == temporal.ProfileRationalQ {
					p, err = temporal.RationalPosition(f.axis, model)
				} else {
					p, err = temporal.LexPosition(f.axis, model, temporal.Int64(micro))
				}
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			whole, err := temporal.All(f.axis)
			if err != nil {
				t.Fatal(err)
			}
			holeAt := position(temporal.RationalInt64(1), 0)
			hole, err := temporal.Point(holeAt)
			if err != nil {
				t.Fatal(err)
			}
			f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 21, Scope: whole})
			f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.IdentityReference}})
			old := f.index
			f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: hole})
			if count := fullOwnCount(t, f); count != 4 {
				t.Fatal("point hole did not preserve both infinite tails/roles", count)
			}
			left, _ := temporal.Fraction(temporal.Int64(999), temporal.Int64(1000), temporal.Limits{})
			right, _ := temporal.Fraction(temporal.Int64(1001), temporal.Int64(1000), temporal.Limits{})
			points := []temporal.Position{position(left, 0), holeAt, position(right, 0)}
			if profile == temporal.ProfileLexicographicQN {
				points = append(points, position(temporal.RationalInt64(1), 1))
			}
			for i, at := range points {
				query := IncidentAtQuery{Endpoint: 2, Life: 999, At: at, Direction: IncidentTarget, Mode: graphstate.IdentityReference, Visible: graphstate.Effective}
				wanted := []IncidentAtCandidate{{3, 31, IncidentTarget}}
				if i == 1 {
					wanted = nil
				}
				current := f.view(t, f.index)
				exactIncidentAt(t, collectIncidentAt(t, current, query), wanted)
				_ = current.Close()
				_ = current.c.view.Close()
				retained := f.view(t, old)
				exactIncidentAt(t, collectIncidentAt(t, retained, query), []IncidentAtCandidate{{3, 31, IncidentTarget}})
				_ = retained.Close()
				_ = retained.c.view.Close()
			}
			for _, index := range []uint64{old, f.index} {
				v := f.view(t, index)
				historical := fullReadIncidents(t, v, graphstate.IncidentPredicate{Endpoint: 2, Window: whole}, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
				if len(historical) != 1 || historical[0] != 3 {
					t.Fatal("point maintenance narrowed historical superset", historical)
				}
				_ = v.Close()
				_ = v.c.view.Close()
			}
			f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 3, Life: 31, Scope: hole, Present: true})
			if count := fullOwnCount(t, f); count != 2 {
				t.Fatal("point restoration did not recoalesce all support", count)
			}
			restored := f.index
			// Ordered patches on the same owner must remove all old-life atoms
			// before installing the newly declared life, even within one stage.
			f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: whole}, graphstate.Operation{Kind: graphstate.Reopen, Owner: 3, Life: 32, Scope: hole})
			query := IncidentAtQuery{Endpoint: 1, At: holeAt, Direction: IncidentBoth, Visible: graphstate.Effective}
			current := f.view(t, f.index)
			exactIncidentAt(t, collectIncidentAt(t, current, query), []IncidentAtCandidate{{3, 32, IncidentSource}})
			_ = current.Close()
			_ = current.c.view.Close()
			retained := f.view(t, restored)
			exactIncidentAt(t, collectIncidentAt(t, retained, query), []IncidentAtCandidate{{3, 31, IncidentSource}})
			_ = retained.Close()
			_ = retained.c.view.Close()
		})
	}
}

func TestFullPresenceSelfLoopBoundLifeAndNodeReopenMasks(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: f.span(t, 0, 50)})
	f.apply(t, graphstate.Operation{Kind: graphstate.Reopen, Owner: 1, Life: 12, Scope: f.span(t, 50, 100)})
	invalid := graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: f.span(t, 0, 100), Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 12}}
	// Distinct bound lives of one node cannot simultaneously cover placement;
	// the semantic API rejects them rather than manufacturing a masked fact.
	c := f.catalog(t, f.index)
	revision, _ := state.NewRevision(99, 99)
	effects, err := StageOperations(t.Context(), c, []graphstate.Operation{invalid}, revision, GraphLimits{})
	if !errors.Is(err, ErrInvalid) || !errors.Is(err, graphstate.ErrOwnerValidity) || !reflect.DeepEqual(effects, GraphEffects{}) {
		t.Fatal("incompatible self-loop lives admitted", effects, err)
	}
	_ = c.view.Close()
	legal := invalid
	legal.Scope = f.span(t, 50, 100)
	legal.Binding.SourceLife = 12
	f.apply(t, legal)
	old := f.index
	at, _ := temporal.IntegerPosition(f.axis, temporal.Int64(75))
	for _, tc := range []struct {
		life      graphstate.LifeID
		direction IncidentDirection
		want      []IncidentAtCandidate
	}{{0, IncidentBoth, []IncidentAtCandidate{{3, 31, IncidentBoth}}}, {11, IncidentBoth, nil}, {12, IncidentBoth, []IncidentAtCandidate{{3, 31, IncidentBoth}}}, {12, IncidentSource, []IncidentAtCandidate{{3, 31, IncidentSource}}}, {12, IncidentTarget, []IncidentAtCandidate{{3, 31, IncidentTarget}}}, {99, IncidentBoth, nil}} {
		for _, visible := range []graphstate.Visibility{graphstate.Declared, graphstate.Effective} {
			query := IncidentAtQuery{Endpoint: 1, Life: tc.life, At: at, Direction: tc.direction, Visible: visible}
			v := f.view(t, f.index)
			exactIncidentAt(t, collectIncidentAt(t, v, query), tc.want)
			_ = v.Close()
			_ = v.c.view.Close()
		}
	}
	beforeCount := fullOwnCount(t, f)
	hole := f.span(t, 70, 80)
	effects = f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 12, Scope: hole}, graphstate.Operation{Kind: graphstate.Reopen, Owner: 1, Life: 13, Scope: hole})
	for _, row := range effects.Writes {
		if len(row.Key) > 0 && row.Key[0] == byte(currentPresenceRecord) {
			t.Fatal("node mutation rewrote incident own atoms")
		}
	}
	if fullOwnCount(t, f) != beforeCount {
		t.Fatal("node mutation changed own support")
	}
	masked := f.index
	query := IncidentAtQuery{Endpoint: 1, At: at, Direction: IncidentBoth, Visible: graphstate.Effective}
	for _, tc := range []struct {
		index uint64
		want  []IncidentAtCandidate
	}{{old, []IncidentAtCandidate{{3, 31, IncidentBoth}}}, {masked, nil}} {
		v := f.view(t, tc.index)
		exactIncidentAt(t, collectIncidentAt(t, v, query), tc.want)
		_ = v.Close()
		_ = v.c.view.Close()
	}
	effects = f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 13, Scope: hole}, graphstate.Operation{Kind: graphstate.Correct, Owner: 1, Life: 12, Scope: hole, Present: true})
	for _, row := range effects.Writes {
		if len(row.Key) > 0 && row.Key[0] == byte(currentPresenceRecord) {
			t.Fatal("endpoint restoration rewrote incident own atoms")
		}
	}
	for _, tc := range []struct {
		index uint64
		want  []IncidentAtCandidate
	}{{masked, nil}, {f.index, []IncidentAtCandidate{{3, 31, IncidentBoth}}}} {
		v := f.view(t, tc.index)
		exactIncidentAt(t, collectIncidentAt(t, v, query), tc.want)
		_ = v.Close()
		_ = v.c.view.Close()
	}
	query.Visible = graphstate.Declared
	for _, index := range []uint64{old, masked, f.index} {
		v := f.view(t, index)
		exactIncidentAt(t, collectIncidentAt(t, v, query), []IncidentAtCandidate{{3, 31, IncidentBoth}})
		_ = v.Close()
		_ = v.c.view.Close()
	}
}

func TestFullPresenceOwnerLocalRecoveryCrossesDirectoryLeaves(t *testing.T) {
	f := newFullFixture(t, GraphLimits{Pages: PageLimits{MaxChildren: 3, MaxCells: 2, MaxTailRecords: 1, MaxWorkRecords: 4096}})
	whole, err := temporal.All(f.axis)
	if err != nil {
		t.Fatal(err)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 21, Scope: whole})
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 21}})
	point := func(n int64) temporal.Scope {
		p, _ := temporal.IntegerPosition(f.axis, temporal.Int64(n))
		scope, e := temporal.Point(p)
		if e != nil {
			t.Fatal(e)
		}
		return scope
	}
	// Fourteen provenance corrections make many tiny leaves while semantic
	// same-life support remains All. Recovery must cross actual directory levels,
	// rather than treating the first materialized leaf as a complete index atom.
	for n := int64(2); n <= 28; n += 2 {
		effects := f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 3, Life: 31, Scope: point(n), Present: true})
		for _, row := range effects.Writes {
			if len(row.Key) > 0 && row.Key[0] == byte(currentPresenceRecord) {
				t.Fatal("provenance fragmentation edited own support")
			}
		}
	}
	old := f.index
	c := f.catalog(t, old)
	v, err := OpenReadView(t.Context(), c, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	q, err := v.begin(t.Context(), graphstate.ReadBudget{})
	if err != nil {
		t.Fatal(err)
	}
	meta, found, err := q.readMeta(graphstate.ComponentKey{Owner: 3, Kind: graphstate.Presence})
	if err != nil || !found {
		t.Fatal(err)
	}
	directory, err := q.directory(meta.Root, meta.Key, meta.Axis)
	if err != nil || directory.Level < 2 {
		t.Fatal("fixture did not force multilevel component recovery", directory.Level, err)
	}
	holes, err := point(10).Union(point(22), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	runs, _, err := readPresenceRuns(q, meta.Key, holes, true)
	if err != nil || len(runs) != 1 || runs[0].life != 31 {
		t.Fatal("bounded recovery did not recover complete old owner atom", runs, err)
	}
	same, err := runs[0].scope.SameSupport(whole, temporal.Limits{})
	if err != nil || !same {
		t.Fatal("component leaves truncated old atom", err)
	}
	if err := v.finish(q, q.budget()); err != nil {
		t.Fatal(err)
	}
	_ = v.Close()
	_ = c.view.Close()
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: holes})
	if count := fullOwnCount(t, f); count != 6 {
		t.Fatal("subtree-spanning point holes did not install three complete dual-role runs", count)
	}
	for _, n := range []int64{9, 10, 11, 21, 22, 23} {
		at, _ := temporal.IntegerPosition(f.axis, temporal.Int64(n))
		query := IncidentAtQuery{Endpoint: 1, At: at, Direction: IncidentBoth, Visible: graphstate.Effective}
		want := []IncidentAtCandidate{{3, 31, IncidentSource}}
		if n == 10 || n == 22 {
			want = nil
		}
		current := f.view(t, f.index)
		exactIncidentAt(t, collectIncidentAt(t, current, query), want)
		_ = current.Close()
		_ = current.c.view.Close()
		retained := f.view(t, old)
		exactIncidentAt(t, collectIncidentAt(t, retained, query), []IncidentAtCandidate{{3, 31, IncidentSource}})
		_ = retained.Close()
		_ = retained.c.view.Close()
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 3, Life: 31, Scope: holes, Present: true})
	if count := fullOwnCount(t, f); count != 2 {
		t.Fatal("cross-directory repair failed to restore one dual-role All atom", count)
	}
}
