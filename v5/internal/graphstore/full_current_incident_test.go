package graphstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

type incidentAtReader interface {
	IncidentAt(context.Context, IncidentAtQuery, graphstate.Cursor, graphstate.ReadBudget) (IncidentAtPage, error)
}

func collectIncidentAt(t *testing.T, v *ReadView, query IncidentAtQuery) []IncidentAtCandidate {
	t.Helper()
	reader, ok := any(v).(incidentAtReader)
	if !ok {
		t.Fatal("Full reader lacks the distinct exact IncidentAt capability")
	}
	var cursor graphstate.Cursor
	var got []IncidentAtCandidate
	for range 4096 {
		page, err := reader.IncidentAt(t.Context(), query, cursor, graphstate.ReadBudget{Rows: 16, Bytes: 64 << 10})
		if err != nil || page.View != v.Identity() || page.Version != v.version() {
			t.Fatal(page, err)
		}
		got = append(got, page.Candidates...)
		if page.Complete {
			if page.Next != 0 {
				t.Fatal("complete page retained a cursor")
			}
			return got
		}
		if page.Next == 0 || page.Next == cursor {
			t.Fatal("current incident page did not advance")
		}
		cursor = page.Next
	}
	t.Fatal("current incident read never completed")
	return nil
}

func exactIncidentAt(t *testing.T, got, want []IncidentAtCandidate) {
	t.Helper()
	compare := func(a, b IncidentAtCandidate) int {
		if a.Relationship < b.Relationship {
			return -1
		}
		if a.Relationship > b.Relationship {
			return 1
		}
		if a.Life < b.Life {
			return -1
		}
		if a.Life > b.Life {
			return 1
		}
		return int(a.Roles) - int(b.Roles)
	}
	slices.SortFunc(got, compare)
	slices.SortFunc(want, compare)
	if !slices.Equal(got, want) {
		t.Fatalf("exact current incidents got=%+v want=%+v", got, want)
	}
}

func TestIncidentAtOwnEndingsEndpointMasksAndRetainedView(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "incident-at-red")
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: whole})
	for _, spec := range []struct {
		id, target graphstate.EntityID
		mode       graphstate.ReferenceMode
	}{{3, 1, graphstate.LifeBound}, {4, 2, graphstate.LifeBound}, {5, 2, graphstate.LifeBound}, {6, 2, graphstate.IdentityReference}} {
		binding := graphstate.LifeRecord{}
		if spec.mode == graphstate.LifeBound {
			binding.SourceLife, binding.TargetLife = 11, 11
		}
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: spec.id, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: spec.target, Mode: spec.mode}, Binding: binding})
	}
	old := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 4, Life: 31, Scope: f.span(t, 50, 100)}, graphstate.Operation{Kind: graphstate.Close, Owner: 2, Life: 11, Scope: f.span(t, 50, 100)})
	query := IncidentAtQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Direction: IncidentBoth, Type: "R", Visible: graphstate.Effective}
	for _, tc := range []struct {
		index uint64
		want  []IncidentAtCandidate
	}{{old, []IncidentAtCandidate{{3, 31, IncidentBoth}, {4, 31, IncidentSource}, {5, 31, IncidentSource}, {6, 31, IncidentSource}}}, {f.index, []IncidentAtCandidate{{3, 31, IncidentBoth}, {6, 31, IncidentSource}}}} {
		c, close := f.catalog(t, tc.index)
		v, err := OpenReadView(t.Context(), c, GraphLimits{})
		if err != nil {
			t.Fatal(err)
		}
		exactIncidentAt(t, collectIncidentAt(t, v, query), tc.want)
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
		close()
	}
}

func TestIncidentAtWideHashRefusalKeepsCursorAndChargesAttempt(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "wide-hash-refusal")
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
	for _, id := range []graphstate.EntityID{3, 4} {
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	}
	c, close := f.catalog(t, f.index)
	defer close()
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	query := IncidentAtQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Direction: IncidentBoth, Visible: graphstate.Declared}
	page, err := v.IncidentAt(t.Context(), query, 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || page.Next == 0 {
		t.Fatal(page, err)
	}
	retained := v.cursors[page.Next]
	before := v.Work()
	output := v.outputBytes
	cursorBytes := v.cursorBytes
	n, err := temporal.ParseInteger(strings.Repeat("9", 1000), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wide, err := temporal.IntegerPosition(f.axis, n)
	if err != nil {
		t.Fatal(err)
	}
	refused := query
	refused.At = wide
	limit := v.limits.MaxSourceBytes
	v.limits.MaxSourceBytes = before.Bytes + 2048
	failed, err := v.IncidentAt(t.Context(), refused, page.Next, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if !errors.Is(err, ErrResourceLimit) || failed.View != (graphstate.ViewID{}) || failed.Next != 0 || len(failed.Candidates) != 0 {
		t.Fatal("wide precharge refusal", failed, err)
	}
	if v.Work().Bytes <= before.Bytes || v.outputBytes != output || v.cursorBytes != cursorBytes || v.cursors[page.Next].current != retained.current {
		t.Fatal("refusal changed cursor/output or hid attempted source", before, v.Work())
	}
	v.limits.MaxSourceBytes = limit
	resumed, err := v.IncidentAt(t.Context(), query, page.Next, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	exactIncidentAt(t, resumed.Candidates, []IncidentAtCandidate{{4, 31, IncidentBoth}})
}

func TestIncidentAtCoordinatePreflightDefaultAndPartialLimits(t *testing.T) {
	wide, err := temporal.ParseInteger(strings.Repeat("9", 1000), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		axis := testAxis(t, byte(profile), profile)
		rational, err := temporal.Fraction(wide, temporal.Int64(3), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		var position temporal.Position
		switch profile {
		case temporal.ProfileIntegerZ:
			position, err = temporal.IntegerPosition(axis, wide)
		case temporal.ProfileRationalQ:
			position, err = temporal.RationalPosition(axis, rational)
		case temporal.ProfileLexicographicQN:
			position, err = temporal.LexPosition(axis, rational, wide)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, limits := range []temporal.Limits{{}, {MaxMagnitudeBits: 4096}, {MaxValueBytes: 65536}, {MaxMagnitudeBits: 4096, MaxValueBytes: 65536}} {
			capacity, _ := cpPositionBudget(position, limits)
			wire, err := temporal.AppendPosition(make([]byte, 0, capacity), position, limits)
			if err != nil || len(wire) > capacity || cap(wire) != capacity {
				t.Fatal("encoding escaped precharged capacity", profile, limits, capacity, len(wire), cap(wire), err)
			}
		}
	}
	f := cpFixture(t, temporal.ProfileIntegerZ)
	cpSeedRelationships(t, f)
	f.commit(t, nil, nil)
	c := openCatalog(t, f.db, f.index, Limits{})
	defer c.view.Close()
	at, err := temporal.IntegerPosition(f.axis, wide)
	if err != nil {
		t.Fatal(err)
	}
	for _, limits := range []temporal.Limits{{}, {MaxMagnitudeBits: 4096}, {MaxValueBytes: 65536}} {
		l := f.limits
		l.codec.temporal = limits
		it, work, err := newCurrentPresenceIteratorWork(t.Context(), c, f.tree, currentPresenceQuery{endpoint: 1, at: at, direction: cpSource}, l)
		if err != nil {
			t.Fatal("default/partial primitive iterator", limits, work, err)
		}
		capacity, scratch := cpPositionBudget(at, limits)
		if work.Bytes < capacity+scratch {
			t.Fatal("constructor omitted coordinate attempt", work, capacity, scratch)
		}
		if err := it.close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIncidentAtBatchResourceBoundaryPublishesCompletedProgress(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "batch-resource")
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
	for id := graphstate.EntityID(3); id < 13; id++ {
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	}
	c, close := f.catalog(t, f.index)
	defer close()
	query := IncidentAtQuery{Endpoint: 1, At: f.position(t, 75), Direction: IncidentBoth, Visible: graphstate.Effective}
	// Measure one resumed row under normal limits; use that observed allowance
	// for an eight-row request. Its first completed advance must survive the
	// next resource refusal, without pretending the entire batch completed.
	calibration, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := calibration.IncidentAt(t.Context(), query, 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	before := calibration.Work()
	_, err = calibration.IncidentAt(t.Context(), query, first.Next, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	oneRowBytes := calibration.Work().Bytes - before.Bytes
	_ = calibration.Close()
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	first, err = v.IncidentAt(t.Context(), query, 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	capBytes := v.limits.Pages.MaxWorkBytes
	v.limits.Pages.MaxWorkBytes = oneRowBytes + 4096
	before = v.Work()
	partial, err := v.IncidentAt(t.Context(), query, first.Next, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
	if err != nil || partial.Complete || partial.Next == 0 || partial.Next == first.Next || len(partial.Candidates) == 0 || len(partial.Candidates) >= 8 {
		t.Fatal("resource boundary erased completed batch progress", partial, err, oneRowBytes, v.Work())
	}
	if _, exists := v.cursors[first.Next]; exists {
		t.Fatal("old cursor remained published")
	}
	if v.Work().Bytes <= before.Bytes {
		t.Fatal("refused attempt was not charged")
	}
	v.limits.Pages.MaxWorkBytes = capBytes
	totalOutput := v.limits.MaxOutputBytes
	v.limits.MaxOutputBytes = v.outputBytes + fullPageOutputBytes + cpCandidateOwned
	quotaPage, err := v.IncidentAt(t.Context(), query, partial.Next, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
	if err != nil || len(quotaPage.Candidates) != 1 || quotaPage.Next == 0 || quotaPage.Complete {
		t.Fatal("shared output cap lost one-row progress", quotaPage, err)
	}
	v.limits.MaxOutputBytes = totalOutput
	got := append(first.Candidates, partial.Candidates...)
	got = append(got, quotaPage.Candidates...)
	cursor := quotaPage.Next
	for cursor != 0 {
		page, err := v.IncidentAt(t.Context(), query, cursor, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page.Candidates...)
		cursor = page.Next
	}
	want := make([]IncidentAtCandidate, 10)
	for i := range want {
		want[i] = IncidentAtCandidate{graphstate.EntityID(i + 3), 31, IncidentBoth}
	}
	exactIncidentAt(t, got, want)
	t.Logf("BATCH_RESOURCE observedOneRowBytes=%d cap=%d completed=%d attemptedWork=%+v", oneRowBytes, oneRowBytes+4096, len(partial.Candidates), v.Work())
}

type incidentBatchCancelContext struct {
	context.Context
	view         *ReadView
	afterWitness int
}

func (c *incidentBatchCancelContext) Err() error {
	if c.view.currentWork.WitnessEntityCalls >= c.afterWitness {
		return context.Canceled
	}
	return c.Context.Err()
}
func TestIncidentAtBatchLateCancellationRollsBackAllProgress(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "batch-cancel")
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
	for id := graphstate.EntityID(3); id < 7; id++ {
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	}
	c, close := f.catalog(t, f.index)
	defer close()
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	query := IncidentAtQuery{Endpoint: 1, At: f.position(t, 75), Direction: IncidentBoth, Visible: graphstate.Effective}
	first, err := v.IncidentAt(t.Context(), query, 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	old := v.cursors[first.Next]
	before := checkpointCurrent(old.current.iterator)
	work := v.Work()
	output := v.outputBytes
	ctx := &incidentBatchCancelContext{Context: t.Context(), view: v, afterWitness: 3}
	page, err := v.IncidentAt(ctx, query, first.Next, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
	if !errors.Is(err, context.Canceled) || page.View != (graphstate.ViewID{}) || len(page.Candidates) != 0 || page.Next != 0 {
		t.Fatal("late cancellation published a batch", page, err)
	}
	if v.currentWork.WitnessEntityCalls < 3 || v.Work().Bytes <= work.Bytes || v.outputBytes != output || !reflect.DeepEqual(before, checkpointCurrent(old.current.iterator)) || v.cursors[first.Next].current != old.current {
		t.Fatal("cancelled batch did not restore whole cursor/charge attempt", work, v.Work())
	}
	remaining, err := v.IncidentAt(t.Context(), query, first.Next, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	exactIncidentAt(t, remaining.Candidates, []IncidentAtCandidate{{4, 31, IncidentBoth}, {5, 31, IncidentBoth}, {6, 31, IncidentBoth}})
}

func TestIncidentAtAxisRegistrationIndependentOfEndpointAndMode(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "axis-contract")
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: f.span(t, 0, 100)})
	other := testAxis(t, 2, temporal.ProfileRationalQ)
	all, err := temporal.All(other)
	if err != nil {
		t.Fatal(err)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 21, Scope: all})
	knownOther, err := temporal.RationalPosition(other, temporal.RationalInt64(75))
	if err != nil {
		t.Fatal(err)
	}
	unknown := testAxis(t, 9, temporal.ProfileIntegerZ)
	unknownAt, err := temporal.IntegerPosition(unknown, temporal.Int64(75))
	if err != nil {
		t.Fatal(err)
	}
	changed := f.axis.Descriptor()
	changed.Reference = "different registered definition"
	changedAxis, err := temporal.NewAxis(changed, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	changedAt, err := temporal.IntegerPosition(changedAxis, temporal.Int64(75))
	if err != nil {
		t.Fatal(err)
	}
	c, close := f.catalog(t, f.index)
	defer close()
	for _, endpoint := range []graphstate.EntityID{1, 99} {
		for _, mode := range []graphstate.ReferenceMode{0, graphstate.LifeBound, graphstate.IdentityReference} {
			for _, visible := range []graphstate.Visibility{graphstate.Declared, graphstate.Effective} {
				for _, tc := range []struct {
					at      temporal.Position
					invalid bool
				}{{f.position(t, 75), false}, {knownOther, false}, {unknownAt, true}, {changedAt, true}} {
					v, err := OpenReadView(t.Context(), c, GraphLimits{})
					if err != nil {
						t.Fatal(err)
					}
					before := v.Work()
					page, err := v.IncidentAt(t.Context(), IncidentAtQuery{Endpoint: endpoint, At: tc.at, Mode: mode, Direction: IncidentBoth, Visible: visible}, 0, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
					if tc.invalid {
						if !errors.Is(err, ErrInvalid) || !errors.Is(err, temporal.ErrAxisMismatch) || page.View != (graphstate.ViewID{}) || page.Next != 0 || len(v.cursors) != 0 || v.Work().Records <= before.Records {
							t.Fatal("mode-dependent axis admission or hidden attempted lookup", endpoint, mode, visible, page, err, before, v.Work())
						}
					} else if err != nil || !page.Complete || len(page.Candidates) != 0 || page.Next != 0 {
						t.Fatal("valid registered phantom/other axis did not return empty", endpoint, mode, visible, page, err)
					}
					_ = v.Close()
				}
			}
		}
	}
}

func TestIncidentAtNilInvalidAndClosedReaderSentinels(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	at, _ := temporal.IntegerPosition(f.axis, temporal.Int64(0))
	query := IncidentAtQuery{Endpoint: 1, At: at, Direction: IncidentBoth, Visible: graphstate.Declared}
	budget := graphstate.ReadBudget{Rows: 8, Bytes: 4096}
	var nilReader *ReadView
	if _, err := nilReader.IncidentAt(t.Context(), query, 0, budget); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil reader", err)
	}
	v := f.view(t, f.index)
	if _, err := v.IncidentAt(nil, query, 0, budget); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil context", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := v.IncidentAt(cancelled, query, 0, budget); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled call", err)
	}
	for _, change := range []func(*IncidentAtQuery){func(q *IncidentAtQuery) { q.Endpoint = 0 }, func(q *IncidentAtQuery) { q.Direction = 0 }, func(q *IncidentAtQuery) { q.Direction = 8 }, func(q *IncidentAtQuery) { q.Mode = 99 }, func(q *IncidentAtQuery) { q.Visible = 99 }, func(q *IncidentAtQuery) { q.Type = " " }, func(q *IncidentAtQuery) { q.Type = strings.Repeat("x", 257) }} {
		invalid := query
		change(&invalid)
		page, err := v.IncidentAt(t.Context(), invalid, 0, budget)
		if !errors.Is(err, ErrInvalid) || page.View != (graphstate.ViewID{}) || page.Next != 0 {
			t.Fatal("invalid predicate", invalid, page, err)
		}
	}
	for _, invalidBudget := range []graphstate.ReadBudget{{Rows: 0, Bytes: 4096}, {Rows: 1, Bytes: 0}, {Rows: 1, Bytes: 127}} {
		if _, err := v.IncidentAt(t.Context(), query, 0, invalidBudget); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid budget", invalidBudget, err)
		}
	}
	malformed := query
	malformed.At = temporal.Position{}
	before := v.Work()
	if _, err := v.IncidentAt(t.Context(), malformed, 0, budget); !errors.Is(err, ErrInvalid) || !errors.Is(err, temporal.ErrInvalidPosition) || v.Work().Bytes <= before.Bytes {
		t.Fatal("position sentinel/attempt work", err, before, v.Work())
	}
	_ = v.Close()
	if _, err := v.IncidentAt(t.Context(), query, 0, budget); !errors.Is(err, ErrClosed) {
		t.Fatal("closed reader", err)
	}
}

func TestIncidentAtEffectiveShortcutCannotHideLiveEndpointCorruption(t *testing.T) {
	for _, fault := range []string{"tombstoned", "wrong-axis"} {
		t.Run(fault, func(t *testing.T) {
			f := newFullFixture(t, GraphLimits{})
			whole := f.span(t, 0, 100)
			f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
			other := testAxis(t, 2, temporal.ProfileRationalQ)
			all, _ := temporal.All(other)
			f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 21, Scope: all})
			f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
			c := f.catalog(t, f.index)
			q, err := c.reader(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			node, found, err := q.entity(EntityRef{c.root.namespace.Graph, 1})
			if err != nil || !found {
				t.Fatal(err)
			}
			row := raftlog.KV{Key: entityKey(c.root.namespace, 1), Deleted: true}
			if fault == "wrong-axis" {
				node.Axis = other
				row.Deleted = false
				row.Value, err = encodeEntity(c.root.namespace, node, c.limits)
				if err != nil {
					t.Fatal(err)
				}
			}
			base, err := c.view.Root()
			if err != nil {
				t.Fatal(err)
			}
			f.install(t, GraphEffects{Base: base, Root: c.root, Writes: []raftlog.KV{row}})
			_ = c.view.Close()
			at, _ := temporal.IntegerPosition(f.axis, temporal.Int64(75))
			for _, mode := range []graphstate.ReferenceMode{0, graphstate.LifeBound} {
				for _, visible := range []graphstate.Visibility{graphstate.Declared, graphstate.Effective} {
					t.Run(fmt.Sprintf("mode%d/visibility%d", mode, visible), func(t *testing.T) {
						// Each probe has independent poisoning: ordinary current candidate
						// validation must reject the same live row in both visibility modes.
						v := f.view(t, f.index)
						before := v.Work()
						page, err := v.IncidentAt(t.Context(), IncidentAtQuery{Endpoint: 1, At: at, Direction: IncidentBoth, Mode: mode, Visible: visible}, 0, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
						if !errors.Is(err, ErrCorrupt) || page.View != (graphstate.ViewID{}) || page.Next != 0 || len(page.Candidates) != 0 || len(v.cursors) != 0 || v.Work().Bytes <= before.Bytes {
							t.Fatal("effective shortcut silently hid live endpoint corruption", fault, mode, visible, page, err, before, v.Work())
						}
						_ = v.Close()
						_ = v.c.view.Close()
					})
				}
			}
		})
	}
}

func TestIncidentAtCompatibleClosedEndpointPrunesLifeBoundCandidates(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	old := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 11, Scope: f.span(t, 50, 100)})
	at, _ := temporal.IntegerPosition(f.axis, temporal.Int64(75))
	query := IncidentAtQuery{Endpoint: 1, At: at, Direction: IncidentBoth, Mode: graphstate.LifeBound, Visible: graphstate.Effective}
	current := f.view(t, f.index)
	exactIncidentAt(t, collectIncidentAt(t, current, query), nil)
	if current.currentWork.LoadedPages != 0 || current.currentWork.WitnessEntityCalls != 0 || current.currentWork.WitnessLifeCalls != 0 {
		t.Fatal("known closed endpoint lost batch pruning", current.currentWork)
	}
	_ = current.Close()
	_ = current.c.view.Close()
	retained := f.view(t, old)
	exactIncidentAt(t, collectIncidentAt(t, retained, query), []IncidentAtCandidate{{3, 31, IncidentBoth}})
	_ = retained.Close()
	_ = retained.c.view.Close()
}

func TestIncidentAtSharedOppositePresenceReplaysBoundedPerCall(t *testing.T) {
	f := pilotFixture(t, 64, true)
	metric, err := pilotCurrent(t, f, GraphLimits{})
	if err != nil || !metric.Complete {
		t.Fatal(metric, err)
	}
	want := make([]IncidentAtCandidate, 10)
	for i := range want {
		want[i] = IncidentAtCandidate{graphstate.EntityID(i + 3), 31, IncidentBoth}
	}
	exactIncidentAt(t, metric.Candidates, want)
	// One source proof and at most one two-patch opposite presence per call.
	// This must fail when every eligible relationship replays the same node.
	if metric.Work.PatchPages > 1+2*metric.Calls {
		t.Fatal("same opposite presence replayed per fact", metric.Calls, metric.Work)
	}
}

func TestIncidentScratchPostingHitsPreserveRootParentAndKeyFences(t *testing.T) {
	f := pilotFixture(t, 64, true)
	c, close := pilotQueryCatalog(t, f)
	defer close()
	for _, kind := range []recordKind{canonicalIncidentRecord, declaredIncidentRecord} {
		for _, fault := range []string{"root-level", "root-count", "child-level", "child-count", "child-first", "child-last", "payload", "policy", "mutable", "cancel"} {
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				v, err := OpenReadView(t.Context(), c, GraphLimits{})
				if err != nil {
					t.Fatal(err)
				}
				defer v.Close()
				q, err := v.begin(t.Context(), graphstate.ReadBudget{})
				if err != nil {
					t.Fatal(err)
				}
				s, err := newIncidentScratch(q, &v.descriptor, f.position(t, 75))
				if err != nil {
					t.Fatal(err)
				}
				root := v.descriptor.canonical
				if kind == declaredIncidentRecord {
					root = v.descriptor.declared
				}
				if root.level != 1 {
					t.Fatal("fixture lacks root/leaf fences", root)
				}
				key := postingTestKey(kind, 13)
				limits := keyTreeLimits(q.limits)
				found, err := s.hasPosting(root, key, limits)
				if err != nil || !found {
					t.Fatal(found, err)
				}
				before := q.q.rows
				found, err = s.hasPosting(root, key, limits)
				if err != nil || !found || q.q.rows != before {
					t.Fatal("hit re-decoded page", found, err)
				}
				index := 0
				if kind == declaredIncidentRecord {
					index = 1
				}
				n := &s.posting[index].node
				child := &n.children[postingKeyChildIndex(*n, key)]
				want := ErrCorrupt
				switch fault {
				case "root-level":
					root.level++
				case "root-count":
					root.count++
				case "child-level":
					n.level++
					root.level++
				case "child-count":
					child.count++
					root.count++
				case "child-first":
					child.first.roles ^= 3
				case "child-last":
					child.last.roles ^= 3
				case "payload":
					key.roles = 2
				case "policy":
					limits.maxKeys = 2
					want = ErrResourceLimit
				case "mutable":
					q.q.pending = make(map[string]raftlog.KV)
					want = ErrInvalid
				case "cancel":
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					q.q.ctx = ctx
					want = context.Canceled
				}
				found, err = s.hasPosting(root, key, limits)
				if found || !errors.Is(err, want) {
					t.Fatal("cached binding/policy violation accepted", found, err, want)
				}
			})
		}
	}
}

func TestIncidentScratchPostingEvictsOnlyBoundedNonRootPages(t *testing.T) {
	f := pilotFixture(t, 256, true)
	c, close := pilotQueryCatalog(t, f)
	defer close()
	v, err := OpenReadView(t.Context(), c, pilotQueryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	q, err := v.begin(t.Context(), graphstate.ReadBudget{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := newIncidentScratch(q, &v.descriptor, f.position(t, 75))
	if err != nil {
		t.Fatal(err)
	}
	limits := keyTreeLimits(q.limits)
	for _, kind := range []recordKind{canonicalIncidentRecord, declaredIncidentRecord} {
		root := v.descriptor.canonical
		if kind == declaredIncidentRecord {
			root = v.descriptor.declared
		}
		for id := uint64(3); id < 269; id++ {
			key := postingTestKey(kind, id)
			if id < 13 {
				key.roles = 3
			}
			found, err := s.hasPosting(root, key, limits)
			if err != nil || !found {
				t.Fatal(id, kind, found, err)
			}
			if s.postingBytes > incidentPostingOwnedLimit {
				t.Fatal("scratch ownership grew", s.postingBytes)
			}
		}
	}
	if !s.posting[0].valid || !s.posting[1].valid || s.posting[0].node.id != v.descriptor.canonical.id || s.posting[1].node.id != v.descriptor.declared.id {
		t.Fatal("root slots evicted", s.posting)
	}
	// Both trees have more than the four non-root slots. Returning to the
	// first leaf must decode it again, with all ordinary fences preserved.
	before := q.q.rows
	found, err := s.hasPosting(v.descriptor.canonical, postingKey{family: canonicalIncidentRecord, endpoint: 1, mode: graphstate.LifeBound, relationship: 3, roles: 3}, limits)
	if err != nil || !found || q.q.rows <= before {
		t.Fatal("non-root slots failed to evict", found, err, before, q.q.rows)
	}
}

func TestIncidentScratchDistinctEndpointsPointsAndRetainedCursors(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
	for id := graphstate.EntityID(2); id <= 7; id++ {
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: id, Life: 11, Scope: whole})
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id + 100, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: id, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	}
	old := f.index
	for _, id := range []graphstate.EntityID{2, 4, 6} {
		f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: id, Life: 11, Scope: f.span(t, 50, 100)})
	}
	retained, current := f.view(t, old), f.view(t, f.index)
	defer retained.Close()
	defer retained.c.view.Close()
	defer current.Close()
	defer current.c.view.Close()
	at25, _ := temporal.IntegerPosition(f.axis, temporal.Int64(25))
	at75, _ := temporal.IntegerPosition(f.axis, temporal.Int64(75))
	q, err := current.begin(t.Context(), graphstate.ReadBudget{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := newIncidentScratch(q, &current.descriptor, at75)
	if err != nil {
		t.Fatal(err)
	}
	for id := graphstate.EntityID(2); id <= 7; id++ {
		life, err := s.presenceAt(id)
		want := graphstate.LifeID(11)
		if id%2 == 0 {
			want = 0
		}
		if err != nil || life != want {
			t.Fatal("wrong point presence", id, life, err)
		}
	}
	before := q.q.rows
	if life, err := s.presenceAt(7); err != nil || life != 11 || q.q.rows != before {
		t.Fatal("scalar hit missed", life, err)
	}
	if life, err := s.presenceAt(2); err != nil || life != 0 || q.q.rows <= before {
		t.Fatal("distinct scalar witnesses did not evict", life, err)
	}
	// One-call scratch must not be reachable from the q retained by a cursor.
	if unsafe.Sizeof(*s) > incidentScratchOwned {
		t.Fatal("fixed scratch allowance too small", unsafe.Sizeof(*s))
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[reader](), reflect.TypeFor[pageReader](), reflect.TypeFor[fullCurrentCursor]()} {
		for i := range typ.NumField() {
			if typ.Field(i).Type == reflect.TypeFor[*incidentScratch]() {
				t.Fatal("scratch retained by published cursor", typ)
			}
		}
	}
	probes := []struct {
		v        *ReadView
		at       temporal.Position
		cursor   graphstate.Cursor
		got      []IncidentAtCandidate
		complete bool
	}{{v: retained, at: at75}, {v: current, at: at25}, {v: current, at: at75}}
	for range 8 {
		for i := range probes {
			p := &probes[i]
			if p.complete {
				continue
			}
			page, err := p.v.IncidentAt(t.Context(), IncidentAtQuery{Endpoint: 1, At: p.at, Direction: IncidentBoth, Visible: graphstate.Effective}, p.cursor, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
			if err != nil {
				t.Fatal(i, page, err)
			}
			p.got = append(p.got, page.Candidates...)
			p.cursor = page.Next
			p.complete = page.Complete
		}
	}
	for i, p := range probes {
		if !p.complete || p.cursor != 0 {
			t.Fatal("interleaved cursor incomplete", i, p)
		}
		var want []IncidentAtCandidate
		for id := graphstate.EntityID(2); id <= 7; id++ {
			if i != 2 || id%2 == 1 {
				want = append(want, IncidentAtCandidate{id + 100, 31, IncidentSource})
			}
		}
		exactIncidentAt(t, p.got, want)
	}
}
