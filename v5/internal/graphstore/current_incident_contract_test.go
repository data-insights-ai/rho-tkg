package graphstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// These are measurement adapters, not an engine access API or an index. A
// captured application position identifies the view; query.At supplies valid
// position AND axis. Type/Mode zero mean all within that explicitly chosen axis.
type currentIncidentQuery struct {
	Endpoint graphstate.EntityID
	Life     graphstate.LifeID
	At       temporal.Position
	Mode     graphstate.ReferenceMode
	Type     string
	Visible  graphstate.Visibility
}

type currentIncidentCounters struct {
	RawPages, RawDelivered, RawEndedDelivered int
	EntityCalls, LifeCalls, ValueCalls        int
	ComponentCalls, ComponentPiecesDelivered  int
	EndedComponentCalls, EndedValueCalls      int
	// Work is the actual production PageWork delta, NOT a count inferred from
	// method calls or output pieces. It may charge repeated materialization.
	RawActualWork, EndedComponentActualWork PageWork
}

type currentIncidentProbe struct {
	*ReadView
	Ended      func(graphstate.EntityID) bool
	Projecting graphstate.EntityID
	Counters   currentIncidentCounters
}

func (p *currentIncidentProbe) Entity(ctx context.Context, id graphstate.EntityID) (graphstate.EntityRead, error) {
	p.Counters.EntityCalls++
	return p.ReadView.Entity(ctx, id)
}
func (p *currentIncidentProbe) Life(ctx context.Context, id graphstate.EntityID, life graphstate.LifeID) (graphstate.LifeRead, error) {
	p.Counters.LifeCalls++
	return p.ReadView.Life(ctx, id, life)
}
func (p *currentIncidentProbe) Value(ctx context.Context, id graphstate.ValueID) (graphstate.ValueRead, error) {
	p.Counters.ValueCalls++
	if p.Ended(p.Projecting) {
		p.Counters.EndedValueCalls++
	}
	return p.ReadView.Value(ctx, id)
}
func (p *currentIncidentProbe) ComponentPage(ctx context.Context, query graphstate.ComponentQuery, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.ComponentPage, error) {
	p.Counters.ComponentCalls++
	ended := p.Ended(query.Key.Owner)
	before := p.Work()
	out, err := p.ReadView.ComponentPage(ctx, query, token, budget)
	if ended {
		p.Counters.EndedComponentCalls++
		p.Counters.EndedComponentActualWork = addWork(p.Counters.EndedComponentActualWork, currentWorkDelta(p.Work(), before))
	}
	// This is delivery evidence only; decoded cells come from PageWork above.
	p.Counters.ComponentPiecesDelivered += len(out.Data.Pieces())
	return out, err
}
func currentWorkDelta(after, before PageWork) PageWork {
	return PageWork{after.Records - before.Records, after.Bytes - before.Bytes, after.DirectoryPages - before.DirectoryPages, after.CheckpointPages - before.CheckpointPages, after.PatchPages - before.PatchPages, after.DecodedCells - before.DecodedCells}
}

// currentIncidentObserve is deliberately the slow baseline: retain raw
// candidates for Plan, then Project at the explicit point. One complete reader
// bounds the whole request; no allowance reset, scan, or early ten-result stop.
// Any refusal discards selected IDs and cannot be called a complete answer.
func currentIncidentObserve(ctx context.Context, p *currentIncidentProbe, query currentIncidentQuery) ([]graphstate.EntityID, bool, error) {
	if p == nil || p.ReadView == nil || query.Endpoint == 0 || query.Visible != graphstate.Declared && query.Visible != graphstate.Effective || query.Mode != 0 && query.Mode != graphstate.LifeBound && query.Mode != graphstate.IdentityReference {
		return nil, false, ErrInvalid
	}
	window, err := temporal.Point(query.At)
	if err != nil {
		return nil, false, err
	}
	var cursor graphstate.Cursor
	var last graphstate.EntityID
	ids := make([]graphstate.EntityID, 0, 16)
	for range 4096 {
		before := p.Work()
		page, err := p.IncidentRelationships(ctx, graphstate.IncidentPredicate{Endpoint: query.Endpoint, Life: query.Life, Window: window}, cursor, graphstate.ReadBudget{Rows: 256, Bytes: 4 << 20})
		p.Counters.RawPages++
		p.Counters.RawActualWork = addWork(p.Counters.RawActualWork, currentWorkDelta(p.Work(), before))
		if err != nil {
			return nil, false, err
		}
		for _, id := range page.Entities {
			if id <= last {
				return nil, false, ErrCorrupt
			}
			last = id
			p.Counters.RawDelivered++
			if p.Ended(id) {
				p.Counters.RawEndedDelivered++
			}
			record, err := p.Entity(ctx, id)
			if err != nil {
				return nil, false, err
			}
			if !record.Found || record.Record.Kind != graphstate.Relationship {
				return nil, false, ErrCorrupt
			}
			if record.Record.Axis.Descriptor() != query.At.Axis().Descriptor() || record.Record.Axis.DefinitionHash() != query.At.Axis().DefinitionHash() || query.Type != "" && record.Record.Type != query.Type || query.Mode != 0 && record.Record.Mode != query.Mode {
				continue
			}
			p.Projecting = id
			projection, err := graphstate.Project(ctx, p, id, query.At, query.Visible, p.limits.Planner)
			p.Projecting = 0
			if err != nil {
				return nil, false, err
			}
			if !projection.Active {
				continue
			}
			if record.Record.Mode == graphstate.LifeBound && query.Life != 0 {
				matched := false
				for _, endpoint := range projection.Endpoints {
					matched = matched || endpoint.ID == query.Endpoint && endpoint.BoundLife == query.Life
				}
				if !matched {
					continue
				}
			}
			if len(ids) == 64 {
				return nil, false, ErrResourceLimit
			}
			ids = append(ids, id)
		}
		if page.Complete {
			if page.Next != 0 {
				return nil, false, ErrCorrupt
			}
			return ids, true, nil
		}
		if page.Next == 0 || page.Next == cursor {
			return nil, false, ErrCorrupt
		}
		cursor = page.Next
	}
	return nil, false, ErrResourceLimit
}

type currentIncidentFixture struct {
	db                                                        *raftlog.Store
	config                                                    raftlog.Config
	root                                                      Root
	index                                                     uint64
	axis                                                      temporal.Axis
	digest                                                    hash.Hash
	BuildOperations, BuildBatches, BuildWrites, BuildRefusals int
	StageLimits                                               GraphLimits
	AcceptedBatchWidths                                       [17]int
	BuildWork                                                 PageWork
}

func newCurrentIncidentFixture(t testing.TB, fs vfs.FS, dir string) *currentIncidentFixture {
	t.Helper()
	policy := raftlog.DefaultApplicationPolicy(1)
	// Explicit fixture retention, independent of query allowances. The opt-in
	// disk workload may retain millions of MVCC page versions; not an RSS quota.
	policy.RetainedApplicationBytes = 8 << 30
	policy.RetainedApplicationRecords = 32_000_000
	config := raftlog.Config{Dir: dir, FS: fs, Create: true, Application: policy}
	db, err := raftlog.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	f := &currentIncidentFixture{db: db, config: config, index: 1, digest: sha256.New(), StageLimits: GraphLimits{Pages: PageLimits{MaxWorkRecords: 4096}}}
	t.Cleanup(func() {
		if err := f.db.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := BootstrapSinglePartition(db, testNamespace(), 3); err != nil {
		t.Fatal(err)
	}
	f.axis, err = temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "current-incident/valid-position", CanonicalUnit: "exact-unit"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	c, close := f.catalog(t, 1)
	f.root = c.root
	effects, err := InitializeGraphIndexes(t.Context(), c, []graphstate.PropertyDefinition{{Owner: graphstate.Relationship, Name: "payload", Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	close()
	f.install(t, effects)
	return f
}
func (f *currentIncidentFixture) catalog(t testing.TB, index uint64) (*Catalog, func()) {
	t.Helper()
	v, err := f.db.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCatalog(v, testNamespace(), 3, Limits{MaxReadRows: 4096})
	if err != nil {
		_ = v.Close()
		t.Fatal(err)
	}
	return c, func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}
}
func (f *currentIncidentFixture) position(t testing.TB, coordinate int64) temporal.Position {
	t.Helper()
	p, err := temporal.IntegerPosition(f.axis, temporal.Int64(coordinate))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (f *currentIncidentFixture) span(t testing.TB, lo, hi int64) temporal.Scope {
	t.Helper()
	lower, _ := temporal.FiniteBound(f.position(t, lo), true)
	upper, _ := temporal.FiniteBound(f.position(t, hi), false)
	w, err := temporal.Span(f.axis, lower, upper, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func (f *currentIncidentFixture) install(t testing.TB, e GraphEffects) {
	t.Helper()
	if e.Base.Index != f.index {
		t.Fatal("fixture used stale base")
	}
	root := e.Root
	if len(e.Groups)+len(e.Delta.Entities)+len(e.Delta.Lives)+len(e.Delta.Values) > 0 || root.topology != f.root.topology {
		var err error
		root, err = root.AdvanceEffects(sha256.Sum256(f.digest.Sum(nil)))
		if err != nil {
			t.Fatal(err)
		}
	}
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	index := f.index + 1
	entry := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("bounded current-incident fixture")}
	if err := f.db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: e.Base.Generation, BaseIndex: e.Base.Index, BaseImageHash: e.Base.ImageHash, Image: image, Writes: e.Writes, Changes: []byte("fixture ordered graph CDC"), Outcome: []byte("fixture outcome")}
	if _, err := f.db.ApplicationLimits().Preflight(batch, f.db.Limits()); err != nil {
		t.Fatal(err)
	}
	if err := f.db.InstallApplication(index, batch); err != nil {
		t.Fatal(err)
	}
	f.root, f.index = root, index
	f.BuildBatches++
	f.BuildWrites += len(e.Writes)
	f.BuildWork = addWork(f.BuildWork, e.Work)
}
func (f *currentIncidentFixture) apply(t testing.TB, ops ...graphstate.Operation) {
	t.Helper()
	if err := f.tryApply(t, ops); err != nil {
		t.Fatal("bounded fixture staging", f.index, err)
	}
}
func (f *currentIncidentFixture) tryApply(t testing.TB, ops []graphstate.Operation) error {
	t.Helper()
	if len(ops) > 16 {
		t.Fatal("fixture batch exceeded16 operations")
	}
	c, close := f.catalog(t, f.index)
	defer close()
	before, err := f.db.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(f.index+1, f.index+1000)
	effects, err := StageOperations(t.Context(), c, ops, revision, f.StageLimits)
	if c.stageBytes != 0 || c.fullViewBytes != 0 {
		t.Fatal("fixture leaked staging/read handles")
	}
	if err != nil {
		after, usageErr := f.db.ApplicationUsage()
		if usageErr != nil || after != before || c.root != f.root || !reflect.DeepEqual(effects, GraphEffects{}) {
			t.Fatal("construction refusal mutated fixture", err, usageErr)
		}
		if errors.Is(err, ErrResourceLimit) {
			f.BuildRefusals++
		}
		return err
	}
	for _, op := range ops {
		wire := fmt.Appendf(nil, "%d/%d/%d/%s/%d/%d/%d/%s/%d/%d/%t/%d\x00", op.Kind, op.Owner, op.Life, op.Name, op.Record.Source, op.Record.Target, op.Record.Mode, op.Record.Type, op.Binding.SourceLife, op.Binding.TargetLife, op.Present, op.ValueID)
		scope, err := temporal.AppendScope(nil, op.Scope, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		wire = appendField(wire, scope)
		if op.Value.Kind() != graphstate.ScalarInvalid {
			key, err := op.Value.EqualityKey(graphstate.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			wire = appendField(wire, []byte(key))
		}
		_, _ = f.digest.Write(wire)
	}
	f.BuildOperations += len(ops)
	f.install(t, effects)
	f.AcceptedBatchWidths[len(ops)]++
	return nil
}

// The unit keeps Create+Set together; Close units are one operation each.
// Pure resource refusal publishes nothing, so bounded smaller batches can retry.
func (f *currentIncidentFixture) applyBounded(t testing.TB, ops []graphstate.Operation, unit int) {
	t.Helper()
	if len(ops)%unit != 0 || len(ops) == 0 {
		t.Fatal("invalid fixture construction units")
	}
	err := f.tryApply(t, ops)
	if err == nil {
		return
	}
	if !errors.Is(err, ErrResourceLimit) || len(ops) == unit {
		t.Fatal("single fixture unit could not fit", f.index, err)
	}
	middle := (len(ops) / unit / 2) * unit
	f.applyBounded(t, ops[:middle], unit)
	f.applyBounded(t, ops[middle:], unit)
}
func (f *currentIncidentFixture) observe(t testing.TB, index uint64, q currentIncidentQuery, ended func(graphstate.EntityID) bool, l GraphLimits) ([]graphstate.EntityID, bool, currentIncidentCounters, PageWork, error) {
	t.Helper()
	c, close := f.catalog(t, index)
	defer close()
	v, err := OpenReadView(t.Context(), c, l)
	if err != nil {
		return nil, false, currentIncidentCounters{}, PageWork{}, err
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}()
	p := &currentIncidentProbe{ReadView: v, Ended: ended}
	ids, complete, err := currentIncidentObserve(t.Context(), p, q)
	return ids, complete, p.Counters, v.Work(), err
}
func currentAssertIDs(t testing.TB, got, want []graphstate.EntityID) {
	t.Helper()
	if !slices.IsSorted(got) || len(slices.Compact(slices.Clone(got))) != len(got) || !slices.Equal(got, want) {
		t.Fatal("wrong exact incident point set", got, want)
	}
}

func TestCurrentIncidentPointContractCorrectionsEndpointMaskAndHistoricalView(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "current-contract")
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: whole})
	for _, tc := range []struct {
		id, source, target graphstate.EntityID
		mode               graphstate.ReferenceMode
	}{{3, 1, 2, graphstate.LifeBound}, {4, 1, 2, graphstate.LifeBound}, {5, 1, 1, graphstate.LifeBound}, {6, 1, 2, graphstate.IdentityReference}} {
		binding := graphstate.LifeRecord{}
		if tc.mode == graphstate.LifeBound {
			binding.SourceLife = 11
			binding.TargetLife = 11
		}
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: tc.id, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: tc.source, Target: tc.target, Mode: tc.mode}, Binding: binding}, graphstate.Operation{Kind: graphstate.Set, Owner: tc.id, Life: 31, Scope: whole, Name: "payload", Value: graphstate.String("retained payload"), ValueID: graphstate.ValueID(99 + tc.id)})
	}
	before := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: f.span(t, 50, 100)}, graphstate.Operation{Kind: graphstate.Close, Owner: 2, Life: 11, Scope: f.span(t, 50, 100)})
	masked := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 2, Life: 11, Scope: f.span(t, 70, 80), Present: true}, graphstate.Operation{Kind: graphstate.Correct, Owner: 3, Life: 31, Scope: f.span(t, 70, 80), Present: true}, graphstate.Operation{Kind: graphstate.Correct, Owner: 4, Life: 31, Scope: f.span(t, 70, 80), Present: false})
	corrected := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 2, Life: 11, Scope: f.span(t, 70, 80)}, graphstate.Operation{Kind: graphstate.Reopen, Owner: 2, Life: 12, Scope: f.span(t, 70, 80)})
	for _, tc := range []struct {
		index   uint64
		visible graphstate.Visibility
		want    []graphstate.EntityID
	}{{before, graphstate.Declared, []graphstate.EntityID{3, 4, 5, 6}}, {before, graphstate.Effective, []graphstate.EntityID{3, 4, 5, 6}}, {masked, graphstate.Declared, []graphstate.EntityID{4, 5, 6}}, {masked, graphstate.Effective, []graphstate.EntityID{5, 6}}, {corrected, graphstate.Effective, []graphstate.EntityID{3, 5, 6}}, {f.index, graphstate.Declared, []graphstate.EntityID{3, 5, 6}}, {f.index, graphstate.Effective, []graphstate.EntityID{5, 6}}} {
		q := currentIncidentQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Visible: tc.visible, Type: "R"}
		ids, complete, _, _, err := f.observe(t, tc.index, q, func(id graphstate.EntityID) bool { return id == 3 }, GraphLimits{})
		if err != nil || !complete {
			t.Fatal(err)
		}
		currentAssertIDs(t, ids, tc.want)
	}
	q := currentIncidentQuery{Endpoint: 1, Life: 11, At: f.position(t, 25), Visible: graphstate.Effective}
	ids, complete, _, _, err := f.observe(t, f.index, q, func(graphstate.EntityID) bool { return false }, GraphLimits{})
	if err != nil || !complete {
		t.Fatal(err)
	}
	currentAssertIDs(t, ids, []graphstate.EntityID{3, 4, 5, 6})
	q.Type = "phantom"
	ids, complete, _, _, err = f.observe(t, f.index, q, func(graphstate.EntityID) bool { return false }, GraphLimits{})
	if err != nil || !complete || len(ids) != 0 {
		t.Fatal("phantom type was not exact-empty", ids, err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.config.Create = false
	f.db, err = raftlog.Open(f.config)
	if err != nil {
		t.Fatal(err)
	}
	q.Type = "R"
	q.At = f.position(t, 75)
	ids, complete, _, _, err = f.observe(t, corrected, q, func(graphstate.EntityID) bool { return false }, GraphLimits{})
	if err != nil || !complete {
		t.Fatal(err)
	}
	currentAssertIDs(t, ids, []graphstate.EntityID{3, 5, 6})
}

func TestCurrentIncidentRefusalHasNoCompleteOrPartialAnswer(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "current-refusal")
	w := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: w})
	for id := range 4 {
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: graphstate.EntityID(id + 3), Life: 31, Scope: w, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	}
	q := currentIncidentQuery{Endpoint: 1, At: f.position(t, 75), Visible: graphstate.Declared}
	ids, complete, _, work, err := f.observe(t, f.index, q, func(graphstate.EntityID) bool { return false }, GraphLimits{MaxSourceRows: 30})
	if !errors.Is(err, ErrResourceLimit) || complete || len(ids) != 0 || work.Records <= 5 {
		t.Fatal("resource cliff was published as a partial/complete answer", ids, complete, work, err)
	}
	if _, _, err := currentIncidentObserve(t.Context(), nil, q); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := f.db.ScrubApplication(t.Context()); err != nil {
		t.Fatal("query refusal altered fixture", err)
	}
}

func TestCurrentIncidentExplicitRationalAxisWideEndedPresenceAndValueDelivery(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "current-wide")
	z := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: z}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: z})
	axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{2}, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "another-valid-axis", CanonicalUnit: "rational-unit"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	positions := make([]temporal.Position, 0, 4)
	for _, text := range []string{"-340282366920938463463374607431768211457", "340282366920938463463374607431768211457", "0", "1"} {
		n, err := temporal.ParseInteger(text, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		r, err := temporal.Fraction(n, temporal.Int64(3), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		p, err := temporal.RationalPosition(axis, r)
		if err != nil {
			t.Fatal(err)
		}
		positions = append(positions, p)
	}
	span := func(lower, upper temporal.Position) temporal.Scope {
		lo, _ := temporal.FiniteBound(lower, true)
		hi, _ := temporal.FiniteBound(upper, false)
		w, err := temporal.Span(axis, lo, hi, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	w := span(positions[0], positions[1])
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 7, Life: 31, Scope: w, Record: graphstate.EntityRecord{Axis: axis, Type: "R", Source: 1, Target: 2, Mode: graphstate.IdentityReference}}, graphstate.Operation{Kind: graphstate.Set, Owner: 7, Life: 31, Scope: w, Name: "payload", Value: graphstate.String(strings.Repeat("wide", 1024)), ValueID: 99})
	before := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 7, Life: 31, Scope: span(positions[2], positions[1])})
	q := currentIncidentQuery{Endpoint: 1, Life: 11, At: positions[3], Visible: graphstate.Effective, Mode: graphstate.IdentityReference}
	ids, complete, counters, _, err := f.observe(t, before, q, func(graphstate.EntityID) bool { return false }, GraphLimits{})
	if err != nil || !complete || counters.ValueCalls == 0 {
		t.Fatal("old wide payload did not remain queryable", err, counters)
	}
	currentAssertIDs(t, ids, []graphstate.EntityID{7})
	ids, complete, counters, _, err = f.observe(t, f.index, q, func(id graphstate.EntityID) bool { return id == 7 }, GraphLimits{})
	if err != nil || !complete || len(ids) != 0 || counters.EndedValueCalls != 0 || counters.EndedComponentActualWork.DecodedCells == 0 {
		t.Fatal("wide ended presence/value evidence conflated", ids, counters, err)
	}
	q.At = f.position(t, 75)
	ids, complete, _, _, err = f.observe(t, before, q, func(graphstate.EntityID) bool { return false }, GraphLimits{})
	if err != nil || !complete || len(ids) != 0 {
		t.Fatal("explicit relationship-axis filter substituted another axis", ids, err)
	}
}

func TestCurrentIncidentConstructionBackpressureKeepsAcceptedDigestAndRoot(t *testing.T) {
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "current-backpressure")
	w := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: w})
	f.StageLimits.MaxSourceRows = 200
	ops := make([]graphstate.Operation, 0, 8)
	for i := range 4 {
		id := graphstate.EntityID(i + 3)
		ops = append(ops, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: w, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}}, graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: 31, Scope: w, Name: "payload", Value: graphstate.String("p"), ValueID: graphstate.ValueID(100 + i)})
	}
	digest := f.digest.Sum(nil)
	root, index, accepted := f.root, f.index, f.BuildOperations
	if err := f.tryApply(t, ops); !errors.Is(err, ErrResourceLimit) || !slices.Equal(digest, f.digest.Sum(nil)) || f.root != root || f.index != index || f.BuildOperations != accepted {
		t.Fatal("refused construction changed accepted identity/digest", err)
	}
	f.applyBounded(t, ops, 2)
	if f.BuildRefusals == 0 || f.BuildOperations != accepted+len(ops) || f.AcceptedBatchWidths[2]+f.AcceptedBatchWidths[4] == 0 {
		t.Fatal("smaller unit retry was not recorded", f.BuildRefusals, f.AcceptedBatchWidths)
	}
	ids, complete, _, _, err := f.observe(t, f.index, currentIncidentQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Visible: graphstate.Declared}, func(graphstate.EntityID) bool { return false }, GraphLimits{})
	if err != nil || !complete {
		t.Fatal(err)
	}
	currentAssertIDs(t, ids, []graphstate.EntityID{3, 4, 5, 6})
}
