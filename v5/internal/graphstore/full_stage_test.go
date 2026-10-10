package graphstore

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type fullFixture struct {
	db     *raftlog.Store
	root   Root
	index  uint64
	axis   temporal.Axis
	limits GraphLimits
	hash   func(string) [32]byte
}

func fullSchemas() []graphstate.PropertyDefinition {
	out := []graphstate.PropertyDefinition{}
	for _, owner := range []graphstate.EntityKind{graphstate.Node, graphstate.Relationship} {
		out = append(out, graphstate.PropertyDefinition{Name: "scalar", Owner: owner, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}, graphstate.PropertyDefinition{Name: "set", Owner: owner, Type: graphstate.ScalarI64, Cardinality: graphstate.SetCardinality, Unique: graphstate.UniqueMembers}, graphstate.PropertyDefinition{Name: "ordinary", Owner: owner, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality})
	}
	return out
}
func newFullFixture(t *testing.T, l GraphLimits) *fullFixture {
	t.Helper()
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	root := bootstrapRoot(t, db)
	f := &fullFixture{db: db, root: root, index: 1, axis: testAxis(t, 1, temporal.ProfileIntegerZ), limits: l}
	c := f.catalog(t, f.index)
	defer c.view.Close()
	effects, err := InitializeGraphIndexes(t.Context(), c, fullSchemas(), l)
	if err != nil {
		t.Fatal(err)
	}
	if c.fullViews != 0 || c.stages != 0 || c.records != 0 || c.stageBytes != 0 {
		t.Fatal("initializer leaked handles")
	}
	if effects.Root.next != 6 || effects.Root.epoch != 0 || effects.Root.effect != root.effect {
		t.Fatal("initialization lacks five real roots or advanced semantics")
	}
	f.install(t, effects)
	return f
}
func (f *fullFixture) catalog(t *testing.T, index uint64) *Catalog {
	t.Helper()
	c := openCatalog(t, f.db, index, Limits{})
	if f.hash != nil {
		c.hash = f.hash
	}
	return c
}
func (f *fullFixture) install(t *testing.T, e GraphEffects) {
	t.Helper()
	index, image, err := f.db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if index != e.Base.Index || sha256.Sum256(image) != e.Base.ImageHash {
		t.Fatal("assembler used wrong actual base")
	}
	root := e.Root
	if root.topology != f.root.topology || len(e.Delta.Entities)+len(e.Delta.Lives)+len(e.Delta.Values)+len(e.Groups) > 0 {
		root, err = root.AdvanceEffects(sha256.Sum256([]byte{byte(index), 42}))
		if err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	next := index + 1
	entry := &pb.Entry{Index: new(next), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("fully validated private fixture operations")}
	if err := f.db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(next)}, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: e.Base.Generation, BaseIndex: e.Base.Index, BaseImageHash: e.Base.ImageHash, Image: encoded, Writes: e.Writes, Changes: []byte("fixture complete graph CDC"), Outcome: []byte("fixture outcome")}
	if _, err := f.db.ApplicationLimits().Preflight(batch, f.db.Limits()); err != nil {
		t.Fatal(err)
	}
	if err := f.db.InstallApplication(next, batch); err != nil {
		t.Fatal(err)
	}
	f.root, f.index = root, next
}
func (f *fullFixture) span(t *testing.T, lo, hi int64) temporal.Scope {
	t.Helper()
	a, _ := temporal.IntegerPosition(f.axis, temporal.Int64(lo))
	b, _ := temporal.IntegerPosition(f.axis, temporal.Int64(hi))
	lower, _ := temporal.FiniteBound(a, true)
	upper, _ := temporal.FiniteBound(b, false)
	scope, err := temporal.Span(f.axis, lower, upper, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return scope
}
func (f *fullFixture) apply(t *testing.T, ops ...graphstate.Operation) GraphEffects {
	t.Helper()
	c := f.catalog(t, f.index)
	defer c.view.Close()
	revision, _ := state.NewRevision(f.index+1, f.index+1000)
	effects, err := StageOperations(t.Context(), c, ops, revision, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	if c.fullViews != 0 || c.fullViewBytes != 0 || c.stages != 0 || c.stageBytes != 0 || c.records != 0 {
		t.Fatal("operation helper leaked handles/staging")
	}
	if effects.Base.Index != f.index || effects.Root.epoch != f.root.epoch || effects.Root.effect != f.root.effect {
		t.Fatal("stager advanced semantics or changed base")
	}
	f.install(t, effects)
	return effects
}
func (f *fullFixture) projection(t *testing.T, index uint64, id graphstate.EntityID, at int64, visibility graphstate.Visibility) graphstate.Projection {
	t.Helper()
	c := f.catalog(t, index)
	defer c.view.Close()
	position, _ := temporal.IntegerPosition(f.axis, temporal.Int64(at))
	projection, work, err := Project(t.Context(), c, id, position, visibility, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	if work.Records == 0 || c.fullViews != 0 || c.fullViewBytes != 0 {
		t.Fatal("projection leaked handle or hid work")
	}
	return projection
}
func TestFullStagePlanInstallProjectAndNonUniqueAvoidsPostings(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	effects := f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "a\x00"}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "ordinary", Value: graphstate.I64(7), ValueID: 99})
	if len(effects.Delta.Entities) != 1 || len(effects.Delta.Lives) != 1 || len(effects.Delta.Values) != 1 || len(effects.Groups) != 3 {
		t.Fatal("incomplete owned graph effects", effects.Delta)
	}
	projection := f.projection(t, f.index, 1, 5, graphstate.Declared)
	if !projection.Exists || !projection.Active || projection.Life != 11 || !reflect.DeepEqual(projection.Labels, []string{"a\x00"}) || len(projection.Properties) != 1 || projection.Properties[0].Name != "ordinary" {
		t.Fatal("projection diverges from installed plan", projection)
	}
	c := f.catalog(t, f.index)
	q, _ := c.reader(t.Context())
	d, found, err := q.fullDescriptor(c.root)
	if err != nil || !found || d.unique.count != 0 || d.keys.count != 3 {
		t.Fatal("nonunique data paid uniqueness index cost", d, err)
	}
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	actual, err := v.ValueIdentity(t.Context(), graphstate.I64(7))
	if err != nil || !actual.Found || actual.ID != 99 {
		t.Fatal(actual, err)
	}
	for _, id := range []graphstate.EntityID{1, 2} {
		r, err := v.Entity(t.Context(), id)
		if err != nil || r.View != v.Identity() || r.Found != (id == 1) {
			t.Fatal(r, err)
		}
	}
	if _, err := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: fullSchemas()[2], Value: graphstate.I64(7), Window: scope}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nonunique predicate falsely reported complete coverage", err)
	}
}
func TestFullStageCallerAxesAreNotRepaired(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	other := testAxis(t, 2, temporal.ProfileIntegerZ)
	for _, kind := range []graphstate.OperationKind{graphstate.CreateNode, graphstate.CreateRelationship} {
		c := f.catalog(t, f.index)
		revision, _ := state.NewRevision(1, 0)
		op := graphstate.Operation{Kind: kind, Owner: 1, Life: 11, Scope: scope, Record: graphstate.EntityRecord{Axis: other, Type: "R", Source: 2, Target: 3, Mode: graphstate.LifeBound}}
		effects, err := StageOperations(t.Context(), c, []graphstate.Operation{op}, revision, GraphLimits{})
		if !errors.Is(err, temporal.ErrAxisMismatch) || !reflect.DeepEqual(effects, GraphEffects{}) || c.fullViews != 0 || c.stages != 0 || c.records != 0 {
			t.Fatal("invalid caller axis was normalized or leaked effects", kind, effects, err)
		}
	}
}

func (f *fullFixture) view(t *testing.T, index uint64) *ReadView {
	t.Helper()
	c := f.catalog(t, index)
	v, err := OpenReadView(t.Context(), c, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close(); _ = c.view.Close() })
	return v
}
func fullReadKeys(t *testing.T, v *ReadView, p graphstate.KeyPredicate) []graphstate.ComponentKey {
	t.Helper()
	var out []graphstate.ComponentKey
	var token graphstate.Cursor
	for range 100 {
		page, err := v.ComponentKeys(t.Context(), p, token, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, page.Keys...)
		if page.Complete {
			if page.Next != 0 {
				t.Fatal("terminal token")
			}
			return out
		}
		if page.Next == 0 || page.Next == token {
			t.Fatal("stalled key cursor")
		}
		token = page.Next
	}
	t.Fatal("key pagination failed")
	return nil
}
func fullReadClaims(t *testing.T, v *ReadView, p graphstate.UniquePredicate, budget graphstate.ReadBudget) []graphstate.UniqueClaim {
	t.Helper()
	var out []graphstate.UniqueClaim
	var token graphstate.Cursor
	for range 100 {
		page, err := v.UniqueCandidates(t.Context(), p, token, budget)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, page.Claims...)
		if page.Complete {
			return out
		}
		if page.Next == 0 || page.Next == token {
			t.Fatal("stalled claim cursor")
		}
		token = page.Next
	}
	t.Fatal("claim pagination failed")
	return nil
}
func fullReadIncidents(t *testing.T, v *ReadView, p graphstate.IncidentPredicate, budget graphstate.ReadBudget) []graphstate.EntityID {
	t.Helper()
	var out []graphstate.EntityID
	var token graphstate.Cursor
	for range 100 {
		page, err := v.IncidentRelationships(t.Context(), p, token, budget)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, page.Entities...)
		if page.Complete {
			return out
		}
		if page.Next == 0 || page.Next == token {
			t.Fatal("stalled incident cursor")
		}
		token = page.Next
	}
	t.Fatal("incident pagination failed")
	return nil
}
func TestFullHistoryReopenCorrectionAndRawClaimRetention(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	emptyIndex := f.index
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "old"}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "scalar", Value: graphstate.I64(7), ValueID: 99})
	before := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 11, Scope: f.span(t, 5, 10)})
	closed := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Reopen, Owner: 1, Life: 12, Scope: f.span(t, 5, 10)}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 12, Scope: f.span(t, 5, 10), Name: "new"}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 12, Scope: f.span(t, 5, 10), Name: "scalar", Value: graphstate.I64(8), ValueID: 100})
	reopened := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Correct, Owner: 1, Life: 11, Scope: f.span(t, 2, 3), Present: false})
	if p := f.projection(t, before, 1, 7, graphstate.Declared); !p.Active || p.Life != 11 || !reflect.DeepEqual(p.Labels, []string{"old"}) {
		t.Fatal("old view read reopened state", p)
	}
	if p := f.projection(t, closed, 1, 7, graphstate.Declared); p.Active {
		t.Fatal("closed history read current state", p)
	}
	if p := f.projection(t, reopened, 1, 2, graphstate.Declared); !p.Active || p.Life != 11 {
		t.Fatal(p)
	}
	if p := f.projection(t, f.index, 1, 2, graphstate.Declared); p.Active {
		t.Fatal("historical correction failed", p)
	}
	if p := f.projection(t, f.index, 1, 7, graphstate.Declared); !p.Active || p.Life != 12 || !reflect.DeepEqual(p.Labels, []string{"new"}) {
		t.Fatal(p)
	}
	old := f.view(t, emptyIndex)
	beforeWork := old.Work()
	keys := fullReadKeys(t, old, graphstate.KeyPredicate{Owner: 1})
	if len(keys) != 0 || old.Work().Records-beforeWork.Records != 1 {
		t.Fatal("late-opened empty root visited future pages", keys, old.Work())
	}
	_ = old.Close()
	_ = old.c.view.Close()
	v := f.view(t, f.index)
	keys = fullReadKeys(t, v, graphstate.KeyPredicate{Owner: 1, Life: 11})
	want := []graphstate.ComponentKey{{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "old"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}}
	if !reflect.DeepEqual(keys, want) {
		t.Fatal("closed life keys disappeared", keys)
	}
	claims := fullReadClaims(t, v, graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.I64(7), Window: scope}, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if !reflect.DeepEqual(claims, []graphstate.UniqueClaim{{Owner: 1, Life: 11, Key: want[1]}}) {
		t.Fatal("raw closed claim omitted", claims)
	}
}
func TestFullUniquenessUsesFinalOverlayAndEndpointRestoration(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: scope})
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: scope, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}}, graphstate.Operation{Kind: graphstate.Set, Owner: 3, Life: 31, Scope: scope, Name: "scalar", Value: graphstate.I64(7), ValueID: 99})
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 11, Scope: f.span(t, 5, 10)})
	maskedIndex := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 4, Life: 41, Scope: f.span(t, 5, 10), Record: graphstate.EntityRecord{Type: "R", Source: 2, Target: 2, Mode: graphstate.IdentityReference}}, graphstate.Operation{Kind: graphstate.Set, Owner: 4, Life: 41, Scope: f.span(t, 5, 10), Name: "scalar", Value: graphstate.I64(7), ValueID: 100})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	revision, _ := state.NewRevision(100, 0)
	beforeRoot := c.root
	effects, err := StageOperations(t.Context(), c, []graphstate.Operation{{Kind: graphstate.Correct, Owner: 1, Life: 11, Scope: f.span(t, 5, 10), Present: true}}, revision, GraphLimits{})
	if !errors.Is(err, graphstate.ErrUniqueOverlap) || !reflect.DeepEqual(effects, GraphEffects{}) || c.root != beforeRoot || c.fullViews != 0 || c.stages != 0 {
		t.Fatal("endpoint restoration bypassed raw uniqueness or leaked effects", effects, err)
	}
	if p := f.projection(t, maskedIndex, 3, 7, graphstate.Declared); !p.Active {
		t.Fatal("declared relationship lost endpoint-independent presence", p)
	}
	if p := f.projection(t, f.index, 3, 7, graphstate.Effective); p.Active {
		t.Fatal("masked relationship became effective", p)
	}
	// A final-overlay replacement clears the conflict before restoring the endpoint.
	f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 4, Life: 41, Scope: f.span(t, 5, 10), Name: "scalar", Value: graphstate.I64(8), ValueID: 101}, graphstate.Operation{Kind: graphstate.Correct, Owner: 1, Life: 11, Scope: f.span(t, 5, 10), Present: true})
	if p := f.projection(t, f.index, 3, 7, graphstate.Effective); !p.Active {
		t.Fatal("valid final overlay restoration failed", p)
	}
}

func TestFullIncidentsParallelSelfLoopsLifeRunsAndHistoricalRoots(t *testing.T) {
	f := newFullFixture(t, GraphLimits{Pages: PageLimits{MaxCells: 3, MaxChildren: 3}})
	empty := f.index
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: scope})
	for _, test := range []struct {
		id             graphstate.EntityID
		source, target graphstate.EntityID
		mode           graphstate.ReferenceMode
	}{{3, 1, 2, graphstate.LifeBound}, {4, 1, 2, graphstate.LifeBound}, {5, 2, 2, graphstate.LifeBound}, {6, 2, 2, graphstate.IdentityReference}} {
		binding := graphstate.LifeRecord{}
		if test.mode == graphstate.LifeBound {
			binding.SourceLife = 11
			binding.TargetLife = 11
		}
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: test.id, Life: graphstate.LifeID(test.id*10 + 1), Scope: scope, Record: graphstate.EntityRecord{Type: "R", Source: test.source, Target: test.target, Mode: test.mode}, Binding: binding})
	}
	first := f.index
	smallRun := uint64(0)
	for i := range 79 {
		f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: graphstate.LifeID(31 + i), Scope: scope}, graphstate.Operation{Kind: graphstate.Reopen, Owner: 3, Life: graphstate.LifeID(32 + i), Scope: scope, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
		if i == 14 {
			smallRun = f.index
		}
	}
	predicate := graphstate.IncidentPredicate{Endpoint: 2, Life: 11, Window: scope}
	want := []graphstate.EntityID{3, 4, 5, 6}
	measurements := []PageWork{}
	for _, index := range []uint64{first, smallRun, f.index} {
		v := f.view(t, index)
		before := v.Work()
		got := fullReadIncidents(t, v, predicate, graphstate.ReadBudget{Rows: 128, Bytes: 4 << 20})
		if !reflect.DeepEqual(got, want) {
			t.Fatal("parallel/selfloop/life multiplicity changed", index, got)
		}
		measurements = append(measurements, PageWork{Records: v.Work().Records - before.Records, DirectoryPages: v.Work().DirectoryPages - before.DirectoryPages})
		r, err := v.Entity(t.Context(), 3)
		if err != nil || r.Record.Source != 1 || r.Record.Target != 2 || r.Record.Mode != graphstate.LifeBound {
			t.Fatal("immutable endpoints changed", r, err)
		}
		life, err := v.Life(t.Context(), 3, 31)
		if err != nil || !life.Found || life.Record.SourceLife != 11 || life.Record.TargetLife != 11 {
			t.Fatal("old binding vanished", life, err)
		}
		_ = v.Close()
		_ = v.c.view.Close()
	}
	// A per-life traversal adds at least64 row/page visits from16 to80 lives.
	if measurements[2].Records-measurements[1].Records > 24 || measurements[2].DirectoryPages-measurements[1].DirectoryPages > 24 {
		t.Fatal("large duplicate-life run decoded history instead of skipping subtree fences", measurements)
	}
	old := f.view(t, empty)
	before := old.Work()
	got := fullReadIncidents(t, old, predicate, graphstate.ReadBudget{Rows: 128, Bytes: 4 << 20})
	if len(got) != 0 || old.Work().Records-before.Records != 2 {
		t.Fatal("old empty root visited future incident history", got, old.Work())
	}
	current := f.view(t, f.index)
	if got := fullReadIncidents(t, current, graphstate.IncidentPredicate{Endpoint: 2, Life: 99, Window: scope}, graphstate.ReadBudget{Rows: 128, Bytes: 4 << 20}); !reflect.DeepEqual(got, []graphstate.EntityID{6}) {
		t.Fatal("identity-reference access incorrectly depended on bound life", got)
	}
}
func TestFullUniquenessSwapAndAliasesUnderForcedCollisions(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	f.hash = func(string) [32]byte { return [32]byte{42} }
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: scope})
	f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "scalar", Value: graphstate.I64(7), ValueID: 99}, graphstate.Operation{Kind: graphstate.Set, Owner: 2, Life: 11, Scope: scope, Name: "scalar", Value: graphstate.I64(8), ValueID: 100})
	// A complete private fixture adds an immutable equal-content dictionary alias,
	// then stores a raw historical claim under that alias while maintaining Full
	// key/uniqueness structures. The production API still accepts operations only.
	c := f.catalog(t, f.index)
	v, err := OpenReadView(t.Context(), c, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newFullStage(t.Context(), c, v.descriptor, v.limits.Pages)
	if err != nil {
		t.Fatal(err)
	}
	var root Root
	err = s.operation(t.Context(), func(base *reader) error {
		p := pageStage{&pageReader{q: base, limits: v.limits.Pages}, c.root}
		p.allocation = &p.root
		key, err := p.equalityKey(graphstate.I64(7))
		if err != nil {
			return err
		}
		if err := base.stageValue(refValue(101), graphstate.I64(7), key); err != nil {
			return err
		}
		component := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}
		m, found, err := p.readMeta(component)
		if err != nil || !found {
			return err
		}
		leaf, err := p.findLeaf(m, scope)
		if err != nil {
			return err
		}
		current, err := p.materialize(leaf)
		if err != nil {
			return err
		}
		alias, _ := state.NewValueRef(101, 9)
		patch := pagePatch(t, component, current, scope, alias, 1000, false)
		if _, err := p.validatePatch(patch); err != nil {
			return err
		}
		if err := p.stageRaw(patch); err != nil {
			return err
		}
		if err := p.installPatch(patch); err != nil {
			return err
		}
		base.full.root = p.root
		wire, err := encodeFullDescriptor(base.full.descriptor, p.root, c.limits)
		if err != nil {
			return err
		}
		root = p.root
		return base.put(componentIndexDescriptorKey(p.root.namespace), wire)
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	base, err := c.view.Root()
	if err != nil {
		t.Fatal(err)
	}
	f.install(t, GraphEffects{Base: base, Root: root, Writes: rows})
	_ = s.Close()
	_ = v.Close()
	_ = c.view.Close()
	v = f.view(t, f.index)
	canonical, err := v.ValueIdentity(t.Context(), graphstate.I64(7))
	if err != nil || !canonical.Found || canonical.ID != 99 {
		t.Fatal("alias/collision changed canonical identity", canonical, err)
	}
	alias, err := v.Value(t.Context(), 101)
	if err != nil || !alias.Found || alias.ID != 101 {
		t.Fatal(alias, err)
	}
	claims := fullReadClaims(t, v, graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.I64(7), Window: scope}, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if len(claims) != 1 || claims[0].Owner != 1 {
		t.Fatal("alias bypassed raw typed candidate range", claims)
	}
	_ = v.Close()
	_ = v.c.view.Close()
	c = f.catalog(t, f.index)
	defer c.view.Close()
	revision, _ := state.NewRevision(1001, 0)
	if out, err := StageOperations(t.Context(), c, []graphstate.Operation{{Kind: graphstate.Set, Owner: 2, Life: 11, Scope: scope, Name: "scalar", Value: graphstate.I64(7), ValueID: 102}}, revision, GraphLimits{}); !errors.Is(err, graphstate.ErrUniqueOverlap) || !reflect.DeepEqual(out, GraphEffects{}) {
		t.Fatal("equal-content alias escaped uniqueness", out, err)
	}
	// Same-transaction swap sees the complete final overlay, never intermediate conflicts.
	beforeSwap := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "scalar", Value: graphstate.I64(8), ValueID: 103}, graphstate.Operation{Kind: graphstate.Set, Owner: 2, Life: 11, Scope: scope, Name: "scalar", Value: graphstate.I64(7), ValueID: 104})
	for _, test := range []struct {
		index uint64
		owner graphstate.EntityID
		value int64
	}{{beforeSwap, 1, 7}, {beforeSwap, 2, 8}, {f.index, 1, 8}, {f.index, 2, 7}} {
		p := f.projection(t, test.index, test.owner, 5, graphstate.Declared)
		if len(p.Properties) != 1 || p.Properties[0].Name != "scalar" {
			t.Fatal("swap changed exact property set", p)
		}
		value, ok := p.Properties[0].Scalar.Int64Value()
		if !ok || value != test.value {
			t.Fatal("swap was no-op or historical read substituted current value", test, p)
		}
	}
}

func TestFullAllScalarKindsAndSetMutationOwnExactTypedValues(t *testing.T) {
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	root := bootstrapRoot(t, db)
	f := &fullFixture{db: db, root: root, index: 1, axis: testAxis(t, 1, temporal.ProfileIntegerZ)}
	kinds := []graphstate.ScalarKind{graphstate.ScalarString, graphstate.ScalarBool, graphstate.ScalarI64, graphstate.ScalarF64, graphstate.ScalarScope}
	names := []string{"text", "bool", "i64", "f64", "scope"}
	definitions := []graphstate.PropertyDefinition{}
	for i, kind := range kinds {
		definitions = append(definitions, graphstate.PropertyDefinition{Name: names[i], Owner: graphstate.Node, Type: kind, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar})
	}
	definitions = append(definitions, graphstate.PropertyDefinition{Name: "members", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.SetCardinality, Unique: graphstate.UniqueMembers})
	c := f.catalog(t, 1)
	effects, err := InitializeGraphIndexes(t.Context(), c, definitions, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	f.install(t, effects)
	_ = c.view.Close()
	scope := f.span(t, 0, 10)
	float, _ := graphstate.F64(1.25)
	values := []graphstate.Scalar{graphstate.String("a\x00å"), graphstate.Bool(true), graphstate.I64(-2), float, graphstate.ScopeValue(f.span(t, 2, 3))}
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope})
	for i, value := range values {
		out := f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: names[i], Value: value, ValueID: graphstate.ValueID(99 + i)})
		if out.OwnedBytes <= 0 {
			t.Fatal("computed owned effect accounting omitted")
		}
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Add, Owner: 1, Life: 11, Scope: scope, Name: "members", Value: graphstate.String("a\x00"), ValueID: 200}, graphstate.Operation{Kind: graphstate.Add, Owner: 1, Life: 11, Scope: scope, Name: "members", Value: graphstate.String("😀"), ValueID: 201})
	before := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Remove, Owner: 1, Life: 11, Scope: scope, Name: "members", Value: graphstate.String("a\x00"), ValueID: 202}, graphstate.Operation{Kind: graphstate.Unset, Owner: 1, Life: 11, Scope: f.span(t, 5, 10), Name: "text"})
	for _, index := range []uint64{before, f.index} {
		v := f.view(t, index)
		for i, value := range values {
			dictionary, err := v.ValueIdentity(t.Context(), value)
			if err != nil || !dictionary.Found || dictionary.ID != graphstate.ValueID(99+i) {
				t.Fatal("typed value identity mismatch", i, dictionary, err)
			}
			got, err := v.Value(t.Context(), dictionary.ID)
			if err != nil {
				t.Fatal(err)
			}
			equal, err := got.Value.Equal(value, graphstate.Limits{})
			if err != nil || !equal {
				t.Fatal("typed value lost exact content", got, err)
			}
		}
		_ = v.Close()
		_ = v.c.view.Close()
		p := f.projection(t, index, 1, 7, graphstate.Declared)
		members := []string{}
		textPresent := false
		for _, property := range p.Properties {
			if property.Name == "text" {
				textPresent = true
			}
			if property.Name == "members" {
				for _, value := range property.Members {
					text, ok := value.StringValue()
					if !ok {
						t.Fatal(value)
					}
					members = append(members, text)
				}
			}
		}
		want := []string{"😀"}
		if index == before {
			want = []string{"a\x00", "😀"}
		}
		if !reflect.DeepEqual(members, want) || textPresent != (index == before) {
			t.Fatal("remove/unset history substituted current or phantom values", index, members, textPresent)
		}
	}
	v := f.view(t, f.index)
	claims := fullReadClaims(t, v, graphstate.UniquePredicate{Definition: definitions[len(definitions)-1], Value: graphstate.String("a\x00"), Window: scope}, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	assertFullClaimSet(t, claims, []graphstate.UniqueClaim{{Owner: 1, Life: 11, Key: graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "members", Member: 200}}})
}
func TestFullEffectOwnedBytesExactBoundaryAndSharedBuffers(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	revision, _ := state.NewRevision(100, 0)
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: f.span(t, 0, 10)}}
	out, err := StageOperations(t.Context(), c, ops, revision, GraphLimits{})
	if err != nil || out.OwnedBytes <= 0 {
		t.Fatal(out, err)
	}
	if len(out.Dependencies) > 0 && &out.Dependencies[0] != &out.Delta.Dependencies[0] {
		t.Fatal("dependency sharing contract changed")
	}
	if len(out.Groups) > 0 && len(out.Groups[0].Changes) > 0 && &out.Groups[0].Changes[0] != &out.Delta.Patches[0].Changes[0] {
		t.Fatal("CDC sharing contract changed")
	}
	measured := out.OwnedBytes
	refused, err := StageOperations(t.Context(), c, ops, revision, GraphLimits{MaxOutputBytes: measured - 1})
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(refused, GraphEffects{}) || c.fullViews != 0 || c.stages != 0 {
		t.Fatal("one-short owned output published graph effects", refused, err)
	}
	exact, err := StageOperations(t.Context(), c, ops, revision, GraphLimits{MaxOutputBytes: measured})
	if err != nil || exact.OwnedBytes != measured {
		t.Fatal("exact retained-output allowance refused", exact.OwnedBytes, measured, err)
	}
}

func TestFullStageAndInitializerResourceFailuresLeaveNoPrivateState(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	revision, _ := state.NewRevision(100, 0)
	for _, test := range []struct {
		name   string
		limits GraphLimits
		ops    []graphstate.Operation
		want   error
	}{
		{"nil-revision", GraphLimits{}, nil, state.ErrInvalidRevision},
		{"small-output", GraphLimits{MaxOutputBytes: 1}, nil, ErrResourceLimit},
		{"small-source", GraphLimits{MaxSourceBytes: 1}, nil, ErrResourceLimit},
		{"input-list", GraphLimits{Planner: graphstate.Limits{MaxOperations: 1}}, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, {Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: scope}}, ErrResourceLimit},
		{"unplaced", GraphLimits{}, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: func() temporal.Scope { s, _ := temporal.Unplaced(f.axis); return s }()}}, graphstate.ErrUnsupported},
		{"empty", GraphLimits{}, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: func() temporal.Scope { s, _ := temporal.Empty(f.axis); return s }()}}, graphstate.ErrEmptyMutation},
		{"missing-placement", GraphLimits{}, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11}}, graphstate.ErrValidityRequired},
		{"unknown-schema", GraphLimits{}, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, {Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "phantom", Value: graphstate.I64(7), ValueID: 99}}, graphstate.ErrSchemaMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := f.catalog(t, f.index)
			rev := revision
			if test.name == "nil-revision" {
				rev = state.Revision{}
			}
			effects, err := StageOperations(t.Context(), c, test.ops, rev, test.limits)
			if !errors.Is(err, test.want) || !reflect.DeepEqual(effects, GraphEffects{}) || c.stages != 0 || c.records != 0 || c.stageBytes != 0 || c.fullViews != 0 || c.fullViewBytes != 0 {
				t.Fatal("refusal leaked private graph state", effects, err)
			}
			if _, err := c.Root(); err != nil {
				t.Fatal("caller/resource refusal poisoned catalog", err)
			}
		})
	}
	for _, limits := range []GraphLimits{{MaxOutputBytes: 1}, {MaxSourceBytes: 1}, {Pages: PageLimits{MaxCheckpointBytes: 64, MaxWorkBytes: 192}}} {
		db := topologyStore(t, vfs.NewMem(), f.db.ApplicationLimits())
		_ = bootstrapRoot(t, db)
		c := openCatalog(t, db, 1, Limits{})
		out, err := InitializeGraphIndexes(t.Context(), c, nil, limits)
		if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) || c.stages != 0 || c.records != 0 || c.stageBytes != 0 {
			t.Fatal("initializer published partial roots", out, err)
		}
	}
	for _, definitions := range [][]graphstate.PropertyDefinition{{{Name: "", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}, {fullSchemas()[0], fullSchemas()[0]}} {
		db := topologyStore(t, vfs.NewMem(), f.db.ApplicationLimits())
		_ = bootstrapRoot(t, db)
		c := openCatalog(t, db, 1, Limits{})
		out, err := InitializeGraphIndexes(t.Context(), c, definitions, GraphLimits{})
		if !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(out, GraphEffects{}) || c.stages != 0 || c.stageBytes != 0 {
			t.Fatal("schema refusal left roots", out, err)
		}
	}
}
func TestFullLateStageSharedCapAndTreeHeightFailurePreserveExistingStage(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope})
	c := f.catalog(t, f.index)
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	held, err := newFullStage(t.Context(), c, v.descriptor, v.limits.Pages)
	if err != nil {
		t.Fatal(err)
	}
	_ = v.Close()
	if err := held.Property(t.Context(), graphstate.PropertyDefinition{Name: "reserved", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}); err != nil {
		t.Fatal(err)
	}
	before, _ := held.Writes()
	records, bytes := c.records, c.stageBytes
	root := c.root
	c.limits.MaxStageRecords = 4
	revision, _ := state.NewRevision(200, 0)
	out, err := StageOperations(t.Context(), c, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "a"}}, revision, GraphLimits{})
	after, _ := held.Writes()
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) || !sameWrites(before, after) || c.records != records || c.stageBytes != bytes || c.root != root {
		t.Fatal("late shared cap changed existing private state", out, err)
	}
	_ = held.Close()
	tight := newFullFixture(t, GraphLimits{Pages: PageLimits{MaxCells: 2, MaxLevels: 1, MaxChildren: 3}})
	scope = tight.span(t, 0, 10)
	tight.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope})
	c = tight.catalog(t, tight.index)
	root = c.root
	ops := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "a"}, {Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "b"}, {Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "c"}}
	out, err = StageOperations(t.Context(), c, ops, revision, tight.limits)
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) || c.root != root || c.stages != 0 || c.records != 0 || c.stageBytes != 0 {
		t.Fatal("late split retained earlier effects/root floor", out, err)
	}
}
