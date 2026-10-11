package graphstore

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Reconstruct the exact typed workload of held soleTestGraph.initial, not its
// unaccepted GR3 opener/output-budget code. Accepted Full admission remains GR2.
func nineOperationWorkload(t *testing.T) (temporal.Axis, []graphstate.PropertyDefinition, []graphstate.Operation) {
	t.Helper()
	axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "sole-test:v1", CanonicalUnit: "second"}, temporal.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	schemas := []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}, {Name: "tags", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.SetCardinality}}
	ops := []graphstate.Operation{
		{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: s},
		{Kind: graphstate.CreateNode, Owner: 3, Life: 4, Scope: s},
		{Kind: graphstate.CreateRelationship, Owner: 5, Life: 6, Scope: s, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: 3, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 4}},
		{Kind: graphstate.CreateRelationship, Owner: 7, Life: 8, Scope: s, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: 3, Mode: graphstate.IdentityReference}},
		{Kind: graphstate.CreateRelationship, Owner: 9, Life: 10, Scope: s, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: 1, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 2}},
		{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: s, Name: "old"},
		{Kind: graphstate.Set, Owner: 1, Life: 2, Scope: s, Name: "p", Value: graphstate.I64(9), ValueID: 11},
		{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: s, Name: "p", Value: graphstate.I64(10), ValueID: 12},
		{Kind: graphstate.Add, Owner: 1, Life: 2, Scope: s, Name: "tags", Value: graphstate.String("x"), ValueID: 13},
	}
	return axis, schemas, ops
}

func TestFullStageNineOperationWorkloadPreservesAllEffectsWithinExistingBudgets(t *testing.T) {
	for _, lane := range []string{"ordinary-defaults", "held-common-ceiling"} {
		t.Run(lane, func(t *testing.T) {
			axis, schemas, ops := nineOperationWorkload(t)
			db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
			root := bootstrapRoot(t, db)
			f := &fullFixture{db: db, root: root, index: 1, axis: axis, limits: GraphLimits{}}
			initializer := f.catalog(t, f.index)
			effects, err := InitializeGraphIndexes(t.Context(), initializer, schemas, GraphLimits{})
			if err := errors.Join(err, initializer.view.Close()); err != nil {
				t.Fatal(err)
			}
			f.install(t, effects)
			c := f.catalog(t, f.index)
			defer c.view.Close()
			requested := GraphLimits{}
			if lane == "held-common-ceiling" {
				requested.MaxSourceRows = 512
				requested.MaxSourceBytes = 4 << 20
				requested.MaxOutputBytes = 4 << 20
			}
			resolved, err := requested.resolve()
			if err != nil {
				t.Fatal(err)
			}
			beforeIndex, beforeImage, err := db.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			beforeUsage, err := db.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			revision, err := state.NewRevision(1, 7)
			if err != nil {
				t.Fatal(err)
			}
			staged, work, err := stageOperations(t.Context(), c, ops, revision, requested, nil)
			t.Logf("lane=%s operations=%d catalog=%+v graph=%+v work=%+v output=%d counts(entity/life/value/patch/dependency/group/write)=%d/%d/%d/%d/%d/%d/%d error=%v", lane, len(ops), c.limits, resolved, work, staged.OwnedBytes, len(staged.Delta.Entities), len(staged.Delta.Lives), len(staged.Delta.Values), len(staged.Delta.Patches), len(staged.Dependencies), len(staged.Groups), len(staged.Writes), err)
			afterIndex, afterImage, afterErr := db.Checkpoint()
			afterUsage, usageErr := db.ApplicationUsage()
			if afterErr != nil || usageErr != nil || afterIndex != beforeIndex || !bytes.Equal(afterImage, beforeImage) || afterUsage != beforeUsage || c.fullViews != 0 || c.fullViewBytes != 0 || c.stages != 0 || c.stageBytes != 0 {
				t.Fatal("staging/refusal published or leaked ownership", afterUsage, beforeUsage, afterErr, usageErr)
			}
			if err != nil {
				if !reflect.DeepEqual(staged, GraphEffects{}) {
					t.Fatal("partial effects on refusal", staged)
				}
				t.Fatalf("nine bounded operations unexpectedly refused; actual attempted work=%+v: %v", work, err)
			}
			if len(staged.Delta.Entities) != 5 || len(staged.Delta.Lives) != 5 || len(staged.Delta.Values) != 3 || len(staged.Delta.Patches) != 9 || len(staged.Groups) != 9 || staged.Work != work || staged.Base.Index != beforeIndex {
				t.Fatal("incomplete exact workload effects", staged.Delta, staged.Groups, work)
			}
			ids := make([]graphstate.EntityID, 0, 5)
			for _, entity := range staged.Delta.Entities {
				ids = append(ids, entity.ID)
			}
			if !reflect.DeepEqual(ids, []graphstate.EntityID{1, 3, 5, 7, 9}) {
				t.Fatal("entity set", ids)
			}
			expectedLives := []graphstate.LifeRecord{{Owner: 1, Life: 2}, {Owner: 3, Life: 4}, {Owner: 5, Life: 6, SourceLife: 2, TargetLife: 4}, {Owner: 7, Life: 8}, {Owner: 9, Life: 10, SourceLife: 2, TargetLife: 2}}
			if !reflect.DeepEqual(staged.Delta.Lives, expectedLives) {
				t.Fatal("life-bound/self-loop exact records", staged.Delta.Lives)
			}
		})
	}
}

func TestFullTypedStageAxisConflictIsCallerInvalidAndRetainsCause(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	all, err := temporal.All(f.axis)
	if err != nil {
		t.Fatal(err)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: all})
	original := f.index
	d := f.axis.Descriptor()
	d.Reference = "conflicting-reference:v1"
	conflict, err := temporal.NewAxis(d, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := temporal.All(conflict)
	if err != nil {
		t.Fatal(err)
	}
	c := f.catalog(t, f.index)
	defer c.view.Close()
	r, _ := state.NewRevision(9, 23)
	out, err := StageOperations(t.Context(), c, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Name: "bad", Scope: window}}, r, GraphLimits{})
	if !errors.Is(err, graphstate.ErrInvalidInput) || !errors.Is(err, temporal.ErrAxisMismatch) || !reflect.DeepEqual(out, GraphEffects{}) {
		t.Fatal(out, err)
	}
	if c.fullViews != 0 || c.stages != 0 || c.poison != nil {
		t.Fatal("caller conflict poisoned catalog", c.poison)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Name: "good", Scope: all})
	if got := f.projection(t, original, 1, 0, graphstate.Declared); len(got.Labels) != 0 {
		t.Fatal("old root changed", got)
	}
	if got := f.projection(t, f.index, 1, 0, graphstate.Declared); !reflect.DeepEqual(got.Labels, []string{"good"}) {
		t.Fatal(got)
	}
}

func TestFinalReviewScopeScalarCloneRequiresFreshBackingAdmission(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	parts := make([]temporal.Scope, 512)
	for i := range parts {
		p, err := temporal.IntegerPosition(f.axis, temporal.Int64(int64(2*i)))
		if err != nil {
			t.Fatal(err)
		}
		parts[i], err = temporal.Point(p)
		if err != nil {
			t.Fatal(err)
		}
	}
	region, err := temporal.Region(f.axis, parts, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	all, err := temporal.All(f.axis)
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: all, Name: "window", Value: graphstate.ScopeValue(region), ValueID: 99}}
	wire, err := temporal.AppendScope(nil, region, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	backing, err := scopeOwnedBacking(wire)
	if err != nil {
		t.Fatal(err)
	}
	// Independent inline-atom owner floor: one768-byte interval allowance per
	// point plus four canonical wire lengths, distinct from the encoder's owners.
	if backing != len(parts)*768+4*len(wire) {
		t.Fatal("fresh clone backing omitted interval owners", backing)
	}
	invoke := func(capacity int) ([]graphstate.Operation, int, error) {
		v, err := OpenReadView(t.Context(), c, GraphLimits{})
		if err != nil {
			t.Fatal(err)
		}
		defer v.Close()
		v.arena, err = graphstate.NewOutputBudget(capacity)
		if err != nil {
			t.Fatal(err)
		}
		owned, err := cloneOperations(t.Context(), v, ops)
		return owned, v.arena.Used(), err
	}
	fitting, fittingUsed, err := invoke(4 << 20)
	if err != nil || len(fitting) != 1 || fittingUsed < backing {
		t.Fatal("fitting control omitted fresh scalar owner", fittingUsed, backing, err)
	}
	copied, ok := fitting[0].Value.Scope()
	if !ok {
		t.Fatal("scope scalar erased")
	}
	same, err := copied.SameSupport(region, temporal.Limits{})
	if err != nil || !same {
		t.Fatal("wrong clone", err)
	}
	const tight = 16 << 10
	owned, used, err := invoke(tight)
	t.Logf("parts=%d wire=%d fresh-clone-backing=%d tight=%d used=%d fitting-used=%d clone-count=%d err=%v", len(parts), len(wire), backing, tight, used, fittingUsed, len(owned), err)
	if !errors.Is(err, ErrResourceLimit) || len(owned) != 0 {
		t.Fatalf("fresh512-interval scalar clone allocated despite allowance below decoded backing: backing=%d tight=%d used=%d clone-count=%d err=%v", backing, tight, used, len(owned), err)
	}
}

func TestFinalReviewRegressedRangeEpochCannotAuthorizeLocalWrites(t *testing.T) {
	store, d, binding, cfg, _ := partitionAuthorityFixture(t)
	n := Namespace{Graph: d.Graph(), Partition: 3}
	_, image, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	p := RangePublication{Graph: d.Graph(), RangeID: 1, First: 1, Last: 16, InitialPartition: 3, InitialEpoch: 2, Configuration: cfg, SourcePartition: 3, SourceIndex: 1}
	pub, err := EncodeRangePublication(p)
	if err != nil {
		t.Fatal(err)
	}
	o := RangeOwnership{Graph: d.Graph(), RangeID: 1, Partition: 3, Epoch: 2, Configuration: cfg}
	owner, err := EncodeRangeOwnership(o)
	if err != nil {
		t.Fatal(err)
	}
	rows := []raftlog.KV{{Key: RangePublicationKey(n, 1), Value: pub}, {Key: RangeEndKey(n, 16), Value: pub}, {Key: RangeOwnershipKey(n, 1), Value: owner}}
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			if bytes.Compare(rows[j].Key, rows[i].Key) < 0 {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
	at := ownershipInstallRaw(t, store, image, rows)
	retained := ownershipView(t, store, at)
	old, _, err := OpenPartitionGraphReadView(t.Context(), retained, d, binding, cfg, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	control, err := old.Entity(t.Context(), 1)
	if err != nil || control.Found || control.View == (graphstate.ViewID{}) {
		t.Fatal("positive exact owned-absence control", control, err)
	}
	old.Close()
	o.Epoch = 1
	owner, err = EncodeRangeOwnership(o)
	if err != nil {
		t.Fatal(err)
	}
	at = ownershipInstallRaw(t, store, image, []raftlog.KV{{Key: RangeOwnershipKey(n, 1), Value: owner}})
	current := ownershipView(t, store, at)
	v, _, err := OpenPartitionGraphReadView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := v.Entity(t.Context(), 1)
	v.Close()
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(1, 23)
	effects, _, stageErr := StagePartitionOperations(t.Context(), current, d, binding, cfg, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}}, revision, Limits{}, GraphLimits{}, OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20})
	t.Logf("initial-epoch=%d current-epoch=%d read-found=%v read-view=%x read-err=%v effect-entities=%d effect-writes=%d stage-err=%v", p.InitialEpoch, o.Epoch, got.Found, got.View, readErr, len(effects.Delta.Entities), len(effects.Writes), stageErr)
	lookedUp, own, lookupWork, lookupErr := LookupPublishedRange(t.Context(), current, n, 1, cfg, ownershipBudget())
	if !errors.Is(lookupErr, ErrCorrupt) || lookedUp != (RangePublication{}) || own != (RangeOwnership{}) || lookupWork.Records == 0 {
		t.Fatal("lookup leaked regressed authority", lookedUp, own, lookupWork, lookupErr)
	}
	old, _, err = OpenPartitionGraphReadView(t.Context(), retained, d, binding, cfg, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	still, err := old.Entity(t.Context(), 1)
	if err != nil || !reflect.DeepEqual(still, control) {
		t.Fatal("current corruption changed retained authority", still, control, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	beforeIndex, beforeImage, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	beforeUsage, err := store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	fitting, _, err := StagePartitionOperations(t.Context(), retained, d, binding, cfg, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}}, revision, Limits{}, GraphLimits{}, OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20})
	if err != nil || len(fitting.Delta.Entities) != 1 || len(fitting.Delta.Lives) != 1 || len(fitting.Writes) != 7 {
		t.Fatal("retained initialized lineage refused exact local effects", fitting.Delta, len(fitting.Writes), err)
	}
	afterIndex, afterImage, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	afterUsage, err := store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if beforeIndex != afterIndex || !bytes.Equal(beforeImage, afterImage) || beforeUsage != afterUsage {
		t.Fatal("read/refusal/retained staging installed")
	}
	if !errors.Is(readErr, ErrCorrupt) || !errors.Is(stageErr, ErrCorrupt) || !reflect.DeepEqual(effects, GraphEffects{}) {
		t.Fatalf("regressed persisted ownership epoch authorized local absence/effects: read=%+v readErr=%v writes=%d stageErr=%v", got, readErr, len(effects.Writes), stageErr)
	}
}

func TestFinalReviewDescriptorScalarCloneRequiresFreshBackingAdmission(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	all, err := temporal.All(f.axis)
	if err != nil {
		t.Fatal(err)
	}
	value := storeV1Descriptor(t, 3, bytes.Repeat([]byte{9}, 512))
	key, err := value.EqualityKey(c.limits.valueLimits())
	if err != nil {
		t.Fatal(err)
	}
	backing := descriptorOwnedBacking(len(key) - 1)
	invoke := func(value graphstate.Scalar, limit int) ([]graphstate.Operation, int, error) {
		v, err := OpenReadView(t.Context(), c, GraphLimits{})
		if err != nil {
			t.Fatal(err)
		}
		defer v.Close()
		v.arena, err = graphstate.NewOutputBudget(limit)
		if err != nil {
			t.Fatal(err)
		}
		out, err := cloneOperations(t.Context(), v, []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: all, Name: "evidence", Value: value, ValueID: 99}})
		return out, v.arena.Used(), err
	}
	control, baseline, err := invoke(graphstate.Null(), 4<<20)
	if err != nil || len(control) != 1 {
		t.Fatal(err)
	}
	// Independently replace Null's136B key allowance with the descriptor encoder's
	//128+8*(1+MaxDescriptorBytes). Its fresh decode backing is an additional owner.
	encoder := 128 + 8*(1+c.limits.Temporal.MaxDescriptorBytes)
	tight := baseline - 136 + encoder
	fitting, used, err := invoke(value, 4<<20)
	if err != nil || len(fitting) != 1 || used < tight+backing {
		t.Fatalf("descriptor decode missing independent fresh owner: used=%d keyFloor=%d backing=%d err=%v", used, tight, backing, err)
	}
	same, err := fitting[0].Value.Equal(value, graphstate.Limits{})
	if err != nil || !same {
		t.Fatal("descriptor copy changed", err)
	}
	out, _, err := invoke(value, tight)
	if !errors.Is(err, ErrResourceLimit) || len(out) != 0 {
		t.Fatal("descriptor clone bypassed aggregate", len(out), err)
	}
}

func TestFinalReviewOwnedScalarBackingRefusalReachesPublicStage(t *testing.T) {
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	f := &fullFixture{db: db, root: bootstrapRoot(t, db), index: 1, axis: testAxis(t, 1, temporal.ProfileIntegerZ)}
	c := f.catalog(t, f.index)
	initialized, err := InitializeGraphIndexes(t.Context(), c, []graphstate.PropertyDefinition{{Name: "window", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}, GraphLimits{})
	if err := errors.Join(err, c.view.Close()); err != nil {
		t.Fatal(err)
	}
	f.install(t, initialized)
	c = f.catalog(t, f.index)
	defer c.view.Close()
	parts := make([]temporal.Scope, 512)
	for i := range parts {
		pos, err := temporal.IntegerPosition(f.axis, temporal.Int64(int64(2*i)))
		if err != nil {
			t.Fatal(err)
		}
		parts[i], err = temporal.Point(pos)
		if err != nil {
			t.Fatal(err)
		}
	}
	region, err := temporal.Region(f.axis, parts, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	all, err := temporal.All(f.axis)
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: all}, {Kind: graphstate.Set, Owner: 1, Life: 11, Scope: all, Name: "window", Value: graphstate.ScopeValue(region), ValueID: 99}}
	revision, _ := state.NewRevision(1, 23)
	beforeIndex, beforeImage, err := db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	beforeUsage, err := db.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	parent, _ := graphstate.NewOutputBudget(16 << 10)
	refused, work, err := StageOperationsWithOutputBudget(t.Context(), c, ops, revision, GraphLimits{}, parent)
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(refused, GraphEffects{}) || work.Bytes == 0 {
		t.Fatal("public clone refusal", work, err)
	}
	fitting, _ := graphstate.NewOutputBudget(16 << 20)
	out, _, err := StageOperationsWithOutputBudget(t.Context(), c, ops, revision, GraphLimits{}, fitting)
	if err != nil {
		t.Fatal("supported fitting scope scalar", err)
	}
	if len(out.Delta.Entities) != 1 || len(out.Delta.Lives) != 1 || len(out.Delta.Values) != 1 || len(out.Groups) != 2 {
		t.Fatal("incomplete scalar effects", out.Delta, len(out.Groups))
	}
	copied, ok := out.Delta.Values[0].Value.Scope()
	if !ok {
		t.Fatal("scalar kind erased")
	}
	same, err := copied.SameSupport(region, temporal.Limits{})
	if err != nil || !same {
		t.Fatal("scope support changed", err)
	}
	afterIndex, afterImage, err := db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	afterUsage, err := db.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if beforeIndex != afterIndex || !bytes.Equal(beforeImage, afterImage) || beforeUsage != afterUsage || c.stages != 0 || c.fullViews != 0 || c.poison != nil {
		t.Fatal("refusal/staging installed or poisoned")
	}
}
