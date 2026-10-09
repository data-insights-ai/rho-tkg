package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func testNamespace() Namespace { return Namespace{Graph: graphstate.GraphID{1}, Partition: 7} }
func testAxis(t *testing.T, id byte, profile temporal.Profile) temporal.Axis {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{id}, Profile: profile, Version: 1, Reference: "source/clock@v1", CanonicalUnit: "exact-unit"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func newStore(t *testing.T, fs vfs.FS) (*raftlog.Store, Root) {
	t.Helper()
	r, err := NewRoot(testNamespace(), 3)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(r)
	if err != nil {
		t.Fatal(err)
	}
	s, err := raftlog.Open(raftlog.Config{Dir: "catalog", FS: fs, Create: true, Application: raftlog.DefaultApplicationPolicy(1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, image); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, r
}
func openCatalog(t *testing.T, s *raftlog.Store, index uint64, l Limits) *Catalog {
	t.Helper()
	v, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	c, err := OpenCatalog(v, testNamespace(), 3, l)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func stage(t *testing.T, c *Catalog) *Stage {
	t.Helper()
	s, err := c.NewStage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func commitRows(t *testing.T, s *raftlog.Store, r Root, rows []raftlog.KV) (Root, uint64) {
	t.Helper()
	base, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	r, err = r.AdvanceEffects(sha256.Sum256([]byte{byte(base)}))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeRoot(r)
	if err != nil {
		t.Fatal(err)
	}
	index := base + 1
	e := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("validated-fixture-command")}
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{e}}); err != nil {
		t.Fatal(err)
	}
	b := raftlog.ApplicationBatch{BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: encoded, Writes: rows, Changes: []byte("fixture-cdc"), Outcome: []byte("fixture-outcome")}
	if _, err := s.ApplicationLimits().Preflight(b, s.Limits()); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(index, b); err != nil {
		t.Fatal(err)
	}
	return r, index
}
func commitStage(t *testing.T, db *raftlog.Store, r Root, s *Stage) (Root, uint64) {
	t.Helper()
	rows, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	return commitRows(t, db, r, rows)
}
func refEntity(id uint64) EntityRef { return EntityRef{testNamespace().Graph, graphstate.EntityID(id)} }
func refLife(owner, id uint64) LifeRef {
	return LifeRef{testNamespace().Graph, graphstate.EntityID(owner), graphstate.LifeID(id)}
}
func refValue(id uint64) ValueRef { return ValueRef{testNamespace().Graph, graphstate.ValueID(id)} }

func TestCatalogDurableQualifiedRecordsAndAliases(t *testing.T) {
	fs := vfs.NewCrashableMem()
	db, root := newStore(t, fs)
	old := openCatalog(t, db, 1, Limits{})
	st := stage(t, old)
	axis := testAxis(t, 2, temporal.ProfileIntegerZ)
	other := testAxis(t, 3, temporal.ProfileRationalQ)
	nodeSchema := graphstate.PropertyDefinition{Name: "status", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}
	relSchema := graphstate.PropertyDefinition{Name: "status", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.SetCardinality, Unique: graphstate.UniqueMembers}
	for _, d := range []graphstate.PropertyDefinition{nodeSchema, relSchema} {
		if err := st.Property(t.Context(), d); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{1, 2} {
		if err := st.Entity(t.Context(), refEntity(id), graphstate.EntityRecord{ID: graphstate.EntityID(id), Kind: graphstate.Node, Axis: axis}); err != nil {
			t.Fatal(err)
		}
		if err := st.Life(t.Context(), refLife(id, id+10), graphstate.LifeRecord{Owner: graphstate.EntityID(id), Life: graphstate.LifeID(id + 10)}); err != nil {
			t.Fatal(err)
		}
	}
	rel := graphstate.EntityRecord{ID: 3, Kind: graphstate.Relationship, Axis: axis, Type: "LINK", Source: 1, Target: 2, Mode: graphstate.LifeBound}
	binding := graphstate.LifeRecord{Owner: 3, Life: 33, SourceLife: 11, TargetLife: 12}
	if err := st.Entity(t.Context(), refEntity(3), rel); err != nil {
		t.Fatal(err)
	}
	if err := st.Life(t.Context(), refLife(3, 33), binding); err != nil {
		t.Fatal(err)
	}
	rational, err := temporal.Fraction(temporal.Int64(1), temporal.Int64(3), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	position, err := temporal.RationalPosition(other, rational)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.Point(position)
	if err != nil {
		t.Fatal(err)
	}
	values := []struct {
		id    uint64
		value graphstate.Scalar
	}{{101, graphstate.String("old")}, {100, graphstate.String("old")}, {102, graphstate.I64(7)}, {103, graphstate.Bool(true)}, {104, graphstate.Null()}, {105, graphstate.ScopeValue(scope)}}
	for _, v := range values {
		if err := st.Value(t.Context(), refValue(v.id), v.value); err != nil {
			t.Fatal(err)
		}
	}
	used, err := st.InternLocal(t.Context(), refValue(900), graphstate.String("old"))
	if err != nil || used != refValue(101) {
		t.Fatal("unstable alias canonical ID", used, err)
	}
	root, index := commitStage(t, db, root, st)
	current := openCatalog(t, db, index, Limits{})
	if _, found, err := old.Entity(t.Context(), refEntity(1)); err != nil || found {
		t.Fatal("old root leaked entity", found, err)
	}
	for _, d := range []graphstate.PropertyDefinition{nodeSchema, relSchema} {
		got, found, err := current.Property(t.Context(), d.Owner, d.Name)
		if err != nil || !found || got != d {
			t.Fatal(got, found, err)
		}
	}
	gotRel, found, err := current.Entity(t.Context(), refEntity(3))
	if err != nil || !found || gotRel != rel {
		t.Fatal(gotRel, found, err)
	}
	gotLife, found, err := current.Life(t.Context(), refLife(3, 33))
	if err != nil || !found || gotLife != binding {
		t.Fatal(gotLife, found, err)
	}
	for _, v := range values {
		got, found, err := current.Value(t.Context(), refValue(v.id))
		if err != nil || !found || got.Ref != refValue(v.id) {
			t.Fatal(got, found, err)
		}
		equal, err := got.Value.Equal(v.value, current.limits.valueLimits())
		if err != nil || !equal {
			t.Fatal("value changed", v.id, err)
		}
	}
	if _, found, err := current.Value(t.Context(), refValue(900)); err != nil || found {
		t.Fatal("unused ID was written", found, err)
	}
	canonical, found, err := current.LookupLocalValueIdentity(t.Context(), graphstate.String("old"))
	if err != nil || !found || canonical.Ref != refValue(101) {
		t.Fatal(canonical, found, err)
	}
	recovered, err := raftlog.Open(raftlog.Config{Dir: "catalog", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: raftlog.DefaultApplicationPolicy(1)})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	reopened := openCatalog(t, recovered, index, Limits{})
	gotAxis, found, err := reopened.Axis(t.Context(), other.Descriptor().ID)
	if err != nil || !found || gotAxis.Descriptor() != other.Descriptor() || gotAxis.DefinitionHash() != other.DefinitionHash() {
		t.Fatal(gotAxis, found, err)
	}
	next := stage(t, current)
	if err := next.Life(t.Context(), refLife(1, 44), graphstate.LifeRecord{Owner: 1, Life: 44}); err != nil {
		t.Fatal(err)
	}
	if err := next.Value(t.Context(), refValue(106), graphstate.String("new")); err != nil {
		t.Fatal(err)
	}
	_, later := commitStage(t, db, root, next)
	latest := openCatalog(t, db, later, Limits{})
	if _, found, err := current.Life(t.Context(), refLife(1, 44)); err != nil || found {
		t.Fatal("old life read used current", found, err)
	}
	if _, found, err := latest.Life(t.Context(), refLife(1, 44)); err != nil || !found {
		t.Fatal(found, err)
	}
	if _, found, err := current.LookupLocalValueIdentity(t.Context(), graphstate.String("new")); err != nil || found {
		t.Fatal("old dictionary leaked future", found, err)
	}
	if _, found, err := latest.LookupLocalValueIdentity(t.Context(), graphstate.String("new")); err != nil || !found {
		t.Fatal(found, err)
	}
}

func TestCatalogRebindingWrongNamespaceAndOwnedWrites(t *testing.T) {
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	axis := testAxis(t, 2, temporal.ProfileIntegerZ)
	record := graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: axis}
	definition := graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}
	if err := s.Entity(t.Context(), refEntity(1), record); err != nil {
		t.Fatal(err)
	}
	if err := s.Life(t.Context(), refLife(1, 1), graphstate.LifeRecord{Owner: 1, Life: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Property(t.Context(), definition); err != nil {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(1), graphstate.String("one")); err != nil {
		t.Fatal(err)
	}
	_, idx := commitStage(t, db, root, s)
	c2 := openCatalog(t, db, idx, Limits{})
	s2 := stage(t, c2)
	changed := axis.Descriptor()
	changed.Reference = "different/reference"
	other, err := temporal.NewAxis(changed, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	changedDefinition := definition
	changedDefinition.Type = graphstate.ScalarI64
	changedEntity := record
	changedEntity.Axis = other
	for _, fn := range []func() error{func() error { return s2.Axis(t.Context(), other) }, func() error { return s2.Property(t.Context(), changedDefinition) }, func() error { return s2.Entity(t.Context(), refEntity(1), changedEntity) }, func() error { return s2.Value(t.Context(), refValue(1), graphstate.I64(1)) }} {
		if err := fn(); !errors.Is(err, ErrRebinding) {
			t.Fatal(err)
		}
		rows, err := s2.Writes()
		if err != nil || len(rows) != 0 {
			t.Fatal("failed operation changed stage", rows, err)
		}
	}
	for _, fn := range []func() error{func() error { _, _, e := c2.Entity(t.Context(), EntityRef{graphstate.GraphID{9}, 1}); return e }, func() error { _, _, e := c2.Life(t.Context(), LifeRef{graphstate.GraphID{9}, 1, 1}); return e }, func() error { _, _, e := c2.Value(t.Context(), ValueRef{graphstate.GraphID{9}, 1}); return e }, func() error { return s2.Entity(t.Context(), EntityRef{graphstate.GraphID{9}, 2}, record) }, func() error {
		return s2.Value(t.Context(), ValueRef{graphstate.GraphID{9}, 2}, graphstate.String("two"))
	}} {
		if err := fn(); !errors.Is(err, ErrNamespace) {
			t.Fatal(err)
		}
	}
	view, err := db.ApplicationView(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	for _, n := range []Namespace{{Graph: graphstate.GraphID{9}, Partition: 7}, {Graph: testNamespace().Graph, Partition: 8}} {
		if _, err := OpenCatalog(view, n, 3, Limits{}); !errors.Is(err, ErrNamespace) {
			t.Fatal(err)
		}
	}
	if _, err := OpenCatalog(view, testNamespace(), 4, Limits{}); !errors.Is(err, ErrStaleOwner) {
		t.Fatal(err)
	}
	rows, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if cap(rows[i].Key) != len(rows[i].Key) || cap(rows[i].Value) != len(rows[i].Value) {
			t.Fatal("hidden capacity")
		}
		rows[i].Key[0] ^= 1
		rows[i].Value[0] ^= 1
	}
	original, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	for i := range original {
		if bytes.Equal(original[i].Key, rows[i].Key) || bytes.Equal(original[i].Value, rows[i].Value) {
			t.Fatal("output aliases stage")
		}
	}
}

func TestCatalogHashCollisionsExactValuesAndBudget(t *testing.T) {
	db, root := newStore(t, vfs.NewMem())
	l := Limits{MaxBucketValues: 3}
	c := openCatalog(t, db, 1, l)
	c.hash = func(string) [32]byte { return [32]byte{42} }
	s := stage(t, c)
	for i, value := range []graphstate.Scalar{graphstate.String("1"), graphstate.I64(1), graphstate.Bool(true)} {
		if err := s.Value(t.Context(), refValue(uint64(i+1)), value); err != nil {
			t.Fatal(err)
		}
	}
	before, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(4), graphstate.String("overflow")); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	after, err := s.Writes()
	if err != nil || !sameWrites(after, before) {
		t.Fatal("overflow changed stage", err)
	}
	_, idx := commitStage(t, db, root, s)
	at := openCatalog(t, db, idx, l)
	at.hash = c.hash
	for i, value := range []graphstate.Scalar{graphstate.String("1"), graphstate.I64(1), graphstate.Bool(true)} {
		entry, found, err := at.LookupLocalValueIdentity(t.Context(), value)
		if err != nil || !found || entry.Ref != refValue(uint64(i+1)) {
			t.Fatal("digest substituted equality", entry, found, err)
		}
	}
	if _, found, err := at.LookupLocalValueIdentity(t.Context(), graphstate.String("absent")); err != nil || found {
		t.Fatal("phantom hash match", found, err)
	}
	limited := openCatalog(t, db, idx, Limits{MaxReadRows: 3, MaxBucketValues: 3})
	limited.hash = c.hash
	if _, _, err := limited.LookupLocalValueIdentity(t.Context(), graphstate.Bool(true)); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("collision work uncharged", err)
	}
	if _, _, err := limited.LookupLocalValueIdentity(t.Context(), graphstate.String("1")); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	} // root+member+value+reference reads exceed 3
}

func TestCatalogNullZeroAndEquivalentScopeKeys(t *testing.T) {
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	negative, err := graphstate.F64(math.Copysign(0, -1))
	if err != nil {
		t.Fatal(err)
	}
	positive, err := graphstate.F64(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(10), negative); err != nil {
		t.Fatal(err)
	}
	ref, err := s.InternLocal(t.Context(), refValue(11), positive)
	if err != nil || ref != refValue(10) {
		t.Fatal(ref, err)
	}
	axis := testAxis(t, 2, temporal.ProfileIntegerZ)
	zero, err := temporal.IntegerPosition(axis, temporal.Int64(0))
	if err != nil {
		t.Fatal(err)
	}
	one, err := temporal.IntegerPosition(axis, temporal.Int64(1))
	if err != nil {
		t.Fatal(err)
	}
	point, err := temporal.Point(zero)
	if err != nil {
		t.Fatal(err)
	}
	lo, _ := temporal.FiniteBound(zero, true)
	hi, _ := temporal.FiniteBound(one, false)
	span, err := temporal.Span(axis, lo, hi, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(12), graphstate.ScopeValue(point)); err != nil {
		t.Fatal(err)
	}
	ref, err = s.InternLocal(t.Context(), refValue(13), graphstate.ScopeValue(span))
	if err != nil || ref != refValue(12) {
		t.Fatal("equivalent support omitted", ref, err)
	}
	if err := s.Value(t.Context(), refValue(14), graphstate.Null()); err != nil {
		t.Fatal(err)
	}
	_, idx := commitStage(t, db, root, s)
	at := openCatalog(t, db, idx, Limits{})
	entry, found, err := at.Value(t.Context(), refValue(14))
	if err != nil || !found || entry.Value.Kind() != graphstate.ScalarNull {
		t.Fatal(entry, found, err)
	}
	if _, found, err := at.Value(t.Context(), refValue(99)); err != nil || found {
		t.Fatal("null became absence", found, err)
	}
}

func TestCatalogStageLimitsLifetimeAndNilHelpers(t *testing.T) {
	db, _ := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{MaxStages: 1, MaxStageRecords: 1})
	s := stage(t, c)
	if _, err := c.NewStage(t.Context()); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(1), graphstate.String("three writes")); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	rows, err := s.Writes()
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	d := graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}
	if err := s.Property(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	d.Name = "q"
	if err := s.Property(t.Context(), d); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Writes(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := s.Axis(t.Context(), testAxis(t, 2, temporal.ProfileIntegerZ)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	_ = stage(t, c)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := c.Entity(ctx, refEntity(1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var nilCatalog *Catalog
	var nilStage *Stage
	for _, fn := range []func() error{func() error { _, e := nilCatalog.Root(); return e }, func() error { _, _, e := nilCatalog.Axis(t.Context(), temporal.AxisID{1}); return e }, func() error { _, _, e := nilCatalog.Property(t.Context(), graphstate.Node, "p"); return e }, func() error { _, _, e := nilCatalog.Entity(t.Context(), refEntity(1)); return e }, func() error { _, _, e := nilCatalog.Life(t.Context(), refLife(1, 1)); return e }, func() error { _, _, e := nilCatalog.Value(t.Context(), refValue(1)); return e }, func() error { _, _, e := nilCatalog.LookupLocalValueIdentity(t.Context(), graphstate.Null()); return e }, func() error { _, e := nilCatalog.NewStage(t.Context()); return e }, func() error { return nilStage.Axis(t.Context(), temporal.Axis{}) }, func() error { return nilStage.Property(t.Context(), d) }, func() error { return nilStage.Entity(t.Context(), refEntity(1), graphstate.EntityRecord{}) }, func() error { return nilStage.Life(t.Context(), refLife(1, 1), graphstate.LifeRecord{}) }, func() error { return nilStage.Value(t.Context(), refValue(1), graphstate.Null()) }, func() error { _, e := nilStage.InternLocal(t.Context(), refValue(1), graphstate.Null()); return e }, func() error { _, e := nilStage.Writes(); return e }, nilStage.Close} {
		if err := fn(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := OpenCatalog(nil, testNamespace(), 3, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Root(); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestCatalogConcurrentStagesAndClose(t *testing.T) {
	db, _ := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			d := graphstate.PropertyDefinition{Name: string(rune('a' + i)), Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}
			err := s.Property(t.Context(), d)
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Error(err)
			}
		})
	}
	wg.Go(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
}

func TestCatalogInvalidInputDoesNotPoison(t *testing.T) {
	db, _ := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	if err := s.Axis(t.Context(), temporal.Axis{}); !errors.Is(err, ErrInvalid) || !errors.Is(err, temporal.ErrInvalidAxis) {
		t.Fatal(err)
	}
	if _, _, err := c.LookupLocalValueIdentity(t.Context(), graphstate.Scalar{}); !errors.Is(err, ErrInvalid) || !errors.Is(err, graphstate.ErrInvalidInput) {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(1), graphstate.Scalar{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.InternLocal(t.Context(), refValue(1), graphstate.Scalar{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d := graphstate.PropertyDefinition{Name: "valid", Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}
	if err := s.Property(t.Context(), d); err != nil {
		t.Fatal("invalid input poisoned stage", err)
	}
	if _, _, err := c.LookupLocalValueIdentity(t.Context(), graphstate.String("valid")); err != nil {
		t.Fatal("invalid input poisoned reader", err)
	}
}

func TestCatalogCorruptMembershipAndRecordsPoison(t *testing.T) {
	for _, scenario := range []string{"membership", "ordinal", "type", "entity-id", "axis-reference", "value-reference"} {
		t.Run(scenario, func(t *testing.T) {
			db, root := newStore(t, vfs.NewMem())
			c := openCatalog(t, db, 1, Limits{})
			if scenario == "ordinal" {
				c.hash = func(string) [32]byte { return [32]byte{7} }
			}
			s := stage(t, c)
			a := testAxis(t, 2, temporal.ProfileIntegerZ)
			if err := s.Entity(t.Context(), refEntity(1), graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: a}); err != nil {
				t.Fatal(err)
			}
			if err := s.Value(t.Context(), refValue(10), graphstate.String("A")); err != nil {
				t.Fatal(err)
			}
			if err := s.Value(t.Context(), refValue(20), graphstate.String("B")); err != nil {
				t.Fatal(err)
			}
			root, idx := commitStage(t, db, root, s)
			current := openCatalog(t, db, idx, Limits{})
			current.hash = c.hash
			aKey, _ := graphstate.String("A").EqualityKey(current.limits.valueLimits())
			bKey, _ := graphstate.String("B").EqualityKey(current.limits.valueLimits())
			var row raftlog.KV
			switch scenario {
			case "membership":
				row = raftlog.KV{Key: memberKey(testNamespace(), c.hash(aKey), 0), Value: encodeNumber(testNamespace(), bucketMemberRecord, 20)}
			case "ordinal":
				row = raftlog.KV{Key: memberKey(testNamespace(), c.hash(bKey), 1), Value: encodeNumber(testNamespace(), bucketMemberRecord, 10)}
			case "type":
				record, _ := encodeProperty(testNamespace(), graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}, current.limits)
				row = raftlog.KV{Key: entityKey(testNamespace(), 1), Value: record}
			case "entity-id":
				record, _ := encodeEntity(testNamespace(), graphstate.EntityRecord{ID: 2, Kind: graphstate.Node, Axis: a}, current.limits)
				row = raftlog.KV{Key: entityKey(testNamespace(), 1), Value: record}
			case "axis-reference":
				row = raftlog.KV{Key: axisKey(testNamespace(), a.Descriptor().ID), Deleted: true}
			case "value-reference":
				row = raftlog.KV{Key: bucketKey(testNamespace(), c.hash(aKey)), Deleted: true}
			}
			_, bad := commitRows(t, db, root, []raftlog.KV{row})
			corrupt := openCatalog(t, db, bad, Limits{})
			corrupt.hash = c.hash
			var err error
			switch scenario {
			case "membership":
				_, _, err = corrupt.LookupLocalValueIdentity(t.Context(), graphstate.String("A"))
			case "ordinal":
				_, _, err = corrupt.LookupLocalValueIdentity(t.Context(), graphstate.String("B"))
			case "value-reference":
				entry, found, e := corrupt.Value(t.Context(), refValue(10))
				if !emptyValueEntry(entry) || found {
					t.Fatal("partial value on corruption", entry, found)
				}
				err = e
			default:
				entry, found, e := corrupt.Entity(t.Context(), refEntity(1))
				if entry != (graphstate.EntityRecord{}) || found {
					t.Fatal("partial entity on corruption", entry, found)
				}
				err = e
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal("corrupt record became normal miss", scenario, err)
			}
			if _, err := corrupt.Root(); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
		})
	}
}

func TestCatalogLateBudgetFailureReturnsZero(t *testing.T) {
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	a := testAxis(t, 2, temporal.ProfileIntegerZ)
	if err := s.Entity(t.Context(), refEntity(1), graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: a}); err != nil {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(10), graphstate.String("value")); err != nil {
		t.Fatal(err)
	}
	_, idx := commitStage(t, db, root, s)
	limited := openCatalog(t, db, idx, Limits{MaxReadRows: 1})
	if entry, found, err := limited.Entity(t.Context(), refEntity(1)); !errors.Is(err, ErrResourceLimit) || entry != (graphstate.EntityRecord{}) || found {
		t.Fatal(entry, found, err)
	}
	if entry, found, err := limited.Value(t.Context(), refValue(10)); !errors.Is(err, ErrResourceLimit) || !emptyValueEntry(entry) || found {
		t.Fatal(entry, found, err)
	}
	if _, err := limited.Root(); err != nil {
		t.Fatal("quota failure poisoned catalog", err)
	}
}

func emptyValueEntry(v ValueEntry) bool {
	return v.Ref == (ValueRef{}) && v.Value.Kind() == graphstate.ScalarInvalid && v.PayloadBytes == 0 && v.ordinal == 0
}
func sameWrites(a, b []raftlog.KV) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Deleted != b[i].Deleted || !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

func TestCatalogDescriptorAndNameMaterializationExactBoundary(t *testing.T) {
	n := testNamespace()
	root, _ := NewRoot(n, 3)
	image, _ := EncodeRoot(root)
	p := raftlog.DefaultApplicationPolicy(1)
	p.MaxKeyBytes = 65535
	db, err := raftlog.Open(raftlog.Config{Dir: "large", FS: vfs.NewMem(), Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Initialize([]uint64{1}, image); err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t, db, 1, Limits{MaxNameBytes: 50000})
	s := stage(t, c)
	d := testAxis(t, 8, temporal.ProfileIntegerZ).Descriptor()
	d.Reference = string(bytes.Repeat([]byte{'r'}, 60000))
	axis, err := temporal.NewAxis(d, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Axis(t.Context(), axis); err != nil {
		t.Fatal(err)
	}
	property := graphstate.PropertyDefinition{Name: string(bytes.Repeat([]byte{'n'}, 50000)), Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}
	if err := s.Property(t.Context(), property); err != nil {
		t.Fatal(err)
	}
	_, idx := commitStage(t, db, root, s)
	baseLimits := Limits{MaxNameBytes: 50000, MaxValueBytes: 64, MaxRecordBytes: 65728}
	resolved, err := baseLimits.resolve()
	if err != nil {
		t.Fatal(err)
	}
	axisWire, _ := encodeAxis(n, axis, resolved)
	axisCost := rootBytes + len(axisKey(n, d.ID)) + len(axisWire) + 64 + 27 + len(d.Reference) + len(d.CanonicalUnit)
	exactLimits := baseLimits
	exactLimits.MaxReadBytes = axisCost
	exact := openCatalog(t, db, idx, exactLimits)
	got, found, err := exact.Axis(t.Context(), d.ID)
	if err != nil || !found || got.Descriptor() != d {
		t.Fatal(found, err)
	}
	shortLimits := exactLimits
	shortLimits.MaxReadBytes--
	short := openCatalog(t, db, idx, shortLimits)
	got, found, err = short.Axis(t.Context(), d.ID)
	if !errors.Is(err, ErrResourceLimit) || found || got.Descriptor() != (temporal.AxisDescriptor{}) {
		t.Fatal("descriptor budget bypass/partial output", found, err)
	}
	propertyWire, _ := encodeProperty(n, property, resolved)
	propertyCost := rootBytes + len(schemaKey(n, property.Owner, property.Name)) + len(propertyWire) + 64 + len(property.Name)
	exactLimits.MaxReadBytes = propertyCost
	exact = openCatalog(t, db, idx, exactLimits)
	gotProperty, found, err := exact.Property(t.Context(), property.Owner, property.Name)
	if err != nil || !found || gotProperty != property {
		t.Fatal(found, err)
	}
	exactLimits.MaxReadBytes--
	short = openCatalog(t, db, idx, exactLimits)
	gotProperty, found, err = short.Property(t.Context(), property.Owner, property.Name)
	if !errors.Is(err, ErrResourceLimit) || found || gotProperty != (graphstate.PropertyDefinition{}) {
		t.Fatal("name budget bypass/partial output", found, err)
	}
	if _, err := short.Root(); err != nil {
		t.Fatal("budget failure poisoned catalog", err)
	}
}

func TestCatalogMultipleStageAggregateRelease(t *testing.T) {
	db, _ := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{MaxStages: 2, MaxStageRecords: 2})
	first := stage(t, c)
	second := stage(t, c)
	definition := func(name string) graphstate.PropertyDefinition {
		return graphstate.PropertyDefinition{Name: name, Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := first.Property(t.Context(), definition("a")); err != nil {
			t.Error(err)
		}
	})
	wg.Go(func() {
		if err := second.Property(t.Context(), definition("b")); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	before, _ := second.Writes()
	if err := second.Property(t.Context(), definition("c")); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	after, _ := second.Writes()
	if !sameWrites(before, after) {
		t.Fatal("aggregate rejection mutated surviving stage")
	}
	retained := second.bytes
	records := len(second.writes)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if first.writes != nil || c.stageBytes != retained || c.records != records || c.stages != 1 {
		t.Fatal("closed handle retained accounting/storage", c.stageBytes, c.records, c.stages)
	}
	if err := second.Property(t.Context(), definition("c")); err != nil {
		t.Fatal("closed stage capacity not released", err)
	}
	if len(second.writes) != 2 {
		t.Fatal("other stage lost records")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if c.stageBytes != 0 || c.records != 0 || c.stages != 0 {
		t.Fatal("aggregate accounting not empty")
	}
}
