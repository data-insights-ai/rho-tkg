package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/assertion"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestAssociationTwoPhaseRetainedRevisionsRetractionAndReopen(t *testing.T) {
	for _, owner := range []graphstate.EntityID{1, 3} {
		name := "node"
		if owner == 3 {
			name = "relationship"
		}
		t.Run(name, func(t *testing.T) {
			fs := vfs.NewMem()
			db, root := newStore(t, fs)
			c := openCatalog(t, db, 1, Limits{})
			s := stage(t, c)
			axis := testAxis(t, 1, temporal.ProfileRationalQ)
			for _, entity := range []graphstate.EntityRecord{
				{ID: 1, Kind: graphstate.Node, Axis: axis},
				{ID: 2, Kind: graphstate.Node, Axis: axis},
				{ID: 3, Kind: graphstate.Relationship, Axis: axis, Type: "LINK", Source: 1, Target: 2, Mode: graphstate.IdentityReference},
			} {
				if err := s.Entity(t.Context(), EntityRef{testNamespace().Graph, entity.ID}, entity); err != nil {
					t.Fatal(err)
				}
			}
			first := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: owner}, assertion.NativePlacement, axis)
			if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: first, PrimaryFor: refEntity(uint64(owner))}}, AssociationLimits{}); err != nil {
				t.Fatal(err)
			}
			root, one := commitStage(t, db, root, s)
			old := openCatalog(t, db, one, Limits{})
			oldRead, err := old.Association(t.Context(), first.Ref, AssociationLimits{})
			if err != nil {
				t.Fatal(err)
			}
			oldWire := associationWire(t, oldRead.Record)
			second := first
			second.Previous, second.Revision = 1, associationRevision(t, 2)
			second.Interpretation, second.Role = assertion.Observation, assertion.ObservationTime
			second.Placement.Native = associationPoint(t, axis, 13)
			s = stage(t, old)
			if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: second}}, AssociationLimits{}); err != nil {
				t.Fatal(err)
			}
			root, two := commitStage(t, db, root, s)
			current := openCatalog(t, db, two, Limits{})
			withdraw := second
			withdraw.Previous, withdraw.Revision, withdraw.Retracted = 2, associationRevision(t, 3), true
			s = stage(t, current)
			if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: withdraw}}, AssociationLimits{}); err != nil {
				t.Fatal(err)
			}
			root, three := commitStage(t, db, root, s)
			current = openCatalog(t, db, three, Limits{})
			// Historical queries occur after every correction/retraction, at real views.
			associationAssertRead(t, old, first)
			pOld, err := old.Primary(t.Context(), refEntity(uint64(owner)), AssociationLimits{})
			if err != nil || !pOld.Bound || pOld.Ref != first.Ref || !bytes.Equal(associationWire(t, pOld.Association.Record), oldWire) {
				t.Fatal("old named primary changed", err)
			}
			associationAssertRead(t, current, withdraw)
			pNow, err := current.Primary(t.Context(), refEntity(uint64(owner)), AssociationLimits{})
			if err != nil || !pNow.Bound || pNow.Ref != first.Ref || !bytes.Equal(associationWire(t, pNow.Association.Record), associationSpecWire(t, withdraw)) {
				t.Fatal("complete named primary changed after mutation/retraction/reopen", err)
			}
			for _, spec := range []assertion.Spec{first, second, withdraw} {
				r, err := current.AssociationRevision(t.Context(), spec.Ref, spec.Revision.ID(), AssociationLimits{})
				want, newErr := assertion.New(spec, assertion.Limits{})
				if err != nil || newErr != nil || !r.Found || !bytes.Equal(associationWire(t, want), associationWire(t, r.Record)) {
					t.Fatal("history replaced with current association", err, newErr)
				}
			}
			p, err := current.Primary(t.Context(), refEntity(uint64(owner)), AssociationLimits{})
			if err != nil || !p.Bound || p.Ref != first.Ref || !p.Association.Record.Spec().Retracted {
				t.Fatal("retracted primary fell back to older value", err)
			}
			s = stage(t, current)
			if err := s.Life(t.Context(), refLife(uint64(owner), 2), graphstate.LifeRecord{Owner: owner, Life: 2}); err != nil {
				t.Fatal(err)
			}
			_, four := commitStage(t, db, root, s)
			current = openCatalog(t, db, four, Limits{})
			associationAssertRead(t, current, withdraw) // independent life metadata did not synchronize association
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := raftlog.Open(raftlog.Config{Dir: "catalog", FS: fs, Application: raftlog.DefaultApplicationPolicy(1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			current = openCatalog(t, reopened, four, Limits{})
			associationAssertRead(t, current, withdraw)
			pNow, err = current.Primary(t.Context(), refEntity(uint64(owner)), AssociationLimits{})
			if err != nil || !pNow.Bound || pNow.Ref != first.Ref || !bytes.Equal(associationWire(t, pNow.Association.Record), associationSpecWire(t, withdraw)) {
				t.Fatal("complete named primary changed after mutation/retraction/reopen", err)
			}
			r, err := current.AssociationRevision(t.Context(), first.Ref, 1, AssociationLimits{})
			if err != nil || !r.Found || !bytes.Equal(oldWire, associationWire(t, r.Record)) {
				t.Fatal("old revision lost across durable reopen", err)
			}
		})
	}
}

func TestAssociationHistoricalReuseAndLateFailurePreserveExactStageWrites(t *testing.T) {
	db, root, c, axis := associationFixture(t)
	first := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	s := stage(t, c)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: first}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	root, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	second := first
	second.Previous, second.Revision = 1, associationRevision(t, 2)
	s = stage(t, c)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: second}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	_, index = commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	s = stage(t, c)
	if err := s.Value(t.Context(), refValue(8), graphstate.String("pre-existing stage write")); err != nil {
		t.Fatal(err)
	}
	before, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	valid := associationSpec(t, 11, first.Target, assertion.NativePlacement, axis)
	reuse := first
	reuse.Previous = 2
	result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: valid, PrimaryFor: refEntity(1)}, {Spec: reuse}}, AssociationLimits{})
	after, afterErr := s.Writes()
	if !errors.Is(err, assertion.ErrRevisionReuse) || !errors.Is(err, ErrInvalid) || afterErr != nil || !reflect.DeepEqual(result, AssociationResult{}) || !reflect.DeepEqual(before, after) {
		t.Fatal("late history-reuse refusal leaked pending binding/history/index", err, afterErr)
	}
	associationAssertRead(t, c, second)
	if result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: first}}, AssociationLimits{}); !errors.Is(err, assertion.ErrRevisionReuse) || !reflect.DeepEqual(result, AssociationResult{}) {
		t.Fatal("byte-identical older revision replay rewound current head", err)
	}
	associationAssertRead(t, c, second)
	if r, err := c.Association(t.Context(), valid.Ref, AssociationLimits{}); err != nil || r.Found {
		t.Fatal("failed batch introduced phantom assertion", err)
	}
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: valid}}, AssociationLimits{}); err != nil {
		t.Fatal("caller rejection poisoned subsequent operation", err)
	}
	// New assertions may share the same batch RevisionID; identity is the tuple.
	another := associationSpec(t, 12, first.Target, assertion.NoAssociation, temporal.Axis{})
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: another}}, AssociationLimits{}); err != nil {
		t.Fatal("bare revision ID wrongly treated as graph-global uniqueness", err)
	}
}

func TestAssociationPersistedLedgerCountsEachRetainedRevision(t *testing.T) {
	_, _, c, axis := associationFixture(t)
	for _, kind := range []assertion.PlacementKind{assertion.NoAssociation, assertion.NativePlacement, assertion.SymbolicPlacement} {
		s := stage(t, c)
		spec := associationSpec(t, assertion.ID(20+kind), assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, kind, axis)
		request := AssociationWrite{Spec: spec}
		if kind == assertion.NativePlacement {
			request.PrimaryFor = refEntity(1)
		}
		if _, err := s.Associations(t.Context(), []AssociationWrite{request}, AssociationLimits{}); err != nil {
			t.Fatal(err)
		}
		rows, err := s.Writes()
		if err != nil {
			t.Fatal(err)
		}
		firstBytes := 0
		for _, row := range rows {
			firstBytes += len(row.Key) + len(row.Value)
			if row.Key[0] == byte(axisRecord) {
				t.Fatal("existing axis definition duplicated in revision staging")
			}
		}
		wire := associationSpecWire(t, spec)
		stored := 0
		for _, row := range rows {
			if row.Key[0] == byte(associationRevisionRecord) {
				stored = len(row.Value)
				axisID, body, err := inspectAssociation(row.Value, testNamespace(), c.limits)
				if err != nil || !bytes.Equal(body, wire) || (axisID != (temporal.AxisID{})) != (kind == assertion.NativePlacement) {
					t.Fatal("persisted envelope reference contract", err)
				}
			}
		}
		second := spec
		second.Previous, second.Revision = 1, associationRevision(t, 2)
		if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: second}}, AssociationLimits{}); err != nil {
			t.Fatal(err)
		}
		rows, err = s.Writes()
		if err != nil {
			t.Fatal(err)
		}
		total := 0
		revisions := 0
		for _, row := range rows {
			total += len(row.Key) + len(row.Value)
			if row.Key[0] == byte(associationRevisionRecord) {
				revisions++
			}
		}
		if revisions != 2 || total-firstBytes != 41+stored {
			t.Fatal("revision retention omitted key/body multiplicity", total, firstBytes, stored)
		}
		t.Logf("placement=%d canonical=%d revision-envelope=%d first-key+value=%d two-revision-key+value=%d (head/posting/optional-primary included; engine MVCC/framing/WAL/RSS excluded)", kind, len(wire), stored, firstBytes, total)
	}
}

func TestAssociationConcurrentClosePreservesOperationalCause(t *testing.T) {
	db, root, c, axis := associationFixture(t)
	s := stage(t, c)
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	_, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			<-start
			for range 50 {
				r, err := c.Association(t.Context(), spec.Ref, AssociationLimits{})
				if err == nil {
					if !r.Found {
						t.Error("completed read lost association")
					}
				} else if !errors.Is(err, raftlog.ErrClosed) || errors.Is(err, ErrCorrupt) || !reflect.DeepEqual(r, AssociationRead{}) {
					t.Error("view closure relabeled record corruption or exposed partial read", err)
				}
			}
		})
	}
	close(start)
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

// Err is called once outside and once inside each backend Get. For a native
// record: Current visits head/revision/axis before target; Revision visits
// revision/axis. The selected even-numbered call closes outside the view lock,
// after decoding the record and immediately before fetching its target.
type associationCloseBeforeTarget struct {
	context.Context
	view           *raftlog.ApplicationView
	calls, closeAt int
	closeErr       error
}

func (c *associationCloseBeforeTarget) Err() error {
	c.calls++
	if c.calls == c.closeAt {
		c.closeErr = c.view.Close()
	}
	return c.Context.Err()
}

func TestAssociationCloseExactlyBeforeTargetPreservesCause(t *testing.T) {
	for _, revisionDoor := range []bool{false, true} {
		db, root, c, axis := associationFixture(t)
		s := stage(t, c)
		spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
		if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); err != nil {
			t.Fatal(err)
		}
		_, index := commitStage(t, db, root, s)
		c = openCatalog(t, db, index, Limits{})
		closeAt := 8
		if revisionDoor {
			closeAt = 6
		}
		ctx := &associationCloseBeforeTarget{Context: t.Context(), view: c.view, closeAt: closeAt}
		var got AssociationRead
		var err error
		if revisionDoor {
			got, err = c.AssociationRevision(ctx, spec.Ref, 1, AssociationLimits{})
		} else {
			got, err = c.Association(ctx, spec.Ref, AssociationLimits{})
		}
		if ctx.calls != closeAt || ctx.closeErr != nil || !errors.Is(err, raftlog.ErrClosed) || errors.Is(err, ErrCorrupt) || !reflect.DeepEqual(got, AssociationRead{}) {
			t.Fatalf("target-phase closure mislabeled or failed to reach barrier: revision=%v calls=%d close=%v read=%v", revisionDoor, ctx.calls, ctx.closeErr, err)
		}
	}
}

func TestAssociationStaleApplicationBaseCannotInstallStagedEffects(t *testing.T) {
	db, root, old, axis := associationFixture(t)
	s := stage(t, old)
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec, PrimaryFor: refEntity(1)}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	base, err := old.view.Root()
	if err != nil {
		t.Fatal(err)
	}
	other := stage(t, old)
	if err := other.Value(t.Context(), refValue(8), graphstate.String("concurrent admitted change")); err != nil {
		t.Fatal(err)
	}
	_, current := commitStage(t, db, root, other)
	index := current + 1
	e := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("stale association command")}
	if err := db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{e}}); err != nil {
		t.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: base.Image, Writes: rows, Changes: []byte("not installed"), Outcome: []byte("not acknowledged")}
	if err := db.InstallApplication(index, batch); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("stale root installed staged associations", err)
	}
	fresh := openCatalog(t, db, current, Limits{})
	if r, err := fresh.Association(t.Context(), spec.Ref, AssociationLimits{}); err != nil || r.Found {
		t.Fatal("stale association became visible", err)
	}
	if p, err := fresh.Primary(t.Context(), refEntity(1), AssociationLimits{}); err != nil || p.Bound {
		t.Fatal("stale primary became visible", err)
	}
	got, _, err := db.Checkpoint()
	if err != nil || got != current {
		t.Fatal("failed install advanced applied checkpoint", err)
	}
}

func TestAssociationActualRetainedApplicationLedger(t *testing.T) {
	for _, kind := range []assertion.PlacementKind{assertion.NoAssociation, assertion.NativePlacement, assertion.SymbolicPlacement} {
		db, root, c, axis := associationFixture(t)
		baseline, err := db.ApplicationUsage()
		if err != nil {
			t.Fatal(err)
		}
		spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, kind, axis)
		s := stage(t, c)
		request := AssociationWrite{Spec: spec}
		if kind == assertion.NativePlacement {
			request.PrimaryFor = refEntity(1)
		}
		if _, err := s.Associations(t.Context(), []AssociationWrite{request}, AssociationLimits{}); err != nil {
			t.Fatal(err)
		}
		root, first := commitStage(t, db, root, s)
		one, err := db.ApplicationUsage()
		if err != nil {
			t.Fatal(err)
		}
		c = openCatalog(t, db, first, Limits{})
		second := spec
		second.Previous, second.Revision = 1, associationRevision(t, 2)
		s = stage(t, c)
		if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: second}}, AssociationLimits{}); err != nil {
			t.Fatal(err)
		}
		_, _ = commitStage(t, db, root, s)
		two, err := db.ApplicationUsage()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("placement=%d retained-application first bytes/records=%d/%d second bytes/records=%d/%d (includes MVCC head versions, roots and fixture change/outcome framing; excludes WAL/SST/replicas/RSS)", kind, one.RetainedBytes-baseline.RetainedBytes, one.RetainedRecords-baseline.RetainedRecords, two.RetainedBytes-one.RetainedBytes, two.RetainedRecords-one.RetainedRecords)
	}
}

// These benchmarks expose prototype overhead. Staging is not a durable commit,
// and neither allocation count nor these tiny records establish engine capacity.
func BenchmarkAssociationPrototype(b *testing.B) {
	axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "benchmark", CanonicalUnit: "unit"}, temporal.Limits{})
	if err != nil {
		b.Fatal(err)
	}
	spec := associationSpec(b, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	r, err := assertion.New(spec, assertion.Limits{})
	if err != nil {
		b.Fatal(err)
	}
	db, err := raftlog.Open(raftlog.Config{Dir: "bench-association", FS: vfs.NewMem(), Create: true, Application: raftlog.DefaultApplicationPolicy(1)})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	root, err := NewRoot(testNamespace(), 3)
	if err != nil {
		b.Fatal(err)
	}
	image, err := EncodeRoot(root)
	if err != nil {
		b.Fatal(err)
	}
	if err := db.Initialize([]uint64{1}, image); err != nil {
		b.Fatal(err)
	}
	v, err := db.ApplicationView(1)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := v.Close(); err != nil {
			b.Error(err)
		}
	})
	c, err := OpenCatalog(v, testNamespace(), 3, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	s, err := c.NewStage(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	if err := s.Entity(b.Context(), refEntity(1), graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: axis}); err != nil {
		b.Fatal(err)
	}
	if _, err := s.Associations(b.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); err != nil {
		b.Fatal(err)
	}
	rows, err := s.Writes()
	if err != nil {
		b.Fatal(err)
	}
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
	e := &pb.Entry{Index: new(uint64(2)), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("benchmark catalog setup")}
	if err := db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(2))}, Entries: []*pb.Entry{e}}); err != nil {
		b.Fatal(err)
	}
	if err := db.InstallApplication(2, raftlog.ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, Writes: rows}); err != nil {
		b.Fatal(err)
	}
	current, err := db.ApplicationView(2)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := current.Close(); err != nil {
			b.Error(err)
		}
	})
	c, err = OpenCatalog(current, testNamespace(), 3, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Serialize", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := assertion.AppendRecord(nil, r, assertion.Limits{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("CurrentLookup", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := c.Association(b.Context(), spec.Ref, AssociationLimits{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	write := spec
	write.Previous, write.Revision = 1, associationRevision(b, 2)
	b.Run("StageRevisionAndCopyWrites", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s, err := c.NewStage(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			if _, err := s.Associations(b.Context(), []AssociationWrite{{Spec: write}}, AssociationLimits{}); err != nil {
				b.Fatal(err)
			}
			if _, err := s.Writes(); err != nil {
				b.Fatal(err)
			}
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
