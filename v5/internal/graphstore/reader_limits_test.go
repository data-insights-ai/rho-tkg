package graphstore

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func readerLimitCatalog(t *testing.T) *Catalog {
	t.Helper()
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	a := testAxis(t, 1, temporal.ProfileIntegerZ)
	if err := s.Entity(t.Context(), refEntity(1), graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: a}); err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.All(a)
	if err != nil {
		t.Fatal(err)
	}
	value := graphstate.ScopeValue(scope)
	if err := s.Value(t.Context(), refValue(99), value); err != nil {
		t.Fatal(err)
	}
	_, index := commitStage(t, db, root, s)
	return openCatalog(t, db, index, Limits{})
}
func TestReaderNestedAxisCapsBeforeLookup(t *testing.T) {
	c := readerLimitCatalog(t)
	wide, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := wide.entity(refEntity(1))
	if err != nil || !found || wide.rows != 2 {
		t.Fatal(err)
	}
	row, found, err := c.view.Get(t.Context(), entityKey(testNamespace(), 1), 1024)
	if err != nil || !found {
		t.Fatal(err)
	}
	entity, err := readEntity(row.Value, testNamespace(), c.limits)
	if err != nil {
		t.Fatal(err)
	}
	d := entity.Axis.Descriptor()
	firstBytes := rootBytes + len(row.Key) + len(row.Value) + 64 + 27 + len(d.Reference) + len(d.CanonicalUnit)
	for _, cap := range []struct{ rows, bytes int }{{1, 0}, {2, firstBytes}} {
		q, err := c.reader(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		q.maxRows, q.maxBytes = cap.rows, cap.bytes
		_, found, err := q.entity(refEntity(1))
		if !errors.Is(err, ErrResourceLimit) || found || q.rows != 1 || q.bytes != firstBytes {
			t.Fatalf("nested lookup exceeded cap rows%d bytes%d found%v err%v", q.rows, q.bytes, found, err)
		}
		if _, err := c.Root(); err != nil {
			t.Fatal("ordinary refusal poisoned", err)
		}
	}
	q, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	q.maxBytes = rootBytes + len(row.Key) + len(row.Value) + 64 - 1
	_, _, err = q.entity(refEntity(1))
	if !errors.Is(err, raftlog.ErrLimit) || q.rows != 1 || q.bytes != rootBytes {
		t.Fatalf("before-copy refusal %+v %v", q, err)
	}
	if mapped := c.failure(err); !errors.Is(mapped, ErrResourceLimit) || !errors.Is(mapped, raftlog.ErrLimit) {
		t.Fatal(mapped)
	}
	if _, err := c.Root(); err != nil {
		t.Fatal(err)
	}
}
func TestReaderValueMaterializationAndZeroCaps(t *testing.T) {
	c := readerLimitCatalog(t)
	wide, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	entry, key, found, err := wide.value(refValue(99))
	if err != nil || !found {
		t.Fatal(err)
	}
	scope, ok := entry.Value.Scope()
	if !ok {
		t.Fatal("native scope lost")
	}
	d := scope.Axis().Descriptor()
	extra := len(key) + 27 + len(d.Reference) + len(d.CanonicalUnit)
	tight, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	tight.maxRows, tight.maxBytes = wide.rows, wide.bytes-1
	_, _, found, err = tight.value(refValue(99))
	if !errors.Is(err, ErrResourceLimit) || found || tight.rows != wide.rows || tight.bytes != wide.bytes-extra {
		t.Fatalf("last materialization charge rows%d bytes%d full%d extra%d err%v", tight.rows, tight.bytes, wide.bytes, extra, err)
	}
	if _, err := c.Root(); err != nil {
		t.Fatal(err)
	}
	exact, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	exact.maxRows, exact.maxBytes = wide.rows, wide.bytes
	value, _, found, err := exact.value(refValue(99))
	if err != nil || !found || value.Ref != entry.Ref || exact.rows != wide.rows || exact.bytes != wide.bytes {
		t.Fatal("exact-fit value", err)
	}
	zero, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	value, _, found, err = zero.value(refValue(99))
	if err != nil || !found || value.Ref != entry.Ref || zero.bytes != wide.bytes || zero.rows != wide.rows {
		t.Fatal("zero changed behavior", err)
	}
	q, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	q.maxBytes = rootBytes + 3
	if err := q.materialize(3); err != nil {
		t.Fatal(err)
	}
	if err := q.materialize(1); !errors.Is(err, ErrResourceLimit) || q.bytes != rootBytes+3 {
		t.Fatal(err)
	}
	if err := q.materialize(-1); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}
func TestReaderClampsUnderlyingApplicationReadPolicy(t *testing.T) {
	root, err := NewRoot(testNamespace(), 3)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	policy := raftlog.DefaultApplicationPolicy(1)
	policy.MaxKeyBytes = 128
	policy.MaxValueBytes = 512
	policy.MaxPageBytes = 1024
	policy.MaxChangeBytes = 512
	policy.MaxOutcomeBytes = 512
	db, err := raftlog.Open(raftlog.Config{Dir: "small-read", FS: vfs.NewMem(), Create: true, Application: policy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := db.Initialize([]uint64{1}, wire); err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	a := testAxis(t, 1, temporal.ProfileIntegerZ)
	if c.limits.MaxRecordBytes <= policy.MaxPageBytes {
		t.Fatal("fixture does not exercise clamp")
	}
	if err := s.Axis(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	_, index := commitStage(t, db, root, s)
	current := openCatalog(t, db, index, Limits{})
	axis, found, err := current.Axis(t.Context(), a.Descriptor().ID)
	if err != nil || !found || axis.Descriptor() != a.Descriptor() {
		t.Fatal("tiny valid read rejected", err)
	}
	q, err := current.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	q.maxBytes = 2048
	q.maxRows = 1
	axis, found, err = q.axis(a.Descriptor().ID)
	if err != nil || !found || axis.DefinitionHash() != a.DefinitionHash() || q.rows != 1 {
		t.Fatal("small policy nested read", err)
	}
}
