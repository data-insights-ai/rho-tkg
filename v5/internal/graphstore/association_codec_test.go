package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/assertion"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestAssociationPersistedCorruptionStopsServing(t *testing.T) {
	for _, name := range []string{"zero-axis", "missing-axis", "conflicting-axis", "unknown-child-version", "wrong-assertion-id", "dangling-head", "missing-posting", "wrong-primary"} {
		t.Run(name, func(t *testing.T) {
			db, root, c, axis := associationFixture(t)
			s := stage(t, c)
			spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
			if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec, PrimaryFor: refEntity(1)}}, AssociationLimits{}); err != nil {
				t.Fatal(err)
			}
			root, index := commitStage(t, db, root, s)
			c = openCatalog(t, db, index, Limits{})
			r, err := c.Association(t.Context(), spec.Ref, AssociationLimits{})
			if err != nil {
				t.Fatal(err)
			}
			limits, err := (AssociationLimits{}).resolve()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := encodeAssociation(testNamespace(), r.Record, limits.bounded(c.limits), c.limits)
			if err != nil {
				t.Fatal(err)
			}
			row := raftlog.KV{Key: associationRevisionKey(testNamespace(), 10, 1), Value: encoded}
			switch name {
			case "zero-axis":
				clear(row.Value[29:45])
			case "missing-axis":
				row.Value[29] = 9
			case "conflicting-axis":
				// Registry definition is valid under the same ID but conflicts with
				// the existing canonical scope's committed full definition hash.
				d := axis.Descriptor()
				d.Reference = "other-clock"
				other, err := temporal.NewAxis(d, temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				b, err := encodeAxis(testNamespace(), other, c.limits)
				if err != nil {
					t.Fatal(err)
				}
				row = raftlog.KV{Key: axisKey(testNamespace(), d.ID), Value: b}
			case "unknown-child-version":
				row.Value[49+2] = 99
			case "wrong-assertion-id":
				binary.BigEndian.PutUint64(row.Value[49+19:], 11)
			case "dangling-head":
				row = raftlog.KV{Key: associationHeadKey(testNamespace(), 10), Value: encodeNumber(testNamespace(), associationHeadRecord, 999)}
			case "missing-posting":
				row = raftlog.KV{Key: associationPostingKey(testNamespace(), spec.Target, 10), Deleted: true}
			case "wrong-primary":
				row = raftlog.KV{Key: associationPrimaryKey(testNamespace(), 1), Value: encodeNumber(testNamespace(), associationPrimaryRecord, 999)}
			}
			_, index = commitRows(t, db, root, []raftlog.KV{row})
			bad := openCatalog(t, db, index, Limits{})
			if name == "wrong-primary" {
				p, err := bad.Primary(t.Context(), refEntity(1), AssociationLimits{})
				if !errors.Is(err, ErrCorrupt) || !reflect.DeepEqual(p, PrimaryRead{}) {
					t.Fatal("corrupt binding exposed partial metadata", err)
				}
			} else {
				got, err := bad.Association(t.Context(), spec.Ref, AssociationLimits{})
				if !errors.Is(err, ErrCorrupt) || !reflect.DeepEqual(got, AssociationRead{}) {
					t.Fatal("corrupt native envelope/head/posting served or mislabeled", err)
				}
			}
			if _, err := bad.Association(t.Context(), assertion.Ref{Graph: spec.Ref.Graph, ID: 999}, AssociationLimits{}); !errors.Is(err, ErrPoisoned) {
				t.Fatal("semantic corruption did not stop catalog", err)
			}
			associationAssertRead(t, c, spec) // retained older root remains exact
		})
	}
}

func TestAssociationCodecFramingAndPolicySentinels(t *testing.T) {
	_, _, c, axis := associationFixture(t)
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	r, err := assertion.New(spec, assertion.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	l, err := (AssociationLimits{}).resolve()
	if err != nil {
		t.Fatal(err)
	}
	l = l.bounded(c.limits)
	wire, err := encodeAssociation(testNamespace(), r, l, c.limits)
	if err != nil {
		t.Fatal(err)
	}
	for n := range len(wire) {
		if _, _, err := inspectAssociation(wire[:n], testNamespace(), c.limits); !errors.Is(err, ErrCorrupt) {
			t.Fatal("truncated catalog association frame", n, err)
		}
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 0 },
		func(b []byte) { b[2] = 99 },
		func(b []byte) { b[28] = 9 },
		func(b []byte) { binary.BigEndian.PutUint32(b[45:], ^uint32(0)) },
	} {
		bad := bytes.Clone(wire)
		mutate(bad)
		if _, _, err := inspectAssociation(bad, testNamespace(), c.limits); !errors.Is(err, ErrCorrupt) {
			t.Fatal("malformed/unknown frame accepted", err)
		}
	}
	for _, p := range []AssociationLimits{{MaxOperations: -1}, {MaxReadRows: -1}, {MaxReadBytes: -1}, {MaxOutputBytes: -1}, {MaxOperations: 4097}} {
		if err := p.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid association policy sentinel", err)
		}
		if got, err := c.Association(t.Context(), spec.Ref, p); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(got, AssociationRead{}) {
			t.Fatal("invalid read policy exposed partial output", err)
		}
	}
	if err := DefaultAssociationLimits().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := AssociationLimits{Record: assertion.Limits{Temporal: temporal.Limits{MaxMagnitudeBits: -1}}}
	if err := bad.Validate(); !errors.Is(err, temporal.ErrInvalidLimits) {
		t.Fatal("child policy sentinel lost", err)
	}
	if _, err := c.Association(t.Context(), spec.Ref, bad); !errors.Is(err, temporal.ErrInvalidLimits) || !errors.Is(err, ErrInvalid) {
		t.Fatal("child invalid policy at public layer", err)
	}
}

func TestAssociationBindingOutputExactFitAndTightReadersRecover(t *testing.T) {
	db, root, c, axis := associationFixture(t)
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	r, err := assertion.New(spec, assertion.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	d := axis.Descriptor()
	// Independent ledger: result+transition capacity+binding capacity+record
	// header, canonical record and exactly one retained axis definition.
	exact := 64 + 64 + 96 + 64 + len(associationWire(t, r)) + 27 + len(d.Reference) + len(d.CanonicalUnit)
	s := stage(t, c)
	before, _ := s.Writes()
	result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec, PrimaryFor: refEntity(1)}}, AssociationLimits{MaxOutputBytes: exact - 1})
	after, _ := s.Writes()
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(result, AssociationResult{}) || !reflect.DeepEqual(before, after) {
		t.Fatal("one-short binding output leaked staged effects", err)
	}
	result, err = s.Associations(t.Context(), []AssociationWrite{{Spec: spec, PrimaryFor: refEntity(1)}}, AssociationLimits{MaxOutputBytes: exact})
	if err != nil || len(result.Bindings) != 1 {
		t.Fatal("exact-fit binding/result refused", err)
	}
	_, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	for _, p := range []AssociationLimits{
		{Record: assertion.Limits{MaxRoleBytes: 1}},
		{Record: assertion.Limits{MaxRecordBytes: len(associationWire(t, r))}},
	} {
		if got, err := c.Association(t.Context(), spec.Ref, p); !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrCorrupt) || !reflect.DeepEqual(got, AssociationRead{}) {
			t.Fatal("legal retained record under tight policy became corruption", err)
		}
		associationAssertRead(t, c, spec)
	}
}

func TestAssociationScansClampActualCustomApplicationPolicy(t *testing.T) {
	p := raftlog.DefaultApplicationPolicy(1)
	p.MaxValueBytes, p.MaxPageBytes = 8192, 12288
	p.MaxChangeBytes, p.MaxOutcomeBytes = 8192, 4096
	db, err := raftlog.Open(raftlog.Config{Dir: "custom-association", FS: vfs.NewMem(), Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	root, err := NewRoot(testNamespace(), 3)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Initialize([]uint64{1}, image); err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	axis := testAxis(t, 1, temporal.ProfileRationalQ)
	if err := s.Entity(t.Context(), refEntity(1), graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: axis}); err != nil {
		t.Fatal(err)
	}
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	_, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	query := AssociationQuery{Graph: testNamespace().Graph, Target: spec.Target}
	page, err := c.Associations(t.Context(), query, nil, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}, AssociationLimits{})
	if err != nil || !page.Complete || len(page.Records) != 1 {
		t.Fatal("default/aggregate budget was not clamped to actual engine policy", err)
	}
}

func FuzzAssociationStoredEnvelopeFraming(f *testing.F) {
	f.Add([]byte{})
	f.Add(append(recordHeader(testNamespace(), associationRevisionRecord), 0, 0, 0, 0, 0))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		before := bytes.Clone(b)
		l, err := (Limits{}).resolve()
		if err != nil {
			t.Fatal(err)
		}
		_, _, _ = inspectAssociation(b, testNamespace(), l)
		if !bytes.Equal(before, b) {
			t.Fatal("framing mutated delivered bytes")
		}
	})
}
