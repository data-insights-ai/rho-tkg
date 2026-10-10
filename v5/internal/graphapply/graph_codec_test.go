package graphapply

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func graphCodecRequest(t *testing.T) graphRequest {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "model:v1", CanonicalUnit: "second"}, temporal.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.All(a)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.NewRevision(3, 7)
	if err != nil {
		t.Fatal(err)
	}
	return graphRequest{ns: codecNamespace(), kind: graphOperations, id: requestID{8}, revision: revision, operations: []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 11, Life: 12, Scope: scope, Record: graphstate.EntityRecord{ID: 11, Kind: graphstate.Node, Axis: a}}, {Kind: graphstate.Set, Owner: 11, Life: 12, Scope: scope, Name: "answer", Value: graphstate.ScopeValue(scope), ValueID: 13}}, claims: []freshBinding{{role: entityBinding, id: 11, grant: grantReference{session: codecSession(), sequence: 1}}, {role: lifeBinding, owner: 11, id: 12, grant: grantReference{session: codecSession(), sequence: 1}}, {role: valueBinding, id: 13, grant: grantReference{session: codecSession(), sequence: 1}}}}
}
func TestGraphRequestCanonicalRoundTripAndHostileFraming(t *testing.T) {
	l := defaultMaterializerLimits()
	r := graphCodecRequest(t)
	wire, err := encodeGraphRequest(r, l)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphRequest(wire, l)
	if err != nil {
		t.Fatal(err)
	}
	again, err := encodeGraphRequest(decoded, l)
	if err != nil || !bytes.Equal(wire, again) {
		t.Fatal(err)
	}
	clear(wire)
	again2, err := encodeGraphRequest(decoded, l)
	if err != nil || !bytes.Equal(again, again2) {
		t.Fatal(err)
	}
	for n := range len(again) {
		if _, err := decodeGraphRequest(again[:n], l); !errors.Is(err, errCorrupt) {
			t.Fatalf("truncation%d: %v", n, err)
		}
	}
	hostile := append(owned(again), 0)
	resign(hostile)
	if _, err := decodeGraphRequest(hostile, l); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	l.commandBytes = len(again) - 1
	if _, err := decodeGraphRequest(again, l); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
}
func TestGraphOutcomeIsLeanAndRejectsRechecksummedInvalidUnion(t *testing.T) {
	o := outcome{ns: codecNamespace(), kind: graphOperations, identity: [16]byte{8}, hash: [32]byte{9}, index: 10, disposition: applied}
	b, err := encodeGraphOutcome(o)
	if err != nil || len(b) != 120 {
		t.Fatal(len(b), err)
	}
	got, err := decodeGraphOutcome(b, o.ns)
	if err != nil || got != o {
		t.Fatal(got, err)
	}
	for _, at := range []int{4, 85, 86} {
		bad := owned(b)
		bad[at] = 255
		resign(bad)
		if _, err := decodeGraphOutcome(bad, o.ns); !errors.Is(err, errCorrupt) {
			t.Fatal(at, err)
		}
	}
	for _, reason := range []reason{reasonLimit, reasonExhausted, reasonAlreadyInitialized} {
		bad := o
		bad.reason = reason
		if _, err := encodeGraphOutcome(bad); !errors.Is(err, errInvalid) {
			t.Fatal(reason, err)
		}
	}
	o.grant = codecGrant(t)
	if _, err := encodeGraphOutcome(o); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

func TestEveryTypedScalarRequestBranchAndHostilePayload(t *testing.T) {
	l := defaultMaterializerLimits()
	r := graphCodecRequest(t)
	finite, err := graphstate.F64(1.25)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []graphstate.Scalar{{}, graphstate.Null(), graphstate.String("literal-not-time"), graphstate.Bool(true), graphstate.I64(-8), finite, r.operations[1].Value} {
		r.operations[1].Value = value
		wire, err := encodeGraphRequest(r, l)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeGraphRequest(wire, l)
		if err != nil || got.operations[1].Value.Kind() != value.Kind() {
			t.Fatal(value.Kind(), err)
		}
		if value.Kind() != graphstate.ScalarInvalid {
			same, err := got.operations[1].Value.Equal(value, l.graph.Planner)
			if err != nil || !same {
				t.Fatal(value, err)
			}
		}
	}
	for _, payload := range [][]byte{{byte(graphstate.ScalarNull), 0}, {byte(graphstate.ScalarBool), 2}, {byte(graphstate.ScalarI64), 1}, {byte(graphstate.ScalarF64), 128, 0, 0, 0, 0, 0, 0, 0}, {99}} {
		w := boundedWriter{max: 1024}
		w.field(payload)
		w.u32(0)
		c := graphCursor{b: w.b, maxOwnedBytes: 1024}
		readScalar(&c, nil, l)
		if !errors.Is(c.err, errCorrupt) {
			t.Fatal(payload, c.err)
		}
	}
}
func manyPointScope(t *testing.T, wide bool) temporal.Scope {
	t.Helper()
	profile := temporal.ProfileIntegerZ
	if wide {
		profile = temporal.ProfileRationalQ
	}
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{19}, Profile: profile, Version: 1, Reference: "dense-native:v1", CanonicalUnit: "coordinate"}, temporal.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]temporal.Scope, 64)
	for i := range parts {
		var p temporal.Position
		if wide {
			numerator, err := temporal.ParseInteger("340282366920938463463374607431768211457", temporal.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			denominator := temporal.Int64(int64(2*i + 3))
			ratio, err := temporal.Fraction(numerator, denominator, temporal.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			p, err = temporal.RationalPosition(a, ratio)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			p, err = temporal.IntegerPosition(a, temporal.Int64(int64(2*i)))
			if err != nil {
				t.Fatal(err)
			}
		}
		parts[i], err = temporal.Point(p)
		if err != nil {
			t.Fatal(err)
		}
	}
	s, err := temporal.Region(a, parts, temporal.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestOwnedRegionPreflightExactBoundaryAndMetadata(t *testing.T) {
	l := defaultMaterializerLimits()
	// Independently measured Go metadata, including limb rounding headroom for
	// six possible wide integer components in the two coordinate bounds.
	fixed := 2*int(unsafe.Sizeof(temporal.Bound{})) + 6*(int(unsafe.Sizeof(big.Int{}))+4*int(unsafe.Sizeof(big.Word(0))))
	if fixed > scopeIntervalMetadataBytes || 2*int(unsafe.Sizeof(state.Change{})) > changeMetadataBytes || int(unsafe.Sizeof(graphstate.ValueWrite{})) > 256 || int(unsafe.Sizeof(graphstore.ComponentChangeGroup{})) > 512 {
		t.Fatal("metadata allowance undersized", fixed)
	}
	for _, wide := range []bool{false, true} {
		s := manyPointScope(t, wide)
		wire, err := temporal.AppendScope(nil, s, l.catalog.Temporal)
		if err != nil {
			t.Fatal(err)
		}
		cost, err := scopeBackingCost(wire, l)
		if err != nil || cost < 64*fixed {
			t.Fatal(cost, err)
		}
		t.Run(fmt.Sprint(wide), func(t *testing.T) {
			for _, delta := range []int{-1, 0, 1} {
				c := graphCursor{maxOwnedBytes: cost + delta}
				if ok := preflightScopeBacking(&c, wire, l); ok != (delta >= 0) || delta < 0 && !errors.Is(c.err, errLimit) {
					t.Fatal(delta, ok, c.err)
				}
				if delta >= 0 {
					decoded, err := temporal.DecodeScope(wire, s.Axis(), l.catalog.Temporal)
					if err != nil {
						t.Fatal(err)
					}
					canonical, err := temporal.AppendScope(nil, decoded, l.catalog.Temporal)
					if err != nil || !bytes.Equal(wire, canonical) {
						t.Fatal(err)
					}
				}
			}
		})
		// Both operation placement and Scope-valued property backing are charged.
		r := graphCodecRequest(t)
		r.operations[0].Scope = s
		r.operations[0].Record.Axis = s.Axis()
		r.operations[1].Scope = s
		r.operations[1].Value = graphstate.ScopeValue(s)
		encoded, err := encodeGraphRequest(r, l)
		if err != nil {
			t.Fatal(err)
		}
		lo, hi := 1, l.commandOwnedBytes
		for lo < hi {
			mid := (lo + hi) / 2
			probe := l
			probe.commandOwnedBytes = mid
			if _, err := decodeGraphRequest(encoded, probe); err == nil {
				hi = mid
			} else {
				if !errors.Is(err, errLimit) {
					t.Fatal(err)
				}
				lo = mid + 1
			}
		}
		if lo < 3*cost {
			t.Fatal("decoded scopes undercharged", lo, cost)
		}
		for _, delta := range []int{-1, 0, 1} {
			probe := l
			probe.commandOwnedBytes = lo + delta
			_, err := decodeGraphRequest(encoded, probe)
			if (err == nil) != (delta >= 0) || delta < 0 && !errors.Is(err, errLimit) {
				t.Fatal(delta, err)
			}
		}
	}
	bad := make([]byte, 57)
	copy(bad, []byte{'T', 'S', 1, byte(temporal.ProfileIntegerZ)})
	bad[52] = byte(temporal.ScopeRegion)
	binary.BigEndian.PutUint32(bad[53:], math.MaxUint32)
	if _, err := scopeBackingCost(bad, l); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
}
func TestTypedCDCBindingSchemasAndPhysicalIndependence(t *testing.T) {
	l := defaultMaterializerLimits()
	n := codecNamespace()
	g := graphChanges{ns: n, initialized: true, topology: 1, schema: 1, schemas: []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}}
	logical, err := encodeGraphChanges(g, l)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphChanges(logical, n, l)
	if err != nil || len(decoded.schemas) != 1 {
		t.Fatal(decoded, err)
	}
	other := g
	other.schemas = []graphstate.PropertyDefinition{{Name: "other", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	different, err := encodeGraphChanges(other, l)
	if err != nil {
		t.Fatal(err)
	}
	previous := [32]byte{1}
	if graphEffectDigest(previous, logical) == graphEffectDigest(previous, different) {
		t.Fatal("schema omitted from digest")
	}
	o := outcome{ns: n, kind: initGraph, identity: [16]byte{2}, hash: [32]byte{3}, index: 4, disposition: applied}
	envelope, err := encodeChangeEnvelope(o, logical, l)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeChangeEnvelope(envelope, o, l); err != nil {
		t.Fatal(err)
	}
	mismatched := o
	mismatched.index++
	if _, err := decodeChangeEnvelope(envelope, mismatched, l); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	// Bindings differ across local applied coordinates/generations/page layouts,
	// while the pure logical GCD digest stays identical. The stable ValueID is
	// semantic identity; physical ValueEntry.ordinal never appears in GCD.
	r := graphCodecRequest(t)
	logicalData := graphChanges{ns: n, entities: []graphstate.EntityRecord{r.operations[0].Record}, lives: []graphstate.LifeRecord{{Owner: 11, Life: 12}}, values: []graphstate.ValueWrite{{ID: 13, Value: r.operations[1].Value}}}
	wire, err := encodeGraphChanges(logicalData, l)
	if err != nil {
		t.Fatal(err)
	}
	digest := graphEffectDigest(previous, wire)
	again, err := encodeGraphChanges(logicalData, l)
	if err != nil || graphEffectDigest(previous, again) != digest {
		t.Fatal(err)
	}

	for size := range len(logical) {
		if _, err := decodeGraphChanges(logical[:size], n, l); !errors.Is(err, errCorrupt) {
			t.Fatal(size, err)
		}
	}
}

func TestChangesDecodedBackingPreflightsBeforeAllocation(t *testing.T) {
	l := defaultMaterializerLimits()
	scope := manyPointScope(t, false)
	old, err := state.New(scope.Axis(), l.graph.Planner.Component)
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(1, 0)
	result, err := old.Set(scope, state.Null(), revision, l.graph.Planner.Component)
	if err != nil {
		t.Fatal(err)
	}
	changes := result.Changes()
	wire, err := state.AppendChanges(nil, scope.Axis(), changes, state.CodecLimits{State: l.graph.Planner.Component, MaxEncodedBytes: l.changeBytes})
	if err != nil {
		t.Fatal(err)
	}
	c := graphCursor{maxOwnedBytes: 16 << 20}
	if !preflightChangesBacking(&c, wire, l) {
		t.Fatal(c.err)
	}
	cost := c.ownedBytes
	if cost < len(changes)*(changeMetadataBytes+scopeIntervalMetadataBytes) {
		t.Fatal(cost)
	}
	for _, delta := range []int{-1, 0, 1} {
		c := graphCursor{maxOwnedBytes: cost + delta}
		ok := preflightChangesBacking(&c, wire, l)
		if ok != (delta >= 0) || delta < 0 && !errors.Is(c.err, errLimit) {
			t.Fatal(delta, ok, c.err)
		}
	}
	g := graphChanges{ns: codecNamespace(), groups: []graphstore.ComponentChangeGroup{{Key: graphstate.ComponentKey{Owner: 11, Life: 12, Kind: graphstate.ScalarProperty, Name: "answer"}, Owned: scope, Changes: changes}}}
	encoded, err := encodeGraphChanges(g, l)
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := 1, l.outputBytes
	for lo < hi {
		mid := (lo + hi) / 2
		probe := l
		probe.outputBytes = mid
		if _, err := decodeGraphChanges(encoded, g.ns, probe); err == nil {
			hi = mid
		} else {
			if !errors.Is(err, errLimit) {
				t.Fatal(err)
			}
			lo = mid + 1
		}
	}
	if lo < cost+64*scopeIntervalMetadataBytes {
		t.Fatal("CC and region ownership undercharged", lo, cost)
	}
	for _, delta := range []int{-1, 0, 1} {
		probe := l
		probe.outputBytes = lo + delta
		decoded, err := decodeGraphChanges(encoded, g.ns, probe)
		if (err == nil) != (delta >= 0) || delta < 0 && !errors.Is(err, errLimit) {
			t.Fatal(delta, err)
		}
		if delta >= 0 && len(decoded.groups[0].Changes) != 64 {
			t.Fatal(decoded)
		}
	}
	bad := owned(wire)
	binary.BigEndian.PutUint32(bad[52:56], math.MaxUint32)
	c = graphCursor{maxOwnedBytes: 16 << 20}
	if preflightChangesBacking(&c, bad, l) || !errors.Is(c.err, errLimit) {
		t.Fatal(c.err)
	}
}
func TestGraphRequestRejectsUnionAndSchemaCorruption(t *testing.T) {
	l := defaultMaterializerLimits()
	r := graphCodecRequest(t)
	for _, mutate := range []func(*graphRequest){func(r *graphRequest) { r.kind = 99 }, func(r *graphRequest) { r.id = requestID{} }, func(r *graphRequest) { r.attempt = bootstrapAttemptID{1} }, func(r *graphRequest) { r.revision = state.Revision{} }, func(r *graphRequest) { r.schemas = []graphstate.PropertyDefinition{{}} }, func(r *graphRequest) { r.claims[0].owner = 1 }, func(r *graphRequest) { r.claims[1].owner = 0 }, func(r *graphRequest) { r.claims[0].grant.sequence = 0 }, func(r *graphRequest) { r.claims = append(r.claims, r.claims[0]) }} {
		bad := r
		bad.claims = slices.Clone(r.claims)
		mutate(&bad)
		if _, err := encodeGraphRequest(bad, l); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
	init := graphInit(t)
	for _, badSchemas := range [][]graphstate.PropertyDefinition{{{Name: " ", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}, {{Name: "a", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.SetCardinality, Unique: graphstate.UniqueScalar}}, {{Name: "b", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}, {Name: "a", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}} {
		init.schemas = badSchemas
		if _, err := encodeGraphRequest(init, l); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
	limits := l
	limits.maxOperations = 1
	if _, err := encodeGraphRequest(r, limits); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	limits = l
	limits.maxAxes = 1
	differentAxis, _ := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{2}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "other:v1", CanonicalUnit: "integer"}, l.catalog.Temporal)
	scope, _ := temporal.All(differentAxis)
	r.operations[1].Value = graphstate.ScopeValue(scope)
	if _, err := encodeGraphRequest(r, limits); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
}

func TestTypedLabelSetCDCAndRechecksummedSemanticCorruption(t *testing.T) {
	f := openGraphFixture(t)
	init := graphInit(t)
	init.schemas = []graphstate.PropertyDefinition{{Name: "tags", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.SetCardinality}}
	f.commit(t, init)
	f.control(t, codecRequests(t)[2])
	reserve := codecRequests(t)[3]
	reserve.count = 16
	reserve.sequence = 1
	f.control(t, reserve)
	r := graphCodecRequest(t)
	r.operations = []graphstate.Operation{r.operations[0], {Kind: graphstate.AddLabel, Owner: 11, Life: 12, Scope: r.operations[0].Scope, Name: "visible"}, {Kind: graphstate.Add, Owner: 11, Life: 12, Scope: r.operations[0].Scope, Name: "tags", Value: graphstate.String("blue"), ValueID: 13}}
	o, b := f.commit(t, r)
	if o.reason != reasonNone {
		t.Fatal(o)
	}
	cdc, err := decodeChangeEnvelope(b.Changes, o, f.m.limits)
	if err != nil || len(cdc.groups) != 3 || cdc.groups[1].Key.Kind != graphstate.Label || cdc.groups[2].Key.Kind != graphstate.SetMember || cdc.groups[2].Key.Member != 13 {
		t.Fatal(cdc, err)
	}
	logical, err := encodeGraphChanges(cdc, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func([]byte) []byte{func(b []byte) []byte { b[28] = 2; return b }, func(b []byte) []byte { binary.BigEndian.PutUint64(b[37:45], 1); return b }, func(b []byte) []byte { b[28] = 1; return b }, func(b []byte) []byte { return append(append(owned(b[:len(b)-32]), 0), make([]byte, 32)...) }} {
		bad := mutation(owned(logical))
		resign(bad)
		envelope, err := encodeChangeEnvelope(o, bad, f.m.limits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeChangeEnvelope(envelope, o, f.m.limits); !errors.Is(err, errCorrupt) {
			t.Fatal("semantic corruption survived checksums", err)
		}
	}
	// The real retained record is unchanged by hostile inspection.
	actual, err := f.s.ApplicationRecord(t.Context(), o.index, false, 1<<20)
	if err != nil || !bytes.Equal(actual, b.Changes) {
		t.Fatal(err)
	}
}
func TestInvalidMaterializerPoliciesRefuseBeforeMachineConstruction(t *testing.T) {
	f := openGraphFixture(t)
	for _, test := range []struct {
		name   string
		mutate func(*materializerLimits)
		want   error
	}{
		{"allocation input", func(l *materializerLimits) { l.allocation.inputBytes = 0 }, errInvalid},
		{"catalog read bytes", func(l *materializerLimits) { l.catalog.MaxReadBytes = 1 }, graphstore.ErrInvalid},
		{"graph source rows", func(l *materializerLimits) { l.graph.MaxSourceRows = -1 }, graphstore.ErrInvalid},
		{"command bytes", func(l *materializerLimits) { l.commandBytes = 0 }, errInvalid},
		{"axis count", func(l *materializerLimits) { l.maxAxes = 12289 }, errInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			l := defaultMaterializerLimits()
			test.mutate(&l)
			if _, err := newMaterializer(f.s, f.n, 1, l); !errors.Is(err, test.want) {
				t.Fatal(err)
			}
		})
	}
}
