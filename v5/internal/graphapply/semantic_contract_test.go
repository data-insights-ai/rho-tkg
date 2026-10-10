package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestSemanticContractIDFixedGoldenValueOwnershipAndMethod(t *testing.T) {
	// Independent fixed SHA-256 vectors, reviewed before implementation.
	const expected = "65cf602d484307ec3d716f967111a5e75fe7ce61fec191620c8f490cf684384f"
	id := SemanticContractID()
	if id == (raftlog.ApplicationSemanticContractID{}) || hex.EncodeToString(id[:]) != expected {
		t.Fatal("semantic agreement changed", id)
	}
	id[0] ^= 1
	again := SemanticContractID()
	if hex.EncodeToString(again[:]) != expected {
		t.Fatal("caller changed provider state", again)
	}
	var nilMachine *materializer
	if nilMachine.SemanticContractID() != again {
		t.Fatal("nil receiver lost static capability")
	}
	for _, m := range []*materializer{{}, {ns: codecNamespace(), owner: 9, limits: defaultMaterializerLimits()}} {
		if m.SemanticContractID() != again {
			t.Fatal("instance state entered semantic ID")
		}
		m.ns.graph[0], m.ns.partition, m.owner = m.ns.graph[0]+1, 99, 123
		m.limits.sourceRows, m.limits.outputBytes = 1, 1
		if m.SemanticContractID() != again {
			t.Fatal("namespace or local policy entered semantic ID")
		}
	}
	const changed = "rho-tkg:graphapply:semantic-contract:v1\x00current-root-commands=1\x00typed-native-values=1\x00graph-components=2\x00allocation-admission=1\x00request-recovery=1\x00logical-effects=1\x00"
	other := sha256.Sum256([]byte(changed))
	if hex.EncodeToString(other[:]) != "e264af57595b1642d9fe91faee5335b750a0157f024c29a392511f1578a9f077" || raftlog.ApplicationSemanticContractID(other) == again {
		t.Fatal("meaning revision did not change agreement", other)
	}
}

func semanticAxis(t *testing.T, profile temporal.Profile) temporal.Axis {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: profile, Version: 1, Reference: "semantic:v1", CanonicalUnit: "unit"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func semanticPosition(t *testing.T, a temporal.Axis, n int64) temporal.Position {
	t.Helper()
	var p temporal.Position
	var err error
	if a.Descriptor().Profile == temporal.ProfileIntegerZ {
		p, err = temporal.IntegerPosition(a, temporal.Int64(n))
	} else {
		p, err = temporal.RationalPosition(a, temporal.RationalInt64(n))
	}
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func semanticSpan(t *testing.T, a temporal.Axis, lo, hi int64, lowerClosed, upperClosed bool) temporal.Scope {
	t.Helper()
	lower, err := temporal.FiniteBound(semanticPosition(t, a, lo), lowerClosed)
	if err != nil {
		t.Fatal(err)
	}
	upper, err := temporal.FiniteBound(semanticPosition(t, a, hi), upperClosed)
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.Span(a, lower, upper, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSemanticContractNativeValueConformance(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ} {
		a := semanticAxis(t, profile)
		point, err := temporal.Point(semanticPosition(t, a, 0))
		if err != nil {
			t.Fatal(err)
		}
		span := semanticSpan(t, a, 0, 1, true, false)
		equal, err := point.SameSupport(span, temporal.Limits{})
		if err != nil || equal != (profile == temporal.ProfileIntegerZ) {
			t.Fatal("Z and Q support laws conflated", profile, equal, err)
		}
		left, err := graphstate.ScopeValue(point).EqualityKey(graphstate.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		right, err := graphstate.ScopeValue(span).EqualityKey(graphstate.Limits{})
		if err != nil || (left == right) != equal {
			t.Fatal("canonical equality disagrees with support", err)
		}
	}
	a := semanticAxis(t, temporal.ProfileRationalQ)
	third, err := temporal.ParseRational("2/6", temporal.Limits{})
	if err != nil || third.String() != "1/3" {
		t.Fatal("exact rational lost", third, err)
	}
	p, err := temporal.RationalPosition(a, third)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := temporal.AppendPosition(nil, p, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := temporal.DecodePosition(wire, a, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	r, ok := restored.Rational()
	if !ok || r.Numerator().String() != "1" || r.Denominator().String() != "3" {
		t.Fatal("rational rounded by codec", r)
	}
	lex := semanticAxis(t, temporal.ProfileLexicographicQN)
	first, err := temporal.LexPosition(lex, temporal.RationalInt64(12), temporal.Int64(0))
	if err != nil {
		t.Fatal(err)
	}
	next, err := first.Successor(temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	model, micro, ok := next.Lex()
	order, err := temporal.ComparePositions(first, next, temporal.Limits{})
	if err != nil || order != temporal.Less || !ok || model.String() != "12" || micro.String() != "1" {
		t.Fatal("microstep fabricated model time", model, micro, order, err)
	}
	if _, err := temporal.ComparePositions(p, first, temporal.Limits{}); !errors.Is(err, temporal.ErrAxisMismatch) {
		t.Fatal(err)
	}
	outer, inner := semanticSpan(t, a, 0, 2, true, true), semanticSpan(t, a, 0, 2, false, false)
	remaining, err := outer.Difference(inner, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	parts := remaining.Parts()
	if len(parts) != 2 || parts[0].Kind() != temporal.ScopePoint || parts[1].Kind() != temporal.ScopePoint {
		t.Fatal("endpoint difference lost singleton tails", parts)
	}
	for _, n := range []int64{0, 2} {
		if contains, err := remaining.Contains(semanticPosition(t, a, n), temporal.Limits{}); err != nil || !contains {
			t.Fatal(n, contains, err)
		}
	}
	if contains, err := remaining.Contains(p, temporal.Limits{}); err != nil || contains {
		t.Fatal("difference retained interior phantom", contains, err)
	}
}

func TestSemanticContractRequestRecoveryAndLogicalEffects(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	original, created := f.commit(t, r)
	if original.reason != reasonNone {
		t.Fatal(original)
	}
	root, err := graphstore.DecodeRoot(created.Image)
	if err != nil {
		t.Fatal(err)
	}
	if f.m.SemanticContractID() != SemanticContractID() {
		t.Fatal("live method differs")
	}
	noop := r
	noop.id = requestID{9}
	noop.operations = []graphstate.Operation{r.operations[1]}
	noop.operations[0].ValueID = 999
	noop.claims = nil
	o, b := f.commit(t, noop)
	if o.reason != reasonNone || len(b.Changes) != 0 || !bytes.Equal(b.Image, created.Image) {
		t.Fatal("semantic no-op advanced graph", o, b)
	}
	rejected := r
	rejected.id = requestID{10}
	rejected.operations = []graphstate.Operation{r.operations[0]}
	rejected.operations[0].Owner, rejected.operations[0].Record.ID = 15, 15
	rejected.claims = nil
	o, b = f.commit(t, rejected)
	if o.reason != reasonInvalid || len(b.Changes) != 0 || !bytes.Equal(b.Image, created.Image) {
		t.Fatal("rejection advanced graph", o, b)
	}
	control, b := f.control(t, codecRequests(t)[1])
	if control.reason != reasonNone || !bytes.Equal(b.Image, created.Image) {
		t.Fatal("allocator rotation advanced graph", control, b)
	}
	activate := codecRequests(t)[2]
	activate.id = requestID{11}
	activate.expectedEpoch = 1
	activate.session.Epoch = 2
	activate.session.Incarnation = [16]byte{99}
	control, b = f.control(t, activate)
	if control.reason != reasonNone || !bytes.Equal(b.Image, created.Image) {
		t.Fatal("recipient fence advanced graph", control, b)
	}
	replay, b := f.commit(t, r)
	if replay.disposition != requestReplay || replay.index != original.index || replay.hash != original.hash || replay.reason != original.reason || len(b.Changes)+len(b.Writes) != 0 || !bytes.Equal(b.Image, created.Image) {
		t.Fatal("delayed recovery rebound outcome", replay, b)
	}
	current, err := graphstore.DecodeRoot(b.Image)
	if err != nil || current.SemanticEpoch() != root.SemanticEpoch() || current.EffectDigest() != root.EffectDigest() {
		t.Fatal("non-graph commands changed logical chain", current, err)
	}
	changed := r
	changed.operations = []graphstate.Operation{r.operations[0]}
	o, b = f.commit(t, changed)
	if o.reason != reasonMismatch || len(b.Writes)+len(b.Changes) != 0 || !bytes.Equal(b.Image, created.Image) {
		t.Fatal("changed retry replaced mapping", o, b)
	}
	if again, _ := f.commit(t, r); again.index != original.index || again.hash != original.hash || again.disposition != requestReplay {
		t.Fatal("original outcome lost", again)
	}
}
