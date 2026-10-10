package types_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func defaultBinding(t testing.TB, graph types.GraphID, axis temporal.AxisID) types.DefaultAxisBinding {
	t.Helper()
	b, err := types.NewDefaultAxisBinding(graph, axis, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDefaultAxisBindingExplicitIdentitiesPolicyAndConfigRoundtrip(t *testing.T) {
	graph, axisID := types.GraphID{3}, temporal.AxisID{19}
	b := defaultBinding(t, graph, axisID)
	d := b.Axis().Descriptor()
	if b.Graph() != graph || d.ID != axisID || d.Profile != temporal.ProfileRationalQ || d.Version != 1 || d.CanonicalUnit != "millisecond" || d.Reference != "posix-unix-epoch:1970-01-01T00:00:00Z:v1" {
		t.Fatal("factory changed supplied identity or default coordinate policy", b.Graph(), d)
	}
	if err := b.Check(graph, b.Axis(), temporal.Limits{}); err != nil {
		t.Fatal(err)
	}
	// This private application configuration exercises a full value roundtrip,
	// not a rho persistence format, installed graph root or replication promise.
	type configuration struct {
		Graph types.GraphID
		Axis  temporal.AxisDescriptor
	}
	wire, err := json.Marshal(configuration{b.Graph(), d})
	if err != nil {
		t.Fatal(err)
	}
	var config configuration
	if err := json.Unmarshal(wire, &config); err != nil {
		t.Fatal(err)
	}
	axis, err := temporal.NewAxis(config.Axis, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := types.BindDefaultAxis(config.Graph, axis, temporal.Limits{})
	if err != nil || restored != b || restored.Axis().DefinitionHash() != b.Axis().DefinitionHash() {
		t.Fatal("configuration lost graph or complete descriptor", restored, err)
	}
	if err := restored.Check(graph, b.Axis(), temporal.Limits{}); err != nil {
		t.Fatal(err)
	}
	// Mutating returned identity/descriptor values cannot modify the binding.
	config.Graph[0]++
	config.Axis.ID[0]++
	config.Axis.Reference = "a-different-reference"
	if b.Graph() != graph || b.Axis().Descriptor() != d {
		t.Fatal("accessor/config values aliased binding")
	}
	var zero types.DefaultAxisBinding
	if zero.Graph() != (types.GraphID{}) || zero.Axis() != (temporal.Axis{}) || !errors.Is(zero.Check(graph, b.Axis(), temporal.Limits{}), types.ErrInvalidGraphIdentity) {
		t.Fatal("zero binding selected a global default")
	}
}

func TestDefaultAxisBindingTwoPhaseReplacementRetainsOldDesignation(t *testing.T) {
	graph := types.GraphID{3}
	old := defaultBinding(t, graph, temporal.AxisID{19})
	oldDescriptor := old.Axis().Descriptor()
	oldPosition, err := types.InstantPosition(old.Axis(), 17, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	current := defaultBinding(t, graph, temporal.AxisID{20})
	for _, tc := range []struct {
		binding types.DefaultAxisBinding
		axis    temporal.Axis
	}{{current, old.Axis()}, {old, current.Axis()}} {
		err := tc.binding.Check(graph, tc.axis, temporal.Limits{})
		if !errors.Is(err, types.ErrDefaultAxisMismatch) || !errors.Is(err, temporal.ErrAxisMismatch) {
			t.Fatal("replacement accepted other designation", err)
		}
	}
	if old.Axis().Descriptor() != oldDescriptor || old.Axis().DefinitionHash() == current.Axis().DefinitionHash() || current.Axis().Descriptor().ID != (temporal.AxisID{20}) {
		t.Fatal("factory reused global or derived identity")
	}
	if err := old.Check(graph, oldPosition.Axis(), temporal.Limits{}); err != nil {
		t.Fatal("old immutable designation changed after replacement", err)
	}
	value, err := types.InstantFromPosition(oldPosition, old.Axis(), temporal.Limits{})
	if err != nil || value != 17 {
		t.Fatal(value, err)
	}
	axis, err := temporal.NewAxis(oldDescriptor, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := types.BindDefaultAxis(graph, axis, temporal.Limits{})
	if err != nil || restored != old {
		t.Fatal("retained configuration no longer restores", err)
	}
	// Graph identity is independent: matching actual axis must not invent an
	// axis mismatch when the supplied graph alone is different.
	other := defaultBinding(t, types.GraphID{4}, oldDescriptor.ID)
	err = old.Check(other.Graph(), other.Axis(), temporal.Limits{})
	if !errors.Is(err, types.ErrDefaultAxisMismatch) || errors.Is(err, temporal.ErrAxisMismatch) {
		t.Fatal("graph mismatch was confused with axis mismatch", err)
	}
}

func TestDefaultAxisBindingInstantExactDomainAndOriginalProfiles(t *testing.T) {
	b := defaultBinding(t, types.GraphID{3}, temporal.AxisID{19})
	for _, value := range []types.Instant{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		p, err := types.InstantPosition(b.Axis(), value, temporal.Limits{})
		if err != nil || p.Profile() != temporal.ProfileRationalQ {
			t.Fatal(value, p, err)
		}
		if err := b.Check(b.Graph(), p.Axis(), temporal.Limits{}); err != nil {
			t.Fatal(err)
		}
		got, err := types.InstantFromPosition(p, b.Axis(), temporal.Limits{MaxValueBytes: 8})
		if err != nil || got != value {
			t.Fatal(value, got, err)
		}
	}
	// Millisecond units do not discretize Q: a genuine one-unit span is not
	// the singleton that the same endpoints would denote on discrete Z.
	zero, err := types.InstantPosition(b.Axis(), 0, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	one, err := types.InstantPosition(b.Axis(), 1, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	lo, err := temporal.FiniteBound(zero, true)
	if err != nil {
		t.Fatal(err)
	}
	hi, err := temporal.FiniteBound(one, false)
	if err != nil {
		t.Fatal(err)
	}
	span, err := temporal.Span(b.Axis(), lo, hi, temporal.Limits{})
	if err != nil || span.Kind() != temporal.ScopeSpan {
		t.Fatal(span.Kind(), err)
	}
	if contains, err := span.Contains(scalar(t, b.Axis(), "1/3"), temporal.Limits{}); err != nil || !contains {
		t.Fatal(contains, err)
	}
	for _, tc := range []struct {
		text string
		want error
	}{{"1/3", temporal.ErrNonintegralInstantCodec}, {"9223372036854775808", temporal.ErrInstantCodecRange}} {
		p := scalar(t, b.Axis(), tc.text)
		before, err := temporal.AppendPosition(nil, p, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := types.InstantFromPosition(p, b.Axis(), temporal.Limits{})
		if got != 0 || !errors.Is(err, tc.want) {
			t.Fatal(tc.text, got, err)
		}
		decoded, err := temporal.DecodePosition(before, b.Axis(), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		after, err := temporal.AppendPosition(nil, decoded, temporal.Limits{})
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("compatibility codec narrowed exact Q", err)
		}
	}
	p, err := temporal.ConvertUnits(temporal.RationalInt64(1000), "microsecond", b.Axis(), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := types.InstantFromPosition(p, b.Axis(), temporal.Limits{}); err != nil || got != 1 {
		t.Fatal("exact unbound unit conversion changed", got, err)
	}
	for _, unit := range []string{"millisecond", "microsecond"} {
		original := instantAxis(t, temporal.ProfileIntegerZ, unit)
		originalPosition := scalar(t, original, "7")
		before, err := temporal.AppendPosition(nil, originalPosition, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		want := temporal.ErrIncompatibleDomain
		if unit == "microsecond" {
			want = temporal.ErrExplicitMappingRequired
		}
		if got, err := types.BindDefaultAxis(b.Graph(), original, temporal.Limits{}); got != (types.DefaultAxisBinding{}) || !errors.Is(err, want) || !errors.Is(err, types.ErrDefaultAxisMismatch) {
			t.Fatal("default selection silently remapped original profile", unit, got, err)
		}
		after, err := temporal.AppendPosition(nil, originalPosition, temporal.Limits{})
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("historical explicit profile changed", unit, err)
		}
		if unit == "millisecond" {
			p, err := types.InstantPosition(original, 7, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := types.InstantFromPosition(p, original, temporal.Limits{}); err != nil || got != 7 {
				t.Fatal("existing Z/ms Instant helper was narrowed", got, err)
			}
		}
	}
}

func TestDefaultAxisBindingRefusalsPrecedenceAndTightenedLimits(t *testing.T) {
	b := defaultBinding(t, types.GraphID{3}, temporal.AxisID{19})
	d := b.Axis().Descriptor()
	bytes := 27 + len(d.Reference) + len(d.CanonicalUnit)
	for _, tc := range []struct {
		graph types.GraphID
		id    temporal.AxisID
		l     temporal.Limits
		want  error
	}{
		{types.GraphID{}, temporal.AxisID{}, temporal.Limits{MaxValueBytes: -1}, temporal.ErrInvalidLimits},
		{types.GraphID{}, temporal.AxisID{}, temporal.Limits{}, types.ErrInvalidGraphIdentity},
		{b.Graph(), temporal.AxisID{}, temporal.Limits{}, temporal.ErrInvalidAxis},
		{b.Graph(), d.ID, temporal.Limits{MaxDescriptorBytes: bytes - 1}, temporal.ErrResourceLimit},
	} {
		got, err := types.NewDefaultAxisBinding(tc.graph, tc.id, tc.l)
		if got != (types.DefaultAxisBinding{}) || !errors.Is(err, tc.want) {
			t.Fatal("factory refusal precedence", got, err, tc.want)
		}
	}
	for _, change := range []struct {
		change func(*temporal.AxisDescriptor)
		want   error
	}{
		{func(d *temporal.AxisDescriptor) { d.CanonicalUnit = "microsecond" }, temporal.ErrExplicitMappingRequired},
		{func(d *temporal.AxisDescriptor) { d.Profile = temporal.ProfileIntegerZ }, temporal.ErrIncompatibleDomain},
		{func(d *temporal.AxisDescriptor) { d.Profile = temporal.ProfileLexicographicQN }, temporal.ErrIncompatibleDomain},
		{func(d *temporal.AxisDescriptor) { d.Reference = "posix-unix-epoch:2000-01-01T00:00:00Z:v1" }, temporal.ErrAxisMismatch},
		{func(d *temporal.AxisDescriptor) { d.Reference = "posix-unix-epoch:1970-01-01T00:00:00Z:v2" }, temporal.ErrAxisMismatch},
	} {
		other := d
		change.change(&other)
		axis, err := temporal.NewAxis(other, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := types.BindDefaultAxis(b.Graph(), axis, temporal.Limits{})
		if got != (types.DefaultAxisBinding{}) || !errors.Is(err, types.ErrDefaultAxisMismatch) || !errors.Is(err, change.want) {
			t.Fatal("same units were treated as reference/domain proof", got, err)
		}
		if err := b.Check(b.Graph(), axis, temporal.Limits{}); !errors.Is(err, types.ErrDefaultAxisMismatch) || !errors.Is(err, change.want) {
			t.Fatal("Check bypassed axis policy", err)
		}
	}
	for _, tc := range []struct {
		graph types.GraphID
		axis  temporal.Axis
		l     temporal.Limits
		want  error
	}{
		{types.GraphID{}, temporal.Axis{}, temporal.Limits{MaxInputBytes: -1}, temporal.ErrInvalidLimits},
		{types.GraphID{}, temporal.Axis{}, temporal.Limits{}, types.ErrInvalidGraphIdentity},
		{b.Graph(), temporal.Axis{}, temporal.Limits{}, temporal.ErrInvalidAxis},
		{b.Graph(), b.Axis(), temporal.Limits{MaxDescriptorBytes: bytes - 1}, temporal.ErrResourceLimit},
	} {
		got, err := types.BindDefaultAxis(tc.graph, tc.axis, tc.l)
		if got != (types.DefaultAxisBinding{}) || !errors.Is(err, tc.want) {
			t.Fatal("binding refusal precedence", got, err)
		}
		if err := b.Check(tc.graph, tc.axis, tc.l); !errors.Is(err, tc.want) {
			t.Fatal("Check refusal precedence", err)
		}
	}
	var zero types.DefaultAxisBinding
	if err := zero.Check(types.GraphID{}, temporal.Axis{}, temporal.Limits{MaxValueBytes: -1}); !errors.Is(err, temporal.ErrInvalidLimits) {
		t.Fatal("receiver invalidity masked invalid policy", err)
	}
	for _, limits := range []temporal.Limits{{MaxDescriptorBytes: bytes}, {MaxInputBytes: 1 << 20, MaxValueBytes: 1 << 20, MaxMagnitudeBits: 65536, MaxRegionPieces: 65536, MaxDescriptorBytes: 1 << 20}, {MaxValueBytes: 1, MaxMagnitudeBits: 1}} {
		got, err := types.NewDefaultAxisBinding(b.Graph(), d.ID, limits)
		if err != nil || got != b {
			t.Fatal("axis factory imposed a coordinate codec/default instead of descriptor policy", limits, err)
		}
		if got, err := types.BindDefaultAxis(b.Graph(), b.Axis(), limits); err != nil || got != b {
			t.Fatal(limits, err)
		}
		if err := b.Check(b.Graph(), b.Axis(), limits); err != nil {
			t.Fatal(limits, err)
		}
	}
	// Once values are encoded the coordinate/output policy applies separately.
	if _, err := types.InstantPosition(b.Axis(), 2, temporal.Limits{MaxMagnitudeBits: 1}); !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal(err)
	}
}
