package core

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BACKLOG 14e (lessons 17/58 drift pattern): PropertyTypeClassCounts and
// RelPropertyTypeClassCounts check the store's optional capability BEFORE
// looking up the label/type token, so a store that declines the capability
// (e.g. DisablePlannerStats) fails closed with ErrCapabilityNotSupported
// EVEN for a label/type that was never registered. Their two siblings,
// NodeCountByLabelAndPropertyKey and PropertyStats, did the label lookup
// FIRST and short-circuited to a zero-value success ("unregistered label ->
// 0") before ever reaching the capability check that lives inside
// c.nodeCountByLabelAndPropertyKey / c.nodePropertyStats — so for an
// UNREGISTERED label specifically, those two doors silently returned success
// regardless of DisablePlannerStats, while their siblings correctly failed
// closed. All four are documented (CLAUDE.md) to fail closed with
// ErrCapabilityNotSupported "at RUNTIME" when the store declines — a
// store-level feature-availability gate that should not depend on whether
// the queried label happens to be registered.
//
// TestGraphStats_NodeCountByLabelAndPropertyKeyCapabilityMissing and
// TestGraphStats_PropertyStatsCapabilityMissing already cover the
// capability-missing case, but only for a label the test itself registers
// first — never the unregistered-label combination, which is exactly where
// the drift lived.

func TestGraphStats_NodeCountByLabelAndPropertyKey_CapabilityMissing_UnregisteredLabel(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Stats.NodeCountByLabelAndPropertyKey("NeverRegistered", "id"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("NodeCountByLabelAndPropertyKey(unregistered label, capability missing) = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestGraphStats_PropertyStats_CapabilityMissing_UnregisteredLabel(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Stats.PropertyStats("NeverRegistered", "id"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("PropertyStats(unregistered label, capability missing) = %v, want ErrCapabilityNotSupported", err)
	}
}

// TestGraphStats_PropertyTypeClassCounts_CapabilityMissing_UnregisteredLabel
// and its rel-type mirror confirm the two SIBLING doors already had the
// correct (capability-check-first) ordering, establishing the baseline the
// two tests above now match.
func TestGraphStats_PropertyTypeClassCounts_CapabilityMissing_UnregisteredLabel(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Stats.PropertyTypeClassCounts("NeverRegistered", "id"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("PropertyTypeClassCounts(unregistered label, capability missing) = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestGraphStats_RelPropertyTypeClassCounts_CapabilityMissing_UnregisteredType(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Stats.RelPropertyTypeClassCounts("NEVER_REGISTERED", "id"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("RelPropertyTypeClassCounts(unregistered type, capability missing) = %v, want ErrCapabilityNotSupported", err)
	}
}
