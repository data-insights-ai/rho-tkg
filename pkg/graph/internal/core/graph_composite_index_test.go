// Tests for K3c — composite node property indexes: g.Index.CreateComposite /
// DeleteComposite and g.Nodes.ByLabelAndProperties.

package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestGraphCreateComposite_CreateQueryDrop(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	alice, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"first": "Alice", "last": "Smith"})
	if err != nil {
		t.Fatalf("seed alice: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"first": "Alice", "last": "Jones"}); err != nil {
		t.Fatalf("seed alice2: %v", err)
	}

	if err := g.Index.CreateComposite("Person", []string{"first", "last"}); err != nil {
		t.Fatalf("CreateComposite: %v", err)
	}

	got, err := g.Nodes.ByLabelAndProperties("Person", map[string]any{"first": "Alice", "last": "Smith"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperties: %v", err)
	}
	if len(got) != 1 || got[0].ID() != alice.ID() {
		t.Fatalf("got %+v, want exactly [%d]", got, alice.ID())
	}

	if err := g.Index.DeleteComposite("Person", []string{"first", "last"}); err != nil {
		t.Fatalf("DeleteComposite: %v", err)
	}

	// After drop, the query still answers correctly via the fallback scan.
	got, err = g.Nodes.ByLabelAndProperties("Person", map[string]any{"first": "Alice", "last": "Smith"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperties after drop: %v", err)
	}
	if len(got) != 1 || got[0].ID() != alice.ID() {
		t.Fatalf("after drop got %+v, want exactly [%d]", got, alice.ID())
	}
}

func TestGraphCreateComposite_DuplicateAndNotFound(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	if err := g.Index.CreateComposite("Person", []string{"a", "b"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := g.Index.CreateComposite("Person", []string{"a", "b"}); !errors.Is(err, storepkg.ErrIndexExists) {
		t.Fatalf("duplicate create err = %v, want ErrIndexExists", err)
	}
	if err := g.Index.DeleteComposite("Person", []string{"a", "b"}); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := g.Index.DeleteComposite("Person", []string{"a", "b"}); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("second drop err = %v, want ErrIndexNotFound", err)
	}
	// Dropping on an unregistered label is also ErrIndexNotFound.
	if err := g.Index.DeleteComposite("NeverSeen", []string{"a", "b"}); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("drop on unregistered label err = %v, want ErrIndexNotFound", err)
	}
}

func TestGraphCreateComposite_InvalidKeyCount(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	if err := g.Index.CreateComposite("Person", []string{"onlyone"}); err == nil {
		t.Fatal("expected error for a single-key composite index")
	}
	if err := g.Index.CreateComposite("Person", []string{"a", "b", "c", "d", "e"}); err == nil {
		t.Fatal("expected error for a 5-key composite index")
	}
}

// TestByLabelAndProperties_UnregisteredLabelReturnsEmpty is the "phantom
// value returns empty" negative assertion: a label nobody ever wrote must
// not error and must return no rows.
func TestByLabelAndProperties_UnregisteredLabelReturnsEmpty(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	got, err := g.Nodes.ByLabelAndProperties("Phantom", map[string]any{"a": "x", "b": "y"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperties on unregistered label: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d nodes, want 0", len(got))
	}
}

// TestByLabelAndProperties_PartialMatchExcluded is the adversarial-shape
// negative assertion: nodes matching SOME but not ALL requested properties
// must never be returned.
func TestByLabelAndProperties_PartialMatchExcluded(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	alice, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"first": "Alice", "last": "Smith"})
	if err != nil {
		t.Fatalf("seed alice: %v", err)
	}
	// Same first name, different last name — must NOT match.
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"first": "Alice", "last": "Jones"}); err != nil {
		t.Fatalf("seed alice2: %v", err)
	}
	// Missing "last" entirely — must NOT match.
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"first": "Alice"}); err != nil {
		t.Fatalf("seed alice3: %v", err)
	}

	if err := g.Index.CreateComposite("Person", []string{"first", "last"}); err != nil {
		t.Fatalf("CreateComposite: %v", err)
	}

	got, err := g.Nodes.ByLabelAndProperties("Person", map[string]any{"first": "Alice", "last": "Smith"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperties: %v", err)
	}
	assertNodeSet(t, "first=Alice,last=Smith", got, []types.NodeID{alice.ID()})
}

// TestNodesByLabelAndProperties_TemporalOpts_Adversarial mirrors
// TestNodesByLabelAndProperty_TemporalOpts_Adversarial's shape for the
// composite (2-key) case: the discriminating entity is one whose
// MOST-RECENT overlapping version fails the predicate, so a
// most-recent-overlap-only implementation would miss it in the
// during-interval query (rule 16 / CLAUDE.md's most-recent-overlap lesson).
func TestNodesByLabelAndProperties_TemporalOpts_Adversarial(t *testing.T) {
	g := newTestGraph(t)
	clk := useTestClock(t, g)

	alice, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft", "owner": "alice"})
	if err != nil {
		t.Fatalf("AddNode alice: %v", err)
	}
	bob, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "published", "owner": "bob"})
	if err != nil {
		t.Fatalf("AddNode bob: %v", err)
	}
	carol, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft", "owner": "carol"})
	if err != nil {
		t.Fatalf("AddNode carol: %v", err)
	}
	aliceID := alice.ID()
	bobID := bob.ID()
	carolID := carol.ID()

	t0 := g.nodeValidFrom(carol)

	updatedAlice, err := g.Nodes.Update(context.Background(), aliceID, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode alice: %v", err)
	}
	aliceMutation := updatedAlice.Temporal().UpdatedAt
	tNow := clk.PeekInstant()

	values := map[string]any{"status": "draft", "owner": "alice"}

	// At t0: alice matches (draft, owner=alice).
	hits, err := g.Nodes.ByLabelAndProperties("Doc", values, storepkg.QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("draft/alice@t0: %v", err)
	}
	assertNodeSet(t, "draft/alice@t0", hits, []types.NodeID{aliceID})

	// At tNow: alice's current status is published — no longer matches.
	hits, err = g.Nodes.ByLabelAndProperties("Doc", values, storepkg.QueryOpts{ValidAt: tNow})
	if err != nil {
		t.Fatalf("draft/alice@tNow: %v", err)
	}
	assertNodeSet(t, "draft/alice@tNow", hits, nil)

	// During [t0, tNow): alice's v0 (draft, owner=alice) overlaps the
	// interval even though her MOST-RECENT overlapping version (v1,
	// published) fails the predicate. The during-interval query must still
	// find her (rule 16 predicate-anywhere-in-interval requirement).
	hits, err = g.Nodes.ByLabelAndProperties("Doc", values, storepkg.QueryOpts{ValidStart: t0, ValidEnd: tNow})
	if err != nil {
		t.Fatalf("draft/alice@[t0,tNow): %v", err)
	}
	assertNodeSet(t, "draft/alice@[t0,tNow)", hits, []types.NodeID{aliceID})

	// Boundary: at aliceMutation she is already on v1 (published); one
	// millisecond earlier she is still on v0 (draft).
	hits, err = g.Nodes.ByLabelAndProperties("Doc", values, storepkg.QueryOpts{ValidAt: aliceMutation})
	if err != nil {
		t.Fatalf("draft/alice@aliceMutation: %v", err)
	}
	if containsNodeID(hits, aliceID) {
		t.Errorf("draft/alice@aliceMutation: alice present, but her v1 (published) starts at this instant")
	}
	hits, err = g.Nodes.ByLabelAndProperties("Doc", values, storepkg.QueryOpts{ValidAt: aliceMutation - 1})
	if err != nil {
		t.Fatalf("draft/alice@aliceMutation-1: %v", err)
	}
	if !containsNodeID(hits, aliceID) {
		t.Errorf("draft/alice@aliceMutation-1: alice absent, but her v0 (draft/alice) is still valid at this instant")
	}

	// Negative: bob and carol must NEVER appear for owner=alice.
	hits, err = g.Nodes.ByLabelAndProperties("Doc", values, storepkg.QueryOpts{ValidStart: t0, ValidEnd: tNow})
	if err != nil {
		t.Fatalf("negative check: %v", err)
	}
	if containsNodeID(hits, bobID) || containsNodeID(hits, carolID) {
		t.Errorf("draft/alice@[t0,tNow) must NOT contain bob or carol, got %v", hits)
	}
}

// TestCapability_CompositePropertyIndex_AbsentOnMandatoryOnlyBackend proves
// CompositePropertyIndexCapability is a pure acceleration: a backend that
// satisfies only MandatoryStore (e.g. an out-of-tree/tiered-shaped backend
// in v1 scope) rejects CreateComposite with ErrCapabilityNotSupported but
// still answers ByLabelAndProperties correctly via the graph-layer
// label-scan + post-filter fallback.
func TestCapability_CompositePropertyIndex_AbsentOnMandatoryOnlyBackend(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)

	alice, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"first": "Alice", "last": "Smith"})
	if err != nil {
		t.Fatalf("seed alice: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"first": "Alice", "last": "Jones"}); err != nil {
		t.Fatalf("seed alice2: %v", err)
	}

	if err := g.Index.CreateComposite("Person", []string{"first", "last"}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("CreateComposite err = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Index.DeleteComposite("Person", []string{"first", "last"}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("DeleteComposite err = %v, want ErrCapabilityNotSupported", err)
	}

	got, err := g.Nodes.ByLabelAndProperties("Person", map[string]any{"first": "Alice", "last": "Smith"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperties err = %v, want nil (fallback must answer the query)", err)
	}
	if len(got) != 1 || got[0].ID() != alice.ID() {
		t.Errorf("ByLabelAndProperties returned %+v, want exactly [%d]", got, alice.ID())
	}
}

// TestByLabelAndProperties_RejectsInvalidTargets exercises the validation
// door: too few/too many keys, a shadow-key target, and a non-indexable
// value must all fail closed with a descriptive error rather than silently
// returning zero rows.
func TestByLabelAndProperties_RejectsInvalidTargets(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	if _, err := g.Nodes.ByLabelAndProperties("Person", map[string]any{"a": "x"}, storepkg.QueryOpts{}); err == nil {
		t.Fatal("expected error for a single-key query (v1 requires 2-4)")
	}
	if _, err := g.Nodes.ByLabelAndProperties("Person", map[string]any{"a": "x", "b": "y", "c": "z", "d": "w", "e": "v"}, storepkg.QueryOpts{}); err == nil {
		t.Fatal("expected error for a 5-key query")
	}
	if _, err := g.Nodes.ByLabelAndProperties("Person", map[string]any{"tkg_labels": "x", "b": "y"}, storepkg.QueryOpts{}); err == nil {
		t.Fatal("expected error for a shadow-key target")
	}

	// A non-indexable value (e.g. a slice) is a VALID property value (the
	// allowlist accepts it for storage) but canonicalizes to the empty key
	// — per R4-F9, this is "no match", not an error and not a false match
	// through empty-key coincidence.
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"a": []any{"x"}, "b": "y"}); err != nil {
		t.Fatalf("seed non-indexable: %v", err)
	}
	got, err := g.Nodes.ByLabelAndProperties("Person", map[string]any{"a": []any{"not", "indexable"}, "b": "y"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("non-indexable query value: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("non-indexable query value must match nothing, got %d", len(got))
	}
}
