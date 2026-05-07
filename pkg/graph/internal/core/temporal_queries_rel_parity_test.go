package core

import (
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// These tests guarantee Relationship-side parity with the Node-side
// temporal query convenience methods (CLAUDE.md rules 2 and 17). Each test
// follows the two-phase shape required by rule 15: create → mutate → query
// at the pre-mutation instant, then assert the *historical* state, not the
// post-mutation state.

// --- GetRelationshipsByTypeValidAt ---

// Two-phase: rel exists and has property X at t0, gets property Y at t1.
// Querying at t0 must surface the rel (the type-index path is history-aware).
func TestGetRelationshipsByTypeValidAt_TwoPhase(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, map[string]any{"since": "2020"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()
	queryTime := g.relValidFrom(r)

	// Phase 2: mutate after t0.
	time.Sleep(2 * time.Millisecond)
	if _, err := g.UpdateRelationship(id, map[string]any{"since": "2024"}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	// Querying at the pre-mutation instant must still return the rel — the
	// type didn't change, so this primarily exercises the history-aware
	// chain (covers deleted-rel + closed-out version cases too).
	got, err := g.GetRelationshipsByTypeValidAt("KNOWS", queryTime)
	if err != nil {
		t.Fatalf("GetRelationshipsByTypeValidAt: %v", err)
	}
	if !containsRelID(got, id) {
		t.Fatalf("expected rel %d at t0=%d; got %d rels", id, queryTime, len(got))
	}

	// And the historical version returned must reflect the pre-mutation
	// property value — confirms the named convenience routes through the
	// history-aware planner, not a current-state shortcut.
	for _, rel := range got {
		if rel.ID() != id {
			continue
		}
		v, ok := rel.GetProperty("since")
		if !ok || v != "2020" {
			t.Fatalf("historical rel property: got (%v, %v), want \"2020\"", v, ok)
		}
	}
}

// Adversarial multi-rel set with diverging lifecycles + exact-set assertion.
// Includes a deleted rel that must still be present at the historical instant.
func TestGetRelationshipsByTypeValidAt_Adversarial(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	c, _ := g.AddNode([]string{"Person"}, nil)
	d, _ := g.AddNode([]string{"Person"}, nil)

	// Two KNOWS rels live at t0; one is later deleted, one survives.
	rKept, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship rKept: %v", err)
	}
	rDeleted, err := g.AddRelationship("KNOWS", c, d, nil)
	if err != nil {
		t.Fatalf("AddRelationship rDeleted: %v", err)
	}
	// One LIKES rel should never appear in KNOWS results.
	rOtherType, err := g.AddRelationship("LIKES", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship rOtherType: %v", err)
	}
	t0 := g.relValidFrom(rKept)
	if v := g.relValidFrom(rDeleted); v < t0 {
		t0 = v
	}

	time.Sleep(2 * time.Millisecond)
	if err := g.DeleteRelationship(rDeleted.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	got, err := g.GetRelationshipsByTypeValidAt("KNOWS", t0)
	if err != nil {
		t.Fatalf("GetRelationshipsByTypeValidAt: %v", err)
	}
	assertRelSet(t, "KNOWS at t0", got, []types.RelID{rKept.ID(), rDeleted.ID()})

	// Negative assertion: LIKES rel must not appear in KNOWS results.
	if containsRelID(got, rOtherType.ID()) {
		t.Errorf("KNOWS result must NOT contain LIKES rel %d", rOtherType.ID())
	}

	// Phantom type returns nil/empty.
	phantom, err := g.GetRelationshipsByTypeValidAt("Phantom", t0)
	if err != nil {
		t.Fatalf("GetRelationshipsByTypeValidAt phantom: %v", err)
	}
	if len(phantom) != 0 {
		t.Errorf("phantom type must return empty, got %d rels", len(phantom))
	}
}

// --- RelationshipsByTypePropertyAndTime ---

// Two-phase: property is "draft" at t0, becomes "published" later.
// Querying with value "draft" at t0 must include the rel (history-aware).
func TestRelationshipsByTypePropertyAndTime_TwoPhase(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, err := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()
	queryTime := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if _, err := g.UpdateRelationship(id, map[string]any{"status": "published"}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, err := g.RelationshipsByTypePropertyAndTime("KNOWS", "status", "draft", queryTime)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyAndTime: %v", err)
	}
	if !containsRelID(got, id) {
		t.Fatalf("historical property value missing at %d; got %d rels", queryTime, len(got))
	}

	// Negative assertion: querying with the post-mutation value at t0 must
	// NOT include the rel — at t0 the value was still "draft".
	got, err = g.RelationshipsByTypePropertyAndTime("KNOWS", "status", "published", queryTime)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyAndTime (published @ t0): %v", err)
	}
	if containsRelID(got, id) {
		t.Errorf("rel had \"draft\" at t0 but result for value=\"published\" includes it")
	}
}

// Adversarial: multiple rels with diverging property timelines + exact-set
// assertion + phantom-value returns empty.
func TestRelationshipsByTypePropertyAndTime_Adversarial(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)

	// rDraft: "draft" at t0, mutated to "published" later (must be in result).
	rDraft, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "draft"})
	// rPublished: "published" from creation (must NOT be in result for "draft").
	rPublished, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "published"})
	// rOtherType: LIKES with status=draft (different type, must NOT appear).
	rOtherType, _ := g.AddRelationship("LIKES", a, b, map[string]any{"status": "draft"})

	t0 := g.relValidFrom(rDraft)
	if v := g.relValidFrom(rPublished); v < t0 {
		t0 = v
	}
	if v := g.relValidFrom(rOtherType); v < t0 {
		t0 = v
	}

	time.Sleep(2 * time.Millisecond)
	if _, err := g.UpdateRelationship(rDraft.ID(), map[string]any{"status": "published"}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, err := g.RelationshipsByTypePropertyAndTime("KNOWS", "status", "draft", t0)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyAndTime: %v", err)
	}
	assertRelSet(t, "KNOWS+draft @ t0", got, []types.RelID{rDraft.ID()})

	if containsRelID(got, rPublished.ID()) {
		t.Errorf("result must NOT contain rPublished %d (was \"published\" at t0)", rPublished.ID())
	}
	if containsRelID(got, rOtherType.ID()) {
		t.Errorf("result must NOT contain rOtherType %d (LIKES, not KNOWS)", rOtherType.ID())
	}

	// Phantom value: returns empty (PropertyValueKey of nil → "").
	phantomVal, err := g.RelationshipsByTypePropertyAndTime("KNOWS", "status", nil, t0)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyAndTime nil value: %v", err)
	}
	if len(phantomVal) != 0 {
		t.Errorf("nil property value must return empty, got %d rels", len(phantomVal))
	}

	// Phantom type: nil/empty.
	phantomType, err := g.RelationshipsByTypePropertyAndTime("Phantom", "status", "draft", t0)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyAndTime phantom type: %v", err)
	}
	if len(phantomType) != 0 {
		t.Errorf("phantom type must return empty, got %d rels", len(phantomType))
	}
}

// History-aware candidate set (forEachRelCandidateID) includes ALL history
// rel IDs, not just IDs of the queried type. The HasTypeTokenRaw filter
// inside the iterator must reject rels of other types whose IDs appear via
// the history-IDs union — otherwise property-only matches across types
// would leak into the result.
func TestRelationshipsByTypePropertyAndTime_FiltersOtherTypes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)

	// rKnows: KNOWS with status=draft (must be in result).
	rKnows, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "draft"})
	// rLikes: LIKES with same property value, then mutated → forces it into
	// the rel-history index, so its ID appears in forEachRelCandidateID's
	// union when querying KNOWS. The HasTypeTokenRaw filter must reject it.
	rLikes, _ := g.AddRelationship("LIKES", a, b, map[string]any{"status": "draft"})
	t0 := g.relValidFrom(rKnows)
	if v := g.relValidFrom(rLikes); v < t0 {
		t0 = v
	}

	time.Sleep(2 * time.Millisecond)
	if _, err := g.UpdateRelationship(rLikes.ID(), map[string]any{"status": "published"}); err != nil {
		t.Fatalf("UpdateRelationship rLikes: %v", err)
	}

	got, err := g.RelationshipsByTypePropertyAndTime("KNOWS", "status", "draft", t0)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyAndTime: %v", err)
	}
	assertRelSet(t, "KNOWS+draft @ t0 (with foreign-type history)", got, []types.RelID{rKnows.ID()})
	if containsRelID(got, rLikes.ID()) {
		t.Errorf("KNOWS query must filter out LIKES rel %d even though its ID surfaces via history-IDs union", rLikes.ID())
	}
}

// Deleted rel: history exists but current is gone. The history-aware
// candidate set (current + history IDs) must still surface the rel when
// querying at a t before deletion, exercising the GetRelAt path that
// resolves a version from history alone.
func TestRelationshipsByTypePropertyAndTime_DeletedRel(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, err := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()
	t0 := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if err := g.DeleteRelationship(id); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	got, err := g.RelationshipsByTypePropertyAndTime("KNOWS", "status", "draft", t0)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyAndTime (deleted): %v", err)
	}
	if !containsRelID(got, id) {
		t.Fatalf("deleted rel must be queryable at historical t0=%d; got %d rels", t0, len(got))
	}
}

// --- RelationshipsByTypePropertyDuring ---

// Two-phase + predicate-during-interval edge case (CLAUDE.md "Most-recent-
// overlap is wrong for predicate-during-interval"). The rel had property X
// during *part* of [start, end), then mutated to Y. The most-recent
// overlapping version no longer matches X — but the rel must still be in
// results because findRelVersionMatchingDuring scans all overlapping
// versions, not just the latest.
func TestRelationshipsByTypePropertyDuring_PredicateDuringInterval(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)

	r, err := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()
	start := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	updated, err := g.UpdateRelationship(id, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	// end is *after* the mutation — both versions overlap [start, end), so
	// the most-recent overlapping version is "published". Verify the
	// implementation still finds the "draft" version that held during the
	// earlier portion of the interval.
	end := updated.Temporal().UpdatedAt + 1000

	got, err := g.RelationshipsByTypePropertyDuring("KNOWS", "status", "draft", start, end)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyDuring: %v", err)
	}
	if !containsRelID(got, id) {
		t.Fatalf("rel held status=draft during part of [%d,%d) but is missing from results (size=%d) — most-recent-overlap shortcut would skip it",
			start, end, len(got))
	}
}

// Adversarial: multi-rel diverging lifecycles, exact-set assertion, plus the
// negative case (rel that never had the property value during the interval).
func TestRelationshipsByTypePropertyDuring_Adversarial(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)

	// rDraft: had status=draft during the interval, then mutated.
	rDraft, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "draft"})
	// rNever: never had status=draft (always published).
	rNever, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "published"})
	// rOtherType: LIKES with status=draft (different type, must NOT appear).
	rOtherType, _ := g.AddRelationship("LIKES", a, b, map[string]any{"status": "draft"})

	start := g.relValidFrom(rDraft)
	if v := g.relValidFrom(rNever); v < start {
		start = v
	}
	if v := g.relValidFrom(rOtherType); v < start {
		start = v
	}

	time.Sleep(2 * time.Millisecond)
	updated, err := g.UpdateRelationship(rDraft.ID(), map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	end := updated.Temporal().UpdatedAt + 1000

	got, err := g.RelationshipsByTypePropertyDuring("KNOWS", "status", "draft", start, end)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyDuring: %v", err)
	}
	assertRelSet(t, "KNOWS+draft during [start,end)", got, []types.RelID{rDraft.ID()})

	if containsRelID(got, rNever.ID()) {
		t.Errorf("result must NOT contain rNever %d (never had status=draft)", rNever.ID())
	}
	if containsRelID(got, rOtherType.ID()) {
		t.Errorf("result must NOT contain rOtherType %d (LIKES, not KNOWS)", rOtherType.ID())
	}

	// Phantom type returns empty.
	phantom, err := g.RelationshipsByTypePropertyDuring("Phantom", "status", "draft", start, end)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyDuring phantom type: %v", err)
	}
	if len(phantom) != 0 {
		t.Errorf("phantom type must return empty, got %d rels", len(phantom))
	}

	// Nil property value returns empty.
	nilVal, err := g.RelationshipsByTypePropertyDuring("KNOWS", "status", nil, start, end)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyDuring nil value: %v", err)
	}
	if len(nilVal) != 0 {
		t.Errorf("nil property value must return empty, got %d rels", len(nilVal))
	}
}

// Open-ended end (end=0) is resolved once at entry so all candidates see
// the same upper bound. Mirrors GetNodesValidDuring's open-ended semantic.
func TestRelationshipsByTypePropertyDuring_OpenEnded(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)

	r, err := g.AddRelationship("KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()
	start := g.relValidFrom(r)

	got, err := g.RelationshipsByTypePropertyDuring("KNOWS", "status", "draft", start, 0)
	if err != nil {
		t.Fatalf("RelationshipsByTypePropertyDuring open-ended: %v", err)
	}
	if !containsRelID(got, id) {
		t.Fatalf("open-ended interval must include current rel; got %d rels", len(got))
	}
}
