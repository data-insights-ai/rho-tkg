package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Fix G: temporalIndex lazy sort ---

// TestTemporalIndex_LazySort_BatchInsert verifies that N inserts followed by
// a single queryAt return all correct IDs (sort happens before query, not per-insert).
func TestTemporalIndex_LazySort_BatchInsert(t *testing.T) {
	t.Parallel()
	const n = 100
	ti := newTemporalIndex()

	// Insert out-of-order: descending from values.
	ids := make([]snowflake.ID, n)
	for i := range ids {
		ids[i] = snowflake.ID(i + 1)
		// from = (n-i)*10 so inserts are in reverse chronological order
		ti.add(ids[i], types.Instant((n-i)*10), 0)
	}

	// dirty must be true before the first query.
	if !ti.dirty {
		t.Error("expected dirty=true after batch inserts, got false")
	}

	// queryAt a time that covers all entries.
	got := ti.queryAt(types.Instant(n*10 + 1))
	if len(got) != n {
		t.Fatalf("queryAt returned %d results, want %d", len(got), n)
	}

	// dirty must be false after query.
	if ti.dirty {
		t.Error("expected dirty=false after queryAt, got true")
	}

	// Verify all IDs are present.
	seen := make(map[snowflake.ID]bool, n)
	for _, id := range got {
		seen[id] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("ID %d missing from queryAt result", id)
		}
	}
}

// TestTemporalIndex_LazySort_InterleavedReadsWrites verifies that interleaved
// add and queryAt calls produce correct results at each step.
func TestTemporalIndex_LazySort_InterleavedReadsWrites(t *testing.T) {
	t.Parallel()
	ti := newTemporalIndex()

	id1 := snowflake.ID(1)
	id2 := snowflake.ID(2)
	id3 := snowflake.ID(3)

	// Add id1 [100, 0)
	ti.add(id1, 100, 0)
	// Query before sort — sortIfDirty must fire.
	got := ti.queryAt(150)
	if len(got) != 1 || got[0] != id1 {
		t.Errorf("step1: queryAt(150) = %v, want [1]", got)
	}
	if ti.dirty {
		t.Error("step1: dirty should be false after queryAt")
	}

	// Add id2 [50, 200) — inserts before id1 chronologically.
	ti.add(id2, 50, 200)
	if !ti.dirty {
		t.Error("step2: dirty should be true after add")
	}

	// Query — must re-sort. Both id1 and id2 valid at t=150.
	got = ti.queryAt(150)
	if len(got) != 2 {
		t.Fatalf("step2: queryAt(150) = %v, want 2 results", got)
	}
	foundID1, foundID2 := false, false
	for _, id := range got {
		if id == id1 {
			foundID1 = true
		}
		if id == id2 {
			foundID2 = true
		}
	}
	if !foundID1 || !foundID2 {
		t.Errorf("step2: missing id1=%v or id2=%v in %v", foundID1, foundID2, got)
	}

	// Add id3 [300, 0) — after both.
	ti.add(id3, 300, 0)

	// queryOverlap [250, 400): id2 expired at 200, so only id1 and id3.
	got = ti.queryOverlap(250, 400)
	if len(got) != 2 {
		t.Fatalf("step3: queryOverlap(250,400) = %v, want 2 results", got)
	}
	found1, found3 := false, false
	for _, id := range got {
		if id == id1 {
			found1 = true
		}
		if id == id3 {
			found3 = true
		}
	}
	if !found1 || !found3 {
		t.Errorf("step3: expected id1 and id3, got %v", got)
	}
}

// --- Fix H: RemoveLabelTokenRaw nil extraLabels ---

// TestRemoveLabelTokenRaw_ExtraLabelsNilAfterLastRemoval verifies that after all
// extra labels are removed, ExtraLabelTokens() returns nil (not an empty slice).
func TestRemoveLabelTokenRaw_ExtraLabelsNilAfterLastRemoval(t *testing.T) {
	t.Parallel()

	const primary uint16 = 1
	const extra1 uint16 = 2
	const extra2 uint16 = 3

	t.Run("remove_only_extra", func(t *testing.T) {
		t.Parallel()
		n := types.NewNode(snowflake.ID(1), primary, []uint16{extra1})
		// Sanity: one extra label present.
		if extras := n.ExtraLabelTokens(); len(extras) != 1 {
			t.Fatalf("pre-condition: ExtraLabelTokens = %v, want [2]", extras)
		}
		ok := n.RemoveLabelTokenRaw(extra1)
		if !ok {
			t.Fatal("RemoveLabelTokenRaw returned false, want true")
		}
		// After removal, ExtraLabelTokens must return nil.
		if extras := n.ExtraLabelTokens(); extras != nil {
			t.Errorf("ExtraLabelTokens = %v (len=%d), want nil", extras, len(extras))
		}
	})

	t.Run("promote_only_extra_to_primary", func(t *testing.T) {
		t.Parallel()
		// Node with primary=1, extra=2. Remove primary → extra2 becomes primary.
		n := types.NewNode(snowflake.ID(2), primary, []uint16{extra1})
		ok := n.RemoveLabelTokenRaw(primary)
		if !ok {
			t.Fatal("RemoveLabelTokenRaw(primary) returned false, want true")
		}
		// primary is now extra1; extraLabels must be nil.
		if uint16(n.PrimaryLabelToken()) != extra1 {
			t.Errorf("primary = %d, want %d", n.PrimaryLabelToken(), extra1)
		}
		if extras := n.ExtraLabelTokens(); extras != nil {
			t.Errorf("ExtraLabelTokens = %v, want nil after promotion", extras)
		}
	})

	t.Run("remove_one_of_two_extras_leaves_non_nil", func(t *testing.T) {
		t.Parallel()
		n := types.NewNode(snowflake.ID(3), primary, []uint16{extra1, extra2})
		ok := n.RemoveLabelTokenRaw(extra1)
		if !ok {
			t.Fatal("RemoveLabelTokenRaw returned false")
		}
		// One extra remains — slice must NOT be nil.
		extras := n.ExtraLabelTokens()
		if extras == nil {
			t.Fatal("ExtraLabelTokens = nil, want non-nil (one extra remains)")
		}
		if len(extras) != 1 || uint16(extras[0]) != extra2 {
			t.Errorf("ExtraLabelTokens = %v, want [%d]", extras, extra2)
		}
	})
}
