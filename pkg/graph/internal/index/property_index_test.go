package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Contains returns true if the given node ID exists in any value bucket.
// Test-only helper used during property index verification.
func (pi *PropertyIndex) Contains(id snowflake.ID) bool {
	for _, idSet := range pi.Entries {
		if _, ok := idSet[id]; ok {
			return true
		}
	}
	return false
}

func TestPropertyIndex_Contains(t *testing.T) {
	t.Parallel()
	idx := NewPropertyIndex()

	if idx.Contains(snowflake.ID(1)) {
		t.Error("empty index should not contain anything")
	}

	idx.Add(snowflake.ID(1), "Alice")
	idx.Add(snowflake.ID(2), "Bob")

	if !idx.Contains(snowflake.ID(1)) {
		t.Error("should contain ID 1")
	}
	if !idx.Contains(snowflake.ID(2)) {
		t.Error("should contain ID 2")
	}
	if idx.Contains(snowflake.ID(3)) {
		t.Error("should not contain ID 3")
	}
}
