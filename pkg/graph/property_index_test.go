package graph

import snowflake "github.com/bds421/rho-snowflake-2026"

// contains returns true if the given node ID exists in any value bucket.
// Test-only helper used during property index verification.
func (pi *propertyIndex) contains(id snowflake.ID) bool {
	for _, idSet := range pi.entries {
		if _, ok := idSet[id]; ok {
			return true
		}
	}
	return false
}
