//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package types

import "testing"

// TestRelationshipCompactIntegrityAllocations: rebuilding a compact row's
// integrity costs the struct and one hash string, not a scratch buffer too
// (Integrity and DeepCopy sit on the Get path).
func TestRelationshipCompactIntegrityAllocations(t *testing.T) {
	cp := buildCompactTestRel(t, compactRelCases()[0]).CompactFrozenCopy()
	if a := testing.AllocsPerRun(100, func() { _ = cp.Integrity() }); a > 2 {
		t.Fatalf("Integrity() on a compact row allocates %.0f objects, want <= 2", a)
	}
}
