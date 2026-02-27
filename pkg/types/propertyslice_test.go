package types

import "testing"

func TestPropertySliceRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	if err := ps.Set("tkg_labels", "x"); err == nil {
		t.Fatal("PropertySlice.Set(\"tkg_labels\", ...) should return error")
	}
	// Verify nothing was stored.
	if ps.Len() != 0 {
		t.Fatalf("PropertySlice should be empty after rejected Set, got %d", ps.Len())
	}
}
