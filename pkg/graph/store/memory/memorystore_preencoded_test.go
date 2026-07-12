package memory

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryPutNodesBatchPreEncoded verifies the memory backend's
// store.PreEncodedPutCapability method: it ignores the pre-encoded buffers
// (memory holds live objects, never a serialized row) and persists exactly as
// PutNodesBatch. Nil and non-nil buffer slices behave identically.
func TestMemoryPutNodesBatchPreEncoded(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 2, nil)
	ps, err := types.NewPropertySlice(map[string]any{"k": int64(9)})
	if err != nil {
		t.Fatalf("props: %v", err)
	}
	_ = n2.SetProperties(ps)

	// wireBodies are irrelevant to memory; pass garbage to prove it is ignored.
	if err := ms.PutNodesBatchPreEncoded([]*types.Node{n1, n2}, [][]byte{{0xDE, 0xAD}, nil}); err != nil {
		t.Fatalf("PutNodesBatchPreEncoded: %v", err)
	}
	for _, id := range []types.NodeID{n1.ID(), n2.ID()} {
		if _, err := ms.GetNode(id); err != nil {
			t.Fatalf("GetNode %d: %v", id, err)
		}
	}
	if v, ok := func() (any, bool) {
		got, _ := ms.GetNode(n2.ID())
		return got.GetProperty("k")
	}(); !ok || v != int64(9) {
		t.Fatalf("n2 prop wrong: %v", v)
	}
}
