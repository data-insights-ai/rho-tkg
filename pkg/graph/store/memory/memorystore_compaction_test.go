package memory

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryCompactNodeHistory verifies the atomic trim + meta writes: history is
// truncated to keepVersions AND the supplied meta keys are set in one call.
func TestMemoryCompactNodeHistory(t *testing.T) {
	t.Parallel()
	ms := New()
	id := types.NodeID(snowflake.ID(1))
	for ver := uint32(0); ver < 5; ver++ {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		ms.PutNodeVersion(id, ver, n)
	}

	writes := []storecontract.MetaWrite{
		{Key: "stub/1", Value: []byte("stub-bytes")},
		{Key: "watermark", Value: []byte{0, 0, 0, 0, 0, 0, 0, 9}},
	}
	if err := ms.CompactNodeHistory(id, 2, writes); err != nil {
		t.Fatalf("CompactNodeHistory: %v", err)
	}

	hist, _ := ms.GetNodeHistory(id)
	if len(hist) != 2 || hist[0].Version() != 3 || hist[1].Version() != 4 {
		t.Fatalf("history = %d versions %v, want [3,4]", len(hist), hist)
	}
	if v, _ := ms.MetaGet("stub/1"); string(v) != "stub-bytes" {
		t.Fatalf("stub meta = %q, want stub-bytes", v)
	}
	if v, _ := ms.MetaGet("watermark"); len(v) != 8 {
		t.Fatalf("watermark meta = %v, want 8 bytes", v)
	}

	// A nil-valued MetaWrite deletes the key.
	if err := ms.CompactNodeHistory(id, 2, []storecontract.MetaWrite{{Key: "stub/1", Value: nil}}); err != nil {
		t.Fatalf("CompactNodeHistory delete: %v", err)
	}
	if v, _ := ms.MetaGet("stub/1"); v != nil {
		t.Fatalf("stub meta after nil write = %q, want deleted", v)
	}
}

// TestMemoryCompactRelHistory is the relationship mirror.
func TestMemoryCompactRelHistory(t *testing.T) {
	t.Parallel()
	ms := New()
	id := types.RelID(snowflake.ID(101))
	for ver := uint32(0); ver < 4; ver++ {
		r := types.NewRelationship(id, 5, types.NodeID(1), types.NodeID(2))
		r.SetVersion(ver)
		ms.PutRelVersion(id, ver, r)
	}
	if err := ms.CompactRelHistory(id, 1, []storecontract.MetaWrite{{Key: "rstub/101", Value: []byte("x")}}); err != nil {
		t.Fatalf("CompactRelHistory: %v", err)
	}
	hist, _ := ms.GetRelHistory(id)
	if len(hist) != 1 || hist[0].Version() != 3 {
		t.Fatalf("rel history = %d versions, want [3]", len(hist))
	}
	if v, _ := ms.MetaGet("rstub/101"); string(v) != "x" {
		t.Fatalf("rel stub meta = %q, want x", v)
	}
}

func TestMemoryCompactHistory_RejectsNegativeKeep(t *testing.T) {
	t.Parallel()
	ms := New()
	if err := ms.CompactNodeHistory(types.NodeID(1), -1, nil); err == nil {
		t.Fatal("CompactNodeHistory(-1) accepted a negative keep")
	}
	if err := ms.CompactRelHistory(types.RelID(1), -1, nil); err == nil {
		t.Fatal("CompactRelHistory(-1) accepted a negative keep")
	}
}
