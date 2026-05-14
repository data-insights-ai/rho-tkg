package tiered

import (
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestTiered_ListShards_WithArchive covers the archive-present branch in
// ListShards (the open-archive arm and the closed-archive arm in
// hasArchiveShard). Pre-existing ListShards tests only exercise the "no
// archive yet" path because they call ListShards before any ArchiveNode.
func TestTiered_ListShards_WithArchive(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards with archive: %v", err)
	}
	hasArchive := false
	for _, info := range infos {
		if info.Kind == ShardArchive {
			hasArchive = true
			if info.Name != "archive" {
				t.Fatalf("archive shard name = %q, want \"archive\"", info.Name)
			}
			if !info.Open {
				t.Fatalf("archive shard Open = false, want true (was checked out for write)")
			}
			if info.Nodes < 1 {
				t.Fatalf("archive shard Nodes = %d, want >= 1 after ArchiveNode", info.Nodes)
			}
		}
	}
	if !hasArchive {
		t.Fatalf("ListShards after ArchiveNode = %d shards, none of Kind=ShardArchive", len(infos))
	}
}
