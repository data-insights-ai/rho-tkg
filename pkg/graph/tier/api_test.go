package tier

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAPIForwardsEveryMethod(t *testing.T) {
	t.Parallel()

	ops := &tierOpsSpy{}
	api := New(ops)
	nodeID := types.NodeID(42)

	if err := api.Archive(nodeID); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if err := api.Restore(nodeID); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := api.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	shards, err := api.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	if len(shards) != 1 || shards[0].Name != "hot" {
		t.Fatalf("ListShards = %+v, want hot shard", shards)
	}
	shards[0].Name = "mutated"
	again, err := api.ListShards()
	if err != nil {
		t.Fatalf("ListShards after returned-slice mutation: %v", err)
	}
	if again[0].Name != "hot" {
		t.Fatalf("mutating ListShards result changed ops shards: %+v", again)
	}
	if st, err := api.ShardAccessStats(); err != nil || st.Cold != 7 || st.ColdOpens != 2 {
		t.Fatalf("ShardAccessStats = %+v, %v; want the ops' {Cold 7, ColdOpens 2}", st, err)
	}
	if err := api.MoveShard(context.Background(), "2026-W43", "warm"); err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	if err := api.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog: %v", err)
	}
	repair, err := api.Repair()
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if repair == nil {
		t.Fatal("Repair returned nil result")
	}
	verify, err := api.VerifyShard("archive")
	if err != nil {
		t.Fatalf("VerifyShard: %v", err)
	}
	if verify == nil {
		t.Fatal("VerifyShard returned nil result")
	}

	if ops.archiveID != nodeID || ops.restoreID != nodeID || ops.verifyShard != "archive" ||
		ops.moveShard != [2]string{"2026-W43", "warm"} {
		t.Fatalf("forwarded args = archive %d restore %d shard %q move %q", ops.archiveID, ops.restoreID, ops.verifyShard, ops.moveShard)
	}
	if ops.archiveCalls != 1 || ops.restoreCalls != 1 || ops.forceRotateCalls != 1 ||
		ops.listShardsCalls != 2 || ops.rebuildCatalogCalls != 1 || ops.repairCalls != 1 ||
		ops.verifyShardCalls != 1 || ops.moveShardCalls != 1 || ops.accessStatsCalls != 1 {
		t.Fatalf("unexpected call counts: %+v", ops)
	}
}

type tierOpsSpy struct {
	archiveID types.NodeID
	restoreID types.NodeID

	verifyShard string
	moveShard   [2]string
	shards      []tiered.ShardInfo

	archiveCalls        int
	restoreCalls        int
	forceRotateCalls    int
	listShardsCalls     int
	rebuildCatalogCalls int
	repairCalls         int
	verifyShardCalls    int
	moveShardCalls      int
	accessStatsCalls    int
}

func (s *tierOpsSpy) ShardAccessStats() (tiered.ShardAccessStats, error) {
	s.accessStatsCalls++
	return tiered.ShardAccessStats{Cold: 7, ColdOpens: 2}, nil
}

func (s *tierOpsSpy) Archive(id types.NodeID) error {
	s.archiveCalls++
	s.archiveID = id
	return nil
}

func (s *tierOpsSpy) Restore(id types.NodeID) error {
	s.restoreCalls++
	s.restoreID = id
	return nil
}

func (s *tierOpsSpy) ForceRotate() error {
	s.forceRotateCalls++
	return nil
}

func (s *tierOpsSpy) ListShards() ([]tiered.ShardInfo, error) {
	s.listShardsCalls++
	if s.shards == nil {
		s.shards = []tiered.ShardInfo{{Name: "hot"}}
	}
	return s.shards, nil
}

func (s *tierOpsSpy) MoveShard(_ context.Context, name, volume string) error {
	s.moveShardCalls++
	s.moveShard = [2]string{name, volume}
	return nil
}

func (s *tierOpsSpy) RebuildCatalog() error {
	s.rebuildCatalogCalls++
	return nil
}

func (s *tierOpsSpy) Repair() (*tiered.RepairResult, error) {
	s.repairCalls++
	return &tiered.RepairResult{}, nil
}

func (s *tierOpsSpy) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	s.verifyShardCalls++
	s.verifyShard = shardName
	return &tiered.VerifyResult{}, nil
}
