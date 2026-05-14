package admin

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestAPINilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	for _, check := range []struct {
		name string
		err  error
	}{
		{name: "nil Archive", err: nilAPI.Archive(1)},
		{name: "nil Restore", err: nilAPI.Restore(1)},
		{name: "nil ForceRotate", err: nilAPI.ForceRotate()},
		{name: "nil RebuildCatalog", err: nilAPI.RebuildCatalog()},
		{name: "nil Reset", err: nilAPI.Reset()},
	} {
		if !errors.Is(check.err, grapherr.ErrNilGraph) {
			t.Fatalf("%s = %v, want ErrNilGraph", check.name, check.err)
		}
	}
	if _, err := nilAPI.ListShards(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ListShards = %v, want ErrNilGraph", err)
	}
	if _, err := nilAPI.Repair(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil Repair = %v, want ErrNilGraph", err)
	}
	if _, err := nilAPI.VerifyShard("hot"); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil VerifyShard = %v, want ErrNilGraph", err)
	}
	if got := nilAPI.DecomposeID(1); got != (IDComponents{}) {
		t.Fatalf("nil DecomposeID = %+v, want zero", got)
	}

	api := New((*adminOpsSpy)(nil))
	if err := api.ForceRotate(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil ForceRotate = %v, want ErrNilGraph", err)
	}
	if got := api.DecomposeID(1); got != (IDComponents{}) {
		t.Fatalf("typed-nil DecomposeID = %+v, want zero", got)
	}
}

func TestAPIForwardsEveryMethod(t *testing.T) {
	t.Parallel()

	ops := &adminOpsSpy{}
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
	if err := api.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if got := api.DecomposeID(99); got.NodeID != 7 {
		t.Fatalf("DecomposeID = %+v, want NodeID 7", got)
	}

	if ops.archiveID != nodeID || ops.restoreID != nodeID || ops.verifyShard != "archive" {
		t.Fatalf("forwarded args = archive %d restore %d shard %q", ops.archiveID, ops.restoreID, ops.verifyShard)
	}
	if ops.archiveCalls != 1 || ops.restoreCalls != 1 || ops.forceRotateCalls != 1 ||
		ops.listShardsCalls != 2 || ops.rebuildCatalogCalls != 1 || ops.repairCalls != 1 ||
		ops.verifyShardCalls != 1 || ops.resetCalls != 1 || ops.decomposeIDCalls != 1 {
		t.Fatalf("unexpected call counts: %+v", ops)
	}
}

type adminOpsSpy struct {
	archiveID types.NodeID
	restoreID types.NodeID

	verifyShard string
	shards      []tiered.ShardInfo

	archiveCalls        int
	restoreCalls        int
	forceRotateCalls    int
	listShardsCalls     int
	rebuildCatalogCalls int
	repairCalls         int
	verifyShardCalls    int
	resetCalls          int
	decomposeIDCalls    int
}

func (s *adminOpsSpy) Archive(id types.NodeID) error {
	s.archiveCalls++
	s.archiveID = id
	return nil
}

func (s *adminOpsSpy) Restore(id types.NodeID) error {
	s.restoreCalls++
	s.restoreID = id
	return nil
}

func (s *adminOpsSpy) ForceRotate() error {
	s.forceRotateCalls++
	return nil
}

func (s *adminOpsSpy) ListShards() ([]tiered.ShardInfo, error) {
	s.listShardsCalls++
	if s.shards == nil {
		s.shards = []tiered.ShardInfo{{Name: "hot"}}
	}
	return s.shards, nil
}

func (s *adminOpsSpy) RebuildCatalog() error {
	s.rebuildCatalogCalls++
	return nil
}

func (s *adminOpsSpy) Repair() (*tiered.RepairResult, error) {
	s.repairCalls++
	return &tiered.RepairResult{}, nil
}

func (s *adminOpsSpy) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	s.verifyShardCalls++
	s.verifyShard = shardName
	return &tiered.VerifyResult{}, nil
}

func (s *adminOpsSpy) Reset() error {
	s.resetCalls++
	return nil
}

func (s *adminOpsSpy) DecomposeID(id snowflake.ID) IDComponents {
	s.decomposeIDCalls++
	return IDComponents{NodeID: 7}
}
