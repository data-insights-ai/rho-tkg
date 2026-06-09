package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredStoreStoreWideAdminNilReceiversReturnErrNilStore(t *testing.T) {
	var ts *Store
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "ForceRotate", run: ts.ForceRotate},
		{name: "RotateHotShard", run: ts.RotateHotShard},
		{name: "RebuildCatalog", run: ts.RebuildCatalog},
		{name: "Clear", run: ts.Clear},
	}
	for _, check := range checks {
		if err := check.run(); !errors.Is(err, ErrNilStore) {
			t.Fatalf("%s nil receiver = %v, want ErrNilStore", check.name, err)
		}
	}
}

type panicHashVerifier struct{}

func (*panicHashVerifier) VerifyNodeChain(types.NodeID) (bool, error) {
	panic("nil verifier should not be invoked")
}

func (*panicHashVerifier) VerifyRelChain(types.RelID) (bool, error) {
	panic("nil verifier should not be invoked")
}

func TestVerifyShardNilVerifierReturnsInvalidStoreMutation(t *testing.T) {
	ts := newTestTieredStore(t)

	if _, err := ts.VerifyShard(nil, "reference"); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("VerifyShard(nil verifier) = %v, want ErrInvalidStoreMutation", err)
	}

	var verifier *panicHashVerifier
	if _, err := ts.VerifyShard(verifier, "reference"); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("VerifyShard(typed nil verifier) = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestRebuildCatalog_CountsClosedColdEventShard(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	hotName := ts.HotShardForTest().Name()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	demoteToCold(ts, hotName)
	if !ts.CatalogForTest().UpdateShardStats(hotName, 0, 0) {
		t.Fatalf("UpdateShardStats(%q) returned false", hotName)
	}

	cold := ts.EventShardsForTest()[hotName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	if err := ts.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog: %v", err)
	}
	entry, ok := ts.CatalogForTest().GetShard(hotName)
	if !ok {
		t.Fatalf("GetShard(%q) returned false", hotName)
	}
	if entry.ApproxNodes != 1 || entry.ApproxRels != 0 {
		t.Fatalf("rebuilt cold shard stats = nodes %d rels %d, want 1/0", entry.ApproxNodes, entry.ApproxRels)
	}
	cold.LockShardMuForTest()
	defer cold.UnlockShardMuForTest()
	if cold.Store() != nil {
		t.Fatal("RebuildCatalog left transiently opened cold shard open")
	}
}

func TestRebuildCatalog_ReturnsColdShardOpenFailure(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	coldName := ts.HotShardForTest().Name()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	demoteToCold(ts, coldName)
	if !ts.CatalogForTest().UpdateShardStats(coldName, 7, 3) {
		t.Fatalf("UpdateShardStats(%q) returned false", coldName)
	}

	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	shardPath := filepath.Join(ts.dataDir, cold.Path())
	cold.UnlockShardMuForTest()

	if err := os.RemoveAll(shardPath); err != nil {
		t.Fatalf("RemoveAll shard path: %v", err)
	}
	if err := os.WriteFile(shardPath, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile shard path: %v", err)
	}

	err := ts.RebuildCatalog()
	if err == nil {
		t.Fatal("RebuildCatalog returned nil for cold shard open failure")
	}
	if !strings.Contains(err.Error(), "open event shard") {
		t.Fatalf("RebuildCatalog error = %v, want open event shard context", err)
	}
	entry, ok := ts.CatalogForTest().GetShard(coldName)
	if !ok {
		t.Fatalf("GetShard(%q) returned false", coldName)
	}
	if entry.ApproxNodes != 7 || entry.ApproxRels != 3 {
		t.Fatalf("failed RebuildCatalog stats = nodes %d rels %d, want rolled-back 7/3", entry.ApproxNodes, entry.ApproxRels)
	}
}

func TestAllShardStoresWithLazyOpen_ClosesTransientColdEventShardAfterRelease(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	coldName := ts.HotShardForTest().Name()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	demoteToCold(ts, coldName)
	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	sawCold := false
	for _, ns := range stores {
		if ns.name == coldName {
			sawCold = true
			break
		}
	}
	if !sawCold {
		t.Fatalf("allShardStoresWithLazyOpen did not include cold shard %q", coldName)
	}
	if cold.ActiveReqsForTest().Load() == 0 {
		t.Fatal("cold shard was not pinned while all-shard snapshot was live")
	}
	cold.LockShardMuForTest()
	storeWhilePinned := cold.Store()
	cold.UnlockShardMuForTest()
	if storeWhilePinned == nil {
		t.Fatal("cold shard was not open while all-shard snapshot was live")
	}

	release()
	if cold.ActiveReqsForTest().Load() != 0 {
		t.Fatalf("activeReqs after release = %d, want 0", cold.ActiveReqsForTest().Load())
	}
	cold.LockShardMuForTest()
	defer cold.UnlockShardMuForTest()
	if cold.Store() != nil {
		t.Fatal("allShardStoresWithLazyOpen release left transiently opened cold shard open")
	}
}
