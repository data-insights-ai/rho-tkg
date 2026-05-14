package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestBadgerStoreHighFrequencyIndex_BackfillsExistingNodes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 3_600_000})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	got := bs.HFIndexPointQueryForTest(1, 3_600_000)
	if !containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI point query = %v, want node %d", got, n.ID())
	}
}

func TestBadgerStoreHighFrequencyIndex_MaintainsNodeWrites(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 3_600_000})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if got := bs.HFIndexPointQueryForTest(1, 3_600_000); !containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI point query after PutNode = %v, want node %d", got, n.ID())
	}

	updated := n.DeepCopy()
	updated.SetTemporal(&types.TemporalMetadata{ValidFrom: 7_200_000})
	if err := bs.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	if got := bs.HFIndexPointQueryForTest(1, 3_600_000); containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI old bucket after ReplaceNode = %v, want node removed", got)
	}
	if got := bs.HFIndexPointQueryForTest(1, 7_200_000); !containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI new bucket after ReplaceNode = %v, want node %d", got, n.ID())
	}

	if err := bs.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if got := bs.HFIndexPointQueryForTest(1, 7_200_000); containsHFNodeID(got, n.ID()) {
		t.Fatalf("HFI point query after DeleteNode = %v, want node removed", got)
	}
}

func TestBadgerStoreHighFrequencyIndex_DefinitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labelTok := uint16(1)

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(100)), labelTok, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 3_600_000})
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs1.CreateHighFrequencyIndex(labelTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer func() { _ = bs2.Close() }()

	hasTemporal, hasHFI, bucketSize, err := bs2.TemporalIndexState(labelTok)
	if err != nil {
		t.Fatalf("TemporalIndexState: %v", err)
	}
	if hasTemporal || !hasHFI || bucketSize != time.Hour {
		t.Fatalf("TemporalIndexState = temporal:%v hfi:%v bucket:%v, want only HFI bucket 1h", hasTemporal, hasHFI, bucketSize)
	}
	got := bs2.HFIndexPointQueryForTest(labelTok, 3_600_000)
	if !containsHFNodeID(got, n.ID()) {
		t.Fatalf("reloaded HFI point query = %v, want node %d", got, n.ID())
	}
	nodes, err := bs2.NodesByLabel(labelTok, QueryOpts{ValidAt: 7_200_000})
	if err != nil {
		t.Fatalf("NodesByLabel ValidAt through HFI: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != n.ID() {
		t.Fatalf("NodesByLabel ValidAt through HFI = %#v, want node %d", nodes, n.ID())
	}
}

func TestBadgerStoreHighFrequencyIndex_DropDefinitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labelTok := uint16(1)

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := bs1.CreateHighFrequencyIndex(labelTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := bs1.DropHighFrequencyIndex(labelTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer func() { _ = bs2.Close() }()
	if bs2.HasHFIndexForTest(labelTok) {
		t.Fatal("dropped HFI definition reappeared after restart")
	}
}

func TestBadgerStoreHighFrequencyIndex_ClearDefinitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labelTok := uint16(1)

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := bs1.CreateHighFrequencyIndex(labelTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := bs1.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer func() { _ = bs2.Close() }()
	if bs2.HasHFIndexForTest(labelTok) {
		t.Fatal("cleared HFI definition reappeared after restart")
	}
}

func TestBadgerStoreHighFrequencyIndexRejectsTemporalIndexSameLabel(t *testing.T) {
	bs := newTestBadgerStore(t)

	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := bs.CreateTemporalIndex(1); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("CreateTemporalIndex after HFI = %v, want ErrTemporalIndexExists", err)
	}
}

func TestBadgerStoreHighFrequencyIndex_BackfillFailureRollsBackPlaceholder(t *testing.T) {
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 3_600_000})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()
	if err := bs.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(n.ID().SnowflakeID()), []byte("corrupt-node-wire"))
	}); err != nil {
		t.Fatalf("corrupt node row: %v", err)
	}

	err := bs.CreateHighFrequencyIndex(1, time.Hour)
	if err == nil {
		t.Fatal("CreateHighFrequencyIndex returned nil for corrupt backfill row")
	}
	if bs.HasHFIndexForTest(1) {
		t.Fatal("failed CreateHighFrequencyIndex left high-frequency index placeholder installed")
	}
	if err := bs.DropHighFrequencyIndex(1); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("DropHighFrequencyIndex after failed create = %v, want ErrTemporalIndexNotFound", err)
	}
}

func TestBadgerStoreHighFrequencyIndexLoadRejectsInvalidDefinition(t *testing.T) {
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	defs := []hfIdxDef{{LabelToken: 1, BucketSizeMillis: 0}}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		t.Fatalf("marshal defs: %v", err)
	}
	if err := bs1.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.HighFrequencyIndexDefsKey, data)
	}); err != nil {
		t.Fatalf("write invalid defs: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	_, err = New(Config{Dir: dir})
	if !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("open with invalid HFI definition = %v, want ErrInvalidTemporalIndexConfig", err)
	}
}

func containsHFNodeID(ids []types.NodeID, want types.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
