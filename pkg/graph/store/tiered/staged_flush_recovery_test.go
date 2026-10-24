package tiered

import (
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TestTieredWarmShardRecoversStagedFlush (FLUSHPART): a warm shard left with
// a committed, unapplied staged flush (a crash after its marker) reopens
// through openShardStoreWithRecovery: the read-only probe refuses the marker
// with an error matching badger's ErrTruncateNeeded, the writable open applies
// the staged entry and removes marker and staging. Catches a probe refusal the
// recovery path does not recognise (the tiered store would fail to open).
func TestTieredWarmShardRecoversStagedFlush(t *testing.T) {
	dir := t.TempDir()
	cfg := tuningCfg(dir)
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	evt := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(evt); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	var warmName string
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierWarm {
			warmName = es.Name()
		}
	}
	ts.MuForTest().RUnlock()
	if warmName == "" {
		t.Fatal("fixture: no warm shard")
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// The crash state: one staged entry (a meta probe key) and its marker.
	probe := storepkg.MetaKey("flushpart_tiered_probe")
	const seq = 42
	stageKey := make([]byte, 9, 9+len(probe))
	stageKey[0] = storepkg.KeyFlushStage
	binary.BigEndian.PutUint64(stageKey[1:], seq)
	stageKey = append(stageKey, probe...)
	marker := make([]byte, 17) // sequence, entry count, flags (1 = written under SyncWrites)
	binary.BigEndian.PutUint64(marker, seq)
	binary.BigEndian.PutUint64(marker[8:], 1)
	marker[16] = 1
	shardDir := filepath.Join(dir, "events", warmName)
	db, err := badgerv4.Open(badgerv4.DefaultOptions(shardDir).WithMemTableSize(cfg.MemTableSize).WithValueLogFileSize(cfg.ValueLogFileSize).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Set(stageKey, []byte{1, 0xAB}); err != nil { // 1 = writeOpSet
			return err
		}
		return txn.Set(storepkg.FlushStageMarkerKey, marker)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ts2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen with a staged flush on the warm shard: %v", err)
	}
	defer ts2.Close()
	if _, err := ts2.GetNode(evt.ID()); err != nil {
		t.Fatalf("event node lost: %v", err)
	}
	var warm *BadgerStore
	ts2.MuForTest().RLock()
	for _, es := range ts2.EventShardsForTest() {
		if es.Name() == warmName {
			warm = es.Store()
		}
	}
	ts2.MuForTest().RUnlock()
	if warm == nil {
		t.Fatal("warm shard not open after reopen")
	}
	if err := warm.DBForTest().View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(probe)
		if err != nil {
			t.Fatalf("the staged entry was not applied: %v", err)
		}
		if err := item.Value(func(v []byte) error {
			if len(v) != 1 || v[0] != 0xAB {
				t.Fatalf("applied value %x, want ab", v)
			}
			return nil
		}); err != nil {
			return err
		}
		if _, err := txn.Get(storepkg.FlushStageMarkerKey); err != badgerv4.ErrKeyNotFound {
			t.Fatalf("marker left after recovery (%v)", err)
		}
		if _, err := txn.Get(stageKey); err != badgerv4.ErrKeyNotFound {
			t.Fatalf("staging left after recovery (%v)", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
