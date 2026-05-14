package tiered

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newTestTieredStore creates an in-memory Store with Case/User as
// reference labels. Mirrors the helper in pkg/graph so tests moved into this
// package can construct a Store the same way.
func newTestTieredStore(t *testing.T) *Store {
	t.Helper()
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, // disable periodic flush
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts
}

func newDiskTestTieredStore(t *testing.T) *Store {
	t.Helper()
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, // disable periodic flush
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts
}

func installDefaultTestLabelRegistry(t *testing.T, ts *Store) (caseTok, userTok, signalTok uint16) {
	t.Helper()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	var err error
	caseTok, err = reg.GetOrCreate("Case")
	if err != nil {
		t.Fatal(err)
	}
	userTok, err = reg.GetOrCreate("User")
	if err != nil {
		t.Fatal(err)
	}
	signalTok, err = reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatal(err)
	}
	return caseTok, userTok, signalTok
}

// newTestGen creates a snowflake generator for testing. Mirrors the production
// layout (5 node bits, 10 step bits, microsecond precision).
func newTestGen(t *testing.T, nodeID int64) *snowflake.Node {
	t.Helper()
	gen, err := snowflake.NewNode(nodeID,
		snowflake.WithEpoch(snowflakepkg.Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return gen
}

// tieredNodeGen and tieredRelGen for test entities.
func tieredNodeGen(t *testing.T) *snowflake.Node {
	t.Helper()
	return newTestGen(t, 0)
}

func tieredRelGen(t *testing.T) *snowflake.Node {
	t.Helper()
	return newTestGen(t, 1)
}

// makeRefNode creates a node with a reference label token.
func makeRefNode(t *testing.T, gen *snowflake.Node, ts *Store) *types.Node {
	t.Helper()
	// Token 1 = first label registered. We need the ontology to know about it.
	// For direct store tests, use token 1 and set up the ontology to recognize it.
	return types.NewNode(types.NodeID(gen.Generate()), 1, nil) // token 1 = reference
}

// makeEvtNode creates a node with an event label token.
func makeEvtNode(t *testing.T, gen *snowflake.Node, ts *Store) *types.Node {
	t.Helper()
	return types.NewNode(types.NodeID(gen.Generate()), 3, nil) // token 3 = event (not in ref list)
}

// forceRotation sets the hot shard's timeEnd to the past, triggering rotation
// on the next checkRotation call. Sleeps 2ms after rotation so that new
// snowflake IDs generated afterward have timestamps cleanly within the new
// hot shard's window (snowflake IDs have millisecond resolution).
func forceRotation(t *testing.T, ts *Store) {
	t.Helper()
	ts.MuForTest().Lock()
	ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts.MuForTest().Unlock()
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatalf("forceRotation: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
}

// demoteToCold manually sets a shard to cold tier. Test-only helper.
//
// Bypasses the normal warm→cold transition (driven by ColdAfter and the
// idle-close goroutine) so tests can deterministically observe behaviour
// against a cold shard without sleeping. Holds ts.MuForTest() across the tier flip
// AND the catalog update so a concurrent rotation cannot read a half-updated
// state — but does NOT close the underlying BadgerStore. Pair with
// closeEventShardStore to fully simulate a cold idle-close, or leave the
// store open to test pure tier-based code paths.
func demoteToCold(ts *Store, shardName string) {
	ts.MuForTest().Lock()
	defer ts.MuForTest().Unlock()
	if es, ok := ts.EventShardsForTest()[shardName]; ok {
		es.SetTierForTest(TierCold)
		ts.CatalogForTest().UpdateShardTier(shardName, TierCold)
	}
}

// injectCorruptMemFile creates a .mem WAL file in a Badger directory that
// simulates a partially written WAL from an unclean shutdown (e.g., SIGKILL
// during flush). The file has a valid 20-byte vlog header followed by non-zero
// garbage, which causes Badger's iterate() to stop early (endOff < file size),
// returning ErrTruncateNeeded when opened in read-only mode.
//
// This must be called AFTER the store is closed — during clean close, Badger
// flushes memtables to SSTables and deletes .mem files. An unclean shutdown
// leaves the .mem file on disk.
func injectCorruptMemFile(t *testing.T, badgerDir string) string {
	t.Helper()

	// Use fid 0 — matches Badger's naming convention (00000.mem).
	path := filepath.Join(badgerDir, "00000.mem")

	// vlog header: 8 bytes key ID (0 = plaintext) + 12 bytes base IV.
	const vlogHeaderSize = 20
	const fileSize = 4096 // small but larger than header → triggers truncation check

	// Badger WAL entry format after the 20-byte vlog header:
	//   Meta (1B) | UserMeta (1B) | klen (uvarint) | vlen (uvarint) | expiresAt (uvarint) | ...
	//
	// safeRead.Entry() returns errTruncate when klen > 1<<16 (65536).
	// iterate() catches errTruncate and breaks cleanly with
	// validEndOffset = vlogHeaderSize (20). Since 20 < fileSize,
	// UpdateSkipList returns ErrTruncateNeeded in read-only mode.
	data := make([]byte, fileSize)
	// Header: key ID = 0 (8 bytes), base IV = 0 (12 bytes) — valid plaintext.

	// After header: craft an entry whose klen > 1<<16 → triggers errTruncate.
	data[vlogHeaderSize] = 0x00   // meta
	data[vlogHeaderSize+1] = 0x00 // userMeta
	// klen = 65537 as uvarint: 0x81 0x80 0x04
	// 65537 = 1 + (0 << 7) + (4 << 14) = 1 + 65536
	data[vlogHeaderSize+2] = 0x81 // klen byte 0: value=1, continuation=1
	data[vlogHeaderSize+3] = 0x80 // klen byte 1: value=0, continuation=1
	data[vlogHeaderSize+4] = 0x04 // klen byte 2: value=4, continuation=0 → 1 + 0*128 + 4*16384 = 65537

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt .mem file: %v", err)
	}
	return path
}

// warmShardDir returns the on-disk directory for a warm shard.
func warmShardDir(ts *Store, baseDir string) (string, string) {
	for name, es := range ts.EventShardsForTest() {
		if es.Tier() == TierWarm {
			return filepath.Join(baseDir, es.Path()), name
		}
	}
	return "", ""
}
