// Tests in this file pin the F9 fix from the 2026-05-08 maintainability
// review: the loadIndexes pass that runs at Open used to silently `continue`
// past every loadNodeFromBadger error while rebuilding the property and
// temporal in-memory indexes, leaving operators with no signal that the
// persisted state was degraded. The fix records per-rebuild atomic counters
// (and warns through cfg.Logger when one is supplied), surfaced via
// IndexRebuildStats. These tests exercise both rebuild loops.

package badger

import (
	"sync/atomic"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
)

// captureLogger collects Warningf payloads so a test can assert that
// loadIndexes emitted a warning per skipped record.
type captureLogger struct {
	warnings atomic.Int64
}

func (l *captureLogger) Errorf(string, ...any)            {}
func (l *captureLogger) Infof(string, ...any)             {}
func (l *captureLogger) Debugf(string, ...any)            {}
func (l *captureLogger) Warningf(format string, _ ...any) { l.warnings.Add(1) }

// dropNodeKey opens a raw Badger DB at the given directory and deletes the
// 0x01/<id> row that holds the node entity. The 0x03/<labelToken>/<id>
// label-index row is left intact so loadIndexes still finds the node when
// rebuilding labelIdx, then fails to load the entity, and must report the
// skip via IndexRebuildStats.
func dropNodeKey(t *testing.T, dir string, id int64) {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("raw badger open: %v", err)
	}
	defer db.Close()
	if err := db.Update(func(txn *badgerv4.Txn) error {
		return txn.Delete(storepkg.NodeKey(snowflake.ID(id)))
	}); err != nil {
		t.Fatalf("delete node row: %v", err)
	}
}

func TestIndexRebuildStats_PropertyIndex_SkipsAreReportedAndLogged(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	const labelTok = uint16(7)
	const nodeID = int64(101)
	n := putTestNode(t, bs1, nodeID, labelTok, nil)
	if err := n.SetProperty("name", "alice"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs1.ReplaceNode(n); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	if err := bs1.CreatePropertyIndex(labelTok, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	dropNodeKey(t, dir, nodeID)

	logger := &captureLogger{}
	bs2, err := New(Config{Dir: dir, Logger: logger})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	stats := bs2.IndexRebuildStats()
	if stats.PropertySkipped != 1 {
		t.Errorf("PropertySkipped = %d, want 1", stats.PropertySkipped)
	}
	if stats.TemporalSkipped != 0 {
		t.Errorf("TemporalSkipped = %d, want 0", stats.TemporalSkipped)
	}
	if logger.warnings.Load() == 0 {
		t.Errorf("Warningf was never invoked; F9 fix must log per-skip diagnostics")
	}
}

func TestIndexRebuildStats_TemporalIndex_SkipsAreReportedAndLogged(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	const labelTok = uint16(11)
	const nodeID = int64(202)
	putTestNode(t, bs1, nodeID, labelTok, nil)
	if err := bs1.CreateTemporalIndex(labelTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	dropNodeKey(t, dir, nodeID)

	logger := &captureLogger{}
	bs2, err := New(Config{Dir: dir, Logger: logger})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	stats := bs2.IndexRebuildStats()
	if stats.TemporalSkipped != 1 {
		t.Errorf("TemporalSkipped = %d, want 1", stats.TemporalSkipped)
	}
	if stats.PropertySkipped != 0 {
		t.Errorf("PropertySkipped = %d, want 0", stats.PropertySkipped)
	}
	if logger.warnings.Load() == 0 {
		t.Errorf("Warningf was never invoked; F9 fix must log per-skip diagnostics")
	}
}

func TestIndexRebuildStats_CleanReopen_ReportsZero(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	const labelTok = uint16(13)
	n := putTestNode(t, bs1, 303, labelTok, nil)
	if err := n.SetProperty("name", "bob"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs1.ReplaceNode(n); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	if err := bs1.CreatePropertyIndex(labelTok, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs1.CreateTemporalIndex(labelTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	stats := bs2.IndexRebuildStats()
	if stats.PropertySkipped != 0 || stats.TemporalSkipped != 0 {
		t.Errorf("clean reopen reported skips: %+v", stats)
	}
}

// Sanity: deleting a row that doesn't exist must not return an error from the
// rebuild path — exercises the negative branch (no skip).
func TestIndexRebuildStats_LoggerNil_NoPanic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	const labelTok = uint16(17)
	const nodeID = int64(404)
	putTestNode(t, bs1, nodeID, labelTok, nil)
	if err := bs1.CreatePropertyIndex(labelTok, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	dropNodeKey(t, dir, nodeID)

	bs2, err := New(Config{Dir: dir}) // no Logger
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	if got := bs2.IndexRebuildStats().PropertySkipped; got != 1 {
		t.Errorf("PropertySkipped = %d, want 1", got)
	}
	// If we got here without panicking, the nil-logger branch is correct.
}
