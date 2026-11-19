package tiered

import (
	"testing"
)

// TestRollbackScope_SkipsOnlyShardsSealedBeforeTheStamp (CUTCOST H1): a
// rollback scoped at a write stamp skips a closed shard only when its seal
// was taken, after its last writable handle, at or before the stamp. A
// read-only open and close after the stamp wrote nothing and keeps it
// skipped; a writable handle after the stamp does not, open or closed again;
// without a scope nothing is skipped.
func TestRollbackScope_SkipsOnlyShardsSealedBeforeTheStamp(t *testing.T) {
	t.Parallel()
	ts, _, _, old, _ := closedColdFixtureWith(t, volumeCfg(t.TempDir(), t.TempDir(), ""))
	defer func() { _ = ts.Close() }()
	es := ts.EventShardsForTest()[old]
	skipped := func(stamp uint64) bool {
		end := ts.ScopeCreatedSince(stamp)
		defer end()
		return ts.sealedBeforeCreated(es)
	}
	open := func(readOnly bool) func() {
		t.Helper()
		_, release, err := es.checkoutForReadCharged(ts, readOnly, nil)
		if err != nil {
			t.Fatal(err)
		}
		return release
	}
	idleClose := func() {
		t.Helper()
		es.SetLastAccessForTest(0)
		ts.SetIdleTimeoutForTest(0)
		ts.CloseIdleShardsForTest()
		if es.Store() != nil || es.cachedCounts() == nil {
			t.Fatalf("the idle sweep left the shard open (%v) or unsealed", es.Store() != nil)
		}
	}
	open(true)() // the fixture closed the shard by hand: a read's open and close seal it
	if es.Store() != nil || es.cachedCounts() == nil {
		t.Fatal("fixture: the shard is not closed with its seal")
	}
	stamp := ts.WriteStamp()
	if !skipped(stamp) || ts.sealedBeforeCreated(es) {
		t.Fatalf("sealed before the stamp: skipped %v in the scope, %v outside it; want true and false", skipped(stamp), ts.sealedBeforeCreated(es))
	}
	open(true)()
	if !skipped(stamp) {
		t.Fatal("a read-only open and close after the stamp wrote nothing: want the shard still skipped")
	}
	release := open(false)
	if skipped(stamp) {
		t.Fatal("open writable after the stamp: want it read")
	}
	release()
	idleClose()
	if skipped(stamp) {
		t.Fatal("closed after a writable handle that followed the stamp: want it read, it may hold what the transaction wrote")
	}
	if !skipped(ts.WriteStamp()) {
		t.Fatal("a transaction that begins now wrote nothing there: want it skipped")
	}
}
