package badger

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Direct tests for the store.GroupCommitCapability methods (Testing Rule 1).

func newSyncStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs, dir
}

func groupNode(id int64) *types.Node {
	n := types.NewNode(types.NodeID(id), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{})
	return n
}

// TestGroupCommitHoldsSyncFlushesUntilEnd: under SyncWrites every PutNode
// flushes; inside the window nothing flushes (PendingWriteCount grows) and
// EndGroupCommit drains the buffer in one flush.
func TestGroupCommitHoldsSyncFlushesUntilEnd(t *testing.T) {
	bs, _ := newSyncStore(t)
	if err := bs.PutNode(groupNode(2)); err != nil {
		t.Fatal(err)
	}
	if n := bs.PendingWriteCount(); n != 0 {
		t.Fatalf("SyncWrites store left %d pending ops outside a window", n)
	}
	bs.BeginGroupCommit()
	for i := int64(4); i <= 40; i += 2 {
		if err := bs.PutNode(groupNode(i)); err != nil {
			t.Fatal(err)
		}
	}
	if n := bs.PendingWriteCount(); n == 0 {
		t.Fatal("inside the group-commit window PutNode still flushed per mutation")
	}
	if err := bs.EndGroupCommit(); err != nil {
		t.Fatal(err)
	}
	if n := bs.PendingWriteCount(); n != 0 {
		t.Fatalf("EndGroupCommit left %d pending ops", n)
	}
	// After the window the per-mutation behaviour is back.
	if err := bs.PutNode(groupNode(42)); err != nil {
		t.Fatal(err)
	}
	if n := bs.PendingWriteCount(); n != 0 {
		t.Fatalf("after EndGroupCommit PutNode did not flush synchronously (%d pending)", n)
	}
}

// TestGroupCommitRowsAreDurableAfterEnd: rows written inside a window are
// readable after Close + reopen.
func TestGroupCommitRowsAreDurableAfterEnd(t *testing.T) {
	bs, dir := newSyncStore(t)
	bs.BeginGroupCommit()
	for i := int64(2); i <= 20; i += 2 {
		if err := bs.PutNode(groupNode(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.EndGroupCommit(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	for i := int64(2); i <= 20; i += 2 {
		if _, err := re.GetNode(types.NodeID(i)); err != nil {
			t.Fatalf("node %d not durable after group commit + reopen: %v", i, err)
		}
	}
}

// TestGroupCommitEndOnClosedStoreErrors: EndGroupCommit on a closed store
// returns the closed sentinel rather than hanging in WriteBatch.Flush.
func TestGroupCommitEndOnClosedStoreErrors(t *testing.T) {
	bs, _ := newSyncStore(t)
	bs.BeginGroupCommit()
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	err := bs.EndGroupCommit()
	if err == nil {
		t.Fatal("EndGroupCommit on a closed store returned nil")
	}
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("EndGroupCommit error = %v, want ErrStoreClosed", err)
	}
}

// TestGroupCommitEndClearsWindowEvenOnError: a failed End must not leave the
// store holding flushes forever.
func TestGroupCommitEndClearsWindowEvenOnError(t *testing.T) {
	bs, _ := newSyncStore(t)
	bs.BeginGroupCommit()
	_ = bs.Close()
	_ = bs.EndGroupCommit()
	if bs.groupCommit.Load() {
		t.Fatal("group-commit window still open after a failed EndGroupCommit")
	}
}
