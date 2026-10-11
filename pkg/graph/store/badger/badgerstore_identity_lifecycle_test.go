package badger

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestDiskIdentityClosedDatabaseIterationReturnsErrors(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{
		func() error { _, e := bs.AllNodeIDs(QueryOpts{}); return e },
		func() error { _, e := bs.AllRelIDs(QueryOpts{}); return e },
		func() error { _, e := bs.NodesByLabel(1, QueryOpts{}); return e },
		func() error { _, e := bs.RelationshipsByType(1, QueryOpts{}); return e },
		func() error { return bs.ForEachNodeID(func(types.NodeID) bool { return true }) },
		func() error { return bs.ForEachRelID(func(types.RelID) bool { return true }) },
	} {
		if err := call(); !errors.Is(err, badgerv4.ErrDBClosed) {
			t.Fatal("closed storage became empty result or panic", err)
		}
	}
}

func TestDiskIdentityValueLogCallbackCanWriteAndRotate(t *testing.T) {
	config := diskIdentityConfig(t.TempDir())
	config.ValueLogFileSize = 4 << 20
	config.CacheBudgetBytes = 64 << 10
	bs, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	// Synthetic opaque protocol payloads exceed Badger's 1MiB value threshold.
	// They exercise value-log locking, not legal source text or interpretation.
	payload := strings.Repeat("x", 2<<20)
	put := func(id types.NodeID) error {
		n := types.NewNode(id, 1, nil)
		if err := n.SetProperty("opaque", payload); err != nil {
			return err
		}
		return bs.PutNode(n)
	}
	if err := put(1); err != nil {
		t.Fatal(err)
	}
	count := 0
	var callbackErr error
	err = bs.ForEachNodeID(func(id types.NodeID) bool {
		count++
		if id != 1 {
			callbackErr = errors.New("snapshot included later inserted identity")
			return false
		}
		callbackErr = put(2)
		if callbackErr == nil {
			callbackErr = put(3)
		}
		return callbackErr == nil
	})
	if err != nil || callbackErr != nil || count != 1 {
		t.Fatal("value-log callback reentry", count, err, callbackErr)
	}
	ids, err := bs.AllNodeIDs(QueryOpts{})
	if err != nil || len(ids) != 3 {
		t.Fatal("callback writes were lost", ids, err)
	}
}

func TestDiskIdentityCloseWaitsForAdmittedIterator(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	putTestNode(t, bs, 1, 1, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		readDone <- bs.ForEachNodeID(func(types.NodeID) bool {
			close(entered)
			<-release
			return true
		})
	}()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- bs.Close() }()
	for !bs.closing.Load() {
		runtime.Gosched()
	}
	select {
	case err := <-closeDone:
		t.Fatal("Close invalidated an admitted iterator", err)
	default:
	}
	if err := bs.ForEachRelID(func(types.RelID) bool { return true }); !errors.Is(err, ErrStoreClosed) {
		t.Fatal("closing admitted a new iterator", err)
	}
	close(release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}
