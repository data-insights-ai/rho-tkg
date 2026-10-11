package badger

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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
	if err := bs.beginIdentityRead(); err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- bs.Close() }()
	for !bs.closing.Load() {
		runtime.Gosched()
	}
	select {
	case err := <-closeDone:
		t.Fatal("Close invalidated an admitted snapshot", err)
	default:
	}
	if err := bs.ForEachRelID(func(types.RelID) bool { return true }); !errors.Is(err, ErrStoreClosed) {
		t.Fatal("closing admitted a new iterator", err)
	}
	bs.identityReaders.Done()
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestDiskIdentityCallbackMaintenanceRefusalAndExternalRetry(t *testing.T) {
	config := diskIdentityConfig(t.TempDir())
	config.CacheBudgetBytes = 64 << 10
	bs, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	n := types.NewNode(1, 1, nil)
	if err := n.SetProperty("opaque", strings.Repeat("x", 2<<20)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	readDone := make(chan error, 1)
	var callbackErr error
	go func() {
		readDone <- bs.ForEachNodeID(func(types.NodeID) bool {
			if err := bs.Close(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
				callbackErr = errors.New("own callback Close was not explicitly refused")
			}
			if err := bs.Clear(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
				callbackErr = errors.New("own callback Clear was not explicitly refused")
			}
			close(entered)
			<-release
			return true
		})
	}()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- bs.Close() }()
	if err := <-closeDone; !errors.Is(err, storecontract.ErrCapabilityNotSupported) || bs.closing.Load() {
		t.Fatal("external maintenance refusal changed state", err)
	}
	close(release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiskIdentityCloseAdmissionReleaseRestoresReads(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	putTestNode(t, bs, 1, 1, nil)
	lease, err := bs.AdmitClose()
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatal("unsealed lease closed backend", err)
	}
	if err := bs.Close(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatal("direct Close stole reservation", err)
	}
	if ids, err := bs.AllNodeIDs(QueryOpts{}); err != nil || len(ids) != 1 {
		t.Fatal("reservation disabled accepted-work reads", ids, err)
	}
	if err := lease.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Seal(); err != nil {
		t.Fatal("seal replay", err)
	}
	if err := bs.ForEachNodeID(func(types.NodeID) bool { return true }); !errors.Is(err, ErrStoreClosed) {
		t.Fatal("prepared close admitted iteration", err)
	}
	if err := bs.Clear(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatal("Clear changed close reservation", err)
	}
	// Close admission fences readers, not the registry/clock writes Core must
	// perform before its final backend close.
	putTestNode(t, bs, 2, 1, nil)
	lease.Release()
	lease.Release()
	if err := lease.Seal(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatal("released lease resealed", err)
	}
	if err := lease.Close(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatal("released lease closed backend", err)
	}
	if ids, err := bs.AllNodeIDs(QueryOpts{}); err != nil || len(ids) != 2 {
		t.Fatal("abandoned admission lost usability", ids, err)
	}
	lease, err = bs.AdmitClose()
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if _, err = bs.AdmitClose(); !errors.Is(err, ErrStoreClosed) {
		t.Fatal("closed store readmitted close", err)
	}
	var nilStore *Store
	if _, err = nilStore.AdmitClose(); !errors.Is(err, ErrNilStore) {
		t.Fatal("nil admission", err)
	}
}

func TestDiskIdentityDefaultCloseNeedsNoReservation(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if bs.CloseAdmissionRequired() {
		t.Fatal("default mode needs admission")
	}
	if lease, err := bs.AdmitClose(); err != nil || lease != nil {
		t.Fatal("default close reserved opted gate", lease, err)
	}
	var missing *Store
	if missing.CloseAdmissionRequired() {
		t.Fatal("nil mode requires admission")
	}
}
