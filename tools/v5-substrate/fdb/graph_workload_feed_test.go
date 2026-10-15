package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Only the observation contract is exercised here. Synthetic completion counts
// cannot establish an SDK workload, source oracle or measured performance.
func TestGraphFeedBusinessAccountingKeepsNilLegacyAndWholePhaseBoundaries(t *testing.T) {
	legacy := &graphFeedWorkloadArtifacts{}
	called := false
	if err := legacy.businessPhase("create", func() error { called = true; return nil }); err != nil || !called {
		t.Fatal("nil timing hook changed legacy callback", err)
	}
	body, err := json.Marshal(legacy.end)
	if err != nil || bytes.Contains(body, []byte("BusinessAccounting")) {
		t.Fatal("nil extension changed historical terminal fields", err)
	}
	events, err := os.CreateTemp(t.TempDir(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	latencies, err := os.CreateTemp(t.TempDir(), "latencies")
	if err != nil {
		t.Fatal(err)
	}
	defer latencies.Close()
	a := &graphFeedWorkloadArtifacts{ctx: t.Context(), h: &harness{config: config{Cost: graphCostProfile{Facts: 2}}}, events: events, latencies: latencies, eventHash: sha256.New()}
	if err := a.startBusinessAccounting(); err != nil {
		t.Fatal(err)
	}
	if err := a.startBusinessAccounting(); !errors.Is(err, errInvalid) {
		t.Fatal("clock reset", err)
	}
	if err := a.businessPhase("update", func() error { t.Fatal("out-of-order phase ran"); return nil }); !errors.Is(err, errInvalid) {
		t.Fatal("out-of-order phase admitted", err)
	}
	if err := a.businessPhase("create", func() error {
		begin := time.Now()
		a.end.Creates += 2
		return a.latencyAt("create", 0, 0, begin, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.businessPhase("update", func() error { a.end.Updates += 2; return nil }); err != nil {
		t.Fatal(err)
	}
	x, y := a.end.BusinessAccounting.Phases[0], a.end.BusinessAccounting.Phases[1]
	if x.Useful != 2 || y.Useful != 2 || x.StopNanos > y.StartNanos || x.WallNanos != x.StopNanos-x.StartNanos || y.WallNanos != y.StopNanos-y.StartNanos || a.end.BusinessAccounting.BusinessNanos != x.WallNanos+y.WallNanos {
		t.Fatal("phase wall/count/ordering corrupted", a.end.BusinessAccounting)
	}
	if err := a.businessPhase("update", func() error { t.Fatal("duplicate phase ran"); return nil }); !errors.Is(err, errInvalid) {
		t.Fatal("duplicate phase admitted", err)
	}
}

// Authored source guards only; the complete actual process/cell path is the
// fresh workload executable, never a reconstructed receipt or fake SDK result.
func TestGraphFeedWorkloadFreshBindingAndOwnedChildGuards(t *testing.T) {
	nonce := "01020304050607080910111213141516"
	for _, writers := range []int{1, 8, 32} {
		for _, layout := range []string{"kv", "columns"} {
			spec := graphFeedWorkloadSpec{1, graphFeedWorkloadFamily, 40, 50, writers, layout}
			c := graphFeedWorkloadConfig(spec, nonce)
			if !validGraphFeedWorkloadConfig(c) || !validGraphCostProfile(c) || c.Cost.Version != 5 || graphCostLimit(c) != graphFeedWorkloadLimit(c.Cost) {
				t.Fatal("fresh actual dispatch/budget", c)
			}
			old := c
			old.Cost.Version = 1
			if validGraphFeedWorkloadConfig(old) {
				t.Fatal("legacy profile relabelled feed")
			}
			if err := graphFeedPolicyFor(c.Cost).validate(); err != nil {
				t.Fatal(err)
			}
		}
	}
	directory := t.TempDir()
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if a, err := newGraphFeedWorkloadArtifacts(ctx, new(harness), root, graphFeedWorkloadSpec{}, graphCostStoredRecord{}, cut{}); a != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled run opened artifact handles", a, err)
	}
	if _, err := root.Stat("fdb-feed-attempts.jsonl"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("canceled constructor created output", err)
	}
	a := &graphFeedWorkloadArtifacts{ctx: ctx}
	if err := a.event(struct{}{}); !errors.Is(err, context.Canceled) {
		t.Fatal("artifact event detached from original canceled run", err)
	}
	// A zero database is deliberate: already-canceled new IO must refuse before
	// attempting SDK entry. These guards do not stand in for actual SDK evidence.
	var db fdb.Database
	if _, err := newHarnessContext(ctx, db, config{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled config initialization entered SDK", err)
	}
	if _, err := graphCostReadContext(ctx, db, func(fdb.ReadTransaction) (any, error) {
		t.Fatal("canceled read callback entered")
		return nil, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal("read lost original cancellation", err)
	}
	if _, err := graphCostWriteContext(ctx, db, func(fdb.Transaction) (any, error) {
		t.Fatal("canceled mutation callback entered")
		return nil, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal("mutation lost original cancellation", err)
	}
	if bounded, stop := graphCostIOContext(nil, ctx); bounded != nil {
		stop()
		t.Fatal("legacy nil IO acquired a new lifetime")
	} else {
		stop()
	}
	for _, cancelRun := range []bool{false, true} {
		run, endRun := context.WithCancel(t.Context())
		caller, endCaller := context.WithCancel(t.Context())
		bounded, stop := graphCostIOContext(run, caller)
		if cancelRun {
			endRun()
		} else {
			endCaller()
		}
		wait, endWait := context.WithTimeout(t.Context(), time.Second)
		select {
		case <-bounded.Done():
			if !errors.Is(bounded.Err(), context.Canceled) {
				t.Fatal("combined IO lost cancellation", bounded.Err())
			}
		case <-wait.Done():
			t.Fatal("caller/run cancellation detached from same IO lifetime")
		}
		endWait()
		stop()
		endRun()
		endCaller()
	}
	original := graphFeedChildInput{ReaderRequest: "factory-original", PageRequest: "same-page", DeadlineNanos: 123}
	if err := feedChildWrite(root, "input.json", original); err != nil {
		t.Fatal(err)
	}
	actual, err := feedChildRead[graphFeedChildInput](root, "input.json")
	if err != nil || !graphColumnSame(actual, original) {
		t.Fatal("child original changed", err)
	}
	// An existing final file is immutable too. O_EXCL on the temporary alone is
	// insufficient if Rename would silently replace the established original.
	if err := feedChildWrite(root, "input.json", original); !errors.Is(err, os.ErrExist) {
		t.Fatal("replaced caller original", err)
	}
	file, err := root.OpenFile("oversize.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(bytes.Repeat([]byte{'x'}, (512<<10)+1)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := feedChildRead[graphFeedChildInput](root, "oversize.json"); !errors.Is(err, errLimit) {
		t.Fatal("unbounded child input decode", err)
	}
}

// Pure callback-contract guard, authored only: this is not an actual unknown
// SDK outcome. Both a pending cleanup and its successful same-handle retry must
// report the durable ticket's cleanup fields without losing or relabelling them.
func TestGraphFeedOwnedCleanupWorkAndPendingHandle(t *testing.T) {
	legacy := newSnapshotLifetime(new(graphHistoryRead), graphSnapshotCleanupDefault())
	legacy.allowance.work = graphSnapshotWork{Rows: 64, Bytes: 4 << 20, Callbacks: 2}
	if got := legacy.work(); got != (graphSnapshotWork{CleanupRows: 64, CleanupBytes: 4 << 20, CleanupCallbacks: 2}) {
		t.Fatal("legacy cleanup accounting changed", got)
	}
	owner := graphHistoryOwner{Kind: "read", Request: "same-issued-owner", Secret: [32]byte{1}, State: historyActive}
	owned := newSnapshotLifetime(&graphHistoryRead{id: owner}, graphSnapshotCleanupDefault())
	cause, calls := errors.New("cleanup acknowledgement unobserved"), uint64(0)
	owned.ownedClose = func() (graphSnapshotWork, error) {
		calls++
		work := graphSnapshotWork{CleanupRows: calls * 1024, CleanupBytes: calls * (64 << 20)}
		if calls == 1 {
			return work, errors.Join(errUnknown, cause)
		}
		return work, nil
	}
	err := owned.close()
	pending, ok := errors.AsType[*graphSnapshotReadPending](err)
	if !ok || !errors.Is(err, errUnknown) || !errors.Is(err, cause) || !graphColumnSame(pending.Identity(), owner) {
		t.Fatal("owned cleanup lost pending identity or cause", err)
	}
	if got := pending.Work(); got != (graphSnapshotWork{CleanupRows: 1024, CleanupBytes: 64 << 20}) {
		t.Fatal("pending ticket work lost or relabelled as ordinary rows", got)
	}
	if err := pending.Close(); err != nil {
		t.Fatal(err)
	}
	want := graphSnapshotWork{CleanupRows: 2048, CleanupBytes: 128 << 20}
	if got := pending.Work(); got != want {
		t.Fatal("successful retry reset cumulative ticket work", got)
	}
	if err := pending.Close(); err != nil || calls != 2 || pending.Work() != want {
		t.Fatal("cached Close consumed or reset cleanup credit", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	column := &graphColumnRead{s: &graphColumnStore{ctx: ctx}, id: graphColumnOwnerID{Request: "same-column-owner", Generation: "old", Secret: [32]byte{1}}, cleanupDeadline: time.Now().Add(-time.Second)}
	deadline := column.cleanupDeadline
	err = columnReadCleanup(column)
	columnPending, ok := errors.AsType[*graphColumnReadPending](err)
	if !ok || columnPending.Owner != column || !errors.Is(err, context.DeadlineExceeded) || column.closed || column.cleanupDeadline != deadline {
		t.Fatal("column cleanup reset deadline, lost same owner or entered raw canceled IO", err)
	}
	err = columnReadCleanup(columnPending.Owner)
	if !errors.Is(err, context.DeadlineExceeded) || column.cleanupDeadline != deadline || column.cleanupCallbacks != 0 {
		t.Fatal("pending column cleanup refreshed or spent expired credit", err)
	}
}
