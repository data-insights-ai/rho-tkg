package main

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

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
}
