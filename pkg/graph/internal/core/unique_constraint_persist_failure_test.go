package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// blockingFailUniqueMetaKV wraps a real MetaKVCapability but intercepts
// MetaSet on uniqueConstraintsMeta: the call signals entered (closing the
// channel) and then blocks on proceed before returning failErr. A test swaps
// this in for g.metaKV directly (a white-box field assignment, same
// package) AFTER New() so the graph's store-capability detection still sees
// a real, exact *memory.Store (createUnique's auto-ensure property-index
// step requires PropertyIndexCapability, which the "wrapper-visibility
// guard" — core.go's isExactNativeStore — deliberately declines for any
// non-native wrapper; wrapping the whole Store would break that step before
// ever reaching the persist call this test targets). This lets a test
// synchronize a concurrent observer to the exact window between "the
// failing write was attempted" and "the caller's critical section finishes
// handling the failure" — a plain sleep-based race would be flaky; this is
// deterministic.
type blockingFailUniqueMetaKV struct {
	inner   storepkg.MetaKVCapability
	entered chan struct{}
	proceed chan struct{}
	failErr error
}

func (m *blockingFailUniqueMetaKV) MetaGet(key string) ([]byte, error) {
	return m.inner.MetaGet(key)
}

func (m *blockingFailUniqueMetaKV) MetaSet(key string, v []byte) error {
	if key != uniqueConstraintsMeta {
		return m.inner.MetaSet(key, v)
	}
	close(m.entered)
	<-m.proceed
	return m.failErr
}

// TestCreateUnique_PersistFailureNeverExposesActiveConstraintToConcurrentReader
// is the proof: a concurrent reader taking uniqueMu must NEVER
// observe st.active=true for a constraint whose Phase-3 persist ultimately
// fails. Before the fix, createUnique released uniqueMu right after setting
// st.active=true and attempting the persist, then re-acquired it in
// uninstallPendingConstraint — leaving a window where a concurrent writer
// could see (and be wrongly, transiently rejected by) a constraint that was
// never actually durable. The fix keeps uninstall inside the SAME critical
// section as the failed activation, so a concurrent uniqueMu.Lock() cannot
// even proceed until the whole activate-persist-rollback sequence is done.
//
// This test blocks the persisting MetaSet call and checks with TryLock that
// uniqueMu is held while the write is in flight (the critical section spans
// the write), starts a racer that waits for uniqueMu, then lets the write fail
// and confirms the racer's eventual read sees no active constraint.
//
// The old code released uniqueMu between the failed write and the uninstall,
// a window too narrow to hit by scheduling alone; the TryLock probe pins the
// structural property instead: the lock is held across activate, persist and
// uninstall, so no reader can observe the intermediate state.
func TestCreateUnique_PersistFailureNeverExposesActiveConstraintToConcurrentReader(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	failMK := &blockingFailUniqueMetaKV{
		inner:   g.metaKV,
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
		failErr: errors.New("meta write boom"),
	}
	g.metaKV = failMK

	ctx := context.Background()
	createErrCh := make(chan error, 1)
	go func() {
		createErrCh <- g.Constraints.CreateUnique(ctx, "Person", "email")
	}()

	select {
	case <-failMK.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the persisting MetaSet call to be entered")
	}

	// The persisting MetaSet call is now blocked mid-flight, inside
	// createUnique's uniqueMu critical section. A racer trying to acquire
	// uniqueMu must NOT be able to proceed yet.
	labelTok, ok := g.labels.Lookup("Person")
	if !ok {
		t.Fatal("label Person not yet registered — test setup assumption violated")
	}
	if g.uniqueMu.TryLock() {
		g.uniqueMu.Unlock()
		close(failMK.proceed)
		<-createErrCh
		t.Fatal("uniqueMu is free WHILE the persist is in flight — the critical section does not span the failed write, regression")
	}
	if g.uniqueMu.TryRLock() {
		g.uniqueMu.RUnlock()
		close(failMK.proceed)
		<-createErrCh
		t.Fatal("uniqueMu admits a reader WHILE the persist is in flight, regression")
	}
	racerDone := make(chan bool, 1)
	go func() {
		g.uniqueMu.Lock()
		st, ok := g.lookupConstraintLocked(labelTok, "email")
		active := ok && st.active
		g.uniqueMu.Unlock()
		racerDone <- active
	}()

	close(failMK.proceed) // let the blocked MetaSet return failErr

	createErr := <-createErrCh
	if createErr == nil {
		t.Fatal("CreateUnique with a failing MetaSet = nil, want the persist error")
	}

	select {
	case active := <-racerDone:
		if active {
			t.Fatal("racer observed st.active=true for a constraint whose persist failed (regression)")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the racer to acquire uniqueMu after the failed create finished")
	}

	// The constraint must be fully gone — not just inactive — mirroring
	// uninstallPendingConstraint's contract.
	if _, ok := g.lookupConstraintLocked(labelTok, "email"); ok {
		t.Fatal("constraint entry still present after a failed persist; want fully removed")
	}
}
