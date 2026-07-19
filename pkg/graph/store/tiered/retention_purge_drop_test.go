package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func newPersistentTieredStore(t *testing.T) *Store {
	t.Helper()
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	// Install the label registry (Case=1, User=2, Signal=3).
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	for _, l := range []string{"Case", "User", "Signal"} {
		if _, err := reg.GetOrCreate(l); err != nil {
			t.Fatalf("registry: %v", err)
		}
	}
	return ts
}

// TestTieredColdShardFastDrop is the correctness gate for the whole-shard fast-drop
// (ADR-0008 R4 optimization): a rotated, wholly-aged-out, single-label event shard is
// physically DROPPED (catalog entry + directory removed) rather than row-scanned, and
// its cross-shard edge residue is swept on the surviving reference shard — no dangling
// phantom.
func TestTieredColdShardFastDrop(t *testing.T) {
	ts := newPersistentTieredStore(t)
	const caseTok, signalTok, relType = uint16(1), uint16(3), uint16(5)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	ref := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil) // → reference shard (survivor)
	e1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	e2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{ref, e1, e2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put node: %v", err)
		}
	}
	// Cross-shard edges both directions between ref (survivor) and e1 (dropped shard).
	caseToSig := types.NewRelationship(types.RelID(relGen.Generate()), relType, ref.ID(), e1.ID()) // entity+out on ref shard (residue)
	sigToCase := types.NewRelationship(types.RelID(relGen.Generate()), relType, e1.ID(), ref.ID()) // in-leg orphan on ref shard
	for _, r := range []*types.Relationship{caseToSig, sigToCase} {
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("put rel: %v", err)
		}
	}

	// Rotate: the event shard holding e1/e2 becomes non-hot with a finite window.
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	eventShardsBefore := len(ts.catalog.EventShards())

	// Purge every Signal older than the far future → the old event shard is DROPPED.
	before := types.Instant(1 << 50)
	res, err := ts.PurgeNodesByLabelBefore(signalTok, before, 8)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if res.NodesPurged != 2 {
		t.Fatalf("purged %d signal nodes, want 2", res.NodesPurged)
	}

	// A whole event shard was physically dropped from the catalog.
	if got := len(ts.catalog.EventShards()); got != eventShardsBefore-1 {
		t.Fatalf("event shards %d after purge, want %d (a shard should be dropped)", got, eventShardsBefore-1)
	}

	// Signals gone; reference survives.
	if _, err := ts.GetNode(e1.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("e1 survived the drop: %v", err)
	}
	if _, err := ts.GetNode(e2.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("e2 survived the drop: %v", err)
	}
	if _, err := ts.GetNode(ref.ID()); err != nil {
		t.Fatalf("reference wrongly removed: %v", err)
	}
	// Both cross-shard edges swept — no dangling residue on the ref shard.
	if _, err := ts.GetRelationship(caseToSig.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("case->signal residue survived on ref shard: %v", err)
	}
	if _, err := ts.GetRelationship(sigToCase.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("signal->case orphan in-leg survived on ref shard: %v", err)
	}
	if out, _ := ts.OutgoingRelationships(ref.ID(), 0); len(out) != 0 {
		t.Fatalf("ref outgoing = %d after drop, want 0 (no dangling ref->e1)", len(out))
	}
	if in, _ := ts.IncomingRelationships(ref.ID(), 0); len(in) != 0 {
		t.Fatalf("ref incoming = %d after drop, want 0 (no orphan e1->ref in-leg)", len(in))
	}
}

// TestTieredColdShardFastDrop_ChangeLogDeclines proves the fast-drop is DISABLED under
// change-log (a drop would destroy the shard's log segment) — the shard is row-scanned
// instead, so its catalog entry remains (the rows are removed, the shard is not).
func TestTieredColdShardFastDrop_ChangeLogDeclines(t *testing.T) {
	ts, err := New(Config{
		DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, ChangeLog: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	_ = caseTok

	gen := tieredNodeGen(t)
	for i := 0; i < 3; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	shardsBefore := len(ts.catalog.EventShards())

	drop, derr := ts.fastDropEligibleShards(signalTok, types.Instant(1<<50))
	if derr != nil {
		t.Fatalf("fastDropEligibleShards: %v", derr)
	}
	if drop.NodesPurged != 0 {
		t.Fatalf("fast-drop under change-log dropped %d nodes, want 0 (declined)", drop.NodesPurged)
	}
	if len(ts.catalog.EventShards()) != shardsBefore {
		t.Fatal("a shard was dropped under change-log, want none")
	}
}

// TestTieredColdShardFastDrop_ConcurrentReads stresses the drain protocol: readers
// hammer the store while a purge drops a shard. No panic, no race, and the drop still
// removes the shard cleanly.
func TestTieredColdShardFastDrop_ConcurrentReads(t *testing.T) {
	ts := newPersistentTieredStore(t)
	const signalTok = uint16(3)
	gen := tieredNodeGen(t)

	var ids []types.NodeID
	for i := 0; i < 50; i++ {
		id := types.NodeID(gen.Generate())
		if err := ts.PutNode(types.NewNode(id, signalTok, nil)); err != nil {
			t.Fatalf("put: %v", err)
		}
		ids = append(ids, id)
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for _, id := range ids {
						_, _ = ts.GetNode(id) // tolerate not-found as the drop proceeds
					}
				}
			}
		}()
	}

	if _, err := ts.PurgeNodesByLabelBefore(signalTok, types.Instant(1<<50), 16); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("purge under concurrent reads: %v", err)
	}
	close(stop)
	wg.Wait()

	for _, id := range ids {
		if _, err := ts.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("signal %d survived the concurrent drop: %v", id.SnowflakeID(), err)
		}
	}
	_ = storecontract.ErrNodeNotFound
}

// TestTieredColdShardFastDrop_CloseDoesNotRaceEventShardsMap guards BACKLOG
// 19a: Close() must not run concurrently with the cold-shard-drop drain
// protocol's mutations of ts.eventShards. Before the fix, Close() ranged the
// map under lifecycleMu only while dropOneShard mutated it under ts.mu only —
// two different locks guarding the same map from two different code paths,
// which the race detector (or, without -race, Go's own concurrent
// map-read/map-write fatal-error check) can catch even without precise
// timing, since no happens-before edge related the two accesses at all.
// Several event shards (multiple dropOneShard calls per purge) and repeated
// iterations widen the interleaving window and raise the chance any residual
// race is caught.
func TestTieredColdShardFastDrop_CloseDoesNotRaceEventShardsMap(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		ts, err := New(Config{
			DataDir:       t.TempDir(),
			RefLabels:     []string{"Case", "User"},
			ShardWindow:   7 * 24 * time.Hour,
			FlushInterval: 1<<63 - 1,
		})
		if err != nil {
			t.Fatalf("iter %d: New: %v", iter, err)
		}
		reg := registrypkg.NewLabelRegistry()
		ts.SetLabelRegistry(reg)
		signalTok, err := reg.GetOrCreate("Signal")
		if err != nil {
			t.Fatalf("iter %d: registry: %v", iter, err)
		}
		gen := tieredNodeGen(t)
		// Several rotated shards, so fastDropEligibleShards makes several
		// dropOneShard calls (each with its own I/O-bearing residue-collect
		// step) instead of one, widening the window Close() can land in.
		for shard := 0; shard < 5; shard++ {
			for i := 0; i < 10; i++ {
				if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)); err != nil {
					t.Fatalf("iter %d: put: %v", iter, err)
				}
			}
			if err := ts.ForceRotate(); err != nil {
				t.Fatalf("iter %d: ForceRotate: %v", iter, err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = ts.PurgeNodesByLabelBefore(signalTok, types.Instant(1<<50), 8)
		}()
		go func() {
			defer wg.Done()
			// A short head start lets the purge goroutine clear checkOpen()
			// and reach dropOneShard's I/O-bound residue collection before
			// Close() begins its own eventShards map traversal — widening the
			// window where the two unsynchronized map accesses can overlap.
			time.Sleep(50 * time.Microsecond)
			_ = ts.Close()
		}()
		wg.Wait()
	}
}

// TestTieredColdShardFastDrop_CatalogSaveFailureLeavesShardIntact guards
// BACKLOG 19b: dropOneShard must durably commit the catalog removal BEFORE
// deleting the shard directory, with an in-memory rollback (catalog +
// routing) on a failed Save — mirroring RotateHotShard's discipline. Before
// the fix, the directory was removed FIRST; a Save failure (or a crash in
// that window) left a catalog entry pointing at a directory that no longer
// existed, bricking the shard's reopen. This test forces catalog.Save() to
// fail (read-only meta dir) and asserts the shard's directory, catalog entry,
// and in-memory routing all survive intact — the drop simply fails and can be
// retried, instead of destroying data it couldn't durably record as gone.
func TestTieredColdShardFastDrop_CatalogSaveFailureLeavesShardIntact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod-driven write-failure injection is unreliable on Windows")
	}

	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, err := reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gen := tieredNodeGen(t)
	var ids []types.NodeID
	for i := 0; i < 10; i++ {
		id := types.NodeID(gen.Generate())
		if err := ts.PutNode(types.NewNode(id, signalTok, nil)); err != nil {
			t.Fatalf("put: %v", err)
		}
		ids = append(ids, id)
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	shardsBefore := ts.catalog.EventShards()
	if len(shardsBefore) == 0 {
		t.Fatal("no rotated event shard — test setup broken")
	}
	droppedName := shardsBefore[0].Name
	shardDir := filepath.Join(dir, shardsBefore[0].Path)

	// Make the catalog directory read-only so atomicWriteFile's CreateTemp
	// call inside catalog.Save() fails.
	metaDir := filepath.Join(dir, "meta")
	if err := os.Chmod(metaDir, 0o500); err != nil {
		t.Fatalf("chmod meta read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(metaDir, 0o700) })

	if _, err := ts.PurgeNodesByLabelBefore(signalTok, types.Instant(1<<50), 8); err == nil {
		t.Fatal("PurgeNodesByLabelBefore: nil error, want failure (catalog dir is read-only)")
	}

	// Restore write access so the assertions below (which read the store)
	// aren't themselves impeded, then verify nothing was destroyed.
	if err := os.Chmod(metaDir, 0o700); err != nil {
		t.Fatalf("chmod meta writable: %v", err)
	}

	if _, err := os.Stat(shardDir); err != nil {
		t.Fatalf("shard directory removed despite failed catalog commit: %v", err)
	}
	shardsAfter := ts.catalog.EventShards()
	found := false
	for _, e := range shardsAfter {
		if e.Name == droppedName {
			found = true
		}
	}
	if !found {
		t.Fatalf("catalog no longer lists shard %q after a failed drop — rollback did not restore it", droppedName)
	}
	ts.mu.RLock()
	_, routable := ts.eventShards[droppedName]
	ts.mu.RUnlock()
	if !routable {
		t.Fatalf("shard %q not re-linked into ts.eventShards after failed drop — data intact but unreachable", droppedName)
	}
	// The entities themselves must still be reachable through the surviving,
	// re-linked shard.
	for _, id := range ids {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("node %d unreachable after failed-but-rolled-back drop: %v", id.SnowflakeID(), err)
		}
	}
}
