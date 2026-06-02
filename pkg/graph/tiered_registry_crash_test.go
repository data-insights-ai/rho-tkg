package graph_test

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
	tieredpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/tiered"
)

// TestWriteAheadRegistryCrash verifies the write-ahead durability of the
// property-key registry in ISOLATION from row durability.
//
// The invariant we must guarantee: a property-key token can never become
// durable LATER than a row that references it. Rows are buffered (bs.pending)
// and flushed asynchronously to the event-shard Badger DB under
// SyncWrites=false; the registry lives in a DIFFERENT Badger DB (refShard).
// With only db.Update (buffered) the two DBs fsync independently, so a crash
// can leave a tokenized row durable while its token is still in an unsynced
// refShard buffer — exactly the production "node counter does not match N live
// rows" fatal on reload. The fix fsyncs the registry inside SavePropertyKey-
// Registry, which runs synchronously in the row's Add() path BEFORE the row is
// ever handed to the flush buffer. So the registry token is on stable storage
// strictly before the row can be.
//
// This test exercises precisely that ordering: the child adds an Event node
// carrying a BRAND-NEW property key (existing label → only the property-key
// write-ahead path can persist the token), lets a flush cycle run (which
// persists+fsyncs the registry to the refShard BEFORE the row's unsynced
// WriteBatch), then crashes (os.Exit, no Close). The row's batch is not fsynced
// and is lost, but the registry must already be durable. On reopen the new key
// must be present.
//
// Before the fix: flush() does not touch the registry, so the token only reaches
// disk at Close — os.Exit drops it, the key is absent. After the fix: flush()
// write-aheads + fsyncs the registry, so the token survives.
func TestWriteAheadRegistryCrash(t *testing.T) {
	ctx := context.Background()

	if os.Getenv("TKG_CRASH_CHILD") == "1" {
		dir := os.Getenv("TKG_CRASH_DIR")
		ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Ref"}, FlushInterval: 5 * time.Millisecond})
		if err != nil {
			os.Exit(2)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			os.Exit(3)
		}
		// Existing label "Event", BRAND-NEW property key → the only thing that can
		// persist this token is the property-key write-ahead path in flush().
		if _, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"probe_key_new": "v"}); err != nil {
			os.Exit(4)
		}
		time.Sleep(150 * time.Millisecond) // let a flush cycle write-ahead + fsync the registry
		os.Exit(0)                         // crash: no Close; the row batch is unsynced and lost, registry must survive
	}

	dir := t.TempDir()

	// Setup (clean): establish the "Event" label + a baseline key, then Close so
	// the child's new key cannot ride along on a new-label persist and so the
	// refShard already exists with a durable baseline registry.
	func() {
		ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Ref"}})
		if err != nil {
			t.Fatalf("setup tiered.New: %v", err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			t.Fatalf("setup graph.New: %v", err)
		}
		if _, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"base_key": "v"}); err != nil {
			t.Fatalf("setup add: %v", err)
		}
		if err := g.Close(); err != nil {
			t.Fatalf("setup close: %v", err)
		}
	}()

	// Child: add an Event node with a NEW key under the existing label, then crash.
	cmd := exec.Command(os.Args[0], "-test.run=^TestWriteAheadRegistryCrash$", "-test.v")
	cmd.Env = append(os.Environ(), "TKG_CRASH_CHILD=1", "TKG_CRASH_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child failed: %v\n%s", err, out)
	}

	// Reopen — must NOT crash-loop on the counter reconcile, and the new token
	// must have been write-ahead durable on the refShard.
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Ref"}})
	if err != nil {
		t.Fatalf("reopen after crash failed (counter reconcile fatal — token was not write-ahead durable): %v", err)
	}
	defer func() { _ = ts.Close() }()

	reg := registrypkg.NewPropertyKeyRegistry()
	found, err := ts.LoadPropertyKeyRegistry(reg)
	if err != nil {
		t.Fatalf("LoadPropertyKeyRegistry after crash: %v", err)
	}
	if !found {
		t.Fatal("no property-key registry persisted at all after crash")
	}
	if _, ok := reg.Lookup("base_key"); !ok {
		t.Error("baseline key base_key missing from registry after crash (setup persist lost)")
	}
	if _, ok := reg.Lookup("probe_key_new"); !ok {
		t.Fatal("probe_key_new missing from registry after crash: the write-ahead fsync did not persist the new token before the crash (a durable row referencing it would be undecodable → counter mismatch)")
	}
}
