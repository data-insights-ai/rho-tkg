package graph_test

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// TestWriteAheadRegistryCrash pins the flush-time write-ahead of the
// property-key registry, in isolation from row durability.
//
// The invariant: a property-key token never becomes durable later than a row
// that references it. Rows are buffered and flushed to the event shard's
// Badger DB; the registry lives in a different Badger DB (the reference shard).
// A new key under an existing label allocates no label, so the Add path saves
// no registry; the only save before Close is the flush hook
// (tiered OnPropertyKeyGrow, badger persistRegistryIfGrew), which commits the
// registry to the reference shard ahead of the row WriteBatch.
//
// The child adds an Event node carrying a brand-new key, runs one flush by hand
// (the background flush interval is an hour, so no timer races it), then exits
// without Close, so the Close-time registry save never runs. On reopen the new
// key must be in the persisted registry: without the flush hook it is absent.
func TestWriteAheadRegistryCrash(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ctx := context.Background()

	if os.Getenv("TKG_CRASH_CHILD") == "1" {
		dir := os.Getenv("TKG_CRASH_DIR")
		ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Ref"}, FlushInterval: time.Hour})
		if err != nil {
			os.Exit(2)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			os.Exit(3)
		}
		// Existing label "Event", brand-new property key: the only thing that can
		// persist this token before Close is the write-ahead step in flush().
		if _, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"probe_key_new": "v"}); err != nil {
			os.Exit(4)
		}
		if err := ts.Flush(); err != nil {
			os.Exit(5)
		}
		os.Exit(0) // crash: no Close, so the Close-time registry save never runs
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
