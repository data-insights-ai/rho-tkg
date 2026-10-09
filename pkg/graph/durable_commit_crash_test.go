package graph_test

import (
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Crash harness for the durable-on-return commit (Config.DurableCommit, backlog
// item 11). A child process opens a disk store whose background flush is an hour
// away, writes ONE commit group through one door, and exits without Close the
// moment the door returns (os.Exit: a process crash — the pending write buffer
// is lost, whatever reached Badger's write-ahead log survives). The parent
// reopens the directory and counts what is on disk.
//
// This file compiles against the code before DurableCommit existed: the flag is
// set only through durableCommitHook, which durable_commit_test.go installs.

// durableCommitHook turns Config.DurableCommit on; nil before the feature.
var durableCommitHook func(*graphpkg.Config)

const (
	dcEnvMode    = "TKG_DC_CHILD"
	dcEnvDir     = "TKG_DC_DIR"
	dcEnvBackend = "TKG_DC_BACKEND"
	dcEnvDoor    = "TKG_DC_DOOR"
	dcEnvDurable = "TKG_DC_DURABLE"

	// dcSignals is N: the group is one "Ref" node, N "Signal" nodes, one LINK
	// relationship Ref->Signal[0], and a "Cut" node written LAST (the consumer's
	// cut record). On reopen a durable group shows N+2 nodes and 1 relationship.
	dcSignals = 20
)

// dcOpenStore opens the backend over dir. flushInterval is the background
// flush period (an hour in the child, so only an explicit flush can persist).
func dcOpenStore(backend, dir string, flushInterval time.Duration) (graphpkg.Config, error) {
	switch backend {
	case "badger":
		bs, err := badger.New(badger.Config{Dir: dir, FlushInterval: flushInterval})
		if err != nil {
			return graphpkg.Config{}, err
		}
		return graphpkg.Config{Store: bs}, nil
	case "tiered":
		// "Ref" routes to the reference shard, "Signal"/"Cut" to the hot event
		// shard: one group touches two badger shards.
		ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Ref"}, FlushInterval: flushInterval})
		if err != nil {
			return graphpkg.Config{}, err
		}
		return graphpkg.Config{Store: ts}, nil
	case "sharded":
		// SnowflakeNodeID 0 mints nodes in slot 0 and relationships in slot 1:
		// one group touches two slots.
		st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2, FlushInterval: flushInterval})
		if err != nil {
			return graphpkg.Config{}, err
		}
		return graphpkg.Config{Store: st}, nil
	}
	return graphpkg.Config{}, fmt.Errorf("unknown backend %q", backend)
}

// dcWriteGroup writes the commit group through door and returns the door's error.
func dcWriteGroup(g *graphpkg.Graph, door string) error {
	switch door {
	case "tx":
		tx, err := g.Tx().Begin()
		if err != nil {
			return err
		}
		ref, err := tx.AddNode([]string{"Ref"}, map[string]any{"k": "ref"})
		if err != nil {
			return err
		}
		var first *types.Node
		for i := 0; i < dcSignals; i++ {
			n, err := tx.AddNode([]string{"Signal"}, map[string]any{"i": int64(i)})
			if err != nil {
				return err
			}
			if first == nil {
				first = n
			}
		}
		if _, err := tx.AddRelationship("LINK", ref, first, nil); err != nil {
			return err
		}
		if _, err := tx.AddNode([]string{"Cut"}, map[string]any{"pin": int64(1)}); err != nil {
			return err
		}
		return tx.Commit()
	case "run":
		return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			ref, err := tx.AddNode([]string{"Ref"}, map[string]any{"k": "ref"})
			if err != nil {
				return err
			}
			var first *types.Node
			for i := 0; i < dcSignals; i++ {
				n, err := tx.AddNode([]string{"Signal"}, map[string]any{"i": int64(i)})
				if err != nil {
					return err
				}
				if first == nil {
					first = n
				}
			}
			if _, err := tx.AddRelationship("LINK", ref, first, nil); err != nil {
				return err
			}
			_, err = tx.AddNode([]string{"Cut"}, map[string]any{"pin": int64(1)})
			return err
		})
	case "batch":
		_, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
			ref, err := bb.AddNode([]string{"Ref"}, map[string]any{"k": "ref"})
			if err != nil {
				return err
			}
			var first *types.Node
			for i := 0; i < dcSignals; i++ {
				n, err := bb.AddNode([]string{"Signal"}, map[string]any{"i": int64(i)})
				if err != nil {
					return err
				}
				if first == nil {
					first = n
				}
			}
			if _, err := bb.AddRelationship("LINK", ref, first, nil); err != nil {
				return err
			}
			_, err = bb.AddNode([]string{"Cut"}, map[string]any{"pin": int64(1)})
			return err
		})
		return err
	case "ingest":
		s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true,
			DeclareLabels: []string{"Ref", "Signal", "Cut"}, DeclareRelTypes: []string{"LINK"}})
		if err != nil {
			return err
		}
		ref, err := s.AddNode([]string{"Ref"}, map[string]any{"k": "ref"})
		if err != nil {
			return err
		}
		var first *types.Node
		for i := 0; i < dcSignals; i++ {
			n, err := s.AddNode([]string{"Signal"}, map[string]any{"i": int64(i)})
			if err != nil {
				return err
			}
			if first == nil {
				first = n
			}
		}
		if _, err := s.AddRelationship("LINK", ref, first, nil); err != nil {
			return err
		}
		if _, err := s.AddNode([]string{"Cut"}, map[string]any{"pin": int64(1)}); err != nil {
			return err
		}
		_, err = s.Submit()
		return err
	}
	return fmt.Errorf("unknown door %q", door)
}

// TestDurableCommitCrashChild is the child body; it is a no-op unless the
// parent set dcEnvMode.
func TestDurableCommitCrashChild(t *testing.T) {
	if os.Getenv(dcEnvMode) != "1" {
		t.Skip("crash child: run only by the durable-commit parent tests")
	}
	cfg, err := dcOpenStore(os.Getenv(dcEnvBackend), os.Getenv(dcEnvDir), time.Hour)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	if os.Getenv(dcEnvDurable) == "1" {
		if durableCommitHook == nil {
			fmt.Fprintln(os.Stderr, "DurableCommit requested but not available")
			os.Exit(6)
		}
		durableCommitHook(&cfg)
	}
	g, err := graphpkg.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "graph.New:", err)
		os.Exit(3)
	}
	if err := dcWriteGroup(g, os.Getenv(dcEnvDoor)); err != nil {
		fmt.Fprintln(os.Stderr, "door:", err)
		os.Exit(4)
	}
	os.Exit(0) // crash: no Close, no flush — only what the door made durable survives
}

// dcCrashCounts runs the child for (backend, door, durable) on a fresh
// directory, reopens it, and returns the node and relationship counts on disk.
func dcCrashCounts(t *testing.T, backend, door string, durable bool) (nodes, rels int) {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurableCommitCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), dcEnvMode+"=1", dcEnvDir+"="+dir, dcEnvBackend+"="+backend,
		dcEnvDoor+"="+door, dcEnvDurable+"="+boolDigit(durable))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child %s/%s durable=%v failed: %v\n%s", backend, door, durable, err, out)
	}
	cfg, err := dcOpenStore(backend, dir, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("reopen %s after crash: %v", backend, err)
	}
	g, err := graphpkg.New(cfg)
	if err != nil {
		t.Fatalf("graph.New after crash: %v", err)
	}
	defer func() { _ = g.Close() }()
	nodes, err = g.Nodes().Count()
	if err != nil {
		t.Fatalf("Nodes().Count after crash: %v", err)
	}
	rels, err = g.Rels().Count()
	if err != nil {
		t.Fatalf("Rels().Count after crash: %v", err)
	}
	return nodes, rels
}

func boolDigit(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// flushSpyStore wraps a badger store and counts the store-level flush calls
// the graph makes. durable_commit_test.go adds the DurableFlush counter.
type flushSpyStore struct {
	*badger.Store
	flushes        atomic.Int64
	durableFlushes atomic.Int64
	failDurable    atomic.Int64 // DurableFlush calls still to fail (durable_commit_test.go)
}

func (s *flushSpyStore) Flush() error {
	s.flushes.Add(1)
	return s.Store.Flush()
}

// R0 — default off, before and after the feature.
//
// Catches: an implementation that flushes on GraphTx.Commit / Tx().Run /
// Batch.Execute although DurableCommit is off (the default must stay
// byte-identical: no store flush, a crash before the next background flush loses
// the group). The strong ingest applier is the one door that already flushed
// before this feature (store.GroupCommitCapability.EndGroupCommit: one
// WriteBatch, no fsync), so its group survives a process crash with the flag
// off, and must keep doing so.
func TestDurableCommitOff_CrashBeforeBackgroundFlushLosesGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns crash children")
	}
	for _, door := range []string{"tx", "run", "batch"} {
		t.Run(door, func(t *testing.T) {
			nodes, rels := dcCrashCounts(t, "badger", door, false)
			if nodes != 0 || rels != 0 {
				t.Fatalf("flag off: %d nodes, %d rels on disk after the crash, want 0/0 — the door flushed although DurableCommit is off", nodes, rels)
			}
		})
	}
	t.Run("ingest-group-commit", func(t *testing.T) {
		nodes, rels := dcCrashCounts(t, "badger", "ingest", false)
		if nodes != dcSignals+2 || rels != 1 {
			t.Fatalf("flag off: ingest group commit left %d nodes, %d rels after the crash, want %d/1 (EndGroupCommit flushes before the ack)", nodes, rels, dcSignals+2)
		}
	})
}

// Catches: a default-off path that calls the store's Flush from Commit,
// Execute, Run or the ingest applier.
func TestDurableCommitOff_NoStoreFlushOnCommit(t *testing.T) {
	bs, err := badger.New(badger.Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	spy := &flushSpyStore{Store: bs}
	g, err := graphpkg.New(graphpkg.Config{Store: spy})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()
	for _, door := range []string{"tx", "run", "batch"} {
		if err := dcWriteGroup(g, door); err != nil {
			t.Fatalf("%s: %v", door, err)
		}
	}
	if bs.PendingWriteCount() == 0 {
		t.Fatal("flag off: pending buffer empty after tx/run/batch — something flushed")
	}
	// The ingest applier drains the buffer through EndGroupCommit (not Flush).
	if err := dcWriteGroup(g, "ingest"); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if n := spy.flushes.Load(); n != 0 {
		t.Fatalf("flag off: %d store Flush calls from the commit doors, want 0", n)
	}
}
