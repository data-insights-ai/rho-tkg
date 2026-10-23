package graph_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Crash reopen of the commit clock (backlog 44, lesson 71). A child process
// raises the commit-clock floor an hour past the wall (AdvanceClock, the HLC
// seam; a write burst above one per millisecond does the same in production),
// writes durable groups and a delete (so the newest stamp lives only in a
// history tombstone), prints the newest stamp of every row it wrote, and exits
// without Close. The reopened graph must hand out NowTx() and fresh TxFrom
// stamps above every stored stamp: before the fix the floor watermark was
// persisted only at Close, so a crash reopened with NowTx() at the wall, an
// hour below rows already on disk.

const (
	cfEnvMode     = "TKG_CF_CHILD"
	cfEnvDir      = "TKG_CF_DIR"
	cfEnvBackend  = "TKG_CF_BACKEND"
	cfEnvScenario = "TKG_CF_SCENARIO"
	cfEnvTarget   = "TKG_CF_TARGET"
	cfEnvRelLast  = "TKG_CF_REL_LAST"
)

// TestClockFloorCrashChild is the child body; a no-op unless the parent set
// cfEnvMode.
func TestClockFloorCrashChild(t *testing.T) {
	t.Parallel()
	if os.Getenv(cfEnvMode) != "1" {
		t.Skip("crash child: run only by TestCrashReopenNowTxAboveStoredStamps")
	}
	target, err := strconv.ParseInt(os.Getenv(cfEnvTarget), 10, 64)
	if err != nil {
		cfDie("target", err)
	}
	dir, backend, scenario := os.Getenv(cfEnvDir), os.Getenv(cfEnvBackend), os.Getenv(cfEnvScenario)
	if scenario == "replica" {
		cfReplicaChild(dir, types.Instant(target))
		return
	}
	if scenario == "trusted-crash" {
		// A clean first session, so the crashing session opens from a trusted
		// open image (backlog 54) instead of the full rebuild.
		g := cfOpen(dir, backend)
		if _, err := g.Nodes().Add(t.Context(), []string{"Seed"}, nil); err != nil {
			cfDie("seed", err)
		}
		if err := g.Close(); err != nil {
			cfDie("seed close", err)
		}
	}
	g := cfOpen(dir, backend)
	if _, err := g.Temporal().AdvanceClock(types.Instant(target)); err != nil {
		cfDie("AdvanceClock", err)
	}
	nodes, rels := cfWrite(g, os.Getenv(cfEnvRelLast) == "1")
	cfReport(g, nodes, rels)
	if scenario == "clean" {
		if err := g.Close(); err != nil {
			cfDie("close", err)
		}
	}
	os.Exit(0) // crash: no Close; only what the durable groups made durable survives
}

func cfDie(what string, err error) {
	fmt.Fprintln(os.Stderr, what+":", err)
	os.Exit(3)
}

// cfOpen opens the backend over dir with DurableCommit (each group durable on
// return) and a background flush an hour away.
func cfOpen(dir, backend string) *graphpkg.Graph {
	cfg, err := dcOpenStore(backend, dir, time.Hour)
	if err != nil {
		cfDie("open", err)
	}
	cfg.DurableCommit = true
	g, err := graphpkg.New(cfg)
	if err != nil {
		cfDie("graph.New", err)
	}
	return g
}

// cfWrite writes one group (dcWriteGroup's shape) and then deletes a node in a
// second group (relLast: then the relationship in a third), so the newest stamp
// is a tombstone's, held only in history.
func cfWrite(g *graphpkg.Graph, relLast bool) (nodes []types.NodeID, rels []types.RelID) {
	err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		ref, err := tx.AddNode([]string{"Ref"}, map[string]any{"k": "ref"})
		if err != nil {
			return err
		}
		nodes = append(nodes, ref.ID())
		var first *types.Node
		for i := 0; i < 5; i++ {
			n, err := tx.AddNode([]string{"Signal"}, map[string]any{"i": int64(i)})
			if err != nil {
				return err
			}
			if first == nil {
				first = n
			}
			nodes = append(nodes, n.ID())
		}
		r, err := tx.AddRelationship("LINK", ref, first, nil)
		if err != nil {
			return err
		}
		rels = append(rels, r.ID())
		return nil
	})
	if err != nil {
		cfDie("group", err)
	}
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.DeleteNode(nodes[len(nodes)-1]) }); err != nil {
		cfDie("delete", err)
	}
	if relLast { // the newest stamp on the relationship's shard (sharded: slot 1, not the anchor)
		if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.DeleteRelationship(rels[0]) }); err != nil {
			cfDie("delete rel", err)
		}
	}
	return nodes, rels
}

// cfMaxStamp folds LatestStamps (current and history rows) over the IDs.
func cfMaxStamp(g *graphpkg.Graph, nodes []types.NodeID, rels []types.RelID) (types.Instant, error) {
	var m types.Instant
	for _, id := range nodes {
		from, to, _, err := g.Nodes().LatestStamps(id)
		if err != nil {
			return 0, fmt.Errorf("node %d: %w", id, err)
		}
		m = max(m, from, to)
	}
	for _, id := range rels {
		from, to, _, err := g.Rels().LatestStamps(id)
		if err != nil {
			return 0, fmt.Errorf("rel %d: %w", id, err)
		}
		m = max(m, from, to)
	}
	return m, nil
}

// cfReport prints "CF <max> n:<ids> r:<ids>" for the parent.
func cfReport(g *graphpkg.Graph, nodes []types.NodeID, rels []types.RelID) {
	m, err := cfMaxStamp(g, nodes, rels)
	if err != nil {
		cfDie("max stamp", err)
	}
	ns := make([]string, len(nodes))
	for i, id := range nodes {
		ns[i] = strconv.FormatInt(int64(id), 10)
	}
	rs := make([]string, len(rels))
	for i, id := range rels {
		rs[i] = strconv.FormatInt(int64(id), 10)
	}
	fmt.Printf("\nCF %d n:%s r:%s\n", m, strings.Join(ns, ","), strings.Join(rs, ","))
}

// cfReplicaChild: an in-memory primary writes ahead of the wall; a disk replica
// applies its change records (ApplyChanges flushes the data before the applied
// watermark) and the process exits without Close.
func cfReplicaChild(dir string, target types.Instant) {
	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true})
	if err != nil {
		cfDie("primary", err)
	}
	// Seed every label and type before the bootstrap snapshot, so the applied
	// records resolve against the replica's registry.
	if err := primary.Tx().Run(func(tx *graphpkg.GraphTx) error {
		a, err := tx.AddNode([]string{"Ref"}, nil)
		if err != nil {
			return err
		}
		b, err := tx.AddNode([]string{"Signal"}, nil)
		if err != nil {
			return err
		}
		_, err = tx.AddRelationship("LINK", a, b, nil)
		return err
	}); err != nil {
		cfDie("seed", err)
	}
	bs, err := badger.New(badger.Config{Dir: dir, FlushInterval: time.Hour})
	if err != nil {
		cfDie("replica store", err)
	}
	replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true, Store: bs})
	if err != nil {
		cfDie("replica", err)
	}
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		cfDie("export", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		cfDie("import", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		cfDie("lsn", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		cfDie("set lsn", err)
	}
	if _, err := primary.Temporal().AdvanceClock(target); err != nil {
		cfDie("AdvanceClock", err)
	}
	nodes, rels := cfWrite(primary, false)
	recs, err := primary.Replication().ChangeFeed(lsn0, 0)
	if err != nil {
		cfDie("feed", err)
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		cfDie("apply", err)
	}
	cfReport(replica, nodes, rels)
	os.Exit(0)
}

// cfParse reads the child's report line.
func cfParse(t *testing.T, out []byte) (types.Instant, []types.NodeID, []types.RelID) {
	t.Helper()
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, "CF ") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 4 {
			break
		}
		m, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			break
		}
		var nodes []types.NodeID
		var rels []types.RelID
		for _, s := range strings.Split(strings.TrimPrefix(f[2], "n:"), ",") {
			v, _ := strconv.ParseInt(s, 10, 64)
			nodes = append(nodes, types.NodeID(v))
		}
		for _, s := range strings.Split(strings.TrimPrefix(f[3], "r:"), ",") {
			v, _ := strconv.ParseInt(s, 10, 64)
			rels = append(rels, types.RelID(v))
		}
		return types.Instant(m), nodes, rels
	}
	t.Fatalf("child printed no report line:\n%s", out)
	return 0, nil, nil
}

func TestCrashReopenNowTxAboveStoredStamps(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns crash children")
	}
	t.Parallel()
	cases := []struct {
		name, backend, scenario string
		readOnly                bool // reopen the badger store ReadOnly (no fresh write)
		relLast                 bool // the newest stamp is a relationship tombstone
	}{
		{"badger/clean", "badger", "clean", false, false}, // unchanged: Close persisted the floor
		{"badger/crash", "badger", "crash", false, false},
		{"badger/trusted-image-then-crash", "badger", "trusted-crash", false, false},
		{"badger/crash/read-only-reopen", "badger", "crash", true, false},
		{"badger-sync/crash", "badger-sync", "crash", false, true},
		{"tiered/crash/event-shard-newest", "tiered", "crash", false, false},
		{"tiered/crash/rel-newest", "tiered", "crash", false, true},
		{"sharded/crash/anchor-newest", "sharded", "crash", false, false},
		{"sharded/crash/slot1-newest", "sharded", "crash", false, true},
		{"replica/crash", "badger", "replica", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			target := time.Now().Add(time.Hour).UnixMilli()
			cmd := exec.Command(os.Args[0], "-test.run=^TestClockFloorCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), cfEnvMode+"=1", cfEnvDir+"="+dir, cfEnvBackend+"="+tc.backend,
				cfEnvScenario+"="+tc.scenario, cfEnvTarget+"="+strconv.FormatInt(target, 10), cfEnvRelLast+"="+boolDigit(tc.relLast))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			childMax, nodes, rels := cfParse(t, out)
			if int64(childMax) <= target {
				t.Fatalf("precondition: child's newest stamp %d not above the advanced floor %d", childMax, target)
			}

			var g *graphpkg.Graph
			switch {
			case tc.readOnly:
				// Badger cannot open a crashed directory read-only (its log
				// needs replay), so a bare store open replays it and closes —
				// the store's own Close writes the open image, no graph Close
				// persists a floor — and the graph then opens it read-only from
				// that trusted image.
				rw, err := badger.New(badger.Config{Dir: dir})
				if err != nil {
					t.Fatalf("replay open: %v", err)
				}
				if err := rw.Close(); err != nil {
					t.Fatalf("replay close: %v", err)
				}
				bs, err := badger.New(badger.Config{Dir: dir, ReadOnly: true})
				if err != nil {
					t.Fatalf("read-only reopen: %v", err)
				}
				g, err = graphpkg.New(graphpkg.Config{Store: bs})
				if err != nil {
					t.Fatalf("graph.New read-only: %v", err)
				}
			case tc.scenario == "replica":
				bs, err := badger.New(badger.Config{Dir: dir})
				if err != nil {
					t.Fatalf("replica reopen: %v", err)
				}
				g, err = graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true, Store: bs})
				if err != nil {
					t.Fatalf("graph.New replica: %v", err)
				}
			default:
				cfg, err := dcOpenStore(tc.backend, dir, 100*time.Millisecond)
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				g, err = graphpkg.New(cfg)
				if err != nil {
					t.Fatalf("graph.New: %v", err)
				}
			}
			t.Cleanup(func() { _ = g.Close() })

			// The rows survived the crash: the reopened store holds the same stamps.
			stored, err := cfMaxStamp(g, nodes, rels)
			if err != nil {
				t.Fatalf("stored stamps after reopen: %v", err)
			}
			if stored != childMax {
				t.Fatalf("newest stored stamp after reopen = %d, child saw %d (rows lost or changed)", stored, childMax)
			}
			cfAssertAbove(t, g, stored, !tc.readOnly && tc.scenario != "replica")
		})
	}

	// Memory with a SegmentDir: the sealed relationship rows are the one part
	// of a memory store that outlives the process, and its MetaKV does not, so
	// even a clean Close leaves no floor watermark behind.
	t.Run("memory/segment-dir-reopen", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "segments")
		cfg := func() graphpkg.Config {
			return graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{segOracleSpec}, SegmentDir: dir}
		}
		g, err := graphpkg.New(cfg())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Temporal().AdvanceClock(types.Instant(time.Now().Add(time.Hour).UnixMilli())); err != nil {
			t.Fatal(err)
		}
		r := rand.New(rand.NewSource(7))
		a, err := g.Nodes().Add(t.Context(), []string{"Asset"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes().Add(t.Context(), []string{"Asset"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var rels []types.RelID
		for i := 0; i < 20; i++ {
			rel, err := g.Rels().Add(t.Context(), segOracleHOP, a, b, segHOPProps(r))
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, rel.ID())
		}
		if err := g.Admin().SealRelSegments(segOracleHOP); err != nil {
			t.Fatal(err)
		}
		sealed, err := cfMaxStamp(g, nil, rels)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g2, err := graphpkg.New(cfg())
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer g2.Close()
		stored, err := cfMaxStamp(g2, nil, rels)
		if err != nil || stored != sealed {
			t.Fatalf("sealed stamps after reopen = %d, %v; before %d", stored, err, sealed)
		}
		cfAssertAbove(t, g2, stored, true)
	})
}

// cfAssertAbove asserts NowTx() and, when the graph can write, a fresh
// write's TxFrom exceed stored.
func cfAssertAbove(t *testing.T, g *graphpkg.Graph, stored types.Instant, canWrite bool) {
	t.Helper()
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	if pin <= stored {
		t.Fatalf("NowTx() after reopen = %d <= newest stored stamp %d (%d ms below): a pin at NowTx misses committed rows",
			pin, stored, int64(stored-pin))
	}
	if !canWrite {
		return
	}
	n, err := g.Nodes().Add(t.Context(), []string{"After"}, nil)
	if err != nil {
		t.Fatalf("fresh write: %v", err)
	}
	if tf := n.Temporal().TxFrom; tf <= stored {
		t.Fatalf("fresh write TxFrom %d <= newest stored stamp %d: transaction time went backwards", tf, stored)
	}
}
