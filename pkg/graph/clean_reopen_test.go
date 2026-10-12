package graph_test

// Backlog 54: a clean reopen of a badger store must not decode every row to
// rebuild the RAM indexes. TestCleanReopenCostNotLinear is the cost gate;
// TestCleanReopenMatchesFullRebuild is the correctness gate (every RAM
// structure after a reopen equals a full rebuild from the rows), across the
// break cases: crash without Close, a store without persisted state (older
// release), a read-only open, a write by another binary after Close, a
// corrupted image.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

type reopenCfg struct {
	readOnly, onDisk bool
}

func openReopenGraph(t *testing.T, dir string, rc reopenCfg) (*graphpkg.Graph, *badger.Store) {
	t.Helper()
	bs, err := badger.New(badger.Config{Dir: dir, ReadOnly: rc.readOnly,
		LabelIndexOnDisk: rc.onDisk, AdjacencyIndexOnDisk: rc.onDisk, PropertyIndexOnDisk: rc.onDisk, TemporalIndexOnDisk: rc.onDisk})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	return g, bs
}

// writeAisocRecords writes ai-soc's shape: per record two keyed endpoint
// upserts and one LOGON relationship with 15 properties, 177 records a commit.
func writeAisocRecords(t *testing.T, g *graphpkg.Graph, from, n, devs, accts int) {
	t.Helper()
	ctx := context.Background()
	for c := from; c < from+n; c += 177 {
		err := g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
			for i := c; i < min(c+177, from+n); i++ {
				dev, _, err := tx.GetOrCreateByKey("Device", "key", fmt.Sprintf("dev-%d", i%devs), map[string]any{"estate": "BA"})
				if err != nil {
					return err
				}
				acct, _, err := tx.GetOrCreateByKey("Account", "key", fmt.Sprintf("acct-%d", i%accts), map[string]any{"estate": "BA"})
				if err != nil {
					return err
				}
				vf := int64(1_790_700_000_000 + i*1000)
				_, err = tx.AddRelationshipByID("LOGON", acct.ID(), dev.ID(), map[string]any{"device": "d", "action": "LogonSuccess",
					"logon_type": "Network", "logon_id": "0x2", "account_sid": "S-X", "account": "x", "remote_ip": "10.2.0.2",
					"tkg_valid_from": vf, "tkg_valid_to": vf + 1, "estate": "BA", "group": fmt.Sprintf("g%d", c),
					"source": "DeviceLogonEvents", "record": fmt.Sprintf("r%d", i)})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func buildAisocStore(t *testing.T, dir string, entities int) {
	t.Helper()
	recs := entities * 1000 / 2190 // 436 K records gave 519 K nodes + 436 K rels
	g, _ := openReopenGraph(t, dir, reopenCfg{})
	for _, l := range []string{"Device", "Account"} {
		if err := g.Constraints().CreateUnique(context.Background(), l, "key"); err != nil {
			t.Fatal(err)
		}
	}
	writeAisocRecords(t, g, 0, recs, recs*200/436, recs*319/436)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCleanReopenCostNotLinear: the rows a clean reopen decodes (open, one
// more ai-soc record, Close) stay within a constant factor at 10x entities.
func TestCleanReopenCostNotLinear(t *testing.T) {
	sizes := [2]int{3_000, 30_000}
	if !testing.Short() {
		sizes = [2]int{100_000, 1_000_000}
	}
	var decoded [2]int64
	for i, n := range sizes {
		dir := t.TempDir()
		buildAisocStore(t, dir, n)
		decoded[i] = badger.CountRowDecodesForTest(func() {
			g, _ := openReopenGraph(t, dir, reopenCfg{})
			writeAisocRecords(t, g, 10_000_000, 1, 7, 11)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Logf("rows decoded at a clean reopen: %d entities -> %d, %d entities -> %d", sizes[0], decoded[0], sizes[1], decoded[1])
	if decoded[1] > 2*decoded[0]+64 {
		t.Fatalf("clean reopen decodes rows linear in the store: %d entities -> %d rows, %d entities -> %d rows",
			sizes[0], decoded[0], sizes[1], decoded[1])
	}
}

// copyStoreDir copies a closed badger directory.
func copyStoreDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		in, err := os.Open(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.Create(filepath.Join(dst, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(out, in); err != nil {
			t.Fatal(err)
		}
		_ = in.Close()
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// rawWrite commits one key with plain badger, as another binary would.
func rawWrite(t *testing.T, dir string, fn func(txn *badgerv4.Txn) error) {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(fn); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// fullRebuildDigest is the oracle: a copy of the closed store, written to by a
// foreign writer after Close so no persisted open state can be trusted, opened
// and digested.
func fullRebuildDigest(t *testing.T, dir string, rc reopenCfg) string {
	t.Helper()
	cp := copyStoreDir(t, dir)
	rawWrite(t, cp, func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), []byte("x")) })
	var digest string
	n := badger.CountRowDecodesForTest(func() {
		g, bs := openReopenGraph(t, cp, rc)
		digest = bs.IndexStateDigestForTest()
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	})
	if n == 0 {
		t.Fatal("oracle open decoded no rows: it did not rebuild")
	}
	return digest
}

// reopenDigest opens dir, digests it and closes (or abandons it on crash),
// returning the digest and the rows decoded by the open alone.
func reopenDigest(t *testing.T, dir string, rc reopenCfg, mutate func(g *graphpkg.Graph)) (string, int64) {
	t.Helper()
	var g *graphpkg.Graph
	var bs *badger.Store
	decoded := badger.CountRowDecodesForTest(func() { g, bs = openReopenGraph(t, dir, rc) })
	digest := bs.IndexStateDigestForTest()
	if mutate != nil {
		mutate(g)
	}
	if rc.readOnly {
		// A read-only store cannot take the graph's registry save; close the store.
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		return digest, decoded
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	return digest, decoded
}

// mutateMixed runs a seeded round of every write kind the indexes track.
func mutateMixed(t *testing.T, g *graphpkg.Graph, round int) {
	t.Helper()
	ctx := context.Background()
	writeAisocRecords(t, g, round*1000, 300, 40, 70)
	hosts := make([]*types.Node, 0, 20)
	for i := 0; i < 20; i++ {
		n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{
			"name": fmt.Sprintf("h%d-%d", round, i), "zone": fmt.Sprintf("z%d", i%3), "rank": int64(i * round),
			"emb": []float32{float32(i), float32(round), 1}, "tkg_valid_from": int64(1_790_000_000_000 + i*60_000)})
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, n)
	}
	for i := range hosts {
		r, err := g.Rels().Add(ctx, "OWNS", hosts[i], hosts[(i+1)%len(hosts)], map[string]any{"w": int64(i), "tkg_valid_from": int64(1_790_000_000_000 + i)})
		if err != nil {
			t.Fatal(err)
		}
		switch i % 3 {
		case 0:
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(100 + i)}); err != nil {
				t.Fatalf("rel update: %v", err)
			}
		case 1:
			if err := g.Rels().Delete(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i, h := range hosts {
		switch i % 4 {
		case 0: // property change: property, composite, vector indexes and stats move
			if _, err := g.Nodes().Update(ctx, h.ID(), map[string]any{"zone": "moved", "rank": int64(-i), "emb": []float32{9, 9, 9}}); err != nil {
				t.Fatal(err)
			}
		case 1:
			if err := g.Nodes().Delete(ctx, h.ID()); err != nil {
				t.Fatal(err)
			}
		case 2:
			if err := g.Nodes().AddLabel(ctx, h.ID(), "Server"); err != nil {
				t.Fatal(err)
			}
		}
	}
	// LOGON rels (closed intervals): delete some.
	rels, err := g.Rels().ByType("LOGON", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rels {
		if i%13 == 0 {
			if err := g.Rels().Delete(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A node with relationships: the cascade path.
	dev, err := g.Nodes().ByLabelAndProperty("Device", "key", fmt.Sprintf("dev-%d", round), storepkg.QueryOpts{})
	if err == nil && len(dev) == 1 {
		if err := g.Nodes().Delete(ctx, dev[0].ID()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCleanReopenMatchesFullRebuild(t *testing.T) {
	defer badger.SetOpenImageChunkBytesForTest(4096)() // several chunks, so a damaged one is in the middle
	for _, rc := range []reopenCfg{{}, {onDisk: true}} {
		t.Run(fmt.Sprintf("onDisk=%v", rc.onDisk), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			g, _ := openReopenGraph(t, dir, rc)
			for _, l := range []string{"Device", "Account"} {
				if err := g.Constraints().CreateUnique(ctx, l, "key"); err != nil {
					t.Fatal(err)
				}
			}
			ix := g.Index()
			for _, err := range []error{
				ix.CreateProperty("Host", "zone"), ix.CreateRelProperty("LOGON", "action"),
				ix.CreateComposite("Host", []string{"zone", "rank"}), ix.CreateTemporal("Host"),
				ix.CreateRelTemporal("LOGON"), ix.CreateRelTemporal("OWNS"), ix.CreateRelProperty("OWNS", "w"), ix.CreateHighFrequency("Server", time.Hour), ix.CreateVector("Host", "emb", 3, storepkg.DistanceCosine),
			} {
				if err != nil {
					t.Fatal(err)
				}
			}
			mutateMixed(t, g, 1)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}

			check := func(step string, wantFast bool, readOnly bool, mutate func(g *graphpkg.Graph)) {
				t.Helper()
				orc := rc
				orc.readOnly = readOnly
				want := fullRebuildDigest(t, dir, reopenCfg{onDisk: rc.onDisk})
				got, decoded := reopenDigest(t, dir, orc, mutate)
				if got != want {
					t.Fatalf("%s: reopened state differs from the full rebuild:\n%s", step, firstDiff(got, want))
				}
				if wantFast && decoded > 0 {
					t.Fatalf("%s: clean reopen decoded %d rows", step, decoded)
				}
				if !wantFast && decoded == 0 {
					t.Fatalf("%s: reopen decoded no rows, but its persisted state is stale", step)
				}
			}

			// Clean close, then a session that writes again.
			check("clean reopen", true, false, func(g *graphpkg.Graph) { mutateMixed(t, g, 2) })
			check("second clean reopen", true, false, nil)
			check("read-only open", true, true, nil)
			check("after read-only close", true, false, nil)

			// Writes before the first statistics read: the deferred build must
			// see their rows exactly once.
			g, bs := openReopenGraph(t, dir, rc)
			mutateMixed(t, g, 4)
			live := bs.PlannerStatsDigestForTest()
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			cp := copyStoreDir(t, dir)
			rawWrite(t, cp, func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), []byte("x")) })
			og, obs := openReopenGraph(t, cp, rc)
			if want := obs.PlannerStatsDigestForTest(); live != want {
				t.Fatalf("statistics built after writes differ from a full rebuild:\n%s", firstDiff(live, want))
			}
			if err := og.Close(); err != nil {
				t.Fatal(err)
			}

			// Crash: writes flushed, no Close.
			g, bs = openReopenGraph(t, dir, rc)
			mutateMixed(t, g, 3)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			bs.SetDBClosedForTest(true)
			if err := bs.DBForTest().Close(); err != nil {
				t.Fatal(err)
			}
			check("read-only open after crash", false, true, nil)
			check("reopen after crash", false, false, nil)
			check("clean reopen after crash recovery", true, false, nil)

			// Another binary wrote after Close (an older release knows nothing of the persisted state).
			rawWrite(t, dir, func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign2"), []byte("y")) })
			check("reopen after a foreign write", false, false, nil)

			// A store closed by a release without persisted open state.
			rawWrite(t, dir, func(txn *badgerv4.Txn) error { return badger.DeleteOpenStateForTest(txn) })
			check("older store", false, false, nil)
			check("older store, second open", true, false, nil)

			// A damaged chunk of the persisted state.
			rawWrite(t, dir, func(txn *badgerv4.Txn) error { return badger.CorruptOpenStateForTest(txn) })
			check("damaged persisted state", false, false, nil)

			if !rc.onDisk {
				// Switching the *OnDisk modes on over a clean image: the pending
				// on-disk backfills read the rows (the temporal one folds history
				// again), every other index still comes from the image.
				check("clean reopen", true, false, nil)
				rc.onDisk = true
				check("first open with the *OnDisk modes", false, false, nil)
				check("clean reopen with the *OnDisk modes", true, false, nil)
			}
		})
	}
}

func firstDiff(got, want string) string {
	gl, wl := splitLines(got), splitLines(want)
	for i := 0; i < max(len(gl), len(wl)); i++ {
		var g, w string
		if i < len(gl) {
			g = gl[i]
		}
		if i < len(wl) {
			w = wl[i]
		}
		if g != w {
			return fmt.Sprintf("line %d\n got: %s\nwant: %s", i, g, w)
		}
	}
	return "equal"
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}
