package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// CLEARMETA (USAGE W5). Clear on a badger store whose change log is or was on
// (clearDataPreservingLastLSN) deletes every meta key except last_lsn and
// commits that BEFORE it drops the data. This test builds the store state a
// crash between the two leaves (meta gone, rows present) and reopens it: the
// reopened graph must either refuse to open or answer as before Clear, never
// answer wrongly.
func TestClearCrashBetweenMetaDeleteAndDataDrop(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *graphpkg.Graph {
		bs, err := badger.New(badger.Config{Dir: dir, ChangeLog: true})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: bs})
		if err != nil {
			t.Fatalf("graph: %v", err)
		}
		return g
	}
	g := open()
	if err := g.Index().CreateProperty("Host", "zone"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"zone": fmt.Sprintf("z%d", i%3)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"zone": "z0"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// The crash state: step 2 of clearDataPreservingLastLSN committed (every meta
	// key except last_lsn deleted), step 3 (DropPrefix of the data) not run.
	var deleted []string
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{0x0F}})
		var keys [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if string(k[1:]) != "last_lsn" {
				keys = append(keys, k)
			}
		}
		it.Close()
		for _, k := range keys {
			deleted = append(deleted, string(k[1:]))
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	t.Logf("meta keys deleted as Clear's step 2 does: %v", deleted)

	bs, err := badger.New(badger.Config{Dir: dir, ChangeLog: true})
	if err != nil {
		t.Logf("reopen refused (fails closed): %v", err)
		return
	}
	g, err = graphpkg.New(graphpkg.Config{Store: bs})
	if err != nil {
		t.Logf("graph refused (fails closed): %v", err)
		_ = bs.Close()
		return
	}
	defer g.Close()
	hosts, err := g.Nodes().ByLabel("Host", storepkg.QueryOpts{})
	t.Logf("after reopen: ByLabel(Host) = %d nodes, err %v (before: 20)", len(hosts), err)
	accounts, err := g.Nodes().ByLabel("Account", storepkg.QueryOpts{})
	t.Logf("after reopen: ByLabel(Account) = %d nodes, err %v (before: 1)", len(accounts), err)
	z0, err := g.Nodes().ByLabelAndProperty("Host", "zone", "z0", storepkg.QueryOpts{})
	t.Logf("after reopen: Host zone=z0 = %d nodes, err %v (before: 7)", len(z0), err)
	for _, n := range append(hosts, accounts...) {
		labels, _ := g.Resolve().NodeProperty(n, "tkg_labels")
		zone, _ := n.GetProperty("zone")
		t.Logf("  node %d labels %v zone %v", n.ID(), labels, zone)
		break
	}
	// After the crash the label registry is empty, so the next label written gets
	// a token the old rows already carry.
	if _, err := g.Nodes().Add(ctx, []string{"Invoice"}, map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	inv, err := g.Nodes().ByLabel("Invoice", storepkg.QueryOpts{})
	t.Logf("after reopen, one Invoice node added: ByLabel(Invoice) = %d nodes, err %v (want 1)", len(inv), err)
	if len(inv) != 1 {
		t.Errorf("a label written after the reopen answers %d nodes: it took a token the old rows carry", len(inv))
	}
	if len(hosts) != 20 || len(accounts) != 1 || len(z0) != 7 {
		t.Errorf("the reopened store answers differently from before Clear (and it did not refuse to open)")
	}
}
