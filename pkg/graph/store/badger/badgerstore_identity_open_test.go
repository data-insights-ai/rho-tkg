package badger

import (
	"errors"
	"fmt"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestDiskIdentityStartupBackfillsExistingPropertyDefinitions(t *testing.T) {
	dir := t.TempDir()
	registry := registrypkg.NewPropertyKeyRegistry()
	bs, err := New(Config{Dir: dir, PropertyKeyRegistry: registry})
	if err != nil {
		t.Fatal(err)
	}
	if err = bs.CreatePropertyIndex(1, "value"); err != nil {
		t.Fatal(err)
	}
	if err = bs.CreatePropertyIndex(1, "absent"); err != nil {
		t.Fatal(err)
	}
	bs.BeginGroupCommit()
	for i := range 1030 {
		n := types.NewNode(types.NodeID(i+1), 1, nil)
		if err = n.SetProperty("value", int64(i%2)); err != nil {
			t.Fatal(err)
		}
		if err = bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	putTestRel(t, bs, 2000, 2, 1, 2)
	if err = bs.EndGroupCommit(); err != nil {
		t.Fatal(err)
	}
	if err = bs.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := diskIdentityConfig(dir)
	cfg.ReadOnly = true
	cfg.PropertyKeyRegistry = registry
	if opened, err := New(cfg); !errors.Is(err, storecontract.ErrIndexNotFound) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("readonly bootstrap should refuse incomplete index: %v", err)
	}
	cfg.ReadOnly = false
	for _, readonly := range []bool{false, true} {
		cfg.ReadOnly = readonly
		bs, err = New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for value := range 2 {
			ids := nodeIDsVia(t, bs, 1, "value", int64(value))
			if len(ids) != 515 {
				t.Fatalf("backfill changed complete answers for %d: %d", value, len(ids))
			}
			for i, id := range ids {
				if id != int64(i*2+value+1) {
					t.Fatal("backfill order/identity", i, id)
				}
			}
		}
		if got := nodeIDsVia(t, bs, 1, "absent", int64(0)); len(got) != 0 {
			t.Fatal("missing property became indexed", got)
		}
		nodes, nodeErr := bs.NodeCount()
		rels, relErr := bs.RelationshipCount()
		if nodes != 1030 || rels != 1 || nodeErr != nil || relErr != nil {
			t.Fatal("startup counts", nodes, rels, nodeErr, relErr)
		}
		if len(bs.nodeIDs)+len(bs.relIDs)+len(bs.nodeHashes)+len(bs.typeIdx) != 0 {
			t.Fatal("startup retained corpus identities")
		}
		if err = bs.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDiskIdentityStartupRejectsUnsupportedDefinitionsWithoutChangingData(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	putTestNode(t, bs, 1, 1, nil)
	if err = bs.CreateTemporalIndex(1); err != nil {
		t.Fatal(err)
	}
	if err = bs.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := New(diskIdentityConfig(dir)); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatal("unsupported resident definition silently opened", err)
	}
	bs, err = New(Config{Dir: dir, HistoricalMembershipOnDisk: true, SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if n, err := bs.GetNode(1); err != nil || n.ID() != 1 {
		t.Fatal("failed profile open changed source row", n, err)
	}
}

func TestDiskIdentityStartupRejectsCorruptCurrentRows(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		t.Run(fmt.Sprint(relationship), func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(diskIdentityConfig(dir))
			if err != nil {
				t.Fatal(err)
			}
			putTestNode(t, bs, 1, 1, nil)
			putTestNode(t, bs, 2, 1, nil)
			putTestRel(t, bs, 3, 2, 1, 2)
			if err = bs.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
			if err != nil {
				t.Fatal(err)
			}
			key := storeutil.NodeKey(1)
			if relationship {
				key = storeutil.RelKey(3)
			}
			err = db.Update(func(txn *badgerv4.Txn) error { return txn.Set(key, []byte("corrupt-current-row")) })
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			if opened, err := New(diskIdentityConfig(dir)); err == nil {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatal("corrupt canonical row converted into absence")
			}
		})
	}
}
