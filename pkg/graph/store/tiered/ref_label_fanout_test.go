package tiered

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRefLabelListingSkipsColdShardsWithoutIt (REFFANOUT): a listing of a
// label opens no closed cold shard sealed with no node of that label, while a
// label the shard holds still reads it.
func TestRefLabelListingSkipsColdShardsWithoutIt(t *testing.T) {
	t.Parallel()
	ts := openMintStore(t, mintCfg(t.TempDir(), false))
	ng := tieredNodeGen(t)
	name := hotShardName(ts)
	signal := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	if es.noRows.Load() {
		t.Fatal("setup: the cold shard holds a Signal node")
	}
	kase := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)

	opens, checkouts := es.coldOpens.Load(), es.accesses[2].Load()
	nodes, err := ts.NodesByLabel(mintCaseTok, QueryOpts{Depth: DepthAll})
	if err != nil || len(nodes) != 1 || nodes[0].ID() != kase {
		t.Fatalf("NodesByLabel(Case) = %d nodes, %v; want [%d]", len(nodes), err, kase)
	}
	ids, err := ts.NodeIDsByLabel(mintCaseTok)
	if err != nil || !slices.Equal(ids, []types.NodeID{kase}) {
		t.Fatalf("NodeIDsByLabel(Case) = %v, %v; want [%d]", ids, err, kase)
	}
	if got := es.coldOpens.Load() - opens; got != 0 {
		t.Errorf("a Case listing opened the cold shard without Case nodes %d times", got)
	}
	if got := es.accesses[2].Load() - checkouts; got != 0 {
		t.Errorf("a Case listing checked out the cold shard without Case nodes %d times", got)
	}

	ids, err = ts.NodeIDsByLabel(mintSignalTok)
	if err != nil || !slices.Equal(ids, []types.NodeID{signal}) {
		t.Fatalf("NodeIDsByLabel(Signal) = %v, %v; want [%d]: the cold shard holds it", ids, err, signal)
	}
}

// TestWritableReadPinUnsealsTheSavedCatalog (REFFANOUT review): a read pin that
// may write opens a cold shard writable, and a write can then reach it through
// that handle without a further open; so the open itself must unseal the
// counts in the saved catalog, or a close or crash after the write leaves the
// next start skipping the shard for the label the write added.
func TestWritableReadPinUnsealsTheSavedCatalog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := mintCfg(dir, false)
	ts := openMintStore(t, cfg)
	ng := tieredNodeGen(t)
	name := hotShardName(ts)
	putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	savedSealed := func() bool {
		t.Helper()
		raw, err := os.ReadFile(ts.catalog.path)
		if err != nil {
			t.Fatal(err)
		}
		var saved ShardCatalog
		if err := json.Unmarshal(raw, &saved); err != nil {
			t.Fatal(err)
		}
		for _, e := range saved.Shards {
			if e.Name == name {
				return e.CountsSealed
			}
		}
		t.Fatalf("no catalog entry %s", name)
		return false
	}
	if !savedSealed() {
		t.Fatal("setup: closing the cold shard did not seal its counts")
	}
	_, release, err := es.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if savedSealed() {
		t.Fatal("a writable read pin left the shard's counts sealed in the saved catalog")
	}
}
