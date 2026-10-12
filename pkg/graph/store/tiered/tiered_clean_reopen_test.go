package tiered

import (
	"regexp"
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 54 on tiered: every shard is a badger store that reopens from its
// keys and its clean-close image. With relationships crossing shards (entity
// and outgoing leg on one shard, incoming leg on the endpoint's shard), an
// archived node and deletes, each shard's RAM state after a clean reopen must
// equal a full rebuild of the same files.
func TestTieredCleanReopenMatchesFullRebuild(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{DataDir: dir, RefLabels: []string{"Case", "User"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, userTok, signalTok := installDefaultTestLabelRegistry(t, ts)
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	var users, signals []*types.Node
	var rels []types.RelID
	put := func(label uint16, k int) *types.Node {
		n := types.NewNode(types.NodeID(ng.Generate()), label, nil)
		_ = n.SetProperty("k", int64(k))
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	link := func(a, b *types.Node) {
		r := types.NewRelationship(types.RelID(rg.Generate()), 2, a.ID(), b.ID())
		_ = r.SetProperty("w", int64(len(rels)))
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r.ID())
	}
	round := func(base int) {
		for i := 0; i < 6; i++ {
			users = append(users, put(userTok, base+i))
			signals = append(signals, put(signalTok, base+i))
		}
		for i := range signals { // signal -> user and user -> signal: cross-shard both ways
			link(signals[i], users[(i+1)%len(users)])
			link(users[i%len(users)], signals[i])
		}
	}
	round(0)
	forceRotation(t, ts) // the first signals now live on a warm shard
	round(100)
	for i := 0; i < len(rels); i += 5 {
		if err := ts.DeleteRelationship(rels[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := ts.ArchiveNode(users[1].ID()); err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	oracle := t.TempDir()
	copyTree(t, dir, oracle)
	invalidateOpenImages(t, oracle)
	decoded := map[string]int64{}
	digests := func(dataDir string) map[string]string {
		c := cfg
		c.DataDir = dataDir
		var ts *Store
		var err error
		decoded[dataDir] = badgerpkg.CountRowDecodesForTest(func() { ts, err = New(c) })
		if err != nil {
			t.Fatal(err)
		}
		defer ts.Close()
		stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		out := make(map[string]string, len(stores))
		for _, ns := range stores {
			out[ns.name] = ndvField.ReplaceAllString(ns.StoreForTest().IndexStateDigestForTest(), "NDV:-")
		}
		return out
	}
	got, want := digests(dir), digests(oracle)
	if decoded[dir] != 0 || decoded[oracle] == 0 {
		t.Errorf("rows decoded by the open: clean %d (want 0), rebuild %d (want > 0)", decoded[dir], decoded[oracle])
	}
	if len(got) != len(want) || len(got) < 3 {
		t.Fatalf("shards: reopened %d, oracle %d (want reference, archive and event shards)", len(got), len(want))
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("shard %s: clean reopen differs from the full rebuild:\n got: %.2000s\nwant: %.2000s", name, got[name], w)
		}
	}
}

// ndvField matches the NDV estimate in a digest: NDV never decreases, so a
// clean reopen keeps values earlier sessions deleted and a rebuild does not.
var ndvField = regexp.MustCompile(`NDV:\d+`)
