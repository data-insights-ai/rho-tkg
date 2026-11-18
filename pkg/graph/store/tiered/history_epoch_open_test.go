package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// epochFixture is a node and a relationship, each with a history row, on an
// event shard demoted to cold, plus a reference node ("Case") and its
// history.
type epochFixture struct {
	ts   *Store
	cfg  Config
	node types.NodeID
	rel  types.RelID
	ref  types.NodeID
	old  string
}

func newEpochFixture(t *testing.T, maxCold int) *epochFixture {
	t.Helper()
	cfg := volumeCfg(t.TempDir(), "", "")
	cfg.MaxOpenColdShards = maxCold
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	sig, _ := reg.GetOrCreate("Signal")
	cs, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), sig, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), sig, nil)
	c := types.NewNode(types.NodeID(gen.Generate()), cs, nil)
	for _, n := range []*types.Node{a, b, c} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 5, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatal(err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, old)
	f := &epochFixture{ts: ts, cfg: cfg, node: a.ID(), rel: r.ID(), ref: c.ID(), old: old}
	f.closeCold(t)
	return f
}

// closeCold closes the cold shard's store, as the idle sweep does.
func (f *epochFixture) closeCold(t *testing.T) {
	t.Helper()
	es := f.ts.EventShardsForTest()[f.old]
	es.LockShardMuForTest()
	defer es.UnlockShardMuForTest()
	if es.Store() == nil {
		return
	}
	if err := es.Store().Close(); err != nil {
		t.Fatalf("close cold store: %v", err)
	}
	es.SetStoreForTest(nil)
}

func (f *epochFixture) epochs(t *testing.T) (node, rel uint64) {
	t.Helper()
	node, err := f.ts.NodeHistoryEpoch(f.node)
	if err != nil {
		t.Fatal(err)
	}
	rel, err = f.ts.RelHistoryEpoch(f.rel)
	if err != nil {
		t.Fatal(err)
	}
	return node, rel
}

// TestHistoryEpoch_ClosedShardOpensNothing (R2-5 HEADREAD round 2, H1): the
// history token never opens a shard. A token is good only while the handle
// that issued it stays open, so for an entity whose walk reaches a closed
// shard the answer is 0 (no token: the caller folds and keeps nothing), and
// ColdOpens does not move. Control (MaxOpenColdShards 1, the shard retained
// after a read): a nonzero token, the same on a second call, larger after a
// history write, 0 again once the shard is closed. A reference node with the
// archive in the catalog but not open (after a reopen): 0, the archive left
// closed. It opened the shard (one open per call) and returned its token.
func TestHistoryEpoch_ClosedShardOpensNothing(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	t.Run("cap 0", func(t *testing.T) {
		t.Parallel()
		f := newEpochFixture(t, 0)
		defer func() { _ = f.ts.Close() }()
		o := f.ts.ShardAccessStats().ColdOpens
		node, rel := f.epochs(t)
		opened := f.ts.ShardAccessStats().ColdOpens - o
		t.Logf("closed shard: node token %d, rel token %d, %d opened", node, rel, opened)
		if node != 0 || rel != 0 || opened != 0 {
			t.Fatalf("closed shard: tokens %d / %d, %d opened; want 0 / 0, none opened", node, rel, opened)
		}
	})
	t.Run("cap 1", func(t *testing.T) {
		t.Parallel()
		f := newEpochFixture(t, 1)
		defer func() { _ = f.ts.Close() }()
		if _, err := f.ts.GetNode(f.node); err != nil { // opens the shard; the cap retains it
			t.Fatal(err)
		}
		o := f.ts.ShardAccessStats().ColdOpens
		n1, r1 := f.epochs(t)
		n2, r2 := f.epochs(t)
		if n1 == 0 || r1 == 0 || n1 != n2 || r1 != r2 || f.ts.ShardAccessStats().ColdOpens != o {
			t.Fatalf("open shard: tokens %d,%d then %d,%d, %d opened; want nonzero and stable, none opened", n1, r1, n2, r2, f.ts.ShardAccessStats().ColdOpens-o)
		}
		n, err := f.ts.GetNode(f.node)
		if err != nil {
			t.Fatal(err)
		}
		n = n.DeepCopy()
		n.SetVersion(1)
		if err := f.ts.PutNodeVersion(f.node, 1, n); err != nil {
			t.Fatal(err)
		}
		if n3, _ := f.epochs(t); n3 <= n1 {
			t.Fatalf("after a history write: token %d, was %d; want larger", n3, n1)
		}
		f.closeCold(t)
		o = f.ts.ShardAccessStats().ColdOpens
		n4, r4 := f.epochs(t)
		if n4 != 0 || r4 != 0 || f.ts.ShardAccessStats().ColdOpens != o {
			t.Fatalf("after the close: tokens %d / %d, %d opened; want 0 / 0, none opened", n4, r4, f.ts.ShardAccessStats().ColdOpens-o)
		}
	})
	t.Run("archive", func(t *testing.T) {
		t.Parallel()
		f := newEpochFixture(t, 0)
		if err := f.ts.ArchiveNode(f.ref); err != nil {
			t.Fatal(err)
		}
		if err := f.ts.Close(); err != nil {
			t.Fatal(err)
		}
		ts, err := New(f.cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ts.Close() }()
		if ts.RefArchiveForTest().Load() != nil || !ts.HasArchiveShardForTest() {
			t.Fatal("fixture: the archive is open after the reopen")
		}
		e, err := ts.NodeHistoryEpoch(f.ref)
		t.Logf("archived node, archive closed: token %d, %v; archive open %v", e, err, ts.RefArchiveForTest().Load() != nil)
		if err != nil || e != 0 || ts.RefArchiveForTest().Load() != nil {
			t.Fatalf("archived node: token %d, %v, archive open %v; want 0, the archive left closed", e, err, ts.RefArchiveForTest().Load() != nil)
		}
	})
}
