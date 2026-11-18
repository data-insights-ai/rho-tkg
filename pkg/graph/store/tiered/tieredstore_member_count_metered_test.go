package tiered

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// mcNetTok is the relationship type of the member-count fixture.
const mcNetTok uint16 = 5

// mcColdStore builds, in dir, a store with three cold event shards, each with
// two current Signal nodes and two current relationships of type mcNetTok
// between them, and a handle cap of 8 (every open handle is kept). It returns
// the cold shards, still open: mcCloseUnsealed or idle-close them.
func mcColdStore(t *testing.T, dir string) (*Store, []*EventShard) {
	t.Helper()
	return mcColdStoreCap(t, dir, 8)
}

// mcColdStoreCap is mcColdStore with the handle cap maxOpen (0: every cold
// read handle is transient, closed and sealed at its check-in).
func mcColdStoreCap(t *testing.T, dir string, maxOpen int) (*Store, []*EventShard) {
	t.Helper()
	cfg := mintCfg(dir, false)
	cfg.MaxOpenColdShards = maxOpen
	ts := openMintStore(t, cfg)
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	var names []string
	for range 3 {
		a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
		b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
		putMintRel(t, ts, types.RelID(rg.Generate()), mcNetTok, a, b)
		putMintRel(t, ts, types.RelID(rg.Generate()), mcNetTok, b, a)
		names = append(names, hotShardName(ts))
		rotate(t, ts)
	}
	var cold []*EventShard
	for _, name := range names {
		demoteToCold(ts, name)
		ts.mu.RLock()
		es := ts.eventShards[name]
		ts.mu.RUnlock()
		if es == nil {
			t.Fatalf("fixture: no event shard %q", name)
		}
		cold = append(cold, es)
	}
	return ts, cold
}

// mcCloseUnsealed closes every shard of cold without sealing its counts (an
// unclean stop, a failed seal): the next count of it must open it.
func mcCloseUnsealed(t *testing.T, ts *Store, cold []*EventShard) {
	t.Helper()
	for _, es := range cold {
		es.shardMu.Lock()
		if es.store != nil {
			if err := es.store.Close(); err != nil {
				es.shardMu.Unlock()
				t.Fatalf("close %s: %v", es.name, err)
			}
			es.clearStoreLocked(ts)
		}
		es.counts.Store(nil)
		es.shardMu.Unlock()
	}
}

// mcSealIdle idle-closes every open cold shard, which seals its counts.
func mcSealIdle(t *testing.T, ts *Store, cold []*EventShard) {
	t.Helper()
	prev := ts.idleTimeoutValue()
	ts.SetIdleTimeoutForTest(time.Millisecond)
	for _, es := range cold {
		es.SetLastAccessForTest(0)
	}
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(prev)
	for _, es := range cold {
		es.shardMu.Lock()
		open, sealed := es.store != nil, es.cachedCounts() != nil
		es.shardMu.Unlock()
		if open || !sealed {
			t.Fatalf("fixture: shard %s open %t, sealed %t after the idle close", es.name, open, sealed)
		}
	}
}

func mcOpens(ts *Store) uint64 { return ts.ShardAccessStats().ColdOpens }

// TestMemberCountMetered: the tiered store's metered member counts
// (storecontract.MemberCountMeteredCapability) charge each closed shard they
// open to the read's tally before the open, where the open happens: the
// unlimited control counts exactly the native opens (the store's ColdOpens)
// and each open's native proof/header probe;
// a ShardOpens budget below them refuses with no count and the shard past the
// limit left closed; a shard with sealed counts opens nothing; several counts
// on one meter share its allowance; the archive's lazy open is charged too.
func TestMemberCountMetered(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	var _ storecontract.MemberCountMeteredCapability = (*Store)(nil)
	counts := []struct {
		name   string
		tok    uint16
		count  func(ts *Store, tok uint16, t *storecontract.ReadTally) (int, error)
		plain  func(ts *Store, tok uint16) (int, error)
		other  string // the other count, for the several-keys case
		otherF func(ts *Store, t *storecontract.ReadTally) (int, error)
	}{
		{"NodeCountByLabel", mintSignalTok,
			func(ts *Store, tok uint16, t *storecontract.ReadTally) (int, error) {
				return ts.NodeCountByLabelMetered(tok, t)
			},
			func(ts *Store, tok uint16) (int, error) { return ts.NodeCountByLabel(tok) },
			"RelCountByType", func(ts *Store, t *storecontract.ReadTally) (int, error) {
				return ts.RelCountByTypeMetered(mcNetTok, t)
			}},
		{"RelCountByType", mcNetTok,
			func(ts *Store, tok uint16, t *storecontract.ReadTally) (int, error) {
				return ts.RelCountByTypeMetered(tok, t)
			},
			func(ts *Store, tok uint16) (int, error) { return ts.RelCountByType(tok) },
			"NodeCountByLabel", func(ts *Store, t *storecontract.ReadTally) (int, error) {
				return ts.NodeCountByLabelMetered(mintSignalTok, t)
			}},
	}
	ctx := context.Background()
	for _, c := range counts {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, cold := mcColdStore(t, t.TempDir())

			// Plain, nil tally: the count opens every unsealed shard.
			mcCloseUnsealed(t, ts, cold)
			o := mcOpens(ts)
			n, err := c.count(ts, c.tok, nil)
			if err != nil || n != 6 || mcOpens(ts)-o != 3 {
				t.Fatalf("nil tally = %d, %v, %d opened; want 6 and 3 opens", n, err, mcOpens(ts)-o)
			}
			if pn, err := c.plain(ts, c.tok); err != nil || pn != 6 {
				t.Fatalf("plain = %d, %v; want 6", pn, err)
			}

			// Every native open and its proof/header probe are separate costs.
			mcCloseUnsealed(t, ts, cold)
			o = mcOpens(ts)
			_, meter := storecontract.WithReadBudget(ctx, storecontract.ReadBudget{})
			n, err = c.count(ts, c.tok, meter.Tally("count"))
			opened := mcOpens(ts) - o
			control := storecontract.ReadWork{Candidates: 3, ShardOpens: 3}
			if err != nil || n != 6 || opened != 3 || meter.Work() != control {
				t.Fatalf("unlimited = %d, %v, %d opened, work %+v; want 6, 3 opens, work %+v", n, err, opened, meter.Work(), control)
			}

			// The control's figure is the smallest budget that answers.
			mcCloseUnsealed(t, ts, cold)
			o = mcOpens(ts)
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{Candidates: 3, ShardOpens: 3})
			n, err = c.count(ts, c.tok, meter.Tally("count"))
			if err != nil || n != 6 || mcOpens(ts)-o != 3 || meter.Work() != control {
				t.Fatalf("Candidates/ShardOpens 3 = %d, %v, %d opened, work %+v; want 6 and the control's work", n, err, mcOpens(ts)-o, meter.Work())
			}

			// One under: refused before the third open, no count.
			mcCloseUnsealed(t, ts, cold)
			o = mcOpens(ts)
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{ShardOpens: 2})
			n, err = c.count(ts, c.tok, meter.Tally("count"))
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "shard opens" || rb.Door != "count" || n != 0 ||
				mcOpens(ts)-o != 2 || rb.Work.ShardOpens != 3 || rb.Work.Candidates != 2 || meter.Work() != rb.Work {
				t.Fatalf("ShardOpens 2 = %d, %v, %d opened, meter %+v; want the shard-opens refusal at 3, no count, 2 opens", n, err, mcOpens(ts)-o, meter.Work())
			}
			still := 0
			for _, es := range cold {
				es.shardMu.Lock()
				if es.store == nil {
					still++
				}
				es.shardMu.Unlock()
			}
			if still != 1 {
				t.Fatalf("ShardOpens 2: %d shards closed after the refusal, want 1 (the refused one)", still)
			}

			// A Candidate refusal occurs inside the third native constructor,
			// before its key probe: two handles complete and the third stays closed.
			mcCloseUnsealed(t, ts, cold)
			o = mcOpens(ts)
			var nativeOpens atomic.Uint64
			ts.SetShardOpenHookForTest(func(BadgerStoreConfig) { nativeOpens.Add(1) })
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{Candidates: 2})
			n, err = c.count(ts, c.tok, meter.Tally("count"))
			ts.SetShardOpenHookForTest(nil)
			if !errors.As(err, &rb) || rb.Limit != "candidates" || rb.Door != "count" || n != 0 ||
				mcOpens(ts)-o != 2 || nativeOpens.Load() != 3 || meter.Work() != control || rb.Work != meter.Work() {
				t.Fatalf("Candidates 2 = %d, %v, %d completed opens/%d native constructors, work %+v; want refusal at the third open probe and no count", n, err, mcOpens(ts)-o, nativeOpens.Load(), meter.Work())
			}

			// Several keys on one meter: each count alone fits 5, both do not.
			mcCloseUnsealed(t, ts, cold)
			o = mcOpens(ts)
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{ShardOpens: 5})
			if n, err := c.count(ts, c.tok, meter.Tally("count")); err != nil || n != 6 {
				t.Fatalf("first key = %d, %v", n, err)
			}
			mcCloseUnsealed(t, ts, cold)
			n, err = c.otherF(ts, meter.Tally("count"))
			if !errors.As(err, &rb) || rb.Limit != "shard opens" || n != 0 || mcOpens(ts)-o != 5 || meter.Work().ShardOpens != 6 || meter.Work().Candidates != 5 {
				t.Fatalf("second key (%s) = %d, %v, %d opened, work %+v; want the refusal at 6 with 5 native opens", c.other, n, err, mcOpens(ts)-o, meter.Work())
			}

			// The probes also share one allowance across the two keys.
			mcCloseUnsealed(t, ts, cold)
			o = mcOpens(ts)
			nativeOpens.Store(0)
			ts.SetShardOpenHookForTest(func(BadgerStoreConfig) { nativeOpens.Add(1) })
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{Candidates: 5})
			if n, err := c.count(ts, c.tok, meter.Tally("count")); err != nil || n != 6 {
				t.Fatalf("first key under Candidates 5 = %d, %v", n, err)
			}
			mcCloseUnsealed(t, ts, cold)
			n, err = c.otherF(ts, meter.Tally("count"))
			ts.SetShardOpenHookForTest(nil)
			if !errors.As(err, &rb) || rb.Limit != "candidates" || n != 0 || mcOpens(ts)-o != 5 || nativeOpens.Load() != 6 ||
				meter.Work() != (storecontract.ReadWork{Candidates: 6, ShardOpens: 6}) || rb.Work != meter.Work() {
				t.Fatalf("second key under Candidates 5 = %d, %v, %d completed opens/%d native constructors, work %+v; want refusal at the sixth probe", n, err, mcOpens(ts)-o, nativeOpens.Load(), meter.Work())
			}

			// Sealed counts: the count opens nothing and charges nothing.
			mcCloseUnsealed(t, ts, cold)
			if _, err := c.count(ts, c.tok, nil); err != nil { // open all three again
				t.Fatal(err)
			}
			mcSealIdle(t, ts, cold)
			o = mcOpens(ts)
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{ShardOpens: 1})
			n, err = c.count(ts, c.tok, meter.Tally("count"))
			if err != nil || n != 6 || mcOpens(ts)-o != 0 || meter.Work() != (storecontract.ReadWork{}) {
				t.Fatalf("sealed = %d, %v, %d opened, work %+v; want 6, no open, no work", n, err, mcOpens(ts)-o, meter.Work())
			}
		})
	}
}

// TestMemberCountMetered_TransientDoubleOpen: with no room under the handle cap
// a cold read handle is transient, so the count's open closes again at its
// check-in, sealing the shard's counts; the listing that follows on the same
// meter opens the shard a second time. Both opens are native work and each is
// charged once (6 native, 6 counted); a later count reads the seal and opens
// nothing.
func TestMemberCountMetered_TransientDoubleOpen(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, cold := mcColdStoreCap(t, t.TempDir(), 0)
	mcCloseUnsealed(t, ts, cold)
	o := mcOpens(ts)
	_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	tl := meter.Tally("count")
	n, err := ts.NodeCountByLabelMetered(mintSignalTok, tl)
	if err != nil || n != 6 || mcOpens(ts)-o != 3 || meter.Work().ShardOpens != 3 {
		t.Fatalf("count = %d, %v, %d opened, work %+v; want 6 and 3 opens counted", n, err, mcOpens(ts)-o, meter.Work())
	}
	for _, es := range cold {
		es.shardMu.Lock()
		open, sealed := es.store != nil, es.cachedCounts() != nil
		es.shardMu.Unlock()
		if open || !sealed {
			t.Fatalf("shard %s open %t, sealed %t after a transient count; want closed and sealed", es.name, open, sealed)
		}
	}
	ids, err := ts.NodeIDsByLabelMetered(mintSignalTok, tl)
	if err != nil || len(ids) != 6 || mcOpens(ts)-o != 6 || meter.Work().ShardOpens != 6 {
		t.Fatalf("listing = %d, %v, %d opened, work %+v; want 6 members, 6 opens, each counted once", len(ids), err, mcOpens(ts)-o, meter.Work())
	}
	n, err = ts.RelCountByTypeMetered(mcNetTok, tl)
	if err != nil || n != 6 || mcOpens(ts)-o != 6 || meter.Work().ShardOpens != 6 {
		t.Fatalf("sealed count = %d, %v, %d opened, work %+v; want 6, no further open", n, err, mcOpens(ts)-o, meter.Work())
	}
}

// TestMemberCountMetered_Archive: after a reopen the archive is closed until a
// read needs it; both metered counts charge its open before it (the cold
// shards sealed, so it is the only open), refuse with the archive left closed
// when the meter has no room. A healthy archive counts two native opens:
// the readonly repair preflight and the retained writable handle. Each open
// also admits its native proof/header probe.
func TestMemberCountMetered_Archive(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, name := range []string{"NodeCountByLabel", "RelCountByType"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			ts, cold := mcColdStore(t, dir)
			ng := tieredNodeGen(t)
			c := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
			if err := ts.ArchiveNode(c); err != nil {
				t.Fatalf("ArchiveNode: %v", err)
			}
			mcSealIdle(t, ts, cold)
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}
			count := func(ts *Store, tl *storecontract.ReadTally) (int, error) {
				if name == "NodeCountByLabel" {
					return ts.NodeCountByLabelMetered(mintSignalTok, tl)
				}
				return ts.RelCountByTypeMetered(mcNetTok, tl)
			}
			reopen := func() *Store {
				cfg := mintCfg(dir, false)
				cfg.MaxOpenColdShards = 8
				ts := openMintStore(t, cfg)
				if !ts.HasArchiveShardForTest() || ts.RefArchiveForTest().Load() != nil {
					t.Fatal("fixture: want a catalogued archive, closed since New")
				}
				return ts
			}
			ctx := context.Background()
			var nativeOpens atomic.Uint64

			// No room left: refused, the archive stays closed.
			ts = reopen()
			ts.SetShardOpenHookForTest(func(BadgerStoreConfig) { nativeOpens.Add(1) })
			_, meter := storecontract.WithReadBudget(ctx, storecontract.ReadBudget{ShardOpens: 1})
			if err := meter.AddShardOpens("prior", 1); err != nil {
				t.Fatal(err)
			}
			n, err := count(ts, meter.Tally("count"))
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "shard opens" || n != 0 || ts.RefArchiveForTest().Load() != nil || meter.Work().ShardOpens != 2 || nativeOpens.Load() != 0 {
				t.Fatalf("no room = %d, %v, archive open %t, work %+v; want the refusal with the archive closed", n, err, ts.RefArchiveForTest().Load() != nil, meter.Work())
			}

			// Unlimited: both archive opens and their native probes count.
			o := mcOpens(ts)
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{})
			n, err = count(ts, meter.Tally("count"))
			control := storecontract.ReadWork{Candidates: 2, ShardOpens: 2}
			if err != nil || n != 6 || ts.RefArchiveForTest().Load() == nil || mcOpens(ts)-o != 0 || meter.Work() != control || nativeOpens.Load() != 2 {
				t.Fatalf("unlimited = %d, %v, archive open %t, %d cold opens, work %+v; want 6, the archive open and work %+v", n, err, ts.RefArchiveForTest().Load() != nil, mcOpens(ts)-o, meter.Work(), control)
			}
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}

			// ShardOpens 2, cold shards sealed: both archive opens fit.
			ts = reopen()
			nativeOpens.Store(0)
			ts.SetShardOpenHookForTest(func(BadgerStoreConfig) { nativeOpens.Add(1) })
			o = mcOpens(ts)
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{Candidates: 2, ShardOpens: 2})
			n, err = count(ts, meter.Tally("count"))
			if err != nil || n != 6 || mcOpens(ts)-o != 0 || ts.RefArchiveForTest().Load() == nil || meter.Work() != control || nativeOpens.Load() != 2 {
				t.Fatalf("Candidates/ShardOpens 2 = %d, %v, %d cold opens, archive open %t, work %+v; want 6 and both archive opens/probes", n, err, mcOpens(ts)-o, ts.RefArchiveForTest().Load() != nil, meter.Work())
			}
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}

			// One probe short: the writable constructor starts but cannot
			// admit its probe, so no archive handle or count is published.
			ts = reopen()
			nativeOpens.Store(0)
			ts.SetShardOpenHookForTest(func(BadgerStoreConfig) { nativeOpens.Add(1) })
			o = mcOpens(ts)
			_, meter = storecontract.WithReadBudget(ctx, storecontract.ReadBudget{Candidates: 1})
			n, err = count(ts, meter.Tally("count"))
			if !errors.As(err, &rb) || rb.Limit != "candidates" || rb.Work != meter.Work() || n != 0 || mcOpens(ts)-o != 0 ||
				ts.RefArchiveForTest().Load() != nil || meter.Work() != control || nativeOpens.Load() != 2 {
				t.Fatalf("Candidates 1 = %d, %v, archive open %t, %d native constructors, work %+v; want refusal before the second probe with no retained archive", n, err, ts.RefArchiveForTest().Load() != nil, nativeOpens.Load(), meter.Work())
			}
		})
	}
}
