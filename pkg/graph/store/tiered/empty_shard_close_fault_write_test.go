package tiered

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// R1: a failed close of an empty demoted shard detaches the handle and fences
// the store (the persistence safeguard). The write whose rotation ran that
// cleanup is a public write: it returns the fence's error and commits nothing
// (no row, no change-log record, no shard made for it), and every row committed
// before stays readable after a reopen. These tests drive the contract through
// the public write doors, not through checkRotation.

// closeFaultStore is a disk tiered store with the change log on and rows of each
// kind committed before a close fault, ready to fail the next rotation's cleanup.
type closeFaultStore struct {
	ts       *Store
	cfg      Config
	clock    *tierClock
	handle   *BadgerStore // the empty shard whose close will fail
	event    types.NodeID // committed before: an event node on 2026-07-23
	refA     types.NodeID // committed before: two reference nodes and a rel
	refB     types.NodeID
	rel      types.RelID
	feed     []uint64 // the change-log LSNs committed before
	shardSet []string // the event shards before the faulted rotation
}

func newCloseFaultStore(t *testing.T) *closeFaultStore {
	t.Helper()
	f := &closeFaultStore{clock: &tierClock{}}
	f.clock.set(julDay(22, 12))
	f.cfg = tierClockCfg(t.TempDir(), f.clock)
	f.cfg.ChangeLog = true
	f.ts = openMintStore(t, f.cfg)
	f.handle = f.ts.hotShard.store

	// The first write on 2026-07-23 rotates 2026-07-22 to warm; 2026-07-22 keeps
	// no row of its own, the rows below land in 2026-07-23 and the reference shard.
	f.clock.set(julDay(23, 12))
	f.event = putMintNode(t, f.ts, types.NodeID(vtID(vtCounter(julDay(23, 12)))), mintSignalTok)
	gen := tieredNodeGen(t)
	f.refA = putMintNode(t, f.ts, types.NodeID(gen.Generate()), mintCaseTok)
	f.refB = putMintNode(t, f.ts, types.NodeID(gen.Generate()), mintCaseTok)
	f.rel = types.RelID(tieredRelGen(t).Generate())
	putMintRel(t, f.ts, f.rel, 1, f.refA, f.refB)
	if err := f.ts.Flush(); err != nil {
		t.Fatal(err)
	}
	feed, err := f.ts.ChangeFeed(0, 0)
	if err != nil || len(feed) == 0 {
		t.Fatalf("change feed before the fault = %v, %v", feed, err)
	}
	f.feed = lsns(feed)
	f.shardSet = f.eventShardNames()

	// A failed metadata-only flush leaves pending work on the empty shard while
	// HoldsNoRows stays true; without pending work Badger's final flush returns
	// before its guard (the fixture of TestEmptyShardDemotion_CloseFailureDetachesAndFences).
	seed := errors.New("seed pending metadata")
	f.handle.FailNextFlushForTest(seed)
	if err := f.handle.MetaSetDurableRaising("empty-close-fixture", []byte{1}, 0); !errors.Is(err, seed) {
		t.Fatalf("metadata fixture flush = %v, want injected failure", err)
	}
	if empty, err := f.handle.HoldsNoRows(); err != nil || !empty {
		t.Fatalf("metadata fixture is not row-empty: %v, %v", empty, err)
	}
	return f
}

func (f *closeFaultStore) eventShardNames() []string {
	f.ts.mu.RLock()
	defer f.ts.mu.RUnlock()
	names := make([]string, 0, len(f.ts.eventShards))
	for name := range f.ts.eventShards {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// failWrite runs write with the clock on 2026-07-27, so its rotation demotes the
// empty shard and the close of that shard fails; it returns what the write
// returned.
func (f *closeFaultStore) failWrite(t *testing.T, write func() error) error {
	t.Helper()
	f.handle.LockFlushMuForTest()
	locked := true
	defer func() {
		if locked {
			f.handle.UnlockFlushMuForTest()
		}
	}()
	f.clock.set(julDay(27, 12))
	done := make(chan error, 1)
	go func() { done <- write() }()
	select {
	case <-f.handle.GCDoneForTest(): // Close signalled stop; its final flush is blocked.
	case <-time.After(5 * time.Second):
		t.Fatal("the write's rotation did not reach Close")
	}
	f.handle.SetDBClosedForTest(true)
	f.handle.UnlockFlushMuForTest()
	locked = false
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the faulted write did not finish")
		return nil
	}
}

func TestEmptyShardDemotion_CloseFailureFailsTriggeringWrite(t *testing.T) {
	event := func(day int) *types.Node {
		return types.NewNode(types.NodeID(vtID(vtCounter(julDay(day, 12)))), mintSignalTok, nil)
	}
	ref := func(id int64) *types.Node { return types.NewNode(types.NodeID(id), mintCaseTok, nil) }
	cases := []struct {
		name  string
		write func(t *testing.T, f *closeFaultStore) error
		// absent: nodes the refused write must not leave behind.
		absent []types.NodeID
	}{
		{
			name:   "PutNode event",
			write:  func(t *testing.T, f *closeFaultStore) error { return f.ts.PutNode(event(27)) },
			absent: []types.NodeID{event(27).ID()},
		},
		{
			name:   "PutNode reference",
			write:  func(t *testing.T, f *closeFaultStore) error { return f.ts.PutNode(ref(1<<40 + 7)) },
			absent: []types.NodeID{ref(1<<40 + 7).ID()},
		},
		{
			// A backfilled event whose window has no shard: the refused write must
			// not make that window's shard (a catalog write and an open handle).
			name:   "PutNode backfill into a window without a shard",
			write:  func(t *testing.T, f *closeFaultStore) error { return f.ts.PutNode(event(25)) },
			absent: []types.NodeID{event(25).ID()},
		},
		{
			name: "PutNodesBatch",
			write: func(t *testing.T, f *closeFaultStore) error {
				return f.ts.PutNodesBatch([]*types.Node{event(27), ref(1<<40 + 9)})
			},
			absent: []types.NodeID{event(27).ID(), ref(1<<40 + 9).ID()},
		},
		{
			name: "PutRelationship",
			write: func(t *testing.T, f *closeFaultStore) error {
				return f.ts.PutRelationship(types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, f.refA, f.refB))
			},
		},
		{
			name: "PutRelationshipsBatch",
			write: func(t *testing.T, f *closeFaultStore) error {
				return f.ts.PutRelationshipsBatch([]*types.Relationship{
					types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, f.refA, f.refB),
					types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 2, f.refB, f.refA),
				})
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newCloseFaultStore(t)
			err := f.failWrite(t, func() error { return c.write(t, f) })

			// The triggering write reports the fence: the cleanup's Close failure.
			if err == nil {
				t.Fatal("the write whose rotation's cleanup failed to close a shard returned nil")
			}
			if !errors.Is(err, badgerv4.ErrDBClosed) || !strings.Contains(err.Error(), "close empty demoted shard") {
				t.Fatalf("write error = %v, want the demotion Close failure", err)
			}
			if f.ts.backgroundError() == nil {
				t.Fatal("no sticky fault recorded")
			}
			// The rotation committed; the write made nothing else: no shard for its
			// window, nothing buffered in a shard it might have reached.
			if f.ts.hotShard.name != "2026-07-27" {
				t.Fatalf("hot shard %q: the committed rotation was rolled back", f.ts.hotShard.name)
			}
			if got, want := f.eventShardNames(), append(slices.Clone(f.shardSet), "2026-07-27"); !slices.Equal(got, want) {
				t.Fatalf("event shards = %v, want %v: the refused write made a shard", got, want)
			}
			for name, bs := range map[string]*BadgerStore{
				"reference": f.ts.refShard, "2026-07-23": eventShardStoreLocked(f.ts, "2026-07-23"), "2026-07-27": f.ts.hotShard.store,
			} {
				if bs == nil {
					continue // closed (the handle sweep closes a demoted shard): nothing buffered
				}
				if n := bs.PendingWriteCount(); n != 0 {
					t.Fatalf("shard %s buffers %d writes of the refused write", name, n)
				}
			}

			// Close reports the fault; every row committed before survives a reopen
			// and the refused write left no row and no change-log record.
			if cerr := f.ts.Close(); !errors.Is(cerr, badgerv4.ErrDBClosed) {
				t.Fatalf("Close = %v, want the sticky fault", cerr)
			}
			re := openMintStore(t, f.cfg)
			nodes, err := re.AllNodes(QueryOpts{Depth: DepthAll})
			if err != nil {
				t.Fatal(err)
			}
			got := make([]types.NodeID, len(nodes))
			for i, n := range nodes {
				got[i] = n.ID()
			}
			want := []types.NodeID{f.event, f.refA, f.refB}
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("nodes after reopen = %v, want the committed %v", got, want)
			}
			if n, err := re.RelationshipCount(); err != nil || n != 1 {
				t.Fatalf("rels after reopen = %d, %v; want the one committed", n, err)
			}
			if _, err := re.GetRelationship(f.rel); err != nil {
				t.Fatalf("committed rel lost: %v", err)
			}
			feed, err := re.ChangeFeed(0, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !equalLSNs(lsns(feed), f.feed) {
				t.Fatalf("change feed after reopen = %v, want %v: the refused write left a record", lsns(feed), f.feed)
			}
			for _, rec := range feed {
				if rec.Tag != storecontract.ChangeNodePut && rec.Tag != storecontract.ChangeRelPut {
					t.Fatalf("unexpected change record %v", rec.Tag)
				}
			}
			for _, id := range c.absent {
				if _, err := re.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
					t.Fatalf("GetNode(%d) after reopen = %v, want ErrNodeNotFound", id, err)
				}
			}
		})
	}
}

// A store already fenced refuses the next write at its first step: no rotation
// and no window shard are made for a write that cannot commit (R1).
func TestEmptyShardDemotion_FencedStoreMakesNothingForAWrite(t *testing.T) {
	f := newCloseFaultStore(t)
	trigger := types.NewNode(types.NodeID(vtID(vtCounter(julDay(27, 12)))), mintSignalTok, nil)
	if err := f.failWrite(t, func() error { return f.ts.PutNode(trigger) }); err == nil {
		t.Fatal("the triggering write returned nil")
	}
	shards := f.eventShardNames()
	writes := []struct {
		name string
		day  int
	}{{"backfill window", 25}, {"next rotation", 29}}
	for _, w := range writes {
		f.clock.set(julDay(w.day, 12))
		node := types.NewNode(types.NodeID(vtID(vtCounter(julDay(w.day, 12)))), mintSignalTok, nil)
		if err := f.ts.PutNode(node); !errors.Is(err, badgerv4.ErrDBClosed) {
			t.Fatalf("%s: write under the fence = %v, want the sticky fault", w.name, err)
		}
		if got := f.eventShardNames(); !slices.Equal(got, shards) || f.ts.hotShard.name != "2026-07-27" {
			t.Fatalf("%s: shards = %v (hot %s), want %v unchanged", w.name, got, f.ts.hotShard.name, shards)
		}
	}
}

// A fault recorded after checkRotation passed (another goroutine's close failed
// meanwhile) still stops the window shard a write would make.
func TestEmptyShardDemotion_FenceStopsWindowShardMaking(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts := openMintStore(t, tierClockCfg(t.TempDir(), clock))
	fault := errors.New("recorded after the write's first check")
	ts.recordBackgroundError(fault)
	raw := types.NodeID(vtID(vtCounter(julDay(21, 12)))).SnowflakeID()
	if err := ts.ensureWindowShard(raw); !errors.Is(err, fault) {
		t.Fatalf("ensureWindowShard under the fence = %v, want the fault", err)
	}
	ts.mu.RLock()
	_, made := ts.eventShards["2026-07-21"]
	ts.mu.RUnlock()
	if made {
		t.Fatal("a fenced store made the window shard")
	}
}

func eventShardStoreLocked(ts *Store, name string) *BadgerStore {
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	return es.store
}
