package tiered

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// moveFixture is a store with a warm volume holding one event node in a
// rotated-out shard, demoted to cold and closed. It returns the store, its
// config, the node, the shard's name and the label token.
func moveFixture(t *testing.T) (*Store, Config, *types.Node, string, uint16) {
	t.Helper()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, old)
	es := ts.EventShardsForTest()[old]
	es.LockShardMuForTest()
	if err := es.Store().Close(); err != nil {
		es.UnlockShardMuForTest()
		t.Fatalf("close cold store: %v", err)
	}
	es.SetStoreForTest(nil)
	es.UnlockShardMuForTest()
	return ts, cfg, n, old, tok
}

// shardCopies lists every directory entry of the shard under each volume's
// events/ (a copy, or a copy being made).
func shardCopies(t *testing.T, cfg Config, name string) []string {
	t.Helper()
	var out []string
	for _, root := range []string{cfg.DataDir, cfg.WarmDir} {
		ents, err := os.ReadDir(filepath.Join(root, "events"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		for _, e := range ents {
			if e.Name() == name || strings.HasPrefix(e.Name(), name+".moving-") {
				out = append(out, filepath.Join(root, "events", e.Name()))
			}
		}
	}
	return out
}

// TestMoveClosedShard_CrashMatrix pins SHARD step 4: a closed cold shard
// moves to the warm volume; a crash after the copy, the rename, the catalog
// save or the removal leaves, after a reopen, exactly one copy of the shard,
// on the volume the catalog names (the old one before the catalog save, the
// new one from it on), with its row readable. Catches a copy left beside the
// shard (orphans not removed at open), a removed source before the commit and
// a catalog naming a volume without the shard.
func TestMoveClosedShard_CrashMatrix(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		crashAfter string
		want       string
	}{
		{"copied", VolumeHot}, {"renamed", VolumeHot}, {"saved", VolumeWarm}, {"removed", VolumeWarm}, {"", VolumeWarm},
	} {
		t.Run("crash after "+c.crashAfter, func(t *testing.T) {
			t.Parallel()
			ts, cfg, n, old, _ := moveFixture(t)
			crash := errors.New("crash")
			ts.SetMoveFaultForTest(func(step string) error {
				if step == c.crashAfter {
					return crash
				}
				return nil
			})
			err := ts.MoveShard(context.Background(), old, VolumeWarm)
			if c.crashAfter == "" && err != nil || c.crashAfter != "" && !errors.Is(err, crash) {
				t.Fatalf("MoveShard = %v", err)
			}
			if err := ts.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			ts, err = New(cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = ts.Close() }()
			if _, err := ts.GetNode(n.ID()); err != nil {
				t.Fatalf("GetNode after the reopen: %v", err)
			}
			root := cfg.DataDir
			if c.want == VolumeWarm {
				root = cfg.WarmDir
			}
			want := filepath.Join(root, "events", old)
			if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != want {
				t.Fatalf("shard copies %v, want only %s", got, want)
			}
			infos, err := ts.ListShards()
			if err != nil {
				t.Fatal(err)
			}
			for _, si := range infos {
				if si.Name == old && (si.Volume != c.want || si.Path != want) {
					t.Fatalf("listing: volume %q path %q, want %q %q", si.Volume, si.Path, c.want, want)
				}
			}
		})
	}
}

// TestMoveShardRefusals: MoveShard refuses an unknown shard, the hot shard
// and an unmapped or unknown volume, and moving a shard to the volume it is
// on does nothing. Each refusal leaves the shard where it was.
func TestMoveShardRefusals(t *testing.T) {
	t.Parallel()
	ts, cfg, _, old, _ := moveFixture(t)
	defer func() { _ = ts.Close() }()
	ctx := context.Background()
	hot := ts.HotShardForTest().Name()
	for name, c := range map[string]struct{ shard, vol, want string }{
		"unknown shard":   {"2001-W01", VolumeWarm, "no event shard"},
		"hot shard":       {hot, VolumeWarm, "hot"},
		"unmapped volume": {old, VolumeCold, "does not map"},
		"unknown volume":  {old, "blue", "not a volume"},
	} {
		if err := ts.MoveShard(ctx, c.shard, c.vol); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: MoveShard = %v, want an error containing %q", name, err, c.want)
		}
	}
	if err := ts.MoveShard(ctx, old, VolumeHot); err != nil {
		t.Fatalf("a move to the shard's own volume = %v, want nil", err)
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.DataDir, "events", old) {
		t.Fatalf("shard copies %v after the refusals, want only the one under DataDir", got)
	}
}

// TestMoveShardCancelledLeavesSource: a move cancelled during the copy
// leaves the shard on its volume, readable, and no copy behind.
func TestMoveShardCancelledLeavesSource(t *testing.T) {
	t.Parallel()
	ts, cfg, n, old, _ := moveFixture(t)
	defer func() { _ = ts.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ts.MoveShard(ctx, old, VolumeWarm); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled MoveShard = %v, want context.Canceled", err)
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.DataDir, "events", old) {
		t.Fatalf("shard copies %v after a cancelled move, want only the source", got)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode after a cancelled move: %v", err)
	}
}

// TestMovedShardReadAndDrop: after a move the shard's files are byte for
// byte the source's, its row reads from the new volume, its bytes on disk are what the source held (sparse files stay
// sparse); a retention drop of the moved shard removes its directory there.
// Catches a drop that resolves the directory after removing the catalog
// entry (it deletes under DataDir and leaves the moved shard behind).
func TestMovedShardReadAndDrop(t *testing.T) {
	t.Parallel()
	ts, cfg, n, old, tok := moveFixture(t)
	defer func() { _ = ts.Close() }()
	before := diskBytesOf(t, ts, old)
	srcFiles := fileDigests(t, filepath.Join(cfg.DataDir, "events", old))
	if err := ts.MoveShard(context.Background(), old, VolumeWarm); err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	if dst := fileDigests(t, filepath.Join(cfg.WarmDir, "events", old)); !equalDigests(srcFiles, dst) {
		t.Fatalf("the moved copy differs from the source:\n%v\n%v", srcFiles, dst)
	}
	if after := diskBytesOf(t, ts, old); after > before {
		t.Errorf("bytes on disk %d after the move, %d before: the copy filled sparse files", after, before)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode from the warm volume: %v", err)
	}
	if _, err := ts.PurgeNodesByLabelBefore(tok, types.Instant(1<<50), 8); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, ok := ts.CatalogForTest().GetShard(old); ok {
		t.Fatal("the moved shard was not dropped")
	}
	if got := shardCopies(t, cfg, old); len(got) != 0 {
		t.Fatalf("shard copies %v after the drop, want none", got)
	}
}

// fileDigests is each file's size and SHA-256 under dir, by relative path.
func fileDigests(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p) // #nosec G304 -- test temp dir
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		sum := sha256.Sum256(data)
		out[rel] = fmt.Sprintf("%d:%s", len(data), hex.EncodeToString(sum[:8]))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func equalDigests(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func diskBytesOf(t *testing.T, ts *Store, name string) int64 {
	t.Helper()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}
	for _, si := range infos {
		if si.Name == name {
			return si.DiskBytes
		}
	}
	t.Fatalf("shard %s not listed", name)
	return 0
}

// holdMoveAt makes the store's moves block after step until release is
// closed, and returns a channel closed when a move reaches it.
func holdMoveAt(ts *Store, step string, release <-chan struct{}) <-chan struct{} {
	reached := make(chan struct{})
	ts.SetMoveFaultForTest(func(s string) error {
		if s == step {
			close(reached)
			<-release
		}
		return nil
	})
	return reached
}

// awaitHold waits until the move reaches its hold, failing when it returns
// first.
func awaitHold(t *testing.T, reached <-chan struct{}, moved <-chan error) {
	t.Helper()
	select {
	case <-reached:
	case err := <-moved:
		t.Fatalf("MoveShard returned before its hold: %v", err)
	}
}

// TestMoveGateHoldsReaders: a read of a shard being moved waits for the move
// and then reads the shard from the new volume. Catches a lazy open during
// the move (it opens the source, which the move then removes).
func TestMoveGateHoldsReaders(t *testing.T) {
	t.Parallel()
	ts, cfg, n, old, _ := moveFixture(t)
	defer func() { _ = ts.Close() }()
	release := make(chan struct{})
	reached := holdMoveAt(ts, "copied", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	read := make(chan error, 1)
	go func() {
		_, err := ts.GetNode(n.ID())
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("a read of the shard returned during its move (err %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	if err := <-read; err != nil {
		t.Fatalf("the held read: %v", err)
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.WarmDir, "events", old) {
		t.Fatalf("shard copies %v, want only the one on the warm volume", got)
	}
}

// TestCloseStopsMove: Close during a move stops it before its catalog save;
// the shard stays on its volume and nothing is left behind. Catches a move
// that runs on while Close waits for it.
func TestCloseStopsMove(t *testing.T) {
	t.Parallel()
	ts, cfg, n, old, _ := moveFixture(t)
	release := make(chan struct{})
	reached := holdMoveAt(ts, "gated", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()
	for !ts.ClosingForTest() {
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-moved; !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("MoveShard during Close = %v, want ErrStoreClosed", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.DataDir, "events", old) {
		t.Fatalf("shard copies %v, want only the source", got)
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode after the stopped move: %v", err)
	}
}

// TestOrphanKeptWhenNamedCopyMissing: a copy the catalog does not name is
// kept at open when the copy it names is missing (a lost volume), since it
// may be the only one left. Catches an orphan removal that deletes it.
func TestOrphanKeptWhenNamedCopyMissing(t *testing.T) {
	t.Parallel()
	ts, cfg, _, old, _ := moveFixture(t)
	ts.SetMoveFaultForTest(func(step string) error {
		if step == "saved" {
			return errors.New("crash")
		}
		return nil
	})
	_ = ts.MoveShard(context.Background(), old, VolumeWarm)
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	lost := filepath.Join(cfg.WarmDir, "events", old)
	if err := os.Rename(lost, lost+"-lost"); err != nil {
		t.Fatal(err)
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "events", old)); err != nil {
		t.Fatalf("the old copy was removed while the catalog's copy is missing: %v", err)
	}
}

// TestMoveRetryAfterCrashBeforeSave: a move that crashed between its rename
// and its catalog save left an uncataloged copy on the target volume; a
// retry in the same process replaces it and completes. Catches a retry that
// fails on the copy left there.
func TestMoveRetryAfterCrashBeforeSave(t *testing.T) {
	t.Parallel()
	ts, cfg, n, old, _ := moveFixture(t)
	defer func() { _ = ts.Close() }()
	ts.SetMoveFaultForTest(func(step string) error {
		if step == "renamed" {
			return errors.New("crash")
		}
		return nil
	})
	_ = ts.MoveShard(context.Background(), old, VolumeWarm)
	ts.SetMoveFaultForTest(nil)
	if err := ts.MoveShard(context.Background(), old, VolumeWarm); err != nil {
		t.Fatalf("the retried move: %v", err)
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.WarmDir, "events", old) {
		t.Fatalf("shard copies %v, want only the one on the warm volume", got)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
}

// warmMoveFixture is a store with a warm volume whose rotated-out shard is
// warm (open), holding one node; it also returns an ID minted in that
// shard's window, not yet written.
func warmMoveFixture(t *testing.T) (*Store, Config, *types.Node, types.NodeID, string, uint16) {
	t.Helper()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), tok, nil)
	later := types.NodeID(gen.Generate())
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	if tier := ts.EventShardsForTest()[old].Tier(); tier != TierWarm {
		t.Fatalf("rotated-out shard is %s, want warm", tier)
	}
	return ts, cfg, n, later, old, tok
}

// TestMoveWarmShard_WriterWaitsAndWriteLandsOnNewVolume pins SHARD step 5: an
// open warm shard moves; a write to it during the move waits, then lands on
// the new volume; the shard is open again (warm) after the move, and after a
// reopen both rows read back from the warm volume. Catches a warm shard
// refused, a write taken by the old copy during the move (lost with it) and a
// shard left closed.
func TestMoveWarmShard_WriterWaitsAndWriteLandsOnNewVolume(t *testing.T) {
	t.Parallel()
	ts, cfg, n, later, old, tok := warmMoveFixture(t)
	release := make(chan struct{})
	reached := holdMoveAt(ts, "copied", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	wrote := make(chan error, 1)
	go func() { wrote <- ts.PutNode(types.NewNode(later, tok, nil)) }()
	select {
	case err := <-wrote:
		t.Fatalf("a write to the shard returned during its move (err %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	if err := <-wrote; err != nil {
		t.Fatalf("the held write: %v", err)
	}
	es := ts.EventShardsForTest()[old]
	if es.Store() == nil || es.Tier() != TierWarm {
		t.Fatalf("after the move the shard is %s, open %v; want warm and open", es.Tier(), es.Store() != nil)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.WarmDir, "events", old) {
		t.Fatalf("shard copies %v, want only the one on the warm volume", got)
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	for _, id := range []types.NodeID{n.ID(), later} {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("GetNode %v after the reopen: %v", id, err)
		}
	}
}

// TestDurableFlushDuringMoveDoesNotWait: a durable flush during a warm
// shard's move returns without waiting for the copy (the shard's close
// synced what it held). Catches durable commits stalled for a whole move.
func TestDurableFlushDuringMoveDoesNotWait(t *testing.T) {
	t.Parallel()
	ts, _, _, _, old, _ := warmMoveFixture(t)
	defer func() { _ = ts.Close() }()
	release := make(chan struct{})
	reached := holdMoveAt(ts, "copied", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	flushed := make(chan error, 1)
	go func() { flushed <- ts.DurableFlush() }()
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("DurableFlush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DurableFlush waited for the move")
	}
	close(release)
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
}

// TestMoveOpenColdShardStaysClosed: a cold shard a read left open moves and
// stays closed afterwards (a cold shard opens on use) with its counts sealed,
// its row still read.
func TestMoveOpenColdShardStaysClosed(t *testing.T) {
	t.Parallel()
	ts, cfg, n, old, _ := moveFixture(t)
	defer func() { _ = ts.Close() }()
	es := ts.EventShardsForTest()[old]
	if _, err := es.CheckoutStoreForTest(ts); err != nil {
		t.Fatal(err)
	}
	es.CheckinStoreForTest()
	if err := ts.MoveShard(context.Background(), old, VolumeWarm); err != nil {
		t.Fatalf("MoveShard of an open cold shard: %v", err)
	}
	if es.Store() != nil {
		t.Fatal("the moved cold shard was reopened")
	}
	if es.cachedCounts() == nil {
		t.Fatal("the move closed the cold shard without sealing its counts (a count would reopen it)")
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.WarmDir, "events", old) {
		t.Fatalf("shard copies %v, want only the one on the warm volume", got)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
}

// TestMovedShardThatDoesNotReopenIsDemoted: a warm shard whose new copy is
// gone when the move reopens it (a volume lost at that moment) is demoted to
// cold; the move reports why, a read of the shard fails with
// fs.ErrNotExist, and the store still takes writes. Catches a warm shard left
// with no store (every checkout of it would get nil).
func TestMovedShardThatDoesNotReopenIsDemoted(t *testing.T) {
	t.Parallel()
	ts, cfg, n, _, old, tok := warmMoveFixture(t)
	defer func() { _ = ts.Close() }()
	ts.SetMoveFaultForTest(func(step string) error {
		if step == "saved" {
			return os.Rename(filepath.Join(cfg.WarmDir, "events", old), filepath.Join(cfg.WarmDir, "lost"))
		}
		return nil
	})
	if err := ts.MoveShard(context.Background(), old, VolumeWarm); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("MoveShard = %v, want fs.ErrNotExist from the reopen", err)
	}
	if tier := ts.EventShardsForTest()[old].Tier(); tier != TierCold {
		t.Fatalf("the shard is %s, want cold", tier)
	}
	if e, _ := ts.CatalogForTest().GetShard(old); e.Tier != TierCold {
		t.Fatalf("catalog tier %s, want cold", e.Tier)
	}
	if _, err := ts.GetNode(n.ID()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetNode on the lost shard = %v, want fs.ErrNotExist", err)
	}
	if err := ts.PutNode(types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)); err != nil {
		t.Fatalf("a write to the hot shard after the failed reopen: %v", err)
	}
}

// TestMoveWaitsForPins: a move of an open shard closes it only once the pin
// taken before the move is returned. Catches a close under a running read or
// write.
func TestMoveWaitsForPins(t *testing.T) {
	t.Parallel()
	ts, _, _, _, old, _ := warmMoveFixture(t)
	defer func() { _ = ts.Close() }()
	es := ts.EventShardsForTest()[old]
	if _, err := es.CheckoutStoreForTest(ts); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	close(release)
	reached := holdMoveAt(ts, "copied", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	select {
	case <-reached:
		t.Fatal("the move copied the shard while it was pinned")
	case err := <-moved:
		t.Fatalf("MoveShard returned while the shard was pinned: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	es.CheckinStoreForTest()
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
}

// TestMovedWarmShardKeepsIndexes: a warm shard reopened after its move
// carries the relationship temporal index (rebuilt in memory) and a temporal
// index created while it was closed for the move. Catches a reopen without
// the index sync an open shard gets.
func TestMovedWarmShardKeepsIndexes(t *testing.T) {
	t.Parallel()
	ts, _, _, _, old, tok := warmMoveFixture(t)
	defer func() { _ = ts.Close() }()
	const relType = uint16(5)
	if err := ts.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	reached := holdMoveAt(ts, "copied", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	created := make(chan error, 1)
	go func() { created <- ts.CreateTemporalIndex(tok) }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	if err := <-created; err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	store := ts.EventShardsForTest()[old].Store()
	if types, err := store.RelTemporalIndexTypes(); err != nil || !slices.Contains(types, relType) {
		t.Fatalf("relationship temporal indexes %v (%v), want %d", types, err, relType)
	}
	if has, _, _, err := store.TemporalIndexState(tok); err != nil || !has {
		t.Fatalf("temporal index on the moved shard: %v (%v)", has, err)
	}
}

// sweepFixture is a store with a warm and a cold volume and two rotated-out
// shards on hot, each holding one node: the older demoted to cold, the newer
// warm. It returns the store, the nodes and the shard names, older first.
func sweepFixture(t *testing.T) (*Store, [2]*types.Node, [2]string) {
	t.Helper()
	ts, err := New(volumeCfg(t.TempDir(), t.TempDir(), t.TempDir()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	var nodes [2]*types.Node
	var names [2]string
	for i := range nodes {
		nodes[i] = types.NewNode(types.NodeID(gen.Generate()), tok, nil)
		if err := ts.PutNode(nodes[i]); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		names[i] = ts.HotShardForTest().Name()
		forceRotation(t, ts)
	}
	demoteToCold(ts, names[0])
	return ts, nodes, names
}

// shardVolumes is each event shard's volume, as ListShards reports it.
func shardVolumes(t *testing.T, ts *Store) map[string]string {
	t.Helper()
	shards, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	out := map[string]string{}
	for _, s := range shards {
		if s.Kind == ShardEvent {
			out[s.Name] = s.Volume
		}
	}
	return out
}

// wantVolumes fails unless the shards are on the given volumes and every
// other event shard (the hot one) is on hot.
func wantVolumes(t *testing.T, ts *Store, want map[string]string) {
	t.Helper()
	for name, vol := range shardVolumes(t, ts) {
		w, ok := want[name]
		if !ok {
			w = VolumeHot
		}
		if vol != w {
			t.Fatalf("shard %s is on %s, want %s (all: %v)", name, vol, w, shardVolumes(t, ts))
		}
	}
}

// TestSweepMovesWarmToWarmDirAndColdToColdDir pins SHARD step 6: each idle
// sweep tick moves one shard, the oldest first, to its tier's volume (a cold
// shard to the cold volume, a warm one to the warm volume); the hot shard
// stays, and a shard already on a slower volume than its tier's is left
// there. Catches a sweep moving every shard at once, a shard sent to the
// wrong volume and a manual move undone.
func TestSweepMovesWarmToWarmDirAndColdToColdDir(t *testing.T) {
	t.Parallel()
	ts, nodes, names := sweepFixture(t)
	ts.sweepMoveOne()
	wantVolumes(t, ts, map[string]string{names[0]: VolumeCold})
	ts.sweepMoveOne()
	wantVolumes(t, ts, map[string]string{names[0]: VolumeCold, names[1]: VolumeWarm})
	for _, n := range nodes {
		if _, err := ts.GetNode(n.ID()); err != nil {
			t.Fatalf("GetNode after the sweep: %v", err)
		}
	}
	if err := ts.MoveShard(context.Background(), names[1], VolumeCold); err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	ts.sweepMoveOne()
	wantVolumes(t, ts, map[string]string{names[0]: VolumeCold, names[1]: VolumeCold})
}

// TestSweepVolume pins the volume the sweep moves a shard of each tier to:
// its tier's own, or the next faster one the config maps.
func TestSweepVolume(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		warm, cold string
		tier       ShardTier
		want       string
	}{
		{"w", "c", TierWarm, VolumeWarm},
		{"w", "c", TierCold, VolumeCold},
		{"w", "", TierCold, VolumeWarm},
		{"", "c", TierWarm, VolumeHot},
		{"", "c", TierCold, VolumeCold},
		{"w", "c", TierHot, VolumeHot},
	} {
		ts := &Store{warmDir: c.warm, coldDir: c.cold}
		if got := volumeName(ts.sweepVolume(c.tier)); got != c.want {
			t.Errorf("warm %q cold %q: a %s shard goes to %s, want %s", c.warm, c.cold, c.tier, got, c.want)
		}
	}
}

// TestMoverErrorIsNotStoreWide pins SHARD step 6: a sweep move that fails
// leaves the shard where it was and the store working (no background error,
// writes and reads go on); the next tick moves another shard, the one after
// retries the failed one. Catches a mover error that fails the store and a
// failing shard that blocks the others.
func TestMoverErrorIsNotStoreWide(t *testing.T) {
	t.Parallel()
	ts, nodes, names := sweepFixture(t)
	failed := false
	ts.SetMoveFaultForTest(func(step string) error {
		if step == "copied" && !failed {
			failed = true
			return errors.New("injected copy failure")
		}
		return nil
	})
	ts.sweepMoveOne()
	if !failed {
		t.Fatal("the sweep did not try a move")
	}
	wantVolumes(t, ts, nil)
	if err := ts.backgroundError(); err != nil {
		t.Fatalf("background error after a failed move: %v", err)
	}
	if _, err := ts.GetNode(nodes[0].ID()); err != nil {
		t.Fatalf("GetNode of the shard that did not move: %v", err)
	}
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), nodes[0].LabelTokenRawAt(0), nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode after a failed move: %v", err)
	}
	ts.sweepMoveOne()
	wantVolumes(t, ts, map[string]string{names[1]: VolumeWarm})
	ts.sweepMoveOne()
	wantVolumes(t, ts, map[string]string{names[0]: VolumeCold, names[1]: VolumeWarm})
}

// TestSweepRunsOnTheIdleLoop pins that the idle loop runs the sweep, and
// that a store with a volume mapped runs the loop by default.
func TestSweepRunsOnTheIdleLoop(t *testing.T) {
	t.Parallel()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := ts.idleTimeoutValue(); got != 5*time.Minute {
		t.Fatalf("IdleTimeout with a warm volume = %v, want the 5m default", got)
	}
	_ = ts.Close()
	cfg.IdleTimeout = 10 * time.Millisecond
	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = ts.Close() }()
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	deadline := time.Now().Add(10 * time.Second)
	for shardVolumes(t, ts)[old] != VolumeWarm {
		if time.Now().After(deadline) {
			t.Fatalf("shard %s still on %s after 10s of idle ticks", old, shardVolumes(t, ts)[old])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
