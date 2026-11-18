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
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// moveFixture is a store with a warm volume holding one event node in a
// rotated-out shard, demoted to cold and closed. It returns the store, its
// config, the node, the shard's name and the label token.
func moveFixture(t *testing.T) (*Store, Config, *types.Node, string, uint16) {
	t.Helper()
	return closedColdFixtureWith(t, volumeCfg(t.TempDir(), t.TempDir(), ""))
}

// closedColdFixtureWith is moveFixture with the given config.
func closedColdFixtureWith(t *testing.T, cfg Config) (*Store, Config, *types.Node, string, uint16) {
	t.Helper()
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

// TestMoveClosedShard_CrashMatrix pins the closed-shard move: a closed cold shard
// moves to the warm volume; a crash after the copy, the rename, the catalog
// save or the removal leaves, after a reopen, exactly one copy of the shard,
// on the volume the catalog names (the old one before the catalog save, the
// new one from it on), with its row readable. Catches a copy left beside the
// shard (orphans not removed at open), a removed source before the commit and
// a catalog naming a volume without the shard.
func TestMoveClosedShard_CrashMatrix(t *testing.T) {
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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

// TestMoveReadsGoOnFromTheSource pins MOVEREAD for a closed cold shard: a
// read of the shard during its copy returns its row at once (from a
// read-only handle on the source) instead of waiting for the move; after the
// move the shard is closed and only the copy on the new volume is left.
// Catches a read held for the whole copy and a lazy writable open of the
// source during the move (left open over a removed directory).
func TestMoveReadsGoOnFromTheSource(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, cfg, n, old, _ := moveFixture(t)
	defer func() { _ = ts.Close() }()
	release := make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold() // a failure ends the move before Close waits for it
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
		if err != nil {
			t.Fatalf("read during the move: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a read of the shard waited for its move")
	}
	unhold()
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	if ts.EventShardsForTest()[old].Store() != nil {
		t.Fatal("the moved cold shard is open: a read opened its source during the move")
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.WarmDir, "events", old) {
		t.Fatalf("shard copies %v, want only the one on the warm volume", got)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode after the move: %v", err)
	}
}

// TestMoveReadsGoOnForAWarmShard pins MOVEREAD for an open warm shard: during
// its copy a by-ID read and a label scan return its row at once, while a
// write to it waits (TestMoveWarmShard_WriterWaitsAndWriteLandsOnNewVolume).
func TestMoveReadsGoOnForAWarmShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, _, n, _, old, tok := warmMoveFixture(t)
	defer func() { _ = ts.Close() }()
	release := make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold() // a failure ends the move before Close waits for it
	reached := holdMoveAt(ts, "copied", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	read := make(chan error, 1)
	go func() {
		if _, err := ts.GetNode(n.ID()); err != nil {
			read <- err
			return
		}
		nodes, err := ts.NodesByLabel(tok, QueryOpts{})
		if err == nil && len(nodes) != 1 {
			err = fmt.Errorf("NodesByLabel = %d nodes, want 1", len(nodes))
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("reads during the move: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a read of the warm shard waited for its move")
	}
	unhold()
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
}

// TestMoveSourceRemovalWaitsForReaders pins that the move removes its source
// only after the readers holding the source's read-only handle finish (and the
// handle is closed). Catches the source deleted under a read.
func TestMoveSourceRemovalWaitsForReaders(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, cfg, n, old, _ := moveFixture(t)
	defer func() { _ = ts.Close() }()
	release := make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold() // a failure ends the move before Close waits for it
	reached, saved := make(chan struct{}), make(chan struct{})
	var held, removedWhileHeld atomic.Bool
	ts.SetMoveFaultForTest(func(step string) error {
		switch step {
		case "copied":
			close(reached)
			<-release
		case "saved": // committed; next the move ends reads and removes the source
			close(saved)
		case "removed":
			removedWhileHeld.Store(held.Load())
		}
		return nil
	})
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	store, unpin, err := ts.EventShardsForTest()[old].checkoutStoreReadOnly(ts)
	if err != nil {
		t.Fatalf("checkout during the move: %v", err)
	}
	held.Store(true)
	unhold()
	// The move reaches its removal step while the reader holds the source.
	awaitHold(t, saved, moved)
	// No hook inside the wait for readers: a short window shows the move does
	// not pass it (the "removed" step also records whether the reader held on).
	select {
	case err := <-moved:
		t.Fatalf("the move ended while a reader held its source (err %v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "events", old)); err != nil {
		t.Fatalf("the source went while a reader held it: %v", err)
	}
	if _, err := store.GetNode(n.ID()); err != nil {
		t.Fatalf("read from the held source: %v", err)
	}
	held.Store(false)
	unpin()
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	if removedWhileHeld.Load() {
		t.Fatal("the move removed its source while a reader held it")
	}
	if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.WarmDir, "events", old) {
		t.Fatalf("shard copies %v, want only the one on the warm volume", got)
	}
}

// TestCloseStopsMove: Close during a move stops it before its catalog save;
// the shard stays on its volume and nothing is left behind. Catches a move
// that runs on while Close waits for it.
func TestCloseStopsMove(t *testing.T) {
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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

// TestMoveWarmShard_WriterWaitsAndWriteLandsOnNewVolume pins the warm-shard move: an
// open warm shard moves; a write to it during the move waits, then lands on
// the new volume; the shard is open again (warm) after the move, and after a
// reopen both rows read back from the warm volume. Catches a warm shard
// refused, a write taken by the old copy during the move (lost with it) and a
// shard left closed.
func TestMoveWarmShard_WriterWaitsAndWriteLandsOnNewVolume(t *testing.T) {
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	// Wait until the create has begun: it holds tempIdxMu for its whole fan-out
	// and cannot finish while the move holds the shard closed, so once the
	// mutex is seen held it meets the shard during the move.
	awaitCreateStarted := func() {
		for {
			select {
			case err := <-created:
				created <- err // finished without meeting the closed shard
				return
			default:
			}
			if !ts.tempIdxMu.TryLock() {
				return
			}
			ts.tempIdxMu.Unlock()
			runtime.Gosched()
		}
	}
	awaitCreateStarted()
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

// TestSweepMovesWarmToWarmDirAndColdToColdDir pins the sweep move: each idle
// sweep tick moves one shard, the oldest first, to its tier's volume (a cold
// shard to the cold volume, a warm one to the warm volume); the hot shard
// stays, and a shard already on a slower volume than its tier's is left
// there. Catches a sweep moving every shard at once, a shard sent to the
// wrong volume and a manual move undone.
func TestSweepMovesWarmToWarmDirAndColdToColdDir(t *testing.T) {
	testtier.Slow(t)
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

// TestMoverErrorIsNotStoreWide pins the sweep move's error scope: a sweep move that fails
// leaves the shard where it was and the store working (no background error,
// writes and reads go on); the next tick moves another shard, the one after
// retries the failed one. Catches a mover error that fails the store and a
// failing shard that blocks the others.
func TestMoverErrorIsNotStoreWide(t *testing.T) {
	testtier.Slow(t)
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
	testtier.Slow(t)
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

// TestCloseStoppingWarmMoveLeavesShardCold pins that a warm shard whose move
// Close stopped (so it is not reopened) is cold in memory: its accesses take
// the locked path, which sees the store closed or opens it, never the warm
// fast path with no store (a nil dereference for a write or a listing in the
// moment before Close marks the store closed). Its catalog tier stays warm.
func TestCloseStoppingWarmMoveLeavesShardCold(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, cfg, n, _, old, _ := warmMoveFixture(t)
	release := make(chan struct{})
	reached := holdMoveAt(ts, "copied", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()
	for !ts.ClosingForTest() {
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-moved
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if tier := ts.EventShardsForTest()[old].Tier(); tier != TierCold {
		t.Fatalf("shard whose move Close stopped is %s in memory, want cold", tier)
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	if tier := ts.EventShardsForTest()[old].Tier(); tier != TierWarm {
		t.Fatalf("shard reopens %s, want warm (the catalog keeps its tier)", tier)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode after the stopped move: %v", err)
	}
}

// wantDetachedAndFenced asserts what a close fault on es's installed handle
// leaves (CLOSEFAULT, the design's (2) to (4)): the handle is detached, the
// store fenced with the fault, and es's read and write checkouts refused.
func wantDetachedAndFenced(t *testing.T, ts *Store, es *EventShard, fault error) {
	t.Helper()
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("the closed handle stays installed after its close failed")
	}
	if err := ts.backgroundError(); !errors.Is(err, fault) {
		t.Fatalf("background error = %v, want the close fault (the store fenced)", err)
	}
	if _, release, err := es.checkoutStoreReadOnly(ts); err == nil {
		release()
		t.Fatal("a read pinned the shard after its close failed")
	}
	if _, err := es.CheckoutStoreForTest(ts); err == nil {
		es.CheckinStoreForTest()
		t.Fatal("a write pinned the shard after its close failed")
	}
}

// wantCloseFaultFenced is wantDetachedAndFenced for a call that returned err
// (the design's (1) to (6)): err is the fault, no cold handle stays counted,
// and RecoverBackgroundError lifts the fence so every id reads again.
func wantCloseFaultFenced(t *testing.T, ts *Store, es *EventShard, err, fault error, ids ...types.NodeID) {
	t.Helper()
	if !errors.Is(err, fault) {
		t.Fatalf("the call = %v, want the close fault", err)
	}
	wantDetachedAndFenced(t, ts, es, fault)
	if n := ts.openColdShards.Load(); n != 0 {
		t.Fatalf("open cold shards %d after the failed close, want 0 (the detached handle uncounted)", n)
	}
	if err := ts.RecoverBackgroundError(); err != nil {
		t.Fatalf("RecoverBackgroundError: %v", err)
	}
	for _, id := range ids {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("GetNode %v after RecoverBackgroundError: %v", id, err)
		}
	}
}

// wantColdInMemory asserts a warm shard whose close failed is cold in memory
// while the catalog keeps it warm (a restart opens it warm).
func wantColdInMemory(t *testing.T, ts *Store, es *EventShard) {
	t.Helper()
	if tier := es.Tier(); tier != TierCold {
		t.Fatalf("the warm shard whose close failed is %s in memory, want cold", tier)
	}
	if e, _ := ts.CatalogForTest().GetShard(es.name); e.Tier != TierWarm {
		t.Fatalf("catalog tier %s, want warm (untouched)", e.Tier)
	}
}

// reopenAndRead closes ts, opens the store again on cfg and reads every id
// (the design's (7)). The reopened store closes at the test's end.
func reopenAndRead(t *testing.T, ts *Store, cfg Config, ids ...types.NodeID) *Store {
	t.Helper()
	_ = ts.Close()
	re, err := New(cfg)
	if err != nil {
		t.Fatalf("New after the failed close: %v", err)
	}
	t.Cleanup(func() { _ = re.Close() })
	for _, id := range ids {
		if _, err := re.GetNode(id); err != nil {
			t.Fatalf("GetNode %v after the reopen: %v", id, err)
		}
	}
	return re
}

// openColdHandle settles the handle sweep and, when es's cold shard is
// closed, opens it through a checkout so a counted handle is installed
// (TestMoveOpenColdShardStaysClosed). It returns the installed handle.
func openColdHandle(t *testing.T, ts *Store, es *EventShard) *BadgerStore {
	t.Helper()
	ts.SweepHandlesForTest()
	if s, _ := es.StoreStateForTest(); s == nil {
		if _, err := es.CheckoutStoreForTest(ts); err != nil {
			t.Fatal(err)
		}
		es.CheckinStoreForTest()
	}
	s, _ := es.StoreStateForTest()
	if s == nil {
		t.Fatal("no handle installed on the cold shard")
	}
	return s
}

// TestMoveShard_FailedCloseDetachesAndFences (CLOSEFAULT): MoveShard closes
// the shard's installed handle and Close fails. Close may have closed the DB
// whatever it returned, so the handle is detached and the store fenced, as
// the idle close does; nothing moves, a warm shard is cold in memory (the
// catalog keeps warm), and every row reads after RecoverBackgroundError and
// after a reopen. Catches the closed handle left installed with no fence: a
// warm shard's reads fail with ErrStoreClosed until a restart, and the retry
// moves a shard whose final flush failed with no fault recorded.
func TestMoveShard_FailedCloseDetachesAndFences(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, warm := range []bool{false, true} {
		name := "cold-open"
		if warm {
			name = "warm"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var (
				ts  *Store
				cfg Config
				n   *types.Node
				old string
			)
			if warm {
				ts, cfg, n, _, old, _ = warmMoveFixture(t)
			} else {
				ts, cfg, n, old, _ = moveFixture(t)
			}
			t.Cleanup(func() { _ = ts.Close() })
			es := ts.EventShardsForTest()[old]
			var handle *BadgerStore
			if warm {
				ts.SweepHandlesForTest() // the warm-size swap settles first
				handle, _ = es.StoreStateForTest()
			} else {
				handle = openColdHandle(t, ts, es)
			}
			fault := errors.New("injected close fault")
			handle.FailNextCloseForTest(fault)
			err := ts.MoveShard(context.Background(), old, VolumeWarm)
			if warm {
				wantColdInMemory(t, ts, es)
			}
			if got := shardCopies(t, cfg, old); len(got) != 1 || got[0] != filepath.Join(cfg.DataDir, "events", old) {
				t.Fatalf("shard copies %v after the failed close, want only the source", got)
			}
			wantCloseFaultFenced(t, ts, es, err, fault, n.ID())
			wantVolumes(t, ts, nil) // ListShards refuses under the fence
			reopenAndRead(t, ts, cfg, n.ID())
		})
	}
}

// TestMoverCloseFaultFencesStore (CLOSEFAULT): the idle mover's close of the
// shard it picks fails. The store is fenced and the handle detached, as the
// idle close on the same loop does; the shard does not move. SHARD step 6
// (TestMoverErrorIsNotStoreWide) is about a failed copy and stays.
func TestMoverCloseFaultFencesStore(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, _, names := sweepFixture(t)
	es := ts.EventShardsForTest()[names[0]] // the oldest: the sweep picks it first
	fault := errors.New("injected close fault")
	openColdHandle(t, ts, es).FailNextCloseForTest(fault)
	ts.sweepMoveOne()
	wantDetachedAndFenced(t, ts, es, fault)
	if err := ts.RecoverBackgroundError(); err != nil { // ListShards refuses under the fence
		t.Fatalf("RecoverBackgroundError: %v", err)
	}
	wantVolumes(t, ts, nil)
}
