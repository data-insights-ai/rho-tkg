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

// TestMoveShardRefusals: MoveShard refuses an unknown shard, the hot shard,
// an open shard and an unmapped or unknown volume, and moving a shard to the
// volume it is on does nothing. Each refusal leaves the shard where it was.
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
	es := ts.EventShardsForTest()[old]
	if _, err := es.CheckoutStoreForTest(ts); err != nil {
		t.Fatal(err)
	}
	es.CheckinStoreForTest()
	if err := ts.MoveShard(ctx, old, VolumeWarm); !errors.Is(err, ErrShardOpen) {
		t.Fatalf("moving an open shard = %v, want ErrShardOpen", err)
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
	<-reached
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
	<-reached
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
