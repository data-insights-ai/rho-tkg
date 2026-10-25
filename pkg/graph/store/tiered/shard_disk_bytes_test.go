//go:build unix

package tiered

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// allocatedBytes is what du counts: the blocks each regular file under dir
// holds, so badger's sparse preallocations (the open value log, the memtable
// WAL) count only what was written.
func allocatedBytes(t *testing.T, dir string) (alloc, apparent int64) {
	t.Helper()
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		alloc += fi.Sys().(*syscall.Stat_t).Blocks * 512
		apparent += fi.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return alloc, apparent
}

// closedColdFixture is shardDirFixture with the old shard demoted to cold
// and closed.
func closedColdFixture(t *testing.T) (*Store, string, *EventShard, string) {
	t.Helper()
	ts, dir, _, old := shardDirFixture(t)
	demoteToCold(ts, old)
	cold := ts.EventShardsForTest()[old]
	cold.LockShardMuForTest()
	if err := cold.Store().Close(); err != nil {
		cold.UnlockShardMuForTest()
		t.Fatalf("close cold store: %v", err)
	}
	cold.SetStoreForTest(nil)
	cold.UnlockShardMuForTest()
	return ts, dir, cold, old
}

// TestListShardsDiskBytesMatchesDirectory pins SHARD step 2: every shard in
// the listing names its volume and directory, and its bytes on disk are the
// blocks its files hold there — for the open hot shard (whose sparse value log
// is far larger than what it holds), the open reference shard and a closed
// cold shard. Catches a listing without paths or sizes, a path that is not the
// shard's directory and apparent file sizes counted as bytes on disk.
func TestListShardsDiskBytesMatchesDirectory(t *testing.T) {
	t.Parallel()
	ts, dir, cold, old := closedColdFixture(t)
	hot := ts.HotShardForTest().Name()

	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	want := map[string]string{
		"reference": filepath.Join(dir, "reference"),
		hot:         filepath.Join(dir, ts.EventShardsForTest()[hot].Path()),
		old:         filepath.Join(dir, cold.Path()),
	}
	for _, si := range infos {
		path, ok := want[si.Name]
		if !ok {
			continue
		}
		delete(want, si.Name)
		if si.Volume != VolumeHot || si.Path != path {
			t.Errorf("%s: volume %q path %q, want %q %q", si.Name, si.Volume, si.Path, VolumeHot, path)
		}
		alloc, apparent := allocatedBytes(t, path)
		if si.DiskBytes != alloc || alloc <= 0 {
			t.Errorf("%s: DiskBytes = %d, want the %d bytes its files hold (apparent size %d)", si.Name, si.DiskBytes, alloc, apparent)
		}
		if si.Name == hot && apparent < 2*alloc {
			t.Errorf("hot shard: apparent size %d not above its allocation %d; the sparse value log this test relies on is gone", apparent, alloc)
		}
	}
	if len(want) != 0 {
		t.Fatalf("shards missing from the listing: %v", want)
	}
}

// TestListShardsInMemoryHasNoDisk: an in-memory store's shards have no
// volume, path or bytes on disk.
func TestListShardsInMemoryHasNoDisk(t *testing.T) {
	t.Parallel()
	ts, err := New(Config{InMemory: true, RefLabels: []string{"Case"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = ts.Close() }()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	for _, si := range infos {
		if si.Volume != "" || si.Path != "" || si.DiskBytes != 0 {
			t.Errorf("%s: volume %q path %q bytes %d, want none", si.Name, si.Volume, si.Path, si.DiskBytes)
		}
	}
}

// TestListShardsMissingDirIsUnknownBytes: a closed cold shard whose directory
// is gone (a lost volume) lists with DiskBytes -1, the listing still answers
// and the directory is not recreated. Catches a missing directory listed as 0
// bytes (an empty shard) or failing the whole listing.
func TestListShardsMissingDirIsUnknownBytes(t *testing.T) {
	t.Parallel()
	ts, dir, cold, old := closedColdFixture(t)
	shardPath := filepath.Join(dir, cold.Path())
	if err := os.RemoveAll(shardPath); err != nil {
		t.Fatal(err)
	}
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	found := false
	for _, si := range infos {
		if si.Name == old {
			found = true
			if si.DiskBytes != -1 || si.Path != shardPath {
				t.Fatalf("missing shard: DiskBytes %d path %q, want -1 %q", si.DiskBytes, si.Path, shardPath)
			}
		}
	}
	if !found {
		t.Fatalf("shard %s missing from the listing", old)
	}
	if _, err := os.Stat(shardPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the listing recreated %s (stat err %v)", shardPath, err)
	}
}
