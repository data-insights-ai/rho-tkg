package tiered

import (
	"testing"
	"time"
)

// coldOpensOf is the shard's ColdOpens as ListShards reports it.
func coldOpensOf(t *testing.T, ts *Store, name string) int64 {
	t.Helper()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	for _, si := range infos {
		if si.Name == name {
			return si.ColdOpens
		}
	}
	t.Fatalf("no shard %s in the listing", name)
	return 0
}

// TestColdOpensCountsEachOpenOfAClosedShard pins SHARD step 8: a read of a
// closed cold shard opens it and counts once. With MaxOpenColdShards the
// shard is retained and a second read counts nothing, until the idle sweep
// closes it and the next read counts again; without it every read opens the
// shard again and counts. The listing itself opens nothing. Catches an open
// not counted, a retained shard counted per read and a listing that opens
// shards.
func TestColdOpensCountsEachOpenOfAClosedShard(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name    string
		maxOpen int
		want    [3]int64 // after read 1, read 2, and a read after the idle close
	}{
		{"retained", 1, [3]int64{1, 1, 2}},
		{"not retained", 0, [3]int64{1, 2, 3}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
			cfg.MaxOpenColdShards = c.maxOpen
			ts, _, n, old, _ := closedColdFixtureWith(t, cfg)
			defer func() { _ = ts.Close() }()
			if got := coldOpensOf(t, ts, old); got != 0 {
				t.Fatalf("ColdOpens before a read = %d, want 0", got)
			}
			if got := coldOpensOf(t, ts, old); got != 0 {
				t.Fatalf("ColdOpens after two listings = %d, want 0", got)
			}
			for read := 0; read < 3; read++ {
				if read == 2 {
					ts.SetIdleTimeoutForTest(time.Millisecond)
					time.Sleep(5 * time.Millisecond)
					ts.CloseIdleShardsForTest()
					if ts.EventShardsForTest()[old].Store() != nil {
						t.Fatal("the idle sweep left the cold shard open")
					}
				}
				if _, err := ts.GetNode(n.ID()); err != nil {
					t.Fatalf("GetNode %d: %v", read+1, err)
				}
				if got := coldOpensOf(t, ts, old); got != c.want[read] {
					t.Fatalf("ColdOpens after read %d = %d, want %d", read+1, got, c.want[read])
				}
			}
		})
	}
}
