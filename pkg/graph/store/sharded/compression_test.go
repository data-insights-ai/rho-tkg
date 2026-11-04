package sharded

import (
	"testing"

	"github.com/dgraph-io/badger/v4/options"
)

// TestNoCompression_ReachesEveryShard pins that Config.NoCompression reaches
// every slot's badger, and that a non-zero Compression beside it is refused.
func TestNoCompression_ReachesEveryShard(t *testing.T) {
	t.Parallel()
	s, err := New(Config{InMemory: true, SlotCount: 3, NoCompression: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	if len(s.shards) != 3 {
		t.Fatalf("opened %d shards, want 3", len(s.shards))
	}
	for i, shard := range s.shards {
		if got := shard.DBForTest().Opts().Compression; got != options.None {
			t.Errorf("shard %d badger codec = %v, want options.None", i, got)
		}
	}

	if bad, err := New(Config{InMemory: true, SlotCount: 2, NoCompression: true, Compression: options.ZSTD}); err == nil {
		_ = bad.Close()
		t.Fatal("New accepted NoCompression with Compression ZSTD")
	}
}
