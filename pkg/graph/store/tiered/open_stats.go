package tiered

import (
	"log/slog"
	"time"
)

// OpenStats is what New spent opening the store (RESTART, ai-soc 26 Oct: a
// reopen of a faulted store took 2,115 s and rho's share was unknown). New
// also logs it at Info ("graph: tiered store opened").
type OpenStats struct {
	// Duration is New from its call to its return.
	Duration time.Duration
	// Reference, Hot and Warm are the opens of the reference shard, the hot
	// shard and the warm shards (opened concurrently; their wall time).
	Reference, Hot, Warm time.Duration
	// WarmShards were opened at open; ColdShards are cataloged cold and stay
	// closed until a read or write needs one (OPENCOLD).
	WarmShards, ColdShards int
	// Demoted and Promoted shards changed tier at open (ColdAfter,
	// PromoteColdShardsAtOpen).
	Demoted, Promoted int
	// WALTruncations counts shards whose unclean WAL was cut to open them,
	// at open and on every later cold open since.
	WALTruncations int64
}

// OpenStats returns what New spent opening the store.
func (ts *Store) OpenStats() OpenStats {
	s := ts.openStats
	s.WALTruncations = ts.walTruncations.Load()
	return s
}

// finishOpenStats records the shard counts and the duration once New is done,
// and logs them.
func (ts *Store) finishOpenStats(start time.Time) {
	for _, es := range ts.eventShards {
		switch {
		case es == ts.hotShard:
		case es.currentTier() == TierCold:
			ts.openStats.ColdShards++
		default:
			ts.openStats.WarmShards++
		}
	}
	ts.openStats.Duration = time.Since(start)
	s := ts.OpenStats()
	slog.Info("graph: tiered store opened", "duration", s.Duration, "reference", s.Reference, "hot", s.Hot,
		"warm", s.Warm, "warm_shards", s.WarmShards, "cold_shards", s.ColdShards, "demoted", s.Demoted,
		"promoted", s.Promoted, "wal_truncations", s.WALTruncations)
}
