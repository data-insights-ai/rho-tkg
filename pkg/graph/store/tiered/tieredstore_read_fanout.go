package tiered

import "sync"

const maxEventShardQueryParallelism = 16

func queryEventShards(shards []*EventShard, fn func(int, *EventShard)) {
	if len(shards) == 0 {
		return
	}
	var parallel []int
	var cold []int
	for i, shard := range shards {
		if shard != nil && shard.currentTier() == TierCold {
			cold = append(cold, i)
			continue
		}
		parallel = append(parallel, i)
	}
	queryEventShardIndices(shards, parallel, fn)
	for _, i := range cold {
		fn(i, shards[i])
	}
}

// queryEventShardIndices runs fn concurrently over indices (workerCount
// bounded goroutines pulling from a shared job channel).
//
// BACKLOG 19r: the caller (queryEventShards) already partitions shards into
// parallel-safe vs. cold BEFORE calling this function, passing ONLY the
// parallel-safe indices — so the per-index `currentTier() == TierCold` check
// below looks, at a glance, like it can never fire and reads as removable
// dead code. It is NOT dead: it is a genuine (if narrow-window) TOCTOU
// defense. A shard's tier is read again here because time passes between
// the caller's up-front partition and this goroutine actually reaching the
// shard — a concurrent idle-close (closeIdleShards, see tieredstore_
// lifecycle.go) can demote a shard to cold in that window. queryEventShards
// deliberately processes cold shards SEQUENTIALLY (a plain loop, after this
// function returns) rather than on the bounded-worker parallel path — cold
// shards are lazily opened and aggressively idle-closed, so bounding how
// many can be concurrently open at once is the intent. A shard that becomes
// cold mid-flight, caught by this recheck, is routed through coldMu instead
// of the free-for-all parallel path so that intent still holds even under
// the race — not because concurrent access to a single cold shard is
// otherwise known to be unsafe. Do not remove this recheck as "obviously
// already filtered upstream" — the upstream filter's result can be stale by
// the time a worker goroutine gets to it.
func queryEventShardIndices(shards []*EventShard, indices []int, fn func(int, *EventShard)) {
	if len(indices) == 0 {
		return
	}
	workerCount := len(indices)
	if workerCount > maxEventShardQueryParallelism {
		workerCount = maxEventShardQueryParallelism
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	var coldMu sync.Mutex
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for i := range jobs {
				shard := shards[i]
				if shard != nil && shard.currentTier() == TierCold {
					coldMu.Lock()
					fn(i, shard)
					coldMu.Unlock()
					continue
				}
				fn(i, shard)
			}
		}()
	}
	for _, i := range indices {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}
