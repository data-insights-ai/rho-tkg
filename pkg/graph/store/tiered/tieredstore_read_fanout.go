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
