package sharded

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
)

// goid is the running goroutine's ID, from its stack header.
func goid() string {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	return string(bytes.Fields(buf)[1])
}

// TestRound11ShardPoolCallerWorks (round 11): the caller of runShardPool is
// one of its workers, so a fan-out runs at least one shard body on the
// caller's already-grown stack (a fresh goroutine copies its stack as a deep
// shard body grows it: a third of a two-shard string seek before). Every
// index still runs once, at most maxShardWorkers at a time.
func TestRound11ShardPoolCallerWorks(t *testing.T) {
	t.Parallel()
	caller := goid()
	runShardPool(0, func(i int) { t.Fatalf("n=0: fn(%d) ran", i) })
	for _, n := range []int{1, 2, 7, 32} {
		var mu sync.Mutex
		ran := make([]int, n)
		onCaller := 0
		runShardPool(n, func(i int) {
			mu.Lock()
			defer mu.Unlock()
			ran[i]++
			if goid() == caller {
				onCaller++
			}
		})
		for i, c := range ran {
			if c != 1 {
				t.Fatalf("n=%d: index %d ran %d times", n, i, c)
			}
		}
		if onCaller == 0 {
			t.Errorf("n=%d: no shard body ran on the caller's goroutine", n)
		}
	}
}
