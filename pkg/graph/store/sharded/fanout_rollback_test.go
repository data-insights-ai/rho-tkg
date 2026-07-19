package sharded

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// BACKLOG 20b: fanOutUniform (the DDL fan-out primitive backing
// CreatePropertyIndex/CreateCompositePropertyIndex/CreateHighFrequencyIndex/
// CreateRelPropertyIndex/CreateTemporalIndex/CreateVectorIndexWithOptions) had
// no rollback on partial shard failure — a mid-build I/O failure left the
// index built on N-1 shards and absent on one, with no reconciliation path;
// for vector indexes this additionally orphaned per-shard disk state, since
// the store-level vectorDefs map was only updated after the WHOLE fan-out
// succeeded. fanOutUniformCreate closes this: on any non-nil overall result,
// every shard whose create succeeded is rolled back via the matching drop.
//
// These tests exercise fanOutUniformCreate directly with synthetic do/
// rollback closures (not tied to any specific index type) so the FIX
// MECHANISM itself is proven once, covering all 6 real callers that route
// through it; TestCreatePropertyIndex_PartialFailureRollsBackSucceededShards
// below is the end-to-end proof for one real caller.

var errSeedFailure = errors.New("fanout test: seeded shard failure")
var errRollbackFailure = errors.New("fanout test: seeded rollback failure")

func TestFanOutUniformCreate_NoRollbackOnFullSuccess(t *testing.T) {
	st := newMemStore(t, 0, 4)
	var doCalls, rollbackCalls atomic.Int64

	err := st.fanOutUniformCreate(
		func(*badgerShard) error { doCalls.Add(1); return nil },
		func(*badgerShard) error { rollbackCalls.Add(1); return nil },
	)
	if err != nil {
		t.Fatalf("fanOutUniformCreate = %v, want nil", err)
	}
	if got := doCalls.Load(); got != int64(len(st.shards)) {
		t.Fatalf("do called %d times, want %d", got, len(st.shards))
	}
	if got := rollbackCalls.Load(); got != 0 {
		t.Fatalf("rollback called %d times, want 0 (full success needs no rollback)", got)
	}
}

func TestFanOutUniformCreate_UniformFailureNoRollbackNeeded(t *testing.T) {
	st := newMemStore(t, 0, 4)
	var rollbackCalls atomic.Int64

	err := st.fanOutUniformCreate(
		func(*badgerShard) error { return errSeedFailure }, // every shard fails identically
		func(*badgerShard) error { rollbackCalls.Add(1); return nil },
	)
	if !errors.Is(err, errSeedFailure) {
		t.Fatalf("fanOutUniformCreate = %v, want errSeedFailure", err)
	}
	if got := rollbackCalls.Load(); got != 0 {
		t.Fatalf("rollback called %d times, want 0 (nothing succeeded, nothing to undo)", got)
	}
}

// TestFanOutUniformCreate_RollsBackOnPartialFailure is the core regression:
// some shards succeed, one fails — the succeeded ones must be rolled back,
// and the failure must still be visible in the returned error.
func TestFanOutUniformCreate_RollsBackOnPartialFailure(t *testing.T) {
	st := newMemStore(t, 0, 4)
	failShard := 1
	var rolledBack []int
	var mu sync.Mutex

	err := st.fanOutUniformCreate(
		func(shard *badgerShard) error {
			for i, s := range st.shards {
				if s == shard && i == failShard {
					return errSeedFailure
				}
			}
			return nil
		},
		func(shard *badgerShard) error {
			mu.Lock()
			defer mu.Unlock()
			for i, s := range st.shards {
				if s == shard {
					rolledBack = append(rolledBack, i)
				}
			}
			return nil
		},
	)
	if !errors.Is(err, errSeedFailure) {
		t.Fatalf("fanOutUniformCreate = %v, want it to wrap errSeedFailure", err)
	}
	mu.Lock()
	got := append([]int(nil), rolledBack...)
	mu.Unlock()
	if len(got) != len(st.shards)-1 {
		t.Fatalf("rolled back %d shards, want %d (every shard except the failed one) — BACKLOG 20b regression", len(got), len(st.shards)-1)
	}
	for _, i := range got {
		if i == failShard {
			t.Fatalf("the FAILED shard %d was rolled back — only succeeded shards should be", failShard)
		}
	}
}

// TestFanOutUniformCreate_RollbackFailureIsJoinedNotSwallowed proves a
// rollback failure surfaces in the returned error rather than being silently
// dropped — an operator must be able to tell "rollback also failed, manual
// reconciliation needed" from "rollback succeeded cleanly".
func TestFanOutUniformCreate_RollbackFailureIsJoinedNotSwallowed(t *testing.T) {
	st := newMemStore(t, 0, 4)

	err := st.fanOutUniformCreate(
		func(shard *badgerShard) error {
			if shard == st.shards[0] {
				return errSeedFailure
			}
			return nil
		},
		func(shard *badgerShard) error {
			if shard == st.shards[1] {
				return errRollbackFailure
			}
			return nil
		},
	)
	if !errors.Is(err, errSeedFailure) {
		t.Fatalf("fanOutUniformCreate = %v, want it to still wrap the original errSeedFailure", err)
	}
	if !errors.Is(err, errRollbackFailure) {
		t.Fatalf("fanOutUniformCreate = %v, want it to ALSO wrap errRollbackFailure (rollback failure must not be swallowed)", err)
	}
}

// TestCreatePropertyIndex_PartialFailureRollsBackSucceededShards is the
// end-to-end proof for a real caller: pre-seeding ONE shard with the index
// directly (simulating residual state from an earlier failed attempt, or
// simply forcing the exact mixed-result shape a genuine partial I/O failure
// would produce) makes a fresh CreatePropertyIndex call fail on that shard
// (ErrIndexExists) while succeeding fresh on the others — the fix must roll
// the others back rather than leave the index inconsistently present.
func TestCreatePropertyIndex_PartialFailureRollsBackSucceededShards(t *testing.T) {
	st := newMemStore(t, 0, 4)
	const labelTok = uint16(10)
	const key = "email"

	if err := st.shards[1].CreatePropertyIndex(labelTok, key); err != nil {
		t.Fatalf("seed shard[1]: %v", err)
	}

	if err := st.CreatePropertyIndex(labelTok, key); err == nil {
		t.Fatal("CreatePropertyIndex = nil, want an error (shard 1 already had the index)")
	}

	for i, shard := range st.shards {
		if i == 1 {
			continue // pre-seeded — expected to still have its index
		}
		// A fresh CreatePropertyIndex on this shard must succeed (no
		// leftover index from the rolled-back attempt) — if the rollback
		// didn't run, this would fail with ErrIndexExists instead.
		if err := shard.CreatePropertyIndex(labelTok, key); err != nil {
			t.Fatalf("shard[%d] still has the index after the failed CreatePropertyIndex call — BACKLOG 20b regression: %v", i, err)
		}
	}
}
