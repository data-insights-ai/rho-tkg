package tiered

import (
	"encoding/binary"
	"fmt"
	"sync"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.CommitFloorCapability = (*Store)(nil)

// Commit floor (store.CommitFloorCapability, backlog 44). The reference shard
// persists its own floor with its rows. Every other shard (hot, warm, cold,
// archive) hands its floor to persistShardCommitFloor BEFORE its row batch
// (badger.Config.OnCommitFloor), which keeps the largest one on the reference
// shard: an open reads that one key and never opens a cold shard, yet a write
// into a cold shard (an update of an old event) is covered.

// shardCommitFloorKey is the reference-shard meta entry holding the largest
// floor any non-reference shard has persisted.
const shardCommitFloorKey = "tx_commit_floor_shards"

// shardCommitFloor tracks shardCommitFloorKey: written is the value in the
// reference shard, synced the part of it known fsynced.
type shardCommitFloor struct {
	mu      sync.Mutex
	written types.Instant
	synced  types.Instant
}

// loadShardCommitFloor reads the persisted value at open; a malformed value
// fails the open.
func (ts *Store) loadShardCommitFloor() error {
	raw, err := ts.refShard.MetaGet(shardCommitFloorKey)
	if err != nil {
		return fmt.Errorf("graph: read shard commit floor: %w", err)
	}
	if raw == nil {
		return nil
	}
	if len(raw) != 8 {
		return fmt.Errorf("graph: shard commit floor: value size %d, want 8", len(raw))
	}
	v := int64(binary.BigEndian.Uint64(raw)) // #nosec G115 -- inverse of the persisted encoding
	if v < 0 {
		return fmt.Errorf("graph: shard commit floor: negative value %d", v)
	}
	ts.shardFloor.written, ts.shardFloor.synced = types.Instant(v), types.Instant(v)
	return nil
}

// persistShardCommitFloor is every non-reference shard's
// badger.Config.OnCommitFloor: it raises the reference shard's entry to floor
// before that shard's rows can commit. Inside a durable flush (durable) the
// entry is fsynced before it returns (MetaSetDurable), so the event rows a
// DurableCommit makes durable never outlive their floor on a power loss; a
// background flush writes it without an fsync. Monotone under its mutex, so
// concurrent shard flushes never lower it.
func (ts *Store) persistShardCommitFloor(floor types.Instant, durable bool) error {
	ts.shardFloor.mu.Lock()
	defer ts.shardFloor.mu.Unlock()
	f := &ts.shardFloor
	target := max(floor, f.written)
	if floor <= f.written && (!durable || floor <= f.synced) {
		return nil
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(target)) // #nosec G115 -- a stored stamp, non-negative
	if durable {
		if err := ts.refShard.MetaSetDurable(shardCommitFloorKey, buf); err != nil {
			return err
		}
		f.synced = target
	} else if err := ts.refShard.MetaSet(shardCommitFloorKey, buf); err != nil {
		return err
	}
	f.written = target
	return nil
}

// resetShardCommitFloor follows a Clear of the reference shard, which wiped the
// entry: the next shard flush with rows writes it again.
func (ts *Store) resetShardCommitFloor() {
	ts.shardFloor.mu.Lock()
	ts.shardFloor.written, ts.shardFloor.synced = 0, 0
	ts.shardFloor.mu.Unlock()
}

// StoredCommitFloor implements store.CommitFloorCapability: the larger of the
// reference shard's own floor and the one every other shard persisted on it.
func (ts *Store) StoredCommitFloor() (types.Instant, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	ref, err := ts.refShard.StoredCommitFloor()
	if err != nil {
		return 0, err
	}
	ts.shardFloor.mu.Lock()
	shards := ts.shardFloor.written
	ts.shardFloor.mu.Unlock()
	return max(ref, shards), nil
}
