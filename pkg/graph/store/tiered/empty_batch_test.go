package tiered

import (
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestTieredStoreEmptyPutBatchesDoNotRotate(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Store) error
	}{
		{name: "PutNodesBatch nil", run: func(ts *Store) error { return ts.PutNodesBatch(nil) }},
		{name: "PutNodesBatch empty", run: func(ts *Store) error { return ts.PutNodesBatch([]*types.Node{}) }},
		{name: "PutRelationshipsBatch nil", run: func(ts *Store) error { return ts.PutRelationshipsBatch(nil) }},
		{name: "PutRelationshipsBatch empty", run: func(ts *Store) error {
			return ts.PutRelationshipsBatch([]*types.Relationship{})
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestTieredStore(t)
			hotBefore := ts.HotShardForTest().Name()
			ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))

			if err := tc.run(ts); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}

			if hotAfter := ts.HotShardForTest().Name(); hotAfter != hotBefore {
				t.Fatalf("%s rotated hot shard from %q to %q", tc.name, hotBefore, hotAfter)
			}
			ts.MuForTest().RLock()
			shardCount := len(ts.EventShardsForTest())
			ts.MuForTest().RUnlock()
			if shardCount != 1 {
				t.Fatalf("%s changed event shard count to %d, want 1", tc.name, shardCount)
			}
		})
	}
}
