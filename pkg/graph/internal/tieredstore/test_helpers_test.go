package tieredstore

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
)

// newTestTieredStore creates an in-memory TieredStore with Case/User as
// reference labels. Mirrors the helper in pkg/graph so tests moved into this
// package can construct a TieredStore the same way.
func newTestTieredStore(t *testing.T) *TieredStore {
	t.Helper()
	ts, err := NewTieredStore(TieredStoreConfig{
		InMemory:      true,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, // disable periodic flush
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts
}

// newTestGen creates a snowflake generator for testing. Mirrors the production
// layout (5 node bits, 10 step bits, microsecond precision).
func newTestGen(t *testing.T, nodeID int64) *snowflake.Node {
	t.Helper()
	gen, err := snowflake.NewNode(nodeID,
		snowflake.WithEpoch(storepkg.SnowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return gen
}

// tieredNodeGen and tieredRelGen for test entities.
func tieredNodeGen(t *testing.T) *snowflake.Node {
	t.Helper()
	return newTestGen(t, 0)
}

func tieredRelGen(t *testing.T) *snowflake.Node {
	t.Helper()
	return newTestGen(t, 1)
}
