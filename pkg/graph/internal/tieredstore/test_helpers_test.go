package tieredstore

import (
	"testing"
	"time"
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
