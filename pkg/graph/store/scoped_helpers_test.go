package store_test

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// scopedStore is a store with the scoped change-log doors (BACKLOG 11f): the
// scoped_*_test.go battery runs on every backend that implements it.
type scopedStore interface {
	storecontract.Store
	storecontract.ChangeFeedCapability
	storecontract.TxChangeLogScope
	storecontract.ScopedTxCapability
}

// forScopedStores runs fn once per backend with the scoped doors, on a fresh
// store with the change-log on (changeLog) or off.
func forScopedStores(t *testing.T, changeLog bool, fn func(t *testing.T, bs scopedStore)) {
	t.Helper()
	backends := []struct {
		name string
		open func(t *testing.T) scopedStore
	}{
		{"memory", func(t *testing.T) scopedStore {
			if changeLog {
				return memory.New(memory.WithChangeLog())
			}
			return memory.New()
		}},
		{"badger", func(t *testing.T) scopedStore {
			bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: changeLog})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return bs
		}},
	}
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s := b.open(t)
			t.Cleanup(func() { _ = s.Close() })
			fn(t, s)
		})
	}
}

// scopedFlush flushes a store that buffers writes (badger); memory writes are
// immediate.
func scopedFlush(s scopedStore) error {
	if f, ok := s.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

func newScopedNode(id int64, primary uint16) *types.Node {
	return types.NewNode(types.NodeID(snowflake.ID(id)), primary, nil)
}

func newScopedRel(id int64, relType uint16, startID, endID int64) *types.Relationship {
	return types.NewRelationship(types.RelID(snowflake.ID(id)), relType, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
}

func putTestNode(t *testing.T, bs scopedStore, id int64, primary uint16, extras []uint16) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), primary, extras)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
	return n
}

func putTestRel(t *testing.T, bs scopedStore, id int64, relType uint16, startID, endID int64) *types.Relationship {
	t.Helper()
	r := newScopedRel(id, relType, startID, endID)
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
	return r
}

func drainFeed(t *testing.T, bs scopedStore) []storecontract.ChangeRecord {
	t.Helper()
	return afterFeed(t, bs, 0)
}

func afterFeed(t *testing.T, bs scopedStore, after uint64) []storecontract.ChangeRecord {
	t.Helper()
	if err := scopedFlush(bs); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, err := bs.ChangeFeed(after, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	return recs
}

func tagSeq(recs []storecontract.ChangeRecord) []storecontract.ChangeTag {
	out := make([]storecontract.ChangeTag, len(recs))
	for i, r := range recs {
		out[i] = r.Tag
	}
	return out
}

func assertLSNContiguous(t *testing.T, recs []storecontract.ChangeRecord) {
	t.Helper()
	for i, r := range recs {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record[%d] LSN = %d, want %d (LSNs must be 1..N gap-free, ascending)", i, r.LSN, i+1)
		}
	}
}
