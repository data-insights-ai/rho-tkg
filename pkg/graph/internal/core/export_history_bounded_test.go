package core

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// exportHistoryIDPageStore counts the node-history ID reads Export makes: the
// paged AllNodeHistoryIDsFrom calls, the largest page it asked for and got,
// and any call to the unbounded AllNodeHistoryIDs.
type exportHistoryIDPageStore struct {
	*memory.Store
	pageCalls      atomic.Int64
	maxLimit       atomic.Int64
	maxPage        atomic.Int64
	unboundedCalls atomic.Int64
}

func (s *exportHistoryIDPageStore) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	s.pageCalls.Add(1)
	if int64(limit) > s.maxLimit.Load() {
		s.maxLimit.Store(int64(limit))
	}
	ids, err := s.Store.AllNodeHistoryIDsFrom(after, limit)
	if int64(len(ids)) > s.maxPage.Load() {
		s.maxPage.Store(int64(len(ids)))
	}
	return ids, err
}

func (s *exportHistoryIDPageStore) AllNodeHistoryIDs() ([]types.NodeID, error) {
	s.unboundedCalls.Add(1)
	return s.Store.AllNodeHistoryIDs()
}

// TestExportGraph_HistoryBoundedMemory: Export holds at most
// exportHistoryBatchSize node-history IDs at once. With one more
// history-bearing node than a page, it reads the history ID set in exactly two
// pages of at most exportHistoryBatchSize IDs, never calls the unbounded
// AllNodeHistoryIDs, and still writes every history record. (Paging each
// entity's versions is TestExportGraph_HistoryVersionPagingSplitsDeepEntity.)
func TestExportGraph_HistoryBoundedMemory(t *testing.T) {
	t.Parallel()
	store := &exportHistoryIDPageStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Seed through the store directly: one current row and one history
	// version per node.
	const nodeCount = exportHistoryBatchSize + 1
	for i := 1; i <= nodeCount; i++ {
		nid := types.NodeID(i)
		if err := store.PutNode(types.NewNode(nid, 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		n := types.NewNode(nid, 1, nil)
		if err := store.PutNodeVersion(nid, 0, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", i, err)
		}
	}

	var out bytes.Buffer
	if err := g.IO.Export(&out); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if got := store.unboundedCalls.Load(); got != 0 {
		t.Fatalf("Export called the unbounded AllNodeHistoryIDs %d times", got)
	}
	if got := store.pageCalls.Load(); got != 2 {
		t.Fatalf("AllNodeHistoryIDsFrom calls = %d, want 2 for %d history nodes", got, nodeCount)
	}
	if got := store.maxLimit.Load(); got != exportHistoryBatchSize {
		t.Fatalf("largest AllNodeHistoryIDsFrom limit = %d, want %d", got, exportHistoryBatchSize)
	}
	if got := store.maxPage.Load(); got != exportHistoryBatchSize {
		t.Fatalf("largest history ID page = %d, want %d", got, exportHistoryBatchSize)
	}

	histRecords := 0
	for {
		tag, _, err := readExportRecord(&out)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read export record: %v", err)
		}
		if tag == exportTagNodeHist {
			histRecords++
		}
	}
	if histRecords != nodeCount {
		t.Fatalf("node history records = %d, want %d", histRecords, nodeCount)
	}
}
