package graph

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Fix #5: ImportGraph does not block reads during streaming ---

// TestImportGraph_DoesNotBlockReadsWhileStreaming verifies that concurrent GetNode
// calls succeed during ImportGraph execution. Before fix #5, ImportGraph held
// g.mu.Lock for the entire io.Reader read, blocking all reads.
func TestImportGraph_DoesNotBlockReadsWhileStreaming(t *testing.T) {
	t.Parallel()

	// Build a source graph with a few nodes.
	src, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close() //nolint:errcheck

	n1, err := src.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	n2, err := src.AddNode([]string{"City"}, map[string]any{"city": "Vienna"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = src.AddRelationship("LIVES_IN", n1, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	// Export to a buffer.
	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Build the destination graph. Keep the label registry empty so the imported
	// registry record (Person/City from src) does not conflict with dst's registry.
	dst, err := New(Config{})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck

	// Pre-populate one node directly in the store (bypass graph.AddNode so the
	// label registry stays empty). The concurrent reader fetches this node via
	// dst.GetNode, which acquires g.mu.RLock — proving that reads are non-blocking
	// during ImportGraph Phase 1 (which holds no lock after the fix).
	const existingID = snowflake.ID(0xABCD0001)
	if err := dst.store.PutNode(types.NewNode(types.NodeID(existingID), 1, nil)); err != nil {
		t.Fatalf("pre-populate store: %v", err)
	}

	// Wrap the export buffer in a slow reader that yields to other goroutines,
	// giving concurrent reads a chance to run while ImportGraph is active.
	slow := &slowReader{r: bytes.NewReader(buf.Bytes()), delay: 2 * time.Millisecond}

	// Launch concurrent reader that polls until import is done.
	var readSucceeded atomic.Bool
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for i := 0; i < 20; i++ {
			_, err := dst.GetNode(types.NodeID(existingID))
			if err == nil {
				readSucceeded.Store(true)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	if err := dst.ImportGraph(slow); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	readerWg.Wait()

	if !readSucceeded.Load() {
		t.Error("concurrent GetNode never succeeded during ImportGraph — reads were likely blocked")
	}
}

// slowReader wraps an io.Reader and introduces a small delay before each Read
// to simulate slow I/O (network stream, large file) during ImportGraph.
type slowReader struct {
	r     io.Reader
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.r.Read(p)
}
