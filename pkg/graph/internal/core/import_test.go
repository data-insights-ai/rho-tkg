package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	snowflake "github.com/bds421/rho-snowflake-2026"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestImportNodeWithID_Basic(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	id := snowflake.ID(12345)
	n, err := g.Nodes.Import(context.Background(), types.NodeID(id), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}
	if n.ID() != types.NodeID(id) {
		t.Errorf("ID = %d, want %d", n.ID(), id)
	}

	// Verify retrievable by the same ID.
	got, err := g.Nodes.Get(context.Background(), types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	name, ok := got.GetProperty("name")
	if !ok || name != "Alice" {
		t.Errorf("name = %v (ok=%v), want Alice", name, ok)
	}
}

func TestImportNodeWithID_Collision(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	id := snowflake.ID(12345)
	_, err := g.Nodes.Import(context.Background(), types.NodeID(id), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("first import: %v", err)
	}

	// Second import with same ID should fail.
	_, err = g.Nodes.Import(context.Background(), types.NodeID(id), []string{"B"}, nil)
	if !errors.Is(err, storepkg.ErrNodeExists) {
		t.Errorf("err = %v, want storepkg.ErrNodeExists", err)
	}
}

type importNodeLockProbeStore struct {
	storepkg.MandatoryStore
	beforePut  chan struct{}
	unblockPut chan struct{}
	once       sync.Once
}

func (s *importNodeLockProbeStore) PutNode(n *types.Node) error {
	s.once.Do(func() {
		close(s.beforePut)
		<-s.unblockPut
	})
	return s.MandatoryStore.PutNode(n)
}

func TestImportNodeWithID_LocksCallerNodeID(t *testing.T) {
	t.Parallel()

	probe := &importNodeLockProbeStore{
		MandatoryStore: memory.New(),
		beforePut:      make(chan struct{}),
		unblockPut:     make(chan struct{}),
	}
	g, err := New(Config{Store: probe})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	nodeID := types.NodeID(12345)
	importDone := make(chan error, 1)
	go func() {
		_, err := g.Nodes.Import(context.Background(), nodeID, []string{"Person"}, nil)
		importDone <- err
	}()

	select {
	case <-probe.beforePut:
	case <-time.After(time.Second):
		t.Fatal("import did not reach PutNode")
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- g.Nodes.Delete(context.Background(), nodeID)
	}()

	select {
	case err := <-deleteDone:
		t.Fatalf("DeleteNode returned while ImportNodeWithID was writing same node ID: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(probe.unblockPut)
	if err := <-importDone; err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteNode after import release: %v", err)
	}
}

func TestImportNodeWithID_ZeroID(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	_, err := g.Nodes.Import(context.Background(), 0, []string{"A"}, nil)
	if !errors.Is(err, ErrZeroID) {
		t.Errorf("err = %v, want ErrZeroID", err)
	}
}

func TestImportNodeWithID_NegativeID(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	_, err := g.Nodes.Import(context.Background(), types.NodeID(-1), []string{"A"}, nil)
	if !errors.Is(err, ErrInvalidID) {
		t.Errorf("err = %v, want ErrInvalidID", err)
	}
}

func TestImportNodeWithID_Validation(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// No labels.
	_, err := g.Nodes.Import(context.Background(), types.NodeID(100), nil, nil)
	if !errors.Is(err, ErrNoLabels) {
		t.Errorf("no labels: err = %v, want ErrNoLabels", err)
	}
}

func TestImportRelationshipWithID_Basic(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

	relID := snowflake.ID(99999)
	r, err := g.Rels.Import(context.Background(), types.RelID(relID), "KNOWS", n1, n2, map[string]any{"since": int64(2024)})
	if err != nil {
		t.Fatalf("ImportRelationshipWithID: %v", err)
	}
	if r.ID() != types.RelID(relID) {
		t.Errorf("ID = %d, want %d", r.ID(), relID)
	}

	// Verify retrievable.
	got, err := g.Rels.Get(context.Background(), types.RelID(relID))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	since, ok := got.GetProperty("since")
	if !ok || since != int64(2024) {
		t.Errorf("since = %v (ok=%v), want 2024", since, ok)
	}
}

func TestImportRelationshipWithID_Collision(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

	relID := snowflake.ID(99999)
	_, err := g.Rels.Import(context.Background(), types.RelID(relID), "KNOWS", n1, n2, nil)
	if err != nil {
		t.Fatalf("first import: %v", err)
	}

	_, err = g.Rels.Import(context.Background(), types.RelID(relID), "LIKES", n1, n2, nil)
	if !errors.Is(err, storepkg.ErrRelExists) {
		t.Errorf("err = %v, want storepkg.ErrRelExists", err)
	}
}

type importRelLockProbeStore struct {
	storepkg.MandatoryStore
	beforePut  chan struct{}
	unblockPut chan struct{}
	once       sync.Once
}

func (s *importRelLockProbeStore) PutRelationship(r *types.Relationship) error {
	s.once.Do(func() {
		close(s.beforePut)
		<-s.unblockPut
	})
	return s.MandatoryStore.PutRelationship(r)
}

func TestImportRelationshipWithID_LocksCallerRelID(t *testing.T) {
	t.Parallel()

	probe := &importRelLockProbeStore{
		MandatoryStore: memory.New(),
		beforePut:      make(chan struct{}),
		unblockPut:     make(chan struct{}),
	}
	g, err := New(Config{Store: probe})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n1, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add node A: %v", err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add node B: %v", err)
	}

	relID := types.RelID(99999)
	importDone := make(chan error, 1)
	go func() {
		_, err := g.Rels.Import(context.Background(), relID, "KNOWS", n1, n2, nil)
		importDone <- err
	}()

	select {
	case <-probe.beforePut:
	case <-time.After(time.Second):
		t.Fatal("import did not reach PutRelationship")
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- g.Rels.Delete(context.Background(), relID)
	}()

	select {
	case err := <-deleteDone:
		t.Fatalf("DeleteRelationship returned while ImportRelationshipWithID was writing same rel ID: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(probe.unblockPut)
	if err := <-importDone; err != nil {
		t.Fatalf("ImportRelationshipWithID: %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteRelationship after import release: %v", err)
	}
}

func TestImportRelationshipWithID_NegativeID(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

	_, err := g.Rels.Import(context.Background(), types.RelID(-1), "KNOWS", n1, n2, nil)
	if !errors.Is(err, ErrInvalidID) {
		t.Errorf("err = %v, want ErrInvalidID", err)
	}
}

func TestGraphTx_ImportNodeWithID(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	tx, _ := g.BeginTx()
	defer tx.Rollback()

	id := snowflake.ID(55555)
	n, err := tx.ImportNodeWithID(context.Background(), types.NodeID(id), []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}
	if n.ID() != types.NodeID(id) {
		t.Errorf("ID = %d, want %d", n.ID(), id)
	}

	// Tracked for rollback.
	created := tx.CreatedNodeIDs()
	if len(created) != 1 || created[0] != types.NodeID(id) {
		t.Errorf("CreatedNodeIDs = %v, want [%d]", created, id)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify persisted.
	got, err := g.Nodes.Get(context.Background(), types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNode after commit: %v", err)
	}
	name, _ := got.GetProperty("name")
	if name != "Bob" {
		t.Errorf("name = %v, want Bob", name)
	}
}

func TestGraphTx_ImportNegativeIDs(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

	tx, _ := g.BeginTx()
	defer tx.Rollback()

	if _, err := tx.ImportNodeWithID(context.Background(), types.NodeID(-1), []string{"Person"}, nil); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("ImportNodeWithID negative ID = %v, want ErrInvalidID", err)
	}
	if _, err := tx.ImportRelationshipWithID(context.Background(), types.RelID(-1), "KNOWS", n1, n2, nil); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("ImportRelationshipWithID negative ID = %v, want ErrInvalidID", err)
	}
}

func TestGraphTx_ImportNodeWithID_Rollback(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	id := snowflake.ID(77777)

	tx, _ := g.BeginTx()
	_, err := tx.ImportNodeWithID(context.Background(), types.NodeID(id), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Node should be gone after rollback.
	_, err = g.Nodes.Get(context.Background(), types.NodeID(id))
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("after rollback: err = %v, want storepkg.ErrNodeNotFound", err)
	}
}

func TestGraphTx_ImportNodeWithID_NilContextDoesNotMaterializeDeletedHistory(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	original, err := g.Nodes.Add(context.Background(), []string{"Original"}, map[string]any{"state": "initial"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	original, err = g.Nodes.Update(context.Background(), original.ID(), map[string]any{"state": "committed"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := tx.DeleteNode(original.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if len(tx.deletedNodes) != 1 || !tx.deletedNodes[0].useHistoryTrim {
		t.Fatalf("deleted node snapshot = %+v, want native trim snapshot", tx.deletedNodes)
	}

	var ctx context.Context
	_, err = tx.ImportNodeWithID(ctx, original.ID(), []string{"Replacement"}, nil) //nolint:staticcheck // intentional nil context boundary test
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("ImportNodeWithID(nil ctx) = %v, want ErrNilContext", err)
	}
	if !tx.deletedNodes[0].useHistoryTrim {
		t.Fatal("nil-context import materialized deleted node history before context rejection")
	}
	if tx.deletedNodes[0].nodeHistory != nil {
		t.Fatalf("nil-context import captured node history length %d, want nil trim snapshot", len(tx.deletedNodes[0].nodeHistory))
	}
}

func TestGraphTx_ImportRelationshipWithID_NilContextDoesNotMaterializeDeletedHistory(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	original, err := g.Rels.Add(context.Background(), "ORIGINAL_REL", a, b, map[string]any{"state": "initial"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	original, err = g.Rels.Update(context.Background(), original.ID(), map[string]any{"state": "committed"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := tx.DeleteRelationship(original.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if len(tx.deletedRels) != 1 || !tx.deletedRels[0].useHistoryTrim {
		t.Fatalf("deleted rel snapshot = %+v, want native trim snapshot", tx.deletedRels)
	}

	var ctx context.Context
	_, err = tx.ImportRelationshipWithID(ctx, original.ID(), "REPLACEMENT_REL", a, b, nil) //nolint:staticcheck // intentional nil context boundary test
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("ImportRelationshipWithID(nil ctx) = %v, want ErrNilContext", err)
	}
	if !tx.deletedRels[0].useHistoryTrim {
		t.Fatal("nil-context import materialized deleted relationship history before context rejection")
	}
	if tx.deletedRels[0].history != nil {
		t.Fatalf("nil-context import captured rel history length %d, want nil trim snapshot", len(tx.deletedRels[0].history))
	}
}

func TestGraphTx_ImportWithID_CanceledContextDoesNotWaitBehindTxMutex(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tx.mu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			tx.mu.Unlock()
		}
	}()

	cases := []struct {
		name string
		call func() error
	}{
		{name: "ImportNodeWithID", call: func() error {
			_, err := tx.ImportNodeWithID(ctx, types.NodeID(123456789), []string{"Blocked"}, nil)
			return err
		}},
		{name: "ImportRelationshipWithID", call: func() error {
			_, err := tx.ImportRelationshipWithID(ctx, types.RelID(987654321), "BLOCKED", a, b, nil)
			return err
		}},
	}
	for _, tc := range cases {
		done := make(chan error, 1)
		go func() {
			done <- tc.call()
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s = %v, want context.Canceled", tc.name, err)
			}
		case <-time.After(50 * time.Millisecond):
			tx.mu.Unlock()
			unlocked = true
			t.Fatalf("%s waited behind tx.mu despite pre-canceled context", tc.name)
		}
	}

	tx.mu.Unlock()
	unlocked = true
}

// --- Fix #5: ImportGraph does not block reads during streaming ---

// TestImportGraph_DoesNotBlockReadsWhileStreaming pins fix #5: ImportGraph
// streams the io.Reader with no graph lock held (Phase 1), so a read issued
// while the stream is stalled mid-way completes. The reader is held
// mid-stream until the read has returned; a regression that takes c.mu
// before streaming blocks the read for as long as the stream is held.
func TestImportGraph_DoesNotBlockReadsWhileStreaming(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	src, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close() //nolint:errcheck
	n1, err := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	n2, err := src.Nodes.Add(ctx, []string{"City"}, map[string]any{"city": "Vienna"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := src.Rels.Add(ctx, "LIVES_IN", n1, n2, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, err := New(Config{})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck
	// Put the probe node straight into the store so dst's label registry stays
	// empty and does not conflict with the imported one.
	const existingID = snowflake.ID(0xABCD0001)
	if err := dst.store.PutNode(types.NewNode(types.NodeID(existingID), 1, nil)); err != nil {
		t.Fatalf("pre-populate store: %v", err)
	}

	hr := newHeldReader(buf.Bytes(), buf.Len()/2)
	defer hr.release() // runs before dst.Close: never leave the import parked
	importDone := make(chan error, 1)
	go func() { importDone <- dst.IO.Import(hr, tkgio.ImportOptions{}) }()

	const bound = 10 * time.Second // deadlock bound, not the oracle
	select {
	case <-hr.held:
	case err := <-importDone:
		t.Fatalf("import returned before reaching the mid-stream hold: %v", err)
	case <-time.After(bound):
		t.Fatal("import never reached the mid-stream hold")
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := dst.Nodes.Get(ctx, types.NodeID(existingID))
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("Get while import is held mid-stream: %v", err)
		}
	case <-time.After(bound):
		hr.release()
		<-importDone
		<-readDone
		t.Fatal("Get blocked while ImportGraph was held mid-stream: the import holds the graph lock while streaming")
	}

	hr.release()
	if err := <-importDone; err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}
	if _, err := dst.Nodes.Get(ctx, n1.ID()); err != nil {
		t.Fatalf("imported node missing after import: %v", err)
	}
}

// heldReader serves data up to cut, then parks the next Read (closing held)
// until release is called, then serves the rest.
type heldReader struct {
	data     []byte
	off, cut int
	parked   bool
	held     chan struct{}
	gate     chan struct{}
	once     sync.Once
}

func newHeldReader(data []byte, cut int) *heldReader {
	return &heldReader{data: data, cut: cut, held: make(chan struct{}), gate: make(chan struct{})}
}

func (h *heldReader) release() { h.once.Do(func() { close(h.gate) }) }

func (h *heldReader) Read(p []byte) (int, error) {
	if h.off >= h.cut && !h.parked {
		h.parked = true
		close(h.held)
		<-h.gate
	}
	if h.off >= len(h.data) {
		return 0, io.EOF
	}
	end := len(h.data)
	if !h.parked {
		end = h.cut
	}
	n := copy(p, h.data[h.off:end])
	h.off += n
	return n, nil
}
