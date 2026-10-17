package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Row 66: nodeHashes is a lazy cache. No node write fills it (v4.60.0 kept a
// 64-hex string per live node, ~99 B), NodeIntegrityHash fills it from the
// current row, and every write that changes the row drops the cached hash.

func lnhNode(id int64, hash string, extra ...uint16) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 10, extra)
	n.SetIntegrity(&types.NodeIntegrity{Hash: hash})
	return n
}

func lnhCached(bs *Store, nid types.NodeID) (string, bool) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	h, ok := bs.nodeHashes[nid]
	return h, ok
}

func lnhCount(bs *Store) int {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return len(bs.nodeHashes)
}

func lnhWant(t *testing.T, bs *Store, nid types.NodeID, want string) {
	t.Helper()
	got, err := bs.NodeIntegrityHash(nid)
	if err != nil || got != want {
		t.Fatalf("NodeIntegrityHash(%d) = %q, %v; want %q", nid, got, err, want)
	}
	if h, ok := lnhCached(bs, nid); !ok || h != want {
		t.Fatalf("node %d after a hash read: cached %q (%v), want %q", nid, h, ok, want)
	}
}

func TestNodeHashesFilledLazilyOnRead(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	if err := bs.PutNode(lnhNode(100, "h100")); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodesBatch([]*types.Node{lnhNode(101, "h101"), lnhNode(102, "h102")}); err != nil {
		t.Fatal(err)
	}
	if n := lnhCount(bs); n != 0 {
		t.Fatalf("after 3 node writes nodeHashes holds %d entries, want 0 (filled on read)", n)
	}
	lnhWant(t, bs, 101, "h101")
	if n := lnhCount(bs); n != 1 {
		t.Fatalf("after one hash read nodeHashes holds %d, want 1", n)
	}
	from, to, err := bs.EndpointIntegrityHashes(100, 102)
	if err != nil || from != "h100" || to != "h102" {
		t.Fatalf("EndpointIntegrityHashes = %q, %q, %v", from, to, err)
	}
	if n := lnhCount(bs); n != 3 {
		t.Fatalf("after the endpoint read nodeHashes holds %d, want 3", n)
	}
	if _, err := bs.NodeIntegrityHash(999); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("missing node: %v", err)
	}
	if n := lnhCount(bs); n != 3 {
		t.Fatalf("a miss cached something: %d entries", n)
	}
}

// Every door that rewrites a live node's current row drops a cached hash, so
// the next read answers the new row's hash.
func TestNodeHashWriteDoorsInvalidate(t *testing.T) {
	t.Parallel()
	const nid = types.NodeID(1)
	doors := []struct {
		name  string
		extra []uint16
		apply func(bs *Store, cur *types.Node) error
	}{
		{"ReplaceNode", nil, func(bs *Store, cur *types.Node) error {
			next := cur.DeepCopy()
			next.SetIntegrity(&types.NodeIntegrity{Hash: "v1"})
			return bs.ReplaceNode(next)
		}},
		{"ReplaceNodeWithHistory", nil, func(bs *Store, cur *types.Node) error {
			next := cur.DeepCopy()
			next.SetIntegrity(&types.NodeIntegrity{Hash: "v1"})
			next.SetVersion(1)
			return bs.ReplaceNodeWithHistory(next, 0, cur.DeepCopy())
		}},
		{"AddNodeLabelToken", nil, func(bs *Store, cur *types.Node) error {
			next := cur.DeepCopy()
			next.AddLabelTokenRaw(20)
			next.SetIntegrity(&types.NodeIntegrity{Hash: "v1"})
			return bs.AddNodeLabelToken(nid, 20, next)
		}},
		{"RemoveNodeLabelToken", []uint16{20}, func(bs *Store, cur *types.Node) error {
			next := cur.DeepCopy()
			next.RemoveLabelTokenRaw(20)
			next.SetIntegrity(&types.NodeIntegrity{Hash: "v1"})
			return bs.RemoveNodeLabelToken(nid, 20, next)
		}},
		{"AddNodeLabelTokenWithHistory", nil, func(bs *Store, cur *types.Node) error {
			next := cur.DeepCopy()
			next.AddLabelTokenRaw(20)
			next.SetIntegrity(&types.NodeIntegrity{Hash: "v1"})
			next.SetVersion(1)
			return bs.AddNodeLabelTokenWithHistory(nid, 20, next, 0, cur.DeepCopy())
		}},
		{"RemoveNodeLabelTokenWithHistory", []uint16{20}, func(bs *Store, cur *types.Node) error {
			next := cur.DeepCopy()
			next.RemoveLabelTokenRaw(20)
			next.SetIntegrity(&types.NodeIntegrity{Hash: "v1"})
			next.SetVersion(1)
			return bs.RemoveNodeLabelTokenWithHistory(nid, 20, next, 0, cur.DeepCopy())
		}},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			t.Parallel()
			bs := newTestBadgerStore(t)
			cur := lnhNode(1, "v0", d.extra...)
			if err := bs.PutNode(cur); err != nil {
				t.Fatal(err)
			}
			lnhWant(t, bs, nid, "v0")
			if err := d.apply(bs, cur); err != nil {
				t.Fatal(err)
			}
			if h, ok := lnhCached(bs, nid); ok {
				t.Fatalf("hash %q still cached after the write", h)
			}
			lnhWant(t, bs, nid, "v1")
		})
	}
}

// Delete doors drop the cached hash; the same ID re-created answers its new hash.
func TestNodeHashDeleteDoorsThenRecreate(t *testing.T) {
	t.Parallel()
	doors := map[string]func(bs *Store, nid types.NodeID) error{
		"DeleteNode":        func(bs *Store, nid types.NodeID) error { return bs.DeleteNode(nid) },
		"DeleteNodeCascade": func(bs *Store, nid types.NodeID) error { return bs.DeleteNodeCascade(nid) },
		"DeleteNodesBatch":  func(bs *Store, nid types.NodeID) error { return bs.DeleteNodesBatch([]types.NodeID{nid}) },
	}
	for name, del := range doors {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bs := newTestBadgerStore(t)
			if err := bs.PutNode(lnhNode(7, "v0")); err != nil {
				t.Fatal(err)
			}
			lnhWant(t, bs, 7, "v0")
			if err := del(bs, 7); err != nil {
				t.Fatal(err)
			}
			if _, err := bs.NodeIntegrityHash(7); !errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("after delete: %v, want ErrNodeNotFound", err)
			}
			if n := lnhCount(bs); n != 0 {
				t.Fatalf("after delete nodeHashes holds %d", n)
			}
			if err := bs.PutNode(lnhNode(7, "v2")); err != nil {
				t.Fatal(err)
			}
			lnhWant(t, bs, 7, "v2")
		})
	}
}

// A write landing between the lazy read's row read and its fill must not
// leave the older row's hash cached (the fill is guarded by the row
// revision): on the cache-hit and on the disk path.
func TestNodeHashFillRacingWriteNotCached(t *testing.T) {
	for _, disk := range []bool{false, true} {
		name := map[bool]string{false: "cache", true: "disk"}[disk]
		t.Run(name, func(t *testing.T) {
			bs := newTestBadgerStore(t)
			cur := lnhNode(3, "v0")
			if err := bs.PutNode(cur); err != nil {
				t.Fatal(err)
			}
			if disk {
				if err := bs.flush(); err != nil {
					t.Fatal(err)
				}
				bs.NodeCacheForTest().ResetForTest()
			}
			bs.idxMu.Lock()
			delete(bs.nodeHashes, cur.ID()) // v4.60.0 filled it on write
			bs.idxMu.Unlock()
			ran := false
			bs.nodeHashFillTestHook = func() {
				if ran {
					return
				}
				ran = true
				next := cur.DeepCopy()
				next.SetIntegrity(&types.NodeIntegrity{Hash: "v1"})
				if err := bs.ReplaceNode(next); err != nil {
					t.Error(err)
				}
			}
			got, err := bs.NodeIntegrityHash(cur.ID())
			bs.nodeHashFillTestHook = nil
			if err != nil || (got != "v0" && got != "v1") {
				t.Fatalf("racing read = %q, %v", got, err)
			}
			if !ran {
				t.Fatal("the read did not reach the fill")
			}
			if h, ok := lnhCached(bs, cur.ID()); ok && h != "v1" {
				t.Fatalf("stale hash %q cached over the newer row", h)
			}
			lnhWant(t, bs, cur.ID(), "v1")
		})
	}
}

// A reopen (clean: open image; unclean: full row rebuild) fills no hash; the
// first read answers the stored row's.
func TestNodeHashesLazyAfterReopen(t *testing.T) {
	t.Parallel()
	for _, rebuild := range []bool{false, true} {
		name := map[bool]string{false: "image", true: "rebuild"}[rebuild]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			for id := int64(1); id <= 50; id++ {
				if err := bs.PutNode(lnhNode(id, "h"+string(rune('A'+id%26)))); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if rebuild {
				db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Delete(openImageHeaderKey) }); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if bs, err = New(Config{Dir: dir}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			if n := lnhCount(bs); n != 0 {
				t.Fatalf("reopen filled %d hashes, want 0", n)
			}
			for id := int64(1); id <= 50; id++ {
				lnhWant(t, bs, types.NodeID(id), "h"+string(rune('A'+id%26)))
			}
		})
	}
}
