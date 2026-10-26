package tiered

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestOwnerIndexLifecycle pins UNIQIDX step (a): the owner index of a
// declared (label, key) keeps its claims across a reopen, a claim is replaced
// by a later one for the same value, a drop removes the declaration and its
// claims, Clear empties it, and a store that never declared one makes no index
// directory. In memory it works the same within the store's life.
func TestOwnerIndexLifecycle(t *testing.T) {
	t.Parallel()
	cfg := Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "index")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an index directory without a declaration (stat %v)", err)
	}
	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	const tok, other = uint16(7), uint16(8)
	if ts.UniqueOwnerIndexDeclared(tok, "key") {
		t.Fatal("declared before DeclareUniqueOwnerIndex")
	}
	if err := ts.DeclareUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatalf("Declare: %v", err)
	}
	if err := ts.PutUniqueOwner(tok, "key", "s:a", types.NodeID(101)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := ts.PutUniqueOwner(other, "key", "s:a", types.NodeID(999)); err == nil {
		t.Fatal("a claim for an undeclared (label, key) was accepted")
	}
	owner := func(ts *Store, value string) (types.NodeID, bool) {
		t.Helper()
		id, ok, err := ts.UniqueOwner(tok, "key", value)
		if err != nil {
			t.Fatalf("UniqueOwner: %v", err)
		}
		return id, ok
	}
	if id, ok := owner(ts, "s:a"); !ok || id != 101 {
		t.Fatalf("owner of s:a = %d, %v; want 101", id, ok)
	}
	if _, ok := owner(ts, "s:b"); ok {
		t.Fatal("an owner for a value never claimed")
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = ts.Close() }()
	if !ts.UniqueOwnerIndexDeclared(tok, "key") || ts.UniqueOwnerIndexDeclared(other, "key") {
		t.Fatal("the declarations did not survive the reopen as they were")
	}
	if id, ok := owner(ts, "s:a"); !ok || id != 101 {
		t.Fatalf("owner of s:a after reopen = %d, %v; want 101", id, ok)
	}
	if err := ts.PutUniqueOwner(tok, "key", "s:a", types.NodeID(202)); err != nil {
		t.Fatal(err)
	}
	if id, _ := owner(ts, "s:a"); id != 202 {
		t.Fatalf("owner after a second claim = %d, want 202", id)
	}
	if err := ts.DropUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if ts.UniqueOwnerIndexDeclared(tok, "key") {
		t.Fatal("declared after the drop")
	}
	if _, _, err := ts.UniqueOwner(tok, "key", "s:a"); !errors.Is(err, ErrUniqueOwnerIndexNotDeclared) {
		t.Fatalf("UniqueOwner after the drop = %v, want ErrUniqueOwnerIndexNotDeclared", err)
	}
	if err := ts.DeclareUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatal(err)
	}
	if _, ok := owner(ts, "s:a"); ok {
		t.Fatal("a claim survived the drop")
	}
	if err := ts.PutUniqueOwner(tok, "key", "s:c", types.NodeID(303)); err != nil {
		t.Fatal(err)
	}
	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if ts.UniqueOwnerIndexDeclared(tok, "key") {
		t.Fatal("declared after Clear")
	}
	if err := ts.DeclareUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatal(err)
	}
	if _, ok := owner(ts, "s:c"); ok {
		t.Fatal("a claim survived Clear")
	}

	mem, err := New(Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mem.Close() }()
	if err := mem.DeclareUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatalf("in-memory Declare: %v", err)
	}
	if err := mem.PutUniqueOwner(tok, "key", "s:a", types.NodeID(5)); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := mem.UniqueOwner(tok, "key", "s:a"); err != nil || !ok || id != 5 {
		t.Fatalf("in-memory owner = %d, %v, %v", id, ok, err)
	}
}

// TestOwnerIndexSyncedBeforeDurableRows pins UNIQIDX's write order without a
// sync per claim: a claim is synced by the first flush that makes rows
// durable after it (SyncWrites, DurableFlush), once, before those rows; a flush
// that makes nothing durable does not sync it, nor does a durable flush with
// no claim since the last sync. Catches the hook not called and a sync per
// claim or per flush.
func TestOwnerIndexSyncedBeforeDurableRows(t *testing.T) {
	t.Parallel()
	for _, syncWrites := range []bool{false, true} {
		ts, err := New(Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour,
			FlushInterval: 1<<63 - 1, SyncWrites: syncWrites})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		_, _, sigTok := installDefaultTestLabelRegistry(t, ts)
		if err := ts.DeclareUniqueOwnerIndex(sigTok, "key"); err != nil {
			t.Fatal(err)
		}
		gen := tieredNodeGen(t)
		claimAndWrite := func(v string) {
			t.Helper()
			n := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
			if err := n.SetProperty("key", v); err != nil {
				t.Fatal(err)
			}
			if err := ts.PutUniqueOwner(sigTok, "key", "s:"+v, n.ID()); err != nil {
				t.Fatal(err)
			}
			if err := ts.PutNode(n); err != nil {
				t.Fatal(err)
			}
		}
		claimAndWrite("a")
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
		want := int64(0)
		if syncWrites {
			want = 1
		}
		if got := ts.ownerIdxSyncs.Load(); got != want {
			t.Fatalf("SyncWrites=%v: %d owner-index syncs after a flush, want %d", syncWrites, got, want)
		}
		if err := ts.DurableFlush(); err != nil {
			t.Fatal(err)
		}
		want = 1 // the claim is synced exactly once, by the first durable flush after it
		if got := ts.ownerIdxSyncs.Load(); got != want {
			t.Fatalf("SyncWrites=%v: %d owner-index syncs after DurableFlush, want %d", syncWrites, got, want)
		}
		// A row without a claim: its durable flush runs the hook, which has
		// nothing to sync.
		if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)); err != nil {
			t.Fatal(err)
		}
		if err := ts.DurableFlush(); err != nil {
			t.Fatal(err)
		}
		if got := ts.ownerIdxSyncs.Load(); got != want {
			t.Fatalf("SyncWrites=%v: a durable flush without new claims synced the index (%d)", syncWrites, got)
		}
		_ = ts.Close()
	}
}
