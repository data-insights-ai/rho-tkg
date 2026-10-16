package graph

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func memStorageConfig(t testing.TB, records uint64) Config {
	c := engineConfig(t, "")
	c.Dir, c.Memory, c.Durability = "", true, DurabilityBuffered
	c.Storage = StorageLimits{RetainedRecords: records}
	return c
}

func addNode(g *Graph, t testing.TB, i int) error {
	_, err := g.Update(context.Background(), func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(int64(i)), "name": String("node-" + string(rune('a'+i%26)))}, Scope: scopeAll(t)})
		return err
	})
	return err
}

// fill commits nodes until the first refusal or max commits; it returns the count accepted.
func fill(t testing.TB, g *Graph, max int) (int, error) {
	for i := 0; i < max; i++ {
		if err := addNode(g, t, i); err != nil {
			return i, err
		}
	}
	return max, nil
}

func TestGraphAcceptsBeyondOldRetainedCap(t *testing.T) {
	if d := (StorageLimits{}).resolve(); d.RetainedRecords <= 1<<20 || d.RetainedBytes <= 256<<20 {
		t.Fatalf("default caps %+v do not exceed the old 1<<20 records / 256 MiB", d)
	}
	ctx := context.Background()
	g, err := Open(ctx, memStorageConfig(t, 16384))
	if err != nil {
		t.Fatal(err)
	}
	small, err := fill(t, g, 5000)
	if !errors.Is(err, ErrInvalidRequest) || small < 5 || small >= 5000 {
		t.Fatalf("small cap: accepted %d commits, err=%v; want a limit refusal after a few commits", small, err)
	}
	_ = g.Close()
	g, err = Open(ctx, memStorageConfig(t, 16384*8))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if n, err := fill(t, g, small*3); err != nil {
		t.Fatalf("raised cap: refused after %d commits (small cap stopped at %d): %v", n, small, err)
	}
	// A cap below the raftlog minimum is refused at Open.
	if _, err := Open(ctx, memStorageConfig(t, 10)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("tiny cap Open = %v, want ErrInvalidConfig", err)
	}
}

func TestReclaimKeepsRaftLogBounded(t *testing.T) {
	ctx := context.Background()
	g, err := Open(ctx, memStorageConfig(t, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := fill(t, g, 300); err != nil {
		t.Fatal(err)
	}
	st := g.eng.(*pebbleEngine).store
	first, _ := st.FirstIndex()
	last, _ := st.LastIndex()
	if last < 300 || last-first+1 > 128 {
		t.Fatalf("raft log holds %d entries after %d commits (first %d, last %d); want it bounded", last-first+1, last, first, last)
	}
}

func TestReopenWithStoredPolicy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := engineConfig(t, dir)
	cfg.Storage = StorageLimits{RetainedRecords: 1 << 21, RetainedBytes: 1 << 30}
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := addNode(g, t, 1); err != nil {
		t.Fatal(err)
	}
	_ = g.Close()
	// zero Storage adopts the stored policy
	z := engineConfig(t, dir)
	g, err = Open(ctx, z)
	if err != nil {
		t.Fatalf("reopen with zero Storage: %v", err)
	}
	if err := addNode(g, t, 2); err != nil {
		t.Fatal(err)
	}
	_ = g.Close()
	// the same explicit policy reopens
	if g, err = Open(ctx, cfg); err != nil {
		t.Fatalf("reopen with same Storage: %v", err)
	}
	_ = g.Close()
	// a different explicit policy is a named error
	cfg.Storage.RetainedRecords = 1 << 22
	if _, err := Open(ctx, cfg); !errors.Is(err, ErrStoragePolicy) || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("different policy Open = %v, want ErrStoragePolicy", err)
	}
	// a corrupt policy file is refused, not guessed
	if err := os.WriteFile(filepath.Join(dir, policyFileName), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, z); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("corrupt policy file Open = %v, want ErrInvalidConfig", err)
	}
	// no policy file = a store of an older build: it opens with the old caps,
	// and an explicit different policy is the named error.
	d2 := t.TempDir()
	old := engineConfig(t, d2)
	old.Storage = StorageLimits{RetainedRecords: legacyRetainedRecords, RetainedBytes: legacyRetainedBytes}
	g, err = Open(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	_ = g.Close()
	if err := os.Remove(filepath.Join(d2, policyFileName)); err != nil {
		t.Fatal(err)
	}
	if g, err = Open(ctx, engineConfig(t, d2)); err != nil {
		t.Fatalf("legacy store (no policy file) Open: %v", err)
	}
	_ = g.Close()
	if _, err := Open(ctx, cfg2(engineConfig(t, d2), 1<<25)); !errors.Is(err, ErrStoragePolicy) {
		t.Fatalf("legacy store with new explicit policy = %v, want ErrStoragePolicy", err)
	}
}

func cfg2(c Config, records uint64) Config { c.Storage.RetainedRecords = records; return c }
