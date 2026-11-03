package badger

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// VALUETHRESH (ai-soc R3, tasks/evidence/VALUETHRESH/): Config.ValueThreshold
// sends values of at least that size to the value log. badger refuses a
// threshold above 1 MB or above its transaction limit (db.go:146-157) and
// accepts any smaller one, negative included; rho refuses everything outside
// [1 KB, 1 MB] by name before badger creates the directory. In memory badger
// keeps every value in the LSM and refuses one above the configured threshold
// (db.go:245 reads it before db.go:308 lifts it; txn.go:370), so the write
// door's limit follows the knob.

func TestValueThresholdValidatedBeforeOpen(t *testing.T) {
	for _, bad := range []int64{-1, 512, 1<<10 - 1, 1<<20 + 1, 2 << 20} {
		dir := filepath.Join(t.TempDir(), "db")
		bs, err := New(Config{Dir: dir, ValueThreshold: bad})
		if err == nil {
			_ = bs.Close()
			t.Errorf("ValueThreshold %d: opened, want refused", bad)
			continue
		}
		if !strings.HasPrefix(err.Error(), "graph: ValueThreshold") {
			t.Errorf("ValueThreshold %d: err %q, want rho's own refusal naming ValueThreshold", bad, err)
		}
		if _, serr := os.Stat(dir); !errors.Is(serr, os.ErrNotExist) {
			t.Errorf("ValueThreshold %d: refused after a side effect: the directory exists (%v)", bad, serr)
		}
	}
	for _, good := range []int64{0, 1 << 10, 64 << 10, 1 << 20} {
		bs, err := New(Config{Dir: t.TempDir(), ValueThreshold: good, MemTableSize: 8 << 20})
		if err != nil {
			t.Errorf("ValueThreshold %d: %v", good, err)
			continue
		}
		_ = bs.Close()
	}
}

func TestValueThresholdInMemoryLimitFollows(t *testing.T) {
	row := func(id int, n int) map[string]string {
		return map[string]string{"a": strings.Repeat("x", n/2), "b": strings.Repeat("y", n/2)}
	}
	bs, err := New(Config{InMemory: true, ChangeLog: true, ValueThreshold: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err := bs.PutNode(esNode(1, 7, row(1, 100_000))); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("in memory at ValueThreshold 64 KB, a 100 KB row: err %v, want ErrEntryTooLarge at the door", err)
	}
	if err := bs.PutNode(esNode(2, 7, row(2, 30_000))); err != nil {
		t.Fatalf("a 30 KB row: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("the store stopped persisting: %v", err)
	}

	def, err := New(Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer def.Close()
	if err := def.PutNode(esNode(1, 7, row(1, 100_000))); err != nil {
		t.Fatalf("in memory at the default threshold, a 100 KB row: %v", err)
	}
	if err := def.Flush(); err != nil {
		t.Fatal(err)
	}
}
