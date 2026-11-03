package badger

import (
	"fmt"
	"strings"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// VALUETHRESH F1 (Fable's review, 3 Nov): badger's value-log GC deletes a
// rewritten file at once when no iterator is open (value.go:342-355); a read
// transaction that only Gets pins nothing. The rewrite keeps each moved
// entry's version (value.go:217-224), so a Get after it finds the new pointer;
// an item a Get returned before it holds the old one, and its Value after the
// delete is logged and comes back empty with a nil error (iterator.go:155-189):
// rho reads it as ErrCorruptWire. Here a read through rho's read seam gets the
// row's item, waits while rho's GC pass rewrites the file holding it, then
// reads the value.

const vgThreshold = 1 << 10

func vgConfig(dir string) Config {
	return Config{Dir: dir, ValueThreshold: vgThreshold, ValueLogFileSize: 1 << 20, MemTableSize: 8 << 20, GCInterval: 1<<63 - 1}
}

func vgNode(id int, fill string) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 7, nil)
	_ = n.SetProperty("p", strings.Repeat(fill, 2_000)) // a 2 KB row: in the value log
	return n
}

// vgGarbageStore: node 1's row is the only live value in a value-log file
// otherwise full of node 2's dead versions, and compaction has dropped those
// (badger's discard statistics come from compaction only, value.go:1003-1016),
// so the GC picks the file.
func vgGarbageStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	bs, err := New(vgConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(vgNode(1, "x")); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(vgNode(2, "a")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1_200; i++ {
		if err := bs.ReplaceNode(vgNode(2, string(rune('a'+i%26)))); err != nil {
			t.Fatal(err)
		}
		if i%50 == 0 {
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Each Close leaves an L0 table; at five the compactors merge them and
	// drop node 2's old versions.
	for cycle := 0; cycle < 5; cycle++ {
		if err := bs.ReplaceNode(vgNode(2, fmt.Sprint(cycle))); err != nil {
			t.Fatal(err)
		}
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		if bs, err = New(vgConfig(dir)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for bs.db.Levels()[0].NumTables >= 5 {
		if time.Now().After(deadline) {
			t.Fatal("L0 not compacted within 10 s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

func TestValueLogGCKeepsTheFileOfAnOpenRead(t *testing.T) {
	bs := vgGarbageStore(t)
	key := storepkg.NodeKey(snowflake.ID(1))
	opened, gcDone, readDone := make(chan struct{}), make(chan int, 1), make(chan error, 1)
	go func() {
		readDone <- bs.view(func(txn *badgerv4.Txn) error {
			item, err := txn.Get(key) // the item holds the row's value pointer
			if err != nil {
				return err
			}
			close(opened)
			select { // until the GC pass is over, or 300 ms if it waits for this read
			case n := <-gcDone:
				gcDone <- n
			case <-time.After(300 * time.Millisecond):
			}
			return item.Value(func(val []byte) error {
				var w storepkg.NodeWire
				return storepkg.SafeUnmarshal(val, &w)
			})
		})
	}()
	<-opened
	go func() { gcDone <- bs.runValueLogGC() }()
	if err := <-readDone; err != nil {
		t.Fatalf("a read opened before the value-log GC rewrote its row's file: %v", err)
	}
	if n := <-gcDone; n == 0 {
		t.Fatal("the GC rewrote no file: the test proves nothing")
	}
	n, err := bs.GetNode(types.NodeID(snowflake.ID(1)))
	if err != nil {
		t.Fatalf("node 1 after the GC: %v", err)
	}
	if v, _ := n.GetProperty("p"); v != strings.Repeat("x", 2_000) {
		t.Fatal("node 1 after the GC: wrong value")
	}
}

// VALUETHRESH F2: NodeStoredSizes reported a value-log row's length from
// Item.ValueSize, which subtracts an assumed header (iterator.go:238-252); the
// stored length is the value's.
func TestNodeStoredSizesExactForValueLogRows(t *testing.T) {
	bs, err := New(vgConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	big := types.NewNode(types.NodeID(snowflake.ID(1)), 7, nil)
	_ = big.SetProperty("p", strings.Repeat("x", 20_000)) // its length takes 3 varint bytes in the entry header
	if err := bs.PutNode(big); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	var want int
	if err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(snowflake.ID(1)))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { want = len(val); return nil })
	}); err != nil {
		t.Fatal(err)
	}
	if want < vgThreshold {
		t.Fatalf("row of %d bytes is inline: the test proves nothing", want)
	}
	rows, err := bs.NodeStoredSizes(types.NodeID(snowflake.ID(1)))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].Current || rows[0].Bytes != want {
		t.Fatalf("stored sizes %+v, want one current row of %d bytes", rows, want)
	}
}
