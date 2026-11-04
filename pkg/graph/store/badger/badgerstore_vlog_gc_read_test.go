package badger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
// rho reads it as ErrCorruptWire. Here a read through each of rho's seams gets
// the row's item, waits until rho's GC pass has rewritten the file holding it,
// then reads the value.

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

// vgSeam opens a transaction through one of rho's seams and runs read in it.
type vgSeam struct {
	name string
	run  func(bs *Store, read func(txn *badgerv4.Txn) error) error
}

var vgSeams = []vgSeam{
	{"view", func(bs *Store, read func(txn *badgerv4.Txn) error) error { return bs.view(read) }},
	{"update", func(bs *Store, read func(txn *badgerv4.Txn) error) error { return bs.update(read) }},
	{"newReadTxn", func(bs *Store, read func(txn *badgerv4.Txn) error) error {
		txn, pin := bs.newReadTxn()
		defer bs.endReadTxn(txn, pin, true)
		return read(txn)
	}},
}

// vgHook makes afterVlogRewrite close the returned channel once.
func vgHook(t *testing.T) <-chan struct{} {
	t.Helper()
	rewritten := make(chan struct{})
	var once sync.Once
	afterVlogRewrite = func() { once.Do(func() { close(rewritten) }) }
	t.Cleanup(func() { afterVlogRewrite = nil })
	return rewritten
}

// The read gets its row's item, rho's GC pass rewrites the file holding it
// (afterVlogRewrite: the rewrite is done, the pass has not waited yet), then
// the read reads the value. Every seam rho opens transactions through.
func TestValueLogGCKeepsTheFileOfAnOpenRead(t *testing.T) {
	for _, seam := range vgSeams {
		t.Run(seam.name, func(t *testing.T) {
			bs := vgGarbageStore(t)
			rewritten := vgHook(t)
			key := storepkg.NodeKey(snowflake.ID(1))
			opened, gcDone, readDone := make(chan struct{}), make(chan int, 1), make(chan error, 1)
			go func() {
				readDone <- seam.run(bs, func(txn *badgerv4.Txn) error {
					item, err := txn.Get(key) // the item holds the row's value pointer
					if err != nil {
						return err
					}
					close(opened)
					select {
					case <-rewritten:
					case <-time.After(10 * time.Second):
						return errors.New("the GC pass did not finish its rewrite within 10 s")
					}
					return item.Value(func(val []byte) error {
						var w storepkg.NodeWire
						return storepkg.SafeUnmarshal(val, &w)
					})
				})
			}()
			<-opened
			go func() { gcDone <- bs.runValueLogGC() }()
			deadline := time.After(20 * time.Second)
			select {
			case err := <-readDone:
				if err != nil {
					t.Fatalf("a read whose item was taken before the value-log GC rewrote its row's file: %v", err)
				}
			case <-deadline:
				t.Fatal("the read did not end within 20 s")
			}
			select {
			case n := <-gcDone:
				if n == 0 {
					t.Fatal("the GC rewrote no file: the test proves nothing")
				}
			case <-deadline:
				t.Fatal("the GC pass did not end within 20 s of the read's end")
			}
			// Once the read ended the pass deleted the files it rewrote: node 1's
			// original file is gone.
			if _, err := os.Stat(filepath.Join(bs.db.Opts().Dir, "000001.vlog")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the rewritten value-log file is still there after the pass: %v", err)
			}
			n, err := bs.GetNode(types.NodeID(snowflake.ID(1)))
			if err != nil {
				t.Fatalf("node 1 after the GC: %v", err)
			}
			if v, _ := n.GetProperty("p"); v != strings.Repeat("x", 2_000) {
				t.Fatal("node 1 after the GC: wrong value")
			}
		})
	}
}

// A slot below zero (an exit without its enter) is reported, never waited on:
// waiting on it stalled every later pass for the life of the store.
func TestVlogDrainDoesNotWaitOnANegativeSlot(t *testing.T) {
	var r vlogReaders
	p := r.enter()
	r.exit(p)
	r.exit(p)
	done := make(chan bool, 1)
	go func() { _, _, negative := r.drain(make(chan struct{})); done <- negative }()
	select {
	case negative := <-done:
		if !negative {
			t.Fatal("drain did not report the negative slot")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain waits on a negative slot")
	}
}

type vgLogger struct {
	mu   sync.Mutex
	warn []string
}

func (l *vgLogger) Errorf(string, ...any) {}
func (l *vgLogger) Infof(string, ...any)  {}
func (l *vgLogger) Debugf(string, ...any) {}
func (l *vgLogger) Warningf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warn = append(l.warn, fmt.Sprintf(f, a...))
}

// A read held open holds the pass up; a wait above vlogDrainSlow is logged
// with its duration, and Close during the wait returns at once.
func TestValueLogGCSlowWaitLoggedAndCloseReturns(t *testing.T) {
	bs := vgGarbageStore(t)
	rewritten := vgHook(t)
	log := &vgLogger{}
	bs.logger = log
	slow := vlogDrainSlow
	vlogDrainSlow = 50 * time.Millisecond
	t.Cleanup(func() { vlogDrainSlow = slow })

	txn, pin := bs.newReadTxn() // held, as across a caller's callback
	gcDone := make(chan int, 1)
	go func() { gcDone <- bs.runValueLogGC() }()
	<-rewritten
	time.Sleep(150 * time.Millisecond) // the pass waits on the held read
	select {
	case <-gcDone:
		t.Fatal("the pass ended while a read that began before it is open")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- bs.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5 s while the pass waited")
	}
	select {
	case <-gcDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass did not end at Close")
	}
	bs.endReadTxn(txn, pin, false)
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.warn) != 1 || !strings.Contains(log.warn[0], "waited") {
		t.Fatalf("warnings %q, want one naming the wait", log.warn)
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

// A pass that finds badger closed under the store (a crash simulation closes
// the DB without Close, and gcLoop keeps ticking) returns 0: opening the
// guard iterator on a closed DB panics (iterator.go:464-466), which took down
// the package's race run in ci-docker.
func TestValueLogGCOnAClosedDBReturns(t *testing.T) {
	bs := vgGarbageStore(t)
	if err := bs.db.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if n := bs.runValueLogGC(); n != 0 {
		t.Fatalf("a pass on a closed DB rewrote %d files", n)
	}
}
