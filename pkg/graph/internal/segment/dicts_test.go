package segment

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0011 S2: segments that share one store-level node dictionary.

// newTestDicts shares endpoints and the test schema's string column "s".
func newTestDicts() *Dicts {
	return &Dicts{Nodes: NewNodeDict(), Strings: map[string]*StringDict{"s": NewStringDict()}}
}

func encodeDict(tb testing.TB, rows []*types.Relationship, d *Dicts, pageRows int) *Segment {
	tb.Helper()
	data, err := encode(testSchema(), rows, Options{IntegrityBlockRows: 8}, pageRows, d)
	if err != nil {
		tb.Fatalf("encode with dictionaries: %v", err)
	}
	seg, err := OpenWithDicts(data, d)
	if err != nil {
		tb.Fatalf("open with node dictionary: %v", err)
	}
	return seg
}

// Rows round-trip bit for bit through segments sharing one dictionary,
// including endpoint hashes that differ from the node's first hash (inside
// one segment and across segments), literal (non-hex, empty) hashes, and
// every row's recomputed hash.
func TestNodeDict_RoundTripAcrossSegments(t *testing.T) {
	d := newTestDicts()
	all := corpus(t, 6000, 41)
	for i, r := range all { // literal and empty endpoint hashes too
		switch i % 97 {
		case 0:
			r.Integrity().FromNodeHash = ""
		case 1:
			r.Integrity().ToNodeHash = "ABCDEF" // not lowercase 64-hex
		}
	}
	var segs []*Segment
	for lo := 0; lo < len(all); lo += 1500 {
		chunk := all[lo : lo+1500]
		seg := encodeDict(t, chunk, d, 256)
		got := scanAll(t, seg)
		want := byKey(chunk)
		for _, g := range got {
			w := want[rowKey{g.ID(), g.Version()}]
			assertSameRow(t, w, g)
			if g.Integrity().Hash != w.Integrity().Hash {
				t.Fatalf("id %d: hash %s, stored %s", g.ID(), g.Integrity().Hash, w.Integrity().Hash)
			}
		}
		if err := seg.Verify(); err != nil {
			t.Fatal(err)
		}
		if _, ok := seg.secs[secNodeHash]; ok {
			t.Fatal("a NodeDict segment must not carry its own node hashes")
		}
		if !seg.props[0].strs.present || seg.props[0].strs.mode != strShared {
			t.Fatalf("string column s: mode %d, want shared codes", seg.props[0].strs.mode)
		}
		segs = append(segs, seg)
	}
	// An earlier segment still reads identically after later ones grew d.
	for i, seg := range segs {
		for _, g := range scanAll(t, seg) {
			assertSameRow(t, byKey(all[i*1500 : i*1500+1500])[rowKey{g.ID(), g.Version()}], g)
		}
	}
	nodes, hashes := d.Nodes.Len()
	if nodes == 0 || nodes > 37 || hashes < nodes || d.Nodes.Bytes() <= 0 {
		t.Fatalf("dictionary: %d nodes, %d hashes, %d bytes (corpus has 37 endpoints)", nodes, hashes, d.Nodes.Bytes())
	}
	if n := d.Strings["s"].Len(); n == 0 || n > 5 || d.Strings["s"].Bytes() <= 0 {
		t.Fatalf("string dictionary holds %d values (corpus has 5)", n)
	}
}

// Re-encoding the same endpoints adds nothing to the dictionary.
func TestNodeDict_SameEndpointsDoNotGrowIt(t *testing.T) {
	d := newTestDicts()
	rows := corpus(t, 800, 42)
	encodeDict(t, rows, d, PageRows)
	n1, h1 := d.Nodes.Len()
	b1, s1 := d.Nodes.Bytes(), d.Strings["s"].Bytes()
	encodeDict(t, corpus(t, 800, 42), d, PageRows)
	if n2, h2 := d.Nodes.Len(); n2 != n1 || h2 != h1 || d.Nodes.Bytes() != b1 || d.Strings["s"].Bytes() != s1 {
		t.Fatalf("re-encoding the same endpoints and values grew the dictionaries: %d/%d -> %d/%d", n1, h1, n2, h2)
	}
}

func TestNodeDict_OpenRequiresTheDictionaryAndChecksCodes(t *testing.T) {
	d := newTestDicts()
	data, err := EncodeWithDicts(testSchema(), corpus(t, 300, 43), Options{}, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(data); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Open of a dictionary segment without its dictionaries = %v, want ErrUnsupportedVersion", err)
	}
	if _, err := OpenWithDicts(data, &Dicts{Nodes: d.Nodes}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Open without the string dictionary = %v, want ErrUnsupportedVersion", err)
	}
	if _, err := OpenWithDicts(data, &Dicts{Nodes: NewNodeDict(), Strings: d.Strings}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with a node dictionary that lacks its codes = %v, want ErrCorrupt", err)
	}
	if _, err := OpenWithDicts(data, &Dicts{Nodes: d.Nodes, Strings: map[string]*StringDict{"s": NewStringDict()}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with a string dictionary that lacks its codes = %v, want ErrCorrupt", err)
	}
	if _, err := EncodeWithDicts(testSchema(), nil, Options{}, nil); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("EncodeWithDicts(nil) = %v, want ErrInvalidOptions", err)
	}
	// A self-contained segment opens with or without dictionaries.
	plain := mustEncode(t, corpus(t, 50, 44), Options{}, PageRows)
	if _, err := OpenWithDicts(plain, d); err != nil {
		t.Fatalf("self-contained segment with dictionaries: %v", err)
	}
}

// A column whose distinct values grow like the rows is not interned (the
// shared dictionary must not grow with the rows): that segment encodes the
// column itself, and still round-trips.
func TestStringDict_HighCardinalityColumnIsNotInterned(t *testing.T) {
	d := newTestDicts()
	rows := make([]*types.Relationship, 0, 6000)
	for i := 0; i < 6000; i++ {
		rows = append(rows, mkRow(t, int64(1000+i), 0, 1, 2, map[string]any{"s": fmt.Sprintf("unique-%06d", i)}, nil, nil))
	}
	seg := encodeDict(t, rows, d, PageRows)
	if seg.props[0].strs.mode == strShared || d.Strings["s"].Len() != 0 {
		t.Fatalf("a unique-per-row column was interned: mode %d, dictionary %d values", seg.props[0].strs.mode, d.Strings["s"].Len())
	}
	for _, g := range scanAll(t, seg) {
		assertSameRow(t, byKey(rows)[rowKey{g.ID(), g.Version()}], g)
	}
	// A low-cardinality column in the same store is interned.
	low := make([]*types.Relationship, 0, 6000)
	for i := 0; i < 6000; i++ {
		low = append(low, mkRow(t, int64(100_000+i), 0, 1, 2, map[string]any{"s": fmt.Sprintf("v%d", i%50)}, nil, nil))
	}
	seg = encodeDict(t, low, d, PageRows)
	if seg.props[0].strs.mode != strShared || d.Strings["s"].Len() != 50 {
		t.Fatalf("a 50-value column: mode %d, dictionary %d values", seg.props[0].strs.mode, d.Strings["s"].Len())
	}
	err := seg.ScanBatches(func(b *Batch) bool {
		codes, dict, present, ok := b.StringColumn(0)
		if !ok || len(dict) != 50 {
			t.Fatalf("StringColumn on a shared column: ok %t, %d values", ok, len(dict))
		}
		for k := 0; k < b.N; k++ {
			if !present(k) || dict[codes[k]] != fmt.Sprintf("v%d", (b.Base+k)%50) && dict[codes[k]] == "" {
				t.Fatalf("row %d: code %d", b.Base+k, codes[k])
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Readers of existing segments run while another segment is encoded into
// the same dictionary (run with -race).
func TestNodeDict_ConcurrentReadsWhileEncoding(t *testing.T) {
	d := newTestDicts()
	first := encodeDict(t, corpus(t, 2000, 45), d, 256)
	want := scanAll(t, first)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := 0
				if err := first.Scan(func(_ int, r *types.Relationship) bool {
					assertSameRow(t, want[i], r)
					i++
					return true
				}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for s := uint64(0); s < 6; s++ {
		encodeDict(t, corpus(t, 1500, 46+s), d, 256)
	}
	close(stop)
	wg.Wait()
}

// FuzzOpenWithNodeDict: arbitrary bytes against a populated dictionary never
// panic.
func FuzzOpenWithNodeDict(f *testing.F) {
	d := newTestDicts()
	for i, n := range []int{1, 9, 70} {
		data, err := encode(testSchema(), corpus(f, n, uint64(300+i)), Options{IntegrityBlockRows: 4}, 8, d)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		seg, err := OpenWithDicts(data, d)
		if err != nil {
			return
		}
		n := 0
		_ = seg.Scan(func(int, *types.Relationship) bool { n++; return n < 20000 })
		for i := 0; i < seg.Len() && i < 32; i++ {
			if r, err := seg.Row(i); err == nil {
				_, _, _ = seg.OutRows(r.StartNodeID())
				_, _ = seg.InRows(r.EndNodeID())
			}
		}
	})
}
