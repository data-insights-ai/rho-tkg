package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// TestCompression_ReachesBadgerOptions pins that Config.Compression and
// ZSTDCompressionLevel reach the Badger options, and that the zero value
// (options.None) keeps Badger's default codec. Mutation: dropping the WithCompression call, or
// applying it for the zero value, fails a row.
func TestCompression_ReachesBadgerOptions(t *testing.T) {
	t.Parallel()
	defaultCodec := (func() options.CompressionType {
		bs, err := New(Config{InMemory: true})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer bs.Close()
		return bs.DBForTest().Opts().Compression
	})()
	cases := []struct {
		name      string
		cfg       Config
		wantCodec options.CompressionType
		wantLevel int
	}{
		{"none is the zero value: keeps the default", Config{Compression: options.None}, defaultCodec, 0},
		{"snappy", Config{Compression: options.Snappy}, options.Snappy, 0},
		{"zstd level 3", Config{Compression: options.ZSTD, ZSTDCompressionLevel: 3}, options.ZSTD, 3},
		{"zero keeps default", Config{}, defaultCodec, 0},
		{"in-memory zstd", Config{InMemory: true, Compression: options.ZSTD}, options.ZSTD, 0},
		{"no compression", Config{NoCompression: true}, options.None, 0},
		{"in-memory no compression", Config{InMemory: true, NoCompression: true}, options.None, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := c.cfg
			if !cfg.InMemory {
				cfg.Dir = t.TempDir()
			}
			bs, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer bs.Close()
			opts := bs.DBForTest().Opts()
			if opts.Compression != c.wantCodec {
				t.Errorf("Compression reached Badger as %v, want %v", opts.Compression, c.wantCodec)
			}
			if c.wantLevel > 0 && opts.ZSTDCompressionLevel != c.wantLevel {
				t.Errorf("ZSTDCompressionLevel reached Badger as %d, want %d", opts.ZSTDCompressionLevel, c.wantLevel)
			}
		})
	}
}

// TestCompression_ZSTD_DataSurvivesReopen writes under ZSTD, closes (which
// flushes), reopens and reads the row back from disk, not from the cache.
func TestCompression_ZSTD_DataSurvivesReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{Dir: dir, Compression: options.ZSTD, ZSTDCompressionLevel: 3}

	bs1, err := New(cfg)
	if err != nil {
		t.Fatalf("New (write): %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(6)), 7, nil)
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs2, err := New(cfg)
	if err != nil {
		t.Fatalf("New (read): %v", err)
	}
	defer bs2.Close()
	got, err := bs2.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after reopen: %v", err)
	}
	if got.ID() != n.ID() || got.PrimaryLabelToken() != 7 {
		t.Fatalf("reopened node = id %v label %d, want id %v label 7", got.ID(), got.PrimaryLabelToken(), n.ID())
	}
}

// manifestCodecs replays dir's badger MANIFEST and counts its live tables by
// codec: badger records each table's compression there (manifest.go:62-66).
func manifestCodecs(t *testing.T, dir string) map[options.CompressionType]int {
	t.Helper()
	fp, err := os.Open(filepath.Join(dir, badgerv4.ManifestFilename))
	if err != nil {
		t.Fatalf("open MANIFEST: %v", err)
	}
	defer fp.Close()
	m, _, err := badgerv4.ReplayManifestFile(fp, 0, badgerv4.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("ReplayManifestFile: %v", err)
	}
	out := make(map[options.CompressionType]int)
	for _, tm := range m.Tables {
		out[tm.Compression]++
	}
	return out
}

// putLabelledNodes writes ids as nodes with label token 7.
func putLabelledNodes(t *testing.T, bs *Store, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 7, nil)); err != nil {
			t.Fatalf("PutNode %d: %v", id, err)
		}
	}
}

// wantLabelledNodes reads ids back and fails on a missing or wrong row.
func wantLabelledNodes(t *testing.T, bs *Store, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		want := types.NodeID(snowflake.ID(id))
		got, err := bs.GetNode(want)
		if err != nil {
			t.Fatalf("GetNode %d: %v", id, err)
		}
		if got.ID() != want || got.PrimaryLabelToken() != 7 {
			t.Fatalf("node %d = id %v label %d, want label 7", id, got.ID(), got.PrimaryLabelToken())
		}
	}
}

// TestNoCompressionWritesUncompressedTables pins that NoCompression reaches
// the tables badger writes: Close flushes the memtable to an L0 table, and
// the manifest must record every table as options.None. Mutation: dropping
// the NoCompression case in buildBadgerOptions writes Snappy tables.
func TestNoCompressionWritesUncompressedTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, NoCompression: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	putLabelledNodes(t, bs, 11, 12, 13)
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	codecs := manifestCodecs(t, dir)
	total := 0
	for _, n := range codecs {
		total += n
	}
	if total == 0 {
		t.Fatal("manifest holds no tables: Close did not flush the memtable")
	}
	if codecs[options.None] != total {
		t.Fatalf("manifest tables by codec = %v, want all %d options.None", codecs, total)
	}
}

// TestNoCompressionRefusedWithCompression pins that NoCompression with a
// non-zero Compression fails at New, on disk and in memory, naming the field.
func TestNoCompressionRefusedWithCompression(t *testing.T) {
	t.Parallel()
	for _, inMem := range []bool{false, true} {
		cfg := Config{InMemory: inMem, NoCompression: true, Compression: options.ZSTD}
		if !inMem {
			cfg.Dir = t.TempDir()
		}
		bs, err := New(cfg)
		if err == nil {
			_ = bs.Close()
			t.Fatalf("InMemory=%v: New accepted NoCompression with Compression ZSTD", inMem)
		}
		if !strings.Contains(err.Error(), "NoCompression") {
			t.Fatalf("InMemory=%v: error %q does not name NoCompression", inMem, err)
		}
	}
}

// TestNoCompressionReopenAcrossSetting pins that tables keep their own codec
// across a change of the setting: Snappy tables written by default read under
// NoCompression, None tables read under the default, and a compaction under
// NoCompression rewrites both kinds as None. Each session's Close flushes one
// L0 table; badger compacts L0 only past NumLevelZeroTables (5, levels.go
// pickCompactLevels), so the test writes more sessions than that.
func TestNoCompressionReopenAcrossSetting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func(noComp bool) *Store {
		t.Helper()
		bs, err := New(Config{Dir: dir, NoCompression: noComp})
		if err != nil {
			t.Fatalf("New(NoCompression=%v): %v", noComp, err)
		}
		return bs
	}
	closeStore := func(bs *Store) {
		t.Helper()
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	bs := open(false)
	putLabelledNodes(t, bs, 21, 22)
	closeStore(bs)

	bs = open(true)
	wantLabelledNodes(t, bs, 21, 22)
	putLabelledNodes(t, bs, 31, 32)
	closeStore(bs)

	if codecs := manifestCodecs(t, dir); codecs[options.Snappy] == 0 || codecs[options.None] == 0 {
		t.Fatalf("manifest tables by codec = %v, want both Snappy and None", codecs)
	}

	bs = open(false)
	wantLabelledNodes(t, bs, 21, 22, 31, 32)
	closeStore(bs)

	all := []int64{21, 22, 31, 32}
	for i := range 6 { // past NumLevelZeroTables, so Flatten compacts L0
		id := int64(41 + i)
		bs = open(true)
		putLabelledNodes(t, bs, id)
		closeStore(bs)
		all = append(all, id)
	}
	bs = open(true)
	if err := bs.DBForTest().Flatten(2); err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	closeStore(bs)
	if codecs := manifestCodecs(t, dir); codecs[options.Snappy] != 0 || codecs[options.None] == 0 {
		t.Fatalf("manifest tables by codec after a compaction under NoCompression = %v, want None only", codecs)
	}

	// Reopen under the default so the reads come from the rewritten tables.
	bs = open(false)
	defer bs.Close()
	wantLabelledNodes(t, bs, all...)
}

// BenchmarkNoCompressionColdRead (NOCOMPRESS) reads every row of a store
// reopened cold, once with Snappy tables and once with None: allocation per
// pass (badger's block decode, table.go:792-815, against mmap slices) and the
// SST bytes on disk. Rows carry compressible text, about 500 B each.
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench NoCompressionColdRead -benchtime 5x
func BenchmarkNoCompressionColdRead(b *testing.B) {
	const rows = 20000
	text := strings.Repeat("process started by service account with arguments ", 10)
	for _, arm := range []struct {
		name   string
		noComp bool
	}{{"snappy", false}, {"none", true}} {
		b.Run(arm.name, func(b *testing.B) {
			dir := b.TempDir()
			cfg := Config{Dir: dir, NoCompression: arm.noComp, BlockCacheSize: 1 << 20}
			bs, err := New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			for i := range rows {
				n := types.NewNode(types.NodeID(snowflake.ID(i+1)), 7, nil)
				_ = n.SetProperty("cmd", fmt.Sprintf("%s%d", text, i))
				if err := bs.PutNode(n); err != nil {
					b.Fatal(err)
				}
			}
			if err := bs.Close(); err != nil {
				b.Fatal(err)
			}
			var sst int64
			_ = filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
				if err == nil && strings.HasSuffix(p, ".sst") {
					sst += fi.Size()
				}
				return nil
			})
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				bs, err := New(cfg)
				if err != nil {
					b.Fatal(err)
				}
				for i := range rows {
					if _, err := bs.GetNode(types.NodeID(snowflake.ID(i + 1))); err != nil {
						b.Fatal(err)
					}
				}
				_ = bs.Close()
			}
			b.ReportMetric(float64(sst)/rows, "sst-B/row")
		})
	}
}
