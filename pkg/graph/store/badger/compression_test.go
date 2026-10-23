package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/dgraph-io/badger/v4/options"
)

// TestCompression_ReachesBadgerOptions pins that Config.Compression and
// ZSTDCompressionLevel reach the Badger options, and that the zero value
// (options.None) keeps Badger's default codec. Mutation: dropping the WithCompression call, or
// applying it for the zero value, fails a row.
func TestCompression_ReachesBadgerOptions(t *testing.T) {
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
