package badger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	badgerv4 "github.com/dgraph-io/badger/v4"
)

// ErrOversizedWAL is returned when a data dir holds a WAL that cannot be safely
// opened at the configured MemTableSize and the store cannot flush it first —
// instead of letting Badger replay it into a too-small arena and abort the whole
// process via log.Fatal/os.Exit. Two cases: a read-only open (which cannot
// flush) over an oversized WAL, and a WAL written under a memtable larger than
// the 1GB cap (a foreign/older writer) that the migration cannot flush within
// supported bounds. The remedy is a single writable open at a large enough
// MemTableSize to migrate the dir first.
var ErrOversizedWAL = errors.New("graph: oversized WAL cannot be opened at the configured MemTableSize")

// Per-instance footprint tuning bounds. A non-zero Config knob outside its
// range is rejected at New() with a message naming the field, so a bad value
// fails loudly at construction instead of deep inside Badger's Open (or, for
// the memtable floor, with an obscure ValueThreshold panic).
const (
	// minValueLogFileSize / maxValueLogFileSize mirror Badger's own accepted
	// range for ValueLogFileSize: [1MB, 2GB).
	minValueLogFileSize = 1 << 20
	maxValueLogFileSize = 2<<30 - 1
	// minMemTableSize is the practical floor: Badger's Open rejects a
	// MemTableSize where the (default 1MB) ValueThreshold exceeds 15% of it, so
	// memtables below ~6.7MB fail; 8MB is the safe, round floor.
	minMemTableSize = 8 << 20
	maxMemTableSize = 1 << 30
	// minNumCompactors is Badger's documented minimum compactor count.
	minNumCompactors = 2
)

// validateTuningConfig rejects out-of-range per-instance footprint knobs. Zero
// means "keep Badger's stock default" for every knob and always validates.
func validateTuningConfig(cfg Config) error {
	if cfg.ValueLogFileSize != 0 &&
		(cfg.ValueLogFileSize < minValueLogFileSize || cfg.ValueLogFileSize > maxValueLogFileSize) {
		return fmt.Errorf("graph: ValueLogFileSize %d out of range [1MB, 2GB)", cfg.ValueLogFileSize)
	}
	if cfg.MemTableSize != 0 &&
		(cfg.MemTableSize < minMemTableSize || cfg.MemTableSize > maxMemTableSize) {
		return fmt.Errorf("graph: MemTableSize %d out of range [8MB, 1GB]", cfg.MemTableSize)
	}
	if cfg.BlockCacheSize < 0 {
		return fmt.Errorf("graph: BlockCacheSize %d must not be negative", cfg.BlockCacheSize)
	}
	if cfg.NumCompactors != 0 && cfg.NumCompactors < minNumCompactors {
		return fmt.Errorf("graph: NumCompactors %d must be 0 (badger default) or at least 2", cfg.NumCompactors)
	}
	return nil
}

// buildBadgerOptions translates a Config into Badger options. It is the SINGLE
// option-construction path — shared by New's open and the WAL-migration open —
// so the two can never drift (the ai-assisted-soc precedent: four hand-written
// option literals silently diverged). Every per-instance footprint knob is
// applied only when non-zero, so zero preserves Badger's stock default.
func buildBadgerOptions(cfg Config) badgerv4.Options {
	opts := badgerv4.DefaultOptions(cfg.Dir)
	if cfg.InMemory {
		opts = opts.WithInMemory(true)
	}
	if cfg.ReadOnly {
		opts = opts.WithReadOnly(true)
	}
	if cfg.Logger != nil {
		opts = opts.WithLogger(cfg.Logger)
	} else {
		opts = opts.WithLogger(nil) // suppress default Badger logs
	}
	if cfg.SyncWrites && !cfg.ReadOnly {
		opts = opts.WithSyncWrites(true)
	}
	if cfg.Compression != 0 {
		opts = opts.WithCompression(cfg.Compression)
	}
	if cfg.ZSTDCompressionLevel > 0 {
		opts = opts.WithZSTDCompressionLevel(cfg.ZSTDCompressionLevel)
	}
	if cfg.ValueLogFileSize > 0 {
		opts = opts.WithValueLogFileSize(cfg.ValueLogFileSize)
	}
	if cfg.MemTableSize > 0 {
		opts = opts.WithMemTableSize(cfg.MemTableSize)
	}
	if cfg.BlockCacheSize > 0 {
		opts = opts.WithBlockCacheSize(cfg.BlockCacheSize)
	}
	if cfg.NumCompactors > 0 {
		opts = opts.WithNumCompactors(cfg.NumCompactors)
	}
	return opts
}

// MigrateOversizedWAL one-time flushes WAL files written under a LARGER
// MemTableSize than cfg now configures, so a subsequent open at the smaller
// size does not fail with "Arena too small" while replaying them.
//
// The failure mode (caught only by a real production data dump, never by unit
// tests against a clean Close): a data dir copied from a RUNNING server — or
// left by a crash — holds live `.mem` WAL files. Badger replays each WAL into
// an arena sized by the CURRENT MemTableSize; entries beyond it abort the open.
// A clean Close flushes memtables to SSTs and deletes the WALs, so this is
// invisible until you meet real data.
//
// Badger creates each WAL at exactly 2x the memtable that wrote it, so the
// original size is recoverable from the file size. One open/close cycle at that
// recovered size flushes and deletes the WALs; the caller then opens at the
// tuned size. Runs only when an oversized WAL actually exists — clean dirs (no
// `.mem` files at all, or none larger than 2x the configured memtable) skip
// straight through.
//
// IMPORTANT (read-only probe): a read-only Badger open replays WALs into the
// same MemTableSize-bounded arena (badger memtable.go openMemTables runs before
// the read-only branch), so it ALSO fails on an oversized WAL — and "Arena too
// small" is not ErrTruncateNeeded. The tiered store therefore calls this
// EXPLICITLY before its read-only recovery probe; New runs it on the writable
// open. Both are gated to a writable, on-disk store with MemTableSize > 0.
// Idempotent: a second call after the flush finds nothing to do.
func MigrateOversizedWAL(cfg Config) error {
	if cfg.Dir == "" || cfg.InMemory || cfg.MemTableSize <= 0 {
		return nil
	}
	maxWAL, err := maxWALFileSize(cfg.Dir)
	if err != nil {
		return err
	}
	if maxWAL <= 2*cfg.MemTableSize {
		return nil // WALs (if any) fit the configured arena
	}

	// The flush open must size its arena to the WAL's ORIGINAL memtable
	// (maxWAL/2). If that exceeds the 1GB cap, the WAL was written under a
	// memtable larger than this library supports (a foreign or pre-cap writer):
	// opening at the clamped 1GB would replay it into a too-small arena and
	// os.Exit — the exact crash this function exists to prevent. Fail closed
	// BEFORE opening instead.
	recovered := maxWAL / 2
	if recovered > maxMemTableSize {
		return fmt.Errorf("%w: WAL in %s is %d bytes (written under a memtable larger than the 1GB cap); cannot flush within supported bounds",
			ErrOversizedWAL, cfg.Dir, maxWAL)
	}

	// Open once at the WALs' original memtable size to flush them, then close
	// (which deletes the flushed WALs). Reuse buildBadgerOptions so the flush
	// open matches the real open in every other respect. ReadOnly is forced
	// off: we must write to flush.
	flushCfg := cfg
	flushCfg.ReadOnly = false
	flushCfg.MemTableSize = recovered
	if cfg.Logger != nil {
		cfg.Logger.Infof("graph: flushing oversized WAL before memtable shrink: dir=%s wal_bytes=%d flush_memtable=%d target_memtable=%d",
			cfg.Dir, maxWAL, flushCfg.MemTableSize, cfg.MemTableSize)
	}
	db, err := badgerv4.Open(buildBadgerOptions(flushCfg))
	if err != nil {
		return fmt.Errorf("graph: WAL migration open %s: %w", cfg.Dir, err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("graph: WAL migration close %s: %w", cfg.Dir, err)
	}
	return nil
}

// guardReadOnlyOversizedWAL returns ErrOversizedWAL when a read-only open at
// cfg.MemTableSize would replay an oversized WAL and terminate the process. A
// read-only open cannot flush, so there is no in-place recovery — the caller
// must do one writable open (which migrates) first. Gated by New() to
// on-disk, tuned, read-only opens.
func guardReadOnlyOversizedWAL(cfg Config) error {
	maxWAL, err := maxWALFileSize(cfg.Dir)
	if err != nil {
		return err
	}
	if maxWAL <= 2*cfg.MemTableSize {
		return nil
	}
	return fmt.Errorf("%w: read-only open at MemTableSize=%d cannot replay the %d-byte WAL in %s; open writable once to migrate first",
		ErrOversizedWAL, cfg.MemTableSize, maxWAL, cfg.Dir)
}

// maxWALFileSize returns the largest apparent .mem (WAL) file size in dir, or 0
// if the dir has none or does not exist. Badger creates each WAL at exactly 2x
// the memtable that wrote it, so this is how the original memtable size is
// recovered.
func maxWALFileSize(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // fresh dir — nothing to scan
		}
		return 0, fmt.Errorf("graph: scan %s for WAL files: %w", dir, err)
	}
	var maxWAL int64
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".mem" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, fmt.Errorf("graph: stat WAL %s: %w", e.Name(), err)
		}
		if info.Size() > maxWAL {
			maxWAL = info.Size()
		}
	}
	return maxWAL, nil
}
