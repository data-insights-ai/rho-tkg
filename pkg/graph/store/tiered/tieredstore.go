package tiered

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	badger "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
)

// Default configuration values for Store.
const (
	defaultShardWindow = 7 * 24 * time.Hour // 1 week
)

// Config configures a Store instance.
type Config struct {
	// DataDir is the root data directory. Required unless InMemory is true.
	DataDir string
	// InMemory creates in-memory shards (testing only).
	InMemory bool
	// RefLabels are entity labels classified as reference (long-lived).
	// All other labels default to event classification.
	RefLabels []string
	// ShardWindow is the time window for event shards. Default: 1 week.
	ShardWindow time.Duration
	// CacheCapacity is the per-shard LRU capacity. Default: 10,000.
	CacheCapacity int
	// FlushInterval is the per-shard flush interval. Default: 100ms.
	FlushInterval time.Duration
	// ColdAfter demotes warm shards to cold after this duration. 0 = never demote.
	ColdAfter time.Duration
	// IdleTimeout closes idle cold shards after this duration. 0 = never close.
	// Default: 5 minutes when ColdAfter > 0.
	IdleTimeout time.Duration
	// Compression sets the SSTable compression algorithm for all shards.
	// Valid values: options.None (0), options.Snappy (1), options.ZSTD (2).
	// Zero keeps the Badger default (Snappy).
	Compression options.CompressionType
	// ZSTDCompressionLevel sets the ZSTD compression level (1-15) for all shards.
	// Only effective when Compression is options.ZSTD.
	// Zero keeps the Badger default (1).
	ZSTDCompressionLevel int
}

// EventShard wraps a BadgerStore with metadata for an event shard.
type EventShard struct {
	name       string
	store      *BadgerStore // nil when cold + closed (lazy-open)
	tier       ShardTier
	timeStart  time.Time    // shard window start (inclusive)
	timeEnd    time.Time    // shard window end (exclusive)
	readOnly   bool         // warm/cold shards are read-only
	path       string       // relative path for lazy-open (e.g., "events/2026-W10")
	shardMu    sync.Mutex   // protects lazy open/close
	activeReqs atomic.Int64 // outstanding read requests; blocks idle-close
	lastAccess atomic.Int64 // unix ms, idle-close tracking
}

// Store implements the Store interface by routing entities across
// multiple BadgerStore instances based on ontology classification.
//
// Reference entities (Case, Organization, User) live in refShard.
// Event entities (Signal, Alert) live in time-windowed event shards.
// Phase 3a: exactly one hot event shard. Phases 3b-3e add warm/cold/archive.
type Store struct {
	mu                sync.RWMutex                // protects hotShard + eventShards during rotation
	refShard          *BadgerStore                // reference shard (always hot)
	refArchive        atomic.Pointer[BadgerStore] // nil until first archive/restore or DepthAll with archive catalog; atomic so reads need not hold archiveMu
	archiveMu         sync.Mutex                  // serializes lazy-open of refArchive (single-flight)
	archiveActiveReqs atomic.Int64                // refcount for refArchive — Close spin-waits on this before archive.Close()
	eventShards       map[string]*EventShard      // name -> event shard
	hotShard          *EventShard                 // convenience pointer to current hot shard
	ontology          *OntologyMapping
	catalog           *ShardCatalog
	regFile           string // path to registry.msgpack
	dataDir           string
	inMemory          bool
	shardWindow       time.Duration
	cacheCap          int
	flushInt          time.Duration
	coldAfter         time.Duration
	idleTimeout       time.Duration
	compression       options.CompressionType
	zstdLevel         int
	closeCh           chan struct{} // signals idle-close goroutine to stop
	closeOnce         sync.Once
	closed            atomic.Bool // set under archiveMu inside Close before tearing the archive down;
	// readers consult this from ensureRefArchive to refuse re-opening the archive after Close
	// has already closed it (prevents an orphan re-open + leaked DB handle)

	// Temporal indexes — tracked so new hot shards inherit them on rotation.
	tempIdxMu     sync.Mutex
	tempIdxLabels []uint16

	// Vector indexes — in-memory brute-force k-NN index spanning all shards.
	// Not persisted; must be rebuilt via CreateVectorIndex after restart.
	vectorIdxMu   sync.RWMutex
	vectorIndexes map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex
}

// New creates a Store with a reference shard and one hot event shard.
func New(cfg Config) (*Store, error) {
	if !cfg.InMemory && cfg.DataDir == "" {
		return nil, fmt.Errorf("graph: Config.DataDir required unless InMemory")
	}

	window := cfg.ShardWindow
	if window == 0 {
		window = defaultShardWindow
	}
	if window < time.Minute {
		return nil, fmt.Errorf("graph: Config.ShardWindow must be >= 1 minute, got %v", window)
	}
	cacheCap := cfg.CacheCapacity
	if cacheCap <= 0 {
		cacheCap = badger.DefaultCacheCapacity
	}
	flushInt := cfg.FlushInterval
	if flushInt == 0 {
		flushInt = badger.DefaultFlushInterval
	}

	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 && cfg.ColdAfter > 0 {
		idleTimeout = 5 * time.Minute
	}

	ts := &Store{
		eventShards:   make(map[string]*EventShard),
		ontology:      NewOntologyMapping(cfg.RefLabels),
		dataDir:       cfg.DataDir,
		inMemory:      cfg.InMemory,
		shardWindow:   window,
		cacheCap:      cacheCap,
		flushInt:      flushInt,
		coldAfter:     cfg.ColdAfter,
		idleTimeout:   idleTimeout,
		compression:   cfg.Compression,
		zstdLevel:     cfg.ZSTDCompressionLevel,
		closeCh:       make(chan struct{}),
		vectorIndexes: make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex),
	}

	// Create directory layout for disk-backed stores.
	if !cfg.InMemory {
		dirs := []string{
			filepath.Join(cfg.DataDir, "meta"),
			filepath.Join(cfg.DataDir, "reference"),
			filepath.Join(cfg.DataDir, "events"),
			filepath.Join(cfg.DataDir, "archive"),
		}
		for _, d := range dirs {
			if err := os.MkdirAll(d, 0o750); err != nil {
				return nil, fmt.Errorf("graph: create dir %s: %w", d, err)
			}
		}
		ts.regFile = filepath.Join(cfg.DataDir, "meta", "registry.msgpack")
		ts.catalog = NewShardCatalog(filepath.Join(cfg.DataDir, "meta", "shard_catalog.json"))
	} else {
		ts.catalog = NewShardCatalog("") // in-memory catalog (never persisted)
	}

	// Load existing catalog.
	if !cfg.InMemory {
		if err := ts.catalog.Load(); err != nil {
			return nil, fmt.Errorf("graph: load catalog: %w", err)
		}
	}

	// Open reference shard.
	refStore, err := ts.openBadgerStore("reference", false)
	if err != nil {
		return nil, fmt.Errorf("graph: open reference shard: %w", err)
	}
	ts.refShard = refStore

	// Register reference shard in catalog if new.
	if _, ok := ts.catalog.GetShard("reference"); !ok {
		ts.catalog.AddShard(ShardEntry{
			Name:   "reference",
			Kind:   ShardReference,
			Tier:   TierHot,
			Path:   "data/reference",
			Labels: cfg.RefLabels,
		})
	}

	// Open hot event shard.
	now := time.Now()
	hotName := shardWindowName(now, window)
	windowStart := shardWindowStart(now, window)
	windowEnd := windowStart.Add(window)

	// Check if a hot shard already exists in catalog (mid-window restart).
	if existing, ok := ts.catalog.HotEventShard(); ok {
		hotName = existing.Name
		windowStart = existing.TimeStart
		windowEnd = existing.TimeEnd
	}

	hotDir := hotName
	if !cfg.InMemory {
		hotDir = filepath.Join("events", hotName)
	}
	hotStore, err := ts.openBadgerStore(hotDir, false)
	if err != nil {
		_ = refStore.Close() // best-effort cleanup; returning primary error
		return nil, fmt.Errorf("graph: open hot event shard: %w", err)
	}

	es := &EventShard{
		name:      hotName,
		store:     hotStore,
		tier:      TierHot,
		timeStart: windowStart,
		timeEnd:   windowEnd,
		readOnly:  false,
		path:      hotDir,
	}
	ts.eventShards[hotName] = es
	ts.hotShard = es

	// Register hot shard in catalog if new.
	if _, ok := ts.catalog.GetShard(hotName); !ok {
		ts.catalog.AddShard(ShardEntry{
			Name:      hotName,
			Kind:      ShardEvent,
			Tier:      TierHot,
			Path:      hotDir,
			TimeStart: windowStart,
			TimeEnd:   windowEnd,
		})
	}

	// Reopen warm and cold event shards from catalog.
	for _, entry := range ts.catalog.EventShards() {
		switch entry.Tier {
		case TierWarm:
			// Read-only on disk; in-memory shards stay read-write (Badger limitation).
			var warmStore *BadgerStore
			var err error
			if cfg.InMemory {
				warmStore, err = ts.openBadgerStore(entry.Path, false)
			} else {
				warmStore, err = ts.openBadgerStoreWithRecovery(entry.Path)
			}
			if err != nil {
				// Clean up already-opened warm shards to prevent file handle leaks.
				for _, es := range ts.eventShards {
					if es.store != nil {
						_ = es.store.Close()
					}
				}
				_ = hotStore.Close()
				_ = refStore.Close()
				return nil, fmt.Errorf("graph: open warm shard %s: %w", entry.Name, err)
			}
			warmES := &EventShard{
				name:      entry.Name,
				store:     warmStore,
				tier:      TierWarm,
				timeStart: entry.TimeStart,
				timeEnd:   entry.TimeEnd,
				readOnly:  true,
				path:      entry.Path,
			}
			ts.eventShards[entry.Name] = warmES
		case TierCold:
			// Cold shards are NOT opened on startup — lazy-open on first access.
			coldES := &EventShard{
				name:      entry.Name,
				store:     nil, // lazy-open
				tier:      TierCold,
				timeStart: entry.TimeStart,
				timeEnd:   entry.TimeEnd,
				readOnly:  true,
				path:      entry.Path,
			}
			ts.eventShards[entry.Name] = coldES
		}
	}

	// Recover archive shard from catalog (lazy — just register, don't open).
	// The archive will be opened on first access via ensureRefArchive().

	// Persist catalog.
	if !cfg.InMemory {
		if err := ts.catalog.Save(); err != nil {
			_ = hotStore.Close() // best-effort cleanup; returning primary error
			_ = refStore.Close() // best-effort cleanup; returning primary error
			return nil, fmt.Errorf("graph: save catalog: %w", err)
		}
	}

	// Start idle-close goroutine for cold shards.
	if ts.idleTimeout > 0 {
		go ts.idleCloseLoop()
	}

	return ts, nil
}

// Close closes all shards and saves the catalog. Idempotent via sync.Once.
func (ts *Store) Close() error {
	var closeErr error
	ts.closeOnce.Do(func() {
		// Mark the store as closed BEFORE tearing the archive down. The flag
		// is consulted by ensureRefArchive (under archiveMu) so a concurrent
		// reader that observed refArchive==nil cannot lazy-open a fresh
		// archive after Close has already closed it (which would leak a DB
		// handle). archiveMu is taken so the close-vs-open transition is a
		// single critical section: either the open completes before close
		// runs, or close runs first and the open returns ErrStoreClosed.
		ts.archiveMu.Lock()
		ts.closed.Store(true)
		archive := ts.refArchive.Load()
		ts.refArchive.Store(nil)
		ts.archiveMu.Unlock()

		// Stop the idle-close goroutine.
		close(ts.closeCh)

		// Save catalog.
		if !ts.inMemory && ts.catalog != nil {
			if err := ts.catalog.Save(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: save catalog on close: %w", err))
			}
		}

		// Wait for any in-flight checkouts to drain before closing event
		// shard stores. Badger v4 WriteBatch.Flush blocks forever on a
		// closed DB (CLAUDE.md: closeIdleShards uses the same pattern), so
		// closing while a long-running RunRepair/VerifyShard still holds a
		// checkout would deadlock that caller. Spin-wait with a short
		// sleep — Close is rare and the wait is bounded by whatever
		// outermost admin call is in flight.
		for _, es := range ts.eventShards {
			for es.activeReqs.Load() > 0 {
				time.Sleep(time.Millisecond)
			}
		}

		// Close all event shards. Cold shards may have nil stores.
		for _, es := range ts.eventShards {
			if es.store != nil {
				if err := es.store.Close(); err != nil {
					closeErr = errors.Join(closeErr, fmt.Errorf("graph: close event shard %s: %w", es.name, err))
				}
			}
		}

		// Close reference archive if it was open at close time. Drain
		// archiveActiveReqs first so a concurrent AllNodeHistoryIDs /
		// ForEachNodeHistoryID / similar archive reader cannot race the
		// underlying db.Close() — same Badger v4 Flush-on-closed-DB
		// concern as event shards above.
		if archive != nil {
			for ts.archiveActiveReqs.Load() > 0 {
				time.Sleep(time.Millisecond)
			}
			if err := archive.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: close ref archive: %w", err))
			}
		}

		// Close reference shard.
		if ts.refShard != nil {
			if err := ts.refShard.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: close ref shard: %w", err))
			}
		}
	})
	return closeErr
}

// Clear clears all open shards. Cold shards with nil stores are skipped.
// Also resets store-level state that lives on the Store itself rather
// than on individual shards: the vector-index map and the tracked temporal
// index labels list (which would otherwise re-install temporal indexes for
// stale labels on the next hot-shard rotation).
//
// Concurrency: each open event shard is pinned via checkoutStore for the
// duration of its Clear() call. Without the pin, Close (which doesn't
// take ts.mu and only spin-waits on activeReqs) could free the
// underlying DB while Clear was still touching it — Badger v4 Flush on
// a closed DB blocks forever.
//
// Note: the snapshot-then-clear pattern races a concurrent ForceRotate
// that could replace the hot shard between the snapshot and its Clear().
// The new hot shard would survive uncleared. This is pre-existing
// behaviour shared by all snapshot-based admin paths (ListShards,
// RebuildCatalog, index-create/drop). Treat Clear as admin-only and
// serialise externally against rotation.
func (ts *Store) Clear() error {
	ts.mu.RLock()
	shards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		shards = append(shards, es)
	}
	ts.mu.RUnlock()

	if err := ts.refShard.Clear(); err != nil {
		return fmt.Errorf("graph: clear ref shard: %w", err)
	}
	for _, es := range shards {
		// Best-effort skip for shards observed cold-and-empty under the
		// snapshot RLock above. This is an OPTIMIZATION, not a strict
		// guarantee: a concurrent caller could lazy-open this shard via
		// checkoutStore between our nil-check and the loop below. That's
		// acceptable — Clear is admin-only (not a concurrent-safe API)
		// and the worst case is we skip a freshly-opened shard, which
		// will simply contain post-Clear writes that the next admin
		// caller can address. The pin discipline below is what protects
		// us from Close racing the Clear call itself.
		if es.store == nil {
			continue
		}
		store, coErr := es.checkoutStore(ts)
		if coErr != nil {
			// Close started after our snapshot — skip rather than crash.
			continue
		}
		err := store.Clear()
		es.checkinStore()
		if err != nil {
			return fmt.Errorf("graph: clear event shard %s: %w", es.name, err)
		}
	}

	// Reset Store-level state. Without this, post-Clear callers see
	// "already exists" on a logically empty store (vectorIndexes), and the
	// next rotation re-creates temporal indexes on the new hot shard for
	// labels that were dropped along with the shard data (tempIdxLabels).
	ts.vectorIdxMu.Lock()
	ts.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)
	ts.vectorIdxMu.Unlock()

	ts.tempIdxMu.Lock()
	ts.tempIdxLabels = nil
	ts.tempIdxMu.Unlock()

	// Skip the archive checkout entirely when neither the in-memory pointer
	// nor the catalog has an archive. checkoutArchive would otherwise be a
	// no-op too, but bailing early makes the intent explicit and avoids the
	// theoretical lazy-open-then-clear churn if the archive is on disk but
	// has not been opened this session.
	if ts.refArchive.Load() == nil && !ts.hasArchiveShard() {
		return nil
	}

	// Pin via checkoutArchive — see resolveShardStore("archive") doc.
	// A raw refArchive.Load() races Close, which drains archiveActiveReqs
	// (sees 0) and proceeds to archive.Close() while Clear is still
	// touching the DB → Badger v4 Flush-on-closed-DB hang.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		if err := archive.Clear(); err != nil {
			archiveCheckin()
			return fmt.Errorf("graph: clear ref archive: %w", err)
		}
		archiveCheckin()
	}
	return nil
}

// --- Internal helpers ---

// openBadgerStore creates a new BadgerStore with the configured defaults.
// For disk-backed stores, name is the relative path under DataDir.
// readOnly opens Badger in read-only mode (no flushLoop, no gcLoop).
func (ts *Store) openBadgerStore(name string, readOnly bool) (*BadgerStore, error) {
	cfg := BadgerStoreConfig{
		InMemory:             ts.inMemory,
		CacheCapacity:        ts.cacheCap,
		FlushInterval:        ts.flushInt,
		ReadOnly:             readOnly,
		Compression:          ts.compression,
		ZSTDCompressionLevel: ts.zstdLevel,
	}
	if !ts.inMemory {
		cfg.Dir = filepath.Join(ts.dataDir, name)
	}
	return NewBadgerStore(cfg)
}

// openBadgerStoreWithRecovery opens a BadgerStore in read-only mode. If the WAL
// is corrupt (ErrTruncateNeeded from an unclean shutdown), it recovers by
// opening read-write (which auto-truncates the corrupt tail), closing, and
// reopening read-only. The truncation discards at most one flush window (~100ms)
// of buffered writes that were not fsynced before the crash.
func (ts *Store) openBadgerStoreWithRecovery(name string) (*BadgerStore, error) {
	store, err := ts.openBadgerStore(name, true)
	if err == nil {
		return store, nil
	}
	if !isTruncateNeeded(err) {
		return nil, err
	}
	slog.Warn("graph: recovering corrupt WAL by truncation", "shard", name)
	rwStore, err := ts.openBadgerStore(name, false)
	if err != nil {
		return nil, fmt.Errorf("graph: recovery open (read-write) %s: %w", name, err)
	}
	if err := rwStore.Close(); err != nil {
		return nil, fmt.Errorf("graph: recovery close %s: %w", name, err)
	}
	return ts.openBadgerStore(name, true)
}

// isTruncateNeeded checks whether the error indicates a Badger WAL truncation
// is required. Badger v4's y.Wrap uses %+v (not %w) when debugMode is off,
// which breaks errors.Is(). We fall back to string matching on the sentinel
// error message as a secondary check.
func isTruncateNeeded(err error) bool {
	if errors.Is(err, badgerv4.ErrTruncateNeeded) {
		return true
	}
	return strings.Contains(err.Error(), badgerv4.ErrTruncateNeeded.Error())
}
