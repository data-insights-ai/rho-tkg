package tieredstore

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

	snowflake "github.com/bds421/rho-snowflake-2026"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/badgerstore"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Default configuration values for TieredStore.
const (
	defaultShardWindow = 7 * 24 * time.Hour // 1 week
)

// TieredStoreConfig configures a TieredStore instance.
type TieredStoreConfig struct {
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

// getStore returns the BadgerStore for this shard, lazily opening it if cold.
// For hot/warm shards: zero overhead (direct pointer return).
// For cold shards: acquires shardMu, opens BadgerStore if nil, updates lastAccess.
func (es *EventShard) getStore(ts *TieredStore) (*BadgerStore, error) {
	if es.tier != TierCold {
		return es.store, nil // hot/warm: zero overhead
	}
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store != nil {
		es.lastAccess.Store(time.Now().UnixMilli())
		return es.store, nil
	}
	store, err := ts.openBadgerStoreWithRecovery(es.path)
	if err != nil {
		return nil, fmt.Errorf("graph: lazy-open cold shard %s: %w", es.name, err)
	}
	es.store = store
	es.lastAccess.Store(time.Now().UnixMilli())
	return store, nil
}

// checkoutStore returns the BadgerStore and increments activeReqs to prevent
// idle-close from closing the store while the caller is still using it.
// Caller must call checkinStore() when done (typically via defer).
//
// For hot/warm shards: zero overhead (direct pointer return + atomic increment).
// For cold shards: acquires shardMu, opens if nil, increments activeReqs while
// still holding the lock to prevent TOCTOU race with closeIdleShards.
func (es *EventShard) checkoutStore(ts *TieredStore) (*BadgerStore, error) {
	// Refuse new checkouts after Close has been initiated. Close drains
	// activeReqs per shard before es.store.Close(), so a checkout that
	// races past the drain would see its store closed under it. The
	// activeReqs increment must therefore be guarded by the same flag
	// Close consults; without this gate the spin-wait in Close gives
	// only the illusion of safety. Mirrors ensureRefArchive's
	// ErrStoreClosed semantics.
	if ts.closed.Load() {
		return nil, ErrStoreClosed
	}
	if es.tier != TierCold {
		// Hot/warm: stores are always open, never closed by idle-close.
		es.activeReqs.Add(1)
		es.lastAccess.Store(time.Now().UnixMilli())
		// Re-check closed AFTER the increment so a concurrent Close that
		// observed activeReqs == 0 between our load and our add doesn't
		// proceed to close the store. If we lost the race, decrement and
		// surface ErrStoreClosed to the caller.
		if ts.closed.Load() {
			es.activeReqs.Add(-1)
			return nil, ErrStoreClosed
		}
		return es.store, nil
	}
	// Cold shard: acquire shardMu to atomically open + increment activeReqs.
	// This prevents closeIdleShards from closing the store between open and
	// the activeReqs increment (the v3.0.30 TOCTOU bug).
	es.shardMu.Lock()
	if ts.closed.Load() {
		es.shardMu.Unlock()
		return nil, ErrStoreClosed
	}
	if es.store == nil {
		store, err := ts.openBadgerStoreWithRecovery(es.path)
		if err != nil {
			es.shardMu.Unlock()
			return nil, fmt.Errorf("graph: lazy-open cold shard %s: %w", es.name, err)
		}
		es.store = store
	}
	es.activeReqs.Add(1)
	es.lastAccess.Store(time.Now().UnixMilli())
	store := es.store
	es.shardMu.Unlock()
	return store, nil
}

// checkinStore decrements activeReqs, signalling that the caller is done
// using the store returned by checkoutStore.
func (es *EventShard) checkinStore() {
	es.activeReqs.Add(-1)
}

// checkoutArchive returns the refArchive pointer pinned against a concurrent
// Close. Callers MUST invoke checkin exactly once. If the store has been
// closed or the catalog has no archive shard the returned pointer is nil
// and checkin is a safe no-op — callers should treat nil as "no archive
// available" and skip archive-side work.
//
// Cold-start handling: if the catalog records an archive shard but the
// in-memory pointer is nil (e.g. fresh process restart, no GetNode has
// triggered lazy-open yet), this helper invokes ensureRefArchive so bulk
// scans see archive content without needing a prior point lookup. Without
// this, AllNodes / AllNodeHistoryIDs / similar bulk APIs would silently
// omit archived entities until some unrelated lookup opened the archive.
//
// Concurrency: mirrors EventShard.checkoutStore. Close drains
// archiveActiveReqs before calling archive.Close(), so a checkout active
// at Close-time will be observed and waited for. The post-increment
// closed re-check closes the window where Close already advanced past
// the spin-wait between our load and our increment.
func (ts *TieredStore) checkoutArchive() (*BadgerStore, func(), error) {
	noop := func() {}
	if ts.closed.Load() {
		return nil, noop, nil
	}
	archive := ts.refArchive.Load()
	if archive == nil {
		// Cold-start: open the archive on demand if the catalog says one
		// exists. ensureRefArchive itself refuses to open after Close, so
		// a racing Close still surfaces the right error.
		if !ts.hasArchiveShard() {
			return nil, noop, nil
		}
		if err := ts.ensureRefArchive(); err != nil {
			if errors.Is(err, ErrStoreClosed) {
				return nil, noop, nil
			}
			return nil, noop, err
		}
		archive = ts.refArchive.Load()
		if archive == nil {
			return nil, noop, nil
		}
	}
	ts.archiveActiveReqs.Add(1)
	if ts.closed.Load() {
		ts.archiveActiveReqs.Add(-1)
		return nil, noop, nil
	}
	// Re-load after the increment: Close stores nil into refArchive under
	// archiveMu, and a snapshot taken before the increment may have raced.
	if ts.refArchive.Load() == nil {
		ts.archiveActiveReqs.Add(-1)
		return nil, noop, nil
	}
	return archive, func() { ts.archiveActiveReqs.Add(-1) }, nil
}

// TieredStore implements the Store interface by routing entities across
// multiple BadgerStore instances based on ontology classification.
//
// Reference entities (Case, Organization, User) live in refShard.
// Event entities (Signal, Alert) live in time-windowed event shards.
// Phase 3a: exactly one hot event shard. Phases 3b-3e add warm/cold/archive.
type TieredStore struct {
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

// NewTieredStore creates a TieredStore with a reference shard and one hot event shard.
func NewTieredStore(cfg TieredStoreConfig) (*TieredStore, error) {
	if !cfg.InMemory && cfg.DataDir == "" {
		return nil, fmt.Errorf("graph: TieredStoreConfig.DataDir required unless InMemory")
	}

	window := cfg.ShardWindow
	if window == 0 {
		window = defaultShardWindow
	}
	if window < time.Minute {
		return nil, fmt.Errorf("graph: TieredStoreConfig.ShardWindow must be >= 1 minute, got %v", window)
	}
	cacheCap := cfg.CacheCapacity
	if cacheCap <= 0 {
		cacheCap = badgerstore.DefaultCacheCapacity
	}
	flushInt := cfg.FlushInterval
	if flushInt == 0 {
		flushInt = badgerstore.DefaultFlushInterval
	}

	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 && cfg.ColdAfter > 0 {
		idleTimeout = 5 * time.Minute
	}

	ts := &TieredStore{
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
func (ts *TieredStore) Close() error {
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
// Also resets store-level state that lives on the TieredStore itself rather
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
func (ts *TieredStore) Clear() error {
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

	// Reset TieredStore-level state. Without this, post-Clear callers see
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

// --- Shard routing helpers ---

// shardForNode routes a node to the correct BadgerStore by its primary label.
// For event nodes, always routes to the hot shard (new writes).
func (ts *TieredStore) shardForNode(primaryLabel uint16) *BadgerStore {
	if ts.ontology.ClassifyByToken(primaryLabel) == ClassReference {
		return ts.refShard
	}
	ts.mu.RLock()
	s := ts.hotShard.store
	ts.mu.RUnlock()
	return s
}

// shardForNodeID resolves which shard owns a node ID.
// O(1): try ref (HasNodeID), miss -> archive check -> timestamp extraction -> event shard.
// Returns error if cold shard lazy-open fails.
func (ts *TieredStore) shardForNodeID(id types.NodeID) (*BadgerStore, error) {
	raw := id.SnowflakeID()
	if ts.refShard.HasNodeID(raw) {
		return ts.refShard, nil
	}
	// Check archive if open or catalog says it exists.
	archive := ts.refArchive.Load()
	if archive != nil && archive.HasNodeID(raw) {
		return archive, nil
	}
	if archive == nil && ts.hasArchiveShard() {
		if err := ts.ensureRefArchive(); err != nil {
			return nil, err
		}
		archive = ts.refArchive.Load()
		if archive != nil && archive.HasNodeID(raw) {
			return archive, nil
		}
	}
	return ts.timestampToEventShard(raw)
}

// shardForNodeVersion resolves the shard that owns a node history entry.
// When the current node still exists, shardForNodeID gives the exact owner.
// After a reference node has been deleted, current indexes are gone and ID
// timestamp fallback would select an event shard, so use the snapshot label.
func (ts *TieredStore) shardForNodeVersion(id snowflake.ID, n *types.Node) (*BadgerStore, error) {
	if n != nil && ts.ontology.ClassifyByToken(n.PrimaryLabelToken().Value()) == ClassReference {
		return ts.refShard, nil
	}
	return ts.shardForNodeID(types.NodeID(id))
}

// timestampToEventShard extracts the creation timestamp from a snowflake ID
// and maps it to the correct event shard. Falls back to the hot shard if no
// shard window matches (entity from before the oldest shard).
// Returns error if cold shard lazy-open fails.
func (ts *TieredStore) timestampToEventShard(id snowflake.ID) (*BadgerStore, error) {
	created := storepkg.SnowflakeLayout.CreatedAt(id)

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	for _, es := range ts.eventShards {
		if !created.Before(es.timeStart) && created.Before(es.timeEnd) {
			return es.getStore(ts)
		}
	}
	return ts.hotShard.store, nil // fallback: newest shard (always open)
}

// timestampToEventShardEntry extracts the creation timestamp from a snowflake ID
// and returns the *EventShard responsible for that timestamp. Unlike
// timestampToEventShard, it returns the shard struct so callers can use
// checkoutStore/checkinStore for safe concurrent access.
// Falls back to hotShard if no window matches.
func (ts *TieredStore) timestampToEventShardEntry(id snowflake.ID) *EventShard {
	created := storepkg.SnowflakeLayout.CreatedAt(id)

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	for _, es := range ts.eventShards {
		if !created.Before(es.timeStart) && created.Before(es.timeEnd) {
			return es
		}
	}
	return ts.hotShard // fallback: newest shard (always open)
}

// shardForRelIDChecked resolves the storage shard for a relationship ID and
// increments activeReqs on any event shard returned, mirroring
// shardForNodeIDChecked.
//
// Cross-shard relationships may live in a shard that does not match their
// creation timestamp (the entity is stored in the start node's shard). The
// timestamp candidate is checked first as a fast path, then every other event
// shard (including cold shards) is probed in turn. Cold shards are included
// because a cross-shard rel created while the start-node shard was warm can
// later age to cold — the rel never moves, so the lookup must follow it.
//
// The caller MUST invoke the returned checkin function exactly once.
// refShard checkin is a no-op; event shard checkin decrements activeReqs.
func (ts *TieredStore) shardForRelIDChecked(id types.RelID) (store *BadgerStore, checkin func(), err error) {
	raw := id.SnowflakeID()
	if ts.refShard.HasRelID(raw) {
		return ts.refShard, func() {}, nil
	}

	// Probe refArchive: ArchiveNode migrates a reference node AND its
	// rels to refArchive, so archived rels live there after archive.
	// Pin via checkoutArchive (mirrors the node resolver) so a
	// concurrent Close cannot tear it down mid-use.
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, nil, err
	}
	if archive != nil {
		if archive.HasRelID(raw) {
			return archive, archiveCheckin, nil
		}
		archiveCheckin()
	}

	candidateEntry := ts.timestampToEventShardEntry(raw)
	candidate, err := candidateEntry.checkoutStore(ts)
	if err != nil {
		return nil, nil, err
	}
	if candidate.HasRelID(raw) {
		return candidate, func() { candidateEntry.checkinStore() }, nil
	}

	// Probe every other event shard (excluding the candidate we already
	// checked). Cold shards are included so a cross-shard rel that aged to
	// cold is still found.
	ts.mu.RLock()
	probe := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if es == candidateEntry {
			continue
		}
		probe = append(probe, es)
	}
	ts.mu.RUnlock()

	for _, es := range probe {
		s, err := es.checkoutStore(ts)
		if err != nil {
			candidateEntry.checkinStore()
			return nil, nil, err
		}
		if s.HasRelID(raw) {
			candidateEntry.checkinStore()
			return s, func() { es.checkinStore() }, nil
		}
		es.checkinStore()
	}

	// Not found anywhere — return the timestamp candidate so downstream reads
	// surface a typed ErrRelNotFound. The caller still owns its checkin.
	return candidate, func() { candidateEntry.checkinStore() }, nil
}

// shardForNodeIDChecked resolves the storage shard for a node ID and increments
// its activeReqs counter so closeIdleShards cannot close the DB while the caller
// is using the returned store pointer.
//
// The caller MUST invoke the returned checkin function exactly once when done
// (typically via defer checkin()). Failing to call checkin() leaks activeReqs,
// which prevents idle cold shards from ever being closed.
//
// refShard and refArchive are never subject to idle-close, so their checkin is
// a no-op. Only event shards (especially cold tier) need the checkout/checkin
// protocol.
func (ts *TieredStore) shardForNodeIDChecked(id types.NodeID) (store *BadgerStore, checkin func(), err error) {
	raw := id.SnowflakeID()
	if ts.refShard.HasNodeID(raw) {
		return ts.refShard, func() {}, nil // refShard: never closed, no-op checkin
	}

	// Archive probe — pin via checkoutArchive so a concurrent Close
	// cannot tear down the underlying DB while the caller is using
	// the returned pointer for GetNode / Update / etc. The previous
	// no-op checkin was incorrect: refArchive IS closed by Close, just
	// not by closeIdleShards. checkoutArchive also handles cold-start
	// lazy-open via ensureRefArchive when the catalog has the entry but
	// the in-memory pointer is nil.
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, nil, err
	}
	if archive != nil {
		if archive.HasNodeID(raw) {
			return archive, archiveCheckin, nil
		}
		archiveCheckin()
	}

	// Event shard: resolve via timestamp, then checkout to prevent idle-close race.
	es := ts.timestampToEventShardEntry(raw)
	store, err = es.checkoutStore(ts)
	if err != nil {
		return nil, nil, err
	}
	return store, func() { es.checkinStore() }, nil
}

// --- Rotation and depth helpers ---

// RotateHotShard demotes the current hot shard to warm and creates a new hot shard.
// Caller must hold ts.mu.Lock.
func (ts *TieredStore) RotateHotShard() error {
	oldHot := ts.hotShard
	now := time.Now()

	// Flush pending writes to the old hot shard.
	if err := oldHot.store.Flush(); err != nil {
		return fmt.Errorf("graph: flush hot shard before rotation: %w", err)
	}

	// Mark old hot as warm. Adjust timeEnd to the actual rotation time so that
	// entities created during this shard's tenure still resolve correctly via
	// snowflake ID timestamp extraction.
	oldHot.tier = TierWarm
	oldHot.readOnly = true
	// Align boundary to millisecond precision + 1ms: snowflake IDs encode time at
	// millisecond resolution. Adding 1ms ensures entities created in the same
	// millisecond as the rotation (which are in the old shard) resolve correctly.
	boundary := now.Truncate(time.Millisecond).Add(time.Millisecond)
	oldHot.timeEnd = boundary
	ts.catalog.UpdateShardTier(oldHot.name, TierWarm)
	ts.catalog.UpdateShardTimeEnd(oldHot.name, boundary)

	// Create new hot shard.
	newName := shardWindowName(now, ts.shardWindow)
	windowStart := boundary // new shard starts at next ms after rotation (contiguous)
	windowEnd := shardWindowStart(now, ts.shardWindow).Add(ts.shardWindow)

	// If the computed name collides with the old hot shard (rotation within
	// the same window — can happen with forced rotation or edge timing),
	// append a disambiguating suffix.
	if _, exists := ts.eventShards[newName]; exists {
		newName = fmt.Sprintf("%s-%d", newName, now.UnixNano())
	}

	newDir := newName
	if !ts.inMemory {
		newDir = filepath.Join("events", newName)
	}
	newStore, err := ts.openBadgerStore(newDir, false)
	if err != nil {
		return fmt.Errorf("graph: open new hot shard: %w", err)
	}

	// Set up temporal indexes on the new hot shard before it begins accepting writes.
	ts.tempIdxMu.Lock()
	for _, tok := range ts.tempIdxLabels {
		// Errors other than ErrTemporalIndexExists are unexpected on a fresh shard.
		if err := newStore.CreateTemporalIndex(tok); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			slog.Error("graph: create temporal index on new hot shard", "token", tok, "error", err)
		}
	}
	ts.tempIdxMu.Unlock()

	newES := &EventShard{
		name:      newName,
		store:     newStore,
		tier:      TierHot,
		timeStart: windowStart,
		timeEnd:   windowEnd,
		readOnly:  false,
		path:      newDir,
	}
	ts.eventShards[newName] = newES
	ts.hotShard = newES

	// Register in catalog and persist.
	ts.catalog.AddShard(ShardEntry{
		Name:      newName,
		Kind:      ShardEvent,
		Tier:      TierHot,
		Path:      newDir,
		TimeStart: windowStart,
		TimeEnd:   windowEnd,
	})

	// Demote warm shards to cold if they exceed ColdAfter threshold.
	if ts.coldAfter > 0 {
		coldThreshold := now.Add(-ts.coldAfter)
		for _, es := range ts.eventShards {
			if es.tier == TierWarm && es.timeEnd.Before(coldThreshold) {
				es.tier = TierCold
				ts.catalog.UpdateShardTier(es.name, TierCold)
				// Do NOT close the store here — let idle-close handle it safely
				// (avoids closing while in-flight reads hold pointers from snapshots).
			}
		}
	}

	if !ts.inMemory {
		if err := ts.catalog.Save(); err != nil {
			return fmt.Errorf("graph: save catalog after rotation: %w", err)
		}
	}

	return nil
}

// checkRotation checks if the hot shard's time window has expired and rotates if needed.
// Fast path: single time comparison (~1ns). Slow path: acquire Lock, double-check, rotate.
func (ts *TieredStore) checkRotation() error {
	ts.mu.RLock()
	hotEnd := ts.hotShard.timeEnd
	ts.mu.RUnlock()

	if time.Now().Before(hotEnd) {
		return nil // fast path: within window
	}

	// Slow path: acquire write lock and rotate.
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Double-check after acquiring write lock.
	if time.Now().Before(ts.hotShard.timeEnd) {
		return nil // another goroutine already rotated
	}

	return ts.RotateHotShard()
}

// idleCloseLoop periodically checks cold shards and closes those that have been
// idle longer than IdleTimeout. Runs every IdleTimeout/2. Stopped via closeCh.
func (ts *TieredStore) idleCloseLoop() {
	tick := time.NewTicker(ts.idleTimeout / 2)
	defer tick.Stop()

	for {
		select {
		case <-ts.closeCh:
			return
		case <-tick.C:
			ts.closeIdleShards()
		}
	}
}

// closeIdleShards closes cold shards that have been idle longer than idleTimeout.
func (ts *TieredStore) closeIdleShards() {
	nowMs := time.Now().UnixMilli()
	thresholdMs := ts.idleTimeout.Milliseconds()

	ts.mu.RLock()
	var coldShards []*EventShard
	for _, es := range ts.eventShards {
		if es.tier == TierCold {
			coldShards = append(coldShards, es)
		}
	}
	ts.mu.RUnlock()

	for _, es := range coldShards {
		es.shardMu.Lock()
		if es.store != nil && es.activeReqs.Load() == 0 && (nowMs-es.lastAccess.Load()) > thresholdMs {
			_ = es.store.Close() // idle eviction; Close error can't be surfaced to any caller
			es.store = nil
		}
		es.shardMu.Unlock()
	}
}

// eventShardSnapshot returns a snapshot of event shards filtered by depth.
// Caller must hold at least ts.mu.RLock. Returns []*EventShard so callers
// can use es.getStore(ts) for lazy-open of cold shards.
func (ts *TieredStore) eventShardSnapshot(depth ShardDepth) []*EventShard {
	var shards []*EventShard
	for _, es := range ts.eventShards {
		switch depth {
		case DepthHot:
			if es.tier == TierHot {
				shards = append(shards, es)
			}
		case DepthWarm:
			if es.tier == TierHot || es.tier == TierWarm {
				shards = append(shards, es)
			}
		default: // DepthAll
			shards = append(shards, es)
		}
	}
	return shards
}

// --- Registry file integration ---

// SaveRegistries persists both registries atomically to the registry file.
// Writing both halves in a single call eliminates the read-modify-write race
// that existed when SaveLabelRegistry and SaveRelTypeRegistry were separate.
func (ts *TieredStore) SaveRegistries(labelReg *indexpkg.LabelRegistry, relTypeReg *indexpkg.RelTypeRegistry) error {
	if ts.inMemory {
		return nil
	}
	return saveRegistryFile(ts.regFile, labelReg.ExportNames(), relTypeReg.ExportNames())
}

// SaveLabelRegistry persists the label registry to the registry file.
// Deprecated: use SaveRegistries to avoid read-modify-write race.
// Kept for backward compatibility with single-registry callers.
func (ts *TieredStore) SaveLabelRegistry(reg *indexpkg.LabelRegistry) error {
	if ts.inMemory {
		return nil
	}
	labels := reg.ExportNames()
	// Load existing relTypes to preserve them.
	_, existingRelTypes, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return err
	}
	return saveRegistryFile(ts.regFile, labels, existingRelTypes)
}

// LoadLabelRegistry restores the label registry from the registry file.
// Returns the number of labels imported (excluding reserved token 0).
func (ts *TieredStore) LoadLabelRegistry(reg *indexpkg.LabelRegistry) (int, error) {
	if ts.inMemory {
		return 0, nil
	}
	labels, _, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return 0, err
	}
	if labels == nil {
		return 0, nil // no file yet
	}
	if err := reg.ImportNames(labels); err != nil {
		return 0, err
	}
	return len(labels) - 1, nil
}

// SaveRelTypeRegistry persists the reltype registry to the registry file.
// Deprecated: use SaveRegistries to avoid read-modify-write race.
// Kept for backward compatibility with single-registry callers.
func (ts *TieredStore) SaveRelTypeRegistry(reg *indexpkg.RelTypeRegistry) error {
	if ts.inMemory {
		return nil
	}
	relTypes := reg.ExportNames()
	// Load existing labels to preserve them.
	existingLabels, _, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return err
	}
	return saveRegistryFile(ts.regFile, existingLabels, relTypes)
}

// LoadRelTypeRegistry restores the reltype registry from the registry file.
// Returns the number of relTypes imported (excluding reserved token 0).
func (ts *TieredStore) LoadRelTypeRegistry(reg *indexpkg.RelTypeRegistry) (int, error) {
	if ts.inMemory {
		return 0, nil
	}
	_, relTypes, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return 0, err
	}
	if relTypes == nil {
		return 0, nil
	}
	if err := reg.ImportNames(relTypes); err != nil {
		return 0, err
	}
	return len(relTypes) - 1, nil
}

// SetLabelRegistry wires the ontology to the label registry for token resolution.
func (ts *TieredStore) SetLabelRegistry(reg *indexpkg.LabelRegistry) {
	ts.ontology.SetLabelRegistry(reg)
}

// --- Reference archive ---

// openRefArchive lazily opens the reference archive BadgerStore.
// Caller must hold ts.archiveMu.Lock to serialize against other openers.
// The Load/Store pattern still applies for readers — archiveMu only protects
// the open operation itself, not field access.
func (ts *TieredStore) openRefArchive() error {
	if ts.refArchive.Load() != nil {
		return nil
	}
	store, err := ts.openBadgerStore("archive", false) // NOT read-only: archive needs writes
	if err != nil {
		return fmt.Errorf("graph: open ref archive: %w", err)
	}
	ts.refArchive.Store(store)

	// Register archive shard in catalog if new.
	if _, ok := ts.catalog.GetShard("archive"); !ok {
		ts.catalog.AddShard(ShardEntry{
			Name: "archive",
			Kind: ShardArchive,
			Tier: TierCold,
			Path: "archive",
		})
		if !ts.inMemory {
			_ = ts.catalog.Save() // best-effort persist
		}
	}
	return nil
}

// ensureRefArchive opens the archive if needed. Thread-safe via archiveMu.
// Refuses to re-open after Close has run — `closed` is set under archiveMu,
// so an open call that wins the lock against Close completes first; one that
// loses sees `closed == true` and returns ErrStoreClosed instead of opening
// a fresh archive that would never be closed.
func (ts *TieredStore) ensureRefArchive() error {
	ts.archiveMu.Lock()
	defer ts.archiveMu.Unlock()
	if ts.closed.Load() {
		return ErrStoreClosed
	}
	return ts.openRefArchive()
}

// hasArchiveShard returns true if the catalog has an archive shard entry.
func (ts *TieredStore) hasArchiveShard() bool {
	_, ok := ts.catalog.GetShard("archive")
	return ok
}

// --- Internal helpers ---

// openBadgerStore creates a new BadgerStore with the configured defaults.
// For disk-backed stores, name is the relative path under DataDir.
// readOnly opens Badger in read-only mode (no flushLoop, no gcLoop).
func (ts *TieredStore) openBadgerStore(name string, readOnly bool) (*BadgerStore, error) {
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
func (ts *TieredStore) openBadgerStoreWithRecovery(name string) (*BadgerStore, error) {
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
	if errors.Is(err, badger.ErrTruncateNeeded) {
		return true
	}
	return strings.Contains(err.Error(), badger.ErrTruncateNeeded.Error())
}

// shardWindowName computes the canonical name for a time window.
func shardWindowName(t time.Time, window time.Duration) string {
	switch {
	case window >= 7*24*time.Hour:
		// Weekly: "2026-W09"
		year, week := t.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", year, week)
	case window >= 24*time.Hour:
		// Daily: "2026-03-02"
		return t.Format("2006-01-02")
	default:
		// Monthly fallback: "2026-03"
		return t.Format("2006-01")
	}
}

// shardWindowStart computes the start time for a shard window containing t.
func shardWindowStart(t time.Time, window time.Duration) time.Time {
	switch {
	case window >= 7*24*time.Hour:
		// ISO week start (Monday).
		year, week := t.ISOWeek()
		// Compute day 1 of ISO week.
		jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
		jan1Weekday := jan1.Weekday()
		if jan1Weekday == time.Sunday {
			jan1Weekday = 7
		}
		// Days from Jan 1 to Monday of ISO week 1.
		daysToMonday := int(time.Monday - jan1Weekday)
		if daysToMonday > 0 {
			daysToMonday -= 7
		}
		isoWeek1Monday := jan1.AddDate(0, 0, daysToMonday)
		return isoWeek1Monday.AddDate(0, 0, (week-1)*7)
	case window >= 24*time.Hour:
		// Day start.
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	default:
		// Month start.
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	}
}
