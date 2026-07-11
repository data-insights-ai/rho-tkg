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

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
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
	// Must not be negative.
	ColdAfter time.Duration
	// IdleTimeout closes idle cold shards after this duration. 0 = never close.
	// Default: 5 minutes when ColdAfter > 0. If set, must be at least 1ms.
	IdleTimeout time.Duration
	// Compression sets the SSTable compression algorithm for all shards.
	// Valid values: options.None (0), options.Snappy (1), options.ZSTD (2).
	// Zero keeps the Badger default (Snappy).
	Compression options.CompressionType
	// ZSTDCompressionLevel sets the ZSTD compression level (1-15) for all shards.
	// Only effective when Compression is options.ZSTD.
	// Zero keeps the Badger default (1).
	ZSTDCompressionLevel int
	// ValueLogFileSize / MemTableSize / BlockCacheSize / IndexCacheSize /
	// NumCompactors tune each shard's Badger per-instance footprint. ONE
	// Badger instance opens per shard, so stock sizes multiply by shard count
	// — a deployment with a reference shard plus a dozen weekly event shards
	// pre-creates tens of GB of apparent vlog and allocates a memtable arena
	// per shard even when every shard holds little data. Zero keeps Badger's
	// stock defaults. Validated per-shard at open (same bounds as
	// badger.Config) — an out-of-range value surfaces when the reference
	// shard opens in New.
	ValueLogFileSize int64 // bytes; valid [1MB, 2GB)
	MemTableSize     int64 // bytes; valid [8MB, 1GB]
	BlockCacheSize   int64 // bytes; >= 0
	IndexCacheSize   int64 // bytes; >= 0
	NumCompactors    int   // 0 = badger default (4); minimum 2
	// EncryptionKey / EncryptionKeyRotation enable AES encryption-at-rest for
	// EVERY shard (reference, hot, warm, lazy cold/archive, and
	// rotation-created) — see badger.Config for length validation and the
	// wrong-key/plaintext-dir failure modes. Encryption REQUIRES both
	// BlockCacheSize > 0 and IndexCacheSize > 0 above (Badger panics at Open
	// without the former, and on the first encrypted SSTable flush without
	// the latter); validated per-shard at open (same bounds as
	// badger.Config) — surfaces when the reference shard opens in New.
	EncryptionKey         []byte
	EncryptionKeyRotation time.Duration
	// CacheBudgetBytes bounds EACH shard's entity caches (nodes, rels) by
	// estimated resident BYTES rather than entry count. With one cache pair per
	// open shard, CacheCapacity alone (an entry count, default 10,000 per cache)
	// can pin hundreds of thousands of entities in heap across many shards
	// regardless of their size; this knob bounds that by bytes. Soft limit
	// (dirty entries are never evicted). 0 disables byte accounting. When set
	// and CacheCapacity is 0, the byte budget alone governs.
	CacheBudgetBytes int64
	// PropertyIndexOnDisk is the tiered sibling of badger.Config's
	// PropertyIndexOnDisk: it does NOT change WHERE property indexes live —
	// property indexes remain reference-shard-only (CreatePropertyIndex still
	// rejects event labels with ErrEventPropertyIndex; see CLAUDE.md "Property
	// indexes on reference entities only"). It only changes HOW the reference
	// shard answers them: false (default) keeps the reference shard's
	// PropertyIndex.Entries/numBuckets maps in RAM; true answers
	// equality/range reads from the reference shard's persisted 0x0A keyspace
	// instead (see badgerstore_property_disk.go), surviving reopen without an
	// in-memory rebuild. Passed through badgerCfg to EVERY shard (reference,
	// hot, warm, lazy cold/archive, and rotation-created) for uniformity —
	// event shards never build a property index, so the flag is a no-op
	// there, harmless.
	PropertyIndexOnDisk bool
	// ChangeLog enables the durable, ordered change-log (op-log) across all
	// shards. A store-level monotonic allocator hands every shard's change-log
	// record a store-global LSN (a total commit order); each shard co-commits
	// its records in its own WriteBatch (as standalone badger does), and the
	// feed methods (ForEachChange/ChangeFeed/LastCommittedLSN) k-way merge the
	// per-shard logs by LSN behind a flush-before-read durability barrier
	// (ADR-0005 §2). Off by default (zero overhead). Surfaces
	// store.ChangeFeedCapability / ChangeLogStatusCapability / TxChangeLogScope.
	ChangeLog bool
}

// EventShard wraps a BadgerStore with metadata for an event shard.
type EventShard struct {
	name              string
	store             *BadgerStore // nil when cold + closed (lazy-open)
	tier              ShardTier
	tierCode          atomic.Uint32
	timeStart         time.Time    // shard window start (inclusive)
	timeEnd           time.Time    // shard window end (exclusive)
	readOnly          bool         // warm/cold tier marker; Badger handles remain writable for owner-shard mutations
	path              string       // relative path for lazy-open (e.g., "events/2026-W10")
	shardMu           sync.Mutex   // protects lazy open/close
	activeReqs        atomic.Int64 // outstanding read requests; blocks idle-close
	lastAccess        atomic.Int64 // unix ms, idle-close tracking
	readTransientOpen bool         // shardMu-protected: cold store was opened only for transient read fanout
}

const (
	eventShardTierUnset uint32 = iota
	eventShardTierHot
	eventShardTierWarm
	eventShardTierCold
)

func eventShardTierCode(t ShardTier) uint32 {
	switch t {
	case TierHot:
		return eventShardTierHot
	case TierWarm:
		return eventShardTierWarm
	case TierCold:
		return eventShardTierCold
	default:
		return eventShardTierUnset
	}
}

func eventShardTierFromCode(code uint32) ShardTier {
	switch code {
	case eventShardTierHot:
		return TierHot
	case eventShardTierWarm:
		return TierWarm
	case eventShardTierCold:
		return TierCold
	default:
		return ""
	}
}

func (es *EventShard) initTier(t ShardTier) {
	es.tier = t
	es.tierCode.Store(eventShardTierCode(t))
}

func (es *EventShard) setTier(t ShardTier) {
	es.tier = t
	es.tierCode.Store(eventShardTierCode(t))
}

func (es *EventShard) currentTier() ShardTier {
	if es == nil {
		return ""
	}
	return eventShardTierFromCode(es.tierCode.Load())
}

// Store implements the Store interface by routing entities across
// multiple BadgerStore instances based on ontology classification.
//
// Reference entities (Case, Organization, User) live in refShard.
// Event entities (Signal, Alert) live in time-windowed event shards.
// Phase 3a: exactly one hot event shard. Phases 3b-3e add warm/cold/archive.
type Store struct {
	mu                    sync.RWMutex                                    // protects hotShard + eventShards during rotation
	refShard              *BadgerStore                                    // reference shard (always hot)
	propKeyReg            atomic.Pointer[registrypkg.PropertyKeyRegistry] // single canonical property-key registry, injected into every shard at open
	refActiveReqs         atomic.Int64                                    // refcount for refShard — Close spin-waits on this before refShard.Close()
	refArchive            atomic.Pointer[BadgerStore]                     // nil until first archive/restore or DepthAll with archive catalog; atomic so reads need not hold archiveMu
	archiveMu             sync.Mutex                                      // serializes lazy-open of refArchive (single-flight)
	archiveActiveReqs     atomic.Int64                                    // refcount for refArchive — Close spin-waits on this before archive.Close()
	eventShards           map[string]*EventShard                          // name -> event shard
	hotShard              *EventShard                                     // convenience pointer to current hot shard
	ontology              *OntologyMapping
	catalog               *ShardCatalog
	regFile               string // path to registry.msgpack
	temporalIdxFile       string // path to temporal_indexes.msgpack
	vectorIdxFile         string // path to vector_indexes.msgpack
	dataDir               string
	inMemory              bool
	shardWindow           time.Duration
	cacheCap              int
	flushInt              time.Duration
	coldAfter             time.Duration
	idleTimeout           time.Duration
	compression           options.CompressionType
	zstdLevel             int
	valueLogFileSize      int64 // per-shard Badger footprint knobs; 0 = stock default
	memTableSize          int64
	blockCacheSize        int64
	indexCacheSize        int64
	numCompactors         int
	encryptionKey         []byte // per-shard AES encryption-at-rest key; nil = disabled
	encryptionKeyRotation time.Duration
	cacheBudgetBytes      int64         // per-shard entity-cache byte budget; 0 = off
	propertyIndexOnDisk   bool          // reference-shard-only scope unchanged; changes RAM vs disk representation
	closeCh               chan struct{} // signals idle-close goroutine to stop
	closeOnce             sync.Once
	lifecycleMu           sync.RWMutex // blocks Close while long sequential store-wide operations release per-shard pins
	closed                atomic.Bool  // set under archiveMu inside Close before tearing the archive down;
	// readers consult this from ensureRefArchive to refuse re-opening the archive after Close
	// has already closed it (prevents an orphan re-open + leaked DB handle)
	bgErrMu sync.Mutex
	bgErr   error

	// Change-log (op-log) — opt-in via Config.ChangeLog. logEnabled gates record
	// production on every shard; changeLogAlloc is the store-global LSN allocator
	// injected into each shard (badger.Config.ChangeLogSeqSource) so LSNs are a
	// total commit order across shards. See tieredstore_changelog.go.
	logEnabled     bool
	changeLogAlloc *changeLogAllocator

	nodeCreateMu sync.Mutex // serializes cross-shard node ID uniqueness checks with writes
	relCreateMu  sync.Mutex // serializes cross-shard relationship ID uniqueness checks with writes

	// Temporal indexes — tracked so new hot/archive shards inherit them.
	tempIdxMu     sync.Mutex
	tempIdxLabels []uint16
	hfIdxBuckets  map[uint16]time.Duration

	// Vector indexes — in-memory brute-force k-NN index spanning all shards.
	// In-memory; CreateVectorIndex rebuilds entries from current node properties.
	vectorIdxMu   sync.RWMutex
	vectorIndexes map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex
}

// New creates a Store with a reference shard and one hot event shard.
func New(cfg Config) (*Store, error) {
	if !cfg.InMemory && cfg.DataDir == "" {
		return nil, fmt.Errorf("graph: Config.DataDir required unless InMemory")
	}
	for i, label := range cfg.RefLabels {
		if strings.TrimSpace(label) == "" {
			return nil, fmt.Errorf("graph: Config.RefLabels[%d] must not be empty", i)
		}
	}

	window := cfg.ShardWindow
	if window == 0 {
		window = defaultShardWindow
	}
	if window < time.Minute {
		return nil, fmt.Errorf("graph: Config.ShardWindow must be >= 1 minute, got %v", window)
	}
	if window%time.Millisecond != 0 {
		return nil, fmt.Errorf("graph: Config.ShardWindow must be a whole millisecond, got %v", window)
	}
	if cfg.ColdAfter < 0 {
		return nil, fmt.Errorf("graph: Config.ColdAfter must not be negative, got %v", cfg.ColdAfter)
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
	if idleTimeout < 0 {
		return nil, fmt.Errorf("graph: Config.IdleTimeout must not be negative, got %v", idleTimeout)
	}
	if idleTimeout > 0 && idleTimeout < time.Millisecond {
		return nil, fmt.Errorf("graph: Config.IdleTimeout must be >= 1 millisecond when set, got %v", idleTimeout)
	}
	if idleTimeout > 0 && idleTimeout%time.Millisecond != 0 {
		return nil, fmt.Errorf("graph: Config.IdleTimeout must be a whole millisecond, got %v", idleTimeout)
	}

	ts := &Store{
		eventShards:           make(map[string]*EventShard),
		ontology:              NewOntologyMapping(cfg.RefLabels),
		dataDir:               cfg.DataDir,
		inMemory:              cfg.InMemory,
		shardWindow:           window,
		cacheCap:              cacheCap,
		flushInt:              flushInt,
		coldAfter:             cfg.ColdAfter,
		idleTimeout:           idleTimeout,
		compression:           cfg.Compression,
		zstdLevel:             cfg.ZSTDCompressionLevel,
		valueLogFileSize:      cfg.ValueLogFileSize,
		memTableSize:          cfg.MemTableSize,
		blockCacheSize:        cfg.BlockCacheSize,
		indexCacheSize:        cfg.IndexCacheSize,
		numCompactors:         cfg.NumCompactors,
		encryptionKey:         cfg.EncryptionKey,
		encryptionKeyRotation: cfg.EncryptionKeyRotation,
		cacheBudgetBytes:      cfg.CacheBudgetBytes,
		propertyIndexOnDisk:   cfg.PropertyIndexOnDisk,
		closeCh:               make(chan struct{}),
		hfIdxBuckets:          make(map[uint16]time.Duration),
		vectorIndexes:         make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex),
		logEnabled:            cfg.ChangeLog,
	}
	// Build the store-global change-log allocator BEFORE opening any shard, so it
	// can be injected via badgerCfg (ChangeLogSeqSource). Each shard folds its
	// durable watermark into it at open via Observe; after the reference shard
	// opens we reseed the allocator from the refShard catalog watermark so a cold
	// shard never has to be opened at startup (ADR-0005 §2.1-reseed).
	if cfg.ChangeLog {
		ts.changeLogAlloc = newChangeLogAllocator(ts)
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
		ts.temporalIdxFile = filepath.Join(cfg.DataDir, "meta", "temporal_indexes.msgpack")
		ts.vectorIdxFile = filepath.Join(cfg.DataDir, "meta", "vector_indexes.msgpack")
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
	// The reference shard holds the single canonical property-key registry
	// (persisted only here). Capture it so every other shard — opened now,
	// lazily (cold/archive), or after rotation — decodes tokenized rows with the
	// SAME instance, instead of its own (empty) per-shard meta copy.
	if reg := refStore.PropertyKeyRegistry(); reg != nil {
		ts.propKeyReg.Store(reg)
	}
	// Reseed the store-global change-log allocator from the reference shard's
	// catalog watermark now that refShard is open. This covers cold shards that
	// are NOT opened at startup (their max LSN was folded into this watermark
	// while they were hot). refShard's own LastLSNKey was already folded via
	// Observe at its open. An unreadable watermark fails the change-log CLOSED
	// (sticky background error; the allocator refuses to hand out LSNs) rather
	// than reseeding below a durable cold-shard LSN and risking reuse.
	if ts.changeLogAlloc != nil {
		// A reseed failure poisons ONLY the change-log capability (the feed doors
		// fail closed and the allocator refuses LSNs); the store still serves its
		// primary reads/writes. This is the change-log fence of ADR-0005 §2.1-reseed
		// Problem 1 — narrower than the store-wide recordBackgroundError gate.
		ts.changeLogAlloc.reseedFromRefShard()
	}

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
	es.initTier(TierHot)
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
			// Warm is a routing tier, not a write permission. The shard may
			// still own existing event entities that need updates/deletes.
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
			warmES.initTier(TierWarm)
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
			coldES.initTier(TierCold)
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

	if !cfg.InMemory {
		if err := ts.loadTemporalIndexDefs(); err != nil {
			for _, es := range ts.eventShards {
				if es.store != nil {
					_ = es.store.Close()
				}
			}
			_ = refStore.Close()
			return nil, fmt.Errorf("graph: load temporal indexes: %w", err)
		}
		if err := ts.loadVectorIndexDefs(); err != nil {
			for _, es := range ts.eventShards {
				if es.store != nil {
					_ = es.store.Close()
				}
			}
			_ = refStore.Close()
			return nil, fmt.Errorf("graph: load vector indexes: %w", err)
		}
	}

	// Start idle-close goroutine for cold shards.
	if ts.idleTimeout > 0 {
		go ts.idleCloseLoop()
	}

	return ts, nil
}

// MetaGet delegates to the reference shard (which is always present and
// persistent). The graph-layer schema_version marker lives there.
func (ts *Store) MetaGet(key string) ([]byte, error) {
	if ts == nil {
		return nil, ErrNilStore
	}
	if ts.refShard == nil {
		return nil, ErrStoreClosed
	}
	return ts.refShard.MetaGet(key)
}

// MetaSet delegates to the reference shard.
func (ts *Store) MetaSet(key string, value []byte) error {
	if ts == nil {
		return ErrNilStore
	}
	if ts.refShard == nil {
		return ErrStoreClosed
	}
	return ts.refShard.MetaSet(key, value)
}

// SavePropertyKeyRegistry delegates to the reference shard. Property keys
// are shared across event + reference shards (they index the same Property
// type), so a single canonical registry stored on refShard is sufficient.
func (ts *Store) SavePropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) error {
	if ts == nil {
		return ErrNilStore
	}
	if ts.refShard == nil {
		return ErrStoreClosed
	}
	return ts.refShard.SavePropertyKeyRegistry(reg)
}

// LoadPropertyKeyRegistry delegates to the reference shard.
func (ts *Store) LoadPropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) (bool, error) {
	if ts == nil {
		return false, ErrNilStore
	}
	if ts.refShard == nil {
		return false, ErrStoreClosed
	}
	return ts.refShard.LoadPropertyKeyRegistry(reg)
}

// SetPropertyKeyRegistry installs the property-key registry on every shard
// (reference + each event shard). Wire encoders + decoders consult the
// registry to dictionary-encode property keys on disk.
func (ts *Store) SetPropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) {
	if ts == nil {
		return
	}
	// Track the current canonical instance so shards opened LATER (lazy cold/
	// archive, rotation) inject it at open — closing the gap where SetProperty-
	// KeyRegistry only reached already-open shards.
	ts.propKeyReg.Store(reg)
	if ts.refShard != nil {
		ts.refShard.SetPropertyKeyRegistry(reg)
	}
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		if es != nil && es.store != nil {
			es.store.SetPropertyKeyRegistry(reg)
		}
	}
	ts.mu.RUnlock()
	if arc := ts.refArchive.Load(); arc != nil {
		arc.SetPropertyKeyRegistry(reg)
	}
}

// Close closes all shards and saves the catalog. Idempotent via sync.Once.
func (ts *Store) Close() error {
	if ts == nil {
		return ErrNilStore
	}
	if ts.closeCh == nil || ts.refShard == nil {
		return ErrStoreClosed
	}
	var closeErr error
	ts.closeOnce.Do(func() {
		ts.lifecycleMu.Lock()
		defer ts.lifecycleMu.Unlock()

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

		// Close all event shards. Cold shards may have nil stores. Use the
		// shard mutex because closeIdleShards also closes cold stores and
		// clears es.store under this lock.
		for _, es := range ts.eventShards {
			es.shardMu.Lock()
			if es.store != nil {
				if err := es.store.Close(); err != nil {
					closeErr = errors.Join(closeErr, fmt.Errorf("graph: close event shard %s: %w", es.name, err))
				}
				es.store = nil
				es.readTransientOpen = false
			}
			es.shardMu.Unlock()
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

		// Close reference shard. Drain refActiveReqs first so public
		// store methods that already passed checkOpen cannot have the
		// Badger handle closed underneath a read/write call.
		for ts.refActiveReqs.Load() > 0 {
			time.Sleep(time.Millisecond)
		}
		if ts.refShard != nil {
			if err := ts.refShard.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: close ref shard: %w", err))
			}
		}
		closeErr = errors.Join(closeErr, ts.backgroundError())
	})
	return closeErr
}

// recordBackgroundError appends a background-task failure to the store's
// sticky error set. Once recorded, every subsequent checkout/read/write path
// returns the combined error via backgroundError() and the store fails
// closed. This is intentional "fail loud" behavior for catalog-save
// corruption, idle-shard close failures, and rotation errors that compromise
// persistence integrity.
//
// Operational note: a transient OS-level close error (e.g., an NFS hiccup
// during idle-shard close) produces the same poison effect as a catalog-save
// failure. Once the underlying condition is fixed, RecoverBackgroundError
// re-probes the persistence path and clears the gate without a
// close/re-open cycle.
func (ts *Store) recordBackgroundError(err error) {
	if err == nil {
		return
	}
	ts.bgErrMu.Lock()
	ts.bgErr = errors.Join(ts.bgErr, err)
	ts.bgErrMu.Unlock()
}

// backgroundError returns the sticky background-error set. Once non-nil it
// stays non-nil until either the store is closed and re-opened, or an
// operator runs RecoverBackgroundError after fixing the underlying condition.
func (ts *Store) backgroundError() error {
	ts.bgErrMu.Lock()
	defer ts.bgErrMu.Unlock()
	return ts.bgErr
}

// RecoverBackgroundError attempts to clear the sticky background error after
// the operator has fixed the underlying condition (e.g. the filesystem came
// back after the NFS hiccup that failed an idle-shard close). It re-probes
// the persistence path by atomically saving the shard catalog — the same
// write machinery whose failure modes the background error guards. On a
// successful probe the recorded error is cleared and the store is usable
// again WITHOUT a close/re-open cycle; on a failed probe the original error
// is retained (with the probe failure joined in) and returned.
//
// Recovery clears the lifecycle gate only — it does not re-verify shard
// data. After close failures on cold shards, run VerifyShard / RunRepair
// before trusting the affected shards for critical reads.
//
// Returns nil when there was no background error to clear.
func (ts *Store) RecoverBackgroundError() error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ts.bgErrMu.Lock()
	defer ts.bgErrMu.Unlock()
	if ts.bgErr == nil {
		return nil
	}
	if probeErr := ts.catalog.Save(); probeErr != nil {
		ts.bgErr = errors.Join(ts.bgErr, fmt.Errorf("graph: background-error recovery probe (catalog save): %w", probeErr))
		return ts.bgErr
	}
	cleared := ts.bgErr
	ts.bgErr = nil
	slog.Warn("tiered store background error cleared after successful recovery probe",
		"cleared", cleared.Error())
	return nil
}

func (ts *Store) checkOpen() error {
	if ts == nil {
		return ErrNilStore
	}
	if ts.closeCh == nil || ts.refShard == nil {
		return ErrStoreClosed
	}
	if ts.closed.Load() {
		return ErrStoreClosed
	}
	return nil
}

func (ts *Store) beginSequentialStoreWideOperation() (func(), error) {
	if ts == nil {
		return func() {}, ErrNilStore
	}
	ts.lifecycleMu.RLock()
	if err := ts.checkOpen(); err != nil {
		ts.lifecycleMu.RUnlock()
		return func() {}, err
	}
	return ts.lifecycleMu.RUnlock, nil
}

// Clear clears all shards, including closed cold event shards.
// It also resets store-level state that lives on the Store itself rather
// than on individual shards: the vector-index map and the tracked temporal
// index labels list (which would otherwise re-install temporal indexes for
// stale labels on the next hot-shard rotation).
//
// Concurrency: takes ts.mu.Lock for the full clear so direct Store callers get
// the same topology exclusion as g.Admin().Reset. Each open event shard is also
// pinned via checkoutStore for the duration of its Clear() call. Without the
// pin, Close (which doesn't take ts.mu and only spin-waits on activeReqs) could
// free the underlying DB while Clear was still touching it — Badger v4 Flush on
// a closed DB blocks forever.
func (ts *Store) Clear() error {
	releaseLifecycle, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return err
	}
	defer releaseLifecycle()

	ts.mu.Lock()
	defer ts.mu.Unlock()
	if err := ts.checkOpen(); err != nil {
		return err
	}

	restoreIndexMetadata, err := ts.prepareIndexMetadataForClear()
	if err != nil {
		return err
	}
	rollbackIndexMetadata := func(clearErr error) error {
		if restoreIndexMetadata == nil {
			return clearErr
		}
		if restoreErr := restoreIndexMetadata(); restoreErr != nil {
			return fmt.Errorf("%w (index metadata rollback failed: %v)", clearErr, restoreErr)
		}
		return clearErr
	}

	shards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		shards = append(shards, es)
	}

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return rollbackIndexMetadata(err)
	}
	if err := ref.Clear(); err != nil {
		refCheckin()
		return rollbackIndexMetadata(fmt.Errorf("graph: clear ref shard: %w", err))
	}
	refCheckin()
	for _, es := range shards {
		if err := ts.clearEventShard(es); err != nil {
			return rollbackIndexMetadata(fmt.Errorf("graph: clear event shard %s: %w", es.name, err))
		}
	}

	// Reset Store-level state. Without this, post-Clear callers see
	// "already exists" on a logically empty store (vectorIndexes), and the
	// next rotation re-creates temporal indexes on the new hot shard for
	// labels that were dropped along with the shard data (tempIdxLabels).
	ts.vectorIdxMu.Lock()
	ts.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)
	if err := ts.persistVectorIndexDefsLocked(); err != nil {
		ts.vectorIdxMu.Unlock()
		return err
	}
	ts.vectorIdxMu.Unlock()

	ts.tempIdxMu.Lock()
	ts.tempIdxLabels = nil
	ts.hfIdxBuckets = make(map[uint16]time.Duration)
	if err := ts.persistTemporalIndexDefsLocked(); err != nil {
		ts.tempIdxMu.Unlock()
		return err
	}
	ts.tempIdxMu.Unlock()

	// Skip the archive checkout entirely when neither the in-memory pointer
	// nor the catalog has an archive. checkoutArchive would otherwise be a
	// no-op too, but bailing early makes the intent explicit and avoids the
	// theoretical lazy-open-then-clear churn if the archive is on disk but
	// has not been opened this session.
	if ts.refArchive.Load() != nil || ts.hasArchiveShard() {
		// Pin via checkoutArchive — see resolveShardStore("archive") doc.
		// A raw refArchive.Load() races Close, which drains archiveActiveReqs
		// (sees 0) and proceeds to archive.Close() while Clear is still
		// touching the DB → Badger v4 Flush-on-closed-DB hang.
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return rollbackIndexMetadata(archiveErr)
		}
		if archive != nil {
			if err := archive.Clear(); err != nil {
				archiveCheckin()
				return rollbackIndexMetadata(fmt.Errorf("graph: clear ref archive: %w", err))
			}
			archiveCheckin()
		}
	}
	if err := ts.resetCatalogAfterClear(); err != nil {
		return err
	}
	return nil
}

func (ts *Store) prepareIndexMetadataForClear() (func() error, error) {
	if ts.inMemory {
		return nil, nil
	}

	ts.vectorIdxMu.Lock()
	vectorSnapshot, err := snapshotVectorIndexFile(ts.vectorIdxFile)
	if err == nil {
		err = saveVectorIndexFile(ts.vectorIdxFile, nil)
	}
	if err != nil {
		restoreErr := restoreVectorIndexFile(vectorSnapshot)
		ts.vectorIdxMu.Unlock()
		if restoreErr != nil {
			return nil, fmt.Errorf("graph: clear vector index definitions: %w (rollback failed: %v)", err, restoreErr)
		}
		return nil, fmt.Errorf("graph: clear vector index definitions: %w", err)
	}
	ts.vectorIdxMu.Unlock()

	ts.tempIdxMu.Lock()
	temporalSnapshot, err := snapshotTemporalIndexFile(ts.temporalIdxFile)
	if err == nil {
		err = saveTemporalIndexFile(ts.temporalIdxFile, temporalIndexFileData{})
	}
	if err != nil {
		restoreErr := errors.Join(
			restoreVectorIndexFile(vectorSnapshot),
			restoreTemporalIndexFile(temporalSnapshot),
		)
		ts.tempIdxMu.Unlock()
		if restoreErr != nil {
			return nil, fmt.Errorf("graph: clear temporal index definitions: %w (rollback failed: %v)", err, restoreErr)
		}
		return nil, fmt.Errorf("graph: clear temporal index definitions: %w", err)
	}
	ts.tempIdxMu.Unlock()

	return func() error {
		return errors.Join(
			restoreVectorIndexFile(vectorSnapshot),
			restoreTemporalIndexFile(temporalSnapshot),
		)
	}, nil
}

func (ts *Store) clearEventShard(es *EventShard) error {
	if ts.closed.Load() {
		return ErrStoreClosed
	}
	if ts.inMemory && es.store == nil {
		return nil
	}
	if !es.readOnly || ts.inMemory {
		store, coErr := es.checkoutStore(ts)
		if coErr != nil {
			return coErr
		}
		defer es.checkinStore()
		return store.Clear()
	}

	es.shardMu.Lock()
	defer es.shardMu.Unlock()

	es.activeReqs.Add(1)
	defer es.activeReqs.Add(-1)
	for es.activeReqs.Load() > 1 {
		time.Sleep(time.Millisecond)
	}

	if es.store != nil {
		if err := es.store.Close(); err != nil {
			return err
		}
		es.store = nil
		es.readTransientOpen = false
	}

	store, err := ts.openBadgerStore(es.path, false)
	if err != nil {
		return err
	}
	clearErr := store.Clear()
	closeErr := store.Close()
	var reopenErr error
	if es.currentTier() == TierWarm && !ts.closed.Load() {
		es.store, reopenErr = ts.openBadgerStoreWithRecovery(es.path)
		es.readTransientOpen = false
	}
	return errors.Join(clearErr, closeErr, reopenErr)
}

func (ts *Store) resetCatalogAfterClear() error {
	snapshot := ts.catalog.snapshotShards()
	for _, entry := range snapshot {
		ts.catalog.UpdateShardStats(entry.Name, 0, 0)
		ts.catalog.UpdateShardVerified(entry.Name, false)
	}
	if ts.inMemory {
		return nil
	}
	if err := ts.catalog.Save(); err != nil {
		// Shard data has already been cleared by this point and cannot be
		// reconstructed from catalog metadata. Keep the live catalog aligned
		// with the cleared stores even when durable catalog persistence fails.
		return fmt.Errorf("graph: save catalog after clear: %w", err)
	}
	return nil
}

// --- Internal helpers ---

// openBadgerStore creates a new BadgerStore with the configured defaults.
// For disk-backed stores, name is the relative path under DataDir.
// readOnly opens Badger in read-only mode (no flushLoop, no gcLoop).
func (ts *Store) openBadgerStore(name string, readOnly bool) (*BadgerStore, error) {
	return NewBadgerStore(ts.badgerCfg(name, readOnly))
}

// badgerCfg builds the per-shard BadgerStoreConfig from the tiered store's
// configuration. It is the SINGLE source of truth for shard options — shared by
// openBadgerStore and by the explicit WAL migration in
// openBadgerStoreWithRecovery — so a migration open and the real open can never
// diverge. The per-instance footprint knobs and the per-shard cache byte budget
// pass through to every shard opened through here: reference, hot, warm, lazy
// cold/archive, and rotation-created.
func (ts *Store) badgerCfg(name string, readOnly bool) BadgerStoreConfig {
	cfg := BadgerStoreConfig{
		InMemory:              ts.inMemory,
		CacheCapacity:         ts.cacheCap,
		CacheBudgetBytes:      ts.cacheBudgetBytes,
		FlushInterval:         ts.flushInt,
		ReadOnly:              readOnly,
		Compression:           ts.compression,
		ZSTDCompressionLevel:  ts.zstdLevel,
		ValueLogFileSize:      ts.valueLogFileSize,
		MemTableSize:          ts.memTableSize,
		BlockCacheSize:        ts.blockCacheSize,
		IndexCacheSize:        ts.indexCacheSize,
		NumCompactors:         ts.numCompactors,
		EncryptionKey:         ts.encryptionKey,
		EncryptionKeyRotation: ts.encryptionKeyRotation,
		PropertyIndexOnDisk:   ts.propertyIndexOnDisk,
	}
	if !ts.inMemory {
		cfg.Dir = filepath.Join(ts.dataDir, name)
	}
	// Inject the canonical property-key registry so this shard's loadIndexes can
	// resolve tokenized property keys at open. Nil while the reference shard
	// itself is being opened (it loads the canonical copy from its own meta);
	// set for every shard opened afterwards — hot, warm, lazy cold/archive, and
	// rotation-created shards (all route through here).
	if reg := ts.propKeyReg.Load(); reg != nil {
		cfg.PropertyKeyRegistry = reg
	}
	// Write-ahead hook: before a flush persists rows that reference newly-
	// allocated property-key tokens, commit the shared registry to the reference
	// shard (with fsync) ahead of the row WriteBatch. Set only for non-reference
	// shards (ts.refShard is nil while the reference shard itself is opening — it
	// falls back to committing its own meta, which is the canonical copy).
	if ts.refShard != nil {
		cfg.OnPropertyKeyGrow = func() error {
			reg := ts.propKeyReg.Load()
			if reg == nil {
				return nil
			}
			return ts.refShard.SavePropertyKeyRegistry(reg)
		}
	}
	// Change-log: slave this shard's LSNs to the store-global allocator and, for
	// non-reference shards, persist the allocator watermark to the reference
	// shard after each log-bearing flush (the reference shard writes its own
	// watermark directly — see changeLogAllocator.persistWatermark). ReadOnly
	// (warm/cold) shards never produce records but still Observe their watermark.
	if ts.changeLogAlloc != nil && !ts.changeLogAlloc.isPoisoned() {
		cfg.ChangeLog = true
		cfg.ChangeLogSeqSource = ts.changeLogAlloc
		if ts.refShard != nil {
			cfg.OnChangeLogFlush = ts.changeLogAlloc.persistWatermark
		}
	}
	return cfg
}

// openBadgerStoreWithRecovery opens a writable BadgerStore for a warm/cold event
// shard. It probes read-only first so ErrTruncateNeeded from an unclean shutdown
// is recovered by a read-write open, but the returned handle must be mutable:
// existing event entities keep routing to their owner shard after rotation.
func (ts *Store) openBadgerStoreWithRecovery(name string) (*BadgerStore, error) {
	// Flush any oversized WAL BEFORE the read-only probe. A read-only open
	// replays WALs into the same MemTableSize-bounded arena as a writable one
	// (badger openMemTables runs before its read-only branch), so an oversized
	// WAL — left when MemTableSize was shrunk on an existing dir — fails the
	// probe with "Arena too small". That is not ErrTruncateNeeded, so without
	// this the probe would abort recovery before the writable open is ever
	// tried. Idempotent and a no-op on clean dirs, stock memtable sizes, and
	// in-memory shards.
	if err := badger.MigrateOversizedWAL(ts.badgerCfg(name, false)); err != nil {
		return nil, fmt.Errorf("graph: WAL migration %s: %w", name, err)
	}
	probe, err := ts.openBadgerStore(name, true)
	if err == nil {
		if err := probe.Close(); err != nil {
			return nil, fmt.Errorf("graph: recovery probe close %s: %w", name, err)
		}
		return ts.openBadgerStore(name, false)
	}
	if !isTruncateNeeded(err) {
		return nil, err
	}
	slog.Warn("graph: recovering corrupt WAL by truncation", "shard", name)
	return ts.openBadgerStore(name, false)
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
