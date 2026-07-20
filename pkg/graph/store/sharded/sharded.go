// Package sharded provides sharded.Store — a Store implementation (ADR-0007,
// stage S1) that routes entities across N badger.Store shards by the SLOT
// carried in the snowflake node field of every ID. Unlike tiered.Store (which
// routes by time window and ontology class), sharded.Store routes by a pure,
// immutable function of the ID: shardFor(id) = catalog[decompose(id).Node].
//
// EXPERIMENTAL (ADR-0007, integration branch): the mandatory Store contract
// (CRUD, adjacency, bulk-read, batch, history, stats, iteration) plus the S3
// store-global change-log/feed (Config.ChangeLog), S4 per-lane unified ID
// generators (Config.IngestLanes) + pre-encoded-put, and S5 full index/stats
// capability parity (property / rel-property / composite / temporal / high-
// frequency indexes + type-class / presence / range-cardinality stats, fanned
// out per shard) are all IMPLEMENTED over multi-slot spreads. It still declines,
// with reason, the capabilities tiered also declines (TransactionTimeQuery,
// HistoryRollbackTrim, label/rel-type-tx membership, depth iteration). With
// Config.IngestLanes unset, core mints legacy dual-generator IDs, so a plain
// graph-level deployment must claim slots covering BOTH nodeID*2 and *2+1; set
// IngestLanes to pin a whole ingest group into one slot (one shard, one batched
// door call).
package sharded

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/dgraph-io/badger/v4/options"
)

// Compile-time assertion: sharded.Store satisfies the mandatory Store contract.
var _ storecontract.MandatoryStore = (*Store)(nil)

// Store-contract sentinel aliases for readability inside this package. Canonical
// declarations live in pkg/graph/store.
var (
	ErrNodeNotFound         = storecontract.ErrNodeNotFound
	ErrRelNotFound          = storecontract.ErrRelNotFound
	ErrRelExists            = storecontract.ErrRelExists
	ErrInvalidStoreMutation = storecontract.ErrInvalidStoreMutation
	ErrNilStore             = storecontract.ErrNilStore
	ErrStoreClosed          = storecontract.ErrStoreClosed
)

// Package-owned sentinels (ADR-0007). ErrSlotNotLocal is the fail-closed result
// for any door reached with an ID whose slot is not claimed by this store — at
// the horizontal stage this becomes "route to the owning machine".
var (
	// ErrSlotNotLocal is returned when a door is reached with an entity ID
	// whose snowflake slot is outside this store's claimed range. Point ops
	// fail closed with it rather than silently returning empty. Re-exported from
	// the store contract (SAME value) so the partition-agnostic graph layer can
	// errors.Is against it — e.g. to recognize a Model-A stub during tx rollback.
	ErrSlotNotLocal = storecontract.ErrSlotNotLocal
	// ErrCatalogConflict is returned at open when the persisted slot catalog
	// disagrees with the config (different claimed range, unknown discipline,
	// missing shard directory).
	ErrCatalogConflict = errors.New("graph: sharded: slot catalog conflicts with config")
	// ErrCatalogCorrupt is returned when the persisted catalog blob cannot be
	// decoded.
	ErrCatalogCorrupt = errors.New("graph: sharded: slot catalog corrupt")
	// ErrForeignEndpointLocal is returned by PutRelationshipForeignEnd when the
	// "foreign" END node's slot is actually LOCAL to this store — a misuse that
	// would silently skip the local existence check for a node this store can
	// (and must) validate. The caller should use the normal PutRelationship
	// door for a local-to-local edge (ADR-0010 §3.2).
	ErrForeignEndpointLocal = errors.New("graph: sharded: foreign-endpoint door reached with a local end node")
)

// QueryOpts / RelTombstone are Store-contract aliases.
type (
	QueryOpts    = storecontract.QueryOpts
	RelTombstone = storecontract.RelTombstone
)

// badgerShard is the single-shard backend unit. Store composes SlotCount of them.
type badgerShard = badger.Store

// isRelNotFound reports whether err is (or wraps) ErrRelNotFound.
func isRelNotFound(err error) bool { return errors.Is(err, ErrRelNotFound) }

// Config configures a sharded.Store. Dir/InMemory + BaseSlot/SlotCount define the
// topology; the remaining fields are per-shard badger passthroughs applied
// uniformly to every shard.
type Config struct {
	// Dir is the root data directory. Each shard lives under Dir/slot-NN/.
	// Required unless InMemory is true.
	Dir string
	// InMemory opens every shard memory-only (no disk I/O). For testing.
	InMemory bool
	// BaseSlot is the first claimed snowflake slot (0..31).
	BaseSlot uint8
	// SlotCount is the number of contiguous claimed slots (1..32);
	// BaseSlot+SlotCount must be <= 32. One badger.Store opens per claimed slot.
	SlotCount uint8

	// ChangeLog enables the store-global change-log/op-log (ADR-0007 S3): a single
	// LSN allocator is injected into every shard so records draw one total order,
	// and the feed doors (ForEachChange/ChangeFeed/LastCommittedLSN) barrier-flush
	// then k-way-merge the per-shard logs. Off by default (zero overhead).
	ChangeLog bool

	// DisablePlannerStats turns OFF query-planner statistics on every shard —
	// see badger.Config.DisablePlannerStats. The stat methods fail closed with
	// ErrCapabilityNotSupported and the cross-shard fold declines. Default false.
	DisablePlannerStats bool

	// HistoryDeltaEncoding turns ON anchor+delta version-history storage on every
	// slot's badger store — see badger.Config.HistoryDeltaEncoding (ADR-0009/B6).
	// Default false (opt-in).
	HistoryDeltaEncoding bool

	// Per-shard badger passthroughs (applied uniformly to every shard).
	Compression           options.CompressionType
	ZSTDCompressionLevel  int
	CacheCapacity         int
	CacheBudgetBytes      int64
	SyncWrites            bool
	FlushInterval         time.Duration
	MemTableSize          int64
	ValueLogFileSize      int64
	BlockCacheSize        int64
	IndexCacheSize        int64
	NumCompactors         int
	EncryptionKey         []byte
	EncryptionKeyRotation time.Duration
}

// Store implements the mandatory Store contract over N badger.Store shards with
// slot routing. All shards are open local badgers, so scans fold in parallel and
// point ops route in O(1).
type Store struct {
	mu       sync.RWMutex // guards closed
	closed   bool
	catalog  *slotCatalog
	base     uint8
	count    uint8
	inMemory bool
	dir      string

	// shards[k] owns the slot base+k (identity map). shards[0] is the anchor
	// shard: it owns MetaKV, registries, markers, and the slot catalog.
	shards []*badger.Store

	// propKeyReg tracks the canonical property-key registry currently installed
	// on every shard so SetPropertyKeyRegistry reaches all of them.
	propKeyReg *registrypkg.PropertyKeyRegistry

	// logEnabled + changeLogAlloc drive the store-global change-log (ADR-0007 S3;
	// see changelog.go). changeLogAlloc is injected into every shard's badger.Config
	// as ChangeLogSeqSource so LSNs form one total order across shards. Both zero
	// when Config.ChangeLog is off.
	logEnabled     bool
	changeLogAlloc *changeLogAllocator

	// vectorDefs records the dims + metric of every store-level vector index
	// (ADR-0007 S5; see vector_index.go). The actual per-shard VectorIndexes live
	// inside each badger shard and are maintained by badger on every write; the
	// sharded store keeps ONLY this def metadata so it can globally re-rank the
	// union of per-shard top-k results by distance. Persisted to the anchor's
	// MetaKV (vectorDefsMetaKey) and reloaded at open.
	vectorDefMu sync.RWMutex
	vectorDefs  map[vectorDefKey]vectorDefMeta
}

// anchor returns the anchor shard (slot base), which owns store-global metadata.
func (s *Store) anchor() *badger.Store { return s.shards[0] }

// New opens (or creates) a sharded.Store. It opens all shards in parallel, loads
// or creates the slot catalog on the anchor shard, and fails closed if a
// persisted catalog disagrees with the config.
func New(cfg Config) (*Store, error) {
	if err := validateConfigRange(cfg.BaseSlot, cfg.SlotCount); err != nil {
		return nil, err
	}
	if !cfg.InMemory && cfg.Dir == "" {
		return nil, errors.New("graph: sharded.Config.Dir required unless InMemory")
	}

	s := &Store{
		base:       cfg.BaseSlot,
		count:      cfg.SlotCount,
		inMemory:   cfg.InMemory,
		dir:        cfg.Dir,
		shards:     make([]*badger.Store, cfg.SlotCount),
		vectorDefs: make(map[vectorDefKey]vectorDefMeta),
	}
	if cfg.ChangeLog {
		s.logEnabled = true
		s.changeLogAlloc = &changeLogAllocator{}
	}

	// Create the per-shard directory layout for disk stores, and detect which
	// shard directories already exist (a pre-existing catalog references them).
	if !cfg.InMemory {
		if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
			return nil, fmt.Errorf("graph: sharded: create dir %s: %w", cfg.Dir, err)
		}
	}

	// Open the anchor shard first so its catalog can be read/validated before the
	// remaining shards are opened. The anchor self-loads its own property-key
	// registry from meta; the shared registry is then injected into the rest.
	anchorCfg := s.shardConfig(cfg, 0, nil)
	anchor, err := badger.New(anchorCfg)
	if err != nil {
		return nil, fmt.Errorf("graph: sharded: open anchor shard: %w", err)
	}
	s.shards[0] = anchor

	// Load or create the catalog on the anchor.
	if err := s.loadOrCreateCatalog(cfg); err != nil {
		_ = anchor.Close()
		return nil, err
	}

	// The shared property-key registry: seed it from the anchor's persisted copy
	// so shards opened below decode tokenized rows consistently.
	reg := registrypkg.NewPropertyKeyRegistry()
	if _, lerr := anchor.LoadPropertyKeyRegistry(reg); lerr != nil {
		_ = anchor.Close()
		return nil, fmt.Errorf("graph: sharded: load property-key registry: %w", lerr)
	}
	s.propKeyReg = reg

	// Open the remaining shards in parallel with the shared registry injected.
	if cfg.SlotCount > 1 {
		errs := make([]error, cfg.SlotCount)
		var wg sync.WaitGroup
		for k := 1; k < int(cfg.SlotCount); k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				shard, oerr := badger.New(s.shardConfig(cfg, uint8(k), reg))
				if oerr != nil {
					errs[k] = fmt.Errorf("graph: sharded: open shard %d: %w", k, oerr)
					return
				}
				s.shards[k] = shard
			}(k)
		}
		wg.Wait()
		for _, e := range errs {
			if e != nil {
				s.closeOpenShards()
				return nil, e
			}
		}
	}

	// Reload the store-level vector-index def metadata (dims + metric) so a
	// reopened store can globally re-rank per-shard search results. The shards
	// independently rebuild their own vector indexes from persisted defs.
	if err := s.loadVectorDefs(); err != nil {
		s.closeOpenShards()
		return nil, err
	}

	return s, nil
}

// shardConfig builds the per-shard badger.Config for shard index k.
func (s *Store) shardConfig(cfg Config, k uint8, reg *registrypkg.PropertyKeyRegistry) badger.Config {
	bc := badger.Config{
		InMemory:              cfg.InMemory,
		Compression:           cfg.Compression,
		ZSTDCompressionLevel:  cfg.ZSTDCompressionLevel,
		CacheCapacity:         cfg.CacheCapacity,
		CacheBudgetBytes:      cfg.CacheBudgetBytes,
		SyncWrites:            cfg.SyncWrites,
		FlushInterval:         cfg.FlushInterval,
		MemTableSize:          cfg.MemTableSize,
		ValueLogFileSize:      cfg.ValueLogFileSize,
		BlockCacheSize:        cfg.BlockCacheSize,
		IndexCacheSize:        cfg.IndexCacheSize,
		NumCompactors:         cfg.NumCompactors,
		EncryptionKey:         cfg.EncryptionKey,
		EncryptionKeyRotation: cfg.EncryptionKeyRotation,
		DisablePlannerStats:   cfg.DisablePlannerStats,
		HistoryDeltaEncoding:  cfg.HistoryDeltaEncoding,
		PropertyKeyRegistry:   reg,
	}
	if cfg.ChangeLog {
		// Every shard records to its own co-committed log but draws LSNs from the
		// ONE store-global allocator, so the merged feed is a single total order.
		// badger.New folds each shard's durable LastLSNKey into the allocator via
		// ChangeLogSeqSource.Observe at open — automatic reseed, no persisted
		// watermark needed (every shard is always open, unlike tiered's cold shards).
		bc.ChangeLog = true
		bc.ChangeLogSeqSource = s.changeLogAlloc
	}
	if !cfg.InMemory {
		bc.Dir = filepath.Join(cfg.Dir, shardDirName(cfg.BaseSlot+k))
	}
	return bc
}

func shardDirName(slot uint8) string { return fmt.Sprintf("slot-%02d", slot) }

// loadOrCreateCatalog reads the catalog from the anchor and validates it against
// the config, or creates and persists a fresh identity catalog. It also verifies
// (for disk stores) that every mapped shard directory exists — a missing one is
// a fail-closed conflict.
func (s *Store) loadOrCreateCatalog(cfg Config) error {
	data, err := s.anchor().MetaGet(catalogMetaKey)
	if err != nil {
		return fmt.Errorf("graph: sharded: read catalog: %w", err)
	}
	if data == nil {
		cat := newIdentityCatalog(cfg.BaseSlot, cfg.SlotCount)
		blob, encErr := cat.encode()
		if encErr != nil {
			return fmt.Errorf("graph: sharded: encode catalog: %w", encErr)
		}
		if setErr := s.anchor().MetaSet(catalogMetaKey, blob); setErr != nil {
			return fmt.Errorf("graph: sharded: persist catalog: %w", setErr)
		}
		s.catalog = cat
		return nil
	}
	cat, decErr := decodeCatalog(data)
	if decErr != nil {
		return decErr
	}
	if valErr := cat.validateAgainstConfig(cfg.BaseSlot, cfg.SlotCount); valErr != nil {
		return valErr
	}
	if !cfg.InMemory {
		for slot := range cat.SlotShard {
			dir := filepath.Join(cfg.Dir, shardDirName(slot))
			if _, statErr := os.Stat(dir); statErr != nil {
				return fmt.Errorf("%w: mapped shard directory %s: %v", ErrCatalogConflict, dir, statErr)
			}
		}
	}
	s.catalog = cat
	return nil
}

// --- Routing ---

// slotOf returns the snowflake slot carried by id.
func slotOf(id snowflake.ID) uint8 {
	return uint8(snowflakepkg.DecomposeID(id).NodeID)
}

// shardForID resolves the shard owning id, or ErrSlotNotLocal if the slot is
// not claimed by this store.
func (s *Store) shardForID(id snowflake.ID) (*badger.Store, error) {
	slot := slotOf(id)
	idx, ok := s.catalog.shardIndexForSlot(slot)
	if !ok {
		return nil, fmt.Errorf("%w: slot %d (id %d)", ErrSlotNotLocal, slot, id)
	}
	return s.shards[idx], nil
}

func (s *Store) shardForNodeID(id types.NodeID) (*badger.Store, error) {
	return s.shardForID(id.SnowflakeID())
}

func (s *Store) shardForRelID(id types.RelID) (*badger.Store, error) {
	return s.shardForID(id.SnowflakeID())
}

// --- Lifecycle ---

func (s *Store) checkOpen() error {
	if s == nil {
		return ErrNilStore
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrStoreClosed
	}
	return nil
}

// ClaimedSlotRange returns the store's claimed snowflake slot range
// (BaseSlot, SlotCount) as configured at New() — the immutable identity of
// this deployment (BACKLOG 20h). Callers wiring a snowflake-ID-generating
// component against a sharded store (e.g. Config.IngestLanes's per-lane
// generators, each pinned to a specific node-field/slot) can use this to
// cross-validate BEFORE minting any ID that every slot the generators will
// use is actually claimed by this store, instead of discovering a mismatch
// reactively at the first write to an unclaimed slot.
func (s *Store) ClaimedSlotRange() (base, count uint8) {
	return s.base, s.count
}

// Close closes every shard, joining errors.
func (s *Store) Close() error {
	if s == nil {
		return ErrNilStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	for i, shard := range s.shards {
		if shard == nil {
			continue
		}
		if err := shard.Close(); err != nil {
			errs = append(errs, fmt.Errorf("graph: sharded: close shard %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// closeOpenShards is the New() cleanup path: close any shard already opened.
func (s *Store) closeOpenShards() {
	for _, shard := range s.shards {
		if shard != nil {
			_ = shard.Close()
		}
	}
}

// Clear truncates every shard (data, indexes, history, counters) and re-persists
// the slot catalog on the anchor. Registries are a graph-layer concern and are
// not part of Clear.
func (s *Store) Clear() error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	var errs []error
	for i, shard := range s.shards {
		if err := shard.Clear(); err != nil {
			errs = append(errs, fmt.Errorf("graph: sharded: clear shard %d: %w", i, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	// badger.Clear wipes meta too; re-anchor the catalog so a later reopen finds it.
	blob, err := s.catalog.encode()
	if err != nil {
		return fmt.Errorf("graph: sharded: encode catalog: %w", err)
	}
	if err := s.anchor().MetaSet(catalogMetaKey, blob); err != nil {
		return fmt.Errorf("graph: sharded: re-persist catalog: %w", err)
	}
	// BACKLOG 20n: each shard.Clear() above already reset that shard's OWN
	// vectorIndexes map to empty, but the store-level vectorDefs cache (kept
	// so cross-shard merges can re-rank without re-deriving dims/metric) is
	// a SEPARATE field Clear never touched — verified this was already
	// harmless (SearchNearestNodes/SearchNearestFiltered call vectorDefFor,
	// which would still report the stale def as present, but every shard
	// then uniformly answers ErrVectorIndexNotFound and coalesceUniform
	// surfaces that single error rather than wrong/empty data; a later
	// CreateVectorIndex for the same key just overwrites the stale entry).
	// Resetting it here closes that residual staleness window outright
	// instead of relying on downstream error-uniformity to paper over it.
	// propKeyReg is deliberately NOT touched — see the doc comment above:
	// registries are a graph-layer concern, not Clear's.
	s.vectorDefMu.Lock()
	s.vectorDefs = make(map[vectorDefKey]vectorDefMeta)
	verr := s.persistVectorDefsLocked()
	s.vectorDefMu.Unlock()
	if verr != nil {
		return fmt.Errorf("graph: sharded: reset vector defs: %w", verr)
	}
	return nil
}

// Flush folds Flush over every shard (satisfies the graph-layer storeFlusher
// probe used by snapshot/export paths).
func (s *Store) Flush() error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	var errs []error
	for i, shard := range s.shards {
		if err := shard.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("graph: sharded: flush shard %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// --- Parallel fold helper ---

// maxShardWorkers bounds every shard fan-out to a fixed-size worker pool
// (BACKLOG 20k / lesson 8: "fan-out helpers should use bounded worker pools,
// not one goroutine per shard"). The store's own 32-shard hard cap (a 5-bit
// slot field) already keeps an unbounded fan-out modest, but the pool is
// capped independently of that limit so a future cap raise cannot silently
// regress it into scheduler/memory pressure.
const maxShardWorkers = 8

// runShardPool runs fn(idx) once for every idx in [0,n) using a worker pool of
// at most maxShardWorkers goroutines, blocking until every index has run. Each
// task touches only its own idx, so callers passing shard-indexed fn bodies
// (errs[idx] = ...) need no additional synchronization.
func runShardPool(n int, fn func(idx int)) {
	workers := maxShardWorkers
	if n < workers {
		workers = n
	}
	if workers <= 0 {
		return
	}
	tasks := make(chan int, n)
	for i := 0; i < n; i++ {
		tasks <- i
	}
	close(tasks)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for idx := range tasks {
				fn(idx)
			}
		}()
	}
	wg.Wait()
}

// forEachShard runs fn against every shard through the bounded worker pool and
// returns the joined error. fn receives the shard index and the shard.
func (s *Store) forEachShardErr(fn func(idx int, shard *badger.Store) error) error {
	errs := make([]error, len(s.shards))
	runShardPool(len(s.shards), func(i int) {
		errs[i] = fn(i, s.shards[i])
	})
	return errors.Join(errs...)
}
