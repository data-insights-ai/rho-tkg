package tiered

import (
	"fmt"
	"reflect"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// ShardInfo describes a shard for admin API queries.
type ShardInfo struct {
	Name      string
	Kind      ShardKind
	Tier      ShardTier
	TimeStart time.Time
	TimeEnd   time.Time
	Nodes     int  // live count from open store, 0 if closed
	Rels      int  // live count from open store, 0 if closed
	Open      bool // whether the store is currently open
	Verified  bool
}

// VerifyResult holds the outcome of a per-shard hash chain verification.
type VerifyResult struct {
	ShardName   string
	NodesOK     int
	RelsOK      int
	NodesFailed int
	RelsFailed  int
	Cached      bool // true if result came from catalog cache
}

// ForceRotate triggers a hot-shard rotation with internal locking.
// It is kept as an admin-facing alias for RotateHotShard.
func (ts *Store) ForceRotate() error {
	return ts.RotateHotShard()
}

// ListShards returns information about all shards in the catalog, enriched
// with live counts from open stores. Returns an error if the archive shard
// is recorded in the catalog but cannot be opened — a silent skip would
// hide a real disk/LSM failure.
//
// Concurrency: each open shard is pinned via checkoutStore /
// checkoutArchive while NodeCount / RelationshipCount run, so a racing
// Close (which doesn't take ts.mu and only spin-waits on activeReqs)
// cannot free the underlying DB mid-call. Cold shards report Open=false
// based on the under-RLock snapshot — we deliberately do NOT lazy-open
// them just to read counts.
func (ts *Store) ListShards() ([]ShardInfo, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	type esSnapshot struct {
		es        *EventShard
		name      string
		tier      ShardTier
		timeStart time.Time
		timeEnd   time.Time
		wasOpen   bool
	}

	ts.mu.RLock()
	snaps := make([]esSnapshot, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		es.shardMu.Lock()
		wasOpen := es.store != nil
		es.shardMu.Unlock()
		snaps = append(snaps, esSnapshot{
			es:        es,
			name:      es.name,
			tier:      es.currentTier(),
			timeStart: es.timeStart,
			timeEnd:   es.timeEnd,
			wasOpen:   wasOpen,
		})
	}
	ts.mu.RUnlock()

	var infos []ShardInfo

	// Reference shard. Pin it so a racing Close cannot free the handle while
	// the informational count calls are running.
	refEntry, _ := ts.catalog.GetShard("reference") // (ShardEntry, bool) — bool discarded
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, fmt.Errorf("graph: list shards: open reference: %w", err)
	}
	refNodes, _ := ref.NodeCount()
	refRels, _ := ref.RelationshipCount()
	refCheckin()
	refInfo := ShardInfo{
		Name:     "reference",
		Kind:     ShardReference,
		Tier:     TierHot,
		Open:     true,
		Nodes:    refNodes,
		Rels:     refRels,
		Verified: refEntry != nil && refEntry.Verified,
	}
	infos = append(infos, refInfo)

	// Archive shard (if open or in catalog). Pin via checkoutArchive so a
	// concurrent Close cannot free the handle between Load and the
	// NodeCount/RelationshipCount calls — see resolveShardStore("archive").
	// Propagate ensureRefArchive errors rather than silently swallowing —
	// a corrupt LSM or disk error would otherwise surface as "archive not
	// open" in the result set, hiding the real failure mode.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return nil, fmt.Errorf("graph: list shards: open archive: %w", archiveErr)
	}
	if archive != nil {
		archiveEntry, _ := ts.catalog.GetShard("archive") // (ShardEntry, bool) — bool discarded
		archNodes, _ := archive.NodeCount()               // informational; 0 on failure is acceptable
		archRels, _ := archive.RelationshipCount()
		archiveCheckin()
		archiveInfo := ShardInfo{
			Name:     "archive",
			Kind:     ShardArchive,
			Tier:     TierCold,
			Open:     true,
			Nodes:    archNodes,
			Rels:     archRels,
			Verified: archiveEntry != nil && archiveEntry.Verified,
		}
		infos = append(infos, archiveInfo)
	} else if ts.hasArchiveShard() {
		archiveEntry, _ := ts.catalog.GetShard("archive") // (ShardEntry, bool) — bool discarded
		infos = append(infos, ShardInfo{
			Name:     "archive",
			Kind:     ShardArchive,
			Tier:     TierCold,
			Open:     false,
			Nodes:    approxNodes(archiveEntry),
			Rels:     approxRels(archiveEntry),
			Verified: archiveEntry != nil && archiveEntry.Verified,
		})
	}

	// Event shards. For each shard observed open in the snapshot, take a
	// short-lived checkoutStore pin around NodeCount/RelationshipCount so
	// Close cannot free the underlying DB while we read it. checkoutStore
	// returns ErrStoreClosed if Close started after our snapshot — in
	// that case we report Open=false rather than crashing.
	for _, sn := range snaps {
		entry, _ := ts.catalog.GetShard(sn.name) // (ShardEntry, bool) — bool discarded
		si := ShardInfo{
			Name:      sn.name,
			Kind:      ShardEvent,
			Tier:      sn.tier,
			TimeStart: sn.timeStart,
			TimeEnd:   sn.timeEnd,
			Verified:  entry != nil && entry.Verified,
			Nodes:     approxNodes(entry),
			Rels:      approxRels(entry),
		}
		if sn.wasOpen {
			store, release, open, err := sn.es.checkoutOpenStoreForRead(ts)
			if err == nil && open {
				si.Open = true
				si.Nodes, _ = store.NodeCount() // informational; 0 on failure is acceptable
				si.Rels, _ = store.RelationshipCount()
				release()
			}
		}
		infos = append(infos, si)
	}

	return infos, nil
}

func approxNodes(entry *ShardEntry) int {
	if entry == nil {
		return 0
	}
	return entry.ApproxNodes
}

func approxRels(entry *ShardEntry) int {
	if entry == nil {
		return 0
	}
	return entry.ApproxRels
}

// RebuildCatalog reconstructs shard tiers and counts from the backing stores.
//
// Concurrency: holds ts.mu.Lock for the duration of the rebuild,
// blocking every reader path that takes ts.mu.RLock (label/property
// queries, eventShardSnapshot, etc.) until all NodeCount /
// RelationshipCount calls complete. This is acceptable for an
// infrequent admin operation; callers that need to serve reads at the
// same time should schedule RebuildCatalog during a quiet window.
func (ts *Store) RebuildCatalog() error {
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

	catalogSnapshot := ts.catalog.snapshotShards()
	rollbackCatalog := func(err error) error {
		ts.catalog.restoreShards(catalogSnapshot)
		return err
	}

	// Update reference shard.
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: open reference: %w", err))
	}
	refNodes, err := ref.NodeCount()
	if err != nil {
		refCheckin()
		return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: ref node count: %w", err))
	}
	refRels, err := ref.RelationshipCount()
	refCheckin()
	if err != nil {
		return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: ref rel count: %w", err))
	}
	ts.catalog.UpdateShardStats("reference", refNodes, refRels)

	// Update archive if open. Pin via checkoutArchive — see ListShards.
	// Propagate the error so a corrupt LSM during catalog rebuild surfaces
	// at the API boundary instead of silently leaving stats stale.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: open archive: %w", archiveErr))
	}
	if archive != nil {
		archNodes, err := archive.NodeCount()
		if err != nil {
			archiveCheckin()
			return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: archive node count: %w", err))
		}
		archRels, err := archive.RelationshipCount()
		if err != nil {
			archiveCheckin()
			return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: archive rel count: %w", err))
		}
		archiveCheckin()
		ts.catalog.UpdateShardStats("archive", archNodes, archRels)
	}

	// Update event shards. Pin each shard while counting so a racing Close
	// cannot free the underlying DB. Use the read checkout path so closed
	// cold shards opened only for this rebuild are closed again immediately
	// instead of accumulating Badger handles across many historical shards.
	for _, es := range ts.eventShards {
		ts.catalog.UpdateShardTier(es.name, es.currentTier())
		store, release, coErr := es.checkoutStoreForRead(ts)
		if coErr != nil {
			return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: open event shard %s: %w", es.name, coErr))
		}
		nc, err := store.NodeCount()
		if err != nil {
			release()
			return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: shard %s node count: %w", es.name, err))
		}
		rc, err := store.RelationshipCount()
		if err != nil {
			release()
			return rollbackCatalog(fmt.Errorf("graph: rebuild catalog: shard %s rel count: %w", es.name, err))
		}
		release()
		ts.catalog.UpdateShardStats(es.name, nc, rc)
	}

	if !ts.inMemory {
		if err := ts.catalog.Save(); err != nil {
			return rollbackCatalog(err)
		}
	}
	return nil
}

// HashChainVerifier is the dependency-inverted interface that VerifyShard uses
// to run per-entity hash-chain verification. The Graph layer (pkg/graph)
// satisfies it via Graph.Hash.VerifyNodeChain / Graph.Hash.VerifyRelChain.
//
// The interface lets Store call into the Graph layer for label/type
// resolution without taking a hard import dependency on pkg/graph (which would
// be a circular import).
type HashChainVerifier interface {
	VerifyNodeChain(id types.NodeID) (bool, error)
	VerifyRelChain(id types.RelID) (bool, error)
}

// VerifyShard runs hash chain verification on all entities in the named shard.
// Takes a HashChainVerifier (the Graph layer) because hash chain verification
// needs label/type resolution. For immutable shards (warm/cold) that have
// already been verified, returns the cached result without re-scanning.
func (ts *Store) VerifyShard(g HashChainVerifier, shardName string) (*VerifyResult, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if isNilHashChainVerifier(g) {
		return nil, fmt.Errorf("%w: hash chain verifier must not be nil", ErrInvalidStoreMutation)
	}
	// Look up shard in catalog.
	entry, ok := ts.catalog.GetShard(shardName)
	if !ok {
		return nil, fmt.Errorf("graph: shard %q not found in catalog", shardName)
	}

	// Check if immutable and already verified → return cached result.
	isImmutable := entry.Tier == TierWarm || entry.Tier == TierCold
	if isImmutable && entry.Verified {
		return &VerifyResult{
			ShardName: shardName,
			NodesOK:   entry.ApproxNodes,
			RelsOK:    entry.ApproxRels,
			Cached:    true,
		}, nil
	}

	// Get the BadgerStore for this shard. release pins cold shards so
	// closeIdleShards cannot race-close them mid-verification.
	store, release, err := ts.resolveShardStore(shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	// Enumerate all entities.
	nodeIDs, err := store.AllNodeIDs(QueryOpts{})
	if err != nil {
		return nil, fmt.Errorf("graph: verify shard %s: list nodes: %w", shardName, err)
	}
	relIDs, err := store.AllRelIDs(QueryOpts{})
	if err != nil {
		return nil, fmt.Errorf("graph: verify shard %s: list rels: %w", shardName, err)
	}

	result := &VerifyResult{ShardName: shardName}

	// Verify each node.
	for _, id := range nodeIDs {
		ok, err := g.VerifyNodeChain(id)
		if err != nil {
			return nil, fmt.Errorf("graph: verify node %d: %w", id, err)
		}
		if ok {
			result.NodesOK++
		} else {
			result.NodesFailed++
		}
	}

	// Verify each relationship.
	for _, id := range relIDs {
		ok, err := g.VerifyRelChain(id)
		if err != nil {
			return nil, fmt.Errorf("graph: verify rel %d: %w", id, err)
		}
		if ok {
			result.RelsOK++
		} else {
			result.RelsFailed++
		}
	}

	// Cache result for immutable shards if all passed.
	if isImmutable && result.NodesFailed == 0 && result.RelsFailed == 0 {
		catalogSnapshot := ts.catalog.snapshotShards()
		ts.catalog.UpdateShardVerified(shardName, true)
		ts.catalog.UpdateShardStats(shardName, result.NodesOK, result.RelsOK)
		if !ts.inMemory {
			if err := ts.catalog.Save(); err != nil {
				ts.catalog.restoreShards(catalogSnapshot)
				return result, fmt.Errorf("graph: save verification cache for shard %q: %w", shardName, err)
			}
		}
	}

	return result, nil
}

func isNilHashChainVerifier(g HashChainVerifier) bool {
	if g == nil {
		return true
	}
	v := reflect.ValueOf(g)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// resolveShardStore returns the BadgerStore for a named shard along with a
// release function that the caller MUST invoke (typically via defer) to
// balance the active request increment. refShard and refArchive are not
// idle-closed, but Close does close them, so admin scans pin both. Cold event
// shards are pinned via checkoutStore for the same reason.
func (ts *Store) resolveShardStore(name string) (*BadgerStore, func(), error) {
	noop := func() {}
	if err := ts.checkOpen(); err != nil {
		return nil, noop, err
	}
	if name == "reference" {
		return ts.checkoutRefShard()
	}
	if name == "archive" {
		archive, archiveCheckin, err := ts.checkoutArchive()
		if err != nil {
			return nil, noop, err
		}
		if archive == nil {
			return nil, noop, fmt.Errorf("graph: shard %q not found (archive does not exist)", name)
		}
		return archive, archiveCheckin, nil
	}

	ts.mu.RLock()
	es, ok := ts.eventShards[name]
	ts.mu.RUnlock()
	if !ok {
		return nil, noop, fmt.Errorf("graph: shard %q not found", name)
	}
	store, err := es.checkoutStore(ts)
	if err != nil {
		return nil, noop, err
	}
	return store, es.checkinStore, nil
}

type namedStore struct {
	name  string
	store *BadgerStore
}

// allShardStoresWithLazyOpen returns all BadgerStore instances along with a
// release function that the caller MUST invoke (typically via defer) to
// balance the activeReqs increments taken on reference, archive, and event
// shards. Cold event shards are pinned while the caller is still iterating,
// and cold shards opened only for this enumeration are closed again by release.
//
// On error, any checkouts already taken are released before returning, so
// the caller never has to handle partial cleanup.
func (ts *Store) allShardStoresWithLazyOpen() ([]namedStore, func(), error) {
	if err := ts.checkOpen(); err != nil {
		return nil, func() {}, err
	}
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.allShardStoresWithLazyOpenLocked()
}

// allShardStoresWithLazyOpenLocked is allShardStoresWithLazyOpen for callers
// that already hold ts.mu.RLock or ts.mu.Lock.
func (ts *Store) allShardStoresWithLazyOpenLocked() ([]namedStore, func(), error) {
	if err := ts.checkOpen(); err != nil {
		return nil, func() {}, err
	}
	var stores []namedStore
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, func() {}, err
	}
	stores = append(stores, namedStore{name: "reference", store: ref})

	// Pin refArchive (if open / lazy-openable) so a concurrent Close cannot
	// free the handle mid-iteration. Missing the archive here makes
	// RunRepair scanners blind to archive-resident entities — and Phase 1
	// then deletes their cross-shard in/ entries as "orphans".
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		refCheckin()
		return nil, func() {}, archiveErr
	}
	if archive != nil {
		stores = append(stores, namedStore{name: "archive", store: archive})
	} else {
		// Defensive reassignment: checkoutArchive already returns a noop
		// closure when archive == nil (see tieredstore.go:170-205), so this
		// is structurally equivalent. Kept explicit so a future caller that
		// reads only this function can see archiveCheckin is safe to invoke
		// unconditionally without grepping checkoutArchive's contract.
		archiveCheckin = func() {}
	}

	eventShards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		eventShards = append(eventShards, es)
	}

	eventReleases := make([]func(), 0, len(eventShards))
	releaseAll := func() {
		archiveCheckin()
		refCheckin()
		for _, release := range eventReleases {
			release()
		}
	}

	for _, es := range eventShards {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			releaseAll()
			return nil, func() {}, err
		}
		eventReleases = append(eventReleases, release)
		stores = append(stores, namedStore{name: es.name, store: store})
	}
	return stores, releaseAll, nil
}

// findRelInAnyShardStore locates which BadgerStore owns a relationship
// entity by scanning the caller-supplied pinned-store snapshot
// (typically obtained via allShardStoresWithLazyOpen). Probing through
// the pinned snapshot — rather than re-resolving via checkoutArchive +
// a fresh ts.eventShards walk — closes a Close-race window: Close sets
// closed=true and nil's refArchive BEFORE waiting for archiveActiveReqs
// to drain, so a fresh checkoutArchive() during that window returns nil
// even though the archive (kept alive by the caller's outer pin) still
// owns the rel. Without consulting the pinned snapshot, RunRepair
// Phase 1 would treat the archived rel's in/ entries as orphaned and
// delete them — silent data loss.
//
// The returned *BadgerStore pointer is owned by the caller's pin and is
// safe to dereference for as long as the caller holds the snapshot's
// release function.
func (ts *Store) findRelInAnyShardStore(relID snowflake.ID, stores []namedStore) *BadgerStore {
	for _, ns := range stores {
		if relationshipRowExists(ns.store, types.RelID(relID)) {
			return ns.store
		}
	}
	return nil
}

// findNodeInAnyShardStore locates which BadgerStore owns a node entity by
// scanning the caller-supplied pinned-store snapshot. It mirrors
// findRelInAnyShardStore for repair code that must resolve relationship
// endpoints against the same stable shard set it is already scanning.
func (ts *Store) findNodeInAnyShardStore(nodeID snowflake.ID, stores []namedStore) (*BadgerStore, error) {
	for _, ns := range stores {
		live, err := nodeRowLive(ns.store, types.NodeID(nodeID))
		if err != nil {
			return nil, err
		}
		if live {
			return ns.store, nil
		}
	}
	return nil, nil
}
