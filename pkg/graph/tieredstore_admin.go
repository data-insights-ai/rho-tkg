package graph

import (
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
// Unlike RotateHotShard() which expects the caller to hold ts.mu.Lock,
// ForceRotate acquires the lock internally.
func (ts *TieredStore) ForceRotate() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
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
func (ts *TieredStore) ListShards() ([]ShardInfo, error) {
	type esSnapshot struct {
		es        *eventShard
		name      string
		tier      ShardTier
		timeStart time.Time
		timeEnd   time.Time
		wasOpen   bool
	}

	ts.mu.RLock()
	snaps := make([]esSnapshot, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		snaps = append(snaps, esSnapshot{
			es:        es,
			name:      es.name,
			tier:      es.tier,
			timeStart: es.timeStart,
			timeEnd:   es.timeEnd,
			wasOpen:   es.store != nil,
		})
	}
	ts.mu.RUnlock()

	var infos []ShardInfo

	// Reference shard.
	refEntry, _ := ts.catalog.GetShard("reference")
	refNodes, _ := ts.refShard.NodeCount()
	refRels, _ := ts.refShard.RelationshipCount()
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
		archiveEntry, _ := ts.catalog.GetShard("archive")
		archNodes, _ := archive.NodeCount()
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
		infos = append(infos, ShardInfo{
			Name: "archive",
			Kind: ShardArchive,
			Tier: TierCold,
			Open: false,
		})
	}

	// Event shards. For each shard observed open in the snapshot, take a
	// short-lived checkoutStore pin around NodeCount/RelationshipCount so
	// Close cannot free the underlying DB while we read it. checkoutStore
	// returns ErrStoreClosed if Close started after our snapshot — in
	// that case we report Open=false rather than crashing.
	for _, sn := range snaps {
		entry, _ := ts.catalog.GetShard(sn.name)
		si := ShardInfo{
			Name:      sn.name,
			Kind:      ShardEvent,
			Tier:      sn.tier,
			TimeStart: sn.timeStart,
			TimeEnd:   sn.timeEnd,
			Verified:  entry != nil && entry.Verified,
		}
		if sn.wasOpen {
			store, err := sn.es.checkoutStore(ts)
			if err == nil {
				si.Open = true
				si.Nodes, _ = store.NodeCount()
				si.Rels, _ = store.RelationshipCount()
				sn.es.checkinStore()
			}
		}
		infos = append(infos, si)
	}

	return infos, nil
}

// RebuildCatalog reconstructs the shard catalog from the live in-memory state.
// Updates node/rel counts and tier info for all open shards.
//
// Concurrency: holds ts.mu.Lock for the duration of the rebuild,
// blocking every reader path that takes ts.mu.RLock (label/property
// queries, eventShardSnapshot, etc.) until all NodeCount /
// RelationshipCount calls complete. This is acceptable for an
// infrequent admin operation; callers that need to serve reads at the
// same time should schedule RebuildCatalog during a quiet window.
func (ts *TieredStore) RebuildCatalog() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Update reference shard.
	refNodes, _ := ts.refShard.NodeCount()
	refRels, _ := ts.refShard.RelationshipCount()
	ts.catalog.UpdateShardStats("reference", refNodes, refRels)

	// Update archive if open. Pin via checkoutArchive — see ListShards.
	// Propagate the error so a corrupt LSM during catalog rebuild surfaces
	// at the API boundary instead of silently leaving stats stale.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return fmt.Errorf("graph: rebuild catalog: open archive: %w", archiveErr)
	}
	if archive != nil {
		archNodes, _ := archive.NodeCount()
		archRels, _ := archive.RelationshipCount()
		archiveCheckin()
		ts.catalog.UpdateShardStats("archive", archNodes, archRels)
	}

	// Update event shards. Pin each open shard via checkoutStore so a
	// racing Close (which doesn't take ts.mu and only spin-waits on
	// activeReqs) cannot free the underlying DB while we count it.
	// Cold shards (es.store == nil) are skipped — the catalog already
	// holds their last persisted counts. checkoutStore returns
	// ErrStoreClosed if Close started after our snapshot; in that
	// case we leave the catalog stat untouched rather than crash.
	for _, es := range ts.eventShards {
		ts.catalog.UpdateShardTier(es.name, es.tier)
		if es.store == nil {
			continue
		}
		store, coErr := es.checkoutStore(ts)
		if coErr != nil {
			// Close started after we observed es.store != nil; skip
			// the count update rather than crash on a closed DB.
			continue
		}
		nc, _ := store.NodeCount()
		rc, _ := store.RelationshipCount()
		es.checkinStore()
		ts.catalog.UpdateShardStats(es.name, nc, rc)
	}

	if !ts.inMemory {
		return ts.catalog.Save()
	}
	return nil
}

// VerifyShard runs hash chain verification on all entities in the named shard.
// Takes a *Graph because hash chain verification needs label/type resolution.
// For immutable shards (warm/cold) that have already been verified, returns
// the cached result without re-scanning.
func (ts *TieredStore) VerifyShard(g *Graph, shardName string) (*VerifyResult, error) {
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
		ok, err := g.VerifyNodeHashChain(types.NodeID(id))
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
		ok, err := g.VerifyRelHashChain(types.RelID(id))
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
		ts.catalog.UpdateShardVerified(shardName, true)
		ts.catalog.UpdateShardStats(shardName, result.NodesOK, result.RelsOK)
		if !ts.inMemory {
			_ = ts.catalog.Save() // best-effort persist
		}
	}

	return result, nil
}

// resolveShardStore returns the BadgerStore for a named shard along with a
// release function that the caller MUST invoke (typically via defer) to
// balance the activeReqs/archiveActiveReqs increment. refShard is never
// closed by closeIdleShards or Close so its release is a no-op. refArchive
// IS closed by Close (after draining archiveActiveReqs), so it must be
// pinned via checkoutArchive — otherwise a long-running admin op like
// VerifyShard("archive") races Close and hits Badger v4's Flush-on-closed-DB
// hang. Cold event shards are pinned via checkoutStore for the same reason.
func (ts *TieredStore) resolveShardStore(name string) (*BadgerStore, func(), error) {
	noop := func() {}
	if name == "reference" {
		return ts.refShard, noop, nil
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
// balance the activeReqs increments taken on event shards. Cold event
// shards are pinned via checkoutStore so closeIdleShards cannot race-close
// them while the caller is still iterating.
//
// On error, any checkouts already taken are released before returning, so
// the caller never has to handle partial cleanup.
func (ts *TieredStore) allShardStoresWithLazyOpen() ([]namedStore, func(), error) {
	var stores []namedStore
	stores = append(stores, namedStore{name: "reference", store: ts.refShard})

	// Pin refArchive (if open / lazy-openable) so a concurrent Close cannot
	// free the handle mid-iteration. Missing the archive here makes
	// RunRepair scanners blind to archive-resident entities — and Phase 1
	// then deletes their cross-shard in/ entries as "orphans".
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
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

	ts.mu.RLock()
	eventShards := make([]*eventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		eventShards = append(eventShards, es)
	}
	ts.mu.RUnlock()

	checkedOut := make([]*eventShard, 0, len(eventShards))
	releaseAll := func() {
		archiveCheckin()
		for _, es := range checkedOut {
			es.checkinStore()
		}
	}

	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			releaseAll()
			return nil, func() {}, err
		}
		checkedOut = append(checkedOut, es)
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
func (ts *TieredStore) findRelInAnyShardStore(relID snowflake.ID, stores []namedStore) *BadgerStore {
	for _, ns := range stores {
		if ns.store != nil && ns.store.hasRelID(relID) {
			return ns.store
		}
	}
	return nil
}
