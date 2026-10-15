package tiered

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship-type temporal indexes and composite node indexes (backlog 10):
// each covered shard builds and maintains its own badger index over its own
// rows and the tiered store folds the per-shard answers. Composites cover
// every shard; the relationship temporal index covers the reference, hot and
// warm shards only. Nothing moves on rotation. Definitions persist in the
// shard; entries are RAM-resident and rebuilt when the shard opens.
//
// The REFERENCE shard is the anchor: an index exists iff it holds the
// definition. Create builds on every other shard first and the reference
// last; drop removes from the others first and the reference last, so an
// interrupted DDL never leaves an anchor that disagrees with what was fully
// applied. syncAnchoredIndexes copies the anchor onto shards that open later
// and repairs every open shard at store open.
//
// HOT + WARM BOUND: demoting a shard to cold frees its relationship temporal
// index (freeColdRelTemporalIndexes), a cold shard opens with definitions
// discarded (openColdBadgerStoreWithRecovery), the DDL never opens cold
// shards or the archive, and PromoteColdShardsAtOpen rebuilds from the anchor.
// A cold shard's relationships are never pruned: answers are unchanged, only
// the acceleration is bounded.
//
// See docs/architecture.md (tiered.Store) and docs/query-planners.md
// (Composite property indexes).

var (
	_ storecontract.RelTypeTemporalIndexCapability        = (*Store)(nil)
	_ storecontract.RelTypeTemporalCandidateCapability    = (*Store)(nil)
	_ storecontract.CompositePropertyIndexCapability      = (*Store)(nil)
	_ storecontract.CompositeIndexIntrospectionCapability = (*Store)(nil)
)

// anchoredIndex describes one per-shard index definition for the fan-out.
type anchoredIndex struct {
	what string
	// hotWarmOnly: the fan-out reaches the reference shard and the hot and
	// warm event shards only (never the archive, never a cold shard).
	hotWarmOnly bool
	has         func(*BadgerStore) (bool, error)
	create      func(*BadgerStore) error
	drop        func(*BadgerStore) error
	exists      error // the badger create's duplicate sentinel
	missing     error // the badger drop's not-found sentinel
}

func relTemporalIndexDef(relType uint16) anchoredIndex {
	return anchoredIndex{
		what:        "relationship temporal index",
		hotWarmOnly: true,
		has: func(s *BadgerStore) (bool, error) {
			toks, err := s.RelTemporalIndexTypes()
			return slices.Contains(toks, relType), err
		},
		create:  func(s *BadgerStore) error { return s.CreateRelTemporalIndex(relType) },
		drop:    func(s *BadgerStore) error { return s.DropRelTemporalIndex(relType) },
		exists:  ErrTemporalIndexExists,
		missing: ErrTemporalIndexNotFound,
	}
}

func compositeIndexDef(labelToken uint16, keys []string) anchoredIndex {
	return anchoredIndex{
		what: "composite index",
		has: func(s *BadgerStore) (bool, error) {
			defs, err := s.ListCompositePropertyIndexes(labelToken)
			return slices.ContainsFunc(defs, func(d []string) bool { return slices.Equal(d, keys) }), err
		},
		create:  func(s *BadgerStore) error { return s.CreateCompositePropertyIndex(labelToken, keys) },
		drop:    func(s *BadgerStore) error { return s.DropCompositePropertyIndex(labelToken, keys) },
		exists:  storecontract.ErrIndexExists,
		missing: storecontract.ErrIndexNotFound,
	}
}

// anchorLast orders the fan-out refs with the reference shard last.
// temporalIndexShardRefsLocked always puts the reference shard first.
func anchorLast(refs []temporalIndexShardRef) []temporalIndexShardRef {
	out := make([]temporalIndexShardRef, 0, len(refs))
	out = append(out, refs[1:]...)
	return append(out, refs[0])
}

// fanOutAnchoredIndex runs a create (create=true) or drop of def on every
// shard, reference shard last, undoing the shards it changed on failure.
func (ts *Store) fanOutAnchoredIndex(def anchoredIndex, create bool) error {
	releaseLifecycle, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return err
	}
	defer releaseLifecycle()

	ts.mu.RLock()
	defer ts.mu.RUnlock()
	if err := ts.ensureTemporalIndexArchiveOpenLocked(); err != nil {
		return err
	}
	refs := ts.temporalIndexShardRefsLocked()
	if def.hotWarmOnly {
		// ts.mu (held) excludes rotation, the only door that changes a tier
		// on a live store.
		refs = slices.DeleteFunc(refs, func(r temporalIndexShardRef) bool {
			return r.archive || (r.event != nil && r.event.currentTier() == TierCold)
		})
	}
	ts.shardIdxMu.Lock()
	defer ts.shardIdxMu.Unlock()

	var anchored bool
	if err := ts.withTemporalIndexShard(refs[0], func(ns namedStore) error {
		var hasErr error
		anchored, hasErr = def.has(ns.store)
		return hasErr
	}); err != nil {
		return err
	}
	if create && anchored {
		return def.exists
	}
	if !create && !anchored {
		return def.missing
	}

	do, undo, tolerated, undoTolerated := def.create, def.drop, def.exists, def.missing
	verb := "create"
	if !create {
		do, undo, tolerated, undoTolerated = def.drop, def.create, def.missing, def.exists
		verb = "drop"
	}
	changed := make([]temporalIndexShardRef, 0, len(refs))
	for pos, ref := range anchorLast(refs) {
		err := ts.withTemporalIndexShard(ref, func(ns namedStore) error {
			if ts.shardIdxFault != nil {
				if err := ts.shardIdxFault(pos, ns.name); err != nil {
					return fmt.Errorf("graph: %s %s on shard %q: %w", verb, def.what, ns.name, err)
				}
			}
			if err := do(ns.store); err != nil {
				// An interrupted earlier DDL can leave a non-reference shard
				// already in the target state.
				if errors.Is(err, tolerated) {
					return nil
				}
				return fmt.Errorf("graph: %s %s on shard %q: %w", verb, def.what, ns.name, err)
			}
			changed = append(changed, ref)
			return nil
		})
		if err == nil {
			continue
		}
		var rollbackErr error
		for i := len(changed) - 1; i >= 0; i-- {
			rollbackErr = errors.Join(rollbackErr, ts.withTemporalIndexShard(changed[i], func(ns namedStore) error {
				if uerr := undo(ns.store); uerr != nil && !errors.Is(uerr, undoTolerated) {
					return fmt.Errorf("shard %q: %w", ns.name, uerr)
				}
				return nil
			}))
		}
		if rollbackErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
		}
		return err
	}
	return nil
}

// syncAnchoredIndexes makes store carry exactly the reference shard's
// composite definitions and, when relTemporal (hot/warm event shard), its
// relationship temporal definitions; the archive carries none. It creates
// missing and drops extra ones (left by interrupted DDL).
func (ts *Store) syncAnchoredIndexes(store *BadgerStore, relTemporal bool) error {
	ref := ts.refShard
	if store == nil || ref == nil || store == ref {
		return nil
	}
	var want []uint16
	if relTemporal {
		var err error
		if want, err = ref.RelTemporalIndexTypes(); err != nil {
			return fmt.Errorf("graph: read anchored relationship temporal indexes: %w", err)
		}
	}
	have, err := store.RelTemporalIndexTypes()
	if err != nil {
		return err
	}
	for _, tok := range want {
		if !slices.Contains(have, tok) {
			if err := store.CreateRelTemporalIndex(tok); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
				return fmt.Errorf("graph: create relationship temporal index %d: %w", tok, err)
			}
		}
	}
	for _, tok := range have {
		if !slices.Contains(want, tok) {
			if err := store.DropRelTemporalIndex(tok); err != nil && !errors.Is(err, ErrTemporalIndexNotFound) {
				return fmt.Errorf("graph: drop relationship temporal index %d: %w", tok, err)
			}
		}
	}

	wantDefs, err := ref.CompositePropertyIndexDefs()
	if err != nil {
		return fmt.Errorf("graph: read anchored composite indexes: %w", err)
	}
	haveDefs, err := store.CompositePropertyIndexDefs()
	if err != nil {
		return err
	}
	contains := func(defs map[uint16][][]string, tok uint16, keys []string) bool {
		return slices.ContainsFunc(defs[tok], func(d []string) bool { return slices.Equal(d, keys) })
	}
	for tok, defs := range wantDefs {
		for _, keys := range defs {
			if contains(haveDefs, tok, keys) {
				continue
			}
			if err := store.CreateCompositePropertyIndex(tok, keys); err != nil && !errors.Is(err, storecontract.ErrIndexExists) {
				return fmt.Errorf("graph: create composite index %d %v: %w", tok, keys, err)
			}
		}
	}
	for tok, defs := range haveDefs {
		for _, keys := range defs {
			if contains(wantDefs, tok, keys) {
				continue
			}
			if err := store.DropCompositePropertyIndex(tok, keys); err != nil && !errors.Is(err, storecontract.ErrIndexNotFound) {
				return fmt.Errorf("graph: drop composite index %d %v: %w", tok, keys, err)
			}
		}
	}
	return nil
}

// freeColdRelTemporalIndexes drops every relationship temporal index of a
// just-demoted cold shard (hot + warm bound). Best effort and sound on
// failure (the prune may still use the index): logged, not returned. store may
// be nil.
func freeColdRelTemporalIndexes(store *BadgerStore, shard string) {
	if store == nil {
		return
	}
	toks, err := store.RelTemporalIndexTypes()
	if err != nil {
		slog.Error("graph: free cold shard relationship temporal indexes", "shard", shard, "error", err)
		return
	}
	for _, tok := range toks {
		if err := store.DropRelTemporalIndex(tok); err != nil && !errors.Is(err, ErrTemporalIndexNotFound) {
			slog.Error("graph: free cold shard relationship temporal index", "shard", shard, "type", tok, "error", err)
		}
	}
}

// syncColdShardIndexes syncs a lazily opened cold shard to the anchor
// (composites only, no relationship temporal index). Best effort: a failure
// costs work but never changes an answer, so it is logged.
//
// Skipped while an anchored fan-out runs (shardIdxMu held): the anchor does
// not yet reflect the half-applied DDL. TryLock because callers hold the
// shard's shardMu (lock order); the next lazy open heals the shard.
func (ts *Store) syncColdShardIndexes(store *BadgerStore, shard string) {
	if !ts.shardIdxMu.TryLock() {
		return
	}
	defer ts.shardIdxMu.Unlock()
	if err := ts.syncAnchoredIndexes(store, false); err != nil {
		slog.Error("graph: sync cold shard indexes to the anchor", "shard", shard, "error", err)
	}
}

// --- relationship-type temporal indexes ---

// CreateRelTemporalIndex builds a temporal interval index over relType on the
// reference shard and every hot and warm event shard (hot + warm bound).
// Returns ErrTemporalIndexExists if it already exists.
func (ts *Store) CreateRelTemporalIndex(relType uint16) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}
	return ts.fanOutAnchoredIndex(relTemporalIndexDef(relType), true)
}

// DropRelTemporalIndex removes the rel-type temporal index from the shards
// carrying it. Returns ErrTemporalIndexNotFound if no such index exists.
func (ts *Store) DropRelTemporalIndex(relType uint16) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}
	return ts.fanOutAnchoredIndex(relTemporalIndexDef(relType), false)
}

// RelTemporalIndexTypes lists the relationship-type tokens carrying a
// temporal interval index, ascending, from the anchoring reference shard.
func (ts *Store) RelTemporalIndexTypes() ([]uint16, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	defer refCheckin()
	return ref.RelTemporalIndexTypes()
}

// PruneRelTypeTemporalCandidates implements
// store.RelTypeTemporalCandidateCapability. Each consulted shard prunes the
// ids it covers and an id any shard drops is dropped; an id no consulted shard
// covers is kept.
//
// Consulted: already-open event shards in opts.Depth (never opens one; cold
// shards carry no index). The reference shard only while no archive shard
// exists: ArchiveNode splits a relationship's rows across archive and
// reference, so neither envelope then covers all of them.
func (ts *Store) PruneRelTypeTemporalCandidates(relType uint16, ids []types.RelID, opts QueryOpts) ([]types.RelID, bool) {
	if ts == nil {
		return ids, false
	}
	if opts.ValidAt == 0 && (opts.ValidStart <= 0 || opts.ValidEnd <= 0) {
		return ids, false
	}
	if err := ts.checkOpen(); err != nil {
		return ids, false
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return ids, false
	}
	refKept, indexed := ref.PruneRelTypeTemporalCandidates(relType, ids, opts)
	refCheckin()
	if !indexed {
		return ids, false
	}

	dropped := make([]bool, len(ids))
	anyDropped := false
	mark := func(kept []types.RelID) {
		// A shard's kept list preserves the order of ids.
		j := 0
		for i, id := range ids {
			if j < len(kept) && kept[j] == id {
				j++
				continue
			}
			dropped[i] = true
			anyDropped = true
		}
	}
	if ts.refArchive.Load() == nil && !ts.hasArchiveShard() {
		mark(refKept)
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, release, open, err := es.checkoutOpenStoreForRead(ts)
		if err != nil || !open {
			continue
		}
		kept, ok := store.PruneRelTypeTemporalCandidates(relType, ids, opts)
		release()
		if ok {
			mark(kept)
		}
	}
	if !anyDropped {
		return ids, true
	}
	out := make([]types.RelID, 0, len(ids))
	for i, id := range ids {
		if !dropped[i] {
			out = append(out, id)
		}
	}
	return out, true
}

// --- composite node indexes ---

// CreateCompositePropertyIndex builds a composite index over the ordered keys
// under labelToken on every shard (reference and event labels alike — an
// event label's nodes spread over the event shards). Returns ErrIndexExists
// for a duplicate (labelToken, ordered keys) definition.
func (ts *Store) CreateCompositePropertyIndex(labelToken uint16, keys []string) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}
	return ts.fanOutAnchoredIndex(compositeIndexDef(labelToken, slices.Clone(keys)), true)
}

// DropCompositePropertyIndex removes the composite index declared over the
// exact ordered keys from every shard. Returns ErrIndexNotFound if absent.
func (ts *Store) DropCompositePropertyIndex(labelToken uint16, keys []string) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}
	return ts.fanOutAnchoredIndex(compositeIndexDef(labelToken, slices.Clone(keys)), false)
}

// ListCompositePropertyIndexes returns the declared key tuples registered
// under labelToken, from the anchoring reference shard (caller-owned copies).
func (ts *Store) ListCompositePropertyIndexes(labelToken uint16) ([][]string, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	defer refCheckin()
	return ref.ListCompositePropertyIndexes(labelToken)
}

// NodesByLabelAndProperties folds each shard's AND-conjunction equality
// matches (its composite index, or its label scan + filter without one) into
// one ID-sorted, paginated result. Shards follow NodesByLabelAndProperty:
// reference always, archive at DepthAll, event shards by opts.Depth.
func (ts *Store) NodesByLabelAndProperties(labelToken uint16, values map[string]any, opts QueryOpts) ([]*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	shardOpts := stripDepth(opts)
	shardOpts.Limit, shardOpts.After = 0, 0

	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refNodes, err := ref.NodesByLabelAndProperties(labelToken, values, shardOpts)
	refCheckin()
	if err != nil {
		return nil, err
	}
	parts := [][]*types.Node{refNodes}

	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
		archive, archiveCheckin, err := ts.checkoutArchive()
		if err != nil {
			return nil, err
		}
		if archive != nil {
			nodes, err := archive.NodesByLabelAndProperties(labelToken, values, shardOpts)
			archiveCheckin()
			if err != nil {
				return nil, err
			}
			parts = append(parts, nodes)
		}
	}
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()
	results := make([][]*types.Node, len(eventShards))
	errs := make([]error, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			errs[i] = err
			return
		}
		defer release()
		results[i], errs[i] = store.NodesByLabelAndProperties(labelToken, values, shardOpts)
	})
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	parts = append(parts, results...)
	return applyNodePagination(mergeNodeSlices(parts), opts), nil
}
