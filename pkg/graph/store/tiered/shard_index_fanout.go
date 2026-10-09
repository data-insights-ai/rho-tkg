package tiered

import (
	"errors"
	"fmt"
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship-type temporal indexes and composite node indexes (backlog 10,
// ai-soc request 3) — the sharded store's fan-out shape on the tiered store:
// every shard (reference, archive, every event shard, cold shards opened for
// the DDL) builds and maintains its own badger index over its own rows, and
// the tiered store folds the per-shard answers. Nothing moves on rotation: a
// relationship or node stays on the shard it was written to, and so does its
// index entry. A shard's index lives exactly as badger keeps it — the
// definition persists in the shard, the entries are RAM-resident and rebuilt
// when the shard opens (also a lazily reopened cold shard).
//
// The REFERENCE shard is the anchor: an index exists iff the reference shard
// holds its definition. Create builds on every other shard first and on the
// reference shard last; drop removes it from the others first and from the
// reference shard last — so an interrupted DDL leaves at most extra or missing
// copies on non-reference shards, never an anchor that disagrees with what
// was fully applied. syncAnchoredIndexes copies the anchor's definitions onto
// a shard that opens later (the hot shard a rotation creates, the archive)
// and repairs every open shard at store open.
//
// Sizing (CHANGELOG, measured): the rel temporal index costs ~156 B per
// indexed relationship and a 3-key composite ~310 B per indexed node, per
// open shard; a reopened shard rebuilds at ~0.02 M rels/s.

var (
	_ storecontract.RelTypeTemporalIndexCapability        = (*Store)(nil)
	_ storecontract.RelTypeTemporalCandidateCapability    = (*Store)(nil)
	_ storecontract.CompositePropertyIndexCapability      = (*Store)(nil)
	_ storecontract.CompositeIndexIntrospectionCapability = (*Store)(nil)
)

// anchoredIndex describes one per-shard index definition for the fan-out.
type anchoredIndex struct {
	what    string
	has     func(*BadgerStore) (bool, error)
	create  func(*BadgerStore) error
	drop    func(*BadgerStore) error
	exists  error // the badger create's duplicate sentinel
	missing error // the badger drop's not-found sentinel
}

func relTemporalIndexDef(relType uint16) anchoredIndex {
	return anchoredIndex{
		what: "relationship temporal index",
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
	for _, ref := range anchorLast(refs) {
		err := ts.withTemporalIndexShard(ref, func(ns namedStore) error {
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
// relationship temporal and composite definitions: it creates the missing
// ones (a hot shard a rotation opens, the archive, a shard an interrupted
// create skipped) and drops the extra ones (a shard an interrupted create
// reached before it failed). Creating builds the index from the shard's rows.
func (ts *Store) syncAnchoredIndexes(store *BadgerStore) error {
	ref := ts.refShard
	if store == nil || ref == nil || store == ref {
		return nil
	}
	want, err := ref.RelTemporalIndexTypes()
	if err != nil {
		return fmt.Errorf("graph: read anchored relationship temporal indexes: %w", err)
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

// --- relationship-type temporal indexes ---

// CreateRelTemporalIndex builds a temporal interval index over relType on
// every shard. Returns ErrTemporalIndexExists if it already exists.
func (ts *Store) CreateRelTemporalIndex(relType uint16) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}
	return ts.fanOutAnchoredIndex(relTemporalIndexDef(relType), true)
}

// DropRelTemporalIndex removes the rel-type temporal index from every shard.
// Returns ErrTemporalIndexNotFound if no such index exists.
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
// store.RelTypeTemporalCandidateCapability. A relationship's rows (current and
// history) live on one shard, whose envelope index is a sound superset of
// them; an id no consulted shard covers is kept. Each consulted shard prunes
// the ids it covers, and an id any shard drops is dropped.
//
// Consulted: the event shards in opts.Depth that are already open — the prune
// never opens a cold shard (the query's own candidate scan does, and a shard
// it opened stays open under MaxOpenColdShards), and leaving a shard out only
// keeps its ids. The reference shard is consulted only while no archive shard
// exists: ArchiveNode moves a relationship's row to the archive and leaves its
// history on the reference shard, so neither shard's envelope then covers all
// of that relationship's rows.
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
