package tiered

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DocValues (X5 columnar aggregation) on tiered — ADR-0005 §3.4.
//
// Design: CONCATENATE-WITH-ORDINAL-OFFSETS, not fold-into-one-column. Each
// shard (refShard, refArchive, every event shard regardless of tier) builds
// its OWN columnar snapshot over its OWN membership exactly as badger/memory
// already do internally, and streams its rows directly to the caller's fn;
// the tiered layer never merges columns into one aggregate vector. This is
// correct because the consumer receives rows ONE AT A TIME and performs its
// own grouping/aggregation — the GLOBAL ordinal order across shards does not
// matter, only that every label member is emitted EXACTLY ONCE with its
// correct value. Reference labels are effectively single/two-shard (refShard
// + refArchive, since reference entities never route to an event shard), so
// most DocValues queries on tiered touch only those; an event label may span
// many event shards.
//
// Membership-vs-decline. A per-shard ForEachDocValues/ForEachDocValuesMulti
// call returns ok=false for BOTH "this shard has zero members for the
// label(s)" and "this shard has members but the column is unbuildable
// (mixed/unsupported types, or over the size cap)" — the two cannot be told
// apart from the (gen, ok, err) tuple alone (gen is literally 0 in the
// declined case, not the shard's real epoch). Silently skipping a declining
// shard would be safe ONLY in the first case; in the second it would
// silently OMIT real members from the aggregate — an undercount a caller
// would never detect (ok=true would still be returned overall). So every
// fold here first determines an EXACT lower bound on membership via the O(1)
// NodeCountByLabel counter(s) — for Multi, the MINIMUM per-shard count across
// the label tuple, since an intersection can never exceed its smallest
// constituent set — and only calls the shard's own DocValues method when
// that bound is nonzero. A shard whose bound is nonzero but whose own call
// still declines forces the WHOLE tiered call to decline (never a partial or
// silently-incomplete columnar answer).

// shardMinLabelCount returns the minimum, over tokens, of store's exact
// NodeCountByLabel — a safe lower bound on how many nodes on this shard could
// possibly satisfy every label in tokens (their intersection can never
// exceed the smallest constituent label's count on this shard). A result of
// 0 proves the shard contributes no rows and can be skipped without calling
// into its (possibly expensive) column builder at all.
func shardMinLabelCount(store *BadgerStore, tokens []uint16) (int, error) {
	minCount := -1
	for _, tok := range tokens {
		n, err := store.NodeCountByLabel(tok)
		if err != nil {
			return 0, err
		}
		if minCount == -1 || n < minCount {
			minCount = n
		}
	}
	if minCount < 0 {
		return 0, nil
	}
	return minCount, nil
}

// foldDocValues is the shared cross-shard traversal for ForEachDocValues,
// ForEachDocValuesMulti, and DocValuesSnapshot: checkout ref shard, checkout
// archive (if any), then every event shard (ref + archive, DepthAll — a
// label's membership is not Depth-gated; DocValues has no QueryOpts), in the
// same checkout/checkin discipline as NodeCountByLabelAndPropertyKey
// (tieredstore_read_bulk.go). tokens is the membership-count probe (a single
// label for ForEachDocValues/DocValuesSnapshot, the full tuple for Multi);
// call is invoked ONLY on a shard whose shardMinLabelCount is nonzero, and
// its bool return is the per-shard "usable" signal (mirrors the store-level
// ok). The Gate-1 epoch snapshot (returned as gen) is taken ONCE, before any
// shard is touched, exactly like badger/memory sample their own nodeEpoch at
// build-start — the caller's post-scan NodeMutationEpoch()==gen re-check
// (Gate 2) is what actually detects a concurrent writer.
func (ts *Store) foldDocValues(tokens []uint16, call func(store *BadgerStore) (bool, error)) (gen uint64, ok bool, err error) {
	if ts == nil {
		return 0, false, ErrNilStore
	}
	if cerr := ts.checkOpen(); cerr != nil {
		return 0, false, cerr
	}

	gen = ts.NodeMutationEpoch()
	anyOK := false

	visit := func(store *BadgerStore) (declined bool, verr error) {
		n, cerr := shardMinLabelCount(store, tokens)
		if cerr != nil {
			return false, cerr
		}
		if n == 0 {
			return false, nil // provably no members here — safe to skip
		}
		shardOK, shardErr := call(store)
		if shardErr != nil {
			return false, shardErr
		}
		if !shardOK {
			return true, nil // members exist but the column path couldn't serve them
		}
		anyOK = true
		return false, nil
	}

	ref, refCheckin, rerr := ts.checkoutRefShard()
	if rerr != nil {
		return 0, false, rerr
	}
	declined, verr := visit(ref)
	refCheckin()
	if verr != nil {
		return 0, false, verr
	}
	if declined {
		return 0, false, nil
	}

	archive, archiveCheckin, aerr := ts.checkoutArchive()
	if aerr != nil {
		return 0, false, aerr
	}
	if archive != nil {
		declined, verr = visit(archive)
		archiveCheckin()
		if verr != nil {
			return 0, false, verr
		}
		if declined {
			return 0, false, nil
		}
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, release, serr := es.checkoutStoreForRead(ts)
		if serr != nil {
			return 0, false, serr
		}
		declined, verr = visit(store)
		release()
		if verr != nil {
			return 0, false, verr
		}
		if declined {
			return 0, false, nil
		}
	}

	if !anyOK {
		return 0, false, nil // empty label/intersection everywhere → caller falls back
	}
	return gen, true, nil
}

// ForEachDocValues streams the requested property columns for a label's
// nodes, in per-shard ordinal order, concatenating each contributing shard's
// rows one after another (ADR-0005 §3.4). See the package-level DocValues
// doc above for the membership/decline contract.
func (ts *Store) ForEachDocValues(labelToken uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	return ts.foldDocValues([]uint16{labelToken}, func(store *BadgerStore) (bool, error) {
		_, shardOK, shardErr := store.ForEachDocValues(labelToken, propKeys, fn)
		return shardOK, shardErr
	})
}

// ForEachDocValuesMulti streams the requested property columns for a LABEL
// INTERSECTION (a multi-label pattern like (p:A:B)), concatenating each
// contributing shard's intersection rows. Same membership/decline contract as
// ForEachDocValues, with the per-shard bound taken as the MINIMUM count across
// the label tuple.
func (ts *Store) ForEachDocValuesMulti(labelTokens []uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if len(labelTokens) == 0 {
		return 0, false, nil
	}
	return ts.foldDocValues(labelTokens, func(store *BadgerStore) (bool, error) {
		_, shardOK, shardErr := store.ForEachDocValuesMulti(labelTokens, propKeys, fn)
		return shardOK, shardErr
	})
}

// DocValuesSnapshot returns a random-access point-lookup handle spanning every
// shard that contributes a member of labelToken (the X5 expand-aggregation
// target side). See tieredColumnSnapshot for the dispatch strategy.
func (ts *Store) DocValuesSnapshot(labelToken uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	var readers []types.NodeColumnReader
	gen, ok, err = ts.foldDocValues([]uint16{labelToken}, func(store *BadgerStore) (bool, error) {
		s, _, shardOK, shardErr := store.DocValuesSnapshot(labelToken, propKeys)
		if shardErr != nil || !shardOK {
			return shardOK, shardErr
		}
		readers = append(readers, s)
		return true, nil
	})
	if err != nil || !ok {
		return nil, gen, ok, err
	}
	return &tieredColumnSnapshot{readers: readers, epoch: gen}, gen, true, nil
}

// tieredColumnSnapshot implements types.NodeColumnReader over a label spanning
// multiple shards (concatenate-with-ordinal-offsets, ADR-0005 §3.4): each
// underlying reader is one shard's own immutable point-lookup snapshot, tried
// in shard-fold order until one reports membership. A node lives on exactly
// one shard at a time (a version chain never splits across shards — CLAUDE.md
// B33, primary-label class immutable), so at most one reader ever reports a
// hit — no double-emission risk. Every underlying snapshot is self-contained
// (no store lock or shard checkout retained after DocValuesSnapshot returns:
// badger/memory hand back a pointer into an immutable, already-built column),
// so holding this slice pins no shard open.
type tieredColumnSnapshot struct {
	readers []types.NodeColumnReader
	epoch   uint64
}

// Row dispatches to the first underlying shard reader that reports id as a
// member. Returns false (buffers untouched) if no shard's snapshot has id.
func (s *tieredColumnSnapshot) Row(id types.NodeID, vals []any, present []bool) bool {
	for _, r := range s.readers {
		if r.Row(id, vals, present) {
			return true
		}
	}
	return false
}

// Epoch is the aggregate (store-wide) epoch the tiered snapshot was built at.
func (s *tieredColumnSnapshot) Epoch() uint64 { return s.epoch }

// NodeMutationEpoch returns the store-global node-mutation epoch: the SUM of
// every CURRENTLY-OPEN shard's own node-mutation epoch (refShard, refArchive
// if open, every event shard that is not presently a closed cold shard).
// ADR-0005 §3.4's Gate-2 fold: each mutation on a shard bumps that shard's own
// atomic counter by exactly 1 (badger: bs.nodeEpoch.Add(1) on every non-delete
// node write, delete, and Clear), so the SUM changes deterministically
// whenever ANY shard is mutated — the consumer's post-scan
// NodeMutationEpoch()==gen re-check (Gate 2) discards a torn cross-shard
// aggregate whenever any shard the fold touched (or could have touched)
// changed in between.
//
// Deliberately does NOT force-open a closed cold shard merely to read its
// counter — that would turn every staleness poll into an O(shards) Badger
// open storm, defeating tiered's lazy-open contract for a check callers run
// after every aggregation. A closed cold shard therefore contributes 0
// (documented ADR-0005 §3.4 risk: SUM is stable for a QUIESCENT store because
// a shard that stays closed across two samples always contributes 0 both
// times, and a rotation-created shard starts at epoch 0 too — but a write
// that reaches a cold shard and is followed by that shard idle-closing again
// before the NEXT poll can make the SUM return to its pre-write value,
// masking that one mutation if no OTHER shard's contribution changed in the
// same window. In practice a write requires the shard to be open, so the
// unsafe window is the idle-close interval — several minutes by default,
// narrow relative to a single aggregation's Gate-1→Gate-2 span, and the same
// order of trade-off already accepted by every other tiered fold that treats
// a closed cold shard as contributing nothing until touched, e.g.
// NodeCountByLabelAndPropertyKey's checkout discipline).
func (ts *Store) NodeMutationEpoch() uint64 {
	if ts == nil {
		return 0
	}
	var sum uint64
	if ts.refShard != nil {
		sum += ts.refShard.NodeMutationEpoch()
	}
	if archive := ts.refArchive.Load(); archive != nil {
		sum += archive.NodeMutationEpoch()
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, release, open, err := es.checkoutOpenStoreForRead(ts)
		if err != nil || !open {
			continue // closed cold shard, or a fenced store — contributes 0
		}
		sum += store.NodeMutationEpoch()
		release()
	}
	return sum
}
