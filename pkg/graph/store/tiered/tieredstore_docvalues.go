package tiered

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DocValues (X5 columnar aggregation) on tiered, ADR-0005 §3.4.
//
// CONCATENATE-WITH-ORDINAL-OFFSETS: every shard (reference, archive, every
// event shard) builds its own columnar snapshot over its own membership and
// streams rows to the caller's fn; columns are never merged. The consumer
// aggregates row by row, so only exactly-once emission matters, not global
// ordinal order.
//
// Membership-vs-decline: a per-shard call returns ok=false both for "zero
// members" and for "members but column unbuildable" (mixed types, size cap),
// and the (gen, ok, err) tuple cannot tell them apart. Skipping a declining
// shard is safe only in the first case; in the second it silently undercounts.
// So each fold first takes an exact lower bound on membership from the O(1)
// NodeCountByLabel counter (for Multi the MINIMUM across the label tuple) and
// calls the shard only when it is nonzero. A nonzero shard that still
// declines makes the WHOLE tiered call decline.

// shardMinLabelCount returns the minimum over tokens of store's exact
// NodeCountByLabel: a lower bound on nodes satisfying every label. 0 proves the
// shard contributes no rows, so its column builder is never called.
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
// ForEachDocValuesMulti and DocValuesSnapshot: ref shard, archive (if any),
// then every event shard, DepthAll (membership is not Depth-gated), with the
// checkout/checkin discipline of NodeCountByLabelAndPropertyKey. tokens is the
// membership probe; call runs only on shards with nonzero shardMinLabelCount
// and its bool is the per-shard "usable" signal. The Gate-1 epoch (gen) is
// sampled ONCE before any shard is touched; the caller's post-scan
// NodeMutationEpoch()==gen re-check (Gate 2) detects concurrent writers.
func (ts *Store) foldDocValues(tokens []uint16, call func(store *BadgerStore) (bool, error)) (gen uint64, ok bool, err error) {
	return ts.foldDocValuesServingEmpty(tokens, false, call)
}

// foldDocValuesServingEmpty is foldDocValues; with emptyServes a fold where
// every shard is proved empty by its counts is served as an empty answer
// (ok=true) instead of declined (the intersection door).
func (ts *Store) foldDocValuesServingEmpty(tokens []uint16, emptyServes bool, call func(store *BadgerStore) (bool, error)) (gen uint64, ok bool, err error) {
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

	ts.refArchiveGapHook()
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

	if !anyOK && !emptyServes {
		return 0, false, nil // empty label everywhere → caller falls back
	}
	return gen, true, nil
}

// ForEachDocValues streams the requested property columns for a label's nodes
// shard by shard, in per-shard ordinal order (ADR-0005 §3.4). Membership and
// decline contract: see the file header.
func (ts *Store) ForEachDocValues(labelToken uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	return ts.foldDocValues([]uint16{labelToken}, func(store *BadgerStore) (bool, error) {
		_, shardOK, shardErr := store.ForEachDocValues(labelToken, propKeys, fn)
		return shardOK, shardErr
	})
}

// ForEachDocValuesMulti is ForEachDocValues for a LABEL INTERSECTION (e.g.
// (p:A:B)); the per-shard bound is the MINIMUM count across the tuple.
func (ts *Store) ForEachDocValuesMulti(labelTokens []uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if len(labelTokens) == 0 {
		return 0, false, nil
	}
	return ts.foldDocValuesServingEmpty(labelTokens, true, func(store *BadgerStore) (bool, error) {
		_, shardOK, shardErr := store.ForEachDocValuesMulti(labelTokens, propKeys, fn)
		return shardOK, shardErr
	})
}

// DocValuesSnapshot returns a random-access point-lookup handle spanning every
// shard contributing a member of labelToken. See tieredColumnSnapshot.
func (ts *Store) DocValuesSnapshot(labelToken uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	return ts.snapshotOver([]uint16{labelToken}, false, func(store *BadgerStore) (types.NodeColumnReader, bool, error) {
		s, _, shardOK, shardErr := store.DocValuesSnapshot(labelToken, propKeys)
		return s, shardOK, shardErr
	})
}

// DocValuesSnapshotMulti is DocValuesSnapshot over a LABEL INTERSECTION
// (round 10), under ForEachDocValuesMulti's contract (an intersection empty
// on every shard is served: an empty snapshot). Tiered numbers no node, so
// it has no ordinal snapshot doors.
func (ts *Store) DocValuesSnapshotMulti(labelTokens []uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	if len(labelTokens) == 0 {
		return nil, 0, false, nil
	}
	return ts.snapshotOver(labelTokens, true, func(store *BadgerStore) (types.NodeColumnReader, bool, error) {
		s, _, shardOK, shardErr := store.DocValuesSnapshotMulti(labelTokens, propKeys)
		return s, shardOK, shardErr
	})
}

// snapshotOver folds the shards holding members of tokens through read into
// one reader over their rows in shard-fold order, reading as chunks where
// the shards' snapshots concatenate (indexpkg.NewChunkConcat).
func (ts *Store) snapshotOver(tokens []uint16, emptyServes bool,
	read func(store *BadgerStore) (types.NodeColumnReader, bool, error)) (types.NodeColumnReader, uint64, bool, error) {
	var readers []types.NodeColumnReader
	gen, ok, err := ts.foldDocValuesServingEmpty(tokens, emptyServes, func(store *BadgerStore) (bool, error) {
		s, shardOK, shardErr := read(store)
		if shardErr != nil || !shardOK {
			return shardOK, shardErr
		}
		readers = append(readers, s)
		return true, nil
	})
	if err != nil || !ok {
		return nil, gen, ok, err
	}
	snapshot := &tieredColumnSnapshot{readers: readers, epoch: gen}
	if chunks, chunked := indexpkg.NewChunkConcat(readers); chunked {
		return &tieredChunkSnapshot{tieredColumnSnapshot: snapshot, chunks: chunks}, gen, true, nil
	}
	return snapshot, gen, true, nil
}

// tieredChunkSnapshot is a tieredColumnSnapshot whose shards' columns also
// read as typed chunks (types.NodeColumnChunkReader, sigma-tkgd round 9):
// the shards' rows concatenated in shard-fold order, as RowAt reads them; a
// column holding strings on one shard and numbers on another reads as
// types.ColumnVectorMixed (round 10).
type tieredChunkSnapshot struct {
	*tieredColumnSnapshot
	chunks *indexpkg.ChunkConcat
}

var _ types.NodeColumnChunkReader = (*tieredChunkSnapshot)(nil)

// ReadChunk implements types.NodeColumnChunkReader.
func (s *tieredChunkSnapshot) ReadChunk(from, to int, chunk *types.ColumnChunk) {
	s.chunks.ReadChunk(from, to, chunk)
}

// tieredColumnSnapshot implements types.NodeColumnReader over a label spanning
// several shards: each reader is one shard's immutable point-lookup snapshot,
// tried in shard-fold order until one reports membership. A node lives on one
// shard (primary-label class immutable, B33), so at most one reader hits. No
// store lock or shard checkout is retained, so nothing is pinned open.
type tieredColumnSnapshot struct {
	readers []types.NodeColumnReader
	epoch   uint64
}

// Len is the member count over every shard when each shard's snapshot reads
// rows by position, else 0 (then RowAt has no rows; use Row or
// ForEachDocValues).
func (s *tieredColumnSnapshot) Len() int {
	n := 0
	for _, r := range s.readers {
		rr, ok := r.(types.NodeColumnRowReader)
		if !ok {
			return 0
		}
		n += rr.Len()
	}
	return n
}

// RowAt reads position i of the shards' rows concatenated in shard-fold order
// (types.NodeColumnRowReader); i outside 0..Len()-1 panics.
func (s *tieredColumnSnapshot) RowAt(i int, vals []any, present []bool) types.NodeID {
	for _, r := range s.readers {
		rr := r.(types.NodeColumnRowReader)
		if i < rr.Len() {
			return rr.RowAt(i, vals, present)
		}
		i -= rr.Len()
	}
	panic("tiered column snapshot: row position out of range")
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

// NodeMutationEpoch returns the store-global node-mutation epoch. Every shard
// (reference, archive, event, including lazily opened or rotation-created
// ones) shares one badger.SharedMutationEpochs, so it moves on every node
// write whether or not the shard is still open (a sum of open shards' epochs
// could return to a held value). Never force-opens a shard.
func (ts *Store) NodeMutationEpoch() uint64 {
	if ts == nil {
		return 0
	}
	return ts.mutationEpochs.NodeMutationEpoch()
}

// RelMutationEpoch is NodeMutationEpoch's relationship mirror, including both
// halves of a cross-shard relationship write.
func (ts *Store) RelMutationEpoch() uint64 {
	if ts == nil {
		return 0
	}
	return ts.mutationEpochs.RelMutationEpoch()
}

var _ types.NodeColumnRowReader = (*tieredColumnSnapshot)(nil)
