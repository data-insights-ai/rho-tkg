package tiered

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// NodePropertyStats folds NDV/min/max/count planner statistics for
// (labelToken, propertyKey) across every shard — refShard, refArchive (if
// open/present), and every event shard (hot + warm + cold) — mirroring the
// checkout/checkin fold discipline of NodeCountByLabelAndPropertyKey
// (tieredstore_read_bulk.go).
//
// Count and Min/Max fold trivially (SUM and min-of-mins/max-of-maxes), but
// NDV is a HyperLogLog ESTIMATE and estimates do not sum: a value present on
// two shards would be double-counted by summing per-shard Estimate()s. The
// correct fold is a register-max MERGE of the raw per-shard sketches
// (indexpkg.HyperLogLog.Merge), with Estimate() called exactly ONCE on the
// merged sketch — see docs/adr/0005-tiered-parity.md §3.1. Each shard's
// concrete *BadgerStore exposes the raw sketch via the store-internal (not
// public-contract) NodePropertyStatsSketch method.
//
// Merge returns ErrHLLPrecisionMismatch if two shards' sketches were built
// at different precisions — every shard uses the same
// indexpkg.DefaultHLLPrecision, so this cannot fire in practice, but the
// error is PROPAGATED rather than discarded: silently ignoring a precision
// mismatch would silently under-count NDV, exactly the failure mode the
// merge exists to prevent.
func (ts *Store) NodePropertyStats(labelToken uint16, propertyKey string) (storecontract.PropertyStats, error) {
	return ts.foldNodePropertySketches(labelToken, propertyKey, (*BadgerStore).NodePropertyStatsSketch)
}

// NodePropertyNDV implements store.NodePropertyNDVCapability: the same
// cross-shard sketch merge as NodePropertyStats, over each shard's
// NodePropertyNDVSketch, which never rescans a shard's Min/Max (round 5
// request 1).
func (ts *Store) NodePropertyNDV(labelToken uint16, propertyKey string) (storecontract.PropertyNDV, error) {
	stats, err := ts.foldNodePropertySketches(labelToken, propertyKey, func(bs *BadgerStore, tok uint16, key string) (*indexpkg.HyperLogLog, any, any, int64, error) {
		sketch, count, err := bs.NodePropertyNDVSketch(tok, key)
		return sketch, nil, nil, count, err
	})
	if err != nil {
		return storecontract.PropertyNDV{}, err
	}
	return storecontract.PropertyNDV{NDV: stats.NDV, Count: stats.Count}, nil
}

// shardPropertySketch reads one shard's sketch, extrema and count for the
// fold (NodePropertyStatsSketch, or NodePropertyNDVSketch without extrema).
type shardPropertySketch func(bs *BadgerStore, labelToken uint16, propertyKey string) (*indexpkg.HyperLogLog, any, any, int64, error)

// foldNodePropertySketches is the cross-shard fold of NodePropertyStats (see
// there) over fetch.
func (ts *Store) foldNodePropertySketches(labelToken uint16, propertyKey string, fetch shardPropertySketch) (storecontract.PropertyStats, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyStats{}, err
	}

	var (
		sketch   *indexpkg.HyperLogLog
		min, max any
		count    int64
	)

	// merge folds one shard's (sketch, min, max, count) into the running
	// totals. Sequential — called once per shard, never concurrently with
	// itself, even though event-shard sketches are COLLECTED under
	// queryEventShards' internal goroutines (each goroutine writes into its
	// own results[i] slot; this function only runs over the collected slice
	// afterward).
	merge := func(shardSketch *indexpkg.HyperLogLog, shardMin, shardMax any, shardCount int64) error {
		if shardSketch != nil {
			if sketch == nil {
				sketch = shardSketch.Clone()
			} else if err := sketch.Merge(shardSketch); err != nil {
				return err
			}
		}
		min, max = indexpkg.CombineExtrema(min, max, shardMin, shardMax)
		count += shardCount
		return nil
	}

	move := ts.lockMoveRead() // a node moved mid-read would count twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return storecontract.PropertyStats{}, err
	}
	refSketch, refMin, refMax, refCount, refErr := fetch(ref, labelToken, propertyKey)
	refCheckin()
	if refErr != nil {
		return storecontract.PropertyStats{}, refErr
	}
	if err := merge(refSketch, refMin, refMax, refCount); err != nil {
		return storecontract.PropertyStats{}, err
	}

	ts.refArchiveGapHook()
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return storecontract.PropertyStats{}, archiveErr
	}
	if archive != nil {
		aSketch, aMin, aMax, aCount, aErr := fetch(archive, labelToken, propertyKey)
		archiveCheckin()
		if aErr != nil {
			return storecontract.PropertyStats{}, aErr
		}
		if err := merge(aSketch, aMin, aMax, aCount); err != nil {
			return storecontract.PropertyStats{}, err
		}
	}
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	type result struct {
		sketch   *indexpkg.HyperLogLog
		min, max any
		count    int64
		err      error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].sketch, results[i].min, results[i].max, results[i].count, results[i].err =
			fetch(store, labelToken, propertyKey)
	})
	for _, r := range results {
		if r.err != nil {
			return storecontract.PropertyStats{}, r.err
		}
		if err := merge(r.sketch, r.min, r.max, r.count); err != nil {
			return storecontract.PropertyStats{}, err
		}
	}

	var ndv int64
	if sketch != nil {
		ndv = sketch.Estimate()
	}
	return storecontract.PropertyStats{NDV: ndv, Min: min, Max: max, Count: count}, nil
}
