package sharded

import (
	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DocValues (X5 columnar reads) on the sharded store.
//
// Each slot's badger shard builds and caches its own column snapshot over its
// own members, exactly as a single badger store does; the sharded layer never
// merges columns. A node lives on exactly one slot for its whole life (the
// slot is part of its ID), so concatenating the slots' rows emits every label
// member exactly once.
//
// Order: ForEachDocValues / ForEachDocValuesMulti stream slot by slot in slot
// order, each slot's rows in that slot's ordinal (ascending ID) order; the
// concatenation is not globally ascending by ID. A snapshot's positions
// (RowAt) follow the same order.
//
// Membership against decline: a shard's own call returns ok=false both when it
// has no member and when its column cannot be built (mixed values, over the
// size cap). Skipping the second kind would silently drop members, so each
// shard is first bounded by its exact NodeCountByLabel (the minimum over the
// labels for an intersection): a shard with no member is skipped without
// building anything, and a shard with members whose call declines makes the
// whole call decline. No member on any shard declines too (the caller falls
// back and finds zero rows), as on badger.
//
// gen: the sum over slots of the per-label epochs (NodeLabelMutationEpoch, for
// an intersection the sum over its labels), sampled once before any shard is
// read. Every node write bumps the epochs of its labels on the slot it lands
// on, so the sum moves on every write to a member of the label on any slot; a
// consumer re-checks NodeLabelMutationEpoch(label) (summed over the labels for
// an intersection) after consuming the rows, the same Gate-2 as on badger.

// NodeLabelMutationEpoch sums the slots' per-label node-mutation epochs for
// labelToken (see badger.Store.NodeLabelMutationEpoch): it advances whenever a
// node carrying the label is written on any slot.
func (s *Store) NodeLabelMutationEpoch(labelToken uint16) uint64 {
	if s == nil {
		return 0
	}
	var sum uint64
	for _, shard := range s.shards {
		if shard != nil {
			sum += shard.NodeLabelMutationEpoch(labelToken)
		}
	}
	return sum
}

// labelsEpoch is the sum of NodeLabelMutationEpoch over tokens.
func (s *Store) labelsEpoch(tokens []uint16) uint64 {
	var sum uint64
	for _, tok := range tokens {
		sum += s.NodeLabelMutationEpoch(tok)
	}
	return sum
}

// shardMinLabelCount is the minimum over tokens of the shard's exact
// NodeCountByLabel: 0 proves the shard holds no member of the intersection.
func shardMinLabelCount(shard *badger.Store, tokens []uint16) (int, error) {
	minCount := -1
	for _, tok := range tokens {
		n, err := shard.NodeCountByLabel(tok)
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

// foldDocValues visits every shard in slot order. call runs only on a shard
// with members (see the membership note above); its bool is the shard's
// usable signal. gen is sampled before the first shard is read.
func (s *Store) foldDocValues(tokens []uint16, call func(idx int, shard *badger.Store) (bool, error)) (gen uint64, ok bool, err error) {
	if err := s.checkOpen(); err != nil {
		return 0, false, err
	}
	gen = s.labelsEpoch(tokens)
	anyOK := false
	for idx, shard := range s.shards {
		n, err := shardMinLabelCount(shard, tokens)
		if err != nil {
			return 0, false, err
		}
		if n == 0 {
			continue
		}
		shardOK, err := call(idx, shard)
		if err != nil {
			return 0, false, err
		}
		if !shardOK {
			return 0, false, nil
		}
		anyOK = true
	}
	if !anyOK {
		return 0, false, nil
	}
	return gen, true, nil
}

// ForEachDocValues streams the requested property columns of labelToken's
// nodes slot by slot (see the order note above). ok=false (no error) when the
// column path cannot serve the label: no member, or a slot with members whose
// column is unbuildable (mixed or unsupported values, over the size cap, a
// shard without RAM label membership).
func (s *Store) ForEachDocValues(labelToken uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if s == nil {
		return 0, false, ErrNilStore
	}
	stopped := false
	return s.foldDocValues([]uint16{labelToken}, func(_ int, shard *badger.Store) (bool, error) {
		if stopped {
			return true, nil
		}
		_, shardOK, err := shard.ForEachDocValues(labelToken, propKeys, func(id types.NodeID, vals []any, present []bool) bool {
			if !fn(id, vals, present) {
				stopped = true
				return false
			}
			return true
		})
		return shardOK, err
	})
}

// ForEachDocValuesMulti is ForEachDocValues over the intersection of
// labelTokens (a multi-label pattern). Same order and decline contract; a
// shard is bounded by its smallest label count.
func (s *Store) ForEachDocValuesMulti(labelTokens []uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if s == nil {
		return 0, false, ErrNilStore
	}
	if len(labelTokens) == 0 {
		return 0, false, nil
	}
	stopped := false
	return s.foldDocValues(labelTokens, func(_ int, shard *badger.Store) (bool, error) {
		if stopped {
			return true, nil
		}
		_, shardOK, err := shard.ForEachDocValuesMulti(labelTokens, propKeys, func(id types.NodeID, vals []any, present []bool) bool {
			if !fn(id, vals, present) {
				stopped = true
				return false
			}
			return true
		})
		return shardOK, err
	})
}

// DocValuesSnapshot returns a point-lookup and by-position reader over
// labelToken's members on every slot: Row routes an ID to its own slot's
// snapshot (no probing of the others), Len/RowAt read the slots' rows
// concatenated in slot order. Each slot's snapshot is immutable once built,
// so the reader pins no shard and is safe for concurrent use with a buffer
// pair per goroutine. Same decline contract as ForEachDocValues.
func (s *Store) DocValuesSnapshot(labelToken uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	if s == nil {
		return nil, 0, false, ErrNilStore
	}
	readers := make([]types.NodeColumnReader, len(s.shards))
	gen, ok, err = s.foldDocValues([]uint16{labelToken}, func(idx int, shard *badger.Store) (bool, error) {
		r, _, shardOK, err := shard.DocValuesSnapshot(labelToken, propKeys)
		if err != nil || !shardOK {
			return shardOK, err
		}
		readers[idx] = r
		return true, nil
	})
	if err != nil || !ok {
		return nil, 0, ok, err
	}
	out := &columnSnapshot{store: s, readers: readers, epoch: gen, offsets: make([]int, len(readers)+1)}
	out.rows = true
	for i, r := range readers {
		n := 0
		if r != nil {
			rr, isRows := r.(types.NodeColumnRowReader)
			if !isRows {
				out.rows = false
			} else {
				n = rr.Len()
			}
		}
		out.offsets[i+1] = out.offsets[i] + n
	}
	return out, gen, true, nil
}

// columnSnapshot is the sharded types.NodeColumnRowReader: one immutable
// snapshot per slot (nil for a slot without members), offsets[i] the position
// of slot i's first row.
type columnSnapshot struct {
	store   *Store
	readers []types.NodeColumnReader
	offsets []int
	rows    bool
	epoch   uint64
}

var _ types.NodeColumnRowReader = (*columnSnapshot)(nil)

// Row fills vals/present for id from its own slot's snapshot; false (buffers
// untouched) for a non-member or an ID of a slot this store does not claim.
func (c *columnSnapshot) Row(id types.NodeID, vals []any, present []bool) bool {
	idx, ok := c.store.catalog.shardIndexForSlot(slotOf(id.SnowflakeID()))
	if !ok || idx >= len(c.readers) || c.readers[idx] == nil {
		return false
	}
	return c.readers[idx].Row(id, vals, present)
}

// Epoch is the gen the snapshot was built at (see the gen note above).
func (c *columnSnapshot) Epoch() uint64 { return c.epoch }

// Len is the member count over every slot, or 0 when a slot's snapshot does
// not read by position (then RowAt has no rows; use Row or ForEachDocValues).
func (c *columnSnapshot) Len() int {
	if !c.rows {
		return 0
	}
	return c.offsets[len(c.offsets)-1]
}

// RowAt reads position i of the slots' rows concatenated in slot order;
// i outside 0..Len()-1 panics.
func (c *columnSnapshot) RowAt(i int, vals []any, present []bool) types.NodeID {
	if i < 0 || i >= c.Len() {
		panic("sharded column snapshot: row position out of range")
	}
	for k, r := range c.readers {
		if i < c.offsets[k+1] {
			return r.(types.NodeColumnRowReader).RowAt(i-c.offsets[k], vals, present)
		}
	}
	panic("sharded column snapshot: row position out of range")
}
