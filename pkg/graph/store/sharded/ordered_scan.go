package sharded

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ordered walks and stated bounds over the slots (sigma-tkgd round 10). A
// property index lives on every slot and an entity's rows on its own ID's
// slot only, so the slots are disjoint parts: the ordered walks merge the
// slots' walks in the index's order (index.MergeNumericOrdered /
// MergeStringOrdered, lazily: a LIMIT reads about k rows per slot), the
// bounds fold the slots' bounds (index.FoldPropertyBounds). A slot without a
// usable index (none, Elements, building) fails the walk with
// ErrIndexNotFound and the bounds with ok=false, as one badger store does.

var (
	_ storecontract.NodeOrderedRangeScanCapability = (*Store)(nil)
	_ storecontract.NodePrefixScanCapability       = (*Store)(nil)
	_ storecontract.RelOrderedRangeScanCapability  = (*Store)(nil)
	_ storecontract.RelPrefixScanCapability        = (*Store)(nil)
	_ storecontract.NodePropertyBoundsCapability   = (*Store)(nil)
	_ storecontract.RelPropertyBoundsCapability    = (*Store)(nil)
)

// slotParts is one ordered part per slot.
func slotParts[R any](s *Store, walk func(shard *badgerShard, yield func(R) bool) error) []indexpkg.OrderedPart[R] {
	parts := make([]indexpkg.OrderedPart[R], len(s.shards))
	for i, shard := range s.shards {
		parts[i] = func(yield func(R) bool) error { return walk(shard, yield) }
	}
	return parts
}

// ForEachNodeByLabelPropertyRangeOrdered implements
// store.NodeOrderedRangeScanCapability over the slots.
func (s *Store) ForEachNodeByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Node) bool) error {
	if err := s.orderedScanArgs(fn == nil); err != nil {
		return err
	}
	return indexpkg.MergeNodesNumeric(slotParts(s, func(shard *badgerShard, yield func(*types.Node) bool) error {
		return shard.ForEachNodeByLabelPropertyRangeOrdered(token, propKey, min, max, inclMin, inclMax, desc, yield)
	}), propKey, desc, fn)
}

// ForEachNodeByLabelPropertyPrefix implements store.NodePrefixScanCapability
// over the slots.
func (s *Store) ForEachNodeByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(*types.Node) bool) error {
	if err := s.orderedScanArgs(fn == nil); err != nil {
		return err
	}
	return indexpkg.MergeNodesString(slotParts(s, func(shard *badgerShard, yield func(*types.Node) bool) error {
		return shard.ForEachNodeByLabelPropertyPrefix(token, propKey, prefix, desc, yield)
	}), propKey, desc, fn)
}

// ForEachRelByTypePropertyRangeOrdered implements
// store.RelOrderedRangeScanCapability over the slots.
func (s *Store) ForEachRelByTypePropertyRangeOrdered(typeToken uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Relationship) bool) error {
	if err := s.orderedScanArgs(fn == nil); err != nil {
		return err
	}
	return indexpkg.MergeRelsNumeric(slotParts(s, func(shard *badgerShard, yield func(*types.Relationship) bool) error {
		return shard.ForEachRelByTypePropertyRangeOrdered(typeToken, propKey, min, max, inclMin, inclMax, desc, yield)
	}), propKey, desc, fn)
}

// ForEachRelByTypePropertyPrefix implements store.RelPrefixScanCapability
// over the slots.
func (s *Store) ForEachRelByTypePropertyPrefix(typeToken uint16, propKey, prefix string, desc bool, fn func(*types.Relationship) bool) error {
	if err := s.orderedScanArgs(fn == nil); err != nil {
		return err
	}
	return indexpkg.MergeRelsString(slotParts(s, func(shard *badgerShard, yield func(*types.Relationship) bool) error {
		return shard.ForEachRelByTypePropertyPrefix(typeToken, propKey, prefix, desc, yield)
	}), propKey, desc, fn)
}

// orderedScanArgs is the walks' own check (the slots validate the rest).
func (s *Store) orderedScanArgs(nilFn bool) error {
	if s == nil {
		return ErrNilStore
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	if nilFn {
		return storecontract.ErrInvalidStoreMutation
	}
	return nil
}

// NodePropertyBounds implements store.NodePropertyBoundsCapability: the
// slots' bounds folded; ok=false when any slot cannot state its own.
func (s *Store) NodePropertyBounds(labelToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	return s.foldBounds(func(shard *badgerShard) (storecontract.PropertyBounds, bool, error) {
		return shard.NodePropertyBounds(labelToken, propertyKey)
	})
}

// RelPropertyBounds implements store.RelPropertyBoundsCapability over the
// slots, as NodePropertyBounds.
func (s *Store) RelPropertyBounds(relTypeToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	return s.foldBounds(func(shard *badgerShard) (storecontract.PropertyBounds, bool, error) {
		return shard.RelPropertyBounds(relTypeToken, propertyKey)
	})
}

func (s *Store) foldBounds(read func(*badgerShard) (storecontract.PropertyBounds, bool, error)) (storecontract.PropertyBounds, bool, error) {
	if s == nil {
		return storecontract.PropertyBounds{}, false, ErrNilStore
	}
	if err := s.checkOpen(); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	parts := make([]storecontract.PropertyBounds, len(s.shards))
	oks := make([]bool, len(s.shards))
	if err := s.forEachShardErr(func(i int, shard *badgerShard) error {
		var err error
		parts[i], oks[i], err = read(shard)
		return err
	}); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	b, ok := indexpkg.FoldStatedBounds(parts, oks)
	return b, ok, nil
}

// StatesPropertyBounds implements store.PropertyBoundsStatedCapability: every
// slot is opened with the store's configuration, so the first slot answers.
func (s *Store) StatesPropertyBounds() (nodes, rels bool) {
	if s == nil || len(s.shards) == 0 {
		return false, false
	}
	return s.shards[0].StatesPropertyBounds()
}
