package core

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// orderedIDCtxEvery is how many entries an ordered ID walk hands out between
// two checks of its context.
const orderedIDCtxEvery = 4096

// nodeOrderedIDScanner is the OPTIONAL store capability behind the ordered
// node ID walks (memory, badger).
type nodeOrderedIDScanner interface {
	ForEachNodeIDByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(types.NodeID, any) bool) error
	ForEachNodeIDByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(types.NodeID, any) bool) error
}

// relOrderedIDScanner is the relationship mirror of nodeOrderedIDScanner.
type relOrderedIDScanner interface {
	ForEachRelIDByTypePropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(types.RelID, any) bool) error
	ForEachRelIDByTypePropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(types.RelID, any) bool) error
}

// ForEachIDByLabelPropertyRangeOrdered streams (ID, value) of the label's
// nodes whose numeric propKey value lies within [min, max] (inclusivity per
// the flags, applied EXACTLY: no re-check needed) in value order, ascending
// or descending when desc, ties by node ID ascending in both directions, from
// the property index alone: no node row is read or decoded. The value is the
// one stored, in its stored type (an int64 3 and a float64 3.0 share a
// position in the order and keep their types); a float32 sorts by its
// shortest decimal rendering. fn returning false stops; it runs with no lock
// held (pages of 256 entries are read under the store's read lock, so the
// walk is not one snapshot: an entry moved by a concurrent write may be seen
// at its old or new position, or twice, as with the ordered row door). The
// context is checked before the walk and every 4,096 entries.
//
// Unsupported cases are reported, never answered by a slower walk:
// ErrIndexNotFound for a temporal QueryOpts (the index holds current values;
// use ForEachByLabelPropertyRangeOrdered), a store without the capability
// (tiered, sharded, external stores), badger with PropertyIndexOnDisk, no
// property index on (label, propKey), an Elements index, an index being
// built, or an index holding an integer of magnitude past 2^53. An unknown
// label walks nothing.
func (n *NodeOps) ForEachIDByLabelPropertyRangeOrdered(ctx context.Context, label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(types.NodeID, any) bool) error {
	return n.forEachIDOrdered(ctx, label, opts, fn, func(s nodeOrderedIDScanner, tok uint16, emit func(types.NodeID, any) bool) error {
		return s.ForEachNodeIDByLabelPropertyRangeOrdered(tok, propKey, min, max, inclMin, inclMax, desc, emit)
	})
}

// ForEachIDByLabelPropertyPrefix is the string twin of
// ForEachIDByLabelPropertyRangeOrdered: (ID, value) of the label's nodes whose
// string value begins with prefix (empty: every string value), in byte order
// (descending when desc), ties by ID ascending, from the index alone. Same
// isolation, context and unsupported cases.
func (n *NodeOps) ForEachIDByLabelPropertyPrefix(ctx context.Context, label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(types.NodeID, any) bool) error {
	return n.forEachIDOrdered(ctx, label, opts, fn, func(s nodeOrderedIDScanner, tok uint16, emit func(types.NodeID, any) bool) error {
		return s.ForEachNodeIDByLabelPropertyPrefix(tok, propKey, prefix, desc, emit)
	})
}

func (n *NodeOps) forEachIDOrdered(ctx context.Context, label string, opts storepkg.QueryOpts, fn func(types.NodeID, any) bool,
	walk func(nodeOrderedIDScanner, uint16, func(types.NodeID, any) bool) error) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if temporalOpts(opts) {
		return storepkg.ErrIndexNotFound
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner, native := c.store.(nodeOrderedIDScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return walkWithCtx(ctx, fn, func(emit func(types.NodeID, any) bool) error { return walk(scanner, tok, emit) })
}

// ForEachIDByTypePropertyRangeOrdered is the relationship mirror of
// NodeOps.ForEachIDByLabelPropertyRangeOrdered over a type's property index.
func (r *RelOps) ForEachIDByTypePropertyRangeOrdered(ctx context.Context, typeName, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(types.RelID, any) bool) error {
	return r.forEachIDOrdered(ctx, typeName, opts, fn, func(s relOrderedIDScanner, tok uint16, emit func(types.RelID, any) bool) error {
		return s.ForEachRelIDByTypePropertyRangeOrdered(tok, propKey, min, max, inclMin, inclMax, desc, emit)
	})
}

// ForEachIDByTypePropertyPrefix is the relationship mirror of
// NodeOps.ForEachIDByLabelPropertyPrefix.
func (r *RelOps) ForEachIDByTypePropertyPrefix(ctx context.Context, typeName, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(types.RelID, any) bool) error {
	return r.forEachIDOrdered(ctx, typeName, opts, fn, func(s relOrderedIDScanner, tok uint16, emit func(types.RelID, any) bool) error {
		return s.ForEachRelIDByTypePropertyPrefix(tok, propKey, prefix, desc, emit)
	})
}

func (r *RelOps) forEachIDOrdered(ctx context.Context, typeName string, opts storepkg.QueryOpts, fn func(types.RelID, any) bool,
	walk func(relOrderedIDScanner, uint16, func(types.RelID, any) bool) error) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	if temporalOpts(opts) {
		return storepkg.ErrIndexNotFound
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner, native := c.store.(relOrderedIDScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return walkWithCtx(ctx, fn, func(emit func(types.RelID, any) bool) error { return walk(scanner, tok, emit) })
}

// walkWithCtx runs walk with fn wrapped to check ctx every
// orderedIDCtxEvery entries; a canceled context stops the walk and is the
// error.
func walkWithCtx[ID any](ctx context.Context, fn func(ID, any) bool, walk func(func(ID, any) bool) error) error {
	var ctxErr error
	seen := 0
	err := walk(func(id ID, v any) bool {
		seen++
		if seen%orderedIDCtxEvery == 0 {
			if ctxErr = checkCtx(ctx); ctxErr != nil {
				return false
			}
		}
		return fn(id, v)
	})
	if err != nil {
		return err
	}
	return ctxErr
}
