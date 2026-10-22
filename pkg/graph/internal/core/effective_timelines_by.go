package core

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Effective timelines by property tuples (row 105, sigma O1-3)
//
// RelEffectiveTimelinesByProperties answers many probe tuples (keys[j] =
// tuple[j] for every j) of one relationship type at one pin in one call:
//   - driver: of the keys with a declared (type, key) property index, the one
//     whose tuple values have the fewest current members (the store's O(1)
//     equality count, relEqualCounter); a store without counts keeps the
//     keys order;
//   - candidates: ONE sidecar enumeration of the driver key over the equal
//     keys of every tuple's driver value (forEachRelPropTxMemberOfKeys: one
//     fan-out on tiered and sharded), windowed when the pin has a valid
//     filter, members first recorded after the pin pruned, then the type's
//     envelope prune (pruneRelTypeCandidates);
//   - rows: each candidate's timeline is read once (relEffectiveTimelineFrom,
//     the per-entity door's resolution), each segment kept when it meets the
//     valid filter, and routed through the driver value its row carries to
//     the tuples whose other values the row carries too.
// No usable driver is ErrIndexNotFound: the door never folds the type.
// =============================================================================

// maxTupleKeys bounds the keys of one probe (as a composite index).
const maxTupleKeys = 4

// RelEffectiveTimelinesByProperties implements
// temporal.API.RelEffectiveTimelinesByProperties.
func (t *TempOps) RelEffectiveTimelinesByProperties(ctx context.Context, typeName string, keys []string, tuples [][]any, pin temporal.Pin) ([][]temporal.RelSegment, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return nil, err
	}
	if err := c.validateTupleKeys(keys); err != nil {
		return nil, err
	}
	tupleKeys, err := tupleEqualKeys(keys, tuples)
	if err != nil {
		return nil, err
	}
	pin.Tx = recordedPin(pin.Tx) // a pin <= 0: nothing recorded yet (round 11)
	opts, err := pageQueryOpts(pin)
	if err != nil {
		return nil, err
	}
	if err := c.validateEffectiveScanPin(pin.Tx, c.relTypeRetentionScope(typeName)); err != nil {
		return nil, err
	}
	out := make([][]temporal.RelSegment, len(tuples))
	if len(tuples) == 0 {
		return out, nil
	}
	var (
		tok    uint16
		known  bool
		driver int
		ids    []types.RelID
	)
	if err := c.readUnderRLock(func() error {
		if tok, known = c.lookupRelTypeQueryToken(typeName); !known {
			return nil
		}
		var err error
		driver, ids, err = c.relTupleCandidates(ctx, tok, keys, tuples, tupleKeys, opts)
		return err
	}); err != nil {
		return nil, err
	}
	if !known {
		return out, nil
	}
	route := make(map[string][]int) // driver value key -> tuples
	for i, tk := range tupleKeys {
		for _, vk := range tk[driver] {
			route[vk] = append(route[vk], i)
		}
	}
	validOK := pageValidKeep(c.axis(), opts)
	var hit []int
	err = streamEntityTimelines(c, ids, func(rd entityReads, id types.RelID) ([]temporal.RelSegment, error) {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		segs, err := c.relEffectiveTimelineFrom(rd, id, pin.Tx)
		return skipUnknown(segs, err, storepkg.ErrRelNotFound)
	}, func(_ types.RelID, segs []temporal.RelSegment) bool {
		for _, s := range segs {
			if !validOK(s.ValidFrom, s.ValidTo) || !s.Rel.HasTypeTokenRaw(tok) {
				continue
			}
			hit = hit[:0]
			s.Rel.ForEachPropertyIndexKey(keys[driver], false, func(vk string) bool {
				hit = append(hit, route[vk]...)
				return true
			})
			for _, i := range firstOccurrences(hit) {
				if rowCarriesTuple(s.Rel, keys, tupleKeys[i], driver) {
					out[i] = append(out[i], s)
				}
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// validateTupleKeys checks a probe's keys: 1..maxTupleKeys distinct, each a
// valid index property key.
func (c *Core) validateTupleKeys(keys []string) error {
	if len(keys) < 1 || len(keys) > maxTupleKeys {
		return fmt.Errorf("%w: property tuple requires 1-%d keys, got %d", storepkg.ErrInvalidStoreMutation, maxTupleKeys, len(keys))
	}
	for i, k := range keys {
		if k == "" {
			return fmt.Errorf("%w: property tuple key %d", ErrEmptyName, i)
		}
		if err := c.validateIndexPropertyKey(k); err != nil {
			return err
		}
		if slices.Contains(keys[:i], k) {
			return fmt.Errorf("%w: duplicate property tuple key %q", storepkg.ErrInvalidStoreMutation, k)
		}
	}
	return nil
}

// tupleEqualKeys validates every tuple (len(keys) scalar values with an index
// key) and returns each value's equal keys (types.EqualIndexKeys; none for
// NaN, which matches nothing).
func tupleEqualKeys(keys []string, tuples [][]any) ([][]equalKeys, error) {
	out := make([][]equalKeys, len(tuples))
	for i, tu := range tuples {
		if len(tu) != len(keys) {
			return nil, fmt.Errorf("%w: property tuple %d holds %d values for %d keys", storepkg.ErrInvalidStoreMutation, i, len(tu), len(keys))
		}
		out[i] = make([]equalKeys, len(tu))
		for j, v := range tu {
			if err := types.ValidatePropertyValue(v); err != nil {
				return nil, fmt.Errorf("graph: property tuple %d value %q: %w", i, keys[j], err)
			}
			if types.IndexablePropertyValueKey(v) == "" {
				return nil, fmt.Errorf("graph: property tuple %d value %q (%T) has no index key (a scalar is required): %w", i, keys[j], v, types.ErrUnsupportedValueType)
			}
			out[i][j] = types.EqualIndexKeys(v)
		}
	}
	return out, nil
}

// rowCarriesTuple reports whether r carries every value of a tuple (its equal
// keys per key) other than the driver's, which routing already matched.
func rowCarriesTuple(r *types.Relationship, keys []string, tk []equalKeys, driver int) bool {
	for j, k := range keys {
		if j != driver && !tk[j].rel(r, k, false) {
			return false
		}
	}
	return true
}

// relTupleCandidates returns the driver key's index and the ascending
// candidates of every tuple (c.mu held): the keys are tried in driver order
// (relTupleDriverOrder) until one's sidecar answers; an Elements index never
// drives (its members are list elements). None answers: ErrIndexNotFound.
func (c *Core) relTupleCandidates(ctx context.Context, tok uint16, keys []string, tuples [][]any, tupleKeys [][]equalKeys, opts storepkg.QueryOpts) (int, []types.RelID, error) {
	if c.relPropTxMembers == nil {
		return 0, nil, fmt.Errorf("%w: no relationship property membership sidecar on this store", storepkg.ErrIndexNotFound)
	}
	order, err := c.relTupleDriverOrder(tok, keys, tuples)
	if err != nil {
		return 0, nil, err
	}
	for _, d := range order {
		if err := checkCtx(ctx); err != nil {
			return 0, nil, err
		}
		if elements, err := c.relIndexElements(tok, keys[d]); err != nil {
			return 0, nil, err
		} else if elements {
			continue
		}
		var vks []string
		for _, tk := range tupleKeys {
			vks = append(vks, tk[d]...)
		}
		ids, err := c.relTupleMembers(tok, keys[d], firstOccurrences(vks), opts)
		if errors.Is(err, storepkg.ErrIndexNotFound) {
			continue
		}
		if err != nil {
			return 0, nil, err
		}
		return d, c.pruneRelTypeCandidates(tok, ascending(ids), opts), nil
	}
	return 0, nil, fmt.Errorf("%w: no relationship property index on any of %q", storepkg.ErrIndexNotFound, keys)
}

// relTupleDriverOrder orders the key indexes by the summed current member
// counts of their tuple values, fewest first (c.mu held); keys whose count the
// store declines (no index, no capability, an index being built) follow in
// keys order.
func (c *Core) relTupleDriverOrder(tok uint16, keys []string, tuples [][]any) ([]int, error) {
	type ranked struct {
		i     int
		count int64
		exact bool
	}
	rs := make([]ranked, len(keys))
	counter, native := c.store.(relEqualCounter)
	for j, k := range keys {
		rs[j] = ranked{i: j}
		if !native {
			continue
		}
		rs[j].exact = true
		seen := make(map[string]struct{}, len(tuples))
		for _, tu := range tuples {
			vk := types.IndexablePropertyValueKey(tu[j])
			if _, dup := seen[vk]; dup {
				continue
			}
			seen[vk] = struct{}{}
			n, exact, err := counter.RelEqualCount(tok, k, tu[j])
			if err != nil {
				return nil, err
			}
			if !exact {
				rs[j].exact = false
				break
			}
			rs[j].count += n
		}
	}
	slices.SortStableFunc(rs, func(a, b ranked) int {
		switch {
		case a.exact != b.exact:
			if a.exact {
				return -1
			}
			return 1
		case a.exact:
			return cmp.Compare(a.count, b.count)
		}
		return 0
	})
	order := make([]int, len(rs))
	for i, r := range rs {
		order[i] = r.i
	}
	return order, nil
}

// relTupleMembers enumerates the (tok, key) sidecar over valueKeys in one
// call (c.mu held): windowed when opts has a valid filter and the store has
// the windowed form (a declining windowed form falls back to the plain one,
// as forEachRelPropertyCandidateID), members first recorded after the pin
// dropped. store.ErrIndexNotFound when the index does not answer.
func (c *Core) relTupleMembers(tok uint16, key string, valueKeys []string, opts storepkg.QueryOpts) ([]types.RelID, error) {
	pinTx := txPinOf(opts)
	start, end, windowed := validWindow(opts)
	windowed = windowed && c.relPropTxWindow != nil
	for {
		var (
			ids     []types.RelID
			invalid error
		)
		member := func(id types.RelID, firstTx types.Instant) bool {
			if !c.storeRowsTrust {
				if invalid = storepkg.ValidateRelID(id); invalid != nil {
					return false
				}
			}
			if firstTx == 0 || firstTx <= pinTx {
				ids = append(ids, id)
			}
			return true
		}
		var window *memberWindow
		if windowed {
			window = &memberWindow{start, end, 0}
		}
		err := c.forEachRelPropTxMemberOfKeys(tok, key, valueKeys, window, member)
		if invalid != nil {
			return nil, invalid
		}
		if errors.Is(err, storepkg.ErrIndexNotFound) && windowed {
			windowed = false
			continue
		}
		if err != nil {
			return nil, err
		}
		return firstOccurrences(ids), nil
	}
}
