package core

import (
	"context"
	"errors"
	"fmt"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Paged effective doors (backlog 69, requested by ai-soc 2026-10-14 for
// GET /api/v1/tkg/asof): RelsEffectivePage / NodesEffectivePage.
//
// An effective scan (effective_scans.go) whose candidates come in ascending ID
// order above a keyset cursor, one source per relationship type (node label):
//   - a declared (type, key) property index: the value's members from the
//     store's transaction-time membership sidecar, paged in ID order through
//     its ordered view (store.RelPropertyTxMemberPageCapability), members
//     first recorded after the pin pruned — the candidates of the temporal
//     ByTypeAndProperty (forEachRelPropertyCandidateID), resumed at the cursor;
//   - otherwise the type's candidates at the pin (relTypeCandidateIDsAt, the
//     ForEachRelEffectiveByType gather), above the cursor.
// The sources are merged in ID order (k-way, a candidate in several sources
// once). Every row carrying the value is a row of its own type, so the union
// covers every entity the full walk keeps. Each candidate's timeline is the
// per-entity door's (streamRelTimelines); the page keeps the segments whose
// row carries the value and that meet the valid filter, and ends after limit
// entities with a kept segment.
// =============================================================================

// MaxEffectivePageLimit is the largest limit the paged effective doors accept
// (temporal.MaxPageLimit); a larger one is ErrInvalidQueryLimit.
const MaxEffectivePageLimit = temporal.MaxPageLimit

// pageChunkMin / pageChunkMax bound one sidecar fetch.
const (
	pageChunkMin = 64
	pageChunkMax = 4096
)

// pageSource is one type's (label's) candidate stream above the cursor, in
// ascending ID order.
type pageSource[ID ~int64] struct {
	buf   []ID
	seen  ID   // the highest ID the store returned (the resume point)
	done  bool // the store has nothing above seen
	fetch func(after ID, n int) (ids []ID, last ID, done bool, err error)
}

// fill refills an empty buffer (c.mu held).
func (s *pageSource[ID]) fill(n int) error {
	for len(s.buf) == 0 && !s.done {
		ids, last, done, err := s.fetch(s.seen, n)
		if err != nil {
			return err
		}
		s.buf, s.done = ids, done
		if last > s.seen {
			s.seen = last
		}
	}
	return nil
}

// mergePage takes up to n candidates in ascending ID order from srcs, each ID
// once (c.mu held). It returns fewer only when every source is exhausted.
func mergePage[ID ~int64](srcs []*pageSource[ID], n int) ([]ID, error) {
	chunk := min(max(n, pageChunkMin), pageChunkMax)
	out := make([]ID, 0, min(n, chunk))
	for len(out) < n {
		best := -1
		for i, s := range srcs {
			if err := s.fill(chunk); err != nil {
				return nil, err
			}
			if len(s.buf) > 0 && (best < 0 || s.buf[0] < srcs[best].buf[0]) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		id := srcs[best].buf[0]
		for _, s := range srcs {
			if len(s.buf) > 0 && s.buf[0] == id {
				s.buf = s.buf[1:]
			}
		}
		out = append(out, id)
	}
	return out, nil
}

// above returns the IDs of the ascending ids greater than after.
func above[ID ~int64](ids []ID, after ID) []ID {
	i, found := slices.BinarySearch(ids, after)
	if found {
		i++
	}
	return ids[i:]
}

// sidecarSource is a source from a paged membership sidecar: page fetches at
// most n members above after; members first recorded after pin are dropped
// (they cannot carry the value at the pin) but still advance the resume point.
func sidecarSource[ID ~int64](pin types.Instant, after ID, trust bool, validate func(ID) error,
	page func(after ID, n int, fn func(ID, types.Instant) bool) error) *pageSource[ID] {
	return &pageSource[ID]{seen: after, fetch: func(after ID, n int) ([]ID, ID, bool, error) {
		var (
			ids     []ID
			last    ID
			got     int
			invalid error
		)
		err := page(after, n, func(id ID, firstTx types.Instant) bool {
			if !trust {
				if invalid = validate(id); invalid != nil {
					return false
				}
				if id <= after || id <= last {
					invalid = fmt.Errorf("%w: membership page out of order (%d after %d)", storepkg.ErrInvalidStoreMutation, id, max(after, last))
					return false
				}
			}
			got++
			last = id
			if firstTx == 0 || firstTx <= pin {
				ids = append(ids, id)
			}
			return true
		})
		if invalid != nil {
			return nil, 0, true, invalid
		}
		return ids, last, err == nil && got < n, err
	}}
}

// listSource is a source over a whole ascending candidate list.
func listSource[ID ~int64](ids []ID, after ID) *pageSource[ID] {
	return &pageSource[ID]{buf: above(ids, after), seen: after, done: true}
}

// pageValidKeep returns the segment predicate of pin's valid-time filter.
func pageValidKeep(pin temporal.Pin) func(from, to types.Instant) bool {
	switch {
	case pin.ValidAt != 0:
		at := pin.ValidAt
		return func(from, to types.Instant) bool { return from <= at && (to == 0 || to > at) }
	case pin.ValidStart != 0:
		s, e := pin.ValidStart, pin.ValidEnd // e == 0: open-ended [s, inf)
		return func(from, to types.Instant) bool { return (e == 0 || from < e) && (to == 0 || to > s) }
	}
	return func(types.Instant, types.Instant) bool { return true }
}

// validatePage checks the arguments both paged doors share and returns the
// value's index key.
func (c *Core) validatePage(ctx context.Context, pin temporal.Pin, f temporal.Filter, after int64, limit int, scope retentionScope) (string, error) {
	if err := c.checkOpen(); err != nil {
		return "", err
	}
	if err := checkCtx(ctx); err != nil {
		return "", err
	}
	if limit < 1 || limit > MaxEffectivePageLimit {
		return "", fmt.Errorf("%w: %d (want 1..%d)", ErrInvalidQueryLimit, limit, MaxEffectivePageLimit)
	}
	if after < 0 {
		return "", fmt.Errorf("%w: %d", ErrInvalidQueryCursor, after)
	}
	interval := pin.ValidStart != 0 || pin.ValidEnd != 0
	switch {
	case pin.ValidAt < 0 || pin.ValidStart < 0 || pin.ValidEnd < 0:
		return "", fmt.Errorf("%w: negative valid-time filter", ErrInvalidTimeRange)
	case pin.ValidAt != 0 && interval:
		return "", fmt.Errorf("%w: ValidAt and [ValidStart, ValidEnd) are exclusive", ErrInvalidTimeRange)
	case interval && (pin.ValidStart == 0 || pin.ValidEnd != 0 && pin.ValidStart >= pin.ValidEnd):
		return "", fmt.Errorf("%w: valid interval [%d, %d)", ErrInvalidTimeRange, pin.ValidStart, pin.ValidEnd)
	}
	if f.Prop == "" {
		return "", fmt.Errorf("%w: filter property", ErrEmptyName)
	}
	if err := c.validateIndexPropertyKey(f.Prop); err != nil {
		return "", err
	}
	if err := types.ValidatePropertyValue(f.Eq); err != nil {
		return "", fmt.Errorf("graph: effective page filter value: %w", err)
	}
	vk := types.IndexablePropertyValueKey(f.Eq)
	if vk == "" {
		return "", fmt.Errorf("graph: effective page filter value %T has no index key (a scalar is required): %w", f.Eq, types.ErrUnsupportedValueType)
	}
	if err := c.validateEffectiveScanPin(pin.Tx, scope); err != nil {
		return "", err
	}
	return vk, nil
}

// RelsEffectivePage implements temporal.API.RelsEffectivePage.
func (t *TempOps) RelsEffectivePage(ctx context.Context, pin temporal.Pin, f temporal.Filter, after types.RelID, limit int) ([]temporal.RelSegment, types.RelID, error) {
	c := t.c
	vk, err := c.validatePage(ctx, pin, f, int64(after), limit, scopeAllRels)
	if err != nil {
		return nil, 0, err
	}
	var srcs []*pageSource[types.RelID]
	if err := c.readUnderRLock(func() error {
		n := c.relTypes.Len()
		for i := 1; i <= n; i++ {
			tok := uint16(i) // #nosec G115 -- bounded by the registry's uint16 token space
			src, err := c.relPageSource(tok, f.Prop, vk, pin.Tx, after)
			if err != nil {
				return err
			}
			srcs = append(srcs, src)
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	validOK := pageValidKeep(pin)
	keepRow := func(r *types.Relationship) bool { return r.HasPropertyIndexKey(f.Prop, false, vk) }
	var (
		out      []temporal.RelSegment
		entities int
		last     types.RelID
	)
	for entities < limit {
		if err := checkCtx(ctx); err != nil {
			return nil, 0, err
		}
		var batch []types.RelID
		if err := c.readUnderRLock(func() error {
			var err error
			batch, err = mergePage(srcs, limit-entities)
			return err
		}); err != nil {
			return nil, 0, err
		}
		if len(batch) == 0 {
			return out, 0, nil
		}
		if err := c.streamRelTimelines(batch, pin.Tx, keepRow, func(s temporal.RelSegment) bool {
			if validOK(s.ValidFrom, s.ValidTo) {
				if id := s.Rel.ID(); id != last {
					last = id
					entities++
				}
				out = append(out, s)
			}
			return true
		}); err != nil {
			return nil, 0, err
		}
	}
	return out, last, nil
}

// relPageSource returns tok's candidate source (c.mu held).
func (c *Core) relPageSource(tok uint16, key, vk string, pin types.Instant, after types.RelID) (*pageSource[types.RelID], error) {
	if c.relPropTxPage != nil {
		src := sidecarSource(pin, after, c.storeRowsTrust, storepkg.ValidateRelID,
			func(after types.RelID, n int, fn func(types.RelID, types.Instant) bool) error {
				return c.relPropTxPage.ForEachRelPropertyTxMemberAfter(tok, key, vk, after, n, fn)
			})
		// The first fetch tells an indexed type from one without the index.
		err := src.fill(pageChunkMin)
		if err == nil {
			return src, nil
		}
		if !errors.Is(err, storepkg.ErrIndexNotFound) {
			return nil, err
		}
	}
	ids, err := c.relTypeCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		return nil, err
	}
	return listSource(ascending(ids), after), nil
}

// NodesEffectivePage implements temporal.API.NodesEffectivePage.
func (t *TempOps) NodesEffectivePage(ctx context.Context, pin temporal.Pin, f temporal.Filter, after types.NodeID, limit int) ([]temporal.NodeSegment, types.NodeID, error) {
	c := t.c
	vk, err := c.validatePage(ctx, pin, f, int64(after), limit, scopeNodes)
	if err != nil {
		return nil, 0, err
	}
	var srcs []*pageSource[types.NodeID]
	if err := c.readUnderRLock(func() error {
		n := c.labels.Len()
		for i := 1; i <= n; i++ {
			tok := uint16(i) // #nosec G115 -- bounded by the registry's uint16 token space
			src, err := c.nodePageSource(tok, f.Prop, vk, pin.Tx, after)
			if err != nil {
				return err
			}
			srcs = append(srcs, src)
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	validOK := pageValidKeep(pin)
	keepRow := func(n *types.Node) bool { return n.HasPropertyIndexKey(f.Prop, false, vk) }
	var (
		out      []temporal.NodeSegment
		entities int
		last     types.NodeID
	)
	for entities < limit {
		if err := checkCtx(ctx); err != nil {
			return nil, 0, err
		}
		var batch []types.NodeID
		if err := c.readUnderRLock(func() error {
			var err error
			batch, err = mergePage(srcs, limit-entities)
			return err
		}); err != nil {
			return nil, 0, err
		}
		if len(batch) == 0 {
			return out, 0, nil
		}
		if err := c.streamNodeTimelines(batch, pin.Tx, keepRow, func(s temporal.NodeSegment) bool {
			if validOK(s.ValidFrom, s.ValidTo) {
				if id := s.Node.ID(); id != last {
					last = id
					entities++
				}
				out = append(out, s)
			}
			return true
		}); err != nil {
			return nil, 0, err
		}
	}
	return out, last, nil
}

// nodePageSource returns label tok's candidate source (c.mu held).
func (c *Core) nodePageSource(tok uint16, key, vk string, pin types.Instant, after types.NodeID) (*pageSource[types.NodeID], error) {
	if c.nodePropTxPage != nil {
		src := sidecarSource(pin, after, c.storeRowsTrust, storepkg.ValidateNodeID,
			func(after types.NodeID, n int, fn func(types.NodeID, types.Instant) bool) error {
				return c.nodePropTxPage.ForEachNodePropertyTxMemberAfter(tok, key, vk, after, n, fn)
			})
		err := src.fill(pageChunkMin)
		if err == nil {
			return src, nil
		}
		if !errors.Is(err, storepkg.ErrIndexNotFound) {
			return nil, err
		}
	}
	ids, err := c.nodeLabelCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		return nil, err
	}
	return listSource(ascending(ids), after), nil
}
