package temporal

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Pin is the read coordinate of the paged effective doors
// (API.RelsEffectivePage, API.NodesEffectivePage): the transaction-time pin Tx
// (required, > 0) and at most one valid-time filter — a point ValidAt, or the
// half-open interval [ValidStart, ValidEnd) with 0 < ValidStart < ValidEnd.
// All valid fields zero: no valid-time filter (every segment).
type Pin struct {
	Tx                   types.Instant
	ValidAt              types.Instant
	ValidStart, ValidEnd types.Instant
}

// Filter is the property equality of the paged effective doors: a segment
// matches when its row carries property Prop with a value whose canonical
// index key equals Eq's (the equality of the property indexes,
// types.IndexablePropertyValueKey: list membership does not count). Eq must
// be a valid property value; one without an index key matches nothing.
type Filter struct {
	Prop string
	Eq   any
}

// RelsEffectivePage returns one keyset page of the effective timelines at
// pin.Tx of the relationships, of every type, with a segment whose row carries
// f.Prop = f.Eq and that meets pin's valid-time filter: the first limit such
// relationships with ID > after, ascending by ID, each with exactly those
// segments (ascending). For every page it equals ForEachRelEffective(pin.Tx)
// keeping those segments, grouped by relationship, ordered by ID and cut after
// limit relationships. next is the ID of the page's last relationship when the
// page holds limit of them (the following page may be empty), else 0 (no
// more). A pinned page is stable while writers commit above the pin, so a
// cursor neither repeats nor skips a relationship across pages.
// DECLARED view: not masked by endpoint validity.
//
// Cost: where a (type, f.Prop) relationship property index is declared, the
// candidates of that type come in ID order from the index's transaction-time
// membership sidecar (every relationship whose rows ever carried the value),
// resumed after the cursor, so a page reads about limit timelines plus the
// candidates that do not match at the pin; the types are merged in ID order.
// A type without the index contributes its ever-members at the pin, gathered
// and sorted on every page (the cost of ForEachRelEffectiveByType). Memory,
// badger and sharded keep the ordered sidecar; tiered keeps no relationship
// property index, so every type takes the second path.
//
// Errors: ForEachRelEffective's gates for pin.Tx (ErrInvalidTimeRange,
// ErrTxPinTooNew, ErrHistoryCompacted, ErrRetentionExpired),
// ErrInvalidTimeRange for a negative or conflicting valid filter, an invalid
// property key or value, ErrInvalidQueryLimit (limit < 1),
// ErrInvalidQueryCursor (after < 0), ctx's error, ErrGraphClosed.
func (a *API) RelsEffectivePage(ctx context.Context, pin Pin, f Filter, after types.RelID, limit int) (segs []RelSegment, next types.RelID, err error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, err
	}
	return ops.RelsEffectivePage(ctx, pin, f, after, limit)
}

// NodesEffectivePage is RelsEffectivePage for nodes, over every label: the
// oracle is ForEachNodeEffective, an index is a (label, f.Prop) node property
// index, and a label without one contributes its ever-members at the pin.
// Tiered serves a label from its node index only where the store's sidecar
// answers (reference-class labels).
func (a *API) NodesEffectivePage(ctx context.Context, pin Pin, f Filter, after types.NodeID, limit int) (segs []NodeSegment, next types.NodeID, err error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, err
	}
	return ops.NodesEffectivePage(ctx, pin, f, after, limit)
}
