package temporal

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// MaxPageLimit is the largest limit RelsEffectivePage / NodesEffectivePage
// accept (ErrInvalidQueryLimit above it).
const MaxPageLimit = 10_000

// Pin is the read coordinate of the paged effective doors
// (API.RelsEffectivePage, API.NodesEffectivePage): the transaction-time pin Tx
// (required, > 0) and at most one valid-time filter — a point ValidAt, or the
// half-open interval [ValidStart, ValidEnd) with ValidStart < ValidEnd,
// or the open-ended [ValidStart, inf) with ValidEnd = 0 (unlike QueryOpts,
// where a lone ValidStart is no filter). 0 is the only unset value: a
// negative instant, types.UnboundedValidFrom included, is a real bound (since
// v4.58.1). All valid fields zero: no valid-time filter (every segment).
// ValidEnd without ValidStart is ErrInvalidTimeRange.
//
// Explicit presence (v4.59.0, as store.QueryOpts): ValidAtSet makes ValidAt
// a point filter at any instant (0 included); ValidWindowSet makes
// [ValidStart, ValidEnd) the window as given, either bound may be 0 (no
// open-ended reading: an open end is math.MaxInt64). Point and window stay
// exclusive. Tx has no flag: it is required and > 0.
type Pin struct {
	Tx                   types.Instant
	ValidAt              types.Instant
	ValidStart, ValidEnd types.Instant
	ValidAtSet           bool
	ValidWindowSet       bool
}

// Filter is the property equality of the paged effective doors: a segment
// matches when its row carries property Prop with a value equal to Eq under
// the property equality of every equality door (types.EqualIndexValues: 3
// matches 3.0 and int8(3), NaN matches nothing; list membership does not
// count; since v4.57.1). Eq must be a scalar with an index key: a list, map
// or nil is types.ErrUnsupportedValueType.
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
// A type without the index contributes its ever-members at the pin: they are
// gathered and sorted on every page, and the timeline of each one above the
// cursor is read until the page fills, so a page costs up to one effective
// walk of that type (a type's current property-key counts cannot rule it
// out: history rows may carry the property). Declare the (type, f.Prop) index
// on every type that carries the property. Memory, badger and sharded keep
// the ordered sidecar; tiered keeps no relationship property index, so every
// type takes the second path there.
//
// With a valid filter (ValidAt or an interval), a type with a valid-time
// envelope index (Index().CreateRelTemporal) drops, on either path, each
// candidate whose envelope (earliest valid-from to latest valid-to over all
// its rows) cannot meet the filter before its timeline is read, so a page at
// ValidAt reads only the candidates whose envelope covers it. A type without
// the envelope index reads every candidate and filters its segments. Declare
// CreateRelTemporal on the types paged with a valid filter (all four backends
// prune; the index costs RAM per relationship and is rebuilt at open).
//
// Errors: ForEachRelEffective's gates for pin.Tx (ErrInvalidTimeRange,
// ErrTxPinTooNew, ErrHistoryCompacted, ErrRetentionExpired),
// ErrInvalidTimeRange for a conflicting or reversed valid filter, an invalid
// property key or value, ErrInvalidQueryLimit (limit < 1 or above
// MaxPageLimit),
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
// index, and a label without one contributes its ever-members at the pin
// (the same per-page cost; declare the index on every label that carries the
// property). Tiered offers no paged sidecar, so every label gathers there. A
// label's envelope index (Index().CreateTemporal) prunes a valid-filtered page
// as CreateRelTemporal does; a node listed by several labels is read when one
// of them keeps it. Tiered keeps no node envelope prune.
func (a *API) NodesEffectivePage(ctx context.Context, pin Pin, f Filter, after types.NodeID, limit int) (segs []NodeSegment, next types.NodeID, err error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, err
	}
	return ops.NodesEffectivePage(ctx, pin, f, after, limit)
}

// EffectivePageOpts pages ForEachNodeEffectiveByLabelAndProperties: the
// matches above After (ascending node ID), at most Limit of them (0: all),
// listed from the shards Depth selects (store.QueryOpts.Depth).
type EffectivePageOpts struct {
	After types.NodeID
	Limit int
	Depth storepkg.ShardDepth
}

// ForEachNodeEffectiveByLabelAndProperties pages Nodes().ByLabelAndProperties
// (label, props: 2..4 keys, equality on every pair) by node ID and hands each
// match's effective segments at pin to fn: fn(id, segs) with segs exactly
// NodeEffectiveTimelines(id, []Instant{pin})[0] (TxAt = pin, no valid-time
// filter; empty when the node has no segment at the pin, and fn is still
// called: the page counts matches, not segments). Row 121 (sigma R37).
//
// Paging: the matches above opts.After in ascending ID order, at most
// opts.Limit (0: all). next is the last match's ID when the page holds Limit
// matches, the ID of the match whose fn returned false (which stops the
// call), else 0. A match is judged on the node's current row at the moment
// its timeline is read, so a page never lists a node whose current row no
// longer carries the values.
//
// Rows: segment rows are the store's frozen shared rows (lent), valid while
// the graph is open; read them (GetProperty, ForEachPropertyTypeClass,
// TxStamps), never mutate them. fn runs without graph locks.
//
// Cost: one call per page. With a composite index declared on (label, the
// keys of props) (Index().CreateComposite), the candidates are the index's
// posting list, read without decoding a row; otherwise the page lists through
// ByLabelAndProperties. Each match is then read once: its current row (which
// decides the match), its history skeleton and its winning history row only.
// On badger, sharded and tiered, a chunk of up to 256 candidates reads
// through one read snapshot (one transaction and forward iterators, tiered
// for its reference shard), not one transaction per row.
//
// Errors: as ByLabelAndProperties for label, props and opts.Depth;
// NodeEffectiveTimelines' pin gates (ErrTxPinTooNew, ErrHistoryCompacted,
// ErrRetentionExpired); ErrInvalidQueryLimit (Limit < 0),
// ErrInvalidQueryCursor (After < 0), ErrNilCallback (nil fn),
// ErrNilContext, ctx's error (checked between nodes), ErrGraphClosed. An
// unknown label answers no match (next 0).
func (a *API) ForEachNodeEffectiveByLabelAndProperties(ctx context.Context, label string, props map[string]any, pin types.Instant, opts EffectivePageOpts, fn func(id types.NodeID, segs []NodeSegment) bool) (next types.NodeID, err error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.ForEachNodeEffectiveByLabelAndProperties(ctx, label, props, pin, opts, fn)
}
