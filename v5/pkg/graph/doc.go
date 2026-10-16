// Package graph is the native public API of rho-tkg v5 (gap rows 1-15, 21-22).
//
// This is an API skeleton: types, request validation, godoc and sentinels
// compile without an engine behind them. Open validates its Config and then
// returns ErrNotImplemented; every other door validates its arguments first and
// returns ErrNotImplemented when no engine is attached. Names are drafts for
// API review (PLAN:370).
//
// Rules the types enforce:
//
//   - Every write op carries an explicit temporal.Scope; there is no implicit
//     "now" and no default scope. An empty or zero Scope is refused.
//   - Every read names its view (Declared or Effective), its valid-time scope and
//     its knowledge scope. A snapshot mode never implies a valid coordinate.
//   - Zero is a coordinate, not a sentinel. Infinity is temporal.Bound
//     (NegativeInfinity / PositiveInfinity), never MinInt64 or 0.
//   - IDs are types.NodeID / types.RelID (1..MaxInt64); zero and negative are
//     refused with ErrInvalidID.
//   - Paging uses opaque Cursor values; a zero Cursor is invalid. Pages carry a
//     row limit (at most MaxPageRows) and a mandatory byte limit.
//   - Receipt and Cut are opaque; only the engine mints them.
package graph
