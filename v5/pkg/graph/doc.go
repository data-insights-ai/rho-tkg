// Package graph is the native public API of rho-tkg v5 (gap rows 1-15, 21-22).
//
// One partition is bound over the graphstore and raftlog seams: Open, Update,
// OpenSnapshot (Fresh, Stable, At, After), Lookup, Timeline and Cut work; Scan
// (label and equality pages) is ErrNotImplemented until the engine has those
// index pages. Every door validates its arguments first; the zero Graph and
// Snapshot are ErrClosed. Names are drafts for API review (PLAN:370).
//
// Rules the types enforce:
//
//   - Every write op carries an explicit temporal.Scope; there is no implicit
//     "now" and no default scope. An empty or zero Scope is refused.
//   - Every write op also carries Recorded, the caller's own knowledge instant
//     (gap row 15). It is evidence, never a cut: the zero value means "at
//     commit", a non-zero value is a position on Config.KnowledgeAxis.
//   - Every read names its view (Declared or Effective), its valid-time scope and
//     its knowledge scope. A snapshot mode never implies a valid coordinate.
//   - Zero is a coordinate, not a sentinel. Infinity is temporal.Bound
//     (NegativeInfinity / PositiveInfinity), never MinInt64 or 0.
//   - IDs are types.NodeID / types.RelID (1..MaxInt64); zero and negative are
//     refused with ErrInvalidID.
//   - Property values are the typed Value, never `any`; numeric equality is
//     Cypher equality across widths (3 == 3.0), see Value.
//   - Paging uses opaque Cursor values; a zero Cursor is invalid. Every multi-row
//     door (Scan, Lookup, Timeline, Resume) takes PageLimits: a row limit (at
//     most MaxPageRows) and a mandatory byte limit. Reads validate with the
//     graph's Config.Limits and axes, carried in the Snapshot.
//   - Receipt, Cut and Cursor are opaque and serialisable (Bytes, Parse*);
//     only the engine mints them, and Snapshot.Cut reads the snapshot's cut.
//   - OpenSnapshot has no partition/graph read scope yet (PLAN:372); SnapshotSpec
//     has unexported fields so a WithScope option can be added without breaking
//     callers.
package graph
