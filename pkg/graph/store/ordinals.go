package store

// OrdinalCapability is OPTIONAL. A store implementing it assigns every node
// and every relationship it keeps a dense ordinal (types.Node.Ordinal,
// types.Relationship.Ordinal): 1, 2, 3, ... in the order the store first
// holds the entity, carried by every row it hands out (point reads, lends,
// scans, adjacency, history rows of an entity it still knows). A consumer
// sizes arrays by MaxNodeOrdinal / MaxRelOrdinal + 1 and indexes them by the
// ordinal instead of keying maps by snowflake ID.
//
// The guarantees, for one open store:
//   - Stable: an entity's ordinal does not change while it has a current row.
//   - Unique, never reused: no two entities carry the same ordinal, and an
//     ordinal is never given to another entity after its entity is deleted
//     (a held ordinal can go stale, it can never alias). A recreated ID (an
//     import after a delete) may get a new ordinal.
//   - Dense: ordinals are handed out consecutively, so Max*Ordinal is at most
//     the number of entities the store has held since it opened; deletes
//     leave holes.
//   - Concurrent writes: an ordinal is assigned under the store's write lock
//     before the row becomes visible, so a reader never sees a row without
//     its ordinal or with another one, and Max*Ordinal read after a row was
//     seen is at least that row's ordinal.
//   - 0 means none: a row the store has no ordinal for (see each backend).
//
// Not guaranteed: stability across a reopen (ordinals are not persisted),
// equality between a primary and a replica, or between stores. Max*Ordinal
// return 0 for a closed store.
//
// HasOrdinals reports whether this store assigns ordinals at all (a badger
// store opened with DisableOrdinals implements the interface but does not).
type OrdinalCapability interface {
	HasOrdinals() bool
	MaxNodeOrdinal() uint32
	MaxRelOrdinal() uint32
}
