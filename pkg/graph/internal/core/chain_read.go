package core

// Chain read order (backlog 32). A per-entity resolver (nodeAtLockedTx,
// nodeAsOfLocked, findNodeVersionMatchingDuringTx, findNodeVersionRelating and
// the relationship mirrors) assembles the entity's version chain from TWO
// store reads with no snapshot across them and no entity lock: the current
// row (GetNode / GetRelationship), then the history (GetNodeHistory, the
// temporal skeletons, a history-above probe). A concurrent Update /
// CloseVersion / label change / Delete / cascade moves the current row into
// history under the entity lock, and every store publishes that move
// history-first: the moved row is readable in history no later than the
// current slot changes (memory: one lock over both; badger: the pending
// buffer before the entity cache, publishMoveLocked; the core cascade: the
// demoted row via PutNodeVersion before ReplaceNode).
//
// Against a history-first move, reading the current row FIRST is the order
// that cannot lose the row: a reader that sees the new current row reads
// history after the move published it; a reader that sees the old current row
// may also find it in history (the same version twice), which the chain
// resolver tolerates. Reading history first could see the history before the
// move and the current slot after it — the moved row in neither read, so a
// pinned door answers "absent". Every chain assembly keeps that order;
// afterCurrentRead marks the window between the two reads.

// afterCurrentRead runs chainReadHook (tests only) between a per-entity chain
// assembly's current-row read and its history read, with the entity's raw ID
// (TestPointDoorRace_MoveBetweenChainReads lands a whole move there).
func (c *Core) afterCurrentRead(id int64) {
	if h := c.chainReadHook; h != nil {
		h(id)
	}
}
