package tiered

import (
	"cmp"
	"fmt"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.EntityLendBatchCapability = (*Store)(nil)

// LendNodesSorted implements store.EntityLendBatchCapability: out[i] is the
// row LendNode(ids[i]) lends, nil where none. One move read lock covers the
// call; the IDs are routed as LendNode routes (the reference shard, the
// archive, the event shard of the ID's timestamp) and each shard is pinned
// once and read with its badger LendNodesSorted (one read snapshot). ids
// need not be ascending.
func (ts *Store) LendNodesSorted(ids []types.NodeID, out []*types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if len(out) != len(ids) {
		return fmt.Errorf("graph: lend nodes: %d ids, %d out slots", len(ids), len(out))
	}
	if len(ids) == 0 {
		return nil
	}
	move := ts.lockMoveRead() // route and read see each row on one shard
	defer move.unlock()
	// Every position is routed to one shard, whose read sets its slot.
	l := batchLend[types.NodeID, types.Node]{ids: ids, out: out, lend: (*BadgerStore).LendNodesSorted}
	pending := l.all()

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	var here []int
	here, pending = splitBy(pending, func(i int) bool { return ref.HasNodeID(ids[i].SnowflakeID()) })
	err = l.read(ref, here)
	refCheckin()
	if err != nil || len(pending) == 0 {
		return err
	}

	ts.refArchiveGapHook()
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return err
	}
	if archive != nil {
		here, pending = splitBy(pending, func(i int) bool { return archive.HasNodeID(ids[i].SnowflakeID()) })
		err = l.read(archive, here)
		archiveCheckin()
		if err != nil || len(pending) == 0 {
			return err
		}
	}

	shards, groups := groupByEventShard(ts, ids, pending)
	for k, es := range shards {
		store, release, err := es.checkoutStoreReadOnly(ts)
		if err != nil {
			return err
		}
		err = l.read(store, groups[k])
		release()
		if err != nil {
			return err
		}
	}
	return nil
}

// LendRelationshipsSorted is LendNodesSorted for relationships, routed as
// LendRelationship routes: the reference shard, the archive, the event shard
// of the ID's timestamp, then every other event shard holding rows; a shard
// whose row is missing passes the ID on. Each shard is pinned once per call:
// the event shards stay pinned until the probe of the others is done.
func (ts *Store) LendRelationshipsSorted(ids []types.RelID, out []*types.Relationship) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if len(out) != len(ids) {
		return fmt.Errorf("graph: lend relationships: %d ids, %d out slots", len(ids), len(out))
	}
	clear(out)
	if len(ids) == 0 {
		return nil
	}
	move := ts.lockMoveRead() // route and read see each row on one shard
	defer move.unlock()
	l := batchLend[types.RelID, types.Relationship]{ids: ids, out: out, lend: (*BadgerStore).LendRelationshipsSorted}
	pending := l.all()
	// lendFound reads the pending IDs store lists and keeps those it lent no row.
	lendFound := func(store *BadgerStore, pending []int) ([]int, error) {
		here, rest := splitBy(pending, func(i int) bool { return store.HasRelID(ids[i].SnowflakeID()) })
		if err := l.read(store, here); err != nil {
			return nil, err
		}
		for _, i := range here {
			if out[i] == nil {
				rest = append(rest, i)
			}
		}
		return rest, nil
	}

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	pending, err = lendFound(ref, pending)
	refCheckin()
	if err != nil || len(pending) == 0 {
		return err
	}

	ts.refArchiveGapHook()
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return err
	}
	if archive != nil {
		pending, err = lendFound(archive, pending)
		archiveCheckin()
		if err != nil || len(pending) == 0 {
			return err
		}
	}

	pinned := make(map[*EventShard]*BadgerStore)
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	pin := func(es *EventShard) (*BadgerStore, error) {
		if store, ok := pinned[es]; ok {
			return store, nil
		}
		store, release, err := es.checkoutStoreReadOnly(ts)
		if err != nil {
			return nil, err
		}
		pinned[es] = store
		releases = append(releases, release)
		return store, nil
	}

	shards, groups := groupByEventShard(ts, ids, pending)
	pending = pending[:0]
	for k, es := range shards {
		store, err := pin(es)
		if err != nil {
			return err
		}
		missed, err := lendFound(store, groups[k])
		if err != nil {
			return err
		}
		pending = append(pending, missed...)
	}
	if len(pending) == 0 {
		return nil
	}

	ts.mu.RLock()
	probe := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if !es.noRows.Load() {
			probe = append(probe, es)
		}
	}
	ts.mu.RUnlock()
	for _, es := range probe {
		// An ID's own event shard, which LendRelationship skips here, is
		// pinned already and misses it again (a set lookup).
		store, err := pin(es)
		if err != nil {
			return err
		}
		if pending, err = lendFound(store, pending); err != nil || len(pending) == 0 {
			return err
		}
	}
	return nil
}

// batchLend reads positions of ids from one shard into out with the shard's
// badger batch lend, which wants its IDs ascending.
type batchLend[ID ~int64, Row any] struct {
	ids  []ID
	out  []*Row
	lend func(*BadgerStore, []ID, []*Row) error
}

// all returns every position of ids.
func (l *batchLend[ID, Row]) all() []int {
	pos := make([]int, len(l.ids))
	for i := range pos {
		pos[i] = i
	}
	return pos
}

// read lends the rows of the positions pos from store, in ID order.
func (l *batchLend[ID, Row]) read(store *BadgerStore, pos []int) error {
	if len(pos) == 0 {
		return nil
	}
	byID := func(a, b int) int { return cmp.Compare(l.ids[a], l.ids[b]) }
	if !slices.IsSortedFunc(pos, byID) {
		pos = slices.Clone(pos)
		slices.SortStableFunc(pos, byID)
	}
	sub := make([]ID, len(pos))
	for j, i := range pos {
		sub[j] = l.ids[i]
	}
	rows := make([]*Row, len(pos))
	if err := l.lend(store, sub, rows); err != nil {
		return err
	}
	for j, i := range pos {
		l.out[i] = rows[j]
	}
	return nil
}

// splitBy splits pos into the positions in (f true) and the rest, each in
// pos's order.
func splitBy(pos []int, in func(i int) bool) (yes, no []int) {
	for _, i := range pos {
		if in(i) {
			yes = append(yes, i)
		} else {
			no = append(no, i)
		}
	}
	return yes, no
}

// groupByEventShard groups pos by the event shard of each ID's timestamp, the
// shards in first-seen order, each group in pos's order.
func groupByEventShard[ID ~int64](ts *Store, ids []ID, pos []int) ([]*EventShard, [][]int) {
	var shards []*EventShard
	var groups [][]int
	at := make(map[*EventShard]int)
	for _, i := range pos {
		es := ts.timestampToEventShardEntry(snowflake.ID(ids[i]))
		k, ok := at[es]
		if !ok {
			k = len(shards)
			at[es] = k
			shards = append(shards, es)
			groups = append(groups, nil)
		}
		groups[k] = append(groups[k], i)
	}
	return shards, groups
}
