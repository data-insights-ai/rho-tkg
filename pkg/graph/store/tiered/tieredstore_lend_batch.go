package tiered

import (
	"cmp"
	"errors"
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
// once and read with lendNodesCaching (its badger LendNode per ID: the
// shard's cache serves and keeps the rows). ids need not be ascending. Pins
// are taken one at a time (withPin).
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
	l := batchLend[types.NodeID, types.Node]{ids: ids, out: out, lend: lendNodesCaching}
	pending := l.all()
	// lendListed reads the pending IDs store lists and keeps the others.
	lendListed := func(store *BadgerStore) error {
		var here []int
		here, pending = splitBy(pending, func(i int) bool { return store.HasNodeID(ids[i].SnowflakeID()) })
		return l.read(store, here)
	}

	if err := withPin(ts.checkoutRefShard, lendListed); err != nil || len(pending) == 0 {
		return err
	}
	ts.refArchiveGapHook()
	if err := withPin(ts.checkoutArchive, func(archive *BadgerStore) error {
		if archive == nil {
			return nil
		}
		return lendListed(archive)
	}); err != nil || len(pending) == 0 {
		return err
	}

	shards, groups := groupByEventShard(ts, ids, pending)
	for k, es := range shards {
		if err := withPin(es.readOnlyCheckout(ts), func(store *BadgerStore) error {
			return l.read(store, groups[k])
		}); err != nil {
			return err
		}
	}
	return nil
}

// LendRelationshipsSorted is LendNodesSorted for relationships, routed as
// LendRelationship routes: the reference shard, the archive, the event shard
// of the ID's timestamp, then every other event shard holding rows; a shard
// whose row is missing passes the ID on. Like LendRelationship it holds one
// pin at a time, so a probe over many cold shards keeps at most one of them
// pinned; a shard read for its own IDs is pinned again by the probe.
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
	l := batchLend[types.RelID, types.Relationship]{ids: ids, out: out, lend: lendRelsCaching}
	pending := l.all()
	// lendFound reads the IDs of ask that store lists and adds to pending
	// those it does not list or lent no row for.
	lendFound := func(store *BadgerStore, ask []int) error {
		here, rest := splitBy(ask, func(i int) bool { return store.HasRelID(ids[i].SnowflakeID()) })
		if err := l.read(store, here); err != nil {
			return err
		}
		pending = append(pending, rest...)
		for _, i := range here {
			if out[i] == nil {
				pending = append(pending, i)
			}
		}
		return nil
	}
	// lendPending asks store for every pending ID.
	lendPending := func(store *BadgerStore) error {
		ask := pending
		pending = nil
		return lendFound(store, ask)
	}

	if err := withPin(ts.checkoutRefShard, lendPending); err != nil || len(pending) == 0 {
		return err
	}
	ts.refArchiveGapHook()
	if err := withPin(ts.checkoutArchive, func(archive *BadgerStore) error {
		if archive == nil {
			return nil
		}
		return lendPending(archive)
	}); err != nil || len(pending) == 0 {
		return err
	}

	shards, groups := groupByEventShard(ts, ids, pending)
	pending = nil
	for k, es := range shards {
		if err := withPin(es.readOnlyCheckout(ts), func(store *BadgerStore) error {
			return lendFound(store, groups[k])
		}); err != nil {
			return err
		}
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
		// An ID's own event shard, which LendRelationship skips here, misses
		// it again (a set lookup).
		if err := withPin(es.readOnlyCheckout(ts), lendPending); err != nil || len(pending) == 0 {
			return err
		}
	}
	return nil
}

// withPin pins a shard with checkout, runs read on it and releases the pin,
// also on a panic. The lend doors take every pin through it, one after the
// other: no pin is held while another is taken, so a door never waits on a
// shard's move gate (or a writable upgrade) while holding another shard.
func withPin(checkout func() (*BadgerStore, func(), error), read func(*BadgerStore) error) error {
	store, release, err := checkout()
	if err != nil {
		return err
	}
	defer release()
	return read(store)
}

// readOnlyCheckout is checkoutStoreReadOnly as a withPin checkout.
func (es *EventShard) readOnlyCheckout(ts *Store) func() (*BadgerStore, func(), error) {
	return func() (*BadgerStore, func(), error) { return es.checkoutStoreReadOnly(ts) }
}

// lendNodesCaching lends each ID with the shard's badger LendNode, as
// tiered's per-ID lend did up to v4.102.0: a cache hit is promoted, a miss is
// read in its own transaction and filled into the shard's cache (epoch read
// before the read, prefetchNode). Callers lend the same rows round after
// round, and badger's batch lend would read every one from badger each time:
// it keeps round 7's scan rule (no promotion, no fill; LENDCOST round 2,
// ai-soc: LendByIDs 115 -> 629 s on v4.103.0). out[i] is nil where the shard
// holds no row.
func lendNodesCaching(store *BadgerStore, ids []types.NodeID, out []*types.Node) error {
	for i, id := range ids {
		n, err := store.LendNode(id)
		if errors.Is(err, ErrNodeNotFound) {
			out[i] = nil
			continue
		}
		if err != nil {
			return err
		}
		out[i] = n
	}
	return nil
}

// lendRelsCaching is lendNodesCaching for relationships (LendRelationship).
func lendRelsCaching(store *BadgerStore, ids []types.RelID, out []*types.Relationship) error {
	for i, id := range ids {
		r, err := store.LendRelationship(id)
		if errors.Is(err, ErrRelNotFound) {
			out[i] = nil
			continue
		}
		if err != nil {
			return err
		}
		out[i] = r
	}
	return nil
}

// batchLend reads positions of ids from one shard into out with lend, which
// gets its IDs ascending.
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
