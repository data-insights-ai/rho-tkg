package tiered

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Earlier lives on other shards (backlog 49).
//
// A history read of an ID whose row is live goes to the row's shard (with the
// archive for the reference shard, and the reference shard for the archive):
// a row's history is written where its row lives. A deleted ID re-imported
// onto ANOTHER shard breaks that: a relationship's row lives on its start
// node's shard, so a re-import with a start node elsewhere moves the row; a
// node's row lives on the reference shard or on its ID's event shard by its
// primary-label class, so a re-import with the other class moves it. The
// earlier lives' rows stay where they were written.
//
// Such an ID is recorded when the re-import's row is written (an entry in the
// reference shard's meta, made durable before the row, and an in-memory set
// loaded at open), and every history read of a recorded ID walks every history
// shard, as the read of a deleted ID does. Re-imports written before the record
// existed are found once, by backfillHistoryLives at the first open. The
// history is not moved with the row: a move is a history write plus a history
// truncate in the change log, and a replica replaying that truncate would drop
// the whole chain. Memory cost: one set entry per re-imported-across-shards ID,
// none otherwise; a read of any other ID pays one atomic load. A record is
// dropped only by Clear — not when a purge or a truncation to 0 later removes
// the earlier rows (the walk then finds nothing more; a correct answer at the
// deleted-ID cost) — so the set is bounded by the number of cross-shard
// re-imports ever made.

const (
	historyLivesMetaPrefix = "tiered_history_lives/"
	relLivesMetaPrefix     = historyLivesMetaPrefix + "rel/"
	nodeLivesMetaPrefix    = historyLivesMetaPrefix + "node/"
	// historyLivesBackfilledKey marks a store whose re-imports before the
	// record existed have been found (backfillHistoryLives); outside the
	// prefix loadHistoryLives reads.
	historyLivesBackfilledKey = "tiered_history_lives_backfilled"
)

// historyLives is the set of IDs whose history spans shards beyond their row's.
type historyLives struct {
	n   atomic.Int64
	ids sync.Map // snowflake.ID -> struct{}
}

func (h *historyLives) has(id snowflake.ID) bool {
	if h.n.Load() == 0 {
		return false
	}
	_, ok := h.ids.Load(id)
	return ok
}

func (h *historyLives) add(id snowflake.ID) {
	if _, loaded := h.ids.LoadOrStore(id, struct{}{}); !loaded {
		h.n.Add(1)
	}
}

func (h *historyLives) reset() {
	h.ids.Clear()
	h.n.Store(0)
}

// relLivesRoute is a history read's route for rid: (live, isArchive) as
// the read found the row, or (false, false) — every history shard — for an ID
// with earlier lives on other shards.
func (ts *Store) relLivesRoute(rid types.RelID, live, isArchive bool) (bool, bool) {
	if ts.relLives.has(rid.SnowflakeID()) {
		return false, false
	}
	return live, isArchive
}

// nodeLivesRoute is relLivesRoute for nodes.
func (ts *Store) nodeLivesRoute(nid types.NodeID, live, isArchive bool) (bool, bool) {
	if ts.nodeLives.has(nid.SnowflakeID()) {
		return false, false
	}
	return live, isArchive
}

// loadHistoryLives fills the sets from the reference shard's meta at open.
func (ts *Store) loadHistoryLives() error {
	return ts.refShard.ForEachMetaName(historyLivesMetaPrefix, func(name string) error {
		set, rest := &ts.relLives, strings.TrimPrefix(name, relLivesMetaPrefix)
		if rest == name {
			set, rest = &ts.nodeLives, strings.TrimPrefix(name, nodeLivesMetaPrefix)
		}
		id, err := strconv.ParseInt(rest, 10, 64)
		if rest == name || err != nil || id <= 0 {
			return fmt.Errorf("graph: load history lives: malformed meta entry %q", name)
		}
		set.add(snowflake.ID(id))
		return nil
	})
}

// backfillHistoryLives records, once per store, the IDs a re-import made
// before the record existed (v4.51.1 and earlier) left with history on a shard
// their live row's reads do not reach. Run at open; skipped once
// historyLivesBackfilledKey is set.
//
// An ID needs a record exactly when it has a history row on a shard S and its
// live row on a shard L outside S's read set (S itself, plus the archive for
// the reference shard and the reference shard for the archive; the relation is
// symmetric). Pass 1 collects every history ID with no live row in its shard's
// read set; pass 2 records those live on any shard. Cost, once: every shard
// (cold ones included) is opened up to twice — pass 1 reads its history IDs,
// pass 2 asks its RAM live-ID index — and 8 B per collected ID (an ID deleted
// on its history's shard) is held for the walk.
func (ts *Store) backfillHistoryLives() error {
	done, err := ts.refShard.MetaGet(historyLivesBackfilledKey)
	if err != nil || len(done) > 0 {
		return err
	}
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return err
	}
	if archive != nil {
		defer archiveCheckin()
	}
	pair := func(s *BadgerStore) *BadgerStore {
		switch {
		case s == ts.refShard:
			return archive
		case archive != nil && s == archive:
			return ts.refShard
		}
		return nil
	}
	liveNear := func(s *BadgerStore, has func(*BadgerStore, snowflake.ID) bool, id snowflake.ID) bool {
		o := pair(s)
		return has(s, id) || (o != nil && has(o, id))
	}
	hasRel := func(s *BadgerStore, id snowflake.ID) bool { return s.HasRelID(id) }
	hasNode := func(s *BadgerStore, id snowflake.ID) bool { return s.HasNodeID(id) }

	var rels, nodes []snowflake.ID
	if err := ts.forEachHistoryShard(nil, func(s *BadgerStore) (bool, error) {
		if err := s.ForEachRelHistoryID(func(id types.RelID) bool {
			if !liveNear(s, hasRel, id.SnowflakeID()) {
				rels = append(rels, id.SnowflakeID())
			}
			return true
		}); err != nil {
			return false, err
		}
		return false, s.ForEachNodeHistoryID(func(id types.NodeID) bool {
			if !liveNear(s, hasNode, id.SnowflakeID()) {
				nodes = append(nodes, id.SnowflakeID())
			}
			return true
		})
	}); err != nil {
		return err
	}
	slices.Sort(rels)
	rels = slices.Compact(rels)
	slices.Sort(nodes)
	nodes = slices.Compact(nodes)

	found := map[*historyLives][]snowflake.ID{}
	if len(rels)+len(nodes) > 0 {
		if err := ts.forEachHistoryShard(nil, func(s *BadgerStore) (bool, error) {
			for _, id := range rels {
				if s.HasRelID(id) {
					found[&ts.relLives] = append(found[&ts.relLives], id)
				}
			}
			for _, id := range nodes {
				if s.HasNodeID(id) {
					found[&ts.nodeLives] = append(found[&ts.nodeLives], id)
				}
			}
			return false, nil
		}); err != nil {
			return err
		}
	}
	for set, ids := range found {
		prefix := relLivesMetaPrefix
		if set == &ts.nodeLives {
			prefix = nodeLivesMetaPrefix
		}
		for _, id := range ids {
			if err := ts.refShard.MetaSet(prefix+strconv.FormatInt(int64(id), 10), []byte{1}); err != nil {
				return err
			}
			set.add(id)
		}
	}
	// One sync covers the records above (same write-ahead log).
	return ts.refShard.MetaSetDurable(historyLivesBackfilledKey, []byte{1})
}

// recordHistoryLives records id in set: durably in the reference shard's meta
// first, so a row written after it never outlives the record.
func (ts *Store) recordHistoryLives(set *historyLives, prefix string, id snowflake.ID) error {
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	defer refCheckin()
	if err := ref.MetaSetDurable(prefix+strconv.FormatInt(int64(id), 10), []byte{1}); err != nil {
		return err
	}
	set.add(id)
	return nil
}

// relCreateProbe is a caller-ID relationship create's walk over every shard:
// the duplicate probe (relIDExists' answer: a shard holds rid's row, or a row
// that fails to read) and, in the same walk, the earlier-lives note — when a
// shard outside owner's read set (owner, the shard the new row is written to,
// is pinned; the reference shard and the archive read each other) holds a
// history row of rid, rid is recorded before the row is written. One walk, so
// a cold shard is opened once for both questions.
func (ts *Store) relCreateProbe(rid types.RelID, owner *BadgerStore) error {
	id := rid.SnowflakeID()
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return err
	}
	if archive != nil {
		defer archiveCheckin()
	}
	refSide := func(s *BadgerStore) bool { return s == ts.refShard || (archive != nil && s == archive) }
	ownerReads := func(s *BadgerStore) bool { return s == owner || (refSide(owner) && refSide(s)) }
	exists, earlier := false, ts.relLives.has(id)
	err = ts.forEachHistoryShard(nil, func(s *BadgerStore) (bool, error) {
		if _, ok := probeRelRow(s, rid); ok {
			exists = true
			return true, nil
		}
		if earlier || ownerReads(s) {
			return false, nil
		}
		has, err := s.HasRelHistory(rid)
		earlier = has
		return false, err
	})
	if err != nil {
		return err
	}
	if exists {
		return ErrRelExists
	}
	if !earlier || ts.relLives.has(id) {
		return nil
	}
	return ts.recordHistoryLives(&ts.relLives, relLivesMetaPrefix, id)
}

// noteNodeEarlierLives is relCreateProbe's note for a node created on owner. A
// node's rows can only lie on the reference side or on its ID's event shard,
// so only the side the row is not written to is asked.
func (ts *Store) noteNodeEarlierLives(nid types.NodeID, owner *BadgerStore) error {
	id := nid.SnowflakeID()
	if ts.nodeLives.has(id) {
		return nil
	}
	ask := func(s *BadgerStore) (bool, error) {
		if s == nil || s == owner {
			return false, nil
		}
		return s.HasNodeHistory(nid)
	}
	found := false
	if owner == ts.refShard {
		es := ts.timestampToEventShardEntry(id)
		s, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return err
		}
		found, err = ask(s)
		release()
		if err != nil {
			return err
		}
	} else {
		ref, refCheckin, err := ts.checkoutRefShard()
		if err != nil {
			return err
		}
		found, err = ask(ref)
		refCheckin()
		if err != nil {
			return err
		}
		if !found {
			archive, archiveCheckin, err := ts.checkoutArchive()
			if err != nil {
				return err
			}
			if archive != nil {
				found, err = ask(archive)
				archiveCheckin()
				if err != nil {
					return err
				}
			}
		}
	}
	if !found {
		return nil
	}
	return ts.recordHistoryLives(&ts.nodeLives, nodeLivesMetaPrefix, id)
}
