package tiered

import (
	"fmt"
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
// shard, as the read of a deleted ID does. The history is not moved with the
// row: a move is a history write plus a history truncate in the change log,
// and a replica replaying that truncate would drop the whole chain. Memory
// cost: one set entry per re-imported-across-shards ID, none otherwise; a read
// of any other ID pays one atomic load.

const (
	historyLivesMetaPrefix = "tiered_history_lives/"
	relLivesMetaPrefix     = historyLivesMetaPrefix + "rel/"
	nodeLivesMetaPrefix    = historyLivesMetaPrefix + "node/"
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

// noteRelEarlierLives records rid when a shard other than owner (the shard
// the new row is written to) and its reference/archive pair holds a history
// row of rid. Called by every caller-ID create after its duplicate probe, with
// owner pinned.
func (ts *Store) noteRelEarlierLives(rid types.RelID, owner *BadgerStore) error {
	id := rid.SnowflakeID()
	if ts.relLives.has(id) {
		return nil
	}
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return err
	}
	if archive != nil {
		defer archiveCheckin()
	}
	pairOfOwner := func(s *BadgerStore) bool {
		refSide := owner == ts.refShard || (archive != nil && owner == archive)
		return refSide && (s == ts.refShard || (archive != nil && s == archive))
	}
	found := false
	err = ts.forEachHistoryShard(owner, func(s *BadgerStore) (bool, error) {
		if pairOfOwner(s) {
			return false, nil
		}
		has, err := s.HasRelHistory(rid)
		found = has
		return has, err
	})
	if err != nil || !found {
		return err
	}
	return ts.recordHistoryLives(&ts.relLives, relLivesMetaPrefix, id)
}

// noteNodeEarlierLives is noteRelEarlierLives for a node created on owner. A
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
