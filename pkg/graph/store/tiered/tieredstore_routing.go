package tiered

import (
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Shard routing helpers ---

// shardForNode routes a node to the correct BadgerStore by its primary label.
// For event nodes, always routes to the hot shard (new writes).
func (ts *Store) shardForNode(primaryLabel uint16) *BadgerStore {
	if ts.ontology.ClassifyByToken(primaryLabel) == ClassReference {
		return ts.refShard
	}
	ts.mu.RLock()
	s := ts.hotShard.store
	ts.mu.RUnlock()
	return s
}

// shardForNodeID resolves which shard owns a node ID.
// O(1): try ref (HasNodeID), miss -> archive check -> timestamp extraction -> event shard.
// Returns error if cold shard lazy-open fails.
func (ts *Store) shardForNodeID(id types.NodeID) (*BadgerStore, error) {
	raw := id.SnowflakeID()
	if ts.refShard.HasNodeID(raw) {
		return ts.refShard, nil
	}
	// Check archive if open or catalog says it exists.
	archive := ts.refArchive.Load()
	if archive != nil && archive.HasNodeID(raw) {
		return archive, nil
	}
	if archive == nil && ts.hasArchiveShard() {
		if err := ts.ensureRefArchive(); err != nil {
			return nil, err
		}
		archive = ts.refArchive.Load()
		if archive != nil && archive.HasNodeID(raw) {
			return archive, nil
		}
	}
	return ts.timestampToEventShard(raw)
}

// timestampToEventShard extracts the creation timestamp from a snowflake ID
// and maps it to the correct event shard. Falls back to the hot shard if no
// shard window matches (entity from before the oldest shard).
// Returns error if cold shard lazy-open fails.
func (ts *Store) timestampToEventShard(id snowflake.ID) (*BadgerStore, error) {
	created := snowflakepkg.Layout.CreatedAt(id)

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	for _, es := range ts.eventShards {
		if !created.Before(es.timeStart) && created.Before(es.timeEnd) {
			return es.getStore(ts)
		}
	}
	return ts.hotShard.store, nil // fallback: newest shard (always open)
}

// timestampToEventShardEntry extracts the creation timestamp from a snowflake ID
// and returns the *EventShard responsible for that timestamp. Unlike
// timestampToEventShard, it returns the shard struct so callers can use
// checkoutStore/checkinStore for safe concurrent access.
// Falls back to hotShard if no window matches.
func (ts *Store) timestampToEventShardEntry(id snowflake.ID) *EventShard {
	created := snowflakepkg.Layout.CreatedAt(id)

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	for _, es := range ts.eventShards {
		if !created.Before(es.timeStart) && created.Before(es.timeEnd) {
			return es
		}
	}
	return ts.hotShard // fallback: newest shard (always open)
}

// shardForRelIDChecked resolves the storage shard for a relationship ID and
// increments activeReqs on any event shard returned, mirroring
// shardForNodeIDChecked.
//
// Cross-shard relationships may live in a shard that does not match their
// creation timestamp (the entity is stored in the start node's shard). The
// timestamp candidate is checked first as a fast path, then every other event
// shard (including cold shards) is probed in turn. Cold shards are included
// because a cross-shard rel created while the start-node shard was warm can
// later age to cold — the rel never moves, so the lookup must follow it.
//
// The caller MUST invoke the returned checkin function exactly once.
// refShard checkin is a no-op; event shard checkin decrements activeReqs.
func (ts *Store) shardForRelIDChecked(id types.RelID) (store *BadgerStore, checkin func(), err error) {
	raw := id.SnowflakeID()
	if ts.refShard.HasRelID(raw) {
		return ts.refShard, func() {}, nil
	}

	// Probe refArchive: ArchiveNode migrates a reference node AND its
	// rels to refArchive, so archived rels live there after archive.
	// Pin via checkoutArchive (mirrors the node resolver) so a
	// concurrent Close cannot tear it down mid-use.
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, nil, err
	}
	if archive != nil {
		if archive.HasRelID(raw) {
			return archive, archiveCheckin, nil
		}
		archiveCheckin()
	}

	candidateEntry := ts.timestampToEventShardEntry(raw)
	candidate, err := candidateEntry.checkoutStore(ts)
	if err != nil {
		return nil, nil, err
	}
	if candidate.HasRelID(raw) {
		return candidate, func() { candidateEntry.checkinStore() }, nil
	}

	// Probe every other event shard (excluding the candidate we already
	// checked). Cold shards are included so a cross-shard rel that aged to
	// cold is still found.
	ts.mu.RLock()
	probe := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if es == candidateEntry {
			continue
		}
		probe = append(probe, es)
	}
	ts.mu.RUnlock()

	for _, es := range probe {
		s, err := es.checkoutStore(ts)
		if err != nil {
			candidateEntry.checkinStore()
			return nil, nil, err
		}
		if s.HasRelID(raw) {
			candidateEntry.checkinStore()
			return s, func() { es.checkinStore() }, nil
		}
		es.checkinStore()
	}

	// Not found anywhere — return the timestamp candidate so downstream reads
	// surface a typed ErrRelNotFound. The caller still owns its checkin.
	return candidate, func() { candidateEntry.checkinStore() }, nil
}

// shardForNodeIDChecked resolves the storage shard for a node ID and increments
// its activeReqs counter so closeIdleShards cannot close the DB while the caller
// is using the returned store pointer.
//
// The caller MUST invoke the returned checkin function exactly once when done
// (typically via defer checkin()). Failing to call checkin() leaks activeReqs,
// which prevents idle cold shards from ever being closed.
//
// refShard and refArchive are never subject to idle-close, so their checkin is
// a no-op. Only event shards (especially cold tier) need the checkout/checkin
// protocol.
func (ts *Store) shardForNodeIDChecked(id types.NodeID) (store *BadgerStore, checkin func(), err error) {
	raw := id.SnowflakeID()
	if ts.refShard.HasNodeID(raw) {
		return ts.refShard, func() {}, nil // refShard: never closed, no-op checkin
	}

	// Archive probe — pin via checkoutArchive so a concurrent Close
	// cannot tear down the underlying DB while the caller is using
	// the returned pointer for GetNode / Update / etc. The previous
	// no-op checkin was incorrect: refArchive IS closed by Close, just
	// not by closeIdleShards. checkoutArchive also handles cold-start
	// lazy-open via ensureRefArchive when the catalog has the entry but
	// the in-memory pointer is nil.
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, nil, err
	}
	if archive != nil {
		if archive.HasNodeID(raw) {
			return archive, archiveCheckin, nil
		}
		archiveCheckin()
	}

	// Event shard: resolve via timestamp, then checkout to prevent idle-close race.
	es := ts.timestampToEventShardEntry(raw)
	store, err = es.checkoutStore(ts)
	if err != nil {
		return nil, nil, err
	}
	return store, func() { es.checkinStore() }, nil
}

// eventShardSnapshot returns a snapshot of event shards filtered by depth.
// Caller must hold at least ts.mu.RLock. Returns []*EventShard so callers
// can use es.getStore(ts) for lazy-open of cold shards.
func (ts *Store) eventShardSnapshot(depth ShardDepth) []*EventShard {
	var shards []*EventShard
	for _, es := range ts.eventShards {
		switch depth {
		case DepthHot:
			if es.tier == TierHot {
				shards = append(shards, es)
			}
		case DepthWarm:
			if es.tier == TierHot || es.tier == TierWarm {
				shards = append(shards, es)
			}
		default: // DepthAll
			shards = append(shards, es)
		}
	}
	return shards
}

// shardWindowName computes the canonical name for a time window.
func shardWindowName(t time.Time, window time.Duration) string {
	switch {
	case window >= 7*24*time.Hour:
		// Weekly: "2026-W09"
		year, week := t.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", year, week)
	case window >= 24*time.Hour:
		// Daily: "2026-03-02"
		return t.Format("2006-01-02")
	default:
		// Monthly fallback: "2026-03"
		return t.Format("2006-01")
	}
}

// shardWindowStart computes the start time for a shard window containing t.
func shardWindowStart(t time.Time, window time.Duration) time.Time {
	switch {
	case window >= 7*24*time.Hour:
		// ISO week start (Monday).
		year, week := t.ISOWeek()
		// Compute day 1 of ISO week.
		jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
		jan1Weekday := jan1.Weekday()
		if jan1Weekday == time.Sunday {
			jan1Weekday = 7
		}
		// Days from Jan 1 to Monday of ISO week 1.
		daysToMonday := int(time.Monday - jan1Weekday)
		if daysToMonday > 0 {
			daysToMonday -= 7
		}
		isoWeek1Monday := jan1.AddDate(0, 0, daysToMonday)
		return isoWeek1Monday.AddDate(0, 0, (week-1)*7)
	case window >= 24*time.Hour:
		// Day start.
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	default:
		// Month start.
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	}
}
