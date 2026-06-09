package tiered

import (
	"errors"
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Shard routing helpers ---

// shardForNode routes class-only operations by primary label. Event labels
// return the current hot shard; node create paths with concrete IDs use
// shardForNodeCreate so backfilled event IDs land on their timestamp owner.
func (ts *Store) shardForNode(primaryLabel uint16) *BadgerStore {
	if ts.ontology.ClassifyByToken(primaryLabel) == ClassReference {
		return ts.refShard
	}
	ts.mu.RLock()
	s := ts.hotShard.store
	ts.mu.RUnlock()
	return s
}

// shardForNodeID resolves which shard owns a node ID. It preserves the legacy
// unpinned return contract for tests; production paths use shardForNodeIDChecked.
func (ts *Store) shardForNodeID(id types.NodeID) (*BadgerStore, error) {
	store, checkin, err := ts.shardForNodeIDChecked(id)
	if err != nil {
		return nil, err
	}
	checkin()
	return store, nil
}

// timestampToEventShardEntry extracts the creation timestamp from a snowflake ID
// and returns the *EventShard responsible for that timestamp. Callers then use
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

func (ts *Store) idOutsideHotShardWindow(id snowflake.ID) bool {
	created := snowflakepkg.Layout.CreatedAt(id)

	ts.mu.RLock()
	hotStart := ts.hotShard.timeStart
	hotEnd := ts.hotShard.timeEnd
	ts.mu.RUnlock()

	return created.Before(hotStart) || !created.Before(hotEnd)
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
func (ts *Store) shardForRelIDChecked(id types.RelID) (store *BadgerStore, checkin func(), err error) {
	store, checkin, _, err = ts.shardForRelIDCheckedWithArchive(id)
	return store, checkin, err
}

func (ts *Store) shardForRelIDCheckedWithArchive(id types.RelID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	if err := ts.checkOpen(); err != nil {
		return nil, nil, false, err
	}
	raw := id.SnowflakeID()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, nil, false, err
	}
	if relationshipRowExists(ref, id) {
		return ref, refCheckin, false, nil
	}
	refCheckin()

	// Probe refArchive: ArchiveNode migrates a reference node AND its
	// rels to refArchive, so archived rels live there after archive.
	// Pin via checkoutArchive (mirrors the node resolver) so a
	// concurrent Close cannot tear it down mid-use.
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, nil, false, err
	}
	if archive != nil {
		if relationshipRowExists(archive, id) {
			return archive, archiveCheckin, true, nil
		}
		archiveCheckin()
	}

	candidateEntry := ts.timestampToEventShardEntry(raw)
	candidate, candidateRelease, err := candidateEntry.checkoutStoreForRead(ts)
	if err != nil {
		return nil, nil, false, err
	}
	if relationshipRowExists(candidate, id) {
		return candidate, candidateRelease, false, nil
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
		s, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			candidateRelease()
			return nil, nil, false, err
		}
		if relationshipRowExists(s, id) {
			candidateRelease()
			return s, release, false, nil
		}
		release()
	}

	// Not found anywhere — return the timestamp candidate so downstream reads
	// surface a typed ErrRelNotFound. The caller still owns its checkin.
	return candidate, candidateRelease, false, nil
}

// relationshipRowExists is conservative for placement/collision checks: a
// corrupt but indexed row still occupies the relationship ID until the caller
// reads the row and surfaces the decode error.
func relationshipRowExists(store *BadgerStore, id types.RelID) bool {
	_, found, err := relationshipRow(store, id)
	return found || err != nil
}

// relationshipRowLive is for caller-visible read paths where corruption must
// be returned instead of being treated as ordinary liveness.
func relationshipRowLive(store *BadgerStore, id types.RelID) (bool, error) {
	_, found, err := relationshipRow(store, id)
	if err != nil {
		return false, err
	}
	return found, nil
}

func relationshipRow(store *BadgerStore, id types.RelID) (*types.Relationship, bool, error) {
	if store == nil || !store.HasRelID(id.SnowflakeID()) {
		return nil, false, nil
	}
	// HasRelID is an index-derived fast path; verify the entity row.
	rel, err := store.GetRelationship(id)
	if err != nil {
		if errors.Is(err, ErrRelNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return rel, true, nil
}

func nodeRowLive(store *BadgerStore, id types.NodeID) (bool, error) {
	if store == nil || !store.HasNodeID(id.SnowflakeID()) {
		return false, nil
	}
	_, err := store.GetNode(id)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// shardForNodeIDChecked resolves the storage shard for a node ID and increments
// its activeReqs counter so closeIdleShards cannot close the DB while the caller
// is using the returned store pointer.
//
// The caller MUST invoke the returned checkin function exactly once when done
// (typically via defer checkin()). Failing to call checkin() leaks activeReqs,
// which prevents idle cold shards from ever being closed.
//
// refShard and refArchive are not subject to idle-close, but Close does close
// them, so returned reference stores are pinned just like event shards.
func (ts *Store) shardForNodeIDChecked(id types.NodeID) (store *BadgerStore, checkin func(), err error) {
	store, checkin, _, err = ts.shardForNodeIDCheckedWithArchive(id)
	return store, checkin, err
}

func (ts *Store) shardForNodeIDCheckedWithArchive(id types.NodeID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	if err := ts.checkOpen(); err != nil {
		return nil, nil, false, err
	}
	raw := id.SnowflakeID()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, nil, false, err
	}
	if ref.HasNodeID(raw) {
		return ref, refCheckin, false, nil
	}
	refCheckin()

	// Archive probe — pin via checkoutArchive so a concurrent Close
	// cannot tear down the underlying DB while the caller is using
	// the returned pointer for GetNode / Update / etc. The previous
	// no-op checkin was incorrect: refArchive IS closed by Close, just
	// not by closeIdleShards. checkoutArchive also handles cold-start
	// lazy-open via ensureRefArchive when the catalog has the entry but
	// the in-memory pointer is nil.
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, nil, false, err
	}
	if archive != nil {
		if archive.HasNodeID(raw) {
			return archive, archiveCheckin, true, nil
		}
		archiveCheckin()
	}

	// Event shard: resolve via timestamp, then checkout to prevent idle-close
	// races. Cold shards opened only for this operation close again on checkin.
	es := ts.timestampToEventShardEntry(raw)
	store, checkin, err = es.checkoutStoreForRead(ts)
	if err != nil {
		return nil, nil, false, err
	}
	return store, checkin, false, nil
}

// eventShardSnapshot returns a snapshot of event shards filtered by depth.
// Caller must hold at least ts.mu.RLock. Callers that dereference returned
// shards must pin each store with checkoutStore/checkinStore.
func (ts *Store) eventShardSnapshot(depth ShardDepth) []*EventShard {
	var shards []*EventShard
	for _, es := range ts.eventShards {
		switch depth {
		case DepthHot:
			if es.currentTier() == TierHot {
				shards = append(shards, es)
			}
		case DepthWarm:
			tier := es.currentTier()
			if tier == TierHot || tier == TierWarm {
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
		start := shardWindowStart(t, window)
		return start.UTC().Format("20060102T150405.000Z")
	}
}

// shardWindowStart computes the start time for a shard window containing t.
func shardWindowStart(t time.Time, window time.Duration) time.Time {
	switch {
	case window >= 7*24*time.Hour:
		// ISO week start (Monday).
		year, week := t.ISOWeek()
		// ISO week 1 is the week containing January 4.
		jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, t.Location())
		jan4Weekday := jan4.Weekday()
		if jan4Weekday == time.Sunday {
			jan4Weekday = 7
		}
		daysToMonday := int(time.Monday - jan4Weekday)
		isoWeek1Monday := jan4.AddDate(0, 0, daysToMonday)
		return isoWeek1Monday.AddDate(0, 0, (week-1)*7)
	case window >= 24*time.Hour:
		// Day start.
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	default:
		return t.Truncate(window)
	}
}
