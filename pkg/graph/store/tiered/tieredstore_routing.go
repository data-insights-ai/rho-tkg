package tiered

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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
	hot := ts.hotShard
	ts.mu.RUnlock()
	// es.store under shardMu: the handle sweep swaps a shard's store under it.
	hot.shardMu.Lock()
	defer hot.shardMu.Unlock()
	return hot.store
}

// IsReferenceLabel reports whether the label NAME is reference-class (routed
// to the reference shard); unlisted labels are event-class. It uses the same
// ontology as shardForNode and does not depend on label-token registration.
// The core layer uses it to decide whether a unique constraint is enforceable
// globally: reference entities all live on the reference shard (+ archive);
// event labels have no global value index.
func (ts *Store) IsReferenceLabel(name string) bool {
	return ts.ontology.ClassifyByName(name) == ClassReference
}

// IsReferenceType reports whether the relationship type NAME is listed in
// Config.RefTypes: one of it between two reference nodes lives on the
// reference shard whatever its mint instant.
func (ts *Store) IsReferenceType(name string) bool {
	_, ok := ts.refTypes[name]
	return ok
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

// timestampToEventShardEntry returns the *EventShard whose window covers the
// snowflake ID's timestamp (hotShard if none). Use checkoutStore/checkinStore.
func (ts *Store) timestampToEventShardEntry(id snowflake.ID) *EventShard {
	created := snowflakepkg.Layout.CreatedAt(id)

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if es := shardWindowFor(ts.shardIndex, created); es != nil {
		return es
	}
	return ts.hotShard // fallback: newest shard (always open)
}

// readableOwner is the event shard a read of an ID that es's window owns asks:
// es, unless es is closed and proven to hold no row (noRows), where no row of
// the ID can lie and opening it only to miss is the repeated empty-shard reopen
// of EMPTYBYID. That read asks the hot shard, as the read of an ID no window
// covers does (window_shard.go: reads route to the hot shard and answer
// not-found): the hot shard is open, so nothing opens, and it answers what the
// empty shard would, since an ID's row lies on its window's shard, the reference
// shard or the archive, and a relationship with its start node, which the
// by-ID probes of the other shards still find. A write that may store a row
// under the ID never takes this route: a write into es's window must open and
// wake it (wakeNoRowsLocked). A write that only changes a row the ID already
// has takes it (routeExisting): es holds none.
func (ts *Store) readableOwner(es *EventShard) *EventShard {
	if !es.noRows.Load() {
		return es
	}
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.hotShard
}

// relReadableOwner is readableOwner for a read of relationship id: also the
// hot shard when es is closed and its sealed range misses id
// (sealedWithoutRelID), since no row of id lies there.
func (ts *Store) relReadableOwner(es *EventShard, id snowflake.ID) *EventShard {
	if es.sealedWithoutRelID(id) {
		ts.mu.RLock()
		defer ts.mu.RUnlock()
		return ts.hotShard
	}
	return ts.readableOwner(es)
}

// reindexShardsLocked rebuilds shardIndex from eventShards, sorted by
// timeStart. Caller holds ts.mu.Lock (or owns the Store, as New does); run it
// after every insert into or delete from eventShards. Rotation changes timeEnd in
// place, but windows do not overlap, so the timeStart order is unaffected.
func (ts *Store) reindexShardsLocked() {
	ts.histTopoEpoch.Store(storeutil.NextHistoryEpoch()) // the shard set may change: every history token moves
	idx := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		// A zero-length shard (two rotations in one millisecond) holds no
		// window and shares its timeStart with its successor: indexing it
		// could hide the successor behind an unstable sort.
		if !es.timeEnd.After(es.timeStart) {
			continue
		}
		idx = append(idx, es)
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i].timeStart.Before(idx[j].timeStart) })
	ts.shardIndex = idx
}

// shardWindowFor returns the shard in idx (sorted by timeStart, windows
// non-overlapping) whose [timeStart, timeEnd) contains created, or nil.
func shardWindowFor(idx []*EventShard, created time.Time) *EventShard {
	// First shard starting after created; its predecessor is the only candidate.
	i := sort.Search(len(idx), func(i int) bool { return idx[i].timeStart.After(created) })
	if i == 0 {
		return nil
	}
	if es := idx[i-1]; created.Before(es.timeEnd) {
		return es
	}
	return nil
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
// increments activeReqs on any event shard returned, like
// shardForNodeIDChecked. A cross-shard relationship lives in its start node's
// shard, which may not match its timestamp: the timestamp candidate is probed
// first, then every other event shard including cold ones (the rel never
// moves when its shard ages to cold). The caller MUST call the returned
// checkin exactly once.
func (ts *Store) shardForRelIDChecked(id types.RelID) (store *BadgerStore, checkin func(), err error) {
	store, checkin, _, err = ts.shardForRelIDCheckedWithArchive(id)
	return store, checkin, err
}

func (ts *Store) shardForRelIDCheckedWithArchive(id types.RelID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	store, checkin, isArchive, _, err = ts.routeRelShard(id)
	return store, checkin, isArchive, err
}

// shardForRelIDReadWithArchive is shardForRelIDCheckedWithArchive for a caller
// that only reads the store it gets (MOVEREAD, shardForNodeIDRead).
func (ts *Store) shardForRelIDReadWithArchive(id types.RelID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	store, checkin, isArchive, _, err = ts.routeRelShardFor(id, routeRead, nil)
	return store, checkin, isArchive, err
}

// shardForRelIDReadCharged is shardForRelIDReadWithArchive charging the open
// of a closed archive or event shard to opens before it (READBUDGET step 6).
func (ts *Store) shardForRelIDReadCharged(id types.RelID, opens *storecontract.ReadTally) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	store, checkin, isArchive, _, err = ts.routeRelShardFor(id, routeRead, opens)
	return store, checkin, isArchive, err
}

// relRowProbe is the result of routeRelShard's probe: live (shard holds the
// entity row), err (corrupt but indexed row, which still occupies the ID), or
// neither (no shard holds the row).
type relRowProbe struct {
	live bool
	err  error
}

// probeRelRow probes store for id's entity row through the shard's
// current-row stamps (no decode, no copy); exists is relationshipRowExists.
func probeRelRow(store *BadgerStore, id types.RelID) (p relRowProbe, exists bool) {
	_, found, err := relCurrentStampsRow(store, id)
	return relRowProbe{live: found, err: err}, found || err != nil
}

// routeRelShard is shardForRelIDCheckedWithArchive plus the probe result of
// the shard it returns, so a caller that next asks whether the row is live
// there (the history walks) does not read the row a second time.
func (ts *Store) routeRelShard(id types.RelID) (store *BadgerStore, checkin func(), isArchive bool, found relRowProbe, err error) {
	return ts.routeRelShardFor(id, routeWrite, nil)
}

// routeRelShardFor is routeRelShard in mode (routeMode); opens (nil: none) is
// charged each closed shard's open before it (READBUDGET step 6).
func (ts *Store) routeRelShardFor(id types.RelID, mode routeMode, opens *storecontract.ReadTally) (store *BadgerStore, checkin func(), isArchive bool, found relRowProbe, err error) {
	readOnly := mode.readOnly()
	if err := ts.checkOpen(); err != nil {
		return nil, nil, false, relRowProbe{}, err
	}
	raw := id.SnowflakeID()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, nil, false, relRowProbe{}, err
	}
	if p, ok := probeRelRow(ref, id); ok {
		return ref, refCheckin, false, p, nil
	}
	refCheckin()

	// Probe refArchive: ArchiveNode moves a node and its rels there. Pinned via
	// checkoutArchive so a concurrent Close cannot tear it down.
	ts.refArchiveGapHook()
	archive, archiveCheckin, err := ts.checkoutArchiveCharged(opens)
	if err != nil {
		return nil, nil, false, relRowProbe{}, err
	}
	if archive != nil {
		if p, ok := probeRelRow(archive, id); ok {
			return archive, archiveCheckin, true, p, nil
		}
		archiveCheckin()
	}

	candidateEntry := ts.timestampToEventShardEntry(raw)
	if mode.skipsEmpty() {
		candidateEntry = ts.relReadableOwner(candidateEntry, raw)
	}
	candidate, candidateRelease, err := candidateEntry.checkoutForReadCharged(ts, readOnly, opens)
	if err != nil {
		return nil, nil, false, relRowProbe{}, err
	}
	if p, ok := probeRelRow(candidate, id); ok {
		return candidate, candidateRelease, false, p, nil
	}

	// Probe every other event shard (excluding the candidate we already
	// checked). Cold shards are included so a cross-shard rel that aged to
	// cold is still found.
	ts.mu.RLock()
	probe := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if es == candidateEntry || es.noRows.Load() || es.sealedWithoutRelID(raw) {
			continue
		}
		probe = append(probe, es)
	}
	ts.mu.RUnlock()

	for _, es := range probe {
		s, release, err := es.checkoutForReadCharged(ts, readOnly, opens)
		if err != nil {
			candidateRelease()
			return nil, nil, false, relRowProbe{}, err
		}
		if p, ok := probeRelRow(s, id); ok {
			candidateRelease()
			return s, release, false, p, nil
		}
		release()
	}

	// Not found anywhere — return the timestamp candidate so downstream reads
	// surface a typed ErrRelNotFound. The caller still owns its checkin.
	return candidate, candidateRelease, false, relRowProbe{}, nil
}

// relationshipRowExists is conservative for placement/collision checks: a
// corrupt but indexed row still occupies the ID until the caller reads it and
// surfaces the decode error. It uses the shard's current-row stamps (backlog
// 42) and misses exactly where GetRelationship misses.
func relationshipRowExists(store *BadgerStore, id types.RelID) bool {
	_, found, err := relCurrentStampsRow(store, id)
	return found || err != nil
}

// relationshipRowLive is for caller-visible read paths where corruption must
// be returned instead of being treated as ordinary liveness.
func relationshipRowLive(store *BadgerStore, id types.RelID) (bool, error) {
	return relationshipRowLiveVia(store, id, directRelReads)
}

// relationshipRowLiveVia is relationshipRowLive reading through via (a metered
// read counts the decode of the row it lends on a cache miss).
func relationshipRowLiveVia(store *BadgerStore, id types.RelID, via relVia) (bool, error) {
	_, found, err := lendRelationshipRowVia(store, id, via) // presence only: no copy of the row
	if err != nil {
		return false, err
	}
	return found, nil
}

func relationshipRow(store *BadgerStore, id types.RelID) (*types.Relationship, bool, error) {
	return relationshipRowVia(store, id, directRelReads)
}

// relationshipRowVia is relationshipRow reading the row through via.
func relationshipRowVia(store *BadgerStore, id types.RelID, via relVia) (*types.Relationship, bool, error) {
	if store == nil || !store.HasRelID(id.SnowflakeID()) {
		return nil, false, nil
	}
	// HasRelID is an index-derived fast path; verify the entity row.
	rel, err := via(store).GetRelationship(id)
	if err != nil {
		if errors.Is(err, ErrRelNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return rel, true, nil
}

// lendRelationshipRow is relationshipRow returning the shard's frozen row
// without a copy (LendRelationship).
func lendRelationshipRow(store *BadgerStore, id types.RelID) (*types.Relationship, bool, error) {
	return lendRelationshipRowVia(store, id, directRelReads)
}

// lendRelationshipRowVia is lendRelationshipRow reading the row through via.
func lendRelationshipRowVia(store *BadgerStore, id types.RelID, via relVia) (*types.Relationship, bool, error) {
	if store == nil || !store.HasRelID(id.SnowflakeID()) {
		return nil, false, nil
	}
	rel, err := via(store).LendRelationship(id)
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
	_, err := store.LendNode(id) // presence only: no copy of the row
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// routeMode is what the caller of a by-ID route does with the shard it gets,
// which decides what the route may do with a closed shard proven empty (noRows,
// or for a relationship a sealed ID range that misses the ID).
type routeMode uint8

const (
	// routeWrite: the caller may store a row under the ID (a create, a history
	// version, a repair). The ID's own shard is checked out writable; a closed
	// one is opened and its no-rows mark woken and saved before the write.
	routeWrite routeMode = iota
	// routeExisting: the caller changes only a row the ID already has (an
	// update, a delete, a history trim, an endpoint a create must find) and
	// answers its own miss when there is none. The shard is checked out
	// writable like routeWrite's, but a closed shard proven empty holds no row
	// to change, so it is neither opened nor woken: the route returns the hot
	// shard (readableOwner), which answers the miss, and a write that found no
	// row writes nothing there (EMPTYBYID).
	routeExisting
	// routeRead: the caller only reads (checkoutStoreReadOnly); a shard proven
	// empty is skipped as under routeExisting.
	routeRead
)

func (m routeMode) readOnly() bool   { return m == routeRead }
func (m routeMode) skipsEmpty() bool { return m != routeWrite }

// shardForNodeIDExisting is shardForNodeIDChecked for a write that changes only
// a row the node already has (routeExisting): an update, a delete, a label
// change, an endpoint a relationship create must find. The caller reads the
// node from the store it gets and answers its own miss before it writes.
func (ts *Store) shardForNodeIDExisting(id types.NodeID) (store *BadgerStore, checkin func(), err error) {
	store, checkin, _, err = ts.routeNodeShard(id, routeExisting, nil)
	return store, checkin, err
}

// shardForNodeIDExistingWithArchive is shardForNodeIDExisting reporting an
// archive hit.
func (ts *Store) shardForNodeIDExistingWithArchive(id types.NodeID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	return ts.routeNodeShard(id, routeExisting, nil)
}

// shardForRelIDExisting is shardForRelIDChecked for a write that changes only a
// row the relationship already has (routeExisting).
func (ts *Store) shardForRelIDExisting(id types.RelID) (store *BadgerStore, checkin func(), err error) {
	store, checkin, _, err = ts.shardForRelIDExistingWithArchive(id)
	return store, checkin, err
}

// shardForRelIDExistingWithArchive is shardForRelIDExisting reporting an
// archive hit.
func (ts *Store) shardForRelIDExistingWithArchive(id types.RelID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	store, checkin, isArchive, _, err = ts.routeRelShardFor(id, routeExisting, nil)
	return store, checkin, isArchive, err
}

// shardForNodeIDChecked resolves the storage shard for a node ID and
// increments its activeReqs so closeIdleShards cannot close the DB while the
// caller uses the store. The caller MUST call the returned checkin exactly
// once (defer checkin()); a leak keeps cold shards from ever closing.
// refShard and refArchive are not idle-closed but Close closes them, so they
// are pinned like event shards.
func (ts *Store) shardForNodeIDChecked(id types.NodeID) (store *BadgerStore, checkin func(), err error) {
	store, checkin, _, err = ts.shardForNodeIDCheckedWithArchive(id)
	return store, checkin, err
}

func (ts *Store) shardForNodeIDCheckedWithArchive(id types.NodeID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	return ts.routeNodeShard(id, routeWrite, nil)
}

// shardForNodeIDRead is shardForNodeIDChecked for a caller that only reads
// the store it gets: a moving event shard answers from its source
// (checkoutStoreReadOnly, MOVEREAD).
func (ts *Store) shardForNodeIDRead(id types.NodeID) (store *BadgerStore, checkin func(), err error) {
	store, checkin, _, err = ts.routeNodeShard(id, routeRead, nil)
	return store, checkin, err
}

// shardForNodeIDReadWithArchive is shardForNodeIDRead reporting an archive hit.
func (ts *Store) shardForNodeIDReadWithArchive(id types.NodeID) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	return ts.routeNodeShard(id, routeRead, nil)
}

// shardForNodeIDReadCharged is shardForNodeIDReadWithArchive charging the
// open of a closed archive or event shard to opens before it (READBUDGET step
// 6; nil: none).
func (ts *Store) shardForNodeIDReadCharged(id types.NodeID, opens *storecontract.ReadTally) (store *BadgerStore, checkin func(), isArchive bool, err error) {
	return ts.routeNodeShard(id, routeRead, opens)
}

// routeNodeShard resolves id's shard; opens (nil: none) is charged each closed
// shard's open before it (READBUDGET step 6).
func (ts *Store) routeNodeShard(id types.NodeID, mode routeMode, opens *storecontract.ReadTally) (store *BadgerStore, checkin func(), isArchive bool, err error) {
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

	// Archive probe: pinned via checkoutArchive (Close closes refArchive; it
	// also lazy-opens via ensureRefArchive when only the catalog has the entry).
	ts.refArchiveGapHook()
	archive, archiveCheckin, err := ts.checkoutArchiveCharged(opens)
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
	if mode.skipsEmpty() {
		es = ts.readableOwner(es)
	}
	store, checkin, err = es.checkoutForReadCharged(ts, mode.readOnly(), opens)
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

// eventShardsToRead is eventShardSnapshot for a read of rows: a closed shard
// that holds none is neither opened nor checked out.
func (ts *Store) eventShardsToRead(depth ShardDepth) []*EventShard {
	shards := ts.eventShardSnapshot(depth)
	return slices.DeleteFunc(shards, func(es *EventShard) bool { return es.noRows.Load() })
}

// eventShardsWithLabel is eventShardsToRead for a read of one label's current
// members: a cold shard sealed with no node of the label is neither opened nor
// checked out (REFFANOUT; a reference label's nodes live on the reference shard).
func (ts *Store) eventShardsWithLabel(depth ShardDepth, token uint16) []*EventShard {
	return slices.DeleteFunc(ts.eventShardsToRead(depth), func(es *EventShard) bool { return es.sealedWithoutLabel(token) })
}

// eventShardsWithRelType is eventShardsWithLabel for one relationship type.
func (ts *Store) eventShardsWithRelType(depth ShardDepth, token uint16) []*EventShard {
	return slices.DeleteFunc(ts.eventShardsToRead(depth), func(es *EventShard) bool { return es.sealedWithoutRelType(token) })
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
		// ISO week 1 is the week containing January 4 (date arithmetic in
		// UTC, where every day has 24 hours).
		jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
		jan4Weekday := jan4.Weekday()
		if jan4Weekday == time.Sunday {
			jan4Weekday = 7
		}
		daysToMonday := int(time.Monday - jan4Weekday)
		monday := jan4.AddDate(0, 0, daysToMonday+(week-1)*7)
		return dayStart(monday.Year(), monday.Month(), monday.Day(), t.Location())
	case window >= 24*time.Hour:
		return dayStart(t.Year(), t.Month(), t.Day(), t.Location())
	default:
		return t.Truncate(window)
	}
}

// shardWindowEnd is the end of the shard window containing t: the next
// window's start. A daily or weekly window starts at local midnight, so its
// end counts calendar days, not 24-hour days: a local day is 23 or 25 hours
// long where daylight saving time begins or ends (DSTWIN; 24 h per day ended
// Berlin's 25 October 2026 window an hour early, and a rotation in that hour
// made a window ending before it started).
func shardWindowEnd(t time.Time, window time.Duration) time.Time {
	start := shardWindowStart(t, window)
	const day = 24 * time.Hour
	if window < day {
		return start.Add(window)
	}
	return dayStart(start.Year(), start.Month(), start.Day()+int(window/day), start.Location()).Add(window % day)
}

// dayStart is the first instant of the local calendar day y-m-d (d may
// overflow the month, as in time.Date). That is midnight, except where the
// clocks jump forward AT midnight (America/Santiago, Havana, Asuncion): the
// day has no 00:00, time.Date places it at 23:00 the evening before, and the
// day begins at the jump instead (CALENDAR: the evening's window ended an
// hour early, and a rotation in that hour made a window ending before it
// started, DSTWIN's failure).
func dayStart(y int, m time.Month, d int, loc *time.Location) time.Time {
	noon := time.Date(y, m, d, 12, 0, 0, 0, loc) // the day, its overflow resolved
	midnight := time.Date(noon.Year(), noon.Month(), noon.Day(), 0, 0, 0, 0, loc)
	if midnight.Day() != noon.Day() {
		_, jump := midnight.ZoneBounds()
		return jump
	}
	return midnight
}

// rotationWindow is the new hot shard's window for a rotation at now: from
// the boundary (now + 1 ms; the old shard keeps now's millisecond) to the end
// of the window holding the boundary, named after that window.
func rotationWindow(now time.Time, window time.Duration) (name string, start, end time.Time) {
	boundary := now.Truncate(time.Millisecond).Add(time.Millisecond)
	return shardWindowName(boundary, window), boundary, shardWindowEnd(boundary, window)
}
