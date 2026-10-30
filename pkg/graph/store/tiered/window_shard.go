package tiered

import (
	"fmt"
	"path/filepath"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Window shards made on demand (ADR-0013 step 3, MintAtValidFrom only). A
// valid-time ID can fall in a window that has no shard: a day the clock
// skipped, a day before the store existed, a day ahead. The write door that
// places such an ID makes that window's shard first, before it pins any shard
// (the lifecycle gate it takes waits for Close, which waits for every pin).
// Reads never make one: they route to the hot shard and answer not-found.

// ensureWindowShard makes the shard of the window holding raw's time when no
// shard covers it. Callers gate on the option. The unlocked look is the fast
// path of every create; the look under ts.mu makes one shard when creates of
// one new window race.
func (ts *Store) ensureWindowShard(raw snowflake.ID) error {
	created := snowflakepkg.Layout.CreatedAt(raw)
	ts.mu.RLock()
	covered := shardWindowFor(ts.shardIndex, created) != nil
	ts.mu.RUnlock()
	if covered {
		return nil
	}
	releaseLifecycle, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return err
	}
	defer releaseLifecycle()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if shardWindowFor(ts.shardIndex, created) != nil {
		return nil // another write made it
	}
	return ts.makeWindowShardLocked(created.In(ts.shardLoc))
}

// makeWindowShardLocked makes the shard of t's calendar window, cut to the gap
// between its neighbours (a shard from before the option, rotated mid-window,
// may cover part of it), saving the catalog before the topology changes. Its
// tier follows the clock: cold when it ends ColdAfter before the clock, else
// warm. Caller holds ts.mu.Lock.
func (ts *Store) makeWindowShardLocked(t time.Time) error {
	start, end := shardWindowStart(t, ts.shardWindow), shardWindowEnd(t, ts.shardWindow)
	for _, es := range ts.shardIndex {
		if !es.timeEnd.After(t) && es.timeEnd.After(start) {
			start = es.timeEnd
		}
		if es.timeStart.After(t) && es.timeStart.Before(end) {
			end = es.timeStart
		}
	}
	name := shardWindowName(t, ts.shardWindow)
	_, exists := ts.eventShards[name]
	if _, dropping := ts.droppingShards[name]; exists || dropping {
		name = fmt.Sprintf("%s-%d", name, start.UnixMilli())
	}
	dir := name
	if !ts.inMemory {
		dir = filepath.Join("events", name)
	}
	tier := TierWarm
	if ts.coldAfter > 0 && end.Before(ts.clock().Add(-ts.coldAfter)) {
		tier = TierCold
	}
	store, err := ts.openNewEventShard(name, dir)
	if err != nil {
		return err
	}
	snapshot := ts.catalog.snapshotShards()
	ts.catalog.AddShard(ShardEntry{Name: name, Kind: ShardEvent, Tier: tier, Path: dir, TimeStart: start, TimeEnd: end})
	if !ts.inMemory {
		if err := ts.catalog.Save(); err != nil {
			ts.catalog.restoreShards(snapshot)
			ts.discardNewEventShard(store, name, dir)
			return fmt.Errorf("graph: save catalog after making window shard %s: %w", name, err)
		}
	}
	es := &EventShard{name: name, store: store, tier: tier, timeStart: start, timeEnd: end, readOnly: true, path: dir}
	es.initTier(tier)
	ts.eventShards[name] = es
	ts.reindexShardsLocked()
	if tier == TierCold {
		// Open now; idle close or the cold cap closes it like a demoted shard.
		es.shardMu.Lock()
		es.countColdStoreLocked(ts)
		es.shardMu.Unlock()
		freeColdRelTemporalIndexes(store, name)
	}
	ts.handNewShardToLogScope(store, name)
	return nil
}

// ensureNodeWindowShards makes the window shards that creates of event nodes
// land on.
func (ts *Store) ensureNodeWindowShards(nodes ...*types.Node) error {
	if !ts.calendar {
		return nil
	}
	for _, n := range nodes {
		if ts.ontology.ClassifyByToken(n.PrimaryLabelToken().Value()) == ClassReference {
			continue
		}
		if err := ts.ensureWindowShard(n.ID().SnowflakeID()); err != nil {
			return err
		}
	}
	return nil
}

// ensureRelWindowShards makes the window shards that relationship creates
// placed by mint (RelsByMint: both endpoints on the reference shard) land on.
// Every other relationship lives with its start node.
func (ts *Store) ensureRelWindowShards(rels ...*types.Relationship) error {
	if !ts.calendar || !ts.relsByMint.enabled {
		return nil
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	var byMint []snowflake.ID
	for _, r := range rels {
		if ref.HasNodeID(r.StartNodeID().SnowflakeID()) && ref.HasNodeID(r.EndNodeID().SnowflakeID()) {
			byMint = append(byMint, r.ID().SnowflakeID())
		}
	}
	refCheckin()
	for _, id := range byMint {
		if err := ts.ensureWindowShard(id); err != nil {
			return err
		}
	}
	return nil
}
