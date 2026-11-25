package tiered

import (
	"errors"
	"os"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RHO-35: a relationship retention purge drops whole a rotated day shard that
// holds only the purged types' aged-out relationships (Config.RelsByMint:
// the entity nodes stay on the reference shard).

const (
	dropEventType uint16 = 5
	dropOtherType uint16 = 6
	dropLine             = types.Instant(1 << 50)
)

// relOnlyDayShard opens a RelsByMint store on dir with two reference nodes
// and rels of the given types between them on the hot shard, then rotates it
// out. It returns the store, the rotated shard's name and the rel IDs.
func relOnlyDayShard(t *testing.T, dir string, relTypes ...uint16) (*Store, types.NodeID, string, []types.RelID) {
	t.Helper()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg(dir, true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	day := hotShardName(ts)
	var ids []types.RelID
	for _, tok := range relTypes {
		id := types.RelID(rg.Generate())
		putMintRel(t, ts, id, tok, a, b)
		ids = append(ids, id)
	}
	// The rotated window must end after the last mint: wait for the clock's
	// next millisecond (no fixed sleep).
	last := storeutil.SnowflakeInstant(ids[len(ids)-1].SnowflakeID())
	for types.Instant(time.Now().UnixMilli()) <= last {
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	if got := shardRelCount(t, ts, day); got != len(ids) {
		t.Fatalf("setup: day shard %s holds %d rels, want %d", day, got, len(ids))
	}
	return ts, a, day, ids
}

func wantRelsGone(t *testing.T, ts *Store, a types.NodeID, ids ...types.RelID) {
	t.Helper()
	for _, id := range ids {
		if _, err := ts.GetRelationship(id); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("rel %d survived: %v", id, err)
		}
	}
	if out, err := ts.OutgoingRelationships(a, 0); err != nil || len(out) != 0 {
		t.Fatalf("Outgoing(a) = %d rels, %v; want none", len(out), err)
	}
}

// TestPurgeDropsAWholeDayShard: the drop commits the catalog (entry out,
// directory listed in Dropped) and is killed before the directory goes; the
// next open removes the directory, and no row of the day is readable.
func TestPurgeDropsAWholeDayShard(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, a, day, ids := relOnlyDayShard(t, dir, dropEventType, dropEventType)
	shardDir := ts.shardDir(ts.eventShards[day].path)

	ts.dropRemoveHook = func() { panic("kill") }
	func() {
		defer func() {
			if r := recover(); r != "kill" {
				t.Fatalf("drop did not reach the removal: %v", r)
			}
		}()
		_, _ = ts.DropRelShardsBefore([]uint16{dropEventType}, dropLine)
	}()
	ts.dropRemoveHook = nil
	if _, err := os.Stat(shardDir); err != nil {
		t.Fatalf("kill left no directory to finish: %v", err)
	}
	if _, ok := ts.catalog.GetShard(day); ok {
		t.Fatal("committed drop is still cataloged")
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	ts = openMintStore(t, mintCfg(dir, true))
	if _, err := os.Stat(shardDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reopen left the dropped directory: %v", err)
	}
	if d := ts.catalog.droppedShards(); len(d) != 0 {
		t.Fatalf("catalog still lists dropped shards %v", d)
	}
	if _, ok := ts.catalog.GetShard(day); ok {
		t.Fatal("dropped shard cataloged after reopen")
	}
	wantRelsGone(t, ts, a, ids...)
	res, err := ts.DropRelShardsBefore([]uint16{dropEventType}, dropLine)
	if err != nil || res.RelsPurged != 0 {
		t.Fatalf("second drop = %+v, %v; want nothing", res, err)
	}

	// Without a kill the directory goes at once and the count is reported.
	ts2, a2, day2, ids2 := relOnlyDayShard(t, t.TempDir(), dropEventType)
	dir2 := ts2.shardDir(ts2.eventShards[day2].path)
	res, err = ts2.DropRelShardsBefore([]uint16{dropEventType}, dropLine)
	if err != nil || res.RelsPurged != 1 {
		t.Fatalf("drop = %+v, %v; want 1 rel", res, err)
	}
	if _, err := os.Stat(dir2); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drop left its directory: %v", err)
	}
	if d := ts2.catalog.droppedShards(); len(d) != 0 {
		t.Fatalf("finished drop still listed: %v", d)
	}
	wantRelsGone(t, ts2, a2, ids2...)
}

// TestRelShardDropRefusedReadOnly: a read-only store refuses the drop with
// ErrReadOnly and keeps the shard.
func TestRelShardDropRefusedReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, _, day, _ := relOnlyDayShard(t, dir, dropEventType)
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := mintCfg(dir, true)
	cfg.ReadOnly = true
	ro := openMintStore(t, cfg)
	if _, err := ro.DropRelShardsBefore([]uint16{dropEventType}, dropLine); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only drop = %v, want ErrReadOnly", err)
	}
	if _, ok := ro.catalog.GetShard(day); !ok {
		t.Fatal("read-only drop removed the shard")
	}
}

// TestRelShardDropKeepsShardWithAnotherType: a day shard that also holds a
// relationship of a type outside the purge is not dropped; the row purge
// removes the purged type's rows and the other type's stays.
func TestRelShardDropKeepsShardWithAnotherType(t *testing.T) {
	t.Parallel()
	ts, a, day, ids := relOnlyDayShard(t, t.TempDir(), dropEventType, dropOtherType)
	res, err := ts.DropRelShardsBefore([]uint16{dropEventType}, dropLine)
	if err != nil || res.RelsPurged != 0 {
		t.Fatalf("drop = %+v, %v; want nothing dropped", res, err)
	}
	if _, ok := ts.catalog.GetShard(day); !ok {
		t.Fatal("shard with another type was dropped")
	}
	for {
		r, err := ts.PurgeRelsByTypeBefore(dropEventType, dropLine, 256)
		if err != nil {
			t.Fatal(err)
		}
		if !r.More {
			break
		}
	}
	if _, err := ts.GetRelationship(ids[0]); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("purged type's rel survived: %v", err)
	}
	if _, err := ts.GetRelationship(ids[1]); err != nil {
		t.Fatalf("other type's rel lost: %v", err)
	}
	if out, err := ts.OutgoingRelationships(a, 0); err != nil || len(out) != 1 {
		t.Fatalf("Outgoing(a) = %d, %v; want the other type's 1", len(out), err)
	}
	// Both types named: the shard now holds only rels of the purge and goes.
	if res, err := ts.DropRelShardsBefore([]uint16{dropEventType, dropOtherType}, dropLine); err != nil || res.RelsPurged != 1 {
		t.Fatalf("two-type drop = %+v, %v; want 1", res, err)
	}
	if _, ok := ts.catalog.GetShard(day); ok {
		t.Fatal("shard of the purged types alone was kept")
	}
}
