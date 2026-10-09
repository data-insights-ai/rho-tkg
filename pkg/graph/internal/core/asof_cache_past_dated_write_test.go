package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The as-of DocValues column cache (docvalues_asof_cache.go) keys a label's
// members at a pin P. A write stamped t <= P changes that belief, so the cache
// must be invalidated whenever such a write lands — AFTER it lands, or a build
// racing the write caches the pre-write state under the new epoch. These tests
// break the cache from both sides: the primary's backfill doors (R14) and the
// replica's delete applies (R13).

const pastDatedLabel = "Asset"

// pastDatedBackend opens one Core per backend the as-of cache serves (the cache
// is Core-level, so every backend, including the partitioned ones).
type pastDatedBackend struct {
	name string
	// open returns a fresh graph. changeLog asks for a change-log-emitting
	// primary; replica asks for a read-only replica fed by source.
	open func(t *testing.T, cfg Config, changeLog bool) *Core
}

func pastDatedBackends() []pastDatedBackend {
	return []pastDatedBackend{
		{name: "memory", open: func(t *testing.T, cfg Config, changeLog bool) *Core {
			if changeLog {
				cfg.Store = memory.New(memory.WithChangeLog())
			} else {
				cfg.Store = memory.New()
			}
			return openPastDated(t, cfg)
		}},
		{name: "badger", open: func(t *testing.T, cfg Config, changeLog bool) *Core {
			cfg.BadgerInMemory = true
			cfg.ChangeLog = changeLog
			cfg.SyncWrites = changeLog
			return openPastDated(t, cfg)
		}},
		{name: "sharded", open: func(t *testing.T, cfg Config, changeLog bool) *Core {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: changeLog})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			cfg.Store = st
			return openPastDated(t, cfg)
		}},
		{name: "tiered", open: func(t *testing.T, cfg Config, changeLog bool) *Core {
			st, err := tiered.New(tiered.Config{
				InMemory:      true,
				RefLabels:     []string{"Case"},
				ShardWindow:   7 * 24 * 60 * 60 * 1e9,
				FlushInterval: 1<<63 - 1,
				ChangeLog:     changeLog,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			cfg.Store = st
			return openPastDated(t, cfg)
		}},
	}
}

func openPastDated(t *testing.T, cfg Config) *Core {
	t.Helper()
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// asOfMembers reads the label's members at pin through the cached columnar door
// (ForEachDocValuesAsOf) and the cached count door (CountByLabelAt), fails if
// they disagree, and returns the sorted member IDs.
func asOfMembers(t *testing.T, g *Core, pin types.Instant) []types.NodeID {
	t.Helper()
	var ids []types.NodeID
	_, ok, err := g.Nodes.ForEachDocValuesAsOf(pastDatedLabel, []string{"k"}, pin, func(id types.NodeID, _ []any, _ []bool) bool {
		ids = append(ids, id)
		return true
	})
	if err != nil || !ok {
		t.Fatalf("ForEachDocValuesAsOf(%d): ok=%v err=%v", pin, ok, err)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	n, err := g.Nodes.CountByLabelAt(pastDatedLabel, storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		t.Fatalf("CountByLabelAt(%d): %v", pin, err)
	}
	if n != len(ids) {
		t.Fatalf("CountByLabelAt(%d) = %d, columns hold %d members %v", pin, n, len(ids), ids)
	}
	return ids
}

func assertMembers(t *testing.T, what string, got []types.NodeID, want ...types.NodeID) {
	t.Helper()
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s: members %v, want exactly %v", what, got, want)
	}
}

// warmAsOf builds and caches the column at pin, then proves the next read is a
// cache hit (same pointer) so the test really starts from a warm cache.
func warmAsOf(t *testing.T, g *Core, pin types.Instant) {
	t.Helper()
	c1, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
	if err != nil {
		t.Fatalf("warm build: %v", err)
	}
	c2, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
	if err != nil {
		t.Fatalf("warm rebuild: %v", err)
	}
	if c1 != c2 {
		t.Fatalf("pin %d did not cache (label column not cacheable?)", pin)
	}
}

// pastDatedNodeDoor is one primary create door that honors a caller
// transaction instant, run with t as the backfilled TxFrom. It returns the
// created node's ID.
type pastDatedNodeDoor struct {
	name string
	run  func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID
}

func backfillProps(k int64, txFrom types.Instant) map[string]any {
	return map[string]any{"k": k, "tkg_tx_from": txFrom}
}

func pastDatedNodeDoors() []pastDatedNodeDoor {
	ctx := context.Background()
	return []pastDatedNodeDoor{
		{name: "Nodes.AddWithTx", run: func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID {
			n, err := g.Nodes.AddWithTx(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)}, txFrom)
			if err != nil {
				t.Fatalf("AddWithTx: %v", err)
			}
			return n.ID()
		}},
		{name: "Nodes.Import", run: func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID {
			n, err := g.Nodes.Import(ctx, g.nextNodeID(), []string{pastDatedLabel}, backfillProps(2, txFrom))
			if err != nil {
				t.Fatalf("Import: %v", err)
			}
			return n.ID()
		}},
		{name: "GraphTx.AddNode", run: func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID {
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			n, err := tx.AddNode([]string{pastDatedLabel}, backfillProps(2, txFrom))
			if err != nil {
				t.Fatalf("tx.AddNode: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			return n.ID()
		}},
		{name: "GraphTx.AddNodes", run: func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID {
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			ns, err := tx.AddNodes([]string{pastDatedLabel}, []map[string]any{backfillProps(2, txFrom)})
			if err != nil || len(ns) != 1 {
				t.Fatalf("tx.AddNodes: %d nodes, %v", len(ns), err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			return ns[0].ID()
		}},
		{name: "Batch.AddNode+Execute", run: func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID {
			b, err := NewBatchBuilder(g)
			if err != nil {
				t.Fatalf("NewBatchBuilder: %v", err)
			}
			n, err := b.AddNode([]string{pastDatedLabel}, backfillProps(2, txFrom))
			if err != nil {
				t.Fatalf("batch AddNode: %v", err)
			}
			if _, err := b.Execute(); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			return n.ID()
		}},
		{name: "Ingest.Session(sync)", run: func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID {
			return ingestBackfilledNode(t, g, IngestOptions{Sync: true}, txFrom)
		}},
		{name: "Ingest.Session(concurrent)", run: func(t *testing.T, g *Core, txFrom types.Instant) types.NodeID {
			return ingestBackfilledNode(t, g, IngestOptions{Concurrent: true}, txFrom)
		}},
	}
}

func ingestBackfilledNode(t *testing.T, g *Core, opts IngestOptions, txFrom types.Instant) types.NodeID {
	t.Helper()
	s, err := g.Ingest.NewSession(opts)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	n, err := s.AddNode([]string{pastDatedLabel}, backfillProps(2, txFrom))
	if err != nil {
		t.Fatalf("session AddNode: %v", err)
	}
	if _, err := s.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("session Close: %v", err)
	}
	return n.ID()
}

// TestAsOfCache_PrimaryCacheStale (R14) — a backfilled create at t < P must be
// visible at a pin P whose column a reader built while the door was running.
//
// Faulty implementation caught: the as-of cache bump happens BEFORE the store
// write (resolveBackfillTxFrom bumped at the gate; the batch and ingest queue
// doors bumped at QUEUE time, long before Execute/Submit wrote). A column build
// that runs between that bump and the write reads the bumped epoch, does not see
// the row, and is cached as current — the pin P then misses the node forever.
// The test seam backfillGateHook runs that build exactly in the window: after
// the gate honored t, before any store write. Also caught: a door that lost its
// bump entirely in the move (same stale read, no interleaving needed).
func TestAsOfCache_PrimaryCacheStale(t *testing.T) {
	ctx := context.Background()
	for _, be := range pastDatedBackends() {
		for _, door := range pastDatedNodeDoors() {
			t.Run(be.name+"/"+door.name, func(t *testing.T) {
				g := be.open(t, Config{AllowTxBackfill: true}, false)
				a, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1)})
				if err != nil {
					t.Fatalf("seed: %v", err)
				}
				ta := txFromStamp(t, a.Temporal())
				// Leave room between the seed and the pin for the backfilled stamp.
				if _, err := g.Temporal.AdvanceClock(ta + 1000); err != nil {
					t.Fatalf("AdvanceClock: %v", err)
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				backfill := ta + 500 // seed < backfill < pin
				warmAsOf(t, g, pin)
				assertMembers(t, "warm pin", asOfMembers(t, g, pin), a.ID())

				builds := 0
				g.backfillGateHook = func() {
					// A concurrent reader builds the pin's column between the gate
					// and the store write (its own goroutine, as a dashboard would).
					done := make(chan error, 1)
					go func() {
						_, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
						done <- err
					}()
					if err := <-done; err != nil {
						t.Errorf("interleaved build: %v", err)
					}
					builds++
				}
				id := door.run(t, g, backfill)
				g.backfillGateHook = nil
				if builds == 0 {
					t.Fatal("the door never reached the backfill gate (hook did not run)")
				}

				assertMembers(t, "pin after the backfill", asOfMembers(t, g, pin), a.ID(), id)
				// Counterparts: the backfilled stamp is exact — absent one tick
				// before it, present at it.
				assertMembers(t, "pin backfill-1", asOfMembers(t, g, backfill-1), a.ID())
				assertMembers(t, "pin backfill", asOfMembers(t, g, backfill), a.ID(), id)
			})
		}
	}
}

// TestAsOfCache_RelBackfillDoorsReportPastDatedWrite (R14, relationship doors) —
// the as-of cache holds NODE label columns only (asOfCacheKey is a node label
// token), so a backfilled relationship cannot make a cached column stale and no
// stale read is observable. What stays observable is the contract that every
// backfill door reports a past-dated write through the as-of gen
// (DocValuesSnapshotAsOf's gen = the cache epoch).
//
// Faulty implementation caught: moving the bump out of resolveBackfillTxFrom
// drops it from a relationship door (gen unchanged after a backfilled create).
func TestAsOfCache_RelBackfillDoorsReportPastDatedWrite(t *testing.T) {
	ctx := context.Background()
	doors := []struct {
		name string
		run  func(g *Core, s, e *types.Node, txFrom types.Instant) error
	}{
		{"Rels.AddWithTx", func(g *Core, s, e *types.Node, tf types.Instant) error {
			_, err := g.Rels.AddWithTx(ctx, "R", s, e, nil, tf)
			return err
		}},
		{"Rels.AddByID", func(g *Core, s, e *types.Node, tf types.Instant) error {
			_, err := g.Rels.AddByID(ctx, "R", s.ID(), e.ID(), map[string]any{"tkg_tx_from": tf})
			return err
		}},
		{"Rels.AddByIDIfAbsent", func(g *Core, s, e *types.Node, tf types.Instant) error {
			_, _, err := g.Rels.AddByIDIfAbsent(ctx, "R", s.ID(), e.ID(), map[string]any{"tkg_tx_from": tf})
			return err
		}},
		{"Rels.Import", func(g *Core, s, e *types.Node, tf types.Instant) error {
			_, err := g.Rels.Import(ctx, g.nextRelID(), "R", s, e, map[string]any{"tkg_tx_from": tf})
			return err
		}},
		{"GraphTx.AddRelationships", func(g *Core, s, e *types.Node, tf types.Instant) error {
			tx, err := g.BeginTx()
			if err != nil {
				return err
			}
			if _, err := tx.AddRelationships("R", []RelCreate{{StartID: s.ID(), EndID: e.ID(), Props: map[string]any{"tkg_tx_from": tf}}}); err != nil {
				return err
			}
			return tx.Commit()
		}},
		{"Batch.AddRelationship+Execute", func(g *Core, s, e *types.Node, tf types.Instant) error {
			b, err := NewBatchBuilder(g)
			if err != nil {
				return err
			}
			if _, err := b.AddRelationship("R", s, e, map[string]any{"tkg_tx_from": tf}); err != nil {
				return err
			}
			_, err = b.Execute()
			return err
		}},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			g := openPastDated(t, Config{AllowTxBackfill: true})
			s, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			e, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			pin := txFromStamp(t, e.Temporal())
			_, genBefore, ok, err := g.Nodes.DocValuesSnapshotAsOf(pastDatedLabel, []string{"k"}, pin)
			if err != nil || !ok {
				t.Fatalf("DocValuesSnapshotAsOf: ok=%v err=%v", ok, err)
			}
			if err := d.run(g, s, e, pin); err != nil {
				t.Fatalf("door: %v", err)
			}
			_, genAfter, _, err := g.Nodes.DocValuesSnapshotAsOf(pastDatedLabel, []string{"k"}, pin)
			if err != nil {
				t.Fatalf("DocValuesSnapshotAsOf after: %v", err)
			}
			if genAfter == genBefore {
				t.Fatalf("a backfilled relationship create left the as-of gen at %d — the door no longer reports its past-dated write", genBefore)
			}
		})
	}
}

// replicaPair is a change-log primary and a read-only replica of the same backend.
func replicaPair(t *testing.T, be pastDatedBackend) (primary, replica *Core) {
	t.Helper()
	primary = be.open(t, Config{SnowflakeNodeID: 0}, true)
	replica = be.open(t, Config{SnowflakeNodeID: 1, ReadOnlyReplica: true, ReplicationSource: primary.Repl}, false)
	return primary, replica
}

func changeFeed(t *testing.T, g *Core) []storepkg.ChangeRecord {
	t.Helper()
	recs, err := g.Repl.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	return recs
}

// splitAtDelete returns the records before the delete of id (tag), the delete
// record itself, and fails if the feed carries no such record.
func splitAtDelete(t *testing.T, recs []storepkg.ChangeRecord, tag storepkg.ChangeTag, id int64) ([]storepkg.ChangeRecord, storepkg.ChangeRecord) {
	t.Helper()
	for i, rec := range recs {
		if rec.Tag != tag {
			continue
		}
		var got int64
		switch tag {
		case storepkg.ChangeNodeDelete:
			b, err := storeutil.DecodeNodeDelete(rec.Payload)
			if err != nil {
				t.Fatalf("DecodeNodeDelete: %v", err)
			}
			got = b.ID
		case storepkg.ChangeRelDelete:
			b, err := storeutil.DecodeRelDelete(rec.Payload)
			if err != nil {
				t.Fatalf("DecodeRelDelete: %v", err)
			}
			got = b.ID
		}
		if got == id {
			return recs[:i], rec
		}
	}
	t.Fatalf("no %v record for %d in the feed", tag, id)
	return nil, storepkg.ChangeRecord{}
}

// backdateNodeDelete rewrites a with-history node-delete record's tombstone to
// TxTo = DeletedAt = at — the record a DeleteWithTx(at) on the primary emits.
// TxTo/DeletedAt are not hashed, so the record stays verifiable.
func backdateNodeDelete(t *testing.T, rec storepkg.ChangeRecord, at types.Instant) storepkg.ChangeRecord {
	t.Helper()
	b, err := storeutil.DecodeNodeDelete(rec.Payload)
	if err != nil {
		t.Fatalf("DecodeNodeDelete: %v", err)
	}
	if !b.WithHistory || b.Tombstone == nil {
		t.Fatalf("node delete record carries no tombstone (WithHistory=%v)", b.WithHistory)
	}
	b.Tombstone.TxTo = int64(at)
	b.Tombstone.DeletedAt = int64(at)
	rec.Payload = mustMarshalChangePayload(t, b)
	return rec
}

func backdateRelDelete(t *testing.T, rec storepkg.ChangeRecord, at types.Instant) storepkg.ChangeRecord {
	t.Helper()
	b, err := storeutil.DecodeRelDelete(rec.Payload)
	if err != nil {
		t.Fatalf("DecodeRelDelete: %v", err)
	}
	if !b.WithHistory || b.Tombstone == nil {
		t.Fatalf("rel delete record carries no tombstone (WithHistory=%v)", b.WithHistory)
	}
	b.Tombstone.TxTo = int64(at)
	b.Tombstone.DeletedAt = int64(at)
	rec.Payload = mustMarshalChangePayload(t, b)
	return rec
}

func applyAll(t *testing.T, g *Core, recs []storepkg.ChangeRecord) {
	t.Helper()
	for _, rec := range recs {
		if err := g.Repl.ApplyChange(rec); err != nil {
			t.Fatalf("ApplyChange(lsn %d, tag %v): %v", rec.LSN, rec.Tag, err)
		}
	}
}

// TestAsOfCache_ReplicaDeleteStaleCache (R13) — a replica that cached a label's
// column at pin P must drop a node whose delete record ends its belief at t <= P.
//
// The primary seeds a (TxFrom ta), advances its clock, seeds b (TxFrom tb >> ta)
// and deletes a. The replica applies everything before the delete, warms P, then
// applies the delete record with the tombstone rewritten to the case's instant.
//
// Faulty implementations caught, per case:
//   - below-high-water: the delete apply never reports its tombstone to the
//     cache (applyNodeDeleteLocked had no noteAppliedTxFrom; noteAppliedTx read
//     TxFrom only, and a tombstone's TxFrom is the version start, not its end).
//   - above-high-water: the apply reports the tombstone but the detector judges
//     it only against the highest APPLIED TxFrom (tb), never against the pins it
//     has cached: t > tb passes as a "forward" write although t <= P.
//   - hard-delete: a no-history delete (the primary's rollback/cleanup removal)
//     removes every version of the node and never reports it.
func TestAsOfCache_ReplicaDeleteStaleCache(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		// at/pin derive the tombstone instant and the warmed pin from the seeds.
		at, pin func(ta, tb types.Instant) types.Instant
		hard    bool
	}{
		{
			name: "below-high-water",
			at:   func(ta, _ types.Instant) types.Instant { return ta + 500 },
			pin:  func(_, tb types.Instant) types.Instant { return tb },
		},
		{
			name: "above-high-water",
			at:   func(_, tb types.Instant) types.Instant { return tb + 5 },
			pin:  func(_, tb types.Instant) types.Instant { return tb + 50 },
		},
		{
			name: "hard-delete",
			pin:  func(_, tb types.Instant) types.Instant { return tb },
			hard: true,
		},
	}
	for _, be := range pastDatedBackends() {
		for _, tc := range cases {
			t.Run(be.name+"/"+tc.name, func(t *testing.T) {
				primary, replica := replicaPair(t, be)
				a, err := primary.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1)})
				if err != nil {
					t.Fatalf("seed a: %v", err)
				}
				ta := txFromStamp(t, a.Temporal())
				if _, err := primary.Temporal.AdvanceClock(ta + 1000); err != nil {
					t.Fatalf("AdvanceClock: %v", err)
				}
				b, err := primary.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
				if err != nil {
					t.Fatalf("seed b: %v", err)
				}
				tb := txFromStamp(t, b.Temporal())
				if err := primary.Nodes.Delete(ctx, a.ID()); err != nil {
					t.Fatalf("Delete a: %v", err)
				}
				before, del := splitAtDelete(t, changeFeed(t, primary), storepkg.ChangeNodeDelete, int64(a.ID().SnowflakeID()))
				if tc.hard {
					del.Payload = mustMarshalChangePayload(t, storeutil.NodeDeleteBody{ID: int64(a.ID().SnowflakeID())})
				} else {
					del = backdateNodeDelete(t, del, tc.at(ta, tb))
				}

				applyAll(t, replica, before)
				pin := tc.pin(ta, tb)
				warmAsOf(t, replica, pin)
				assertMembers(t, "replica warm pin", asOfMembers(t, replica, pin), a.ID(), b.ID())

				applyAll(t, replica, []storepkg.ChangeRecord{del})

				assertMembers(t, "replica pin after the delete", asOfMembers(t, replica, pin), b.ID())
				if !tc.hard {
					// Counterpart: one tick before the tombstone the node is
					// still believed (the delete is exact, not over-dropped).
					at := tc.at(ta, tb)
					want := []types.NodeID{a.ID()}
					if at-1 >= tb {
						want = append(want, b.ID())
					}
					assertMembers(t, "replica pin tombstone-1", asOfMembers(t, replica, at-1), want...)
				}
			})
		}
	}
}

// TestAsOfCache_ReplicaRelDeleteFeedsDetector (R13, relationships) — rel deletes
// cannot stale a cached column (the cache holds node label columns only; the
// key is a node label token), so there is no stale read to observe. The
// replica's past-dated detector is nevertheless fed by every applied rel stamp
// (applyRelPutLocked, applyForeignIncomingLocked, applyRelHistoryVersionLocked),
// so the rel-delete apply must feed it the same way.
//
// Faulty implementation caught: applyRelDeleteLocked skips the detector its
// rel-put sibling feeds — a rel tombstone below the applied high-water mark
// leaves the as-of epoch unchanged.
func TestAsOfCache_ReplicaRelDeleteFeedsDetector(t *testing.T) {
	ctx := context.Background()
	be := pastDatedBackends()[0]
	primary, replica := replicaPair(t, be)
	s, err := primary.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1)})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	e, err := primary.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	r, err := primary.Rels.Add(ctx, "R", s, e, nil)
	if err != nil {
		t.Fatalf("rel: %v", err)
	}
	tr := txFromStamp(t, r.Temporal())
	if _, err := primary.Temporal.AdvanceClock(tr + 1000); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	// A later node raises the applied high-water mark above the rel tombstone.
	if _, err := primary.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(3)}); err != nil {
		t.Fatalf("late node: %v", err)
	}
	if err := primary.Rels.Delete(ctx, r.ID()); err != nil {
		t.Fatalf("Delete rel: %v", err)
	}
	before, del := splitAtDelete(t, changeFeed(t, primary), storepkg.ChangeRelDelete, int64(r.ID().SnowflakeID()))
	del = backdateRelDelete(t, del, tr+1)

	applyAll(t, replica, before)
	epoch := replica.asOfColumns.currentEpoch()
	applyAll(t, replica, []storepkg.ChangeRecord{del})
	if replica.asOfColumns.currentEpoch() == epoch {
		t.Fatalf("a rel tombstone (TxTo %d) below the applied high-water mark left the as-of epoch at %d", tr+1, epoch)
	}
	if _, err := replica.store.GetRelationship(r.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("rel still current on the replica after the delete apply: %v", err)
	}
}
