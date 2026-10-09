package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// hasHistoryBackend opens one backend over dir (reopenable backends reopen the
// same data). flush, when non-nil, drains the store's write buffer.
type hasHistoryBackend struct {
	name     string
	reopen   bool
	open     func(t *testing.T, dir string) (storepkg.Store, func() error)
	tieredRf bool // "Ref" routes to the tiered reference shard
}

func hasHistoryBackends() []hasHistoryBackend {
	return []hasHistoryBackend{
		{name: "memory", open: func(t *testing.T, _ string) (storepkg.Store, func() error) {
			return memory.New(), nil
		}},
		{name: "badger-disk", reopen: true, open: func(t *testing.T, dir string) (storepkg.Store, func() error) {
			// FlushInterval 1h: writes stay in the pending buffer until the test
			// flushes, so the set is built and probed with unflushed history.
			st, err := badger.New(badger.Config{Dir: dir, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return st, st.Flush
		}},
		{name: "tiered-disk", reopen: true, tieredRf: true, open: func(t *testing.T, dir string) (storepkg.Store, func() error) {
			st, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return st, nil
		}},
		{name: "sharded-disk", reopen: true, open: func(t *testing.T, dir string) (storepkg.Store, func() error) {
			st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			return st, nil
		}},
	}
}

func hasHistoryGraph(t *testing.T, st storepkg.Store) *graphpkg.Graph {
	t.Helper()
	g, err := graphpkg.New(graphpkg.Config{
		Store:               st,
		AllowRetentionPurge: true,
		AllowReset:          true,
		AllowExactErasure:   true,
		Validation:          graphpkg.ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// assertHasHistoryAgrees checks HasHistory against len(History) > 0 for every
// id, through the graph door AND the store capability directly (a core that
// fell back to History would hide a wrong store answer).
func assertHasHistoryAgrees(t *testing.T, step string, g *graphpkg.Graph, st storepkg.Store, nodes []types.NodeID, rels []types.RelID) {
	t.Helper()
	presence, ok := st.(storepkg.HistoryPresenceCapability)
	if !ok {
		t.Fatalf("%s: store %T lacks HistoryPresenceCapability", step, st)
	}
	for _, id := range nodes {
		h, herr := g.Nodes().History(id)
		got, err := g.Nodes().HasHistory(id)
		direct, derr := presence.HasNodeHistory(id)
		if (herr != nil) != (err != nil) || (herr != nil) != (derr != nil) {
			t.Fatalf("%s: node %d errors: History %v, HasHistory %v, store %v", step, id, herr, err, derr)
		}
		if want := len(h) > 0; got != want || direct != want {
			t.Fatalf("%s: node %d HasHistory=%v store=%v, History has %d rows", step, id, got, direct, len(h))
		}
	}
	for _, id := range rels {
		h, herr := g.Rels().History(id)
		got, err := g.Rels().HasHistory(id)
		direct, derr := presence.HasRelHistory(id)
		if (herr != nil) != (err != nil) || (herr != nil) != (derr != nil) {
			t.Fatalf("%s: rel %d errors: History %v, HasHistory %v, store %v", step, id, herr, err, derr)
		}
		if want := len(h) > 0; got != want || direct != want {
			t.Fatalf("%s: rel %d HasHistory=%v store=%v, History has %d rows", step, id, got, direct, len(h))
		}
	}
}

func hasHistorySeeds() []int64 {
	if s := os.Getenv("RHO_TKG_HAS_HISTORY_SEED"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return []int64{n}
		}
	}
	return []int64{1, 2, 3}
}

// TestHasHistoryDifferential is handover acceptance test 3: randomized create /
// update / cascade / close / label / delete / tx-rollback / tx-commit /
// compaction / retention purge / exact erasure / Clear / flush / reopen
// sequences; after every step HasHistory(id) == (len(History(id)) > 0) for
// every node and relationship ever created (deleted, purged and erased ones
// included) plus IDs that never existed. Faults it catches: a set that is not
// maintained when a history key is deleted (rollback, purge, erasure, Clear), a
// build or probe that skips the pending write buffer (reopen, then writes, then
// the first call builds), a first build over a buffer holding a SET and a
// DELETE of different versions of one entity (the burst after every reopen and
// Clear), a store answer the core fallback would hide.
func TestHasHistoryDifferential(t *testing.T) {
	steps := 160
	if testing.Short() {
		steps = 60
	}
	for _, b := range hasHistoryBackends() {
		for _, seed := range hasHistorySeeds() {
			t.Run(fmt.Sprintf("%s/seed=%d", b.name, seed), func(t *testing.T) {
				runHasHistoryDifferential(t, b, seed, steps)
			})
		}
	}
}

func runHasHistoryDifferential(t *testing.T, b hasHistoryBackend, seed int64, steps int) {
	ctx := context.Background()
	r := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test sequence
	dir := t.TempDir()
	st, flush := b.open(t, dir)
	g := hasHistoryGraph(t, st)
	t.Cleanup(func() { _ = g.Close() })

	var nodes []types.NodeID
	var rels []types.RelID
	// IDs that never existed: a plain miss must be false, not an error.
	ghostNodes := []types.NodeID{types.NodeID(1 << 40), types.NodeID(1<<40 + 7)}
	ghostRels := []types.RelID{types.RelID(1 << 41)}
	ok := map[string]int{}
	pick := func(n int) int { return r.Intn(n) }
	labels := func() []string {
		switch r.Intn(4) {
		case 0:
			if b.tieredRf {
				return []string{"Ref"}
			}
			return []string{"Event"}
		case 1:
			return []string{"Temp"}
		default:
			return []string{"Event"}
		}
	}
	addNode := func() {
		n, err := g.Nodes().Add(ctx, labels(), map[string]any{"v": int64(r.Intn(100))})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		nodes = append(nodes, n.ID())
	}
	addRel := func() {
		if len(nodes) < 2 {
			return
		}
		rel, err := g.Rels().AddByID(ctx, "LINK", nodes[pick(len(nodes))], nodes[pick(len(nodes))], map[string]any{"w": int64(r.Intn(100))})
		if err == nil {
			rels = append(rels, rel.ID())
			ok["addRel"]++
		}
	}
	// burst buffers several history ops per entity before the next check: an
	// update that flushes (older version committed), two more updates (SETs of
	// newer versions, still buffered), then a compaction keeping one history
	// version (DELETEs of the committed older versions). Run right after a
	// reopen or a Clear, the next check is the first call and builds the set
	// over a buffer holding a SET and a DELETE of different versions per ID.
	burst := func() {
		for i := 0; i < 4 && len(nodes) > 0; i++ {
			id := nodes[pick(len(nodes))]
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"v": int64(1000 + i)}); err != nil {
				continue
			}
			ok["burstUpdate"]++
			if flush != nil {
				if err := flush(); err != nil {
					t.Fatalf("burst flush: %v", err)
				}
			}
			for j := 0; j < 2; j++ {
				if _, err := g.Nodes().Update(ctx, id, map[string]any{"v": int64(2000 + 10*i + j)}); err != nil {
					t.Fatalf("burst update: %v", err)
				}
			}
		}
		for i := 0; i < 4 && len(rels) > 0; i++ {
			id := rels[pick(len(rels))]
			if _, err := g.Rels().Update(ctx, id, map[string]any{"w": int64(1000 + i)}); err != nil {
				continue
			}
			if flush != nil {
				if err := flush(); err != nil {
					t.Fatalf("burst flush: %v", err)
				}
			}
			for j := 0; j < 2; j++ {
				if _, err := g.Rels().Update(ctx, id, map[string]any{"w": int64(2000 + 10*i + j)}); err != nil {
					t.Fatalf("burst update: %v", err)
				}
			}
		}
		keep := adminpkg.RetentionPolicy{KeepVersions: 1}
		for _, err := range []error{
			func() error { _, err := g.Admin().CompactHistoryNodes(ctx, keep); return err }(),
			func() error { _, err := g.Admin().CompactHistoryRels(ctx, keep); return err }(),
		} {
			if err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Fatalf("burst compact: %v", err)
			}
		}
	}
	for range 6 {
		addNode()
	}
	for range 4 {
		addRel()
	}
	assertHasHistoryAgrees(t, "seeded", g, st, append(nodes, ghostNodes...), append(rels, ghostRels...))

	for step := 0; step < steps; step++ {
		op := r.Intn(100)
		name := ""
		check := true
		switch {
		case op < 12:
			name = "addNode"
			addNode()
		case op < 22:
			name = "addRel"
			addRel()
		case op < 32:
			name = "updateNode"
			if _, err := g.Nodes().Update(ctx, nodes[pick(len(nodes))], map[string]any{"v": int64(r.Intn(100))}); err == nil {
				ok[name]++
			}
		case op < 40:
			name = "updateRel"
			if len(rels) > 0 {
				if _, err := g.Rels().Update(ctx, rels[pick(len(rels))], map[string]any{"w": int64(r.Intn(100))}); err == nil {
					ok[name]++
				}
			}
		case op < 46:
			name = "cascadeNode"
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			vf := now + types.Instant(r.Intn(5000))
			vt := types.Instant(0)
			if r.Intn(2) == 0 {
				vt = vf + types.Instant(1+r.Intn(5000))
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, nodes[pick(len(nodes))], vf, vt, map[string]any{"v": int64(-1)}); err == nil {
				ok[name]++
			}
		case op < 52:
			name = "cascadeRel"
			if len(rels) > 0 {
				now, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				vf := now + types.Instant(r.Intn(5000))
				if _, err := g.Temporal().SetRelVersionInterval(ctx, rels[pick(len(rels))], vf, vf+types.Instant(1+r.Intn(5000)), map[string]any{"w": int64(-1)}); err == nil {
					ok[name]++
				}
			}
		case op < 55:
			name = "closeOrLabel"
			id := nodes[pick(len(nodes))]
			var err error
			if r.Intn(2) == 0 {
				err = g.Nodes().AddLabel(ctx, id, "Seen")
			} else {
				now, nerr := g.Temporal().NowTx()
				if nerr != nil {
					t.Fatal(nerr)
				}
				err = g.Nodes().CloseVersion(ctx, id, now+1000)
			}
			if err == nil {
				ok[name]++
			}
		case op < 61:
			name = "deleteNode"
			if g.Nodes().Delete(ctx, nodes[pick(len(nodes))]) == nil {
				ok[name]++
			}
		case op < 66:
			name = "deleteRel"
			if len(rels) > 0 && g.Rels().Delete(ctx, rels[pick(len(rels))]) == nil {
				ok[name]++
			}
		case op < 76:
			// A transaction that creates history (updates, a create+update, a
			// cascade) and rolls it back (history trimmed) or commits it.
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateNode(nodes[pick(len(nodes))], map[string]any{"v": int64(r.Intn(100))}); err == nil {
				ok["txUpdateNode"]++
			}
			if len(rels) > 0 {
				if _, err := tx.UpdateRelationship(rels[pick(len(rels))], map[string]any{"w": int64(r.Intn(100))}); err == nil {
					ok["txUpdateRel"]++
				}
			}
			n, err := tx.AddNode([]string{"Event"}, map[string]any{"v": int64(1)})
			if err == nil {
				nodes = append(nodes, n.ID())
				if _, err := tx.UpdateNode(n.ID(), map[string]any{"v": int64(2)}); err == nil {
					ok["txCreateUpdate"]++
				}
			}
			if r.Intn(3) == 0 {
				name = "txCommit"
				if tx.Commit() == nil {
					ok[name]++
				}
			} else {
				name = "txRollback"
				if tx.Rollback() == nil {
					ok[name]++
				}
			}
		case op < 80:
			name = "compact"
			keep := adminpkg.RetentionPolicy{KeepVersions: 1}
			_, nerr := g.Admin().CompactHistoryNodes(ctx, keep)
			_, rerr := g.Admin().CompactHistoryRels(ctx, keep)
			for _, err := range []error{nerr, rerr} {
				if err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
					t.Fatalf("compact: %v", err)
				}
			}
			if nerr == nil {
				ok[name]++
			}
		case op < 84:
			name = "purge"
			farFuture := types.InstantFromTime(time.Now().Add(24 * time.Hour))
			if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Temp", Mode: adminpkg.PurgeByAge, Before: farFuture}); err == nil {
				ok[name]++
			} else if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Fatalf("purge: %v", err)
			}
		case op < 88:
			name = "erase"
			res, err := g.Admin().ResolveExactErasure(ctx, adminpkg.ExactErasureRequest{
				NodeIDs: []types.NodeID{nodes[pick(len(nodes))]},
				Bounds:  publicExactErasureBounds,
			})
			if err == nil {
				if _, err := g.Admin().ExactErase(ctx, res.Request); err == nil {
					ok[name]++
				}
			}
		case op < 90:
			name = "clear"
			if err := g.Admin().Reset(); err != nil {
				t.Fatalf("reset: %v", err)
			}
			ok[name]++
			// Keep the old IDs: after Clear every one must read false.
			addNode()
			addNode()
			r1, err := g.Rels().AddByID(ctx, "LINK", nodes[len(nodes)-2], nodes[len(nodes)-1], nil)
			if err != nil {
				t.Fatalf("add rel after clear: %v", err)
			}
			rels = append(rels, r1.ID())
			burst()
		case op < 95:
			name = "flush"
			if flush != nil {
				if err := flush(); err != nil {
					t.Fatalf("flush: %v", err)
				}
				ok[name]++
			}
		default:
			name = "reopen"
			if b.reopen {
				if err := g.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
				st, flush = b.open(t, dir)
				g = hasHistoryGraph(t, st)
				ok[name]++
				burst()
			}
		}
		if check {
			assertHasHistoryAgrees(t, fmt.Sprintf("step %d %s", step, name), g, st, append(nodes, ghostNodes...), append(rels, ghostRels...))
		}
	}
	assertHasHistoryAgrees(t, "final", g, st, append(nodes, ghostNodes...), append(rels, ghostRels...))
	t.Logf("%s seed %d: successful ops %v", b.name, seed, ok)
	for _, must := range []string{"updateNode", "cascadeNode", "deleteNode", "txRollback", "addRel", "updateRel", "burstUpdate"} {
		if ok[must] == 0 && steps >= 100 {
			t.Errorf("op %s never succeeded; the sequence does not exercise it", must)
		}
	}

}

// TestHasHistoryImport: the history a bootstrap import writes reads the same
// through both doors, on every backend, for updated, cascaded, closed,
// deleted and plain entities. Separate from the randomized differential: an
// export taken after a compaction that trimmed an entity whose newest history
// row is a bounded-cascade piece above its current slot does not import
// (pre-existing, reported with the HasHistory review fixes).
func TestHasHistoryImport(t *testing.T) {
	for _, b := range hasHistoryBackends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			st, _ := b.open(t, t.TempDir())
			g := hasHistoryGraph(t, st)
			t.Cleanup(func() { _ = g.Close() })
			var nodes []types.NodeID
			var rels []types.RelID
			for i := range 6 {
				n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n.ID())
			}
			for i := 1; i < len(nodes); i++ {
				r, err := g.Rels().AddByID(ctx, "LINK", nodes[i-1], nodes[i], map[string]any{"w": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				rels = append(rels, r.ID())
			}
			for _, id := range nodes[:2] {
				if _, err := g.Nodes().Update(ctx, id, map[string]any{"v": int64(-1)}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.Rels().Update(ctx, rels[0], map[string]any{"w": int64(-1)}); err != nil {
				t.Fatal(err)
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, nodes[2], now+10, 0, map[string]any{"v": int64(9)}); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels().Delete(ctx, rels[3]); err != nil {
				t.Fatal(err)
			}
			var snap bytes.Buffer
			if err := g.IO().Export(&snap); err != nil {
				t.Fatalf("export: %v", err)
			}
			st2, _ := b.open(t, t.TempDir())
			g2 := hasHistoryGraph(t, st2)
			t.Cleanup(func() { _ = g2.Close() })
			if err := g2.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
				t.Fatalf("import: %v", err)
			}
			assertHasHistoryAgrees(t, "imported", g2, st2, nodes, rels)
			if has, err := g2.Nodes().HasHistory(nodes[0]); err != nil || !has {
				t.Fatalf("imported updated node: HasHistory = %v, %v", has, err)
			}
			if has, err := g2.Nodes().HasHistory(nodes[5]); err != nil || has {
				t.Fatalf("imported plain node: HasHistory = %v, %v", has, err)
			}
		})
	}
}

// Replica apply writes history through the replica's own doors: after
// ApplyChanges the replica's HasHistory agrees with its History, entity by
// entity, for updated, deleted, cascaded and plain entities.
func TestHasHistoryReplicaApply(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(t *testing.T) storepkg.Store
	}{
		{"memory", func(*testing.T) storepkg.Store { return memory.New() }},
		{"badger", func(t *testing.T) storepkg.Store {
			st, err := badger.New(badger.Config{InMemory: true, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pcfg := graphpkg.Config{SnowflakeNodeID: 1, ChangeLog: true, SyncWrites: true}
			if tc.name == "badger" {
				pcfg.BadgerInMemory = true
			} else {
				pcfg.Store = memory.New(memory.WithChangeLog())
			}
			primary, err := graphpkg.New(pcfg)
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			rst := tc.store(t)
			replica, err := graphpkg.New(graphpkg.Config{Store: rst, SnowflakeNodeID: 2, ReadOnlyReplica: true})
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			replica.SetReplicationSource(primary.Replication())

			var nodes []types.NodeID
			var rels []types.RelID
			for i := range 6 {
				n, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n.ID())
			}
			for i := 1; i < len(nodes); i++ {
				rel, err := primary.Rels().AddByID(ctx, "NEXT", nodes[i-1], nodes[i], nil)
				if err != nil {
					t.Fatal(err)
				}
				rels = append(rels, rel.ID())
			}
			apply := func() {
				t.Helper()
				applied, _ := replica.Replication().AppliedLSN()
				var recs []storepkg.ChangeRecord
				if err := primary.Replication().ForEachChange(applied, func(rec storepkg.ChangeRecord) bool {
					recs = append(recs, rec)
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := replica.Replication().ApplyChanges(recs); err != nil {
					t.Fatal(err)
				}
				if len(recs) == 0 {
					t.Fatal("the primary logged no change records")
				}
			}
			apply()
			// Two-phase: plain before the primary's updates reach the replica
			// (this first call also builds badger's set), history after.
			assertHasHistoryAgrees(t, "replica before", replica, rst, nodes, rels)
			if has, err := replica.Nodes().HasHistory(nodes[0]); err != nil || has {
				t.Fatalf("replica plain node: HasHistory = %v, %v", has, err)
			}
			if _, err := primary.Nodes().Update(ctx, nodes[0], map[string]any{"i": int64(-1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := primary.Rels().Update(ctx, rels[1], map[string]any{"w": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if err := primary.Rels().Delete(ctx, rels[2]); err != nil {
				t.Fatal(err)
			}
			now, err := primary.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := primary.Temporal().SetNodeVersionInterval(ctx, nodes[3], now+10, now+20, map[string]any{"i": int64(9)}); err != nil {
				t.Fatal(err)
			}
			apply()
			if has, err := replica.Nodes().HasHistory(nodes[0]); err != nil || !has {
				t.Fatalf("replica updated node: HasHistory = %v, %v", has, err)
			}
			assertHasHistoryAgrees(t, "replica after", replica, rst, nodes, rels)
		})
	}
}

// Door contract on every backend: zero and negative IDs fail like History
// does (ErrInvalidStoreMutation), a closed graph fails with ErrGraphClosed, a
// plain entity is false and an updated one true (two-phase: the same entity
// before and after the update).
func TestHasHistoryDoorContract(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		for _, bad := range []int64{0, -1} {
			if _, err := g.Nodes().HasHistory(types.NodeID(bad)); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("Nodes().HasHistory(%d) = %v, want ErrInvalidStoreMutation", bad, err)
			}
			if _, err := g.Rels().HasHistory(types.RelID(bad)); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("Rels().HasHistory(%d) = %v, want ErrInvalidStoreMutation", bad, err)
			}
			if _, herr := g.Nodes().History(types.NodeID(bad)); !errors.Is(herr, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("History(%d) = %v: the sibling door's error changed", bad, herr)
			}
		}
		a, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		bnode, err := g.Nodes().Add(ctx, []string{"Event"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := g.Rels().AddByID(ctx, "LINK", a.ID(), bnode.ID(), map[string]any{"w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		if has, err := g.Nodes().HasHistory(a.ID()); err != nil || has {
			t.Fatalf("plain node: HasHistory = %v, %v; want false", has, err)
		}
		if has, err := g.Rels().HasHistory(rel.ID()); err != nil || has {
			t.Fatalf("plain rel: HasHistory = %v, %v; want false", has, err)
		}
		if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(2)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, rel.ID(), map[string]any{"w": int64(2)}); err != nil {
			t.Fatal(err)
		}
		if has, err := g.Nodes().HasHistory(a.ID()); err != nil || !has {
			t.Fatalf("updated node: HasHistory = %v, %v; want true", has, err)
		}
		if has, err := g.Rels().HasHistory(rel.ID()); err != nil || !has {
			t.Fatalf("updated rel: HasHistory = %v, %v; want true", has, err)
		}
		// The neighbour stays plain while a holds history.
		if has, err := g.Nodes().HasHistory(bnode.ID()); err != nil || has {
			t.Fatalf("neighbour node: HasHistory = %v, %v; want false", has, err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().HasHistory(a.ID()); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed Nodes().HasHistory = %v, want ErrGraphClosed", err)
		}
		if _, err := g.Rels().HasHistory(rel.ID()); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed Rels().HasHistory = %v, want ErrGraphClosed", err)
		}
	})
	var nilGraph *graphpkg.Graph
	if _, err := nilGraph.Nodes().HasHistory(1); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("nil graph Nodes().HasHistory = %v, want ErrNilGraph", err)
	}
	if _, err := nilGraph.Rels().HasHistory(1); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("nil graph Rels().HasHistory = %v, want ErrNilGraph", err)
	}
}

// historyOnlyStore hides every optional capability: the core must answer
// HasHistory from History for a third-party store without the capability.
type historyOnlyStore struct{ storepkg.Store }

func TestHasHistoryFallbackWithoutCapability(t *testing.T) {
	ctx := context.Background()
	st := historyOnlyStore{memory.New()}
	if _, ok := any(st).(storepkg.HistoryPresenceCapability); ok {
		t.Fatal("the wrapper must hide the capability")
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	m, err := g.Nodes().Add(ctx, []string{"Event"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels().AddByID(ctx, "LINK", n.ID(), m.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if has, err := g.Nodes().HasHistory(n.ID()); err != nil || has {
		t.Fatalf("fallback plain node = %v, %v", has, err)
	}
	if has, err := g.Rels().HasHistory(rel.ID()); err != nil || has {
		t.Fatalf("fallback plain rel = %v, %v", has, err)
	}
	if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Update(ctx, rel.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	if has, err := g.Nodes().HasHistory(n.ID()); err != nil || !has {
		t.Fatalf("fallback updated node = %v, %v", has, err)
	}
	if has, err := g.Rels().HasHistory(rel.ID()); err != nil || !has {
		t.Fatalf("fallback updated rel = %v, %v", has, err)
	}
}
