package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 30: Nodes().LatestStamps / Rels().LatestStamps. The definition the
// door must equal at every moment: over the entity's current row (Get) and
// every history row (History), txFrom is the largest TxFrom, txTo the largest
// TxTo or DeletedAt (0 when none is set), deleted is true when the entity has
// rows but no current row; an ID with no row at all is ErrNodeNotFound /
// ErrRelNotFound.

// stampsWant is the oracle's answer for one ID.
type stampsWant struct {
	from, to types.Instant
	deleted  bool
	found    bool
	// headFrom is the current row's TxFrom (0 without a current row): a door
	// that reads only the head answers it.
	headFrom types.Instant
}

func (w *stampsWant) fold(tm *types.TemporalMetadata) {
	if tm == nil {
		return
	}
	w.from = max(w.from, tm.TxFrom)
	w.to = max(w.to, tm.TxTo, tm.DeletedAt)
}

// histStamps folds rows the way the history half of the store capability must.
func histStampsOf[T interface {
	Temporal() *types.TemporalMetadata
}](rows []T) (from, to types.Instant) {
	var w stampsWant
	for _, r := range rows {
		w.fold(r.Temporal())
	}
	return w.from, w.to
}

func nodeStampsOracle(g *graphpkg.Graph, id types.NodeID) (stampsWant, []*types.Node, error) {
	var w stampsWant
	cur, cerr := g.Nodes().Get(context.Background(), id)
	if cerr != nil && !errors.Is(cerr, storepkg.ErrNodeNotFound) {
		return w, nil, cerr
	}
	hist, herr := g.Nodes().History(id)
	if herr != nil {
		return w, nil, herr
	}
	if cerr == nil {
		w.fold(cur.Temporal())
		if tm := cur.Temporal(); tm != nil {
			w.headFrom = tm.TxFrom
		}
	}
	for _, h := range hist {
		w.fold(h.Temporal())
	}
	w.found = cerr == nil || len(hist) > 0
	w.deleted = cerr != nil && len(hist) > 0
	return w, hist, nil
}

func relStampsOracle(g *graphpkg.Graph, id types.RelID) (stampsWant, []*types.Relationship, error) {
	var w stampsWant
	cur, cerr := g.Rels().Get(context.Background(), id)
	if cerr != nil && !errors.Is(cerr, storepkg.ErrRelNotFound) {
		return w, nil, cerr
	}
	hist, herr := g.Rels().History(id)
	if herr != nil {
		return w, nil, herr
	}
	if cerr == nil {
		w.fold(cur.Temporal())
		if tm := cur.Temporal(); tm != nil {
			w.headFrom = tm.TxFrom
		}
	}
	for _, h := range hist {
		w.fold(h.Temporal())
	}
	w.found = cerr == nil || len(hist) > 0
	w.deleted = cerr != nil && len(hist) > 0
	return w, hist, nil
}

// stampsCoverage counts the states the sequence reached, so a run that never
// put a history stamp above the head (where a head-only door is wrong) fails.
type stampsCoverage struct {
	aboveHead, deleted, revived, histTo int
}

// assertLatestStampsAgree checks LatestStamps against the oracle for every id,
// through the graph door AND the store capability directly (a core that fell
// back to History would hide a wrong store answer).
func assertLatestStampsAgree(t *testing.T, step string, g *graphpkg.Graph, st storepkg.Store, nodes []types.NodeID, rels []types.RelID, cov *stampsCoverage) {
	t.Helper()
	caps, ok := st.(storepkg.HistoryStampsCapability)
	if !ok {
		t.Fatalf("%s: store %T lacks HistoryStampsCapability", step, st)
	}
	for _, id := range nodes {
		want, hist, oerr := nodeStampsOracle(g, id)
		from, to, deleted, err := g.Nodes().LatestStamps(id)
		hf, ht, has, derr := caps.NodeHistoryStamps(id)
		if oerr != nil {
			if err == nil || derr == nil {
				t.Fatalf("%s: node %d: oracle error %v, LatestStamps %v, store %v", step, id, oerr, err, derr)
			}
			continue
		}
		if derr != nil {
			t.Fatalf("%s: node %d: store NodeHistoryStamps: %v", step, id, derr)
		}
		wf, wt := histStampsOf(hist)
		if has != (len(hist) > 0) || hf != wf || ht != wt {
			t.Fatalf("%s: node %d store history stamps = (%d, %d, %v), History folds to (%d, %d, %v) over %d rows", step, id, hf, ht, has, wf, wt, len(hist) > 0, len(hist))
		}
		if !want.found {
			if !errors.Is(err, storepkg.ErrNodeNotFound) {
				t.Fatalf("%s: node %d has no row: LatestStamps = (%d, %d, %v, %v), want ErrNodeNotFound", step, id, from, to, deleted, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: node %d: LatestStamps: %v", step, id, err)
		}
		if from != want.from || to != want.to || deleted != want.deleted {
			t.Fatalf("%s: node %d LatestStamps = (%d, %d, deleted=%v), want (%d, %d, deleted=%v)", step, id, from, to, deleted, want.from, want.to, want.deleted)
		}
		cov.count(want, len(hist))
	}
	for _, id := range rels {
		want, hist, oerr := relStampsOracle(g, id)
		from, to, deleted, err := g.Rels().LatestStamps(id)
		hf, ht, has, derr := caps.RelHistoryStamps(id)
		if oerr != nil {
			if err == nil || derr == nil {
				t.Fatalf("%s: rel %d: oracle error %v, LatestStamps %v, store %v", step, id, oerr, err, derr)
			}
			continue
		}
		if derr != nil {
			t.Fatalf("%s: rel %d: store RelHistoryStamps: %v", step, id, derr)
		}
		wf, wt := histStampsOf(hist)
		if has != (len(hist) > 0) || hf != wf || ht != wt {
			t.Fatalf("%s: rel %d store history stamps = (%d, %d, %v), History folds to (%d, %d, %v) over %d rows", step, id, hf, ht, has, wf, wt, len(hist) > 0, len(hist))
		}
		if !want.found {
			if !errors.Is(err, storepkg.ErrRelNotFound) {
				t.Fatalf("%s: rel %d has no row: LatestStamps = (%d, %d, %v, %v), want ErrRelNotFound", step, id, from, to, deleted, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: rel %d: LatestStamps: %v", step, id, err)
		}
		if from != want.from || to != want.to || deleted != want.deleted {
			t.Fatalf("%s: rel %d LatestStamps = (%d, %d, deleted=%v), want (%d, %d, deleted=%v)", step, id, from, to, deleted, want.from, want.to, want.deleted)
		}
		cov.count(want, len(hist))
	}
}

func (c *stampsCoverage) count(w stampsWant, histRows int) {
	if c == nil {
		return
	}
	if !w.deleted && w.from > w.headFrom {
		c.aboveHead++
	}
	if w.deleted {
		c.deleted++
	}
	if histRows > 0 && w.to > 0 {
		c.histTo++
	}
}

func latestStampsGraph(t *testing.T, st storepkg.Store) *graphpkg.Graph {
	t.Helper()
	g, err := graphpkg.New(graphpkg.Config{
		Store:               st,
		AllowRetentionPurge: true,
		AllowReset:          true,
		AllowExactErasure:   true,
		AllowTxBackfill:     true,
		Validation:          graphpkg.ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestLatestStampsDifferential: randomized sequences over every write door that
// adds, ends, rewrites or removes a row - create, AddWithTx backfill, update
// (with a future tkg_valid_from, whose TxFrom is pushed above later stamps),
// UpdateInPlace, UpdateWithTx, cascade (open, bounded, one tick), close,
// label, delete, DeleteWithTx, re-import of a deleted ID (a second life),
// GraphTx commit and rollback, compaction, retention purge, exact erasure,
// Clear, flush, reopen - on memory, badger on disk (writes left unflushed),
// badger with delta-encoded history, tiered and sharded, nodes and
// relationships. After EVERY step LatestStamps
// equals the oracle for every ID ever created plus IDs that never existed,
// and the store's history half equals the fold of History. After a reopen or
// a Clear a burst leaves SETs and DELETEs of one entity in the write buffer
// before the next check, which is the first call and builds the sidecar.
//
// Faults it catches (each a mutant in tasks/evidence/latest-stamps): a door
// that reads only the head row, a sidecar whose max is not raised by a history
// write, a tombstone stamp ignored, a history delete (rollback, compaction,
// purge, erasure) that does not invalidate, a build that reads keys only, a
// value that survives Clear, a re-imported life reported deleted.
func TestLatestStampsDifferential(t *testing.T) {
	steps := 160
	if testing.Short() {
		steps = 70
	}
	for _, b := range latestStampsBackends() {
		total := map[string]int{}
		cov := &stampsCoverage{}
		for _, seed := range hasHistorySeeds() {
			t.Run(fmt.Sprintf("%s/seed=%d", b.name, seed), func(t *testing.T) {
				for k, v := range runLatestStampsDifferential(t, b, seed, steps, cov) {
					total[k] += v
				}
			})
		}
		// Every door the sequence draws must have succeeded on this backend
		// (summed over the seeds), and the states where a head-only or
		// history-blind door is wrong must have been reached.
		if steps >= 100 && !t.Failed() {
			for _, must := range []string{"updateNode", "updateRel", "cascadeNode", "cascadeRel", "pastCascade", "deleteNode", "deleteRel", "reimport", "txRollback", "updateWithTx", "deleteWithTx", "addNodeWithTx", "updateInPlace", "compact", "erase", "storeOverwrite"} {
				if total[must+"Unsupported"] > 0 {
					continue // the backend declines the door (erasure: tiered, sharded; compaction: sharded)
				}
				if total[must] == 0 {
					t.Errorf("%s: op %s never succeeded; the sequence does not exercise it", b.name, must)
				}
			}
			if b.reopen && total["burstUpdate"] == 0 {
				t.Errorf("%s: the burst after reopen / Clear never updated", b.name)
			}
			if cov.aboveHead == 0 || cov.deleted == 0 || cov.histTo == 0 || cov.revived == 0 {
				t.Errorf("%s: coverage %+v: a stamp above the head, a deleted entity, a history TxTo or a revived ID never reached", b.name, *cov)
			}
		}
	}
}

// latestStampsBackends is hasHistoryBackends plus badger with delta-encoded
// history rows (anchor every third version): the sidecar's build and every
// maintained write then decode delta values, and a moved row lands as a delta
// over an earlier anchor.
func latestStampsBackends() []hasHistoryBackend {
	return append(hasHistoryBackends(), hasHistoryBackend{name: "badger-delta-disk", reopen: true, open: func(t *testing.T, dir string) (storepkg.Store, func() error) {
		st, err := badger.New(badger.Config{Dir: dir, FlushInterval: time.Hour, HistoryDeltaEncoding: true, HistoryAnchorInterval: 3})
		if err != nil {
			t.Fatal(err)
		}
		return st, st.Flush
	}})
}

func runLatestStampsDifferential(t *testing.T, b hasHistoryBackend, seed int64, steps int, cov *stampsCoverage) map[string]int {
	ctx := context.Background()
	r := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test sequence
	dir := t.TempDir()
	st, flush := b.open(t, dir)
	g := latestStampsGraph(t, st)
	t.Cleanup(func() { _ = g.Close() })

	var nodes []types.NodeID
	var rels []types.RelID
	ghostNodes := []types.NodeID{types.NodeID(1 << 40), types.NodeID(1<<40 + 7)}
	ghostRels := []types.RelID{types.RelID(1 << 41)}
	ok := map[string]int{}
	pick := func(n int) int { return r.Intn(n) }
	// liveNode / liveRel prefer an ID with a current row (a write door on a
	// deleted ID only fails); deadNode / deadRel prefer one without (re-import).
	liveNode := func() types.NodeID {
		id := nodes[pick(len(nodes))]
		for i := 0; i < 6; i++ {
			if _, err := g.Nodes().Get(ctx, id); err == nil {
				return id
			}
			id = nodes[pick(len(nodes))]
		}
		return id
	}
	liveRel := func() types.RelID {
		id := rels[pick(len(rels))]
		for i := 0; i < 6; i++ {
			if _, err := g.Rels().Get(ctx, id); err == nil {
				return id
			}
			id = rels[pick(len(rels))]
		}
		return id
	}
	deadNode := func() types.NodeID {
		id := nodes[pick(len(nodes))]
		for i := 0; i < 6; i++ {
			if _, err := g.Nodes().Get(ctx, id); err != nil {
				return id
			}
			id = nodes[pick(len(nodes))]
		}
		return id
	}
	deadRel := func() types.RelID {
		id := rels[pick(len(rels))]
		for i := 0; i < 6; i++ {
			if _, err := g.Rels().Get(ctx, id); err != nil {
				return id
			}
			id = rels[pick(len(rels))]
		}
		return id
	}
	now := func() types.Instant {
		n, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
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
	addRel := func() bool {
		if len(nodes) < 2 {
			return false
		}
		rel, err := g.Rels().AddByID(ctx, "LINK", liveNode(), liveNode(), map[string]any{"w": int64(r.Intn(100))})
		if err != nil {
			return false
		}
		rels = append(rels, rel.ID())
		return true
	}
	// updateProps sometimes moves the valid start into the future: the
	// update's TxFrom is then pushed past it (validInstantAfter), above the
	// stamps later writes get from the clock.
	updateProps := func(key string) map[string]any {
		p := map[string]any{key: int64(r.Intn(1000))}
		if r.Intn(8) == 0 {
			p["tkg_valid_from"] = now() + types.Instant(1+r.Intn(4000))
		}
		return p
	}
	burst := func() {
		for i := 0; i < 3 && len(nodes) > 0; i++ {
			id := liveNode()
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
		for i := 0; i < 3 && len(rels) > 0; i++ {
			id := liveRel()
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
	all := func() ([]types.NodeID, []types.RelID) {
		return append(append([]types.NodeID(nil), nodes...), ghostNodes...), append(append([]types.RelID(nil), rels...), ghostRels...)
	}
	for range 8 {
		addNode()
	}
	for range 8 {
		addRel()
	}
	ns, rs := all()
	assertLatestStampsAgree(t, "seeded", g, st, ns, rs, cov)

	for step := 0; step < steps; step++ {
		op := r.Intn(124)
		name := ""
		switch {
		case op < 8:
			name = "addNode"
			addNode()
		case op < 11:
			name = "addNodeWithTx"
			if n, err := g.Nodes().AddWithTx(ctx, labels(), map[string]any{"v": int64(1)}, now()-types.Instant(1+r.Intn(100000))); err == nil {
				nodes = append(nodes, n.ID())
				ok[name]++
			}
		case op < 18:
			name = "addRel"
			if addRel() {
				ok[name]++
			}
		case op < 26:
			name = "updateNode"
			if _, err := g.Nodes().Update(ctx, liveNode(), updateProps("v")); err == nil {
				ok[name]++
			}
		case op < 32:
			name = "updateRel"
			if len(rels) > 0 {
				if _, err := g.Rels().Update(ctx, liveRel(), updateProps("w")); err == nil {
					ok[name]++
				}
			}
		case op < 35:
			name = "updateInPlace"
			if r.Intn(2) == 0 {
				if _, err := g.Nodes().UpdateInPlace(ctx, liveNode(), map[string]any{"v": int64(r.Intn(9))}); err == nil {
					ok[name]++
				}
			} else if len(rels) > 0 {
				if _, err := g.Rels().UpdateInPlace(ctx, liveRel(), map[string]any{"w": int64(r.Intn(9))}); err == nil {
					ok[name]++
				}
			}
		case op < 39:
			name = "updateWithTx"
			if r.Intn(2) == 0 {
				if _, err := g.Nodes().UpdateWithTx(ctx, liveNode(), map[string]any{"v": int64(r.Intn(9))}, now()); err == nil {
					ok[name]++
				}
			} else if len(rels) > 0 {
				if _, err := g.Rels().UpdateWithTx(ctx, liveRel(), map[string]any{"w": int64(r.Intn(9))}, now()); err == nil {
					ok[name]++
				}
			}
		case op < 46:
			name = "cascadeNode"
			vf := now() + types.Instant(r.Intn(5000))
			vt := types.Instant(0)
			switch r.Intn(3) {
			case 0:
				vt = vf + types.Instant(1+r.Intn(5000))
			case 1:
				vt = vf + 1 // one tick
			}
			id := liveNode()
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, id, vf, vt, map[string]any{"v": int64(-1)}); err == nil {
				ok[name]++
				// A correction inside the closed window lies before the new
				// current row's valid start: its pieces are appended to
				// history, stamped after the current row, which stays.
				if vt > vf+2 && r.Intn(2) == 0 {
					if _, err := g.Temporal().SetNodeVersionInterval(ctx, id, vf+1, vt-1, map[string]any{"v": int64(-2)}); err == nil {
						ok["pastCascade"]++
					}
				}
			}
		case op < 52:
			name = "cascadeRel"
			if len(rels) > 0 {
				vf := now() + types.Instant(r.Intn(5000))
				vt := types.Instant(0)
				switch r.Intn(3) {
				case 0:
					vt = vf + types.Instant(1+r.Intn(5000))
				case 1:
					vt = vf + 1
				}
				id := liveRel()
				if _, err := g.Temporal().SetRelVersionInterval(ctx, id, vf, vt, map[string]any{"w": int64(-1)}); err == nil {
					ok[name]++
					if vt > vf+2 && r.Intn(2) == 0 {
						if _, err := g.Temporal().SetRelVersionInterval(ctx, id, vf+1, vt-1, map[string]any{"w": int64(-2)}); err == nil {
							ok["pastCascade"]++
						}
					}
				}
			}
		case op < 55:
			name = "closeOrLabel"
			id := liveNode()
			var err error
			if r.Intn(2) == 0 {
				err = g.Nodes().AddLabel(ctx, id, "Seen")
			} else {
				err = g.Nodes().CloseVersion(ctx, id, now()+1000)
			}
			if err == nil {
				ok[name]++
			}
		case op < 61:
			name = "deleteNode"
			if g.Nodes().Delete(ctx, liveNode()) == nil {
				ok[name]++
			}
		case op < 66:
			name = "deleteRel"
			if len(rels) > 0 && g.Rels().Delete(ctx, liveRel()) == nil {
				ok[name]++
			}
		case op < 70:
			name = "deleteWithTx"
			if r.Intn(2) == 0 {
				if g.Nodes().DeleteWithTx(ctx, liveNode(), now()) == nil {
					ok[name]++
				}
			} else if len(rels) > 0 && g.Rels().DeleteWithTx(ctx, liveRel(), now()) == nil {
				ok[name]++
			}
		case op < 76:
			// A deleted ID comes back: its second life continues the chain.
			name = "reimport"
			if r.Intn(2) == 0 {
				id := deadNode()
				if _, err := g.Nodes().Import(ctx, id, []string{"Event"}, map[string]any{"v": int64(77)}); err == nil {
					ok[name]++
					cov.revived++
				}
			} else if len(rels) > 0 && len(nodes) > 1 {
				id := deadRel()
				a, aerr := g.Nodes().Get(ctx, nodes[pick(len(nodes))])
				z, zerr := g.Nodes().Get(ctx, nodes[pick(len(nodes))])
				if aerr == nil && zerr == nil {
					if _, err := g.Rels().Import(ctx, id, "LINK", a, z, map[string]any{"w": int64(77)}); err == nil {
						ok[name]++
						cov.revived++
					}
				}
			}
		case op < 86:
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateNode(liveNode(), map[string]any{"v": int64(r.Intn(100))}); err == nil {
				ok["txUpdateNode"]++
			}
			if len(rels) > 0 {
				if _, err := tx.UpdateRelationship(liveRel(), map[string]any{"w": int64(r.Intn(100))}); err == nil {
					ok["txUpdateRel"]++
				}
			}
			if n, err := tx.AddNode([]string{"Event"}, map[string]any{"v": int64(1)}); err == nil {
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
		case op < 90:
			name = "compact"
			keep := adminpkg.RetentionPolicy{KeepVersions: 1 + r.Intn(2)}
			_, nerr := g.Admin().CompactHistoryNodes(ctx, keep)
			_, rerr := g.Admin().CompactHistoryRels(ctx, keep)
			for _, err := range []error{nerr, rerr} {
				if err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
					t.Fatalf("compact: %v", err)
				}
			}
			if nerr == nil {
				ok[name]++
			} else {
				ok["compactUnsupported"]++
			}
		case op < 94:
			name = "purge"
			farFuture := types.InstantFromTime(time.Now().Add(24 * time.Hour))
			if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Temp", Mode: adminpkg.PurgeByAge, Before: farFuture}); err == nil {
				ok[name]++
			} else if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Fatalf("purge: %v", err)
			}
		case op < 98:
			name = "erase"
			res, err := g.Admin().ResolveExactErasure(ctx, adminpkg.ExactErasureRequest{
				NodeIDs: []types.NodeID{liveNode()},
				Bounds:  publicExactErasureBounds,
			})
			if err == nil {
				_, err = g.Admin().ExactErase(ctx, res.Request)
			}
			switch {
			case err == nil:
				ok[name]++
			case errors.Is(err, storepkg.ErrCapabilityNotSupported):
				ok["eraseUnsupported"]++
			}
		case op < 100:
			name = "clear"
			if err := g.Admin().Reset(); err != nil {
				t.Fatalf("reset: %v", err)
			}
			ok[name]++
			addNode()
			addNode()
			r1, err := g.Rels().AddByID(ctx, "LINK", nodes[len(nodes)-2], nodes[len(nodes)-1], nil)
			if err != nil {
				t.Fatalf("add rel after clear: %v", err)
			}
			rels = append(rels, r1.ID())
			burst()
		case op < 110:
			name = "flush"
			if flush != nil {
				if err := flush(); err != nil {
					t.Fatalf("flush: %v", err)
				}
				ok[name]++
			}
		case op < 118:
			// A store-door rewrite of an existing history version with LOWER
			// stamps (PutNodeVersion / PutRelVersion: the door replica apply
			// and restores use; no graph door lowers a stored row today): the
			// overwritten row may have held the max, so a sidecar that folds
			// every write like an append keeps a stamp no row carries.
			name = "storeOverwrite"
			if storeOverwriteLower(t, g, st, liveNode(), liveRel(), r.Intn(2) == 0) {
				ok[name]++
			}
		default:
			name = "reopen"
			if b.reopen {
				if err := g.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
				st, flush = b.open(t, dir)
				g = latestStampsGraph(t, st)
				ok[name]++
				burst()
			}
		}
		ns, rs := all()
		assertLatestStampsAgree(t, fmt.Sprintf("step %d %s", step, name), g, st, ns, rs, cov)
	}
	t.Logf("%s seed %d: successful ops %v, coverage %+v", b.name, seed, ok, *cov)
	return ok
}

// TestLatestStampsDoorContract: every backend, nodes and relationships.
// Invalid IDs fail like History (ErrInvalidStoreMutation), an ID without rows
// with the not-found sentinel, a closed graph with ErrGraphClosed, a nil graph
// with ErrNilGraph. Two-phase lifecycle on one entity, each answer read again
// after the next write: plain (head only, txTo 0), updated (the superseded
// row's TxTo), bounded cascade (a history row stamped after the head while the
// head stays: a head-only door misses it), deleted (no head; deleted, txTo =
// the tombstone), re-imported (deleted false again, the earlier life's stamps
// still count). The neighbour keeps its own answer throughout.
func TestLatestStampsDoorContract(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		for _, bad := range []int64{0, -1} {
			if _, _, _, err := g.Nodes().LatestStamps(types.NodeID(bad)); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("Nodes().LatestStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
			}
			if _, _, _, err := g.Rels().LatestStamps(types.RelID(bad)); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("Rels().LatestStamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
			}
		}
		if _, _, _, err := g.Nodes().LatestStamps(types.NodeID(1 << 40)); !errors.Is(err, graphpkg.ErrNodeNotFound) {
			t.Fatalf("unknown node: %v, want ErrNodeNotFound", err)
		}
		if _, _, _, err := g.Rels().LatestStamps(types.RelID(1 << 41)); !errors.Is(err, graphpkg.ErrRelNotFound) {
			t.Fatalf("unknown rel: %v, want ErrRelNotFound", err)
		}
		a, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		z, err := g.Nodes().Add(ctx, []string{"Event"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := g.Rels().AddByID(ctx, "LINK", a.ID(), z.ID(), map[string]any{"w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		node := func(step string, id types.NodeID, wantDeleted bool) stampsWant {
			t.Helper()
			want, _, err := nodeStampsOracle(g, id)
			if err != nil {
				t.Fatal(err)
			}
			from, to, deleted, err := g.Nodes().LatestStamps(id)
			if err != nil || from != want.from || to != want.to || deleted != want.deleted || deleted != wantDeleted {
				t.Fatalf("%s: node LatestStamps = (%d, %d, %v, %v), want (%d, %d, %v)", step, from, to, deleted, err, want.from, want.to, wantDeleted)
			}
			return want
		}
		relW := func(step string, id types.RelID, wantDeleted bool) stampsWant {
			t.Helper()
			want, _, err := relStampsOracle(g, id)
			if err != nil {
				t.Fatal(err)
			}
			from, to, deleted, err := g.Rels().LatestStamps(id)
			if err != nil || from != want.from || to != want.to || deleted != want.deleted || deleted != wantDeleted {
				t.Fatalf("%s: rel LatestStamps = (%d, %d, %v, %v), want (%d, %d, %v)", step, from, to, deleted, err, want.from, want.to, wantDeleted)
			}
			return want
		}
		plain := node("plain", a.ID(), false)
		if plain.to != 0 || plain.from == 0 || plain.from != a.Temporal().TxFrom {
			t.Fatalf("plain node stamps (%d, %d), head TxFrom %d", plain.from, plain.to, a.Temporal().TxFrom)
		}
		plainRel := relW("plain", rel.ID(), false)
		if plainRel.to != 0 || plainRel.from != rel.Temporal().TxFrom {
			t.Fatalf("plain rel stamps (%d, %d)", plainRel.from, plainRel.to)
		}
		zPlain := node("neighbour plain", z.ID(), false)

		if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(2)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, rel.ID(), map[string]any{"w": int64(2)}); err != nil {
			t.Fatal(err)
		}
		upd := node("updated", a.ID(), false)
		if upd.to == 0 || upd.from <= plain.from {
			t.Fatalf("updated node stamps (%d, %d) after (%d, %d): the superseded row's TxTo must count", upd.from, upd.to, plain.from, plain.to)
		}
		relW("updated", rel.ID(), false)

		now, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Temporal().SetRelVersionInterval(ctx, rel.ID(), now+10, now+20, map[string]any{"w": int64(3)}); err != nil {
			t.Fatal(err)
		}
		relW("bounded cascade", rel.ID(), false)
		// A correction inside the closed window, before the current row's
		// valid start: its pieces are appended to history, stamped after the
		// current row, which stays (a head-only door misses them).
		if _, err := g.Temporal().SetRelVersionInterval(ctx, rel.ID(), now+11, now+15, map[string]any{"w": int64(4)}); err != nil {
			t.Fatal(err)
		}
		cas := relW("past correction", rel.ID(), false)
		if cas.from <= cas.headFrom {
			t.Fatalf("past correction: stamps from %d, head TxFrom %d: the appended pieces above the head must count", cas.from, cas.headFrom)
		}
		if _, err := g.Temporal().SetNodeVersionInterval(ctx, a.ID(), now+10, now+20, map[string]any{"v": int64(3)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Temporal().SetNodeVersionInterval(ctx, a.ID(), now+11, now+15, map[string]any{"v": int64(5)}); err != nil {
			t.Fatal(err)
		}
		if w := node("node past correction", a.ID(), false); w.from <= w.headFrom {
			t.Fatalf("node past correction: stamps from %d, head TxFrom %d", w.from, w.headFrom)
		}

		if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
			t.Fatal(err)
		}
		del := node("deleted", a.ID(), true)
		if del.to < cas.from {
			t.Fatalf("deleted: txTo %d below earlier stamp %d", del.to, cas.from)
		}
		relW("cascade-deleted rel", rel.ID(), true)

		if _, err := g.Nodes().Import(ctx, a.ID(), []string{"Event"}, map[string]any{"v": int64(4)}); err != nil {
			t.Fatal(err)
		}
		back := node("re-imported", a.ID(), false)
		if back.to != del.to || back.from <= del.from {
			t.Fatalf("re-imported: stamps (%d, %d), deleted life (%d, %d)", back.from, back.to, del.from, del.to)
		}
		if got := node("neighbour after", z.ID(), false); got.from != zPlain.from || got.to != 0 {
			t.Fatalf("neighbour moved: (%d, %d) was (%d, %d)", got.from, got.to, zPlain.from, zPlain.to)
		}

		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := g.Nodes().LatestStamps(a.ID()); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed Nodes().LatestStamps = %v, want ErrGraphClosed", err)
		}
		if _, _, _, err := g.Rels().LatestStamps(rel.ID()); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed Rels().LatestStamps = %v, want ErrGraphClosed", err)
		}
	})
	var nilGraph *graphpkg.Graph
	if _, _, _, err := nilGraph.Nodes().LatestStamps(1); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("nil graph Nodes().LatestStamps = %v, want ErrNilGraph", err)
	}
	if _, _, _, err := nilGraph.Rels().LatestStamps(1); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("nil graph Rels().LatestStamps = %v, want ErrNilGraph", err)
	}
}

// A store without HistoryStampsCapability (a third-party store; the wrapper
// hides every optional capability) is answered from History with the same
// values, deleted and re-imported entities included.
func TestLatestStampsFallbackWithoutCapability(t *testing.T) {
	ctx := context.Background()
	st := historyOnlyStore{memory.New()}
	if _, ok := any(st).(storepkg.HistoryStampsCapability); ok {
		t.Fatal("the wrapper must hide the capability")
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	a, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	z, err := g.Nodes().Add(ctx, []string{"Event"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels().AddByID(ctx, "LINK", a.ID(), z.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	check := func(step string, wantDeleted bool) {
		t.Helper()
		for _, id := range []types.NodeID{a.ID(), z.ID()} {
			want, _, err := nodeStampsOracle(g, id)
			if err != nil {
				t.Fatal(err)
			}
			from, to, deleted, err := g.Nodes().LatestStamps(id)
			if err != nil || from != want.from || to != want.to || deleted != want.deleted {
				t.Fatalf("%s: node %d = (%d, %d, %v, %v), want %+v", step, id, from, to, deleted, err, want)
			}
		}
		want, _, err := relStampsOracle(g, rel.ID())
		if err != nil {
			t.Fatal(err)
		}
		from, to, deleted, err := g.Rels().LatestStamps(rel.ID())
		if err != nil || from != want.from || to != want.to || deleted != want.deleted || deleted != wantDeleted {
			t.Fatalf("%s: rel = (%d, %d, %v, %v), want %+v", step, from, to, deleted, err, want)
		}
	}
	check("plain", false)
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetRelVersionInterval(ctx, rel.ID(), now+10, now+20, map[string]any{"w": int64(3)}); err != nil {
		t.Fatal(err)
	}
	check("bounded cascade", false)
	if _, err := g.Temporal().SetRelVersionInterval(ctx, rel.ID(), now+11, now+15, map[string]any{"w": int64(4)}); err != nil {
		t.Fatal(err)
	}
	check("past correction", false)
	if err := g.Rels().Delete(ctx, rel.ID()); err != nil {
		t.Fatal(err)
	}
	check("deleted", true)
	if _, err := g.Rels().Import(ctx, rel.ID(), "LINK", a, z, nil); err != nil {
		t.Fatal(err)
	}
	check("re-imported", false)
	// The node half of the fallback: a node with history rows, then deleted
	// (its rel is cascaded away with it).
	if _, err := g.Nodes().Update(ctx, z.ID(), map[string]any{"v": int64(9)}); err != nil {
		t.Fatal(err)
	}
	check("node updated", false)
	if err := g.Nodes().Delete(ctx, z.ID()); err != nil {
		t.Fatal(err)
	}
	if _, _, deleted, err := g.Nodes().LatestStamps(z.ID()); err != nil || !deleted {
		t.Fatalf("fallback deleted node: deleted=%v, %v", deleted, err)
	}
	check("node deleted", true)
	if _, _, _, err := g.Nodes().LatestStamps(types.NodeID(1 << 40)); !errors.Is(err, graphpkg.ErrNodeNotFound) {
		t.Fatalf("fallback unknown node: %v", err)
	}
	if _, _, _, err := g.Rels().LatestStamps(types.RelID(1 << 41)); !errors.Is(err, graphpkg.ErrRelNotFound) {
		t.Fatalf("fallback unknown rel: %v", err)
	}
}

// The rows a bootstrap import writes answer the same through LatestStamps as
// through Get + History on every backend: updated, cascaded, deleted and plain
// entities.
func TestLatestStampsImport(t *testing.T) {
	for _, b := range hasHistoryBackends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			st, _ := b.open(t, t.TempDir())
			g := latestStampsGraph(t, st)
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
			if _, err := g.Nodes().Update(ctx, nodes[0], map[string]any{"v": int64(-1)}); err != nil {
				t.Fatal(err)
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetRelVersionInterval(ctx, rels[1], now+10, now+20, map[string]any{"w": int64(9)}); err != nil {
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
			g2 := latestStampsGraph(t, st2)
			t.Cleanup(func() { _ = g2.Close() })
			if err := g2.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
				t.Fatalf("import: %v", err)
			}
			assertLatestStampsAgree(t, "imported", g2, st2, nodes, rels, nil)
			if _, _, deleted, err := g2.Rels().LatestStamps(rels[3]); err != nil || !deleted {
				t.Fatalf("imported deleted rel: deleted=%v, %v", deleted, err)
			}
		})
	}
}

// Replica apply writes rows through the replica's own store doors: after
// ApplyChanges the replica's LatestStamps agrees with its Get + History, and
// equals the primary's, entity by entity. Two-phase: the replica's first call
// (which builds badger's sidecar) runs before the primary's later writes reach
// it, the second after.
func TestLatestStampsReplicaApply(t *testing.T) {
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
			}
			same := func(step string) {
				t.Helper()
				assertLatestStampsAgree(t, step, replica, rst, nodes, rels, nil)
				for _, id := range nodes {
					pf, pt, pd, perr := primary.Nodes().LatestStamps(id)
					rf, rt, rd, rerr := replica.Nodes().LatestStamps(id)
					if pf != rf || pt != rt || pd != rd || (perr == nil) != (rerr == nil) {
						t.Fatalf("%s: node %d primary (%d, %d, %v, %v) replica (%d, %d, %v, %v)", step, id, pf, pt, pd, perr, rf, rt, rd, rerr)
					}
				}
				for _, id := range rels {
					pf, pt, pd, perr := primary.Rels().LatestStamps(id)
					rf, rt, rd, rerr := replica.Rels().LatestStamps(id)
					if pf != rf || pt != rt || pd != rd || (perr == nil) != (rerr == nil) {
						t.Fatalf("%s: rel %d primary (%d, %d, %v, %v) replica (%d, %d, %v, %v)", step, id, pf, pt, pd, perr, rf, rt, rd, rerr)
					}
				}
			}
			apply()
			same("replica before")
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
			same("replica after")
			if _, _, deleted, err := replica.Rels().LatestStamps(rels[2]); err != nil || !deleted {
				t.Fatalf("replica deleted rel: deleted=%v, %v", deleted, err)
			}
		})
	}
}

// TestLatestStampsConcurrentWriters (-race): writers update and cascade a set
// of entities (only adding rows) while readers call LatestStamps, the first
// call building badger's sidecar under the writes. Each reader's answers for
// one ID never go down (a lost raise or a stale install would), and after the
// writers stop every answer equals the oracle.
func TestLatestStampsConcurrentWriters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(t *testing.T) storepkg.Store
	}{
		{"memory", func(*testing.T) storepkg.Store { return memory.New() }},
		{"badger", func(t *testing.T) storepkg.Store {
			st, err := badger.New(badger.Config{InMemory: true, FlushInterval: 2 * time.Millisecond, CacheCapacity: 16})
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			g, err := graphpkg.New(graphpkg.Config{Store: tc.store(t)})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			var nodes []types.NodeID
			var rels []types.RelID
			for i := range 16 {
				n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n.ID())
				if i > 0 {
					r, err := g.Rels().AddByID(ctx, "LINK", nodes[i-1], n.ID(), nil)
					if err != nil {
						t.Fatal(err)
					}
					rels = append(rels, r.ID())
				}
			}
			var stop atomic.Bool
			var wg sync.WaitGroup
			for w := range 4 {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(w))) // #nosec G404 -- deterministic test load
					for i := 0; i < 150; i++ {
						switch r.Intn(4) {
						case 0:
							_, _ = g.Nodes().Update(ctx, nodes[r.Intn(len(nodes))], map[string]any{"v": int64(i)})
						case 1:
							_, _ = g.Rels().Update(ctx, rels[r.Intn(len(rels))], map[string]any{"w": int64(i)})
						case 2:
							now, _ := g.Temporal().NowTx()
							_, _ = g.Temporal().SetNodeVersionInterval(ctx, nodes[r.Intn(len(nodes))], now+10, now+20, map[string]any{"v": int64(-i)})
						default:
							now, _ := g.Temporal().NowTx()
							_, _ = g.Temporal().SetRelVersionInterval(ctx, rels[r.Intn(len(rels))], now+10, now+20, map[string]any{"w": int64(-i)})
						}
					}
				}(w)
			}
			errs := make(chan error, 8)
			var readers sync.WaitGroup
			for range 3 {
				readers.Add(1)
				go func() {
					defer readers.Done()
					lastN := make([][2]types.Instant, len(nodes))
					lastR := make([][2]types.Instant, len(rels))
					for !stop.Load() {
						for i, id := range nodes {
							from, to, deleted, err := g.Nodes().LatestStamps(id)
							if err != nil || deleted || from < lastN[i][0] || to < lastN[i][1] {
								errs <- fmt.Errorf("node %d: (%d, %d, %v, %v) after (%d, %d)", id, from, to, deleted, err, lastN[i][0], lastN[i][1])
								return
							}
							lastN[i] = [2]types.Instant{from, to}
						}
						for i, id := range rels {
							from, to, deleted, err := g.Rels().LatestStamps(id)
							if err != nil || deleted || from < lastR[i][0] || to < lastR[i][1] {
								errs <- fmt.Errorf("rel %d: (%d, %d, %v, %v) after (%d, %d)", id, from, to, deleted, err, lastR[i][0], lastR[i][1])
								return
							}
							lastR[i] = [2]types.Instant{from, to}
						}
					}
				}()
			}
			wg.Wait()
			stop.Store(true)
			readers.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			for _, id := range nodes {
				want, _, err := nodeStampsOracle(g, id)
				if err != nil {
					t.Fatal(err)
				}
				from, to, deleted, err := g.Nodes().LatestStamps(id)
				if err != nil || from != want.from || to != want.to || deleted != want.deleted {
					t.Fatalf("final node %d = (%d, %d, %v, %v), want %+v", id, from, to, deleted, err, want)
				}
			}
			for _, id := range rels {
				want, _, err := relStampsOracle(g, id)
				if err != nil {
					t.Fatal(err)
				}
				from, to, deleted, err := g.Rels().LatestStamps(id)
				if err != nil || from != want.from || to != want.to || deleted != want.deleted {
					t.Fatalf("final rel %d = (%d, %d, %v, %v), want %+v", id, from, to, deleted, err, want)
				}
			}
		})
	}
}

// storeOverwriteLower rewrites the history row holding the entity's largest
// TxFrom with every stamp lowered, through the store's version door, and
// reports whether it found one (node when node is true, else rel).
func storeOverwriteLower(t *testing.T, g *graphpkg.Graph, st storepkg.Store, nid types.NodeID, rid types.RelID, node bool) bool {
	t.Helper()
	lower := func(tm *types.TemporalMetadata) {
		tm.TxFrom /= 2
		tm.TxTo, tm.DeletedAt = 0, 0
	}
	if node {
		hist, err := g.Nodes().History(nid)
		if err != nil || len(hist) == 0 {
			return false
		}
		top := hist[0]
		for _, h := range hist {
			if h.Temporal() != nil && (top.Temporal() == nil || h.Temporal().TxFrom > top.Temporal().TxFrom) {
				top = h
			}
		}
		if top.Temporal() == nil {
			return false
		}
		row := top.DeepCopy()
		lower(row.Temporal())
		if err := st.PutNodeVersion(nid, row.Version(), row); err != nil {
			t.Fatalf("store overwrite node %d v%d: %v", nid, row.Version(), err)
		}
		return true
	}
	hist, err := g.Rels().History(rid)
	if err != nil || len(hist) == 0 {
		return false
	}
	top := hist[0]
	for _, h := range hist {
		if h.Temporal() != nil && (top.Temporal() == nil || h.Temporal().TxFrom > top.Temporal().TxFrom) {
			top = h
		}
	}
	if top.Temporal() == nil {
		return false
	}
	row := top.DeepCopy()
	lower(row.Temporal())
	if err := st.PutRelVersion(rid, row.Version(), row); err != nil {
		t.Fatalf("store overwrite rel %d v%d: %v", rid, row.Version(), err)
	}
	return true
}
