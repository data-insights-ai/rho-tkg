package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 68b (sigma R10, the write side): a correction of an entity whose
// head entry is valid reads a few rows, not its history — the GraphTx
// rollback snapshot and the cascade's pre-chain both used to copy every row.

// r10bHistoryLimit is the history rows a correction with a valid entry may
// read: the base rows of its pieces and the resumption source (one each in
// sigma's shape), never the chain.
const r10bHistoryLimit = 4

func TestR10bCorrectionReadsBoundedHistory(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			cfg := be.cfg()
			cfg.AllowTxBackfill = true
			g, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			nid := r10Node(t, g, 300)
			rid := r10Rel(t, g, 300)
			read := func() {
				if _, err := g.Temporal.NodeEffectiveTimeline(nid, g.peekNow()); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.RelEffectiveTimeline(rid, g.peekNow()); err != nil {
					t.Fatal(err)
				}
			}
			count := func(what string, fn func() error) {
				t.Helper()
				read() // sigma reads the timeline before every correction
				rows := 0
				g.historyRead = func(n int) { rows += n }
				err := fn()
				g.historyRead = nil
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if rows > r10bHistoryLimit {
					t.Errorf("%s: %d history rows read (limit %d)", what, rows, r10bHistoryLimit)
				}
			}
			inTx := func(rollback bool, fn func(tx *GraphTx) error) func() error {
				return func() error {
					tx, err := g.BeginTx()
					if err != nil {
						return err
					}
					if err := fn(tx); err != nil {
						_ = tx.Rollback()
						return err
					}
					if rollback {
						return tx.Rollback()
					}
					return tx.Commit()
				}
			}
			for i := range 3 {
				p := map[string]any{"ov_c1": int64(-i)}
				count(fmt.Sprintf("node GraphTx correction %d, commit", i), inTx(false, func(tx *GraphTx) error {
					_, err := tx.SetNodeVersionInterval(nid, 1000, 2000, p)
					return err
				}))
				count(fmt.Sprintf("rel GraphTx correction %d, commit", i), inTx(false, func(tx *GraphTx) error {
					_, err := tx.SetRelVersionInterval(rid, 1000, 2000, p)
					return err
				}))
				count(fmt.Sprintf("node correction %d", i), func() error {
					_, err := g.Temporal.SetNodeVersionInterval(ctx, nid, 1000, 2000, p)
					return err
				})
				count(fmt.Sprintf("node correction %d at a caller instant", i), func() error {
					f, to, _, _ := g.Nodes.LatestStamps(nid)
					_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, 1000, 2000, p, max(f, to)+1)
					return err
				})
				count(fmt.Sprintf("rel excise %d", i), func() error {
					return g.Temporal.ExciseRelValidity(ctx, rid, 1900, 2000)
				})
				if g.historyTrim == nil {
					continue // sharded, tiered: no rollback trim, a rollback rewrites the history
				}
				count(fmt.Sprintf("node GraphTx correction %d, rollback", i), inTx(true, func(tx *GraphTx) error {
					_, err := tx.SetNodeVersionInterval(nid, 1000, 2000, p)
					return err
				}))
			}
		})
	}
}

// A GraphTx rollback keeps the rows above a gap that history compaction left
// directly above the current row (found by the 68b twin on the path without a
// head entry): compaction trims the oldest history rows, which after bounded
// corrections lie directly above the current row, and the snapshot probed
// only version current+1 (dense versions), took the trim path and the
// rollback trimmed the surviving pre-transaction rows.
func TestTxRollbackKeepsRowsAboveCompactionGap(t *testing.T) {
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(withTxFrom(be.cfg()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			g.headWriteOff = true // the snapshot without a head entry
			n, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"a": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			id := n.ID()
			for i := range 3 {
				if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 1000, 2000, map[string]any{"a": int64(i + 1)}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
				if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
					t.Skip("no history compaction")
				}
				t.Fatal(err)
			}
			versions := func() []uint32 {
				h, err := g.store.GetNodeHistory(id)
				if err != nil {
					t.Fatal(err)
				}
				var out []uint32
				for _, r := range h {
					out = append(out, r.Version())
				}
				return out
			}
			before := versions()
			if fmt.Sprint(before) != "[3]" {
				t.Fatalf("after compaction: history %v, want [3] (the gap above current v0)", before)
			}
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.SetNodeVersionInterval(id, 1000, 2000, map[string]any{"a": int64(9)}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if after := versions(); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("rollback: history %v, before the tx %v", after, before)
			}
		})
	}
}

// compactionGap builds node versions v0 (current) and v1..v3 (bounded
// corrections), then compacts to KeepVersions 1: history [3], a gap at v1.
func compactionGap(t *testing.T, g *Core) types.NodeID {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"a": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 1000, 2000, map[string]any{"a": int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
		if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			t.Skip("no history compaction")
		}
		t.Fatal(err)
	}
	if v := historyVersions(t, g, n.ID()); fmt.Sprint(v) != "[3]" {
		t.Fatalf("after compaction: history %v, want [3]", v)
	}
	return n.ID()
}

func historyVersions(t *testing.T, g *Core, id types.NodeID) []uint32 {
	t.Helper()
	h, err := g.store.GetNodeHistory(id)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint32
	for _, r := range h {
		out = append(out, r.Version())
	}
	return out
}

// relCompactionGap mirrors compactionGap for a relationship.
func relCompactionGap(t *testing.T, g *Core) types.RelID {
	t.Helper()
	ctx := context.Background()
	s, err := g.Nodes.Add(ctx, []string{"H"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"H"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.AddByID(ctx, "LINK", s.ID(), e.ID(), map[string]any{"a": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	rid := r.ID()
	for i := range 3 {
		if _, err := g.Temporal.SetRelVersionInterval(ctx, rid, 1000, 2000, map[string]any{"a": int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
		t.Fatal(err)
	}
	if v := relHistoryVersions(t, g, rid); fmt.Sprint(v) != "[3]" {
		t.Fatalf("rel after compaction: history %v, want [3]", v)
	}
	return rid
}

func relHistoryVersions(t *testing.T, g *Core, id types.RelID) []uint32 {
	t.Helper()
	h, err := g.store.GetRelHistory(id)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint32
	for _, r := range h {
		out = append(out, r.Version())
	}
	return out
}

// Backlog 70: every appending door allocates one above the highest version
// stored, also across a compaction gap directly above the current row (the
// allocator probed only current+1 and wrote v1 beside v3).
func TestVersionAllocatorAboveCompactionGap(t *testing.T) {
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(withTxFrom(be.cfg()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			id := compactionGap(t, g)
			u, err := g.Nodes.Update(ctx, id, map[string]any{"a": int64(7)})
			if err != nil {
				t.Fatal(err)
			}
			if u.Version() != 4 {
				t.Errorf("Update after the gap: version %d, want 4 (history %v)", u.Version(), historyVersions(t, g, id))
			}
			if err := g.Nodes.CloseVersion(ctx, id, g.peekNow()+10); err != nil {
				t.Fatal(err)
			}
			if cur, _ := g.store.GetNode(id); cur.Version() != 5 {
				t.Errorf("CloseVersion after the gap: version %d, want 5", cur.Version())
			}
			rid := relCompactionGap(t, g)
			r, err := g.Rels.Update(ctx, rid, map[string]any{"a": int64(7)})
			if err != nil {
				t.Fatal(err)
			}
			if r.Version() != 4 {
				t.Errorf("rel Update after the gap: version %d, want 4", r.Version())
			}
		})
	}
}

// Backlog 70 under 68b's rollback snapshot: with a head entry (top v3) a
// GraphTx that appends twice must write v4 and v5, so the rollback trim above
// v3 removes them (the allocator wrote v1 and v2, and v1 survived).
func TestTxRollbackAppendsAboveCompactionGap(t *testing.T) {
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(withTxFrom(be.cfg()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			id := compactionGap(t, g)
			if _, err := g.Temporal.NodeEffectiveTimeline(id, g.peekNow()); err != nil {
				t.Fatal(err)
			}
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			for i := range 2 {
				if _, err := tx.UpdateNode(id, map[string]any{"a": int64(10 + i)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			cur, err := g.store.GetNode(id)
			if err != nil {
				t.Fatal(err)
			}
			if h := historyVersions(t, g, id); cur.Version() != 0 || fmt.Sprint(h) != "[3]" {
				t.Fatalf("rollback: current v%d history %v, want v0 [3]", cur.Version(), h)
			}
			rid := relCompactionGap(t, g)
			if _, err := g.Temporal.RelEffectiveTimeline(rid, g.peekNow()); err != nil {
				t.Fatal(err)
			}
			if tx, err = g.BeginTx(); err != nil {
				t.Fatal(err)
			}
			for i := range 2 {
				if _, err := tx.UpdateRelationship(rid, map[string]any{"a": int64(10 + i)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			rcur, err := g.store.GetRelationship(rid)
			if err != nil {
				t.Fatal(err)
			}
			if h := relHistoryVersions(t, g, rid); rcur.Version() != 0 || fmt.Sprint(h) != "[3]" {
				t.Fatalf("rel rollback: current v%d history %v, want v0 [3]", rcur.Version(), h)
			}
		})
	}
}

// Backlog 70, the stores' as-of fast paths: after a compaction gap above the
// current row (current v0, history [3]) the row above the gap is the newest
// belief and must answer as of the head pin — point and bulk, nodes and
// relationships — as the as-of rule over the whole chain answers. The fast
// paths probed only current+1 and answered the current row.
func TestStoreAsOfAboveCompactionGap(t *testing.T) {
	for _, be := range r10Backends() {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(withTxFrom(be.cfg()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			st, ok := g.store.(storepkg.TransactionTimeQueryCapability)
			if !ok {
				t.Skip("no transaction-time doors")
			}
			nid := compactionGap(t, g)
			rid := relCompactionGap(t, g)
			pin := g.peekNow()
			// The reference: the as-of rule over the whole chain.
			h, err := g.store.GetNodeHistory(nid)
			if err != nil {
				t.Fatal(err)
			}
			cur, err := g.store.GetNode(nid)
			if err != nil {
				t.Fatal(err)
			}
			if want, ok := storeutil.SelectAsOfWithCurrent(h, cur, true, pin); !ok || want.Version() != 3 {
				t.Fatalf("the as-of rule answers v%d (%v), the test expects v3", want.Version(), ok)
			}
			n, err := st.NodeAsOf(nid, pin)
			if err != nil || n.Version() != 3 {
				t.Errorf("NodeAsOf: v%d %v, want v3 (the row above the gap)", n.Version(), err)
			}
			r, err := st.RelAsOf(rid, pin)
			if err != nil || r.Version() != 3 {
				t.Errorf("RelAsOf: v%d %v, want v3", r.Version(), err)
			}
			nodes, err := st.NodesAsOf(pin)
			if err != nil {
				t.Fatal(err)
			}
			for _, x := range nodes {
				if x.ID() == nid && x.Version() != 3 {
					t.Errorf("NodesAsOf: v%d, want v3", x.Version())
				}
			}
			rels, err := st.RelsAsOf(pin)
			if err != nil {
				t.Fatal(err)
			}
			for _, x := range rels {
				if x.ID() == rid && x.Version() != 3 {
					t.Errorf("RelsAsOf: v%d, want v3", x.Version())
				}
			}
		})
	}
}

// BenchmarkR10CutPerFact: sigma's cut per fact at exactly N versions — read
// the head-pin timeline, then one correction of the same validity in a
// GraphTx, committed. b.N facts of N versions each, one cut per fact
// (run with -benchtime=<k>x). warm: the fact's timeline was read after its
// last write (sigma's steady state: every cut reads); cold: never read, so
// the cut's read folds the history.
func BenchmarkR10CutPerFact(b *testing.B) {
	for _, be := range r10Backends() {
		for _, versions := range []int{1, 100, 300} {
			for _, warm := range []bool{true, false} {
				b.Run(fmt.Sprintf("%s/versions=%d/warm=%v", be.name, versions, warm), func(b *testing.B) {
					benchCutPerFact(b, be, versions, warm)
				})
			}
		}
	}
}

func benchCutPerFact(b *testing.B, be r10Backend, versions int, warm bool) {
	g, err := New(withTxFrom(be.cfg()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	ids := make([]nodeIDArg, b.N)
	for i := range ids {
		ids[i] = nodeIDArg{r10Node(b, g, versions)}
		if warm {
			if _, err := g.Temporal.NodeEffectiveTimeline(ids[i].id, g.peekNow()); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := ids[i].id
		if _, err := g.Temporal.NodeEffectiveTimeline(id, g.peekNow()); err != nil {
			b.Fatal(err)
		}
		tx, err := g.BeginTx()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.SetNodeVersionInterval(id, 1000, 2000, map[string]any{"ov_c1": int64(-i)}); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
