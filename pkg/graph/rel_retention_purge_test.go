package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 21: Admin().PurgeExpiredRels and the per-type retention gate.

type relRetentionBackend struct {
	name string
	// open returns the graph and the store it runs on.
	open func(t testing.TB, cfg graphpkg.Config) (*graphpkg.Graph, store.Store)
}

func relRetentionBackends() []relRetentionBackend {
	newGraph := func(t testing.TB, cfg graphpkg.Config, st store.Store) (*graphpkg.Graph, store.Store) {
		t.Helper()
		cfg.Store = st
		g, err := graphpkg.New(cfg)
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g, st
	}
	return []relRetentionBackend{
		{"memory", func(t testing.TB, cfg graphpkg.Config) (*graphpkg.Graph, store.Store) {
			return newGraph(t, cfg, memory.New())
		}},
		{"badger", func(t testing.TB, cfg graphpkg.Config) (*graphpkg.Graph, store.Store) {
			st, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return newGraph(t, cfg, st)
		}},
		{"sharded", func(t testing.TB, cfg graphpkg.Config) (*graphpkg.Graph, store.Store) {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return newGraph(t, cfg, st)
		}},
		{"tiered", func(t testing.TB, cfg graphpkg.Config) (*graphpkg.Graph, store.Store) {
			// Machine and Overview are reference labels (always-open ref shard),
			// Event nodes land on event shards: every Event<->Machine edge is
			// cross-shard, in both directions (entity on either side).
			st, err := tiered.New(tiered.Config{
				InMemory:      true,
				RefLabels:     []string{"Machine", "Overview"},
				ShardWindow:   7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			return newGraph(t, cfg, st)
		}},
	}
}

// assertNoRelResidue fails when a purged relationship is still named by the
// store's indexes that the public doors resolve away: the ex-adjacency of
// every fixture node and, on tiered, the reference shard's raw adjacency legs
// (a cross-shard relationship's in-leg lives on its end node's shard).
func assertNoRelResidue(t *testing.T, st store.Store, f relRetentionFixture) {
	t.Helper()
	purged := make(map[types.RelID]string, len(f.purged))
	for name, id := range f.purged {
		purged[id] = name
	}
	nodes := []types.NodeID{f.machine.ID(), f.overview.ID()}
	for _, e := range f.events {
		nodes = append(nodes, e.ID())
	}
	if ex, ok := st.(store.ExAdjacencyCapability); ok {
		for _, n := range nodes {
			for _, incoming := range []bool{false, true} {
				if err := ex.ForEachExAdjacentRelID(n, incoming, func(id types.RelID) bool {
					if name, gone := purged[id]; gone {
						t.Errorf("ex-adjacency of node %d (incoming=%v) still names purged %s", n, incoming, name)
					}
					return true
				}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if ts, ok := st.(*tiered.Store); ok {
		ref := ts.RefShardForTest()
		for _, n := range nodes {
			legs := append(ref.IncomingRelIDs(n.SnowflakeID(), 0), ref.OutgoingRelIDs(n.SnowflakeID())...)
			for _, raw := range legs {
				if name, gone := purged[types.RelID(raw)]; gone {
					t.Errorf("reference shard still holds a leg of purged %s at node %d", name, n)
				}
			}
		}
	}
}

// relRetentionFixture is the sigma-tkgd overview shape: raw EVENT
// relationships age out, the OV_PAIR overview relationship and the Overview
// node stay.
type relRetentionFixture struct {
	machine, overview *types.Node
	events            []*types.Node
	// purged: EVENT rels minted below Before, in every lifecycle.
	purged map[string]types.RelID
	// kept: EVENT rels minted at or after Before.
	boundary, late types.RelID
	ovPair         types.RelID
	before         types.Instant
	// pinLow is a transaction pin below Before at which the purged rels were
	// believed; pinMid is one at which every fixture row is believed.
	pinLow types.Instant
}

func buildRelRetentionFixture(t *testing.T, g *graphpkg.Graph) relRetentionFixture {
	t.Helper()
	ctx := context.Background()
	now := func() types.Instant {
		n, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	must := func(r *types.Relationship, err error) *types.Relationship {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	f := relRetentionFixture{purged: map[string]types.RelID{}}
	var err error
	if f.machine, err = g.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h"}); err != nil {
		t.Fatal(err)
	}
	if f.overview, err = g.Nodes().Add(ctx, []string{"Overview"}, map[string]any{"cut": int64(1)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		e, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		f.events = append(f.events, e)
	}
	f.ovPair = must(g.Rels().Add(ctx, "OV_PAIR", f.overview, f.machine, map[string]any{"n": int64(1)})).ID()

	// Live, both cross-shard directions on tiered.
	f.purged["live-out"] = must(g.Rels().Add(ctx, "EVENT", f.events[0], f.machine, map[string]any{"k": "a"})).ID()
	f.purged["live-in"] = must(g.Rels().Add(ctx, "EVENT", f.machine, f.events[1], map[string]any{"k": "b"})).ID()
	// Mutated after creation: a history row beside the current one.
	mut := must(g.Rels().Add(ctx, "EVENT", f.events[2], f.machine, map[string]any{"k": "c"}))
	f.pinLow = mut.Temporal().TxFrom
	must(g.Rels().Update(ctx, mut.ID(), map[string]any{"k": "c2"}))
	f.purged["mutated"] = mut.ID()
	// Ended by a plain delete: history only.
	del := must(g.Rels().Add(ctx, "EVENT", f.events[3], f.machine, map[string]any{"k": "d"}))
	if err := g.Rels().Delete(ctx, del.ID()); err != nil {
		t.Fatal(err)
	}
	f.purged["deleted"] = del.ID()
	// Ended by DeleteWithTx at a caller instant.
	dwt := must(g.Rels().Add(ctx, "EVENT", f.events[3], f.machine, map[string]any{"k": "e"}))
	if err := g.Rels().DeleteWithTx(ctx, dwt.ID(), now()); err != nil {
		t.Fatal(err)
	}
	f.purged["delete-with-tx"] = dwt.ID()
	// Ended by a node cascade at a caller instant (the node itself stays a
	// history row: nodes are untouched by a relationship purge).
	cas := must(g.Rels().Add(ctx, "EVENT", f.events[5], f.machine, map[string]any{"k": "f"}))
	if err := g.Nodes().DeleteWithTx(ctx, f.events[5].ID(), now()); err != nil {
		t.Fatal(err)
	}
	f.purged["node-cascade"] = cas.ID()
	// Retracted ("never true").
	ret := must(g.Rels().Add(ctx, "EVENT", f.events[4], f.machine, map[string]any{"k": "g"}))
	if err := g.Rels().Retract(ctx, ret.ID()); err != nil {
		t.Fatal(err)
	}
	f.purged["retracted"] = ret.ID()
	// Deleted and re-imported under the same ID (a second life).
	reimp := must(g.Rels().Add(ctx, "EVENT", f.events[0], f.machine, map[string]any{"k": "h"}))
	if err := g.Rels().Delete(ctx, reimp.ID()); err != nil {
		t.Fatal(err)
	}
	must(g.Rels().Import(ctx, reimp.ID(), "EVENT", f.events[1], f.machine, map[string]any{"k": "h2"}))
	f.purged["reimported"] = reimp.ID()

	// The boundary: every rel above was minted, and every stamp above taken,
	// strictly before it (the commit clock may run ahead of the wall clock).
	for hi := now(); types.Instant(time.Now().UnixMilli()) <= hi+1; {
		time.Sleep(time.Millisecond)
	}
	b := must(g.Rels().Add(ctx, "EVENT", f.events[0], f.machine, map[string]any{"k": "boundary"}))
	f.boundary = b.ID()
	f.before = b.ID().MintInstant()
	for name, id := range f.purged {
		if id.MintInstant() >= f.before {
			t.Fatalf("fixture: %s minted at %d, not below Before %d", name, id.MintInstant(), f.before)
		}
	}
	if f.pinLow >= f.before {
		t.Fatalf("fixture: pinLow %d not below Before %d", f.pinLow, f.before)
	}
	f.late = must(g.Rels().Add(ctx, "EVENT", f.machine, f.events[2], map[string]any{"k": "late"})).ID()
	return f
}

// relSetOf turns a (rels, err) result into an ID set, failing t on err.
func relSetOf(t *testing.T) func([]*types.Relationship, error) map[types.RelID]struct{} {
	return func(rels []*types.Relationship, err error) map[types.RelID]struct{} {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		out := make(map[types.RelID]struct{}, len(rels))
		for _, r := range rels {
			out[r.ID()] = struct{}{}
		}
		return out
	}
}

func assertRelIDs(t *testing.T, what string, got map[types.RelID]struct{}, want ...types.RelID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d rels %v, want %v", what, len(got), got, want)
	}
	for _, id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("%s: missing %d (got %v)", what, id, got)
		}
	}
}

func purgeRels(t *testing.T, g *graphpkg.Graph, typ string, before types.Instant) adminpkg.RelPurgeReport {
	t.Helper()
	rep, err := g.Admin().PurgeExpiredRels(context.Background(), adminpkg.RelPurgePolicy{Type: typ, Mode: adminpkg.PurgeByAge, Before: before})
	if err != nil {
		t.Fatalf("PurgeExpiredRels(%s, %d): %v", typ, before, err)
	}
	return rep
}

// TestPurgeExpiredRelsByAge: a relationship purge by mint age removes every
// relationship of the type minted below Before — live, mutated, deleted,
// deleted at a caller instant, cascade-ended, retracted and re-imported —
// with its rows, legs, indexes and whole history, keeps the one minted
// exactly at Before and later ones, keeps other types and every node, and a
// rerun is a no-op. Faulty implementations caught: a purge of live rows
// only (ended rels keep their history), a purge that counts the boundary
// rel (< vs <=), one that leaves a leg or a counter behind, one that touches
// nodes or another type.
func TestPurgeExpiredRelsByAge(t *testing.T) {
	ctx := context.Background()
	for _, be := range relRetentionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, st := be.open(t, graphpkg.Config{AllowRetentionPurge: true, AllowTxBackfill: true})
			f := buildRelRetentionFixture(t, g)
			eventHistBefore, _, err := g.Stats().RelHistoryCountByType("EVENT")
			if err != nil {
				t.Fatal(err)
			}
			histBefore, histOK, err := g.Stats().HistoryCounts()
			if err != nil {
				t.Fatal(err)
			}
			withHistory := 0
			for _, id := range f.purged {
				if has, _ := g.Rels().HasHistory(id); has {
					withHistory++
				}
			}

			rep := purgeRels(t, g, "EVENT", f.before)
			if rep.RelsPurged != len(f.purged) {
				t.Fatalf("RelsPurged = %d, want %d", rep.RelsPurged, len(f.purged))
			}
			if rep.Watermark != f.before {
				t.Fatalf("Watermark = %d, want %d", rep.Watermark, f.before)
			}

			// Survivors, current and pinned at or above Before (two-phase).
			pin, _ := g.Temporal().NowTx()
			assertRelIDs(t, "EVENT current", relSetOf(t)(g.Rels().ByType("EVENT", store.QueryOpts{})), f.boundary, f.late)
			assertRelIDs(t, "EVENT pinned", relSetOf(t)(g.Rels().ByType("EVENT", store.QueryOpts{TxPin: pin})), f.boundary, f.late)
			assertRelIDs(t, "OV_PAIR", relSetOf(t)(g.Rels().ByType("OV_PAIR", store.QueryOpts{TxPin: pin})), f.ovPair)
			if n, err := g.Rels().CountByType("EVENT"); err != nil || n != 2 {
				t.Fatalf("CountByType(EVENT) = %d, %v; want 2", n, err)
			}

			// Purged: no row, no history, no leg.
			for name, id := range f.purged {
				if _, err := g.Rels().Get(ctx, id); !errors.Is(err, graphpkg.ErrRelNotFound) {
					t.Fatalf("%s: Get err = %v, want ErrRelNotFound", name, err)
				}
				if has, err := g.Rels().HasHistory(id); err != nil || has {
					t.Fatalf("%s: HasHistory = %v, %v; want false", name, has, err)
				}
				if h, err := g.Rels().History(id); err != nil || len(h) != 0 {
					t.Fatalf("%s: History = %d rows, %v; want none", name, len(h), err)
				}
			}
			assertRelIDs(t, "machine in EVENT", relSetOf(t)(g.Rels().Incoming(f.machine.ID(), "EVENT")), f.boundary)
			assertRelIDs(t, "machine out EVENT", relSetOf(t)(g.Rels().Outgoing(f.machine.ID(), "EVENT")), f.late)
			assertRelIDs(t, "e0 out", relSetOf(t)(g.Rels().Outgoing(f.events[0].ID(), "")), f.boundary)
			assertRelIDs(t, "e1 out", relSetOf(t)(g.Rels().Outgoing(f.events[1].ID(), "")))
			assertRelIDs(t, "e1 in", relSetOf(t)(g.Rels().Incoming(f.events[1].ID(), "")))
			assertNoRelResidue(t, st, f)
			if histOK {
				histAfter, _, err := g.Stats().HistoryCounts()
				if err != nil {
					t.Fatal(err)
				}
				if want := (store.HistoryCounts{Nodes: histBefore.Nodes, Rels: histBefore.Rels - withHistory}); histAfter != want {
					t.Fatalf("HistoryCounts %+v -> %+v, want %+v (no ghost, nodes untouched)", histBefore, histAfter, want)
				}
			}
			if eventHistAfter, ok, err := g.Stats().RelHistoryCountByType("EVENT"); err != nil {
				t.Fatal(err)
			} else if ok && eventHistAfter >= eventHistBefore {
				t.Fatalf("RelHistoryCountByType(EVENT) %d -> %d, want fewer", eventHistBefore, eventHistAfter)
			}

			// Nodes untouched (the cascade-deleted event stays deleted).
			for i, e := range f.events {
				_, err := g.Nodes().Get(ctx, e.ID())
				if i == 5 {
					if !errors.Is(err, graphpkg.ErrNodeNotFound) {
						t.Fatalf("cascade-deleted event: %v", err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("event %d gone after a relationship purge: %v", i, err)
				}
			}
			if _, err := g.Nodes().Get(ctx, f.overview.ID()); err != nil {
				t.Fatalf("overview: %v", err)
			}
			if has, _ := g.Nodes().HasHistory(f.events[5].ID()); !has {
				t.Fatal("the cascade-deleted event lost its own history")
			}

			// Rerun: no-op, same watermark.
			rep2 := purgeRels(t, g, "EVENT", f.before)
			if rep2.RelsPurged != 0 || rep2.Watermark != f.before {
				t.Fatalf("rerun = %+v, want 0 purged at watermark %d", rep2, f.before)
			}
		})
	}
}

// TestPurgeExpiredRelsByAge_Refusals: the door's gates, each refusing before
// any write. Faulty implementations caught: a door that creates the unknown
// type's token, accepts ByValidTo, or skips a gate.
func TestPurgeExpiredRelsByAge_Refusals(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	cases := []struct {
		name   string
		policy adminpkg.RelPurgePolicy
		want   error
	}{
		{"empty type", adminpkg.RelPurgePolicy{Before: farFuture}, graphpkg.ErrInvalidPurgePolicy},
		{"zero before", adminpkg.RelPurgePolicy{Type: "EVENT"}, graphpkg.ErrInvalidPurgePolicy},
		{"negative before", adminpkg.RelPurgePolicy{Type: "EVENT", Before: -1}, graphpkg.ErrInvalidPurgePolicy},
		{"by valid-to", adminpkg.RelPurgePolicy{Type: "EVENT", Mode: adminpkg.PurgeByValidTo, Before: farFuture}, graphpkg.ErrInvalidPurgePolicy},
		{"unknown mode", adminpkg.RelPurgePolicy{Type: "EVENT", Mode: 9, Before: farFuture}, graphpkg.ErrInvalidPurgePolicy},
	}
	for _, tc := range cases {
		if _, err := g.Admin().PurgeExpiredRels(ctx, tc.policy); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if _, err := g.Admin().PurgeExpiredRels(nil, adminpkg.RelPurgePolicy{Type: "EVENT", Before: farFuture}); !errors.Is(err, graphpkg.ErrNilContext) { //nolint:staticcheck // nil ctx is the case under test
		t.Fatalf("nil ctx: %v", err)
	}
	// Unknown type: a no-op that creates no token.
	rep, err := g.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "NEVER", Before: farFuture})
	if err != nil || rep.RelsPurged != 0 || rep.Watermark != 0 {
		t.Fatalf("unknown type = %+v, %v; want a zero no-op", rep, err)
	}
	if counts, err := g.Stats().AllRelTypeCounts(); err != nil {
		t.Fatal(err)
	} else if _, ok := counts["NEVER"]; ok {
		t.Fatal("an unknown-type purge registered the type")
	}

	off, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer off.Close()
	if _, err := off.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: farFuture}); !errors.Is(err, graphpkg.ErrRetentionPurgeDisabled) {
		t.Fatalf("disabled: %v", err)
	}
	ro, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := ro.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: farFuture}); !errors.Is(err, graphpkg.ErrReadOnlyReplica) {
		t.Fatalf("read-only replica: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := g.Admin().PurgeExpiredRels(cctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: farFuture}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: %v", err)
	}
}

// TestPurgeExpiredRelsByAge_ManyChunks: more relationships than one store
// chunk (256) all go, on every backend.
func TestPurgeExpiredRelsByAge_ManyChunks(t *testing.T) {
	ctx := context.Background()
	for _, be := range relRetentionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, _ := be.open(t, graphpkg.Config{AllowRetentionPurge: true})
			m, _ := g.Nodes().Add(ctx, []string{"Machine"}, nil)
			e, _ := g.Nodes().Add(ctx, []string{"Event"}, nil)
			const n = 300
			for i := 0; i < n; i++ {
				r, err := g.Rels().Add(ctx, "EVENT", e, m, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				if i%3 == 0 {
					if err := g.Rels().Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			rep := purgeRels(t, g, "EVENT", farFuture)
			if rep.RelsPurged != n {
				t.Fatalf("RelsPurged = %d, want %d", rep.RelsPurged, n)
			}
			if c, err := g.Rels().CountByType("EVENT"); err != nil || c != 0 {
				t.Fatalf("CountByType = %d, %v", c, err)
			}
			if c, ok, err := g.Stats().RelHistoryCountByType("EVENT"); err != nil || (ok && c != 0) {
				t.Fatalf("RelHistoryCountByType = %d, %v, %v; want 0", c, ok, err)
			}
		})
	}
}

// TestRelRetentionWatermarkGatesScans: after a purge of type T, a read of T
// (or of every type) pinned below T's watermark fails closed with
// ErrRetentionExpired, while reads of other types and of nodes at the same
// pin are answered; the watermark is max-monotonic, survives a reopen, is
// independent of the node purge (whose own gate is unchanged) and is cleared
// by Reset. Faulty implementations caught: a graph-wide rel watermark (the
// OV_PAIR and Overview reads fail), no rel watermark (the EVENT reads answer
// an incomplete set), a lowered watermark, a watermark lost at reopen.
func TestRelRetentionWatermarkGatesScans(t *testing.T) {
	ctx := context.Background()
	for _, be := range relRetentionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, _ := be.open(t, graphpkg.Config{AllowRetentionPurge: true, AllowTxBackfill: true, AllowReset: true})
			f := buildRelRetentionFixture(t, g)
			purgeRels(t, g, "EVENT", f.before)
			assertRelGateMatrix(t, g, f)
			assertTxRelGates(t, g, f)

			// Max-monotonic: a lower boundary neither lowers the watermark nor
			// reopens the reads.
			if rep := purgeRels(t, g, "EVENT", f.pinLow-10); rep.Watermark != f.before {
				t.Fatalf("lower purge watermark = %d, want %d", rep.Watermark, f.before)
			}
			assertRelGateMatrix(t, g, f)

			// The node purge gate is unchanged: a purge of label Event fails
			// every scan, node and relationship, below its own watermark.
			pin, _ := g.Temporal().NowTx()
			if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: pin}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().ByLabel("Overview", store.QueryOpts{TxPin: pin - 1}); !errors.Is(err, graphpkg.ErrRetentionExpired) {
				t.Fatalf("Overview below the node watermark: %v, want ErrRetentionExpired", err)
			}
			if _, err := g.Rels().ByType("OV_PAIR", store.QueryOpts{TxPin: pin - 1}); !errors.Is(err, graphpkg.ErrRetentionExpired) {
				t.Fatalf("OV_PAIR below the node watermark: %v, want ErrRetentionExpired", err)
			}
			if _, err := g.Nodes().ByLabel("Overview", store.QueryOpts{TxPin: pin}); err != nil {
				t.Fatalf("Overview at the node watermark: %v", err)
			}

			// Reset clears every rel watermark: a reused type token starts clean.
			if err := g.Admin().Reset(); err != nil {
				t.Fatal(err)
			}
			m, _ := g.Nodes().Add(ctx, []string{"Machine"}, nil)
			m2, _ := g.Nodes().Add(ctx, []string{"Machine"}, nil)
			if _, err := g.Rels().Add(ctx, "EVENT", m, m2, nil); err != nil {
				t.Fatal(err)
			}
			// Another type's purge lifts the fast gate again: the reused EVENT
			// token must not meet its pre-Reset watermark behind it.
			if _, err := g.Rels().Add(ctx, "OTHER", m, m2, nil); err != nil {
				t.Fatal(err)
			}
			purgeRels(t, g, "OTHER", f.before)
			if _, err := g.Rels().ByType("EVENT", store.QueryOpts{TxPin: f.pinLow}); err != nil {
				t.Fatalf("EVENT after Reset: %v (a stale rel watermark survived Reset)", err)
			}
			if _, err := g.Rels().ByType("OTHER", store.QueryOpts{TxPin: f.pinLow}); !errors.Is(err, graphpkg.ErrRetentionExpired) {
				t.Fatalf("OTHER below its post-Reset watermark: %v", err)
			}
		})
	}
}

// assertRelGateMatrix checks every door family at f.pinLow (below the EVENT
// watermark) and at a pin above it.
func assertRelGateMatrix(t *testing.T, g *graphpkg.Graph, f relRetentionFixture) {
	t.Helper()
	low := store.QueryOpts{TxPin: f.pinLow}
	noop := func(*types.Relationship) bool { return true }
	expired := []struct {
		name string
		run  func() error
	}{
		{"ByType", func() error { _, err := g.Rels().ByType("EVENT", low); return err }},
		{"ForEachByType", func() error { return g.Rels().ForEachByType("EVENT", low, noop) }},
		{"ByTypeAndProperty", func() error { _, err := g.Rels().ByTypeAndProperty("EVENT", "k", "a", low); return err }},
		{"CountByTypeAt", func() error { _, err := g.Rels().CountByTypeAt("EVENT", low); return err }},
		{"All", func() error { _, err := g.Rels().All(low); return err }},
		{"ForEach", func() error { return g.Rels().ForEach(low, noop) }},
		{"AdjacentRelAt typed", func() error {
			return g.Rels().ForEachAdjacentRelAt(f.machine.ID(), "EVENT", true, low, noop)
		}},
		{"AdjacentRelAt untyped", func() error {
			return g.Rels().ForEachAdjacentRelAt(f.machine.ID(), "", true, low, noop)
		}},
		{"RelsAsOf", func() error { _, err := g.Temporal().RelsAsOf(f.pinLow); return err }},
		{"RelAsOf purged", func() error { _, err := g.Temporal().RelAsOf(f.purged["live-out"], f.pinLow); return err }},
		{"RelAtTx purged", func() error {
			_, err := g.Temporal().RelAtTx(f.purged["mutated"], f.pinLow, f.pinLow)
			return err
		}},
		{"RelEffectiveTimeline purged", func() error {
			_, err := g.Temporal().RelEffectiveTimeline(f.purged["live-in"], f.pinLow)
			return err
		}},
		{"ForEachRelEffectiveByType", func() error {
			return g.Temporal().ForEachRelEffectiveByType("EVENT", f.pinLow, func(temporal.RelSegment) bool { return true })
		}},
		{"ForEachRelEffective", func() error {
			return g.Temporal().ForEachRelEffective(f.pinLow, func(temporal.RelSegment) bool { return true })
		}},
		{"OutgoingForNodesAtPin", func() error {
			_, err := g.Rels().OutgoingForNodesAtPin([]types.NodeID{f.events[0].ID()}, "EVENT", f.pinLow)
			return err
		}},
		{"IncomingForNodesAtPin untyped", func() error {
			_, err := g.Rels().IncomingForNodesAtPin([]types.NodeID{f.machine.ID()}, "", f.pinLow)
			return err
		}},
		{"OutgoingForNodesAtTx", func() error {
			_, err := g.Rels().OutgoingForNodesAtTx([]types.NodeID{f.events[0].ID()}, "EVENT", f.pinLow)
			return err
		}},
		{"IncomingForNodesAtTx untyped", func() error {
			_, err := g.Rels().IncomingForNodesAtTx([]types.NodeID{f.machine.ID()}, "", f.pinLow)
			return err
		}},
		{"AdjacentEndpointAt typed", func() error {
			return g.Rels().ForEachAdjacentEndpointAt(f.machine.ID(), "EVENT", true, low, func(types.RelID, types.NodeID) bool { return true })
		}},
		{"ForEachRelEffectiveAtNodes typed", func() error {
			return g.Temporal().ForEachRelEffectiveAtNodes([]types.NodeID{f.machine.ID()}, temporal.Incoming, []string{"EVENT"}, f.pinLow, func(temporal.RelSegment) bool { return true })
		}},
		{"RelsEffectiveByIDs purged", func() error {
			return g.Temporal().RelsEffectiveByIDs([]types.RelID{f.purged["deleted"]}, f.pinLow, func(temporal.RelSegment) bool { return true })
		}},
		{"RelsByTypeAt", func() error { _, err := g.Temporal().RelsByTypeAt("EVENT", f.pinLow); return err }},
		{"RelsByTypePropertyAt", func() error {
			_, err := g.Temporal().RelsByTypePropertyAt("EVENT", "k", "a", f.pinLow)
			return err
		}},
		{"RelsByTypePropertyDuring", func() error {
			_, err := g.Temporal().RelsByTypePropertyDuring("EVENT", "k", "a", f.pinLow, f.pinLow+1)
			return err
		}},
		{"RelsAt", func() error { _, err := g.Temporal().RelsAt(f.pinLow); return err }},
		{"RelsDuring", func() error { _, err := g.Temporal().RelsDuring(f.pinLow, f.pinLow+1); return err }},
		{"RelsRelating", func() error {
			_, err := g.Temporal().RelsRelating(f.pinLow, f.pinLow+1, types.Before.Set())
			return err
		}},
		{"RelsAtTx", func() error { _, err := g.Temporal().RelsAtTx(f.pinLow, f.pinLow); return err }},
		{"RelsDuringTx", func() error { _, err := g.Temporal().RelsDuringTx(f.pinLow, f.pinLow+1, f.pinLow); return err }},
		{"NeighborsAt", func() error { _, err := g.Temporal().NeighborsAt(f.machine.ID(), f.pinLow); return err }},
		{"OutgoingRelsAt", func() error { _, err := g.Temporal().OutgoingRelsAt(f.machine.ID(), f.pinLow); return err }},
		{"IncomingRelsAt", func() error { _, err := g.Temporal().IncomingRelsAt(f.machine.ID(), f.pinLow); return err }},
		{"Snapshot", func() error { _, err := g.Temporal().Snapshot(f.pinLow); return err }},
		{"Diff", func() error { _, err := g.Temporal().Diff(f.pinLow, f.before); return err }},
		{"DiffCallback", func() error {
			return g.Temporal().DiffCallback(f.pinLow, f.before, temporal.DiffHandlers{OnRelCreated: func(*types.Relationship) error { return nil }})
		}},
	}
	for _, d := range expired {
		if err := d.run(); !errors.Is(err, graphpkg.ErrRetentionExpired) {
			t.Errorf("%s below the EVENT watermark: err = %v, want ErrRetentionExpired", d.name, err)
		}
	}
	answered := []struct {
		name string
		run  func() error
	}{
		{"OV_PAIR ByType", func() error {
			got := relSetOf(t)(g.Rels().ByType("OV_PAIR", store.QueryOpts{TxPin: f.pinLow}))
			assertRelIDs(t, "OV_PAIR at pinLow", got, f.ovPair)
			return nil
		}},
		{"OV_PAIR CountByTypeAt", func() error { _, err := g.Rels().CountByTypeAt("OV_PAIR", low); return err }},
		{"OV_PAIR AdjacentRelAt", func() error {
			return g.Rels().ForEachAdjacentRelAt(f.machine.ID(), "OV_PAIR", true, low, noop)
		}},
		{"OV_PAIR RelAsOf", func() error { _, err := g.Temporal().RelAsOf(f.ovPair, f.pinLow); return err }},
		{"OV_PAIR effective", func() error {
			return g.Temporal().ForEachRelEffectiveByType("OV_PAIR", f.pinLow, func(temporal.RelSegment) bool { return true })
		}},
		{"OV_PAIR RelsByTypePropertyAt", func() error {
			_, err := g.Temporal().RelsByTypePropertyAt("OV_PAIR", "n", int64(1), f.pinLow)
			return err
		}},
		{"OV_PAIR OutgoingForNodesAtTx", func() error {
			_, err := g.Rels().OutgoingForNodesAtTx([]types.NodeID{f.overview.ID()}, "OV_PAIR", f.pinLow)
			return err
		}},
		{"OV_PAIR RelsEffectiveByIDs", func() error {
			return g.Temporal().RelsEffectiveByIDs([]types.RelID{f.ovPair}, f.pinLow, func(temporal.RelSegment) bool { return true })
		}},
		{"OV_PAIR ForEachRelEffectiveAtNodes", func() error {
			return g.Temporal().ForEachRelEffectiveAtNodes([]types.NodeID{f.machine.ID()}, temporal.Incoming, []string{"OV_PAIR"}, f.pinLow, func(temporal.RelSegment) bool { return true })
		}},
		{"DiffCallback nodes only", func() error {
			return g.Temporal().DiffCallback(f.pinLow, f.before, temporal.DiffHandlers{OnNodeCreated: func(*types.Node) error { return nil }})
		}},
		{"NodesAt", func() error { _, err := g.Temporal().NodesAt(f.pinLow); return err }},
		{"NodesAtTx", func() error { _, err := g.Temporal().NodesAtTx(f.pinLow, f.pinLow); return err }},
		{"Overview ByLabel", func() error { _, err := g.Nodes().ByLabel("Overview", low); return err }},
		{"Nodes All", func() error { _, err := g.Nodes().All(low); return err }},
		{"NodesAsOf", func() error { _, err := g.Temporal().NodesAsOf(f.pinLow); return err }},
		{"NodeAsOf", func() error { _, err := g.Temporal().NodeAsOf(f.overview.ID(), f.pinLow); return err }},
		{"EVENT at Before", func() error {
			_, err := g.Rels().ByType("EVENT", store.QueryOpts{TxPin: f.before})
			return err
		}},
	}
	for _, d := range answered {
		if err := d.run(); err != nil {
			t.Errorf("%s: err = %v, want an answer", d.name, err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

// assertTxRelGates checks the transaction-side mirrors of the gated doors.
func assertTxRelGates(t *testing.T, g *graphpkg.Graph, f relRetentionFixture) {
	t.Helper()
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	doors := []struct {
		name string
		run  func() error
	}{
		{"tx RelsByType", func() error { _, err := tx.RelsByType("EVENT", store.QueryOpts{TxPin: f.pinLow}); return err }},
		{"tx AllRels", func() error { _, err := tx.AllRels(store.QueryOpts{TxPin: f.pinLow}); return err }},
		{"tx RelAsOf", func() error { _, err := tx.RelAsOf(f.purged["live-out"], f.pinLow); return err }},
		{"tx RelsAsOf", func() error { _, err := tx.RelsAsOf(f.pinLow); return err }},
		{"tx RelsAt", func() error { _, err := tx.RelsAt(f.pinLow); return err }},
		{"tx RelsDuring", func() error { _, err := tx.RelsDuring(f.pinLow, f.pinLow+1); return err }},
		{"tx NeighborsAt", func() error { _, err := tx.NeighborsAt(f.machine.ID(), f.pinLow); return err }},
		{"tx OutgoingRelsAt", func() error { _, err := tx.OutgoingRelsAt(f.machine.ID(), f.pinLow); return err }},
		{"tx RelsByTypePropertyAt", func() error {
			_, err := tx.RelsByTypePropertyAt("EVENT", "k", "a", f.pinLow)
			return err
		}},
		{"tx Snapshot", func() error { _, err := tx.Snapshot(f.pinLow); return err }},
	}
	for _, d := range doors {
		if err := d.run(); !errors.Is(err, graphpkg.ErrRetentionExpired) {
			t.Errorf("%s below the EVENT watermark: err = %v, want ErrRetentionExpired", d.name, err)
		}
	}
	if _, err := tx.RelsByType("OV_PAIR", store.QueryOpts{TxPin: f.pinLow}); err != nil {
		t.Errorf("tx RelsByType(OV_PAIR): %v", err)
	}
}

// TestRelPointGateAnswersLaterIDs (backlog 21 F3): a relationship ID minted
// at or after every relationship watermark cannot have been purged, so a
// point read of it at a low pin answers normally (here: no version at the
// pin), not ErrRetentionExpired.
func TestRelPointGateAnswersLaterIDs(t *testing.T) {
	g, _ := relRetentionBackends()[0].open(t, graphpkg.Config{AllowRetentionPurge: true, AllowTxBackfill: true})
	f := buildRelRetentionFixture(t, g)
	purgeRels(t, g, "EVENT", f.before)
	unknown := g.Rels().NextID()
	for name, id := range map[string]types.RelID{"unknown later ID": unknown, "survivor minted at Before": f.boundary} {
		if _, err := g.Temporal().RelAsOf(id, f.pinLow); err == nil || errors.Is(err, graphpkg.ErrRetentionExpired) {
			t.Fatalf("RelAsOf(%s) = %v, want the door's own answer (no version at the pin)", name, err)
		}
	}
	if _, err := g.Temporal().RelAsOf(f.purged["deleted"], f.pinLow); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("RelAsOf(purged ID) = %v, want ErrRetentionExpired", err)
	}
}

// TestRelRetentionWatermarkSurvivesReopen: the per-type watermark is
// persisted and reloaded at open, through the clean-reopen open image
// (v4.51.3) and a reopen without it; the rows a crash left behind (watermark
// written, purge not run) stay gated and a rerun removes them.
func TestRelRetentionWatermarkSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *graphpkg.Graph {
		g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir, AllowRetentionPurge: true, AllowTxBackfill: true})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := open()
	f := buildRelRetentionFixture(t, g)
	purgeRels(t, g, "EVENT", f.before)
	assertRelGateMatrix(t, g, f)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		g = open()
		assertRelGateMatrix(t, g, f)
		if rep := purgeRels(t, g, "EVENT", f.pinLow-5); rep.Watermark != f.before {
			t.Fatalf("round %d: watermark after reopen + lower purge = %d, want %d", round, rep.Watermark, f.before)
		}
		if round == 0 {
			// Add a below-boundary row after the purge (an import / replica
			// shape): gated, and the rerun removes it.
			if _, err := g.Rels().Import(ctx, f.purged["deleted"], "EVENT", f.events[0], f.machine, nil); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := g.Rels().Get(ctx, f.purged["deleted"]); !errors.Is(err, graphpkg.ErrRelNotFound) {
				t.Fatalf("re-imported row survived the rerun: %v", err)
			}
		}
		purgeRels(t, g, "EVENT", f.before)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
