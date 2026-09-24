package graph_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Found by the ADR-0011 S2 differential oracle (2026-09-24, seg/s2 backlog):
// the TxAt-only doors and the open-ended (end == 0) interval doors read valid
// time at "now". That "now" was the WALL clock, but every TX-as-valid-time
// start (a later version's UpdatedAt) is stamped by the graph's transaction
// clock, whose monotonic floor runs AHEAD of the wall after a burst of more
// than one write per millisecond (lesson 71), after an HLC AdvanceClock, and on
// a replica that applied a primary's stamps. Until the wall caught up, the
// newer versions sat "in the future" of wall-now, so the door returned an
// older superseded version — and a different one on each call as the wall
// advanced (v0, then v1, then the right row).
//
// The fixture makes the clock-ahead state deterministic with AdvanceClock
// (ten minutes past the wall), then pins three belief states: after the
// create (pin0), after one update (pin1), after two updates (pin2). Every
// door must answer the version the transaction-time pin selects — the newest
// version recorded by the pin, which is also the RelAsOf/NodeAsOf answer
// because no version here closes its valid time — and the same answer on
// every call, on the primary and on a replica fed from its change log, on
// the memory and badger backends.

const txAheadAdvance = 10 * time.Minute

type txAheadFixture struct {
	node, other types.NodeID
	rel         types.RelID
	late        types.RelID      // created after pin2: no pin may see it
	start       types.Instant    // an instant before the create (open-interval start)
	pins        [3]types.Instant // after create, after update 1, after update 2
}

func buildTxAheadPrimary(t *testing.T, g *graph.Graph) txAheadFixture {
	t.Helper()
	ctx := context.Background()
	if err := g.Index().CreateVector("Asset", "vec", 2, store.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}
	var f txAheadFixture
	f.start = types.Instant(time.Now().UnixMilli()) - 1000
	a, err := g.Nodes().Add(ctx, []string{"Asset"}, map[string]any{"k": "x", "m": "z", "vec": []float32{1, 0}})
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Peer"}, map[string]any{"k": "y"})
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	// A back-dated valid-from on the relationship's first version, as in the
	// oracle's reproducing history.
	r, err := g.Rels().Add(ctx, "HOP", a, b, map[string]any{"k": "x", "tkg_valid_from": types.Instant(1_700_000_000_000)})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	f.node, f.other, f.rel = a.ID(), b.ID(), r.ID()
	if f.pins[0], err = g.Temporal().NowTx(); err != nil {
		t.Fatal(err)
	}
	wall := types.Instant(time.Now().UnixMilli())
	if _, err := g.Temporal().AdvanceClock(wall + types.Instant(txAheadAdvance.Milliseconds())); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := g.Nodes().Update(ctx, f.node, map[string]any{"n": int64(i)}); err != nil {
			t.Fatalf("update node %d: %v", i, err)
		}
		if _, err := g.Rels().Update(ctx, f.rel, map[string]any{"n": int64(i)}); err != nil {
			t.Fatalf("update rel %d: %v", i, err)
		}
		if f.pins[i], err = g.Temporal().NowTx(); err != nil {
			t.Fatal(err)
		}
	}
	late, err := g.Rels().Add(ctx, "HOP", a, b, map[string]any{"k": "x"})
	if err != nil {
		t.Fatalf("add late rel: %v", err)
	}
	f.late = late.ID()
	return f
}

// replicateAll applies every primary change record to replica.
func replicateAll(t *testing.T, primary, replica *graph.Graph) {
	t.Helper()
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
}

// txAheadDoor evaluates one read door at one pin into a canonical answer
// "id:version ..." (sorted), plus the versions it returned for the fixture's
// node and relationship (-1 = absent).
type txAheadDoor struct {
	name string
	// primaryOnly marks doors the replica cannot serve here: GraphTx (a
	// read-only replica refuses Tx()) and vector search (index definitions
	// are not in the change log).
	primaryOnly bool
	run         func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error)
}

func relVersions(rs []*types.Relationship) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, fmt.Sprintf("r%d:v%d", r.ID(), r.Version()))
	}
	return out
}

func nodeVersions(ns []*types.Node) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, fmt.Sprintf("n%d:v%d", n.ID(), n.Version()))
	}
	return out
}

func relMapVersions(m map[types.NodeID][]*types.Relationship) []string {
	var out []string
	for _, rs := range m {
		out = append(out, relVersions(rs)...)
	}
	return out
}

func txAheadDoors() []txAheadDoor {
	txAt := func(pin types.Instant) store.QueryOpts { return store.QueryOpts{TxAt: pin} }
	return []txAheadDoor{
		{name: "Rels.ByType{TxAt}", run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			rs, err := g.Rels().ByType("HOP", txAt(pin))
			return relVersions(rs), err
		}},
		{name: "Rels.ForEachByType{TxAt}", run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			var rs []*types.Relationship
			err := g.Rels().ForEachByType("HOP", txAt(pin), func(r *types.Relationship) bool { rs = append(rs, r); return true })
			return relVersions(rs), err
		}},
		{name: "Rels.All{TxAt}", run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			rs, err := g.Rels().All(txAt(pin))
			return relVersions(rs), err
		}},
		{name: "Rels.ByTypeAndProperty{TxAt}", run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			rs, err := g.Rels().ByTypeAndProperty("HOP", "k", "x", txAt(pin))
			return relVersions(rs), err
		}},
		{name: "Rels.OutgoingForNodesAtTx", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			m, err := g.Rels().OutgoingForNodesAtTx([]types.NodeID{f.node}, "HOP", pin)
			return relMapVersions(m), err
		}},
		{name: "Rels.IncomingForNodesAtTx", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			m, err := g.Rels().IncomingForNodesAtTx([]types.NodeID{f.other}, "HOP", pin)
			return relMapVersions(m), err
		}},
		{name: "Rels.ForEachAdjacentRelAt{TxAt}", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			var rs []*types.Relationship
			err := g.Rels().ForEachAdjacentRelAt(f.node, "HOP", false, txAt(pin), func(r *types.Relationship) bool { rs = append(rs, r); return true })
			return relVersions(rs), err
		}},
		{name: "Rels.ForEachAdjacentEndpointAt{TxAt}", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			// The endpoint door yields no version; answer with the RelAsOf
			// version of each yielded id only if the door yielded the rel at
			// all, and fail loudly if it yields one the pin cannot see.
			var out []string
			err := g.Rels().ForEachAdjacentEndpointAt(f.node, "HOP", false, txAt(pin), func(rel types.RelID, _ types.NodeID) bool {
				r, err := g.Temporal().RelAsOf(rel, pin)
				if err != nil {
					out = append(out, fmt.Sprintf("r%d:not-recorded-by-pin", rel))
					return true
				}
				out = append(out, fmt.Sprintf("r%d:v%d", rel, r.Version()))
				return true
			})
			return out, err
		}},
		{name: "Temporal.RelsDuringTx(open end)", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			rs, err := g.Temporal().RelsDuringTx(f.start, 0, pin)
			return relVersions(rs), err
		}},
		{name: "Temporal.RelAsOf", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			r, err := g.Temporal().RelAsOf(f.rel, pin)
			if err != nil {
				return nil, err
			}
			return relVersions([]*types.Relationship{r}), nil
		}},
		{name: "Nodes.ByLabel{TxAt}", run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			ns, err := g.Nodes().ByLabel("Asset", txAt(pin))
			return nodeVersions(ns), err
		}},
		{name: "Nodes.All{TxAt}", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			ns, err := g.Nodes().All(txAt(pin))
			var keep []*types.Node
			for _, n := range ns {
				if n.ID() == f.node {
					keep = append(keep, n)
				}
			}
			return nodeVersions(keep), err
		}},
		{name: "Nodes.ByLabelAndProperty{TxAt}", run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			ns, err := g.Nodes().ByLabelAndProperty("Asset", "k", "x", txAt(pin))
			return nodeVersions(ns), err
		}},
		{name: "Nodes.ByLabelAndProperties{TxAt}", run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			ns, err := g.Nodes().ByLabelAndProperties("Asset", map[string]any{"k": "x", "m": "z"}, txAt(pin))
			return nodeVersions(ns), err
		}},
		{name: "Index.SearchNearest{TxAt}", primaryOnly: true, run: func(g *graph.Graph, _ txAheadFixture, pin types.Instant) ([]string, error) {
			ns, err := g.Index().SearchNearest("Asset", "vec", []float32{1, 0}, 5, txAt(pin))
			return nodeVersions(ns), err
		}},
		{name: "Temporal.NodesDuringTx(open end)", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			ns, err := g.Temporal().NodesDuringTx(f.start, 0, pin)
			var keep []*types.Node
			for _, n := range ns {
				if n.ID() == f.node {
					keep = append(keep, n)
				}
			}
			return nodeVersions(keep), err
		}},
		{name: "Temporal.NodeAsOf", run: func(g *graph.Graph, f txAheadFixture, pin types.Instant) ([]string, error) {
			n, err := g.Temporal().NodeAsOf(f.node, pin)
			if err != nil {
				return nil, err
			}
			return nodeVersions([]*types.Node{n}), nil
		}},
	}
}

// txAheadOpenEndDoors are the current-belief open-interval doors (no TX pin:
// they see every version, so the answer is always the newest, v2).
func txAheadOpenEndDoors() []txAheadDoor {
	keepNode := func(f txAheadFixture, ns []*types.Node) []*types.Node {
		var keep []*types.Node
		for _, n := range ns {
			if n.ID() == f.node {
				keep = append(keep, n)
			}
		}
		return keep
	}
	return []txAheadDoor{
		{name: "Temporal.RelsDuring(open end)", run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			rs, err := g.Temporal().RelsDuring(f.start, 0)
			return relVersions(rs), err
		}},
		{name: "Temporal.RelsByTypePropertyDuring(open end)", run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			rs, err := g.Temporal().RelsByTypePropertyDuring("HOP", "k", "x", f.start, 0)
			return relVersions(rs), err
		}},
		{name: "Temporal.NodesDuring(open end)", run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			ns, err := g.Temporal().NodesDuring(f.start, 0)
			return nodeVersions(keepNode(f, ns)), err
		}},
		{name: "Temporal.NodesByLabelPropertyDuring(open end)", run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			ns, err := g.Temporal().NodesByLabelPropertyDuring("Asset", "k", "x", f.start, 0)
			return nodeVersions(ns), err
		}},
		{name: "GraphTx.RelsDuring(open end)", primaryOnly: true, run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			var out []string
			err := g.Tx().Run(func(tx *graph.GraphTx) error {
				rs, err := tx.RelsDuring(f.start, 0)
				out = relVersions(rs)
				return err
			})
			return out, err
		}},
		{name: "GraphTx.NodesDuring(open end)", primaryOnly: true, run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			var out []string
			err := g.Tx().Run(func(tx *graph.GraphTx) error {
				ns, err := tx.NodesDuring(f.start, 0)
				out = nodeVersions(keepNode(f, ns))
				return err
			})
			return out, err
		}},
		{name: "GraphTx.RelsByTypePropertyDuring(open end)", primaryOnly: true, run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			var out []string
			err := g.Tx().Run(func(tx *graph.GraphTx) error {
				rs, err := tx.RelsByTypePropertyDuring("HOP", "k", "x", f.start, 0)
				out = relVersions(rs)
				return err
			})
			return out, err
		}},
		{name: "GraphTx.NodesByLabelPropertyDuring(open end)", primaryOnly: true, run: func(g *graph.Graph, f txAheadFixture, _ types.Instant) ([]string, error) {
			var out []string
			err := g.Tx().Run(func(tx *graph.GraphTx) error {
				ns, err := tx.NodesByLabelPropertyDuring("Asset", "k", "x", f.start, 0)
				out = nodeVersions(ns)
				return err
			})
			return out, err
		}},
	}
}

// wantTxAhead is the answer the transaction-time pin selects: the newest
// version recorded by the pin, for whichever of the two entities the door
// returns. The late relationship belongs only in a current-belief answer
// (pin == 0); any other entry is an over-report and fails the comparison.
func wantTxAhead(got []string, f txAheadFixture, pin types.Instant, version int) string {
	var want []string
	for _, s := range got {
		switch {
		case strings.HasPrefix(s, fmt.Sprintf("r%d:", f.rel)):
			want = append(want, fmt.Sprintf("r%d:v%d", f.rel, version))
		case strings.HasPrefix(s, fmt.Sprintf("n%d:", f.node)):
			want = append(want, fmt.Sprintf("n%d:v%d", f.node, version))
		case strings.HasPrefix(s, fmt.Sprintf("r%d:", f.late)) && pin == 0:
			want = append(want, fmt.Sprintf("r%d:v0", f.late))
		}
	}
	sort.Strings(want)
	return strings.Join(want, " ")
}

func TestTxAtDoorsWhenTxClockIsAheadOfWall(t *testing.T) {
	backends := []struct {
		name                string
		primaryCfg, replCfg graph.Config
	}{
		{
			name:       "memory",
			primaryCfg: graph.Config{SnowflakeNodeID: 1, Store: memory.New(memory.WithChangeLog())},
			replCfg:    graph.Config{SnowflakeNodeID: 2, Store: memory.New(), ReadOnlyReplica: true},
		},
		{
			name: "badger",
			// SyncWrites: the feed serves committed records only, and the
			// replica is fed right after the writes.
			primaryCfg: graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true},
			replCfg:    graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true},
		},
	}
	const calls = 25
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			primary, err := graph.New(be.primaryCfg)
			if err != nil {
				t.Fatalf("primary: %v", err)
			}
			t.Cleanup(func() { _ = primary.Close() })
			f := buildTxAheadPrimary(t, primary)
			replCfg := be.replCfg
			replCfg.ReplicationSource = primary.Replication()
			replica, err := graph.New(replCfg)
			if err != nil {
				t.Fatalf("replica: %v", err)
			}
			t.Cleanup(func() { _ = replica.Close() })
			replicateAll(t, primary, replica)
			if wall := types.Instant(time.Now().UnixMilli()); wall >= f.pins[1] {
				t.Fatalf("fixture lost the clock-ahead state: wall %d >= pin1 %d", wall, f.pins[1])
			}

			check := func(t *testing.T, g *graph.Graph, d txAheadDoor, pin types.Instant, version int) {
				t.Helper()
				var first string
				for i := 0; i < calls; i++ {
					got, err := d.run(g, f, pin)
					if err != nil {
						t.Fatalf("call %d: %v", i, err)
					}
					sort.Strings(got)
					s := strings.Join(got, " ")
					if i == 0 {
						first = s
						if len(got) == 0 {
							t.Fatalf("door returned nothing, want the fixture entity at v%d", version)
						}
						if want := wantTxAhead(got, f, pin, version); s != want {
							t.Fatalf("answer %q, want %q (the version recorded by the pin)", s, want)
						}
						continue
					}
					if s != first {
						t.Fatalf("call %d answered %q, call 0 answered %q", i, s, first)
					}
				}
			}
			for _, gr := range []struct {
				name string
				g    *graph.Graph
			}{{"primary", primary}, {"replica", replica}} {
				for _, d := range txAheadDoors() {
					if d.primaryOnly && gr.name == "replica" {
						continue
					}
					for v, pin := range f.pins {
						t.Run(fmt.Sprintf("%s/%s/pin%d", gr.name, d.name, v), func(t *testing.T) {
							check(t, gr.g, d, pin, v)
						})
					}
				}
				for _, d := range txAheadOpenEndDoors() {
					if d.primaryOnly && gr.name == "replica" {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s", gr.name, d.name), func(t *testing.T) {
						check(t, gr.g, d, 0, 2)
					})
				}
			}
		})
	}
}
