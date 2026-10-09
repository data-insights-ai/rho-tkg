package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The whole-unit pre-flight (precheckCallerTxOps) for node caller-instant ops
// in a Batch or ingest group, together with relationship ops: one refused op
// refuses the unit with nothing written; a caller-instant op is the only op of
// the unit on its entity, where a node delete's entity is the node AND every
// relationship it cascades.

// txbNodeQueue is the queue surface BatchBuilder and the ingest Session share,
// node and relationship doors.
type txbNodeQueue interface {
	txbQueue
	UpdateNode(id types.NodeID, updates map[string]any) error
	DeleteNode(id types.NodeID) error
	UpdateNodeWithTx(id types.NodeID, updates map[string]any, txFrom types.Instant) error
	DeleteNodeWithTx(id types.NodeID, txTo types.Instant) error
}

// txbNodeQueueModes mirrors txbQueueModes over txbNodeQueue: the unit is
// ALWAYS applied after a queue error, so a queue door that queues and then
// errors is caught by the unchanged-state assertions.
func txbNodeQueueModes() []struct {
	name  string
	apply func(g *Core, queue func(q txbNodeQueue) error) error
} {
	ingest := func(concurrent bool) func(g *Core, queue func(q txbNodeQueue) error) error {
		return func(g *Core, queue func(q txbNodeQueue) error) error {
			s, err := g.Ingest.NewSession(IngestOptions{Sync: true, Concurrent: concurrent})
			if err != nil {
				return fmt.Errorf("%w: NewSession: %w", errTxbSetup, err)
			}
			qErr := queue(s)
			_, subErr := s.Submit()
			if err := s.Close(); err != nil {
				return fmt.Errorf("%w: Session.Close: %w", errTxbSetup, err)
			}
			if qErr != nil {
				return qErr
			}
			return subErr
		}
	}
	return []struct {
		name  string
		apply func(g *Core, queue func(q txbNodeQueue) error) error
	}{
		{"Batch", func(g *Core, queue func(q txbNodeQueue) error) error {
			b, err := NewBatchBuilder(g)
			if err != nil {
				return fmt.Errorf("%w: NewBatchBuilder: %w", errTxbSetup, err)
			}
			qErr := queue(b)
			_, execErr := b.Execute()
			if qErr != nil {
				return qErr
			}
			return execErr
		}},
		{"IngestStrong", ingest(false)},
		{"IngestConcurrent", ingest(true)},
	}
}

// txbUnitFix: two backfilled "Ref" nodes a and b (each with its out/in rels to
// "Ev" nodes) joined by a backfilled rel ab, and a plain "Ref" node p.
type txbUnitFix struct {
	a, b txbNodeFix
	ab   types.RelID
	p    *types.Node
}

func txbUnitFixture(t *testing.T, g *Core) txbUnitFix {
	t.Helper()
	a, b := txbBackfillNodeFix(t, g), txbBackfillNodeFix(t, g)
	ab := txbBackfillRelBetween(t, g, a.id, b.id, a.base, map[string]any{"tkg_valid_from": a.vf})
	p := txbAddNode(t, g, "Ref", map[string]any{"tkg_valid_from": dcY2020, "w": int64(1)})
	return txbUnitFix{a: a, b: b, ab: ab.ID(), p: p}
}

// R16 (nodes) — Batch and ingest node doors: the seam stamps t, or the whole
// unit is refused with nothing written.
//
// Catches:
//   - per-op partial success: a valid node delete at t applied while a node
//     update in the same unit fails its order check (and the reverse);
//   - a pre-flight that knows relationship ops only (the merged W4 one): every
//     node case below then applies partly;
//   - a caller-instant node delete beside another op on one of its cascaded
//     relationships (a plain update stamped with the clock lands first, then
//     the cascade at t falls below it), beside a relationship create that ends
//     at the node (cascaded at t below its own TxFrom), and beside a second
//     node delete that cascades the same relationship;
//   - a caller-instant relationship op beside a PLAIN delete of an endpoint
//     node (the pre-flight must count the plain delete's cascade);
//   - two ops on one node in one unit (reversed caller instants; plain then
//     caller-instant);
//   - a missing node applied as a partial batch; an empty update queued.
//
// Counterpart: the valid mixed unit applies every op with its exact t.
func TestTxBackfillNode_UnitPreflight(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, m := range txbNodeQueueModes() {
			t.Run(be.name+"/"+m.name, func(t *testing.T) {
				g := be.open(t, true)
				f := txbUnitFixture(t, g)
				ta, tb := f.a.base+1000, f.b.base+1000
				rels := append(append(f.a.rels(), f.b.rels()...), f.ab)
				snapAll := func() []hoodSnap {
					return []hoodSnap{snapHood(t, g, f.a.id, rels...), snapHood(t, g, f.b.id), snapHood(t, g, f.p.ID())}
				}
				ids := []types.NodeID{f.a.id, f.b.id, f.p.ID()}
				missing := g.Nodes.NextID()
				cases := []struct {
					name    string
					queue   func(q txbNodeQueue, created **types.Relationship) error
					wantErr error
					stamp   string
				}{
					{"good node delete, bad node update", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.DeleteNodeWithTx(f.a.id, ta); err != nil {
							return err
						}
						if err := q.UpdateNode(f.p.ID(), map[string]any{"w": int64(9)}); err != nil {
							return err
						}
						return q.UpdateNodeWithTx(f.b.id, map[string]any{"w": int64(2)}, f.b.base)
					}, ErrTxOrder, fmt.Sprint(f.b.base)},
					{"good node update, bad node delete", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.UpdateNodeWithTx(f.a.id, map[string]any{"w": int64(2)}, ta); err != nil {
							return err
						}
						return q.DeleteNodeWithTx(f.b.id, f.b.base-1)
					}, ErrTxOrder, fmt.Sprint(f.b.base)},
					{"caller node delete, plain update of a cascaded rel", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.UpdateRelationship(f.a.out, map[string]any{"w": int64(9)}); err != nil {
							return err
						}
						return q.DeleteNodeWithTx(f.a.id, ta)
					}, ErrTxOrder, ""},
					{"caller node delete, rel create ending at the node", func(q txbNodeQueue, created **types.Relationship) error {
						an, err := g.Nodes.Get(context.Background(), f.a.id)
						if err != nil {
							return fmt.Errorf("%w: %w", errTxbSetup, err)
						}
						if *created, err = q.AddRelationship("LINK", f.p, an, nil); err != nil {
							return err
						}
						return q.DeleteNodeWithTx(f.a.id, ta)
					}, ErrTxOrder, ""},
					{"two caller node deletes sharing a rel", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.DeleteNodeWithTx(f.a.id, ta); err != nil {
							return err
						}
						return q.DeleteNodeWithTx(f.b.id, tb)
					}, ErrTxOrder, ""},
					{"caller rel update, plain delete of its endpoint node", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.UpdateRelationshipWithTx(f.b.out, map[string]any{"w": int64(2)}, tb); err != nil {
							return err
						}
						return q.DeleteNode(f.b.ev1)
					}, ErrTxOrder, ""},
					{"two caller node updates, reversed", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.UpdateNodeWithTx(f.a.id, map[string]any{"w": int64(2)}, ta+10); err != nil {
							return err
						}
						return q.UpdateNodeWithTx(f.a.id, map[string]any{"w": int64(3)}, ta)
					}, ErrTxOrder, ""},
					{"plain node update then caller node delete", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.UpdateNode(f.a.id, map[string]any{"w": int64(2)}); err != nil {
							return err
						}
						return q.DeleteNodeWithTx(f.a.id, ta)
					}, ErrTxOrder, ""},
					{"missing node", func(q txbNodeQueue, _ **types.Relationship) error {
						if err := q.UpdateNode(f.p.ID(), map[string]any{"w": int64(9)}); err != nil {
							return err
						}
						return q.DeleteNodeWithTx(missing, ta)
					}, storepkg.ErrNodeNotFound, ""},
					{"empty update at t", func(q txbNodeQueue, _ **types.Relationship) error {
						return q.UpdateNodeWithTx(f.a.id, map[string]any{}, ta)
					}, ErrTxOrder, ""},
				}
				for _, tc := range cases {
					before, exp0 := snapAll(), txbExport(t, g)
					var created *types.Relationship
					err := m.apply(g, func(q txbNodeQueue) error { return tc.queue(q, &created) })
					if errors.Is(err, errTxbSetup) || !errors.Is(err, tc.wantErr) {
						t.Fatalf("[%s] err = %v; want %v", tc.name, err, tc.wantErr)
					}
					if tc.stamp != "" && !bytes.Contains([]byte(err.Error()), []byte(tc.stamp)) {
						t.Fatalf("[%s] err = %q; want it to name the conflicting stamp %s", tc.name, err, tc.stamp)
					}
					for i := range before {
						assertHoodUnchanged(t, g, fmt.Sprintf("%s node %d", tc.name, i), before[i], ids[i])
					}
					if exp1 := txbExport(t, g); !bytes.Equal(exp0, exp1) {
						t.Fatalf("[%s] export bytes changed after a refused unit", tc.name)
					}
					if created != nil {
						if _, gerr := g.Rels.Get(context.Background(), created.ID()); !errors.Is(gerr, storepkg.ErrRelNotFound) {
							t.Fatalf("[%s] queued create persisted (err=%v); want nothing written", tc.name, gerr)
						}
					}
				}

				// Counterpart: the valid mixed unit (a deleted at ta, cascading
				// ab; b updated at tb; p plain).
				err := m.apply(g, func(q txbNodeQueue) error {
					if err := q.DeleteNodeWithTx(f.a.id, ta); err != nil {
						return err
					}
					if err := q.UpdateNodeWithTx(f.b.id, map[string]any{"w": int64(2)}, tb); err != nil {
						return err
					}
					return q.UpdateNode(f.p.ID(), map[string]any{"w": int64(9)})
				})
				if err != nil {
					t.Fatalf("valid mixed unit: %v", err)
				}
				if nt := nodeTombstone(t, g, f.a.id); nt.TxTo != ta || nt.DeletedAt != ta {
					t.Fatalf("a tombstone %+v; want TxTo = DeletedAt = %d", *nt, ta)
				}
				for _, rid := range append(f.a.rels(), f.ab) {
					if rt := relTombstone(t, g, rid); rt.TxTo != ta || rt.DeletedAt != ta {
						t.Fatalf("cascaded rel %d tombstone %+v; want TxTo = DeletedAt = %d", rid, *rt, ta)
					}
				}
				chainB := txbNodeChain(t, g, f.b.id)
				if n := len(chainB); n != 2 || nodeTemporalCopy(chainB[0]).TxTo != tb || nodeTemporalCopy(chainB[1]).TxFrom != tb {
					t.Fatalf("b chain %d rows; want prev.TxTo = TxFrom = %d", n, tb)
				}
				p, err := g.Nodes.Get(context.Background(), f.p.ID())
				if err != nil || p.Temporal().TxFrom <= max(ta, tb) {
					t.Fatalf("p after the valid unit: %v, %v; want a plain update stamped above every caller instant", p, err)
				}
			})
		}
	}
}

// R16 (nodes, strong-mode coalescing) — a group refused for a node caller
// instant fails alone.
//
// Catches: a coalescing rule that recognises relationship caller-instant ops
// only, so a node caller-instant group is merged with sibling groups and its
// whole-unit refusal fails them too (or the refused group is partly applied).
func TestTxBackfillNode_IngestCoalescedGroups(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, true)
			f := txbUnitFixture(t, g)
			ap, err := g.ensureIngestApplier(defaultIngestGroupSize, 0)
			if err != nil {
				t.Fatalf("ensureIngestApplier: %v", err)
			}
			group := func(queue func(b *BatchBuilder) error) *ingestGroup {
				b, err := NewBatchBuilder(g)
				if err != nil {
					t.Fatalf("NewBatchBuilder: %v", err)
				}
				if err := queue(b); err != nil {
					t.Fatalf("queue: %v", err)
				}
				gr := b.takeIngestGroup()
				gr.result = make(chan error, 1)
				return gr
			}
			good := group(func(b *BatchBuilder) error { return b.UpdateNode(f.p.ID(), map[string]any{"w": int64(9)}) })
			bad := group(func(b *BatchBuilder) error {
				if err := b.UpdateNodeWithTx(f.a.id, map[string]any{"w": int64(2)}, f.a.base+1000); err != nil {
					return err
				}
				return b.DeleteNodeWithTx(f.b.id, f.b.base)
			})
			good2 := group(func(b *BatchBuilder) error {
				return b.UpdateNodeWithTx(f.a.id, map[string]any{"w": int64(3)}, f.a.base+2000)
			})
			beforeB := snapHood(t, g, f.b.id, append(f.b.rels(), f.ab)...)
			ap.applyCommitGroup([]*ingestGroup{good, bad, good2})
			if err := <-bad.result; !errors.Is(err, ErrTxOrder) {
				t.Fatalf("refused group err = %v; want ErrTxOrder", err)
			}
			if err := <-good.result; err != nil {
				t.Fatalf("sibling group failed with the refused group: %v", err)
			}
			if err := <-good2.result; err != nil {
				t.Fatalf("later sibling group failed with the refused group: %v", err)
			}
			assertHoodUnchanged(t, g, "refused group's b", beforeB, f.b.id)
			chainA := txbNodeChain(t, g, f.a.id)
			if n := len(chainA); n != 2 || nodeTemporalCopy(chainA[1]).TxFrom != f.a.base+2000 {
				t.Fatalf("a chain %d rows; want only good2's version at %d", n, f.a.base+2000)
			}
			p, err := g.Nodes.Get(context.Background(), f.p.ID())
			if err != nil {
				t.Fatalf("Get(p): %v", err)
			}
			if w, _ := p.GetProperty("w"); w != int64(9) {
				t.Fatalf("p w = %v; want 9 (good group applied)", w)
			}
		})
	}
}
