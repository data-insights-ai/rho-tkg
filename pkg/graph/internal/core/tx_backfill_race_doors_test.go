package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R11 over every door (handover §5 R11; the standalone doors race in
// TestTxBackfillRel_RaceClock / TestTxBackfillNode_RaceClock). Plain Updates
// race the GraphTx, Batch, ingest strong and ingest concurrent caller-instant
// doors on nodes and relationships, each t reserved from NowTx just before the
// call. Run under -race.
//
// Catches: an order check that is not under the entity lock at the moment of
// the write — a GraphTx twin checking before its lock, a Batch / ingest unit
// whose pre-flight and apply see different chains (the concurrent ingest
// pre-flight runs under the shared lock, so the seam must check again) — and
// any door that stamps something other than t. A plain write landing between
// the reservation and the write makes t stale: the door must refuse it with
// ErrTxOrder (errors.Is through the Batch / ingest wrapping); stamping it
// would put a TxFrom below its predecessor's. After the race: every chain has
// strictly rising TxFrom and prev.TxTo == next.TxFrom, every accepted t is the
// TxFrom of exactly one version (update) or the tombstone's TxTo = DeletedAt
// (delete, on the node and on the cascaded relationship).

type txbRaceDoor struct {
	name    string
	nodeUpd func(g *Core, id types.NodeID, m map[string]any, at types.Instant) error
	nodeDel func(g *Core, id types.NodeID, at types.Instant) error
	relUpd  func(g *Core, id types.RelID, m map[string]any, at types.Instant) error
	relDel  func(g *Core, id types.RelID, at types.Instant) error
}

func txbRaceDoors() []txbRaceDoor {
	doors := []txbRaceDoor{{
		name: "GraphTx",
		nodeUpd: func(g *Core, id types.NodeID, m map[string]any, at types.Instant) error {
			return txbTxCommitDo(g, func(tx *GraphTx) error { _, err := tx.UpdateNodeWithTx(id, m, at); return err })
		},
		nodeDel: func(g *Core, id types.NodeID, at types.Instant) error {
			return txbTxCommitDo(g, func(tx *GraphTx) error { return tx.DeleteNodeWithTx(id, at) })
		},
		relUpd: func(g *Core, id types.RelID, m map[string]any, at types.Instant) error {
			return txbTxCommitDo(g, func(tx *GraphTx) error { _, err := tx.UpdateRelationshipWithTx(id, m, at); return err })
		},
		relDel: func(g *Core, id types.RelID, at types.Instant) error {
			return txbTxCommitDo(g, func(tx *GraphTx) error { return tx.DeleteRelationshipWithTx(id, at) })
		},
	}}
	relModes := txbQueueModes()
	for i, nm := range txbNodeQueueModes() {
		nm, rm := nm, relModes[i]
		doors = append(doors, txbRaceDoor{
			name: nm.name,
			nodeUpd: func(g *Core, id types.NodeID, m map[string]any, at types.Instant) error {
				return nm.apply(g, func(q txbNodeQueue) error { return q.UpdateNodeWithTx(id, m, at) })
			},
			nodeDel: func(g *Core, id types.NodeID, at types.Instant) error {
				return nm.apply(g, func(q txbNodeQueue) error { return q.DeleteNodeWithTx(id, at) })
			},
			relUpd: func(g *Core, id types.RelID, m map[string]any, at types.Instant) error {
				return rm.apply(g, func(q txbQueue) error { return q.UpdateRelationshipWithTx(id, m, at) })
			},
			relDel: func(g *Core, id types.RelID, at types.Instant) error {
				return rm.apply(g, func(q txbQueue) error { return q.DeleteRelationshipWithTx(id, at) })
			},
		})
	}
	return doors
}

// txbAcceptedTxFrom asserts every accepted t is the TxFrom of exactly one row.
func txbAcceptedTxFrom(t *testing.T, phase string, rows []types.TemporalMetadata, accepted []types.Instant) {
	t.Helper()
	count := map[types.Instant]int{}
	for _, tm := range rows {
		count[tm.TxFrom]++
	}
	for _, at := range accepted {
		if count[at] != 1 {
			t.Fatalf("[%s] accepted t=%d is the TxFrom of %d rows; want exactly 1", phase, at, count[at])
		}
	}
}

func TestTxBackfill_RaceClockEveryDoor(t *testing.T) {
	t.Parallel()
	const writers, rounds = 3, 20
	for _, be := range txbBackends() {
		for _, d := range txbRaceDoors() {
			t.Run(be.name+"/"+d.name, func(t *testing.T) {
				g := be.open(t, true)
				ctx := context.Background()
				vf := map[string]any{"tkg_valid_from": dcY2020, "w": int64(0)}
				updN := txbAddNode(t, g, "Ref", vf)
				delN := txbAddNode(t, g, "Ref", vf)
				ev := txbAddNode(t, g, "Ev", map[string]any{"tkg_valid_from": dcY2020})
				hot := txbAddRelByID(t, g, delN.ID(), ev.ID(), vf)
				updR := txbPlainRel(t, g, vf)
				delR := txbPlainRel(t, g, vf)

				var (
					wg                  sync.WaitGroup
					mu                  sync.Mutex
					okNodeUpd, okRelUpd []types.Instant
					nodeDelAt, relDelAt types.Instant
					errMu               sync.Mutex
					errs                []error
				)
				report := func(err error) {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
				}
				orderOnly := func(what string, err error) bool {
					if err == nil {
						return true
					}
					if !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom) {
						report(fmt.Errorf("%s: %w", what, err))
					}
					return false
				}
				for w := 0; w < writers; w++ {
					wg.Add(2)
					go func(w int) {
						defer wg.Done()
						for i := 0; i < rounds; i++ {
							v := map[string]any{"w": int64(w*1000 + i + 1)}
							if _, err := g.Nodes.Update(ctx, updN.ID(), v); err != nil {
								report(fmt.Errorf("plain Nodes.Update: %w", err))
							}
							if _, err := g.Nodes.Update(ctx, delN.ID(), v); err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
								report(fmt.Errorf("plain Nodes.Update(del): %w", err))
							}
							if _, err := g.Rels.Update(ctx, hot.ID(), v); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
								report(fmt.Errorf("plain Rels.Update(hot): %w", err))
							}
							if _, err := g.Rels.Update(ctx, updR.ID(), v); err != nil {
								report(fmt.Errorf("plain Rels.Update: %w", err))
							}
							if _, err := g.Rels.Update(ctx, delR.ID(), v); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
								report(fmt.Errorf("plain Rels.Update(del): %w", err))
							}
						}
					}(w)
					go func(w int) {
						defer wg.Done()
						for i := 0; i < rounds; i++ {
							v := map[string]any{"w": int64(-(w*1000 + i + 1))}
							at, _ := g.Temporal.NowTx()
							if orderOnly(d.name+".UpdateNodeWithTx", d.nodeUpd(g, updN.ID(), v, at)) {
								mu.Lock()
								okNodeUpd = append(okNodeUpd, at)
								mu.Unlock()
							}
							at, _ = g.Temporal.NowTx()
							if orderOnly(d.name+".UpdateRelationshipWithTx", d.relUpd(g, updR.ID(), v, at)) {
								mu.Lock()
								okRelUpd = append(okRelUpd, at)
								mu.Unlock()
							}
							if w != 0 || i != rounds/2 {
								continue
							}
							for {
								at, _ := g.Temporal.NowTx()
								if orderOnly(d.name+".DeleteNodeWithTx", d.nodeDel(g, delN.ID(), at)) {
									nodeDelAt = at
									break
								}
							}
							for {
								at, _ := g.Temporal.NowTx()
								if orderOnly(d.name+".DeleteRelationshipWithTx", d.relDel(g, delR.ID(), at)) {
									relDelAt = at
									break
								}
							}
						}
					}(w)
				}
				wg.Wait()
				for _, err := range errs {
					t.Error(err)
				}
				if t.Failed() {
					return
				}
				if len(okNodeUpd) == 0 || len(okRelUpd) == 0 {
					t.Fatalf("no caller-instant update accepted (nodes %d, rels %d): the race never let one through", len(okNodeUpd), len(okRelUpd))
				}

				nodeRows := func(chain []*types.Node) []types.TemporalMetadata {
					out := make([]types.TemporalMetadata, 0, len(chain))
					for _, n := range chain {
						out = append(out, nodeTemporalCopy(n))
					}
					return out
				}
				relRows := func(chain []*types.Relationship) []types.TemporalMetadata {
					out := make([]types.TemporalMetadata, 0, len(chain))
					for _, r := range chain {
						out = append(out, relTemporalCopy(r))
					}
					return out
				}

				un := txbNodeChain(t, g, updN.ID())
				assertNodeTxChainMonotone(t, "updated node", un)
				txbAcceptedTxFrom(t, "updated node", nodeRows(un), okNodeUpd)
				ur := txbChain(t, g, updR.ID())
				assertTxChainMonotone(t, "updated rel", ur)
				txbAcceptedTxFrom(t, "updated rel", relRows(ur), okRelUpd)

				dn := txbNodeChain(t, g, delN.ID())
				assertNodeTxChainMonotone(t, "deleted node", dn)
				if last := nodeTemporalCopy(dn[len(dn)-1]); last.TxTo != nodeDelAt || last.DeletedAt != nodeDelAt {
					t.Fatalf("deleted node's tombstone %+v; want TxTo = DeletedAt = %d", last, nodeDelAt)
				}
				hc := txbChain(t, g, hot.ID())
				assertTxChainMonotone(t, "cascaded rel", hc)
				if last := relTemporalCopy(hc[len(hc)-1]); last.TxTo != nodeDelAt || last.DeletedAt != nodeDelAt {
					t.Fatalf("cascaded rel's tombstone %+v; want the node's t %d", last, nodeDelAt)
				}
				dr := txbChain(t, g, delR.ID())
				assertTxChainMonotone(t, "deleted rel", dr)
				if last := relTemporalCopy(dr[len(dr)-1]); last.TxTo != relDelAt || last.DeletedAt != relDelAt {
					t.Fatalf("deleted rel's tombstone %+v; want TxTo = DeletedAt = %d", last, relDelAt)
				}
			})
		}
	}
}
