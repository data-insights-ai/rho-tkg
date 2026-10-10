package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 43: the retraction across the graph's lifecycle doors — rollback,
// units, unique constraints, re-import, history compaction, retention purge,
// export/import and replica apply. Each test names the faulty implementation
// it kills; the shared probe is retractAbsentEverywhere.

// retractAbsentEverywhere asserts the belief definition for one retracted
// node (and its relationship) at pin: absent at every valid instant of its
// life (the past a Delete keeps included) through the state doors, the record
// door and the timeline.
func retractAbsentEverywhere(t *testing.T, what string, g *graphpkg.Graph, f pubFixture, pin types.Instant) {
	t.Helper()
	for _, v := range []types.Instant{f.vf, f.vf + 1, f.vf + 60_000, pin} {
		if n, err := g.Temporal().NodeAtTx(f.ev, v, pin); err == nil {
			t.Fatalf("%s: NodeAtTx(retracted, v=%d, pin=%d) = v%d; want absent", what, v, pin, n.Version())
		}
		if r, err := g.Temporal().RelAtTx(f.rel, v, pin); err == nil {
			t.Fatalf("%s: RelAtTx(retracted, v=%d, pin=%d) = v%d; want absent", what, v, pin, r.Version())
		}
	}
	if n, err := g.Temporal().NodeAsOf(f.ev, pin); err == nil {
		t.Fatalf("%s: NodeAsOf(retracted, %d) = v%d; want absent", what, pin, n.Version())
	}
	if segs, err := g.Temporal().NodeEffectiveTimeline(f.ev, pin); err == nil && len(segs) != 0 {
		t.Fatalf("%s: NodeEffectiveTimeline(retracted, %d) = %d segments; want none", what, pin, len(segs))
	}
	if segs, err := g.Temporal().RelEffectiveTimeline(f.rel, pin); err == nil && len(segs) != 0 {
		t.Fatalf("%s: RelEffectiveTimeline(retracted, %d) = %d segments; want none", what, pin, len(segs))
	}
}

// retractPresentBefore asserts the node and its relationship answer at a pin
// before the retraction at the fixture's valid start.
func retractPresentBefore(t *testing.T, what string, g *graphpkg.Graph, f pubFixture, pin types.Instant) {
	t.Helper()
	if _, err := g.Temporal().NodeAtTx(f.ev, f.vf, pin); err != nil {
		t.Fatalf("%s: NodeAtTx(v=%d, pin=%d) before the retraction: %v", what, f.vf, pin, err)
	}
	if _, err := g.Temporal().RelAtTx(f.rel, f.vf, pin); err != nil {
		t.Fatalf("%s: RelAtTx(v=%d, pin=%d) before the retraction: %v", what, f.vf, pin, err)
	}
}

func historyStrings(t *testing.T, g *graphpkg.Graph, f pubFixture) string {
	t.Helper()
	nh, err := g.Nodes().History(f.ev)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	rh, err := g.Rels().History(f.rel)
	if err != nil {
		t.Fatalf("Rels().History: %v", err)
	}
	var b bytes.Buffer
	for _, n := range nh {
		fmt.Fprintf(&b, "n v%d %+v\n", n.Version(), *n.Temporal())
	}
	for _, r := range rh {
		fmt.Fprintf(&b, "r v%d %+v\n", r.Version(), *r.Temporal())
	}
	return b.String()
}

// TestRetract_GraphTxRollbackRestoresExactly: a retraction inside a GraphTx
// that rolls back leaves no trace — the node and its relationship are current
// again with their histories exactly as before (no tombstone, no marker) and
// the pinned answers unchanged; a commit equals the standalone door. Catches
// a twin that skips the rollback snapshot or restores without the cascade.
func TestRetract_GraphTxRollbackRestoresExactly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		g := b.open(t)
		for _, withTx := range []bool{false, true} {
			f := newPubFixture(t, g)
			before := historyStrings(t, g, f)
			pin := nowTx(t, g)
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			if withTx {
				err = tx.RetractNodeWithTx(f.ev, pin)
			} else {
				err = tx.RetractNode(f.ev)
			}
			if err != nil {
				t.Fatalf("RetractNode(withTx=%v): %v", withTx, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}
			if after := historyStrings(t, g, f); after != before {
				t.Fatalf("withTx=%v: rollback left history\n%s\nwant\n%s", withTx, after, before)
			}
			if _, err := g.Nodes().Get(ctx, f.ev); err != nil {
				t.Fatalf("withTx=%v: node not restored: %v", withTx, err)
			}
			if _, err := g.Rels().Get(ctx, f.rel); err != nil {
				t.Fatalf("withTx=%v: cascaded relationship not restored: %v", withTx, err)
			}
			retractPresentBefore(t, "after rollback", g, f, nowTx(t, g))
		}
		// Commit equals the standalone door.
		f := newPubFixture(t, g)
		if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.RetractNode(f.ev) }); err != nil {
			t.Fatalf("Run(RetractNode): %v", err)
		}
		retractAbsentEverywhere(t, b.name+" after commit", g, f, nowTx(t, g))
	}
}

// TestRetract_BatchAndIngestWholeUnitRefusal: a unit carrying a refused
// caller-instant retraction writes nothing (a sibling create included); a
// caller-instant retraction must be the only op of its unit on its entity —
// and a node retraction counts the relationships it cascades; a plain
// retraction behaves as a plain delete (the unit keeps its other ops).
// Catches: per-op refusal leaving a partial past, a second op on the same
// entity applied in an order the caller did not ask for.
func TestRetract_BatchAndIngestWholeUnitRefusal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			type unit struct {
				name string
				run  func(q func(bb *graphpkg.BatchBuilder) error) error
			}
			units := []unit{{"batch", func(q func(bb *graphpkg.BatchBuilder) error) error {
				_, err := g.Batch().Run(q)
				return err
			}}}
			for _, u := range units {
				f := newPubFixture(t, g)
				evTxFrom, _, _, _ := g.Nodes().LatestStamps(f.ev)
				before := historyStrings(t, g, f)
				var created *types.Node
				err := u.run(func(bb *graphpkg.BatchBuilder) error {
					var err error
					if created, err = bb.AddNode([]string{"Ev"}, map[string]any{"k": "sibling"}); err != nil {
						return err
					}
					return bb.RetractNodeWithTx(f.ev, evTxFrom) // at the chain's TxFrom: refused
				})
				if !errors.Is(err, graphpkg.ErrTxOrder) {
					t.Fatalf("%s: unit with a refused retraction: err = %v; want ErrTxOrder", u.name, err)
				}
				if _, err := g.Nodes().Get(ctx, created.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Fatalf("%s: the refused unit still created its sibling: %v", u.name, err)
				}
				if after := historyStrings(t, g, f); after != before {
					t.Fatalf("%s: the refused unit wrote:\n%s\nwant\n%s", u.name, after, before)
				}

				// Another op on the retracted node, or on a relationship it cascades.
				at := nowTx(t, g)
				for name, second := range map[string]func(bb *graphpkg.BatchBuilder) error{
					"UpdateNode":          func(bb *graphpkg.BatchBuilder) error { return bb.UpdateNode(f.ev, map[string]any{"k": "z"}) },
					"RetractRelationship": func(bb *graphpkg.BatchBuilder) error { return bb.RetractRelationship(f.rel) },
					"UpdateRelationship": func(bb *graphpkg.BatchBuilder) error {
						return bb.UpdateRelationship(f.rel, map[string]any{"w": int64(7)})
					},
				} {
					err := u.run(func(bb *graphpkg.BatchBuilder) error {
						if err := bb.RetractNodeWithTx(f.ev, at); err != nil {
							return err
						}
						return second(bb)
					})
					if !errors.Is(err, graphpkg.ErrTxOrder) {
						t.Fatalf("%s: RetractNodeWithTx + %s on the same entity: err = %v; want ErrTxOrder", u.name, name, err)
					}
					if after := historyStrings(t, g, f); after != before {
						t.Fatalf("%s: RetractNodeWithTx + %s wrote", u.name, name)
					}
				}
				// A relationship retraction at t plus a plain op on it.
				err = u.run(func(bb *graphpkg.BatchBuilder) error {
					if err := bb.RetractRelationshipWithTx(f.rel, at); err != nil {
						return err
					}
					return bb.UpdateRelationship(f.rel, map[string]any{"w": int64(8)})
				})
				if !errors.Is(err, graphpkg.ErrTxOrder) {
					t.Fatalf("%s: RetractRelationshipWithTx + UpdateRelationship: err = %v; want ErrTxOrder", u.name, err)
				}

				// A plain retraction in a unit with a failing op: partial
				// success, as a plain delete.
				g2 := newPubFixture(t, g)
				res, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
					if err := bb.RetractNode(g2.ev); err != nil {
						return err
					}
					return bb.UpdateNode(g.Nodes().NextID(), map[string]any{"k": "missing"})
				})
				if !errors.Is(err, graphpkg.ErrBatchFailed) || res == nil || res.Failed != 1 {
					t.Fatalf("%s: plain RetractNode + a failing op: res %+v, err %v; want one failed op", u.name, res, err)
				}
				if !newestNode(t, g, g2.ev).Temporal().Retracted {
					t.Fatalf("%s: the plain retraction of a partially failed unit was not applied", u.name)
				}
			}

			// The ingest group refuses as a whole too (strong and concurrent).
			for _, concurrent := range []bool{false, true} {
				f := newPubFixture(t, g)
				evTxFrom, _, _, _ := g.Nodes().LatestStamps(f.ev)
				before := historyStrings(t, g, f)
				var sibling types.NodeID
				err := pubSession(g, concurrent, func(s *ingest.Session) error {
					n, err := s.AddNode([]string{"Ev"}, map[string]any{"k": "sibling"})
					if err != nil {
						return err
					}
					sibling = n.ID()
					return s.RetractNodeWithTx(f.ev, evTxFrom)
				})
				if !errors.Is(err, graphpkg.ErrTxOrder) {
					t.Fatalf("ingest(concurrent=%v): refused retraction: err = %v; want ErrTxOrder", concurrent, err)
				}
				if _, err := g.Nodes().Get(ctx, sibling); !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Fatalf("ingest(concurrent=%v): the refused group created its sibling: %v", concurrent, err)
				}
				if after := historyStrings(t, g, f); after != before {
					t.Fatalf("ingest(concurrent=%v): the refused group wrote", concurrent)
				}
			}
		})
	}
}

// TestRetract_UniqueClaims: a retraction frees a UniqueCurrent value (as a
// Delete) and keeps a UniqueForever claim (as a Delete). Catches a retraction
// that leaks a current claim or drops a forever claim.
func TestRetract_UniqueClaims(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name+"/current", func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			a := mustAddUser(t, g, "one@x.com")
			if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "one@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("duplicate while the owner lives: err = %v; want ErrUniqueViolation", err)
			}
			if err := g.Nodes().Retract(ctx, a.ID()); err != nil {
				t.Fatalf("Retract: %v", err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "one@x.com"}); err != nil {
				t.Fatalf("UniqueCurrent value after the retraction: %v; want free", err)
			}
		})
		t.Run(bc.name+"/forever", func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUniqueForever(t, g)
			a := mustAddUser(t, g, "own@x.com")
			if err := g.Nodes().Retract(ctx, a.ID()); err != nil {
				t.Fatalf("Retract: %v", err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "own@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("UniqueForever value after the retraction: err = %v; want ErrUniqueViolation (claim kept)", err)
			}
		})
	}
}

// TestRetract_ReimportStartsAVisibleLife: re-importing a retracted ID starts a
// new life above the tombstone (versions continue, item L) that answers at
// pins from its own record on, while the retracted life stays absent at
// every pin from T on and answers as before at earlier pins. Catches a
// re-import that the retraction hides (the cap reaching the new life) and a
// re-import that brings the retracted life back.
func TestRetract_ReimportStartsAVisibleLife(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		f := newPubFixture(t, g)
		pinBefore := nowTx(t, g)
		if err := g.Nodes().Retract(ctx, f.ev); err != nil {
			t.Fatalf("Retract: %v", err)
		}
		tomb := newestNode(t, g, f.ev)
		T := tomb.Temporal().DeletedAt
		pinRetracted := nowTx(t, g)
		lifeStart := f.vf + 120_000
		n, err := g.Nodes().Import(ctx, f.ev, []string{"Ev"}, map[string]any{"k": "second life", "tkg_valid_from": lifeStart})
		if err != nil {
			t.Fatalf("Import(retracted id): %v", err)
		}
		if n.Version() <= tomb.Version() {
			t.Fatalf("re-import version %d not above the tombstone's %d", n.Version(), tomb.Version())
		}
		pinNow := nowTx(t, g)
		if got, err := g.Temporal().NodeAtTx(f.ev, lifeStart, pinNow); err != nil || got.Version() != n.Version() {
			t.Fatalf("NodeAtTx(new life) = %v, %v; want v%d", got, err, n.Version())
		}
		if got, err := g.Temporal().NodeAtTx(f.ev, lifeStart+1000, 0); err != nil || got.Version() != n.Version() {
			t.Fatalf("NodeAt(new life) = %v, %v; want v%d", got, err, n.Version())
		}
		for _, pin := range []types.Instant{T, pinRetracted, pinNow} {
			if got, err := g.Temporal().NodeAtTx(f.ev, f.vf, pin); err == nil {
				t.Fatalf("NodeAtTx(retracted life, v=%d, pin=%d) = v%d after the re-import; want absent", f.vf, pin, got.Version())
			}
		}
		if _, err := g.Temporal().NodeAtTx(f.ev, f.vf, pinBefore); err != nil {
			t.Fatalf("NodeAtTx(retracted life, pin before T): %v; want the old belief", err)
		}
		segs, err := g.Temporal().NodeEffectiveTimeline(f.ev, pinNow)
		if err != nil || len(segs) != 1 || segs[0].ValidFrom != lifeStart || segs[0].Node.Version() != n.Version() {
			t.Fatalf("NodeEffectiveTimeline after re-import = %v, %v; want one segment from %d (the new life only)", segs, err, lifeStart)
		}
	})
}

// TestRetract_CompactionKeepsTheMarker: history compaction (KeepVersions 1)
// keeps the newest history row — the retraction tombstone — with its marker,
// and the retracted entity stays absent at pins from T on. Catches a
// compaction that rewrites the tombstone without the marker or trims it
// while keeping older rows of the life.
func TestRetract_CompactionKeepsTheMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		f := newPubFixture(t, g)
		for i := 0; i < 3; i++ {
			if _, err := g.Nodes().Update(ctx, f.ev, map[string]any{"k": fmt.Sprintf("u%d", i)}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if _, err := g.Rels().Update(ctx, f.rel, map[string]any{"w": int64(10 + i)}); err != nil {
				t.Fatalf("Rels().Update: %v", err)
			}
		}
		if err := g.Nodes().Retract(ctx, f.ev); err != nil {
			t.Fatalf("Retract: %v", err)
		}
		_, nErr := g.Admin().CompactHistoryNodes(ctx, adminpkg.RetentionPolicy{KeepVersions: 1})
		_, rErr := g.Admin().CompactHistoryRels(ctx, adminpkg.RetentionPolicy{KeepVersions: 1})
		switch {
		case b.name == "sharded":
			// sharded has no history compaction: the refusal writes nothing.
			if !errors.Is(nErr, graphpkg.ErrCapabilityNotSupported) || !errors.Is(rErr, graphpkg.ErrCapabilityNotSupported) {
				t.Fatalf("sharded compaction: %v / %v; want ErrCapabilityNotSupported", nErr, rErr)
			}
		case nErr != nil || rErr != nil:
			t.Fatalf("CompactHistoryNodes / Rels: %v / %v", nErr, rErr)
		}
		if !newestNode(t, g, f.ev).Temporal().Retracted || !newestRel(t, g, f.rel).Temporal().Retracted {
			t.Fatal("compaction dropped the retraction marker")
		}
		retractAbsentEverywhere(t, "after compaction", g, f, nowTx(t, g))
	})
}

// TestRetract_RetentionPurgeNeverResurrects: a retention purge over the
// label of a retracted node (by age and by valid-to) removes it whole or
// keeps its tombstone with the marker — never older rows of the life without
// it. Catches a purge that separates the marker from its life.
func TestRetract_RetentionPurgeNeverResurrects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, cfg := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{AllowRetentionPurge: true}},
		{"badger", graphpkg.Config{AllowRetentionPurge: true, BadgerInMemory: true}},
	} {
		for _, mode := range []adminpkg.PurgeMode{adminpkg.PurgeByAge, adminpkg.PurgeByValidTo} {
			t.Run(fmt.Sprintf("%s/mode=%d", cfg.name, mode), func(t *testing.T) {
				g, err := graphpkg.New(cfg.cfg)
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				defer g.Close()
				f := newPubFixture(t, g)
				if err := g.Nodes().Retract(ctx, f.ev); err != nil {
					t.Fatalf("Retract: %v", err)
				}
				before := nowTx(t, g)
				if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Ev", Mode: mode, Before: before}); err != nil {
					t.Fatalf("PurgeExpiredNodes: %v", err)
				}
				pin := nowTx(t, g)
				retractAbsentEverywhere(t, "after purge", g, f, pin)
				if h, err := g.Nodes().History(f.ev); err == nil && len(h) > 0 && !newestNode(t, g, f.ev).Temporal().Retracted {
					t.Fatal("the purge kept rows of the retracted life without the marker")
				}
			})
		}
	}
}

// TestRetract_ExportImportKeepsTheMarker: an export after a retraction and
// an import into a fresh graph reproduce the tombstones with the marker, and
// the imported graph answers like the source at pins after T. Catches an
// export or import record format that drops the marker (the import would
// read the retraction as a Delete).
func TestRetract_ExportImportKeepsTheMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			src := b.open(t)
			f := newPubFixture(t, src)
			if err := src.Nodes().Retract(ctx, f.ev); err != nil {
				t.Fatalf("Retract: %v", err)
			}
			var buf bytes.Buffer
			if err := src.IO().Export(&buf); err != nil {
				t.Fatalf("Export: %v", err)
			}
			dst := b.open(t)
			if err := dst.IO().Import(&buf, tkgio.ImportOptions{}); err != nil {
				t.Fatalf("Import: %v", err)
			}
			if got, want := historyStrings(t, dst, f), historyStrings(t, src, f); got != want {
				t.Fatalf("imported history\n%s\nwant\n%s", got, want)
			}
			if !newestNode(t, dst, f.ev).Temporal().Retracted || !newestRel(t, dst, f.rel).Temporal().Retracted {
				t.Fatal("import dropped the retraction marker")
			}
			retractAbsentEverywhere(t, "imported graph", dst, f, nowTx(t, dst))
		})
	}
}

// TestRetract_ReplicaReproducesTheMarker: a replica tailing the change feed
// reproduces the retraction tombstones byte for byte, the marker included
// (compared directly: the marker is not in the content hash, so a hash
// comparison alone would pass a replica that drops it), and answers like the
// primary at pins after T. Catches a change-feed record or an apply path
// that drops the marker: the replica would read a Delete.
func TestRetract_ReplicaReproducesTheMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, withTx := range []bool{false, true} {
		t.Run(fmt.Sprintf("withTx=%v", withTx), func(t *testing.T) {
			primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true, AllowTxBackfill: true})
			if err != nil {
				t.Fatalf("primary New: %v", err)
			}
			defer primary.Close()
			f := newPubFixture(t, primary)
			f2 := newPubFixture(t, primary)
			var snap bytes.Buffer
			if err := primary.IO().Export(&snap); err != nil {
				t.Fatalf("Export: %v", err)
			}
			lsn0, err := primary.Replication().LastCommittedLSN()
			if err != nil {
				t.Fatalf("LastCommittedLSN: %v", err)
			}
			replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
			if err != nil {
				t.Fatalf("replica New: %v", err)
			}
			defer replica.Close()
			if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
				t.Fatalf("replica Import: %v", err)
			}
			if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
				t.Fatalf("SetAppliedLSN: %v", err)
			}
			if withTx {
				at := nowTx(t, primary)
				err = primary.Nodes().RetractWithTx(ctx, f.ev, at)
				if err == nil {
					err = primary.Rels().RetractWithTx(ctx, f2.rel, nowTx(t, primary))
				}
			} else {
				err = primary.Nodes().Retract(ctx, f.ev)
				if err == nil {
					err = primary.Rels().Retract(ctx, f2.rel)
				}
			}
			if err != nil {
				t.Fatalf("retract on the primary: %v", err)
			}
			from, err := replica.Replication().AppliedLSN()
			if err != nil {
				t.Fatalf("AppliedLSN: %v", err)
			}
			var recs []store.ChangeRecord
			if err := primary.Replication().ForEachChange(from, func(rec store.ChangeRecord) bool {
				recs = append(recs, rec)
				return true
			}); err != nil {
				t.Fatalf("ForEachChange: %v", err)
			}
			if _, err := replica.Replication().ApplyChanges(recs); err != nil {
				t.Fatalf("ApplyChanges: %v", err)
			}
			for _, fx := range []pubFixture{f, f2} {
				if got, want := historyStrings(t, replica, fx), historyStrings(t, primary, fx); got != want {
					t.Fatalf("replica history\n%s\nwant (primary)\n%s", got, want)
				}
			}
			if !newestNode(t, replica, f.ev).Temporal().Retracted || !newestRel(t, replica, f2.rel).Temporal().Retracted {
				t.Fatal("the replica dropped the retraction marker")
			}
			pin := nowTx(t, primary)
			retractAbsentEverywhere(t, "primary", primary, f, pin)
			if _, err := replica.Temporal().NodeAtTx(f.ev, f.vf, pin); err == nil {
				t.Fatal("replica: the retracted node answers at its past valid time")
			}
			if _, err := replica.Temporal().RelAtTx(f2.rel, f2.vf, pin); err == nil {
				t.Fatal("replica: the retracted relationship answers at its past valid time")
			}
		})
	}
}
