package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11 review (Fable, 2026-10-17): the doors that write a row valid from
// the explicit 0 other than a create, and the readers the first pass left
// untested. Each test was red on the round's first four commits.

// TestRound11ReplicaAppliesValidFromZero: a replica applies a primary's feed
// after an ExplicitInstant(0) create (the valid_from_zero meta entry and the
// row) and its row keeps the claim, on a memory and a badger replica. Before: the replica failed closed on the
// unknown meta key and stopped applying.
func TestRound11ReplicaAppliesValidFromZero(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"memory", "badger"} {
		t.Run(name, func(t *testing.T) {
			pcfg := graphpkg.Config{SnowflakeNodeID: 1, ChangeLog: true}
			rcfg := graphpkg.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true}
			// The feed comes from a memory primary (its log is readable without a
			// flush); the replica's own store is the one under test.
			pcfg.Store = memory.New(memory.WithChangeLog())
			if name == "memory" {
				rcfg.Store = memory.New()
			} else {
				rcfg.BadgerInMemory = true
			}
			primary, err := graphpkg.New(pcfg)
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			replica, err := graphpkg.New(rcfg)
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			replica.SetReplicationSource(primary.Replication())
			n, err := primary.Nodes().Add(ctx, []string{"Z"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.ExplicitInstant(0)})
			if err != nil {
				t.Fatal(err)
			}
			var recs []storepkg.ChangeRecord
			if err := primary.Replication().ForEachChange(0, func(rec storepkg.ChangeRecord) bool {
				recs = append(recs, rec)
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := replica.Replication().ApplyChanges(recs); err != nil {
				t.Fatalf("replica apply after an ExplicitInstant(0) create: %v", err)
			}
			got, err := replica.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			if !got.StartsAtZero() {
				t.Errorf("replica row: StartsAtZero = false, want the claim")
			}
			if _, err := replica.Temporal().NodeAt(n.ID(), 0); err != nil {
				t.Errorf("replica NodeAt(0): %v, want the row", err)
			}
		})
	}
}

// TestRound11MarkerAfterResetAndImport: marker 8 is raised by every door that
// writes such a row, not once per process: after Admin().Reset() the next
// explicit-0 create raises it again, and an import of an export holding such
// a row raises it on the target, as do a delta merge and a foreign stub (a
// reader of markers up to 7 refuses each store). Positive control: a store that never held such a row opens.
func TestRound11MarkerAfterResetAndImport(t *testing.T) {
	ctx := context.Background()
	zero := map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)}
	openBadger := func(t *testing.T, dir string, cfg graphpkg.Config) *graphpkg.Graph {
		t.Helper()
		st, err := badger.New(badger.Config{Dir: dir, TemporalIndexOnDisk: true})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Store = st
		g, err := graphpkg.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	refusedBy7 := func(t *testing.T, dir, what string, want bool) {
		t.Helper()
		restore := badger.SetMaxReadableStoreFormatVersionForTest(7)
		defer restore()
		err := r11OpenDurable("badger", dir)
		if got := errors.Is(err, storepkg.ErrWireFormatVersionUnsupported); got != want {
			t.Errorf("%s: a reader of markers up to 7 opening the store: %v, want refused=%v", what, err, want)
		}
	}

	t.Run("reset", func(t *testing.T) {
		dir := t.TempDir()
		g := openBadger(t, dir, graphpkg.Config{SnowflakeNodeID: 1, AllowReset: true})
		if _, err := g.Nodes().Add(ctx, []string{"Ref"}, zero); err != nil {
			t.Fatal(err)
		}
		if err := g.Admin().Reset(); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Add(ctx, []string{"Ref"}, zero); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		refusedBy7(t, dir, "after Reset and a new explicit-0 row", true)
	})

	t.Run("import", func(t *testing.T) {
		src, err := graphpkg.New(graphpkg.Config{Store: memory.New(), SnowflakeNodeID: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		n, err := src.Nodes().Add(ctx, []string{"Ref"}, zero)
		if err != nil {
			t.Fatal(err)
		}
		var full bytes.Buffer
		if err := src.IO().Export(&full); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		target := openBadger(t, dir, graphpkg.Config{SnowflakeNodeID: 2})
		if err := target.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
			t.Fatalf("import: %v", err)
		}
		got, err := target.Nodes().Get(ctx, n.ID())
		if err != nil {
			t.Fatal(err)
		}
		if !got.StartsAtZero() {
			t.Errorf("imported row lost the claim")
		}
		if err := target.Close(); err != nil {
			t.Fatal(err)
		}
		refusedBy7(t, dir, "import target holding an explicit-0 row", true)
	})

	openSharded := func(t *testing.T, dir string) *graphpkg.Graph {
		t.Helper()
		st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	refusedBy7Sharded := func(t *testing.T, dir, what string) {
		t.Helper()
		restore := badger.SetMaxReadableStoreFormatVersionForTest(7)
		defer restore()
		if err := r11OpenDurable("sharded", dir); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
			t.Errorf("%s: a reader of markers up to 7 opened the store: %v, want ErrWireFormatVersionUnsupported", what, err)
		}
	}

	t.Run("merge", func(t *testing.T) {
		src, err := graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog()), ChangeLog: true, SnowflakeNodeID: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		if _, err := src.Nodes().Add(ctx, []string{"Ref"}, nil); err != nil {
			t.Fatal(err)
		}
		var full bytes.Buffer
		if err := src.IO().Export(&full); err != nil {
			t.Fatal(err)
		}
		base, err := tkgio.HeaderOf(bytes.NewReader(full.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		target := openBadger(t, dir, graphpkg.Config{SnowflakeNodeID: 2})
		if err := target.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
			t.Fatal(err)
		}
		n, err := src.Nodes().Add(ctx, []string{"Ref"}, zero)
		if err != nil {
			t.Fatal(err)
		}
		var delta bytes.Buffer
		if err := src.IO().ExportSince(&delta, base.To); err != nil {
			t.Fatal(err)
		}
		if err := target.IO().ImportMerge(bytes.NewReader(delta.Bytes()), tkgio.MergeOptions{ExpectBase: base.To}); err != nil {
			t.Fatalf("merge of a delta holding an explicit-0 row: %v", err)
		}
		got, err := target.Nodes().Get(ctx, n.ID())
		if err != nil {
			t.Fatal(err)
		}
		if !got.StartsAtZero() {
			t.Errorf("merged row lost the claim")
		}
		if err := target.Close(); err != nil {
			t.Fatal(err)
		}
		refusedBy7(t, dir, "merge target holding an explicit-0 row", true)
	})

	t.Run("foreign stub", func(t *testing.T) {
		dir := t.TempDir()
		g := openSharded(t, dir)
		end, err := g.Nodes().Add(ctx, []string{"Person"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		edge := storepkg.ForeignIncomingEdge{
			RelID: types.RelID(snowflake.ID(700003)), TypeName: "KNOWS",
			StartID: types.NodeID(snowflake.ID(700001)), EndID: end.ID(),
			FromHash: "aa11", ToHash: "bb22", TxFrom: 1234, Version: 0, AttestTx: 1,
			ValidFromZero: true,
		}
		if err := g.Rels().RecordForeignIncoming(ctx, edge); err != nil {
			t.Fatal(err)
		}
		in, err := g.Rels().Incoming(end.ID(), "KNOWS")
		if err != nil || len(in) != 1 || !in[0].StartsAtZero() {
			t.Fatalf("stub: %v %v, want one row valid from the explicit 0", in, err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		refusedBy7Sharded(t, dir, "a store holding a foreign stub valid from the explicit 0")
	})

	t.Run("control", func(t *testing.T) {
		dir := t.TempDir()
		g := openBadger(t, dir, graphpkg.Config{SnowflakeNodeID: 1})
		if _, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{types.ShadowValidFrom: types.Instant(0)}); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		refusedBy7(t, dir, "a store with a plain 0 only", false)
	})
}

// TestRound11CloseAtZeroTimelines: a version valid from the explicit 0 closed
// at 100 is one segment [0, 100) on the timeline doors as on the point door
// (twice: the second read may come from the head cache), and one count
// segment, on every backend; a correction before 0 on another entity keeps
// its explicit-0 version's end.
func TestRound11CloseAtZeroTimelines(t *testing.T) {
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			n, err := g.Nodes().Add(ctx, []string{"C"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.ExplicitInstant(0)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().CloseVersion(ctx, n.ID(), 100); err != nil {
				t.Fatal(err)
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for pass := range 2 {
				segs, err := g.Temporal().NodeEffectiveTimeline(n.ID(), now)
				if err != nil {
					t.Fatal(err)
				}
				if len(segs) != 1 || segs[0].ValidFrom != 0 || segs[0].ValidTo != 100 {
					t.Errorf("pass %d: NodeEffectiveTimeline = %+v, want one segment [0, 100)", pass, segs)
				}
			}
			var scan []string
			if err := g.Temporal().ForEachNodeEffectiveByLabel("C", now, func(s temporal.NodeSegment) bool {
				scan = append(scan, fmt.Sprintf("[%d,%d)", s.ValidFrom, s.ValidTo))
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if len(scan) != 1 || scan[0] != "[0,100)" {
				t.Errorf("ForEachNodeEffectiveByLabel = %v, want [[0,100)]", scan)
			}
			cnt, _, err := g.Nodes().CountTimelineByLabel("C", now, storepkg.CountTimelineOpts{})
			if err != nil {
				t.Fatal(err)
			}
			if len(cnt) != 1 || cnt[0].From != 0 || cnt[0].To != 100 || cnt[0].Count != 1 {
				t.Errorf("CountTimelineByLabel = %+v, want [{0 100 1}]", cnt)
			}
			if _, err := g.Temporal().NodeAt(n.ID(), 100); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
				t.Errorf("NodeAt(100) after the close: %v, want ErrNoVersionValidAt", err)
			}
		})
	}
}

// TestRound11DryRunClaimAtZero: the dry run takes the claim of a relationship
// valid from the explicit 0 (DryRunRel.ValidFromSet) and finds the violation
// the create is refused with; without ValidFromSet a ValidFrom of 0 keeps
// validating "as if asserted now" (no violation; UntimedFromTxFrom). A node fact carries the
// claim as ExplicitInstant in its properties.
func TestRound11DryRunClaimAtZero(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(txFromCfg) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			if err := g.Constraints().Add(temporal.TemporalConstraint{Kind: temporal.ConstraintRelWithinEndpoints}); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{types.ShadowValidFrom: types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			c, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{types.ShadowValidFrom: types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			_, applyErr := g.Rels().Add(ctx, "R", a, c, map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)})
			if !errors.Is(applyErr, temporal.ErrRelBeforeStartNode) {
				t.Fatalf("create valid from the explicit 0 before its endpoints: %v, want ErrRelBeforeStartNode", applyErr)
			}
			viol, err := g.Constraints().DryRunValidate(ctx, constraintspkg.DryRunFacts{Rels: []constraintspkg.DryRunRel{
				{Ref: "zero", TypeName: "R", StartID: a.ID(), EndID: c.ID(), ValidFrom: 0, ValidFromSet: true},
			}})
			if err != nil || len(viol) != 1 || !errors.Is(viol[0].Err, temporal.ErrRelBeforeStartNode) {
				t.Errorf("dry run of the claim at 0: %+v (%v), want the create's ErrRelBeforeStartNode", viol, err)
			}
			viol, err = g.Constraints().DryRunValidate(ctx, constraintspkg.DryRunFacts{Rels: []constraintspkg.DryRunRel{
				{Ref: "plain", TypeName: "R", StartID: a.ID(), EndID: c.ID()},
			}})
			if err != nil || len(viol) != 0 {
				t.Errorf("dry run with a plain 0: %+v (%v), want none (as if asserted now)", viol, err)
			}
		})
	}
}

// TestRound11ShadowAndForeignPresence: the shadow resolver answers
// ExplicitInstant(0) for tkg_valid_from of a row valid from the explicit 0
// (and Instant(0) for an unset one), so writing the value read back makes the
// same claim; a foreign edge with the marker and a non-zero valid-from is
// refused by Validate.
func TestRound11ShadowAndForeignPresence(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	z, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)})
	if err != nil {
		t.Fatal(err)
	}
	p, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{types.ShadowValidFrom: types.Instant(0)})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := g.Resolve().NodeProperty(z, types.ShadowValidFrom); !ok || v != any(types.ExplicitInstant(0)) {
		t.Errorf("shadow tkg_valid_from of the claim: %#v %v, want ExplicitInstant(0)", v, ok)
	}
	if v, ok := g.Resolve().NodeProperty(p, types.ShadowValidFrom); !ok || v != any(types.Instant(0)) {
		t.Errorf("shadow tkg_valid_from of an unset 0: %#v %v, want Instant(0)", v, ok)
	}
	v, _ := g.Resolve().NodeProperty(z, types.ShadowValidFrom)
	copied, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{types.ShadowValidFrom: v})
	if err != nil {
		t.Fatal(err)
	}
	if !copied.StartsAtZero() {
		t.Errorf("writing the shadow value back lost the claim")
	}
	hub, err := g.Nodes().Add(ctx, []string{"H"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(ctx, "E", hub, z, map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := g.Resolve().RelProperty(r, types.ShadowValidFrom); !ok || v != any(types.ExplicitInstant(0)) {
		t.Errorf("shadow tkg_valid_from of a relationship's claim: %#v %v, want ExplicitInstant(0)", v, ok)
	}
	bad := storepkg.ForeignIncomingEdge{RelID: r.ID(), TypeName: "E", StartID: hub.ID(), EndID: z.ID(), AttestTx: 1, ValidFrom: 5, ValidFromZero: true}
	if err := bad.Validate(); !errors.Is(err, storepkg.ErrInvalidForeignIncoming) {
		t.Errorf("foreign edge with the marker and valid_from 5: %v, want ErrInvalidForeignIncoming", err)
	}
	bad.ValidFrom = 0
	if err := bad.Validate(); err != nil {
		t.Errorf("foreign edge with the marker and valid_from 0: %v, want valid", err)
	}
}

// TestRound11RefusedAtZeroParity: the relationship and GraphTx twins of the
// refusals r11RefusedAtZero covers for nodes (in place, caller instant,
// re-import, excise, transaction updates), and the accept path of the batched
// create AddNodes.
func TestRound11RefusedAtZeroParity(t *testing.T) {
	ctx := context.Background()
	zero := func() map[string]any { return map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)} }
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			refused := func(op string, err error) {
				t.Helper()
				if !errors.Is(err, graphpkg.ErrValidEndAtZero) || !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					t.Errorf("%s: %v, want ErrValidEndAtZero (wrapping ErrInvalidTimeRange)", op, err)
				}
			}
			hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			end, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{types.ShadowValidFrom: types.Instant(-100)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "E", hub, end, map[string]any{types.ShadowValidFrom: types.Instant(-50)})
			if err != nil {
				t.Fatal(err)
			}
			zr, err := g.Rels().Add(ctx, "E", hub, end, zero())
			if err != nil {
				t.Fatal(err)
			}
			_, err = g.Rels().UpdateInPlace(ctx, r.ID(), zero())
			refused("relationship update in place", err)
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			_, err = g.Rels().UpdateWithTx(ctx, r.ID(), zero(), now+1)
			refused("relationship update with a caller instant", err)
			refused("relationship excise crossing 0", g.Temporal().ExciseRelValidity(ctx, zr.ID(), -5, 5))
			gone, err := g.Rels().Add(ctx, "E", hub, end, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Rels().Delete(ctx, gone.ID()); err != nil {
				t.Fatal(err)
			}
			_, err = g.Rels().Import(ctx, gone.ID(), "E", hub, end, zero())
			refused("relationship re-import of a deleted ID", err)
			refused("GraphTx UpdateNode", g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.UpdateNode(end.ID(), zero())
				return err
			}))
			refused("GraphTx UpdateRelationship", g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.UpdateRelationship(r.ID(), zero())
				return err
			}))
			// The rows are unchanged by the refusals: r still starts at -50.
			got, err := g.Rels().Get(ctx, r.ID())
			if err != nil {
				t.Fatal(err)
			}
			if vf, ok := got.ValidFromClaim(); !ok || vf != -50 {
				t.Errorf("relationship after the refusals: valid from %d (%v), want -50", vf, ok)
			}
			// Accept path: a batched create may claim 0.
			var made []*types.Node
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				var err error
				made, err = tx.AddNodes([]string{"Z"}, []map[string]any{zero(), {"k": int64(1)}})
				return err
			}); err != nil {
				t.Fatalf("AddNodes with an explicit-0 create: %v", err)
			}
			if len(made) != 2 || !made[0].StartsAtZero() || made[1].StartsAtZero() {
				t.Errorf("AddNodes: claims %v / %v, want true / false", made[0].StartsAtZero(), made[1].StartsAtZero())
			}
		})
	}
}
