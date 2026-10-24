package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// OPENEXCISE (sigma for ai-soc's BA run). sigma retracts an open tail
// [a, open). Closing the current row by an update first restates its
// properties in a new row and tx-ends the old one, which let an older belief
// read again (A) and shadowed rows written earlier in the same transaction
// (B). ExciseNodeValidity / ExciseRelValidity with an open end (b = 0) end
// validity from a on without that close: at later pins nothing is valid at or
// after a, before a everything answers as before, and pins taken before the
// excise read as before.

func openExciseGraph(t *testing.T, backend string) *graphpkg.Graph {
	t.Helper()
	cfg := graphpkg.Config{SnowflakeNodeID: 3}
	if backend == "badger" {
		cfg.BadgerDir = t.TempDir()
	}
	g, err := graphpkg.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func propAt(g *graphpkg.Graph, id types.NodeID, at types.Instant) string {
	n, err := g.Temporal().NodeAt(id, at)
	if errors.Is(err, graphpkg.ErrNoVersionValidAt) {
		return "absent"
	}
	if err != nil {
		return err.Error()
	}
	v, _ := n.Properties().Get("c")
	return fmt.Sprint(v)
}

func propAtTx(g *graphpkg.Graph, id types.NodeID, at, pin types.Instant) string {
	n, err := g.Temporal().NodeAtTx(id, at, pin)
	if errors.Is(err, graphpkg.ErrNoVersionValidAt) {
		return "absent"
	}
	if err != nil {
		return err.Error()
	}
	v, _ := n.Properties().Get("c")
	return fmt.Sprint(v)
}

// (A): an older row with a later start must not read again.
func TestOpenExciseLeavesNothingAfterAnEarlierOpenStart(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			g := openExciseGraph(t, backend)
			n, err := g.Nodes().Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(1000), "c": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			id := n.ID()
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, id, 500, 0, map[string]any{"c": int64(1)}); err != nil {
				t.Fatal(err)
			}
			before, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal().ExciseNodeValidity(ctx, id, 500, 0); err != nil {
				t.Fatalf("open-ended excise refused: %v", err)
			}
			for _, at := range []types.Instant{500, 700, 1000, 1500, 1 << 40} {
				if got := propAt(g, id, at); got != "absent" {
					t.Errorf("after excising [500, open): NodeAt(%d) = %s, want absent", at, got)
				}
			}
			if got := propAt(g, id, 400); got != "absent" {
				t.Errorf("NodeAt(400) = %s, want absent (nothing was valid before 500)", got)
			}
			if got := propAtTx(g, id, 1500, before); got != "1" {
				t.Errorf("a pin before the excise: NodeAtTx(1500) = %s, want 1 as before", got)
			}
		})
	}
}

// (B): an extension written earlier in the same transaction keeps its range
// below a; only [a, open) goes.
func TestOpenExciseKeepsAnExtensionOfTheSameTransaction(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			g := openExciseGraph(t, backend)
			n, err := g.Nodes().Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(100), "c": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			id := n.ID()
			err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if _, err := tx.SetNodeVersionInterval(id, 500, 0, map[string]any{"c": int64(3)}); err != nil {
					return err
				}
				return tx.ExciseNodeValidity(id, 800, 0)
			})
			if err != nil {
				t.Fatalf("extension then open-ended excise in one transaction: %v", err)
			}
			for at, want := range map[types.Instant]string{200: "1", 600: "3", 799: "3", 800: "absent", 5000: "absent"} {
				if got := propAt(g, id, at); got != want {
					t.Errorf("NodeAt(%d) = %s, want %s", at, got, want)
				}
			}
		})
	}
}

// The relationship door.
func TestOpenExciseRel(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			g := openExciseGraph(t, backend)
			a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"tkg_valid_from": types.Instant(1)})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"tkg_valid_from": types.Instant(1)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().AddByID(ctx, "LINK", a.ID(), b.ID(), map[string]any{"tkg_valid_from": types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			before, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal().ExciseRelValidity(ctx, r.ID(), 300, 0); err != nil {
				t.Fatalf("open-ended relationship excise refused: %v", err)
			}
			if _, err := g.Temporal().RelAt(r.ID(), 200); err != nil {
				t.Errorf("RelAt(200): %v, want the relationship", err)
			}
			for _, at := range []types.Instant{300, 1000, 1 << 40} {
				if _, err := g.Temporal().RelAt(r.ID(), at); !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
					t.Errorf("RelAt(%d) answers after excising [300, open)", at)
				}
			}
			if _, err := g.Temporal().RelAtTx(r.ID(), 1000, before); err != nil {
				t.Errorf("a pin before the excise: RelAtTx(1000): %v, want the relationship", err)
			}
		})
	}
}

// TestOpenExciseRaisesTheStoreMarker: the open void piece is a new row shape,
// so a badger store holding one carries OpenVoidStoreFormatVersion and a
// release reading markers up to 9 refuses to open it (the one-way door), while
// a store that only had bounded excises stays readable there. Catches a marker
// left at 6 (an older binary opens the store and fails on the row
// mid-operation) and one raised for every excise.
func TestOpenExciseRaisesTheStoreMarker(t *testing.T) {
	ctx := context.Background()
	write := func(open bool) string {
		dir := t.TempDir()
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, BadgerDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		n, err := g.Nodes().Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(100), "c": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		b := types.Instant(300)
		if open {
			b = 0
		}
		if err := g.Temporal().ExciseNodeValidity(ctx, n.ID(), 200, b); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	olderOpens := func(dir string) error {
		restore := badger.SetMaxReadableStoreFormatVersionForTest(storeutil.StagedFlushStoreFormatVersion)
		defer restore()
		st, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			return err
		}
		return st.Close()
	}
	if err := olderOpens(write(false)); err != nil {
		t.Fatalf("a store with a bounded excise only must stay readable by a marker-9 release: %v", err)
	}
	if err := olderOpens(write(true)); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
		t.Fatalf("a marker-9 release opened a store holding an open void piece: %v", err)
	}
}
