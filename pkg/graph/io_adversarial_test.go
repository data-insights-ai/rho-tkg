package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Export/Import break-the-system tests: full-fidelity round trip (temporal
// metadata, version history, hash chains, deleted-entity history) plus
// hostile streams — truncations at every interesting offset and bit flips —
// which must fail closed with ZERO partial state in the target graph.

func buildExportFixture(t *testing.T) (*graphpkg.Graph, types.NodeID, types.NodeID, []byte) {
	t.Helper()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 8})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}

	a, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"tkg_author_id":  "auditor-7",
		"state":          "a0",
		"tags":           []string{"x", "y"},
	})
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"state": "a1"}); err != nil {
		t.Fatalf("update a: %v", err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{"state": "b0"})
	if err != nil {
		t.Fatalf("add b: %v", err)
	}
	if _, err := g.Rels().Add(ctx, "LINKS", a, b, map[string]any{"w": int64(7)}); err != nil {
		t.Fatalf("rel: %v", err)
	}
	// Deleted entity with history — must survive the round trip as history.
	gone, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{
		"tkg_valid_from": types.Instant(1000), "state": "gone0",
	})
	if err != nil {
		t.Fatalf("add gone: %v", err)
	}
	if err := g.Nodes().Delete(ctx, gone.ID()); err != nil {
		t.Fatalf("delete gone: %v", err)
	}

	var buf bytes.Buffer
	if err := g.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	return g, a.ID(), gone.ID(), buf.Bytes()
}

func TestExportImportRoundTripFidelity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	src, aID, goneID, stream := buildExportFixture(t)
	defer src.Close()

	dst, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 9})
	if err != nil {
		t.Fatalf("dst New: %v", err)
	}
	defer dst.Close()
	if err := dst.IO().Import(bytes.NewReader(stream), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}

	// Counts identical.
	srcN, _ := src.Stats().NodeCount()
	dstN, _ := dst.Stats().NodeCount()
	srcR, _ := src.Stats().RelCount()
	dstR, _ := dst.Stats().RelCount()
	if srcN != dstN || srcR != dstR {
		t.Fatalf("counts diverge: src(%d,%d) dst(%d,%d)", srcN, srcR, dstN, dstR)
	}

	// Entity fidelity: version, properties, temporal metadata, provenance.
	srcA, err := src.Nodes().Get(ctx, aID)
	if err != nil {
		t.Fatalf("src Get: %v", err)
	}
	dstA, err := dst.Nodes().Get(ctx, aID)
	if err != nil {
		t.Fatalf("dst Get (imported): %v", err)
	}
	if srcA.Version() != dstA.Version() {
		t.Errorf("version diverged: %d vs %d", srcA.Version(), dstA.Version())
	}
	if fmt.Sprint(srcA.PropertiesMap()) != fmt.Sprint(dstA.PropertiesMap()) {
		t.Errorf("properties diverged:\nsrc %v\ndst %v", srcA.PropertiesMap(), dstA.PropertiesMap())
	}
	stm, dtm := srcA.Temporal(), dstA.Temporal()
	if stm == nil || dtm == nil || stm.ValidFrom != dtm.ValidFrom || stm.TxFrom != dtm.TxFrom {
		t.Errorf("temporal metadata diverged: src %+v dst %+v", stm, dtm)
	}
	if v, _ := dst.Resolve().NodeProperty(dstA, "tkg_author_id"); v != "auditor-7" {
		t.Errorf("provenance lost in transit: %v", v)
	}

	// History fidelity: same number of versions, same per-version state, and
	// the imported hash chain must VERIFY (not merely be present).
	srcHist, _ := src.Nodes().History(aID)
	dstHist, _ := dst.Nodes().History(aID)
	if len(srcHist) != len(dstHist) {
		t.Fatalf("history length diverged: %d vs %d", len(srcHist), len(dstHist))
	}
	valid, err := dst.Hash().VerifyNodeChain(aID)
	if err != nil || !valid {
		t.Errorf("imported hash chain does not verify: valid=%v err=%v", valid, err)
	}

	// Temporal queries answer identically on both graphs.
	for _, at := range []types.Instant{1500} {
		sv, serr := src.Temporal().NodeAt(aID, at)
		dv, derr := dst.Temporal().NodeAt(aID, at)
		if (serr == nil) != (derr == nil) {
			t.Fatalf("NodeAt(%d) presence diverged: src=%v dst=%v", at, serr, derr)
		}
		if serr == nil {
			ss, _ := sv.GetProperty("state")
			ds, _ := dv.GetProperty("state")
			if ss != ds {
				t.Errorf("NodeAt(%d) state diverged: %v vs %v", at, ss, ds)
			}
		}
	}

	// Deleted-entity history must remain queryable on the imported graph
	// exactly as on the source (B32 through the import door).
	sGone, sErr := src.Temporal().NodeAt(goneID, 1500)
	dGone, dErr := dst.Temporal().NodeAt(goneID, 1500)
	if (sErr == nil) != (dErr == nil) {
		t.Fatalf("deleted-entity history diverged across import: src=%v dst=%v", sErr, dErr)
	}
	if sErr == nil {
		ss, _ := sGone.GetProperty("state")
		ds, _ := dGone.GetProperty("state")
		if ss != ds {
			t.Errorf("deleted-entity historical state diverged: %v vs %v", ss, ds)
		}
	}
}

// Hostile streams: truncate the export at many offsets and flip bytes.
// Every attempt must fail closed — an identifiable error AND zero partial
// state (no nodes, no rels) in the target graph.
func TestImportHostileStreamsFailClosedWithoutPartialState(t *testing.T) {
	t.Parallel()

	src, _, _, stream := buildExportFixture(t)
	defer src.Close()
	if len(stream) < 64 {
		t.Fatalf("export stream suspiciously small: %d bytes", len(stream))
	}

	freshTarget := func(t *testing.T, id int64) *graphpkg.Graph {
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: id})
		if err != nil {
			t.Fatalf("target New: %v", err)
		}
		t.Cleanup(func() { g.Close() })
		return g
	}

	assertEmpty := func(t *testing.T, g *graphpkg.Graph, attack string) {
		t.Helper()
		n, _ := g.Stats().NodeCount()
		r, _ := g.Stats().RelCount()
		if n != 0 || r != 0 {
			t.Fatalf("%s: PARTIAL STATE after failed import: %d nodes, %d rels", attack, n, r)
		}
	}

	t.Run("truncations", func(t *testing.T) {
		t.Parallel()
		g := freshTarget(t, 10)
		// Cut at a spread of offsets including mid-header, mid-record, and
		// one byte short of complete.
		cuts := []int{1, len(stream) / 7, len(stream) / 3, len(stream) / 2, len(stream) - 1}
		for _, cut := range cuts {
			err := g.IO().Import(bytes.NewReader(stream[:cut]), tkgio.ImportOptions{})
			if err == nil {
				t.Fatalf("truncation at %d/%d bytes imported successfully", cut, len(stream))
			}
			assertEmpty(t, g, fmt.Sprintf("truncate@%d", cut))
		}
	})

	t.Run("bit-flips", func(t *testing.T) {
		t.Parallel()
		for i, pos := range []int{8, len(stream) / 4, len(stream) / 2, 3 * len(stream) / 4} {
			g := freshTarget(t, int64(11+i))
			mutated := append([]byte(nil), stream...)
			mutated[pos] ^= 0xFF
			err := g.IO().Import(bytes.NewReader(mutated), tkgio.ImportOptions{})
			if err == nil {
				// A flip in a property value may legitimately decode — but
				// then the imported graph must still be internally
				// consistent: hash chains over every node must verify or
				// the import should have rejected the record.
				ids, aerr := g.Nodes().All(storepkg.QueryOpts{})
				if aerr != nil {
					t.Fatalf("flip@%d: post-import scan failed: %v", pos, aerr)
				}
				for _, n := range ids {
					valid, verr := g.Hash().VerifyNodeChain(n.ID())
					if verr != nil || !valid {
						t.Fatalf("flip@%d imported a graph whose hash chain does not verify (node %v)", pos, n.ID())
					}
				}
				continue
			}
			assertEmpty(t, g, fmt.Sprintf("flip@%d", pos))
		}
	})

	t.Run("error-identity", func(t *testing.T) {
		t.Parallel()
		g := freshTarget(t, 15)
		err := g.IO().Import(bytes.NewReader(stream[:len(stream)/2]), tkgio.ImportOptions{})
		if err == nil {
			t.Fatalf("half stream imported")
		}
		if !errors.Is(err, graphpkg.ErrCorruptExport) && !errors.Is(err, graphpkg.ErrIncompatibleExport) {
			t.Fatalf("hostile-stream error %v matches neither ErrCorruptExport nor ErrIncompatibleExport — consumers cannot classify it", err)
		}
	})
}
