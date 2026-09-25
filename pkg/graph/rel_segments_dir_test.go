package graph_test

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0011 S3, graph level: Config.SegmentDir.

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "seg-") && strings.HasSuffix(e.Name(), ".tkgs") {
			out = append(out, e.Name())
		}
	}
	return out
}

// The S2 differential oracle with the declared replica's segments on disk:
// every graph door over self-contained, memory-mapped segment files answers
// like the undeclared replica, ScanRelSegments included (ID order, twice).
func TestRelSegmentsDifferentialOracleOnDisk(t *testing.T) {
	var dirs []string
	root := t.TempDir() // outlives the per-seed subtests: checked below
	runSegOracle(t, func(t *testing.T) string {
		d, err := os.MkdirTemp(root, "segments-")
		if err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, d)
		return d
	})
	for _, d := range dirs {
		if len(segFiles(t, d)) == 0 {
			t.Fatalf("%s holds no segment file: the replica never sealed to disk", d)
		}
	}
}

// A graph reopened over its SegmentDir answers the sealed HOP rows exactly
// as the closed graph did — IDs, versions, temporal fields, hashes (so
// VerifyRelChain holds), properties — through the row doors and
// ScanRelSegments; declarations are persisted, and the directory is locked
// while a graph has it open.
func TestRelSegmentsReopenFromSegmentDir(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "segments")
	cfg := func() graph.Config {
		return graph.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graph.RelSegmentSpec{segOracleSpec}, SegmentDir: dir}
	}
	g, err := graph.New(cfg())
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(5))
	var nodes []*types.Node
	for i := 0; i < 12; i++ {
		n, err := g.Nodes().Add(ctx, []string{"Asset"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)
	}
	for i := 0; i < 400; i++ {
		a, b := nodes[r.Intn(len(nodes))], nodes[r.Intn(len(nodes))]
		if a.ID() == b.ID() {
			continue
		}
		if _, err := g.Rels().Add(ctx, segOracleHOP, a, b, segHOPProps(r)); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Admin().SealRelSegments(segOracleHOP); err != nil {
		t.Fatal(err)
	}
	want := segRelsFP(g.Rels().ByType(segOracleHOP, graph.QueryOpts{}))
	o := &segOracle{t: t}
	wantScan, _ := o.scanFacts(g)
	if len(segFiles(t, dir)) != 1 {
		t.Fatalf("one seal, one file: %v", segFiles(t, dir))
	}
	if _, err := graph.New(cfg()); !errors.Is(err, graph.ErrRelSegmentDirLocked) {
		t.Fatalf("a second graph on an open SegmentDir = %v, want ErrRelSegmentDirLocked", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g2, err := graph.New(cfg())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	if got := segRelsFP(g2.Rels().ByType(segOracleHOP, graph.QueryOpts{})); got != want {
		t.Fatalf("reopened ByType differs:\n%.600s", segFirstDiff(got, want))
	}
	gotScan, ok := o.scanFacts(g2)
	if !ok || strings.Join(gotScan, "\n") != strings.Join(wantScan, "\n") {
		t.Fatalf("reopened ScanRelSegments differs (ok %t, %d vs %d rows)", ok, len(gotScan), len(wantScan))
	}
	rows, err := g2.Rels().ByType(segOracleHOP, graph.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if ok, err := g2.Hash().VerifyRelChain(row.ID()); !ok || err != nil {
			t.Fatalf("VerifyRelChain(%d) on a reopened sealed row = %t, %v", row.ID(), ok, err)
		}
	}
	st, err := g2.Admin().RelSegmentStats(segOracleHOP)
	if err != nil || st.Segments != 1 || st.LiveSealedRows != int64(len(rows)) {
		t.Fatalf("reopened stats %+v %v (%d rows)", st, err, len(rows))
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	changed := segOracleSpec
	changed.Columns = changed.Columns[:3]
	if g3, err := graph.New(graph.Config{RelSegments: []graph.RelSegmentSpec{changed}, SegmentDir: dir}); !errors.Is(err, graph.ErrRelSegmentDeclaration) {
		if err == nil {
			_ = g3.Close()
		}
		t.Fatalf("reopen with another schema = %v, want ErrRelSegmentDeclaration", err)
	}
	// Damage fails New closed.
	path := filepath.Join(dir, segFiles(t, dir)[0])
	if err := os.Truncate(path, 100); err != nil {
		t.Fatal(err)
	}
	if g4, err := graph.New(cfg()); !errors.Is(err, graph.ErrRelSegmentFileInvalid) {
		if err == nil {
			_ = g4.Close()
		}
		t.Fatalf("reopen with a truncated segment = %v, want ErrRelSegmentFileInvalid", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if g5, err := graph.New(cfg()); !errors.Is(err, graph.ErrRelSegmentManifestInvalid) {
		if err == nil {
			_ = g5.Close()
		}
		t.Fatalf("reopen with a damaged manifest = %v, want ErrRelSegmentManifestInvalid", err)
	}
}

func TestRelSegmentsSegmentDirConfig(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		cfg  graph.Config
		want error
	}{
		{"SegmentDir without RelSegments", graph.Config{SegmentDir: dir}, graph.ErrRelSegmentDeclaration},
		{"whitespace SegmentDir", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP"}}, SegmentDir: "  "}, graph.ErrRelSegmentDeclaration},
		{"SegmentDir is a file", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP"}}, SegmentDir: segConfigFile(t)}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := graph.New(tc.cfg)
			if err == nil {
				_ = g.Close()
				t.Fatalf("New succeeded, want an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("New = %v, want %v", err, tc.want)
			}
		})
	}
	// The default backend (memory) accepts a SegmentDir; badger has no
	// segments at all (S7).
	g, err := graph.New(graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP"}}, SegmentDir: filepath.Join(dir, "a")})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if bg, err := graph.New(graph.Config{BadgerInMemory: true, RelSegments: []graph.RelSegmentSpec{{Type: "HOP"}}, SegmentDir: filepath.Join(dir, "b")}); !errors.Is(err, graph.ErrCapabilityNotSupported) {
		if err == nil {
			_ = bg.Close()
		}
		t.Fatalf("badger with a SegmentDir = %v, want ErrCapabilityNotSupported", err)
	}
}

func segConfigFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
