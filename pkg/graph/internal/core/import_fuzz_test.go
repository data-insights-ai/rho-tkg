package core

// Fuzz harness for the import trust boundary, end-to-end.
//
// pkg/graph/internal/storeutil/wire_fuzz_test.go fuzzes the per-row wire decode
// in isolation. This harness attacks the WHOLE import pipeline: the record
// framing parser (tag + big-endian length, readImportStageRecord), the staging
// buffer, Phase-2 replay, the per-record dispatch (header / registry / node /
// rel / history), hash verification, and rollback. The contract:
//
//   - Import of ANY bytes must NEVER panic or crash the process — it returns an
//     error (rejected) or a successfully restored graph. (Lesson 44: import is a
//     trust boundary; corrupt/hostile streams fail closed, never crash.)
//   - A stream that imports SUCCESSFULLY must yield a self-consistent graph that
//     re-exports without error — no read-but-cannot-re-emit asymmetry.
//
// The seed corpus (run under plain `go test`) is a permanent regression
// battery; active fuzzing is `go test -fuzz=FuzzImport`.

import (
	"bytes"
	"context"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func fuzzImportSeedStreams(tb testing.TB) [][]byte {
	tb.Helper()
	ctx := context.Background()

	mustNode := func(g *Core, labels []string, props map[string]any) *types.Node {
		n, err := g.Nodes.Add(ctx, labels, props)
		if err != nil {
			tb.Fatalf("seed add node: %v", err)
		}
		return n
	}

	var seeds [][]byte
	export := func(build func(g *Core)) {
		g, err := New(Config{Store: memory.New()})
		if err != nil {
			tb.Fatalf("seed new: %v", err)
		}
		defer func() { _ = g.Close() }()
		build(g)
		var buf bytes.Buffer
		if err := g.IO.Export(&buf); err != nil {
			tb.Fatalf("seed export: %v", err)
		}
		seeds = append(seeds, buf.Bytes())
	}

	// Empty graph (header + registry records only).
	export(func(g *Core) {})

	// Nodes + relationship with mixed property types.
	export(func(g *Core) {
		n1 := mustNode(g, []string{"Person"}, map[string]any{
			"name": "Alice", "age": int64(30), "score": float64(9.5),
			"tags": []string{"a", "b"}, "flag": true,
		})
		n2 := mustNode(g, []string{"City"}, map[string]any{"city": "Vienna"})
		if _, err := g.Rels.Add(ctx, "LIVES_IN", n1, n2, map[string]any{"since": int64(2020)}); err != nil {
			tb.Fatalf("seed add rel: %v", err)
		}
	})

	// Node with version history (exercises the *Hist record paths + integrity).
	export(func(g *Core) {
		n := mustNode(g, []string{"Doc"}, map[string]any{"v": int64(1)})
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(2)}); err != nil {
			tb.Fatalf("seed update 1: %v", err)
		}
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(3)}); err != nil {
			tb.Fatalf("seed update 2: %v", err)
		}
	})

	return seeds
}

func FuzzImport(f *testing.F) {
	for _, s := range fuzzImportSeedStreams(f) {
		f.Add(s)
	}
	// Raw adversarial seeds: truncated/garbage framing.
	for _, b := range [][]byte{
		{},
		{0x01},                               // tag only, no length
		{0x01, 0x00, 0x00, 0x00},             // 4-byte length, truncated header
		{0x01, 0x00, 0x00, 0x00, 0x05},       // header declares 5 bytes, no body
		{0x03, 0xff, 0xff, 0xff, 0xff},       // huge declared length (> max)
		{0x99, 0x00, 0x00, 0x00, 0x00},       // unknown tag, empty body
		{0x01, 0x00, 0x00, 0x00, 0x01, 0xc0}, // header record body = nil
	} {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		g, err := New(Config{Store: memory.New()})
		if err != nil {
			t.Fatalf("new: %v", err)
		}
		defer func() { _ = g.Close() }()

		// Must never panic / crash, regardless of how hostile data is.
		if err := g.IO.Import(bytes.NewReader(data), tkgio.ImportOptions{}); err != nil {
			return // rejected — the correct outcome for corrupt input
		}

		// Imported successfully -> the graph must be self-consistent enough to
		// re-export without error (no read-but-cannot-persist asymmetry).
		var buf bytes.Buffer
		if err := g.IO.Export(&buf); err != nil {
			t.Fatalf("re-export of a successfully imported graph failed: %v", err)
		}
	})
}
