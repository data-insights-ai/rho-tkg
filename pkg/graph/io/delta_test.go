package io_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

func TestCursorIsZero(t *testing.T) {
	t.Parallel()
	if !(tkgio.Cursor{}).IsZero() {
		t.Fatal("zero Cursor.IsZero() = false")
	}
	if (tkgio.Cursor{LSN: 1}).IsZero() {
		t.Fatal("Cursor{LSN:1}.IsZero() = true")
	}
	if (tkgio.Cursor{Epoch: 1}).IsZero() {
		t.Fatal("Cursor{Epoch:1}.IsZero() = true")
	}
}

// changeLogGraph builds a graph whose underlying badger store has the change-log
// enabled (the precondition for delta export). graph.Config.ChangeLog wires to
// the badger backend; the default memory store ignores it.
func changeLogGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.New(graph.Config{BadgerInMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestHeaderOf_FullAndDelta(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := changeLogGraph(t)
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Full export header.
	var full bytes.Buffer
	if err := g.IO().Export(&full); err != nil {
		t.Fatalf("export: %v", err)
	}
	fh, err := tkgio.HeaderOf(bytes.NewReader(full.Bytes()))
	if err != nil {
		t.Fatalf("HeaderOf(full): %v", err)
	}
	if fh.IsDelta {
		t.Fatal("full export IsDelta=true")
	}
	if fh.To.LSN == 0 || fh.To.Epoch == 0 {
		t.Fatalf("full export To = %+v, want complete base cursor", fh.To)
	}
	base := fh.To

	// Delta export header.
	if _, err := g.Nodes().Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("add 2: %v", err)
	}
	var delta bytes.Buffer
	if err := g.IO().ExportSince(&delta, base); err != nil {
		t.Fatalf("ExportSince: %v", err)
	}
	dh, err := tkgio.HeaderOf(bytes.NewReader(delta.Bytes()))
	if err != nil {
		t.Fatalf("HeaderOf(delta): %v", err)
	}
	if !dh.IsDelta {
		t.Fatal("delta IsDelta=false")
	}
	if dh.From != base {
		t.Fatalf("delta From = %+v, want %+v", dh.From, base)
	}
	if dh.To.Epoch != base.Epoch || dh.To.LSN <= base.LSN {
		t.Fatalf("delta To = %+v, want epoch %d and LSN > %d", dh.To, base.Epoch, base.LSN)
	}
}

func TestHeaderOf_Errors(t *testing.T) {
	t.Parallel()
	if _, err := tkgio.HeaderOf(nil); !errors.Is(err, tkgio.ErrNilReader) {
		t.Fatalf("HeaderOf(nil) = %v, want ErrNilReader", err)
	}
	// First record is not a header tag (0x03 = node record).
	if _, err := tkgio.HeaderOf(bytes.NewReader([]byte{0x03, 0, 0, 0, 0})); !errors.Is(err, tkgio.ErrCorruptExport) {
		t.Fatalf("HeaderOf(non-header) = %v, want ErrCorruptExport", err)
	}
	// Empty stream (no header record at all).
	if _, err := tkgio.HeaderOf(bytes.NewReader(nil)); !errors.Is(err, tkgio.ErrCorruptExport) {
		t.Fatalf("HeaderOf(empty) = %v, want ErrCorruptExport", err)
	}
}

// End-to-end through the public façade: full Export then ExportSince/ImportMerge
// reproduce the source after mutations.
func TestDeltaRoundTripThroughFacade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src := changeLogGraph(t)
	a, err := src.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}

	var full bytes.Buffer
	if err := src.IO().Export(&full); err != nil {
		t.Fatalf("export: %v", err)
	}
	base := mustHeaderTo(t, full.Bytes())

	dst, err := graph.New(graph.Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck
	if err := dst.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("import: %v", err)
	}

	if _, err := src.Nodes().Update(ctx, a.ID(), map[string]any{"name": "alice2"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	var delta bytes.Buffer
	if err := src.IO().ExportSince(&delta, base); err != nil {
		t.Fatalf("ExportSince: %v", err)
	}
	if err := dst.IO().ImportMerge(bytes.NewReader(delta.Bytes()), tkgio.MergeOptions{ExpectBase: base}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}

	// The merged node reflects the update.
	n, err := dst.Nodes().Get(ctx, a.ID())
	if err != nil {
		t.Fatalf("get merged node: %v", err)
	}
	if v, _ := n.GetProperty("name"); v != "alice2" {
		t.Fatalf("merged name = %v, want alice2", v)
	}
}

func mustHeaderTo(t *testing.T, stream []byte) tkgio.Cursor {
	t.Helper()
	h, err := tkgio.HeaderOf(bytes.NewReader(stream))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	return h.To
}
