package io_test

import (
	"context"
	"bytes"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/io" // godoc anchor: ExampleAPI_<method> resolves against io.API
)

// ExampleAPI_Export demonstrates writing a portable graph snapshot to an
// io.Writer (here a bytes.Buffer for the example).
func ExampleAPI_Export() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})

	var buf bytes.Buffer
	if err := g.IO().Export(&buf); err != nil {
		panic(err)
	}
}

// ExampleAPI_Import demonstrates restoring a previously-exported graph
// snapshot into a fresh graph instance.
func ExampleAPI_Import() {
	src, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer src.Close()
	_, _ = src.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})

	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		panic(err)
	}

	dst, err := graph.New(graph.Config{SnowflakeNodeID: 1})
	if err != nil {
		panic(err)
	}
	defer dst.Close()
	if err := dst.IO().Import(&buf, tkgio.ImportOptions{}); err != nil {
		panic(err)
	}
}
