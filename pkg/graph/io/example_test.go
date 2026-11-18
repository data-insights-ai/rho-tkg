package io_test

import (
	"bytes"
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io" // godoc anchor: ExampleAPI_<method> resolves against io.API
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
	fmt.Println(buf.Len() > 0)
	// Output: true
}

// ExampleAPI_Import demonstrates restoring a previously-exported graph
// snapshot into a fresh graph instance.
func ExampleAPI_Import() {
	src, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer src.Close()
	n, _ := src.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})

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
	got, err := dst.Nodes().Get(context.Background(), n.ID())
	if err != nil {
		panic(err)
	}
	name, _ := got.GetProperty("name")
	count, _ := dst.Nodes().Count()
	fmt.Println(name, count)
	// Output: Alice 1
}
