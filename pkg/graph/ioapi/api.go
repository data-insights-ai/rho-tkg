// Package ioapi is a sub-API accessor for graph export/import.
package ioapi

import "io"

// Core is the subset of *graph.Graph methods the io sub-API forwards to.
type Core interface {
	ExportGraph(w io.Writer) error
	ImportGraph(r io.Reader) error
}

// API is the io sub-API accessor.
type API struct{ c Core }

// New constructs an io sub-API.
func New(c Core) *API { return &API{c: c} }

// Export writes a length-prefixed msgpack record stream of the graph. Forwards to Graph.ExportGraph.
func (a *API) Export(w io.Writer) error { return a.c.ExportGraph(w) }

// Import reads a length-prefixed msgpack record stream into the graph. Forwards to Graph.ImportGraph.
func (a *API) Import(r io.Reader) error { return a.c.ImportGraph(r) }
