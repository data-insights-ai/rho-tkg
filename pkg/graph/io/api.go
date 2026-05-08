// Package io is a sub-API accessor for graph export/import.
package io

import "io"

// Ops is the subset of *core.IOOps the io sub-API forwards to.
type Ops interface {
	Export(w io.Writer) error
	Import(r io.Reader) error
}

// API is the io sub-API accessor.
type API struct{ ops Ops }

// New constructs an io sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

// Export writes a length-prefixed msgpack record stream of the graph.
func (a *API) Export(w io.Writer) error { return a.ops.Export(w) }

// Import reads a length-prefixed msgpack record stream into the graph.
func (a *API) Import(r io.Reader) error { return a.ops.Import(r) }
