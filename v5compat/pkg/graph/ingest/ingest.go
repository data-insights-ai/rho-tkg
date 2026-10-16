// Package ingest mirrors the v4 import path pkg/graph/ingest: ingest sessions. Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package ingest

import (
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// API is the sub-API handle returned by the graph accessor.
type API struct{}

// IngestOptions is a placeholder for the v4 type of the same name.
type IngestOptions struct{}

// Session is a placeholder for the v4 type of the same name.
type Session struct{}

// SubmitToken is a placeholder for the v4 type of the same name.
type SubmitToken struct{}

// NewSession is declined.
func (a *API) NewSession(opts IngestOptions) (r0 *Session, err error) {
	err = declined("ingest.NewSession")
	return
}

// AppliedSeq is declined.
func (a *API) AppliedSeq() (r0 uint64) {
	return
}

// WaitApplied is declined.
func (a *API) WaitApplied(token SubmitToken) error {
	return declined("ingest.WaitApplied")
}
