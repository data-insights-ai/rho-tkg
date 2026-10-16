// Package ingest mirrors the v4 import path pkg/graph/ingest. Its doors are declined in
// v5compat's first batch (ingest sessions (NewSession, WaitApplied, AppliedSeq)): every entry point reports ErrNotSupportedInV5.
package ingest

import "github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"

// ErrNotSupportedInV5 is the class of every declined door of this package.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5
