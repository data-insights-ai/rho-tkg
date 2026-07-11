package core

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// nodeDocValuesScanner is a CORE-internal interface (docvalues.go), not a
// store-contract capability, so there is no capabilities.go compile-assert
// site for it (ADR-0005 §3.4). This is the tiered-local compile check the
// brief calls for instead: it fails to compile (not merely fails a test) if
// *tiered.Store ever stops satisfying the four DocValues methods core's
// ForEachDocValues / ForEachDocValuesMulti / DocValuesSnapshot /
// NodeMutationEpoch type-assert against.
var _ nodeDocValuesScanner = (*tiered.Store)(nil)
