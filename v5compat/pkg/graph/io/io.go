// Package io mirrors the v4 import path pkg/graph/io. Its doors are declined in
// v5compat's first batch (the v4 export/import format (the importer maps v4 stores into v5)): every entry point reports ErrNotSupportedInV5.
package io

import "github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"

// ErrNotSupportedInV5 is the class of every declined door of this package.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5
