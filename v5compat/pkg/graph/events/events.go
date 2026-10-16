// Package events mirrors the v4 import path pkg/graph/events. Its doors are declined in
// v5compat's first batch (the change feed and watchers): every entry point reports ErrNotSupportedInV5.
package events

import "github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"

// ErrNotSupportedInV5 is the class of every declined door of this package.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5
