// Package constraints mirrors the v4 import path pkg/graph/constraints. Its doors are declined in
// v5compat's first batch (unique constraints DDL (CreateUnique, DropUnique, DryRunValidate); first batch only has GetOrCreateByKey): every entry point reports ErrNotSupportedInV5.
package constraints

import "github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"

// ErrNotSupportedInV5 is the class of every declined door of this package.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5
