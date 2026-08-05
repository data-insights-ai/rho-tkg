package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ScanRelColumns exposes the backend's typed relationship column scan when it has
// one, the sibling of ScanNodeColumns.
//
// ok=false means this backend does not implement RelColumnScanCapability and the
// caller should use RelsByType — the capability is OPTIONAL, like every other one
// asserted in this package.
func (c *Core) ScanRelColumns(relType string, props []string, opts storepkg.QueryOpts,
	fn func(*storepkg.RelColumnBatch) bool) (ok bool, err error) {

	if c == nil {
		return false, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	// TAKES A TYPE NAME, not a token, for the reason the node door documents: every
	// consumer-facing query here names its relationship type, and handing out the
	// token would make a caller reach for an interning API that is not public.
	token, known := c.relTypes.Lookup(relType)
	if !known {
		return true, nil // known capability, no such type: zero rows
	}
	scanner, has := c.store.(storepkg.RelColumnScanCapability)
	if !has {
		return false, nil
	}
	return true, scanner.ScanRelColumns(token, props, opts, fn)
}
