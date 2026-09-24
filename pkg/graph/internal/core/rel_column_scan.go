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

// ScanRelSegments is the columnar door over a declared bulk relationship type
// (ADR-0011 §5.3, S5): every current row of relType in batches of segment
// columns (see store.RelSegmentBatch) from one snapshot, in segment order.
// ok=false — fn is not called — when the backend has no segments, the type
// is not declared, or a requested property is not one of its declared
// columns; the caller then uses the row doors. An unknown type is ok with
// zero rows only if it is declared (declared types always exist).
func (c *Core) ScanRelSegments(relType string, props []string, fn func(*storepkg.RelSegmentBatch) bool) (ok bool, err error) {
	if c == nil {
		return false, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return false, ErrGraphClosed
	}
	token, known := c.relTypes.Lookup(relType)
	if !known {
		return false, nil
	}
	scanner, has := c.store.(storepkg.RelSegmentScanCapability)
	if !has {
		return false, nil
	}
	return scanner.ScanRelSegments(token, props, fn)
}
