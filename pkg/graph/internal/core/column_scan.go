package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ScanNodeColumns exposes the backend's typed column scan when it has one.
//
// ok=false means this backend does not implement NodeColumnScanCapability and the
// caller should use NodesByLabel — the capability is OPTIONAL, exactly like the
// scoped-replace and change-log ones asserted elsewhere in this package.
//
// A temporal opt (ValidAt, ValidStart+ValidEnd, TxAt or TxPin) is answered
// exactly like NodesByLabel answers it — each node's version under opts, history
// included — not from the store's current rows (scan_temporal_opts.go). It is
// validated like NodesByLabel (ErrConflictingTemporalOpts, compaction and
// retention watermarks).
func (c *Core) ScanNodeColumns(label string, props []string, opts storepkg.QueryOpts,
	fn func(*storepkg.ColumnBatch) bool) (ok bool, err error) {

	if c == nil {
		return false, nil
	}
	scanner, has := c.store.(storepkg.NodeColumnScanCapability)
	temporal := hasTemporalFilter(opts)
	if temporal && has {
		if err := c.validateTemporalQueryOptsScan(opts); err != nil {
			return true, err
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	// TAKES A LABEL NAME, not a token. Every consumer-facing query here names its
	// label; the token is an internal encoding, and handing it out would make a
	// caller reach for an interning API that is not public.
	token, known := c.labels.Lookup(label)
	if !known {
		return true, nil // known capability, no such label: zero rows
	}
	if !has {
		return false, nil
	}
	if temporal {
		return true, c.scanNodeColumnsTemporalLocked(label, props, opts, fn)
	}
	return true, scanner.ScanNodeColumns(token, props, opts, fn)
}
