package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ScanNodeColumns exposes the backend's typed column scan when it has one.
//
// ok=false means this backend does not implement NodeColumnScanCapability and the
// caller should use NodesByLabel — the capability is OPTIONAL, exactly like the
// scoped-replace and change-log ones asserted elsewhere in this package.
func (c *Core) ScanNodeColumns(token uint16, props []string, opts storepkg.QueryOpts,
	fn func(*storepkg.ColumnBatch) bool) (ok bool, err error) {

	if c == nil {
		return false, nil
	}
	scanner, has := c.store.(storepkg.NodeColumnScanCapability)
	if !has {
		return false, nil
	}
	return true, scanner.ScanNodeColumns(token, props, opts, fn)
}
