package core

import storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

// ScanKeepsOrder reports whether a current-state ForEachByLabel scan of label
// walks the members in ascending ID order from a list the store keeps between
// scans, so a scan neither collects nor sorts the label's IDs: true on memory
// and on badger with its RAM label index. False on badger with
// LabelIndexOnDisk, on tiered, sharded and external stores (whose scans
// collect, and sort unless NoSort is set) and for a scan with a temporal filter
// (which takes ByLabel's materializing path). A query planner charges a scan's
// sort only where this is false. The answer does not depend on whether the
// label exists. Errors: a malformed label name, ErrGraphClosed.
func (n *NodeOps) ScanKeepsOrder(label string) (bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return false, err
	}
	stater, ok := c.scanOrderStater()
	if !ok {
		return false, nil
	}
	if _, native := c.store.(nodeLabelScanner); !native {
		return false, nil
	}
	var tok uint16
	if err := c.readUnderRLock(func() error {
		tok, _ = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return false, err
	}
	return stater.LabelScanKeepsOrder(tok), nil
}

// ScanKeepsOrder is NodeOps.ScanKeepsOrder for a ForEachByType scan of
// typeName: true on memory (but for a declared segment type, whose scan
// collects its rows) and badger, false on tiered, sharded and external stores.
func (r *RelOps) ScanKeepsOrder(typeName string) (bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return false, err
	}
	stater, ok := c.scanOrderStater()
	if !ok {
		return false, nil
	}
	if _, native := c.store.(relTypeScanner); !native {
		return false, nil
	}
	var tok uint16
	if err := c.readUnderRLock(func() error {
		tok, _ = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return false, err
	}
	return stater.TypeScanKeepsOrder(tok), nil
}

// scanOrderStater is the store's ScanOrderCapability when the graph streams
// its scans directly (a trusted native store); otherwise the graph's scans
// materialize and sort, whatever the store says.
func (c *Core) scanOrderStater() (storepkg.ScanOrderCapability, bool) {
	if !c.storeRowsTrust {
		return nil, false
	}
	stater, ok := c.store.(storepkg.ScanOrderCapability)
	return stater, ok
}
