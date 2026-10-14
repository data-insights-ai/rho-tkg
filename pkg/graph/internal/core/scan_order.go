package core

import storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

// ScanKeepsOrder reports whether a current-state ForEachByLabel scan of label walks
// members in ascending ID order from a list the store keeps, so it neither collects
// nor sorts: true on memory and on badger with its RAM label index; false on badger
// with LabelIndexOnDisk, tiered, sharded, external stores and temporal-filter scans.
// A planner charges a scan's sort only where this is false. Independent of whether
// the label exists. Errors: malformed label name, ErrGraphClosed.
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

// ScanKeepsOrder is NodeOps.ScanKeepsOrder for a ForEachByType scan of typeName:
// true on memory (not for a declared segment type) and badger; false elsewhere.
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

// scanOrderStater is the store's ScanOrderCapability when the graph streams scans
// directly (trusted native store); otherwise scans materialize and sort.
func (c *Core) scanOrderStater() (storepkg.ScanOrderCapability, bool) {
	if !c.storeRowsTrust {
		return nil, false
	}
	stater, ok := c.store.(storepkg.ScanOrderCapability)
	return stater, ok
}
