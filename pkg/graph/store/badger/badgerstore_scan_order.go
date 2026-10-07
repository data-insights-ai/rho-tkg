package badger

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.ScanOrderCapability = (*Store)(nil)

// LabelScanKeepsOrder reports whether ForEachNodeByLabel walks a kept
// ascending member list: true with the RAM label index, false with
// LabelIndexOnDisk (the scan collects the label's keys and sorts them).
func (bs *Store) LabelScanKeepsOrder(uint16) bool {
	return bs != nil && !bs.labelOnDisk
}

// TypeScanKeepsOrder reports true: ForEachRelByType walks the type's kept
// ascending member list from the RAM type index.
func (bs *Store) TypeScanKeepsOrder(uint16) bool { return bs != nil }
