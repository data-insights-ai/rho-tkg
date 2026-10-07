package memory

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.ScanOrderCapability = (*Store)(nil)

// LabelScanKeepsOrder reports true: ForEachNodeByLabel walks the label's kept
// ascending member list (storeutil.MemberOrder).
func (ms *Store) LabelScanKeepsOrder(uint16) bool { return ms != nil }

// TypeScanKeepsOrder reports true for every type but a declared segment type
// (ADR-0011), whose scan collects its rows' references.
func (ms *Store) TypeScanKeepsOrder(relTypeToken uint16) bool {
	if ms == nil {
		return false
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.segTypes[relTypeToken] == nil
}
