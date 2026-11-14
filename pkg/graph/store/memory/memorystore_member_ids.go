package memory

import (
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.MemberIDsCapability = (*Store)(nil)
var _ storecontract.CurrentMemberIDsMeteredCapability = (*Store)(nil)

// NodeIDsByLabel returns an ascending snapshot without loading entity rows.
func (ms *Store) NodeIDsByLabel(tok uint16) ([]types.NodeID, error) {
	return ms.NodeIDsByLabelMetered(tok, nil)
}

// NodeIDsByLabelMetered admits the actual set under the snapshot lock.
func (ms *Store) NodeIDsByLabelMetered(tok uint16, t *storecontract.ReadTally) ([]types.NodeID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(tok); err != nil {
		return nil, err
	}
	set := ms.labelIdx[tok]
	if err := t.Candidates(len(set)); err != nil {
		return nil, err
	}
	if err := t.Buffer(8 * len(set)); err != nil {
		return nil, err
	}
	ids := make([]types.NodeID, 0, len(set))
	for id := range set {
		noteCurrentMemberCopied()
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, nil
}

// RelIDsByType includes current sealed rows, without decoding relationship rows.
func (ms *Store) RelIDsByType(tok uint16) ([]types.RelID, error) {
	return ms.RelIDsByTypeMetered(tok, nil)
}

// RelIDsByTypeMetered charges native memtable and segment entries before copying.
func (ms *Store) RelIDsByTypeMetered(tok uint16, t *storecontract.ReadTally) ([]types.RelID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(tok); err != nil {
		return nil, err
	}
	set := ms.typeIdx[tok]
	if err := t.Candidates(len(set)); err != nil {
		return nil, err
	}
	if err := t.Buffer(8 * len(set)); err != nil {
		return nil, err
	}
	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		noteCurrentMemberCopied()
		ids = append(ids, id)
	}
	if st := ms.segTypes[tok]; st != nil {
		for _, sg := range st.segs {
			for i := 0; i < sg.seg.Len(); i++ {
				if err := t.Candidates(1); err != nil {
					return nil, err
				}
				id, err := sg.seg.IDAt(i)
				if err != nil {
					return nil, err
				}
				if _, dead := ms.segDead[id]; dead {
					continue
				}
				if err := t.Buffer(8); err != nil {
					return nil, err
				}
				noteCurrentMemberCopied()
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	return ids, nil
}
