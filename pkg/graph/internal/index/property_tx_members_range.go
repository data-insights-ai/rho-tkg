package index

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// numericValueView maps a numeric sort key (numericSortKey) to the value keys
// sharing it (int64 3 and float64 3.0 are two value keys, one sort key), plus
// the NaN value keys apart (they have no sort key).
type numericValueView struct {
	keys sortedChunks[float64]
	vks  map[float64][]string
	nans []string
}

func (v *numericValueView) add(vk string) {
	if vk == "f64:nan" || vk == "f32:nan" {
		v.nans = append(v.nans, vk)
		return
	}
	k, ok := numericSortKey(vk)
	if !ok {
		return
	}
	if _, has := v.vks[k]; !has {
		v.keys.insert(k)
	}
	v.vks[k] = append(v.vks[k], vk)
}

// OrderNumeric opens the numeric view over the values recorded so far (a
// no-op when open); Record keeps it from then on. Cost only for a sidecar a
// range read asked for: one sort key and one value-key reference per distinct
// numeric value. The owner calls it under its exclusive lock.
func (m *PropertyTxMembers[ID]) OrderNumeric() {
	if m == nil || m.numeric != nil {
		return
	}
	v := &numericValueView{vks: make(map[float64][]string)}
	for vk := range m.values {
		v.add(vk)
	}
	m.numeric = v
}

// MembersInRange returns, caller-owned, the members of every value whose
// numeric sort key lies in [lo, hi] (inclusive) and of every NaN value, each
// member once with the merged bound (MergeFirstTx) of the values it holds
// there. ok is false when the numeric view is not open (the owner opens it
// with OrderNumeric, then asks again). The owner calls it under at least its
// shared lock.
func (m *PropertyTxMembers[ID]) MembersInRange(lo, hi float64) (out []TxMember[ID], ok bool) {
	if m == nil {
		return nil, true
	}
	if m.numeric == nil {
		return nil, false
	}
	pos := make(map[ID]int)
	addValue := func(vk string) {
		p, has := m.values[vk]
		if !has {
			return
		}
		add := func(id ID, tx types.Instant) {
			if i, seen := pos[id]; seen {
				out[i].FirstTx = MergeFirstTx(out[i].FirstTx, tx)
				return
			}
			pos[id] = len(out)
			out = append(out, TxMember[ID]{ID: id, FirstTx: tx})
		}
		add(p.id, p.tx)
		for id, tx := range p.more {
			add(id, tx)
		}
	}
	if !(lo <= hi) { // empty range (or a NaN bound): the NaN values only
		for _, vk := range m.numeric.nans {
			addValue(vk)
		}
		return out, true
	}
	m.numeric.keys.forEachFrom(lo, func(k float64) bool {
		if k > hi {
			return false
		}
		for _, vk := range m.numeric.vks[k] {
			addValue(vk)
		}
		return true
	})
	for _, vk := range m.numeric.nans {
		addValue(vk)
	}
	return out, true
}
