package index

import (
	"cmp"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

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
	collect := newMemberCollector(m)
	addValue := collect.add
	if !(lo <= hi) { // empty range (or a NaN bound): the NaN values only
		for _, vk := range m.numeric.nans {
			addValue(vk)
		}
		return collect.out, true
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
	return collect.out, true
}

// MembersAll returns, caller-owned, the members of every value, each once
// with the merged bound of the values it holds (round 9: value statistics
// at a pin). The owner calls it under at least its shared lock.
func (m *PropertyTxMembers[ID]) MembersAll() []TxMember[ID] {
	if m == nil {
		return nil
	}
	collect := newMemberCollector(m)
	for vk := range m.values {
		collect.add(vk)
	}
	return collect.out
}

// memberCollector gathers the members of several values, each member once
// with its merged bound (MergeFirstTx).
type memberCollector[ID cmp.Ordered] struct {
	m   *PropertyTxMembers[ID]
	pos map[ID]int
	out []TxMember[ID]
}

func newMemberCollector[ID cmp.Ordered](m *PropertyTxMembers[ID]) *memberCollector[ID] {
	return &memberCollector[ID]{m: m, pos: make(map[ID]int)}
}

func (c *memberCollector[ID]) add(vk string) {
	p, has := c.m.values[vk]
	if !has {
		return
	}
	one := func(id ID, tx types.Instant) {
		if i, seen := c.pos[id]; seen {
			c.out[i].FirstTx = MergeFirstTx(c.out[i].FirstTx, tx)
			return
		}
		c.pos[id] = len(c.out)
		c.out = append(c.out, TxMember[ID]{ID: id, FirstTx: tx})
	}
	one(p.id, p.p.tx)
	for id, q := range p.more {
		one(id, q.tx)
	}
}
