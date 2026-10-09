package txnproto

import (
	"cmp"
	"math"
	"slices"
)

const mathMaxRound = uint64(math.MaxUint64)

func sortTxs(ts []Tx) { slices.SortFunc(ts, func(a, b Tx) int { return cmp.Compare(a.ID, b.ID) }) }
func compatible(a, b View) bool {
	return a.Graph == b.Graph && a.Topology == b.Topology && a.Group <= 1 && b.Group <= 1 && a.Epoch != 0 && b.Epoch != 0 && a.Index > 0 && b.Index > 0
}

// ChooseRound performs Fresh's authoritative collection. Every prepared intent
// requires its coordinator decision, even when the coordinator is outside scope.
// Undecided/missing decisions remain pending/unavailable; no cut is synthesized.
// after is a caller's minimum round requirement, not evidence of a transaction.
func ChooseRound(collections, decisions []Proof, after uint64) (uint64, error) {
	if len(collections) < 1 || len(collections) > 2 {
		return 0, ErrInvalid
	}
	round := after
	seen := map[uint8]bool{}
	base := collections[0].view
	for _, p := range collections {
		v := p.view
		if v.Kind != Collection || !compatible(base, v) || seen[v.Group] {
			return 0, ErrInvalid
		}
		seen[v.Group] = true
		round = max(round, v.Floor)
		for _, t := range v.Pending {
			found := false
			for _, d := range decisions {
				w := d.view
				if w.Kind == Decided && compatible(v, w) && w.Tx != nil && w.Tx.ID == t.ID {
					if digest(*w.Tx) != digest(t) || w.Group != t.Coordinator || w.Decision == nil || !validDecision(t, *w.Decision) {
						return 0, ErrMismatch
					}
					p, ok := part(t, w.Group)
					if !ok || p.Epoch != w.Epoch {
						return 0, ErrStale
					}
					found = true
					round = max(round, w.Decision.Round)
				}
			}
			if !found {
				return 0, ErrUnavailable
			}
		}
	}
	if round == 0 {
		round = 1
	}
	return round, nil
}

// AssembleCut requires EXACT requested scope and identical closed rounds. The
// caller cannot silently enlarge a traversal's scope; request more certificates
// at that same round or restart. No max-log-index substitute is accepted.
func AssembleCut(scope []uint8, certificates []Proof) (Cut, error) {
	if len(scope) < 1 || len(scope) > 2 || len(certificates) != len(scope) {
		return Cut{}, ErrInvalid
	}
	scope = slices.Clone(scope)
	slices.Sort(scope)
	if scope[len(scope)-1] > 1 || len(scope) == 2 && scope[0] == scope[1] {
		return Cut{}, ErrInvalid
	}
	ps := slices.Clone(certificates)
	slices.SortFunc(ps, func(a, b Proof) int { return cmp.Compare(a.view.Group, b.view.Group) })
	base := ps[0].view
	for j, p := range ps {
		v := p.view
		if v.Kind != Certified || !compatible(base, v) || v.Group != scope[j] || v.Certificate == nil || v.Certificate.Index > v.Index || v.Certificate.Round == 0 || v.Certificate.Index == 0 || base.Certificate == nil || v.Certificate.Round != base.Certificate.Round {
			return Cut{}, ErrInvalid
		}
	}
	return Cut{ps}, nil
}
func snapshot(s state, round uint64) Snapshot {
	result := Snapshot{Values: map[string]Value{}}
	for k, h := range s.Rows {
		var selected Value
		for _, v := range h {
			if v.Round <= round {
				selected = v
			}
		}
		if selected.Version > 0 && !selected.Deleted {
			result.Values[k] = selected
		}
	}
	// Current generation is for write validation, not historical snapshot writes.
	if round == mathMaxRound {
		result.Generation = s.Generation
	}
	return result
}

// At reproduces retained local history, including an index/posting deleted or
// moved after the cut. Followers must apply through the certificate position.
// Its Snapshot cannot itself authorize writes from an old read footprint.
func (m *Machine) At(c Cut) (Snapshot, error) {
	if m == nil {
		return Snapshot{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range c.proofs {
		v := p.view
		if v.Group != m.config.Group {
			continue
		}
		if v.Graph != m.config.Graph || v.Topology != m.config.Topology || v.Epoch != m.config.Epochs[v.Group] {
			return Snapshot{}, ErrStale
		}
		if v.Certificate == nil || v.Certificate.Index > m.applied {
			return Snapshot{}, ErrUnavailable
		}
		cert, ok := m.state.Certificates[v.Certificate.Round]
		if !ok || cert != *v.Certificate {
			return Snapshot{}, ErrUnavailable
		}
		return snapshot(m.state, cert.Round), nil
	}
	return Snapshot{}, ErrUnavailable
}
