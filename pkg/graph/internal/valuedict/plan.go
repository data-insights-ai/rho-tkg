// Package valuedict holds the validated value-dictionary configuration (DICT)
// and its counters, shared by every badger store of one tiered store.
package valuedict

import (
	"fmt"
	"sync/atomic"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// MinLen is the shortest string value coded: a token costs up to 3 bytes
// more than its `v` key, so a shorter value saves nothing.
const MinLen = 4

type member struct{ owner, property string }

type counters struct{ coded, inlineFull, newEntries atomic.Uint64 }

// Plan is a validated []store.ValueDictionary with per-dictionary counters.
// Immutable after NewPlan except for the counters.
type Plan struct {
	names    []string
	caps     []int
	nodes    map[member]int
	rels     map[member]int
	nodeKeys map[string]struct{}
	relKeys  map[string]struct{}
	counts   []counters
}

// NewPlan validates cfg: names unique and non-empty, caps positive, each
// member names exactly one of Label and RelType and a property, and a pair
// belongs to one dictionary. nil cfg gives a nil plan (no coding).
func NewPlan(cfg []storepkg.ValueDictionary) (*Plan, error) {
	if len(cfg) == 0 {
		return nil, nil
	}
	p := &Plan{
		nodes:    map[member]int{},
		rels:     map[member]int{},
		nodeKeys: map[string]struct{}{},
		relKeys:  map[string]struct{}{},
		counts:   make([]counters, len(cfg)),
	}
	seen := map[string]bool{}
	for i, d := range cfg {
		if d.Name == "" || seen[d.Name] {
			return nil, fmt.Errorf("value dictionary %d: name %q empty or repeated", i, d.Name)
		}
		seen[d.Name] = true
		if d.Cap <= 0 {
			return nil, fmt.Errorf("value dictionary %q: cap %d, want > 0", d.Name, d.Cap)
		}
		if len(d.Members) == 0 {
			return nil, fmt.Errorf("value dictionary %q: no members", d.Name)
		}
		for _, m := range d.Members {
			if (m.Label == "") == (m.RelType == "") || m.Property == "" {
				return nil, fmt.Errorf("value dictionary %q: member %+v must name a property and exactly one of Label and RelType", d.Name, m)
			}
			into, keys, k := p.nodes, p.nodeKeys, member{m.Label, m.Property}
			if m.RelType != "" {
				into, keys, k = p.rels, p.relKeys, member{m.RelType, m.Property}
			}
			if _, dup := into[k]; dup {
				return nil, fmt.Errorf("value dictionary %q: member %+v already belongs to a dictionary", d.Name, m)
			}
			into[k] = i
			keys[m.Property] = struct{}{}
		}
		p.names = append(p.names, d.Name)
		p.caps = append(p.caps, d.Cap)
	}
	return p, nil
}

// Len is the number of dictionaries.
func (p *Plan) Len() int { return len(p.names) }

// Name is dictionary i's name.
func (p *Plan) Name(i int) string { return p.names[i] }

// Cap is dictionary i's per-store cap.
func (p *Plan) Cap(i int) int { return p.caps[i] }

// Index is the dictionary named name, false when the plan has none.
func (p *Plan) Index(name string) (int, bool) {
	for i, n := range p.names {
		if n == name {
			return i, true
		}
	}
	return 0, false
}

// NodeProperty reports whether property is a member for some label: the
// cheap test before resolving a node's label names.
func (p *Plan) NodeProperty(property string) bool { _, ok := p.nodeKeys[property]; return ok }

// RelProperty is NodeProperty for relationships.
func (p *Plan) RelProperty(property string) bool { _, ok := p.relKeys[property]; return ok }

// NodeMembers reports whether any member is a node property.
func (p *Plan) NodeMembers() bool { return len(p.nodes) > 0 }

// NodeDict is the dictionary coding property on nodes labelled label.
func (p *Plan) NodeDict(label, property string) (int, bool) {
	i, ok := p.nodes[member{label, property}]
	return i, ok
}

// RelDict is the dictionary coding property on relationships of type relType.
func (p *Plan) RelDict(relType, property string) (int, bool) {
	i, ok := p.rels[member{relType, property}]
	return i, ok
}

// NoteCoded counts a value written as a token.
func (p *Plan) NoteCoded(i int) { p.counts[i].coded.Add(1) }

// NoteInlineFull counts a value written inline because the dictionary was full.
func (p *Plan) NoteInlineFull(i int) { p.counts[i].inlineFull.Add(1) }

// NoteNewEntry counts a value added to a dictionary.
func (p *Plan) NoteNewEntry(i int) { p.counts[i].newEntries.Add(1) }

// Stats returns each dictionary's counters, in configuration order.
func (p *Plan) Stats() []storepkg.ValueDictionaryStats {
	if p == nil {
		return nil
	}
	out := make([]storepkg.ValueDictionaryStats, len(p.names))
	for i, n := range p.names {
		c := &p.counts[i]
		out[i] = storepkg.ValueDictionaryStats{Name: n, Coded: c.coded.Load(), InlineFull: c.inlineFull.Load(), NewEntries: c.newEntries.Load()}
	}
	return out
}
