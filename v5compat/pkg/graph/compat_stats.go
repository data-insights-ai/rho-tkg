package graph

import (
	"context"
	"errors"
	"fmt"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// statSource serves Stats() counts by the rule of the list doors (ByLabel,
// ByType): the current entities, valid now, not deleted or retracted. So a count
// equals the length of the matching lookup.
type statSource struct{ g *Graph }

func (s statSource) current(sq scanQ) ([]row, error) {
	g := s.g
	if err := g.alive(); err != nil {
		return nil, err
	}
	now := g.nowValid()
	rows, err := g.scanRows(context.Background(), g.ax.known(), v5.ViewDeclared, span{now, now + 1}, sq, nil)
	if err != nil {
		return nil, err
	}
	var out []row
	for i, r := range rows {
		if i == 0 || r.id != rows[i-1].id {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s statSource) count(sq scanQ) (int, error) {
	rows, err := s.current(sq)
	return len(rows), err
}

func (s statSource) NodeCount() (int, error) { return s.count(scanQ{}) }
func (s statSource) RelCount() (int, error)  { return s.count(scanQ{rel: true}) }

func (s statSource) NodeCountByLabel(label string) (int, error) {
	if label == "" {
		return 0, ErrEmptyName
	}
	return s.count(scanQ{labels: []string{label}})
}

func (s statSource) RelCountByType(typeName string) (int, error) {
	if typeName == "" {
		return 0, ErrEmptyName
	}
	return s.count(scanQ{relType: typeName, rel: true})
}

func (s statSource) NodeCountByLabelAndPropertyKey(label, key string) (int, error) {
	if label == "" || key == "" {
		return 0, ErrEmptyName
	}
	rows, err := s.current(scanQ{labels: []string{label}})
	n := 0
	for _, r := range rows {
		if _, ok := userMap(r.vals)[key]; ok {
			n++
		}
	}
	return n, err
}

func (s statSource) AllLabelCounts() (map[string]int, error) {
	rows, err := s.current(scanQ{})
	out := map[string]int{}
	for _, r := range rows {
		for _, l := range r.labels {
			out[l]++
		}
	}
	return out, err
}

func (s statSource) AllRelTypeCounts() (map[string]int, error) {
	rows, err := s.current(scanQ{rel: true})
	out := map[string]int{}
	for _, r := range rows {
		out[r.relType]++
	}
	return out, err
}

// uniqueSource installs unique constraints over Graph.DeclareUnique.
type uniqueSource struct{ g *Graph }

// uniqueKinds are the kinds a compat property can be unique in: the shadow
// kinds i, s and b (floats, bytes and lists are no claim, as in v4).
var uniqueKinds = []v5.ValueKind{v5.KindInt, v5.KindString, v5.KindBool}

// CreateUnique declares every unique-capable shadow of the key. The claim is per
// key over all nodes (v5 has no label scope); a duplicate in existing data
// refuses it, and a kind declared earlier is skipped (a partial earlier failure
// is completed by a retry).
func (u uniqueSource) CreateUnique(ctx context.Context, label, key string) error {
	g := u.g
	if err := g.alive(); err != nil {
		return err
	}
	if ctx == nil {
		return ErrNilContext
	}
	if label == "" {
		return ErrEmptyName
	}
	if err := checkKey(key); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.alive(); err != nil {
		return err
	}
	declared, existed := 0, 0
	for _, k := range uniqueKinds {
		rcpt, err := g.g.DeclareUnique(ctx, key+"~"+kindTag(k), k)
		switch {
		case err == nil:
			g.clk.note(rcpt)
			declared++
		case errors.Is(err, v5.ErrUniqueDeclared):
			existed++
		case errors.Is(err, v5.ErrUniqueDuplicates):
			return fmt.Errorf("%w: %s.%s (%w)", store.ErrUniqueViolationExisting, label, key, err)
		default:
			return mapV5(err)
		}
	}
	if declared == 0 && existed > 0 {
		return fmt.Errorf("%w: %s.%s", store.ErrUniqueConstraintExists, label, key)
	}
	return nil
}
