package main

import (
	"context"
	"strings"
)

// This fixed scalar door selects actual declared out/in index postings at an
// authenticated retained cut. It is a batched SOURCE SCAN, not a value index or
// indexed latency claim. Endpoint effectiveness is not silently inferred: the
// declared posting and its exact original revision remain the returned view.
type graphIndexPredicate struct {
	Kind   string
	Equals int64
}
type graphIndexMatch struct {
	Selector uint8
	Row      graphSnapshotRow
}
type graphIndexEnd struct {
	Selectors uint8
	Matches   [8][2]uint64
	Source    graphSnapshotEnd
}

func graphIndexPredicates(input []graphIndexPredicate) ([]graphIndexPredicate, error) {
	if len(input) < 1 || len(input) > 8 {
		return nil, errInvalid
	}
	owned := make([]graphIndexPredicate, len(input))
	for i, p := range input {
		if p.Kind != "out" && p.Kind != "in" {
			return nil, errInvalid
		}
		for j := 0; j < i; j++ {
			if input[j] == p {
				return nil, errInvalid
			}
		}
		owned[i] = graphIndexPredicate{strings.Clone(p.Kind), p.Equals}
	}
	return owned, nil
}

// IndexScan streams at most one bounded source page at a time. All matching
// rows are provisional until the independent complete original census returns
// EOF. Consumer failure/cancellation/missing native authority returns ZERO end;
// there is no in-RAM matching-ID set or successful prefix completion. One source
// walk serves all <=8 predicates; work/owner/cleanup are the SAME snapshot's.
func (s *graphSnapshot) IndexScan(ctx context.Context, input []graphIndexPredicate, emit func(graphIndexMatch) error) (graphIndexEnd, error) {
	var end graphIndexEnd
	if s == nil || s.owner == nil || ctx == nil || emit == nil {
		return end, errInvalid
	}
	predicates, err := graphIndexPredicates(input)
	if err != nil {
		return end, err
	}
	end.Selectors = uint8(len(predicates)) // #nosec G115 -- admitted <=8.
	end.Source, err = s.Scan(ctx, func(row graphSnapshotRow) error {
		kind, _, found := strings.Cut(row.Key, "/")
		if !found || kind != "out" && kind != "in" {
			return nil
		}
		edge, err := parseGraphEdge(kind, row.Key, row.Value.Value, s.owner.s.h.config)
		if err != nil {
			return err
		}
		group := uint8(0)
		if kind == "in" {
			group = edge.Target.Owner
		}
		if row.Group != group || row.Value.Deleted {
			return errMismatch
		}
		for i, p := range predicates {
			if p.Kind == kind && p.Equals == row.Value.Value {
				selector := uint8(i) // #nosec G115 -- admitted <=8.
				if err := emit(graphIndexMatch{selector, row}); err != nil {
					return err
				}
				end.Matches[i][group]++
			}
		}
		return nil
	})
	if err != nil {
		return graphIndexEnd{}, err
	}
	return end, nil
}
