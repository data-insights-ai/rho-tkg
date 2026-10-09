package state

import "github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"

type stateBuilder struct {
	limits Limits
	parts  []Piece
	usage  Usage
}

func (b *stateBuilder) add(scope temporal.Scope, cell Cell) error {
	scratch := controlPolicy(b.limits.Temporal)
	if len(b.parts) > 0 && b.parts[len(b.parts)-1].cell == cell {
		last := b.parts[len(b.parts)-1]
		join, err := mergeable(last.scope, scope, b.limits.Temporal)
		if err != nil {
			return err
		}
		if join {
			// Direct outer bounds avoid validating an oversized transient right-hand
			// fragment through Union before it merges into a fitting final atom.
			lo, _, _ := last.scope.Bounds()
			_, hi, _ := scope.Bounds()
			merged, err := temporal.Span(last.scope.Axis(), lo, hi, scratch)
			if err != nil {
				return err
			}
			size, err := measuredScope(merged, scratch)
			if err != nil {
				return err
			}
			candidate := b.usage
			candidate.metadataBytes += size - last.scopeBytes
			if err := b.checkWorking(candidate); err != nil {
				return err
			}
			b.usage = candidate
			b.parts[len(b.parts)-1] = Piece{scope: merged, cell: cell, scopeBytes: size}
			return nil
		}
	}
	// Once a different Cell is appended, the previous atom can never merge or
	// shrink again. Finalize it before appending any additional metadata.
	if err := b.finalizeLast(); err != nil {
		return err
	}
	size, err := measuredScope(scope, scratch)
	if err != nil {
		return err
	}
	candidate := b.usage
	candidate.pieces++
	candidate.metadataBytes += size + cellMetadataBytes
	if cell.references() > b.limits.MaxReferencedBytes-candidate.references {
		return ErrResourceLimit
	}
	candidate.references += cell.references()
	if err := b.checkWorking(candidate); err != nil {
		return err
	}
	b.parts = append(b.parts, Piece{scope: scope, cell: cell, scopeBytes: size})
	b.usage = candidate
	return nil
}
func (b *stateBuilder) finalizeLast() error {
	if len(b.parts) == 0 {
		return nil
	}
	_, err := measuredScope(b.parts[len(b.parts)-1].scope, b.limits.Temporal)
	return err
}
func (b *stateBuilder) checkWorking(candidate Usage) error {
	// Piece/reference counts are monotone. Only the last atomic representation
	// can shrink: allow at most one hard-bounded scratch atom of metadata slack.
	scratch := controlPolicy(b.limits.Temporal)
	if candidate.pieces > b.limits.MaxPieces || candidate.references > b.limits.MaxReferencedBytes || candidate.metadataBytes > b.limits.MaxMetadataBytes+scratch.MaxValueBytes {
		return ErrResourceLimit
	}
	return nil
}
func (b *stateBuilder) finish() error {
	if err := b.finalizeLast(); err != nil {
		return err
	}
	return b.usage.check(b.limits, false)
}

type changeBuilder struct {
	limits Limits
	parts  []Change
	usage  Usage
}

func (b *changeBuilder) add(scope temporal.Scope, before, after Cell) error {
	if before == after {
		return nil
	} // Exact replay has no changed support.
	size, err := measuredScope(scope, b.limits.Temporal)
	if err != nil {
		return err
	}
	// Normalized mutation parts and normalized old full-cell pieces already
	// partition the change stream into maximal identical-before/after runs.

	candidate := b.usage
	candidate.pieces++
	candidate.metadataBytes += size + 2*cellMetadataBytes
	for _, cell := range []Cell{before, after} {
		if cell.references() > b.limits.MaxReferencedBytes-candidate.references {
			return ErrResourceLimit
		}
		candidate.references += cell.references()
	}
	if err := candidate.check(b.limits, true); err != nil {
		return err
	}
	b.parts = append(b.parts, Change{scope: scope, before: before, after: after, scopeBytes: size})
	b.usage = candidate
	return nil
}
