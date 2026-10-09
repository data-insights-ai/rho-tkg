package state

import (
	"cmp"
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

var (
	// ErrInvalidLimits identifies a reducer policy outside its supported range.
	ErrInvalidLimits = errors.New("state: invalid limits")
	// ErrResourceLimit identifies a state or change output exceeding its policy.
	ErrResourceLimit = errors.New("state: resource limit exceeded")
	// ErrInvalidState identifies an uninitialized component state.
	ErrInvalidState = errors.New("state: invalid component state")
	// ErrInvalidValueRef identifies an absent payload identity or malformed null.
	ErrInvalidValueRef = errors.New("state: invalid value reference")
	// ErrInvalidRevision identifies an absent supplied revision identity.
	ErrInvalidRevision = errors.New("state: invalid revision")
)

const hardPieces = 65536
const hardMetadataBytes = 64 << 20
const hardReferenceBytes = 1 << 30

// Limits bounds one component and its change output independently. Zero fields
// use defaults. The embedding engine additionally budgets total operations and
// results; these bounds are not a database-wide or Go heap memory guarantee.
type Limits struct {
	Temporal               temporal.Limits
	MaxPieces              int
	MaxMetadataBytes       int
	MaxReferencedBytes     uint64
	MaxChangePieces        int
	MaxChangeMetadataBytes int
}

// DefaultLimits returns bounded initial reducer policy.
func DefaultLimits() Limits {
	return Limits{Temporal: temporal.DefaultLimits(), MaxPieces: 4096, MaxMetadataBytes: 1 << 20, MaxReferencedBytes: 16 << 20, MaxChangePieces: 4096, MaxChangeMetadataBytes: 1 << 20}
}

// Validate checks all caller policies, including positive hard ceilings.
func (l Limits) Validate() error { _, err := l.resolved(); return err }
func (l Limits) resolved() (Limits, error) {
	if err := l.Temporal.Validate(); err != nil {
		return Limits{}, err
	}
	d := DefaultLimits()
	l.MaxPieces = cmp.Or(l.MaxPieces, d.MaxPieces)
	l.MaxMetadataBytes = cmp.Or(l.MaxMetadataBytes, d.MaxMetadataBytes)
	l.MaxReferencedBytes = cmp.Or(l.MaxReferencedBytes, d.MaxReferencedBytes)
	l.MaxChangePieces = cmp.Or(l.MaxChangePieces, d.MaxChangePieces)
	l.MaxChangeMetadataBytes = cmp.Or(l.MaxChangeMetadataBytes, d.MaxChangeMetadataBytes)
	for _, v := range []struct{ n, max int }{{l.MaxPieces, hardPieces}, {l.MaxChangePieces, hardPieces}, {l.MaxMetadataBytes, hardMetadataBytes}, {l.MaxChangeMetadataBytes, hardMetadataBytes}} {
		if v.n < 1 || v.n > v.max {
			return Limits{}, ErrInvalidLimits
		}
	}
	if l.MaxReferencedBytes > hardReferenceBytes {
		return Limits{}, ErrInvalidLimits
	}
	td := temporal.DefaultLimits()
	l.Temporal.MaxInputBytes = cmp.Or(l.Temporal.MaxInputBytes, td.MaxInputBytes)
	l.Temporal.MaxValueBytes = cmp.Or(l.Temporal.MaxValueBytes, td.MaxValueBytes)
	l.Temporal.MaxMagnitudeBits = cmp.Or(l.Temporal.MaxMagnitudeBits, td.MaxMagnitudeBits)
	l.Temporal.MaxRegionPieces = cmp.Or(l.Temporal.MaxRegionPieces, td.MaxRegionPieces)
	l.Temporal.MaxDescriptorBytes = cmp.Or(l.Temporal.MaxDescriptorBytes, td.MaxDescriptorBytes)
	return l, nil
}

// Usage is a conservative metadata/reference ledger, not a heap measurement.
// Metadata counts a fixed envelope, the full shared axis definition once per
// independently owned State/change ledger, each canonical materialized scope,
// and fixed cell fields. These are representation bytes, not actual Go heap.
// Declared payload bytes are charged per cell occurrence, without dedup.
type Usage struct {
	pieces, metadataBytes int
	references            uint64
}

// Pieces returns the number of materialized pieces or changes.
func (u Usage) Pieces() int { return u.pieces }

// MetadataBytes returns the canonical metadata ledger, excluding payload bytes.
func (u Usage) MetadataBytes() int { return u.metadataBytes }

// DeclaredReferenceBytes returns conservative caller-supplied payload sizes.
func (u Usage) DeclaredReferenceBytes() uint64 { return u.references }
func (u Usage) check(l Limits, changes bool) error {
	pieces, bytes := l.MaxPieces, l.MaxMetadataBytes
	if changes {
		pieces, bytes = l.MaxChangePieces, l.MaxChangeMetadataBytes
	}
	if u.pieces > pieces || u.metadataBytes > bytes || u.references > l.MaxReferencedBytes {
		return fmt.Errorf("%w: component metadata or declared references", ErrResourceLimit)
	}
	return nil
}
func temporalPolicyCovers(requested, validated temporal.Limits) bool {
	return requested.MaxInputBytes >= validated.MaxInputBytes && requested.MaxValueBytes >= validated.MaxValueBytes && requested.MaxMagnitudeBits >= validated.MaxMagnitudeBits && requested.MaxRegionPieces >= validated.MaxRegionPieces && requested.MaxDescriptorBytes >= validated.MaxDescriptorBytes
}
