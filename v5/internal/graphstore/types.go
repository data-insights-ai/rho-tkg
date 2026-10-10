// Package graphstore supplies a provisional local root and bounded catalogs for
// the next graph materializer. It is not a graph writer, constraint validator,
// certified-cut constructor, distributed engine or frozen storage format.
package graphstore

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Catalog sentinels distinguish caller rejection, bounded work and stored corruption.
var (
	ErrInvalid       = errors.New("graphstore: invalid input")
	ErrNamespace     = errors.New("graphstore: graph or partition mismatch")
	ErrStaleOwner    = errors.New("graphstore: ownership epoch mismatch")
	ErrRebinding     = errors.New("graphstore: immutable identity rebinding")
	ErrResourceLimit = errors.New("graphstore: resource limit")
	ErrCorrupt       = errors.New("graphstore: corrupt catalog")
	ErrClosed        = errors.New("graphstore: closed stage")
	ErrPoisoned      = errors.New("graphstore: catalog stopped; recover required")
	// ErrTopologyUnsupported means the root has no single-partition declaration,
	// or its declared topology lacks the index-aware graph writer needed to stage.
	ErrTopologyUnsupported = errors.New("graphstore: topology capability unavailable")
)

// Namespace identifies storage routing. Partition is a logical partition, not
// a server or part of entity/value identity. Rebalance preserves qualified IDs.
type Namespace struct {
	Graph     graphstate.GraphID
	Partition uint64
}

func (n Namespace) validate() error {
	if n.Graph == (graphstate.GraphID{}) || n.Partition == 0 {
		return ErrNamespace
	}
	return nil
}

// EntityRef is stable graph-qualified identity without physical ownership.
type EntityRef struct {
	Graph graphstate.GraphID
	ID    graphstate.EntityID
}

// LifeRef qualifies a life by its stable owner, never by current placement.
type LifeRef struct {
	Graph graphstate.GraphID
	Owner graphstate.EntityID
	ID    graphstate.LifeID
}

// ValueRef qualifies a supplied globally unique value ID. Equal values in other
// partitions may have other IDs; semantic equality never compares IDs alone.
type ValueRef struct {
	Graph graphstate.GraphID
	ID    graphstate.ValueID
}

// ValueEntry owns exact immutable value content and its derived semantic
// payload/axis-definition ledger. PayloadBytes excludes backend framing.
type ValueEntry struct {
	Ref          ValueRef
	Value        graphstate.Scalar
	PayloadBytes uint64
	ordinal      uint64
}

// Root is constant-sized partition metadata. Semantic effect digest/epoch are
// separate from the physical page counter. This prototype explicitly has local
// single-voter integration; a root is not a distributed certificate or lease.
type Root struct {
	namespace          Namespace
	owner, epoch, next uint64
	effect             [32]byte
	topology           topologyDeclaration
}

// NewRoot creates an empty local-root descriptor without allocating logical IDs.
func NewRoot(n Namespace, ownershipEpoch uint64) (Root, error) {
	if err := n.validate(); err != nil {
		return Root{}, err
	}
	if ownershipEpoch == 0 {
		return Root{}, ErrInvalid
	}
	seed := append([]byte("rho-tkg:empty-partition:v1\x00"), n.Graph[:]...)
	seed = binary.BigEndian.AppendUint64(seed, n.Partition)
	return Root{namespace: n, owner: ownershipEpoch, next: 1, effect: sha256.Sum256(seed)}, nil
}
func (r Root) validate() error {
	if err := r.namespace.validate(); err != nil {
		return err
	}
	if r.owner == 0 || r.next == 0 || r.effect == ([32]byte{}) {
		return ErrInvalid
	}
	if r.topology != (topologyDeclaration{}) && r.topology != bootstrapTopology && r.topology != keysOnlyTopology && !isFullTopology(r.topology) {
		return ErrInvalid
	}
	return nil
}

// Namespace returns routing metadata, not identity placement bits.
func (r Root) Namespace() Namespace { return r.namespace }

// OwnershipEpoch returns the caller-supplied fenced ownership identity.
func (r Root) OwnershipEpoch() uint64 { return r.owner }

// SemanticEpoch returns the logical data generation, not a clock or cut.
func (r Root) SemanticEpoch() uint64 { return r.epoch }

// EffectDigest returns the supplied semantic-effect digest.
func (r Root) EffectDigest() [32]byte { return r.effect }

// NextPhysicalID returns a private storage allocation floor, not a graph ID.
func (r Root) NextPhysicalID() uint64 { return r.next }

// AdvanceEffects advances logical metadata with a materializer-supplied digest;
// it does not validate effects, commit them or manufacture a transaction receipt.
func (r Root) AdvanceEffects(digest [32]byte) (Root, error) {
	if err := r.validate(); err != nil {
		return Root{}, err
	}
	if digest == ([32]byte{}) {
		return Root{}, ErrInvalid
	}
	if r.epoch == math.MaxUint64 {
		return Root{}, ErrResourceLimit
	}
	r.epoch++
	r.effect = digest
	return r, nil
}

// ReservePhysical reserves only page handles in a private staged root. The
// caller must publish the returned root atomically with durable effects.
func (r Root) ReservePhysical(count uint64) (Root, uint64, error) {
	if err := r.validate(); err != nil {
		return Root{}, 0, err
	}
	if count == 0 {
		return Root{}, 0, ErrInvalid
	}
	if count > math.MaxUint64-r.next {
		return Root{}, 0, ErrResourceLimit
	}
	first := r.next
	r.next += count
	return r, first, nil
}

// Limits bounds one read operation and all retained staging in one Catalog.
// MaxReadRows charges misses and collision candidates; MaxReadBytes charges
// copied record/key capacities and returned scalar/descriptor materialization.
// Staging is a touched-record map only. Returned owned copies have their own
// matching output cap; these representation ledgers are not hard Go heap/RSS.
type Limits struct {
	Temporal                                                   temporal.Limits
	MaxNameBytes, MaxValueBytes, MaxRecordBytes                int
	MaxReadRows, MaxReadBytes                                  int
	MaxStages, MaxStageRecords, MaxStageBytes, MaxBucketValues int
}

// DefaultLimits supplies finite prototype policy, not production capacity.
func DefaultLimits() Limits {
	return Limits{temporal.DefaultLimits(), 256, 128 << 10, 256 << 10, 512, 4 << 20, 4, 1024, 4 << 20, 1024}
}

// Validate checks concrete resolved record/read/staging bounds.
func (l Limits) Validate() error { _, err := l.resolve(); return err }
func (l Limits) resolve() (Limits, error) {
	d := DefaultLimits()
	if err := l.Temporal.Validate(); err != nil {
		return Limits{}, err
	}
	td := temporal.DefaultLimits()
	l.Temporal.MaxInputBytes = cmp.Or(l.Temporal.MaxInputBytes, td.MaxInputBytes)
	l.Temporal.MaxValueBytes = cmp.Or(l.Temporal.MaxValueBytes, td.MaxValueBytes)
	l.Temporal.MaxDescriptorBytes = cmp.Or(l.Temporal.MaxDescriptorBytes, td.MaxDescriptorBytes)
	l.Temporal.MaxMagnitudeBits = cmp.Or(l.Temporal.MaxMagnitudeBits, td.MaxMagnitudeBits)
	l.Temporal.MaxRegionPieces = cmp.Or(l.Temporal.MaxRegionPieces, td.MaxRegionPieces)
	l.MaxNameBytes = cmp.Or(l.MaxNameBytes, d.MaxNameBytes)
	l.MaxValueBytes = cmp.Or(l.MaxValueBytes, d.MaxValueBytes)
	l.MaxRecordBytes = cmp.Or(l.MaxRecordBytes, d.MaxRecordBytes)
	l.MaxReadRows = cmp.Or(l.MaxReadRows, d.MaxReadRows)
	l.MaxReadBytes = cmp.Or(l.MaxReadBytes, d.MaxReadBytes)
	l.MaxStages = cmp.Or(l.MaxStages, d.MaxStages)
	l.MaxStageRecords = cmp.Or(l.MaxStageRecords, d.MaxStageRecords)
	l.MaxStageBytes = cmp.Or(l.MaxStageBytes, d.MaxStageBytes)
	l.MaxBucketValues = cmp.Or(l.MaxBucketValues, d.MaxBucketValues)
	for _, n := range []int{l.MaxNameBytes, l.MaxValueBytes, l.MaxRecordBytes, l.MaxReadRows, l.MaxReadBytes, l.MaxStages, l.MaxStageRecords, l.MaxStageBytes, l.MaxBucketValues} {
		if n < 1 || n > 64<<20 {
			return Limits{}, ErrInvalid
		}
	}
	if l.MaxNameBytes > 65535 || l.MaxRecordBytes > 1<<20 || l.MaxReadRows > 65536 || l.MaxStages > 64 || l.MaxStageRecords > 4096 || l.MaxBucketValues > 65536 || l.MaxRecordBytes < l.MaxValueBytes+l.Temporal.MaxDescriptorBytes+128 || l.MaxReadBytes < l.MaxRecordBytes+128 || l.MaxStageBytes < l.MaxRecordBytes+256 {
		return Limits{}, ErrInvalid
	}
	return l, nil
}
func (l Limits) valueLimits() graphstate.Limits {
	return graphstate.Limits{MaxNameBytes: l.MaxNameBytes, MaxReadBytes: l.MaxValueBytes, Component: state.Limits{Temporal: l.Temporal}}
}
