package graphstate

import (
	"cmp"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// OutputBudget is one operation's consumptive retained-representation and
// bounded-scratch admission ledger. It has single-goroutine ownership, no reset
// and no refunds: potentially shared originals/copies remain reserved through
// call completion. Used is neither per-result output size nor peak Go heap/RSS;
// runtime/allocator/pool/goroutine-stack bookkeeping is outside this contract.
type OutputBudget struct {
	limit, used  int
	parent       *OutputBudget
	scopeScratch []byte
}

// NewOutputBudget accepts an explicitly exhausted zero allowance; zero never
// selects defaults. Limits and source/page work remain independent ledgers.
func NewOutputBudget(bytes int) (*OutputBudget, error) {
	if bytes < 0 || bytes > 64<<20 {
		return nil, ErrInvalidInput
	}
	return &OutputBudget{limit: bytes}, nil
}

// Reserve admits owned slots/backing before allocation. Failed reservations
// preserve prior consumption; successfully admitted attempts are never refunded.
func (b *OutputBudget) Reserve(bytes int) error {
	if b == nil || bytes < 0 {
		return ErrInvalidInput
	}
	for current := b; current != nil; current = current.parent {
		if bytes > current.limit-current.used {
			return ErrResourceLimit
		}
	}
	for current := b; current != nil; current = current.parent {
		current.used += bytes
	}
	return nil
}

// Remaining returns the unconsumed allowance; nil is explicitly exhausted.
func (b *OutputBudget) Remaining() int {
	if b == nil {
		return 0
	}
	remaining := b.limit - b.used
	for current := b.parent; current != nil; current = current.parent {
		remaining = min(remaining, current.limit-current.used)
	}
	return remaining
}

// Used reports the complete consumptive representation reservation.
func (b *OutputBudget) Used() int {
	if b == nil {
		return 0
	}
	return b.used
}

// ScopeBytes borrows one exact-capacity wire scratch until its next call. Numeric
// scratch is admitted on every attempt even when destination capacity is reused.
// EncodingBounds is inspection, not canonical admission; AppendScope still decides.
func (b *OutputBudget) ScopeBytes(s temporal.Scope, l temporal.Limits) ([]byte, error) {
	if b == nil {
		return nil, ErrInvalidInput
	}
	bounds, err := s.EncodingBounds(l)
	if err != nil {
		return nil, err
	}
	if err := b.Reserve(bounds.NumericBytes); err != nil {
		return nil, err
	}
	if cap(b.scopeScratch) < bounds.WireBytes {
		if err := b.Reserve(bounds.WireBytes); err != nil {
			return nil, err
		}
		b.scopeScratch = make([]byte, 0, bounds.WireBytes)
	}
	// The current requested policy stays intact. An earlier wider reservation
	// cannot override a subsequently tightened MaxValueBytes or revive zero caps.
	wire, err := temporal.AppendScope(b.scopeScratch[:0:bounds.WireBytes], s, l)
	if err != nil {
		return nil, err
	}
	b.scopeScratch = wire
	return wire, nil
}

// ReserveScope admits the fixed atomic slots and variable immutable backing for
// an owned scope copy; borrowed axis strings are charged only by their owner.
func (b *OutputBudget) ReserveScope(s temporal.Scope, l temporal.Limits) error {
	if b == nil {
		return ErrInvalidInput
	}
	info, err := s.EncodingBounds(l)
	if err != nil {
		return err
	}
	return b.Reserve(128 + outputIntervalBytes*info.Parts + 4*info.WireBytes)
}

// ReserveState admits immutable piece metadata/backing before a defensive clone.
// Cached Usage is inspected without calling Pieces, which itself allocates.
func (b *OutputBudget) ReserveState(s state.State) error {
	if b == nil {
		return ErrInvalidInput
	}
	usage := s.Usage()
	if usage.Pieces() < 0 || usage.MetadataBytes() < 0 {
		return ErrInvalidInput
	}
	if usage.Pieces() > b.Remaining()/1024 {
		return ErrResourceLimit
	}
	fixed := 128 + 1024*usage.Pieces()
	if fixed > b.Remaining() {
		return ErrResourceLimit
	}
	if usage.MetadataBytes() > (b.Remaining()-fixed)/4 {
		return ErrResourceLimit
	}
	return b.Reserve(fixed + 4*usage.MetadataBytes())
}

// ReserveSlice admits a fresh header array before growth. Retired arrays stay
// charged; element payload backing is reserved by its owning codec/read separately.
func (b *OutputBudget) ReserveSlice(length, capacity, slots int) (int, error) {
	if b == nil || length < 0 || capacity < length || slots < 1 {
		return capacity, ErrInvalidInput
	}
	if length < capacity {
		return capacity, nil
	}
	if capacity > math.MaxInt/2 {
		return capacity, ErrResourceLimit
	}
	target := max(1, 2*capacity)
	if target > b.Remaining()/slots {
		return capacity, ErrResourceLimit
	}
	if err := b.Reserve(target * slots); err != nil {
		return capacity, err
	}
	return target, nil
}

// ReserveMap admits bounded typed map backing before insertion. On the pinned
// Go1.26.9 Swiss maps, start with eight slots and precharge each doubling before
// the7/8 growth threshold. slots includes portable key/value/control/header and
// retained old-table allowance; this is representation reservation, not RSS.
func (b *OutputBudget) ReserveMap(entries, capacity, slots int) (int, error) {
	if b == nil || entries < 0 || capacity < 0 || slots < 1 {
		return capacity, ErrInvalidInput
	}
	if capacity != 0 && entries < capacity-capacity/8 {
		return capacity, nil
	}
	if capacity > math.MaxInt/2 {
		return capacity, ErrResourceLimit
	}
	target := max(8, 2*capacity)
	if target > b.Remaining()/slots {
		return capacity, ErrResourceLimit
	}
	if err := b.Reserve(target * slots); err != nil {
		return capacity, err
	}
	return target, nil
}

// ScalarKey admits codec backing before constructing an exact typed identity.
// Scope numeric work is reserved by ScopeBytes; preservation descriptors use
// their finite envelope cap, with no numerical interpretation or scratch.
func (b *OutputBudget) ScalarKey(v Scalar, l Limits) (string, error) {
	if b == nil {
		return "", ErrInvalidInput
	}
	l, err := l.resolve()
	if err != nil {
		return "", err
	}
	if v.Kind() == ScalarScope {
		wire, err := b.ScopeBytes(v.scope, l.Component.Temporal)
		if err != nil {
			return "", err
		}
		if len(wire) >= l.MaxReadBytes {
			return "", ErrResourceLimit
		}
		if err := b.Reserve(128 + 2*(len(wire)+1)); err != nil {
			return "", err
		}
		key := make([]byte, len(wire)+1)
		key[0] = byte(ScalarScope)
		copy(key[1:], wire)
		return string(key), nil
	}
	size := 1
	switch v.Kind() {
	case ScalarNull:
	case ScalarString:
		if len(v.text) > l.MaxReadBytes-1 {
			return "", ErrResourceLimit
		}
		size += len(v.text)
	case ScalarBool:
		size = 2
	case ScalarI64, ScalarF64:
		size = 9
	case ScalarDescriptor:
		size = 1 + min(cmp.Or(l.Component.Temporal.MaxDescriptorBytes, temporal.DefaultLimits().MaxDescriptorBytes), l.MaxReadBytes-1)
	default:
		return "", ErrInvalidInput
	}
	if size > l.MaxReadBytes {
		return "", ErrResourceLimit
	}
	if size > (b.Remaining()-128)/8 {
		return "", ErrResourceLimit
	}
	if err := b.Reserve(128 + 8*size); err != nil {
		return "", err
	}
	return v.EqualityKey(l)
}

// Child adds one finite local cap to this same operation ledger. Every successful
// reservation consumes both this child and all ancestors exactly once. Failed
// reservations change neither counter; completion/rejection refunds nothing.
// Its fixed128-byte handle is admitted on the parent before allocation.
func (b *OutputBudget) Child(limit int) (*OutputBudget, error) {
	if b == nil || limit < 0 || limit > 64<<20 {
		return nil, ErrInvalidInput
	}
	if err := b.Reserve(128); err != nil {
		return nil, err
	}
	return &OutputBudget{limit: limit, parent: b}, nil
}
