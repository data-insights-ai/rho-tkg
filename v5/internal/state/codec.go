package state

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

var (
	// ErrInvalidEncoding identifies malformed or noncanonical component bytes.
	ErrInvalidEncoding = errors.New("state: invalid canonical encoding")
	// ErrUnknownVersion identifies an unsupported component codec version.
	ErrUnknownVersion = errors.New("state: unknown codec version")
)

const (
	codecHeaderBytes       = 56 // magic(2),version(1),profile(1),axis(16),definition(32),count(4).
	codecScopeMinimumBytes = 53 // Temporal scope envelope, including ScopeAll.
	codecVersion           = 1
)

// CodecLimits keeps encoded bytes separate from the State metadata/reference
// ledger. MaxEncodedBytes is per appended/decoded envelope, excluding dst's
// prefix. Zero selects 1 MiB; the hard ceiling is 64 MiB. Neither this bound nor
// the ledger is a Go heap guarantee. Payloads are not encoded or dereferenced;
// storage must verify actual dictionary bytes rather than trust declared sizes.
type CodecLimits struct {
	State           Limits
	MaxEncodedBytes int
}

// DefaultCodecLimits returns bounded component page/change-group policy.
func DefaultCodecLimits() CodecLimits {
	return CodecLimits{State: DefaultLimits(), MaxEncodedBytes: 1 << 20}
}

// Validate checks reducer policy and the independent encoded-byte ceiling.
func (l CodecLimits) Validate() error { _, err := l.resolved(); return err }
func (l CodecLimits) resolved() (CodecLimits, error) {
	var err error
	l.State, err = l.State.resolved()
	if err != nil {
		return CodecLimits{}, err
	}
	l.MaxEncodedBytes = cmp.Or(l.MaxEncodedBytes, 1<<20)
	if l.MaxEncodedBytes < 1 || l.MaxEncodedBytes > hardMetadataBytes {
		return CodecLimits{}, ErrInvalidLimits
	}
	return l, nil
}

// Provisional v1 SC/CC envelopes bind an external known axis by profile, ID and
// full definition hash. Each ordered atomic entry has a uint32 big-endian length
// followed by the canonical temporal scope codec and one (SC) or two (CC) fixed
// 34-byte cells. Cells encode presence, value kind and four uint64 big-endian
// fields: payload ID/declared size/revision/provenance. This semantic codec is
// not a compact sealed coordinate-block layout or an integrity seal.

// AppendState appends normalized component metadata, retaining explicit
// retractions. Never-asserted absence is represented only by gaps or an empty snapshot.
// Errors leave dst's length and all bytes of its backing array unchanged. Work
// is linear in entries and exact coordinate bytes/operations, with bounded wire
// staging; state is never reconstructed by replaying Set/Unset.
func AppendState(dst []byte, s State, l CodecLimits) ([]byte, error) {
	l, err := l.resolved()
	if err != nil {
		return dst, err
	}
	if !s.valid {
		return dst, ErrInvalidState
	}
	base, err := New(s.axis, l.State)
	if err != nil {
		return dst, err
	}
	budget, err := newCodecBudget(base.usage, len(s.pieces), false, l)
	if err != nil {
		return dst, err
	}
	for i, p := range s.pieces {
		if err := validateCodecCell(p.cell, false); err != nil {
			return dst, err
		}
		wire, err := temporal.AppendScope(nil, p.scope, l.State.Temporal)
		if err != nil {
			return dst, err
		}
		if err := validateCodecAtom(p.scope, s.axis); err != nil {
			return dst, err
		}
		if i > 0 {
			if err := validateCodecNeighbor(s.pieces[i-1].scope, p.scope, s.pieces[i-1].cell == p.cell, l.State.Temporal); err != nil {
				return dst, err
			}
		}
		if err := budget.add(len(wire), p.cell, Cell{}); err != nil {
			return dst, err
		}
	}
	if budget.wireBytes > int(^uint(0)>>1)-len(dst) {
		return dst, ErrResourceLimit
	}
	wire := appendCodecHeader(make([]byte, 0, budget.wireBytes), s.axis, len(s.pieces), false)
	for _, p := range s.pieces {
		wire, err = appendCodecScope(wire, p.scope, l.State.Temporal)
		if err != nil {
			return dst, err
		}
		wire = appendCodecCell(wire, p.cell)
	}
	return append(slices.Grow(dst, len(wire)), wire...), nil
}

// AppendChanges appends one reducer result/ComponentPatch's exact changed
// regions. It does not group transactions or certify completeness. Runs must be
// sorted, disjoint and maximal for identical before/after cells. Only before
// images may contain authentic zero never-asserted absence. Sequential
// overlapping patches are separate groups, not a concatenated change stream.
// Errors leave every byte of dst's backing array unchanged.
func AppendChanges(dst []byte, axis temporal.Axis, changes []Change, l CodecLimits) ([]byte, error) {
	l, err := l.resolved()
	if err != nil {
		return dst, err
	}
	usage, err := codecInitialUsage(axis, l.State, true)
	if err != nil {
		return dst, err
	}
	budget, err := newCodecBudget(usage, len(changes), true, l)
	if err != nil {
		return dst, err
	}
	for i, c := range changes {
		if err := validateCodecChange(c.before, c.after); err != nil {
			return dst, err
		}
		wire, err := temporal.AppendScope(nil, c.scope, l.State.Temporal)
		if err != nil {
			return dst, err
		}
		if err := validateCodecAtom(c.scope, axis); err != nil {
			return dst, err
		}
		if i > 0 {
			previous := changes[i-1]
			if err := validateCodecNeighbor(previous.scope, c.scope, previous.before == c.before && previous.after == c.after, l.State.Temporal); err != nil {
				return dst, err
			}
		}
		if err := budget.add(len(wire), c.before, c.after); err != nil {
			return dst, err
		}
	}
	if budget.wireBytes > int(^uint(0)>>1)-len(dst) {
		return dst, ErrResourceLimit
	}
	wire := appendCodecHeader(make([]byte, 0, budget.wireBytes), axis, len(changes), true)
	for _, c := range changes {
		wire, err = appendCodecScope(wire, c.scope, l.State.Temporal)
		if err != nil {
			return dst, err
		}
		wire = appendCodecCell(wire, c.before)
		wire = appendCodecCell(wire, c.after)
	}
	return append(slices.Grow(dst, len(wire)), wire...), nil
}
func validateCodecCell(c Cell, allowZero bool) error {
	if c.revision.id == 0 {
		if allowZero && c == (Cell{}) {
			return nil
		}
		return errors.Join(ErrInvalidEncoding, ErrInvalidRevision)
	}
	if !c.present {
		if c.value != (ValueRef{}) {
			return errors.Join(ErrInvalidEncoding, ErrInvalidValueRef)
		}
		return nil
	}
	if err := c.value.validate(); err != nil {
		return errors.Join(ErrInvalidEncoding, err)
	}
	return nil
}
func validateCodecChange(before, after Cell) error {
	if err := validateCodecCell(before, true); err != nil {
		return err
	}
	if err := validateCodecCell(after, false); err != nil {
		return err
	}
	if before == after {
		return ErrInvalidEncoding
	}
	return nil
}
func validateCodecAtom(s temporal.Scope, axis temporal.Axis) error {
	if s.Axis().Descriptor().ID != axis.Descriptor().ID || s.Axis().DefinitionHash() != axis.DefinitionHash() {
		return temporal.ErrAxisMismatch
	}
	switch s.Kind() {
	case temporal.ScopePoint, temporal.ScopeSpan, temporal.ScopeAll:
		return nil
	default:
		return ErrInvalidEncoding
	}
}
func validateCodecNeighbor(previous, next temporal.Scope, same bool, l temporal.Limits) error {
	before, err := scopeBefore(previous, next, l)
	if err != nil {
		return err
	}
	if !before {
		return ErrInvalidEncoding
	}
	if same {
		join, err := mergeable(previous, next, l)
		if err != nil {
			return err
		}
		if join {
			return ErrInvalidEncoding
		}
	}
	return nil
}

func codecInitialUsage(axis temporal.Axis, l Limits, changes bool) (Usage, error) {
	empty, err := temporal.Empty(axis)
	if err != nil {
		return Usage{}, err
	}
	if _, err := temporal.AppendScope(nil, empty, l.Temporal); err != nil {
		return Usage{}, err
	}
	usage := initialUsage(axis)
	if err := usage.check(l, changes); err != nil {
		return Usage{}, err
	}
	return usage, nil
}

type codecBudget struct {
	limits               CodecLimits
	usage                Usage
	wireBytes, cellBytes int
	changes              bool
}

func newCodecBudget(usage Usage, count int, changes bool, l CodecLimits) (codecBudget, error) {
	maxPieces, maxMetadata, cells := l.State.MaxPieces, l.State.MaxMetadataBytes, cellMetadataBytes
	if changes {
		maxPieces, maxMetadata, cells = l.State.MaxChangePieces, l.State.MaxChangeMetadataBytes, 2*cellMetadataBytes
	}
	if count > maxPieces || codecHeaderBytes > l.MaxEncodedBytes || usage.metadataBytes > maxMetadata || count > (l.MaxEncodedBytes-codecHeaderBytes)/(4+codecScopeMinimumBytes+cells) || count > (maxMetadata-usage.metadataBytes)/(codecScopeMinimumBytes+cells) {
		return codecBudget{}, ErrResourceLimit
	}
	return codecBudget{limits: l, usage: usage, wireBytes: codecHeaderBytes, cellBytes: cells, changes: changes}, nil
}
func (b *codecBudget) add(scopeBytes int, first, second Cell) error {
	maxMetadata := b.limits.State.MaxMetadataBytes
	if b.changes {
		maxMetadata = b.limits.State.MaxChangeMetadataBytes
	}
	if scopeBytes > b.limits.MaxEncodedBytes-b.wireBytes-4-b.cellBytes || scopeBytes > maxMetadata-b.usage.metadataBytes-b.cellBytes {
		return ErrResourceLimit
	}
	b.wireBytes += 4 + scopeBytes + b.cellBytes
	b.usage.metadataBytes += scopeBytes + b.cellBytes
	b.usage.pieces++
	for _, c := range [2]Cell{first, second} {
		if c.references() > b.limits.State.MaxReferencedBytes-b.usage.references {
			return ErrResourceLimit
		}
		b.usage.references += c.references()
	}
	return nil
}
func appendCodecHeader(dst []byte, axis temporal.Axis, count int, changes bool) []byte {
	tag := byte('S')
	if changes {
		tag = 'C'
	}
	d, hash := axis.Descriptor(), axis.DefinitionHash()
	dst = append(dst, tag, 'C', codecVersion, byte(d.Profile))
	dst = append(dst, d.ID[:]...)
	dst = append(dst, hash[:]...)
	return binary.BigEndian.AppendUint32(dst, uint32(count)) // #nosec G115 -- validated count <= hardPieces.
}
func appendCodecScope(dst []byte, s temporal.Scope, l temporal.Limits) ([]byte, error) {
	// Fallible encoding writes only private staging, never caller backing bytes.
	at := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	out, err := temporal.AppendScope(dst, s, l)
	if err != nil {
		return dst, err
	}
	binary.BigEndian.PutUint32(out[at:at+4], uint32(len(out)-at-4)) // #nosec G115 -- temporal scope <= 1 MiB.
	return out, nil
}
func appendCodecCell(dst []byte, c Cell) []byte {
	present := byte(0)
	if c.present {
		present = 1
	}
	dst = append(dst, present, byte(c.value.kind))
	for _, n := range [4]uint64{c.value.id, c.value.payloadBytes, c.revision.id, c.revision.provenance} {
		dst = binary.BigEndian.AppendUint64(dst, n)
	}
	return dst
}

type codecCursor struct{ src []byte }

func (c *codecCursor) entry(changes bool) ([]byte, Cell, Cell, error) {
	if len(c.src) < 4 {
		return nil, Cell{}, Cell{}, ErrInvalidEncoding
	}
	n := binary.BigEndian.Uint32(c.src[:4])
	c.src = c.src[4:]
	cells := cellMetadataBytes
	if changes {
		cells *= 2
	}
	if len(c.src) < cells || int64(n) > int64(len(c.src)-cells) || n < codecScopeMinimumBytes {
		return nil, Cell{}, Cell{}, ErrInvalidEncoding
	}
	scope := c.src[:int(n)] // #nosec G115 -- n bounded by available bytes and codec hard cap.
	c.src = c.src[int(n):]  // #nosec G115 -- n bounded by available bytes and codec hard cap.
	first, err := decodeCodecCell(c.src[:cellMetadataBytes], changes)
	c.src = c.src[cellMetadataBytes:]
	if err != nil {
		return nil, Cell{}, Cell{}, err
	}
	var second Cell
	if changes {
		second, err = decodeCodecCell(c.src[:cellMetadataBytes], false)
		c.src = c.src[cellMetadataBytes:]
		if err == nil {
			err = validateCodecChange(first, second)
		}
	}
	return scope, first, second, err
}
func decodeCodecCell(src []byte, allowZero bool) (Cell, error) {
	if src[0] > 1 {
		return Cell{}, ErrInvalidEncoding
	}
	c := Cell{present: src[0] == 1, value: ValueRef{kind: valueKind(src[1]), id: binary.BigEndian.Uint64(src[2:10]), payloadBytes: binary.BigEndian.Uint64(src[10:18])}, revision: Revision{id: binary.BigEndian.Uint64(src[18:26]), provenance: binary.BigEndian.Uint64(src[26:34])}}
	if err := validateCodecCell(c, allowZero); err != nil {
		return Cell{}, err
	}
	return c, nil
}

// inspectCodec checks framing, cells and cumulative resource arithmetic before
// count-sized output allocations. The second pass validates exact coordinates.
func inspectCodec(src []byte, axis temporal.Axis, changes bool, l CodecLimits) (int, error) {
	usage, err := codecInitialUsage(axis, l.State, changes)
	if err != nil {
		return 0, err
	}
	if len(src) > l.MaxEncodedBytes {
		return 0, ErrResourceLimit
	}
	tag := byte('S')
	if changes {
		tag = 'C'
	}
	if len(src) < codecHeaderBytes || src[0] != tag || src[1] != 'C' {
		return 0, ErrInvalidEncoding
	}
	if src[2] != codecVersion {
		return 0, errors.Join(ErrInvalidEncoding, ErrUnknownVersion)
	}
	profile := temporal.Profile(src[3])
	if profile < temporal.ProfileIntegerZ || profile > temporal.ProfileLexicographicQN {
		return 0, errors.Join(ErrInvalidEncoding, temporal.ErrUnknownProfile)
	}
	d, hash := axis.Descriptor(), axis.DefinitionHash()
	if profile != d.Profile || !bytes.Equal(src[4:20], d.ID[:]) || !bytes.Equal(src[20:52], hash[:]) {
		return 0, temporal.ErrAxisMismatch
	}
	n := binary.BigEndian.Uint32(src[52:56])
	if n > hardPieces {
		return 0, ErrResourceLimit
	}
	count := int(n) // #nosec G115 -- n <= hardPieces.
	budget, err := newCodecBudget(usage, count, changes, l)
	if err != nil {
		return 0, err
	}
	if count > (len(src)-codecHeaderBytes)/(4+codecScopeMinimumBytes+budget.cellBytes) {
		return 0, ErrInvalidEncoding
	}
	cursor := codecCursor{src: src[codecHeaderBytes:]}
	for range count {
		wire, first, second, err := cursor.entry(changes)
		if err != nil {
			return 0, err
		}
		if len(wire) > l.State.Temporal.MaxInputBytes || len(wire) > l.State.Temporal.MaxValueBytes {
			return 0, temporal.ErrResourceLimit
		}
		if err := budget.add(len(wire), first, second); err != nil {
			return 0, err
		}
	}
	if len(cursor.src) != 0 {
		return 0, ErrInvalidEncoding
	}
	return count, nil
}

// DecodeState returns owned immutable component metadata on an explicitly known
// axis. It rejects malformed/noncanonical scopes, ordering, overlaps and
// mergeable identical cells instead of sorting/normalizing wire. Empty snapshots
// still validate the axis and charge its shared definition. Input must remain
// unchanged during the call; afterward no mutable input alias is retained.
func DecodeState(src []byte, axis temporal.Axis, l CodecLimits) (State, error) {
	l, err := l.resolved()
	if err != nil {
		return State{}, err
	}
	count, err := inspectCodec(src, axis, false, l)
	if err != nil {
		return State{}, err
	}
	builder := stateBuilder{limits: l.State, usage: initialUsage(axis), parts: make([]Piece, 0, count)}
	cursor := codecCursor{src: src[codecHeaderBytes:]}
	for range count {
		wire, cell, _, err := cursor.entry(false)
		if err != nil {
			return State{}, err
		}
		scope, err := temporal.DecodeScope(wire, axis, l.State.Temporal)
		if err != nil {
			return State{}, errors.Join(ErrInvalidEncoding, err)
		}
		if err := validateCodecAtom(scope, axis); err != nil {
			return State{}, err
		}
		if len(builder.parts) > 0 {
			last := builder.parts[len(builder.parts)-1]
			if err := validateCodecNeighbor(last.scope, scope, last.cell == cell, l.State.Temporal); err != nil {
				return State{}, err
			}
		}
		if err := builder.add(scope, cell); err != nil {
			return State{}, err
		}
	}
	if err := builder.finish(); err != nil {
		return State{}, err
	}
	return State{axis: axis, pieces: builder.parts, usage: builder.usage, policy: l.State, valid: true}, nil
}

// DecodeChanges returns one owned exact changed-region group and its recomputed
// metadata/reference ledger. It checks canonical disjoint/maximal runs and both
// images, without inferring a transaction ID, order or completeness cut.
func DecodeChanges(src []byte, axis temporal.Axis, l CodecLimits) ([]Change, Usage, error) {
	l, err := l.resolved()
	if err != nil {
		return nil, Usage{}, err
	}
	count, err := inspectCodec(src, axis, true, l)
	if err != nil {
		return nil, Usage{}, err
	}
	builder := changeBuilder{limits: l.State, usage: initialUsage(axis), parts: make([]Change, 0, count)}
	cursor := codecCursor{src: src[codecHeaderBytes:]}
	for range count {
		wire, before, after, err := cursor.entry(true)
		if err != nil {
			return nil, Usage{}, err
		}
		scope, err := temporal.DecodeScope(wire, axis, l.State.Temporal)
		if err != nil {
			return nil, Usage{}, errors.Join(ErrInvalidEncoding, err)
		}
		if err := validateCodecAtom(scope, axis); err != nil {
			return nil, Usage{}, err
		}
		if len(builder.parts) > 0 {
			last := builder.parts[len(builder.parts)-1]
			if err := validateCodecNeighbor(last.scope, scope, last.before == before && last.after == after, l.State.Temporal); err != nil {
				return nil, Usage{}, err
			}
		}
		if err := builder.add(scope, before, after); err != nil {
			return nil, Usage{}, err
		}
	}
	return builder.parts, builder.usage, nil
}
