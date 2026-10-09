package temporal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// Provisional scope v1 header: 'T','S',version,profile,AxisID(16),definition
// hash(32),ScopeKind. Points encode one coordinate payload. Spans encode two
// bound tags (BoundKind | 0x80 for inclusive) and finite coordinate payloads.
// Regions encode uint32 count then Point/Span tags and their atomic payloads.
// All integer/rational/lex payloads reuse the scalar canonical codec.
const scopeCodecVersion = 1
const scopeHeaderBytes = positionHeaderBytes + 1
const scopeRegionCountBytes = 4

func intervalBytes(p interval) int {
	switch intervalKind(p) {
	case ScopePoint:
		return positionWireBytes(p.lo.position) - positionHeaderBytes
	case ScopeAll:
		return 0
	default:
		return spanPayloadBytes(p)
	}
}
func spanPayloadBytes(p interval) int {
	n := 2
	for _, b := range []Bound{p.lo, p.hi} {
		if b.kind == BoundFinite {
			n += positionWireBytes(b.position) - positionHeaderBytes
		}
	}
	return n
}
func scopePiecesBytes(count, payloadBytes int) int {
	if count > 1 {
		return payloadBytes + scopeRegionCountBytes + count
	}
	return payloadBytes
}
func scopeWireBytes(s Scope) int {
	size := scopeHeaderBytes
	for _, p := range s.parts {
		size += intervalBytes(p)
	}
	return scopePiecesBytes(len(s.parts), size)
}

// AppendScope appends canonical scope bytes after complete validation. On any
// error both dst's length and all bytes in its backing array remain unchanged.
// MaxValueBytes covers only the appended scope, including its shared axis header.
func AppendScope(dst []byte, s Scope, l Limits) ([]byte, error) {
	l, err := l.resolved()
	if err != nil {
		return dst, err
	}
	if err := s.validate(l); err != nil {
		return dst, err
	}
	if err := validateCanonicalScope(s, l); err != nil {
		return dst, err
	}
	size := scopeWireBytes(s)
	if size > int(^uint(0)>>1)-len(dst) {
		return dst, ErrResourceLimit
	}
	dst = slices.Grow(dst, size)
	dst = append(dst, 'T', 'S', scopeCodecVersion, byte(s.axis.desc.Profile))
	dst = append(dst, s.axis.desc.ID[:]...)
	dst = append(dst, s.axis.digest[:]...)
	dst = append(dst, byte(s.kind))
	switch s.kind {
	case ScopePoint, ScopeSpan:
		dst = appendScopeAtomicPayload(dst, s.parts[0], s.kind)
	case ScopeRegion:
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(s.parts))) // #nosec G115 -- validated piece count <= 65536.
		for _, p := range s.parts {
			kind := intervalKind(p)
			dst = append(dst, byte(kind))
			dst = appendScopeAtomicPayload(dst, p, kind)
		}
	}
	return dst, nil
}
func appendScopeAtomicPayload(dst []byte, p interval, kind ScopeKind) []byte {
	if kind == ScopePoint {
		return appendPositionPayload(dst, p.lo.position)
	}
	for _, b := range []Bound{p.lo, p.hi} {
		tag := byte(b.kind)
		if b.inclusive {
			tag |= 0x80
		}
		dst = append(dst, tag)
		if b.kind == BoundFinite {
			dst = appendPositionPayload(dst, b.position)
		}
	}
	return dst
}

func validateCanonicalScope(s Scope, l Limits) error {
	for i, p := range s.parts {
		if err := validateCanonicalAtomic(p, intervalKind(p), s.axis, l); err != nil {
			return err
		}
		if i > 0 {
			join, err := canMerge(s.parts[i-1], p, s.axis, l)
			if err != nil {
				return err
			}
			if join {
				return ErrInvalidEncoding
			}
		}
	}
	return nil
}
func validateCanonicalAtomic(p interval, kind ScopeKind, axis Axis, l Limits) error {
	if kind == ScopeAll {
		return nil
	}
	if kind != ScopePoint && kind != ScopeSpan {
		return ErrInvalidEncoding
	}
	if kind == ScopeSpan && axis.desc.Profile != ProfileRationalQ && ((p.lo.kind == BoundFinite && !p.lo.inclusive) || (p.hi.kind == BoundFinite && p.hi.inclusive)) {
		return ErrInvalidEncoding
	}
	normal, ok, err := normalizeInterval(p, axis, l)
	if err != nil {
		return err
	}
	if !ok || intervalKind(normal) != kind || compareBound(p.lo, normal.lo, l) != Equal || compareBound(p.hi, normal.hi, l) != Equal || p.lo.inclusive != normal.lo.inclusive || p.hi.inclusive != normal.hi.inclusive {
		return ErrInvalidEncoding
	}
	return nil
}

// DecodeScope decodes only canonical bytes for an explicitly supplied known
// axis. It rejects equivalent alternate syntax, unsupported versions, malformed
// counts/bounds, truncation and trailing bytes before returning an owned value.
func DecodeScope(src []byte, axis Axis, l Limits) (Scope, error) {
	l, err := l.resolved()
	if err != nil {
		return Scope{}, err
	}
	if err := validateScopeAxis(axis, l); err != nil {
		return Scope{}, err
	}
	if len(src) > l.MaxInputBytes || len(src) > l.MaxValueBytes {
		return Scope{}, fmt.Errorf("%w: encoded scope bytes", ErrResourceLimit)
	}
	if len(src) < scopeHeaderBytes || src[0] != 'T' || src[1] != 'S' {
		return Scope{}, ErrInvalidEncoding
	}
	if src[2] != scopeCodecVersion {
		return Scope{}, errors.Join(ErrInvalidEncoding, ErrUnknownVersion)
	}
	profile := Profile(src[3])
	if !validProfile(profile) {
		return Scope{}, errors.Join(ErrInvalidEncoding, ErrUnknownProfile)
	}
	if profile != axis.desc.Profile || !bytes.Equal(src[4:20], axis.desc.ID[:]) || !bytes.Equal(src[20:52], axis.digest[:]) {
		return Scope{}, ErrAxisMismatch
	}
	kind := ScopeKind(src[52])
	d := positionDecoder{src: src[scopeHeaderBytes:], limits: l}
	s := Scope{axis: axis, kind: kind}
	switch kind {
	case ScopeUnplaced, ScopeEmpty:
	case ScopeAll:
		s.parts = []interval{{NegativeInfinity(), PositiveInfinity()}}
	case ScopePoint, ScopeSpan:
		p, err := decodeScopeAtomic(&d, axis, kind)
		if err != nil {
			return Scope{}, err
		}
		s.parts = []interval{p}
	case ScopeRegion:
		if len(d.src) < scopeRegionCountBytes {
			return Scope{}, ErrInvalidEncoding
		}
		count := binary.BigEndian.Uint32(d.src[:4])
		d.src = d.src[4:]
		if uint64(count) > uint64(l.MaxRegionPieces) { // #nosec G115 -- l.resolved validated the positive piece count in [1,65536].
			return Scope{}, ErrResourceLimit
		}
		if count < 2 {
			return Scope{}, ErrInvalidEncoding
		}
		minimum := 4
		switch profile {
		case ProfileRationalQ:
			minimum = 8
		case ProfileLexicographicQN:
			minimum = 11
		}
		if uint64(count) > uint64(len(d.src)/minimum) {
			return Scope{}, ErrInvalidEncoding
		}
		s.parts = make([]interval, int(count)) // #nosec G115 -- bounded by validated count and available source bytes.
		for i := range s.parts {
			if len(d.src) == 0 {
				return Scope{}, ErrInvalidEncoding
			}
			atom := ScopeKind(d.src[0])
			d.src = d.src[1:]
			if atom != ScopePoint && atom != ScopeSpan {
				return Scope{}, ErrInvalidEncoding
			}
			p, err := decodeScopeAtomic(&d, axis, atom)
			if err != nil {
				return Scope{}, err
			}
			if i > 0 {
				join, err := canMerge(s.parts[i-1], p, axis, l)
				if err != nil {
					return Scope{}, err
				}
				if join {
					return Scope{}, ErrInvalidEncoding
				}
			}
			s.parts[i] = p
		}
	default:
		return Scope{}, ErrInvalidEncoding
	}
	if len(d.src) != 0 {
		return Scope{}, ErrInvalidEncoding
	}
	if err := s.validate(l); err != nil {
		return Scope{}, err
	}
	return s, nil
}
func decodeScopeAtomic(d *positionDecoder, axis Axis, kind ScopeKind) (interval, error) {
	var p interval
	if kind == ScopePoint {
		position, err := d.position(axis)
		if err != nil {
			return interval{}, err
		}
		b := Bound{kind: BoundFinite, position: position, inclusive: true}
		p = interval{b, b}
	} else {
		var err error
		p.lo, err = decodeScopeBound(d, axis, true)
		if err != nil {
			return interval{}, err
		}
		p.hi, err = decodeScopeBound(d, axis, false)
		if err != nil {
			return interval{}, err
		}
	}
	if intervalKind(p) != kind {
		return interval{}, ErrInvalidEncoding
	}
	if err := validateCanonicalAtomic(p, kind, axis, d.limits); err != nil {
		return interval{}, err
	}
	return p, nil
}
func decodeScopeBound(d *positionDecoder, axis Axis, lower bool) (Bound, error) {
	if len(d.src) == 0 {
		return Bound{}, ErrInvalidEncoding
	}
	tag := d.src[0]
	d.src = d.src[1:]
	b := Bound{kind: BoundKind(tag & 0x7f), inclusive: tag&0x80 != 0}
	if b.kind == BoundFinite {
		p, err := d.position(axis)
		if err != nil {
			return Bound{}, err
		}
		b.position = p
	}
	if err := b.validate(axis, lower, d.limits); err != nil {
		return Bound{}, errors.Join(ErrInvalidEncoding, err)
	}
	return b, nil
}

// ScopeHash hashes canonical support and explicit placement kind on one stable
// axis. It excludes source identity and interpretation, and is not a source-byte
// integrity hash or an order-preserving storage key.
func ScopeHash(s Scope, l Limits) ([32]byte, error) {
	encoded, err := AppendScope(nil, s, l)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("rho-tkg:scope:v1\x00"), encoded...)), nil
}
