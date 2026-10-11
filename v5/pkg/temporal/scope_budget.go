package temporal

import "math"

// ScopeEncodingBounds separates exact emitted size from conservative numerical
// representation reservations needed by AppendScope. It describes no process
// heap/RSS, allocator metadata, goroutine stack or math/big pool bookkeeping.
// NumericBytes covers temporary numeric backing, including retired word arrays,
// under the supported Go1.26.9 math/big algorithms on32/64-bit words.
// Canonical validation remains AppendScope's responsibility.
type ScopeEncodingBounds struct {
	Parts, WireBytes, NumericBytes int
}

// EncodingBounds inspects immutable private fields without cloning Parts,
// serializing, normalizing or performing numeric arithmetic. Valid-input calls
// allocate no backing. It honors the supplied policy rather than scratch capacity.
func (s Scope) EncodingBounds(l Limits) (ScopeEncodingBounds, error) {
	l, err := l.resolved()
	if err != nil {
		return ScopeEncodingBounds{}, err
	}
	if s.kind < ScopeUnplaced || s.kind > ScopeAll {
		return ScopeEncodingBounds{}, ErrInvalidScope
	}
	if err := validateScopeAxis(s.axis, l); err != nil {
		return ScopeEncodingBounds{}, err
	}
	if len(s.parts) > l.MaxRegionPieces {
		return ScopeEncodingBounds{}, ErrResourceLimit
	}
	if s.kind == ScopeEmpty || s.kind == ScopeUnplaced {
		if len(s.parts) != 0 {
			return ScopeEncodingBounds{}, ErrInvalidScope
		}
	} else if len(s.parts) == 0 || len(s.parts) > 1 && s.kind != ScopeRegion || len(s.parts) == 1 && s.kind != encodingIntervalKind(s.parts[0]) {
		return ScopeEncodingBounds{}, ErrInvalidScope
	}
	out := ScopeEncodingBounds{Parts: len(s.parts), WireBytes: scopeHeaderBytes}
	inlineOnly := true
	if len(s.parts) > 1 {
		out.WireBytes += scopeRegionCountBytes + len(s.parts)
	}
	for i, p := range s.parts {
		kind := encodingIntervalKind(p)
		inlineOnly = inlineOnly && encodingPositionInline(p.lo.position) && encodingPositionInline(p.hi.position)
		for _, b := range []struct {
			bound Bound
			lower bool
		}{{p.lo, true}, {p.hi, false}} {
			if err := b.bound.validate(s.axis, b.lower, l); err != nil {
				return ScopeEncodingBounds{}, err
			}
		}
		n := 0
		switch kind {
		case ScopePoint:
			n = positionWireBytes(p.lo.position) - positionHeaderBytes
		case ScopeAll:
		case ScopeSpan:
			n = spanPayloadBytes(p)
		default:
			return ScopeEncodingBounds{}, ErrInvalidEncoding
		}
		if n > l.MaxValueBytes-out.WireBytes {
			return ScopeEncodingBounds{}, ErrResourceLimit
		}
		out.WireBytes += n
		// Five interval classifications are sufficient for the two size walks,
		// structural check/emission and input/result canonical classifications.
		if err := out.addNumeric(5 * encodingEqualityBytes(p.lo.position, p.hi.position)); err != nil {
			return ScopeEncodingBounds{}, err
		}
		if kind == ScopePoint || kind == ScopeSpan {
			if err := out.addNumeric(encodingCompareBytes(p.lo, p.hi)); err != nil {
				return ScopeEncodingBounds{}, err
			}
			if kind == ScopeSpan && s.axis.desc.Profile != ProfileRationalQ {
				// Three further normalization comparisons may observe stepped
				// lower/upper coordinates. Price their possible extra bits too.
				if err := out.addNumeric(encodingSteppedCompareBytes(p.lo, p.hi, 1, 0) + encodingSteppedCompareBytes(p.lo, p.hi, 1, 1) + encodingSteppedCompareBytes(p.lo, p.hi, 2, 1)); err != nil {
					return ScopeEncodingBounds{}, err
				}
				if err := out.addNumeric(encodingSuccessorBytes(p.lo.position, 0) + encodingSuccessorBytes(p.hi.position, 0) + encodingSuccessorBytes(p.lo.position, 1) + encodingEqualityBytes(p.lo.position, p.hi.position)); err != nil {
					return ScopeEncodingBounds{}, err
				}
			}
			if err := out.addNumeric(encodingCompareBytes(p.lo, p.lo) + encodingCompareBytes(p.hi, p.hi)); err != nil {
				return ScopeEncodingBounds{}, err
			}
			if err := out.addNumeric(encodingEmissionBytes(p.lo.position)); err != nil {
				return ScopeEncodingBounds{}, err
			}
			if kind == ScopeSpan && p.hi.kind == BoundFinite {
				if err := out.addNumeric(encodingEmissionBytes(p.hi.position)); err != nil {
					return ScopeEncodingBounds{}, err
				}
			}
		}
		if i > 0 {
			previous := s.parts[i-1]
			if err := out.addNumeric(encodingCompareBytes(previous.hi, p.lo)); err != nil {
				return ScopeEncodingBounds{}, err
			}
			if s.axis.desc.Profile != ProfileRationalQ && previous.hi.kind == BoundFinite && p.lo.kind == BoundFinite && previous.hi.inclusive && p.lo.inclusive {
				if err := out.addNumeric(encodingEqualityBytes(previous.hi.position, p.lo.position) + encodingSuccessorBytes(previous.hi.position, 0) + encodingSteppedCompareBytes(previous.hi, p.lo, 1, 0)); err != nil {
					return ScopeEncodingBounds{}, err
				}
			}
		}
	}
	if out.WireBytes > l.MaxValueBytes {
		return ScopeEncodingBounds{}, ErrResourceLimit
	}
	// Structurally valid inline positions compare without math/big. In a
	// canonical discrete span a finite successor cannot overflow inline when
	// its greater endpoint is also inline on the same successor chain; invalid
	// flags/order are refused by AppendScope before any widening step.
	if inlineOnly {
		out.NumericBytes = 0
	}
	return out, nil
}

func (b *ScopeEncodingBounds) addNumeric(n int) error {
	if n < 0 || n > math.MaxInt-b.NumericBytes {
		return ErrResourceLimit
	}
	b.NumericBytes += n
	return nil
}

// Mixed-width equality never calls compareInteger: its bigCopy fallback would
// defeat allocation-free introspection. IsInt64/Int64 inspect existing words.
func encodingIntegerEqual(a, b Integer) bool {
	if a.wide == nil && b.wide == nil {
		return a.small == b.small
	}
	if a.wide != nil && b.wide != nil {
		return a.wide.Cmp(b.wide) == 0
	}
	if a.wide != nil {
		return a.wide.IsInt64() && a.wide.Int64() == b.small
	}
	return b.wide.IsInt64() && b.wide.Int64() == a.small
}
func encodingPositionInline(p Position) bool {
	if p.Profile() == ProfileIntegerZ {
		return p.integer.wide == nil
	}
	return p.rational.num.wide == nil && p.rational.den.wide == nil && (p.Profile() != ProfileLexicographicQN || p.micro.wide == nil)
}
func encodingPositionEqual(a, b Position) bool {
	if a.Profile() == ProfileIntegerZ {
		return encodingIntegerEqual(a.integer, b.integer)
	}
	if !encodingIntegerEqual(a.rational.num, b.rational.num) || !encodingIntegerEqual(a.rational.Denominator(), b.rational.Denominator()) {
		return false
	}
	return a.Profile() != ProfileLexicographicQN || encodingIntegerEqual(a.micro, b.micro)
}
func encodingIntervalKind(p interval) ScopeKind {
	if p.lo.kind == BoundNegativeInfinity && p.hi.kind == BoundPositiveInfinity {
		return ScopeAll
	}
	if p.lo.kind == BoundFinite && p.hi.kind == BoundFinite && p.lo.inclusive && p.hi.inclusive && encodingPositionEqual(p.lo.position, p.hi.position) {
		return ScopePoint
	}
	return ScopeSpan
}

const encodingIntHeaderBytes = 64          // Portable conservative representation slot, not unsafe.Sizeof/RSS.
func encodingWords(bits, wordBits int) int { return (bits + wordBits - 1) / wordBits }
func encodingNatCapacity(n int) int {
	if n < 2 {
		return n
	}
	return n + 4 // Go1.26.9 nat.make's explicit growth padding.
}
func encodingCopyBytes(n Integer, wordBits int) int {
	return encodingIntHeaderBytes + encodingNatCapacity(encodingWords(n.magnitudeBits(), wordBits))*(wordBits/8)
}
func encodingIntegerCompareBytes(a, b Integer) int {
	if (a.wide == nil) == (b.wide == nil) {
		return 0
	}
	return max(encodingCopyBytes(a, 32)+encodingCopyBytes(b, 32), encodingCopyBytes(a, 64)+encodingCopyBytes(b, 64))
}
func encodingEqualityBytes(a, b Position) int {
	if a.Profile() == ProfileIntegerZ {
		return encodingIntegerCompareBytes(a.integer, b.integer)
	}
	n := encodingIntegerCompareBytes(a.rational.num, b.rational.num) + encodingIntegerCompareBytes(a.rational.Denominator(), b.rational.Denominator())
	if a.Profile() == ProfileLexicographicQN {
		n += encodingIntegerCompareBytes(a.micro, b.micro)
	}
	return n
}
func encodingEmissionIntegerBytes(n Integer) int {
	if n.wide == nil {
		return 0
	}
	// big.Int.Bytes allocates whole stored words, not just shortest wire bytes.
	return max(encodingWords(n.magnitudeBits(), 32)*4, encodingWords(n.magnitudeBits(), 64)*8)
}
func encodingEmissionBytes(p Position) int {
	if p.Profile() == ProfileIntegerZ {
		return encodingEmissionIntegerBytes(p.integer)
	}
	n := encodingEmissionIntegerBytes(p.rational.num) + encodingEmissionIntegerBytes(p.rational.Denominator())
	if p.Profile() == ProfileLexicographicQN {
		n += encodingEmissionIntegerBytes(p.micro)
	}
	return n
}
func encodingRound4(n int) int { return (n + 3) &^ 3 }
func encodingKaratsubaWords(n int) int {
	if n < 40 {
		return 0
	}
	h := (n + 1) / 2
	return encodingRound4(2*h+1) + encodingKaratsubaWords(h)
}
func encodingProductWords(m, n int) int {
	if m < n {
		m, n = n, m
	}
	words := encodingNatCapacity(m + n)
	live := encodingKaratsubaWords(n)
	if n >= 40 && m > n {
		live += encodingRound4(2 * n)
	}
	// Go1.26.9 stack.nat grows by append; all retired capacities together are
	// bounded by20 times maximum live words (growth>=5/4, final cap<=4*live).
	return words + 20*live
}
func encodingRationalCompareBytes(a, b Rational) int {
	if a.den.Sign() == 0 && b.den.Sign() == 0 {
		return encodingIntegerCompareBytes(a.num, b.num)
	}
	if a.num.wide == nil && a.den.wide == nil && b.num.wide == nil && b.den.wide == nil {
		return 0
	}
	out := 0
	// Four64-byte Int slots cover four Int headers (at most32B each)
	// plus two cold24-byte multiplication-stack headers. Pool bookkeeping
	// and allocator metadata remain outside representation accounting.
	for _, width := range []int{32, 64} {
		n := encodingCopyBytes(a.num, width) + encodingCopyBytes(b.Denominator(), width) + encodingCopyBytes(b.num, width) + encodingCopyBytes(a.Denominator(), width)
		n += (encodingProductWords(encodingWords(a.num.magnitudeBits(), width), encodingWords(b.Denominator().magnitudeBits(), width)) + encodingProductWords(encodingWords(b.num.magnitudeBits(), width), encodingWords(a.Denominator().magnitudeBits(), width))) * (width / 8)
		out = max(out, n)
	}
	return out
}
func encodingCompareBytes(a, b Bound) int {
	if a.kind != BoundFinite || b.kind != BoundFinite {
		return 0
	}
	x, y := a.position, b.position
	if x.Profile() == ProfileIntegerZ {
		return encodingIntegerCompareBytes(x.integer, y.integer)
	}
	n := encodingRationalCompareBytes(x.rational, y.rational)
	if x.Profile() == ProfileLexicographicQN {
		n += encodingIntegerCompareBytes(x.micro, y.micro)
	}
	return n
}
func encodingSuccessorBytes(p Position, priorSteps int) int {
	n := p.integer
	if p.Profile() == ProfileLexicographicQN {
		n = p.micro
	}
	if p.Profile() == ProfileRationalQ || n.wide == nil && n.small < math.MaxInt64-int64(priorSteps) {
		return 0
	}
	out := 0
	for _, width := range []int{32, 64} {
		out = max(out, encodingIntHeaderBytes+(encodingNatCapacity(encodingWords(n.magnitudeBits()+priorSteps, width))+encodingNatCapacity(encodingWords(n.magnitudeBits()+priorSteps+1, width)))*(width/8))
	}
	return out
}

func encodingSteppedIntegerCompareBytes(a, b Integer, aSteps, bSteps int) int {
	if aSteps == 0 && bSteps == 0 {
		return encodingIntegerCompareBytes(a, b)
	}
	if a.wide == nil && b.wide == nil && a.small < math.MaxInt64-int64(aSteps) && b.small < math.MaxInt64-int64(bSteps) {
		return 0
	}
	out := 0
	for _, width := range []int{32, 64} {
		out = max(out, 2*encodingIntHeaderBytes+(encodingNatCapacity(encodingWords(a.magnitudeBits()+aSteps, width))+encodingNatCapacity(encodingWords(b.magnitudeBits()+bSteps, width)))*(width/8))
	}
	return out
}
func encodingSteppedCompareBytes(a, b Bound, aSteps, bSteps int) int {
	if a.kind != BoundFinite || b.kind != BoundFinite {
		return 0
	}
	x, y := a.position, b.position
	if x.Profile() == ProfileIntegerZ {
		return encodingSteppedIntegerCompareBytes(x.integer, y.integer, aSteps, bSteps)
	}
	n := encodingRationalCompareBytes(x.rational, y.rational)
	if x.Profile() == ProfileLexicographicQN {
		n += encodingSteppedIntegerCompareBytes(x.micro, y.micro, aSteps, bSteps)
	}
	return n
}

// ScopeScratchBounds prices named operations on endpoints drawn from two scopes,
// including at most two discrete successor steps. These are per-operation
// representation reservations under the same Go1.26.9 assumptions as EncodingBounds,
// not an algorithm's total allowance or canonical admission.
type ScopeScratchBounds struct {
	ComparisonBytes, SuccessorBytes, AtomicEncodingBytes, AtomicWireBytes int
	shape                                                                 scopeScratchShape
}

type scopeScratchShape struct {
	axis                                               Axis
	integerBits, numBits, denBits, microBits           int
	wideInteger, wideRational, wideMicro, stepMayWiden bool
}

// ScratchBounds inspects both operands without serialization, Parts clones or
// numeric arithmetic. AtomicWireBytes bounds a new atomic scope assembled from
// their endpoints; callers must count their own traversals/codec invocations.
func (s Scope) ScratchBounds(other Scope, l Limits) (ScopeScratchBounds, error) {
	if _, err := s.EncodingBounds(l); err != nil {
		return ScopeScratchBounds{}, err
	}
	if _, err := other.EncodingBounds(l); err != nil {
		return ScopeScratchBounds{}, err
	}
	if err := sameAxis(s.axis, other.axis); err != nil {
		return ScopeScratchBounds{}, err
	}
	var integerBits, numBits, denBits, microBits int
	wideInteger, wideRational, wideMicro := false, false, false
	stepMayWiden := false
	for _, operand := range []Scope{s, other} {
		for _, part := range operand.parts {
			for _, bound := range []Bound{part.lo, part.hi} {
				if bound.kind != BoundFinite {
					continue
				}
				p := bound.position
				integerBits = max(integerBits, p.integer.magnitudeBits())
				numBits = max(numBits, p.rational.num.magnitudeBits())
				denBits = max(denBits, p.rational.Denominator().magnitudeBits())
				microBits = max(microBits, p.micro.magnitudeBits())
				wideInteger = wideInteger || p.integer.wide != nil
				wideRational = wideRational || p.rational.num.wide != nil || p.rational.den.wide != nil
				wideMicro = wideMicro || p.micro.wide != nil
				switch s.axis.desc.Profile {
				case ProfileIntegerZ:
					stepMayWiden = stepMayWiden || p.integer.wide != nil || p.integer.small > math.MaxInt64-2
				case ProfileLexicographicQN:
					stepMayWiden = stepMayWiden || p.micro.wide != nil || p.micro.small > math.MaxInt64-2
				}
			}
		}
	}
	return priceScopeScratch(scopeScratchShape{s.axis, integerBits, numBits, denBits, microBits, wideInteger, wideRational, wideMicro, stepMayWiden}), nil
}

// Merge combines inspected operand widths before pricing cross-products. Max of
// separately priced reservations is insufficient when different operands own
// the widest numerator and denominator. Neither operand backing is copied.
func (b ScopeScratchBounds) Merge(other ScopeScratchBounds) (ScopeScratchBounds, error) {
	if err := b.shape.axis.validate(); err != nil {
		return ScopeScratchBounds{}, err
	}
	if err := other.shape.axis.validate(); err != nil {
		return ScopeScratchBounds{}, err
	}
	if err := sameAxis(b.shape.axis, other.shape.axis); err != nil {
		return ScopeScratchBounds{}, err
	}
	x, y := b.shape, other.shape
	x.integerBits = max(x.integerBits, y.integerBits)
	x.numBits = max(x.numBits, y.numBits)
	x.denBits = max(x.denBits, y.denBits)
	x.microBits = max(x.microBits, y.microBits)
	x.wideInteger = x.wideInteger || y.wideInteger
	x.wideRational = x.wideRational || y.wideRational
	x.wideMicro = x.wideMicro || y.wideMicro
	x.stepMayWiden = x.stepMayWiden || y.stepMayWiden
	return priceScopeScratch(x), nil
}
func priceScopeScratch(shape scopeScratchShape) ScopeScratchBounds {
	out := ScopeScratchBounds{shape: shape}
	profile := shape.axis.desc.Profile
	// Widths are inspected actual operands, with two possible discrete steps;
	// all products below are bounded by the validated hard65536-bit ceiling+2.
	for _, wordBits := range []int{32, 64} {
		copies := func(bits int) int {
			return encodingIntHeaderBytes + encodingNatCapacity(encodingWords(bits, wordBits))*(wordBits/8)
		}
		comparison, successor, emission := 0, 0, 0
		if profile == ProfileIntegerZ {
			if shape.wideInteger || shape.stepMayWiden {
				comparison = 2 * copies(shape.integerBits+2)
				emission = encodingWords(shape.integerBits+2, wordBits) * (wordBits / 8)
			}
			if shape.stepMayWiden {
				successor = encodingIntHeaderBytes + (encodingNatCapacity(encodingWords(shape.integerBits+1, wordBits))+encodingNatCapacity(encodingWords(shape.integerBits+2, wordBits)))*(wordBits/8)
			}
		} else {
			if shape.wideRational {
				comparison = 2*copies(shape.numBits) + 2*copies(shape.denBits) + 2*encodingProductWords(encodingWords(shape.numBits, wordBits), encodingWords(shape.denBits, wordBits))*(wordBits/8)
				emission = (encodingWords(shape.numBits, wordBits) + encodingWords(shape.denBits, wordBits)) * (wordBits / 8)
			}
			if profile == ProfileLexicographicQN {
				if shape.wideMicro || shape.stepMayWiden {
					comparison += 2 * copies(shape.microBits+2)
					emission += encodingWords(shape.microBits+2, wordBits) * (wordBits / 8)
				}
				if shape.stepMayWiden {
					successor = encodingIntHeaderBytes + (encodingNatCapacity(encodingWords(shape.microBits+1, wordBits))+encodingNatCapacity(encodingWords(shape.microBits+2, wordBits)))*(wordBits/8)
				}
			}
		}
		out.ComparisonBytes = max(out.ComparisonBytes, comparison)
		out.SuccessorBytes = max(out.SuccessorBytes, successor)
		// Five classifications, six bound comparisons, three successor attempts,
		// two endpoint emissions cover one atomic AppendScope's numeric paths.
		out.AtomicEncodingBytes = max(out.AtomicEncodingBytes, 11*comparison+3*successor+2*emission)
	}
	coordinate := func(bits int) int { return 5 + (bits+7)/8 }
	position := coordinate(shape.integerBits + 2)
	if profile != ProfileIntegerZ {
		position = coordinate(shape.numBits) + coordinate(shape.denBits)
		if profile == ProfileLexicographicQN {
			position += coordinate(shape.microBits + 2)
		}
	}
	out.AtomicWireBytes = scopeHeaderBytes + 2 + 2*position
	return out
}
