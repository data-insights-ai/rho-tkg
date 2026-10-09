package temporal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
)

// This provisional semantic codec has no physical inline/scale tags and makes
// no lexicographic-key ordering promise. Header: 'T','P',version,profile,
// AxisID(16), axis definition hash(32). Each integer is sign(0/1/2), uint16
// big-endian magnitude length, shortest unsigned big-endian magnitude. A
// rational is its reduced numerator followed by its positive denominator.
const positionHeaderBytes = 52
const positionCodecVersion = 1

func integerWireBytes(n Integer) int { return 3 + (n.magnitudeBits()+7)/8 }
func positionWireBytes(p Position) int {
	size := positionHeaderBytes
	switch p.Profile() {
	case ProfileIntegerZ:
		return size + integerWireBytes(p.integer)
	case ProfileRationalQ:
		return size + integerWireBytes(p.rational.num) + integerWireBytes(p.rational.Denominator())
	case ProfileLexicographicQN:
		return size + integerWireBytes(p.rational.num) + integerWireBytes(p.rational.Denominator()) + integerWireBytes(p.micro)
	default:
		return size
	}
}

// AppendPosition appends a canonical value. Errors leave both dst's length and
// its backing bytes unchanged. The payload limit excludes the existing prefix.
func AppendPosition(dst []byte, p Position, l Limits) ([]byte, error) {
	l, err := l.resolved()
	if err != nil {
		return dst, err
	}
	if err := p.validate(l); err != nil {
		return dst, err
	}
	size := positionWireBytes(p)
	if size > l.MaxValueBytes || size > int(^uint(0)>>1)-len(dst) {
		return dst, fmt.Errorf("%w: position bytes", ErrResourceLimit)
	}
	// Complete all fallible checks before the first append into caller storage.
	return appendPositionUnchecked(slices.Grow(dst, size), p), nil
}

func appendPositionUnchecked(dst []byte, p Position) []byte {
	dst = append(dst, 'T', 'P', positionCodecVersion, byte(p.Profile()))
	dst = append(dst, p.axis.desc.ID[:]...)
	dst = append(dst, p.axis.digest[:]...)
	return appendPositionPayload(dst, p)
}

// appendPositionPayload writes only a validated coordinate, sharing dispatch
// with compound codecs whose header already binds the axis and profile.
func appendPositionPayload(dst []byte, p Position) []byte {
	switch p.Profile() {
	case ProfileIntegerZ:
		dst = appendInteger(dst, p.integer)
	case ProfileRationalQ:
		dst = appendRational(dst, p.rational)
	case ProfileLexicographicQN:
		dst = appendRational(dst, p.rational)
		dst = appendInteger(dst, p.micro)
	}
	return dst
}

func appendRational(dst []byte, r Rational) []byte {
	dst = appendInteger(dst, r.num)
	return appendInteger(dst, r.Denominator())
}
func appendInteger(dst []byte, n Integer) []byte {
	sign := byte(0)
	if n.Sign() > 0 {
		sign = 1
	}
	if n.Sign() < 0 {
		sign = 2
	}
	var magnitude []byte
	if n.wide != nil {
		magnitude = n.wide.Bytes()
	} else if n.small != 0 {
		var buf [8]byte
		var value uint64
		if n.small < 0 {
			value = uint64(-(n.small + 1)) + 1
		} else {
			value = uint64(n.small)
		} // #nosec G115 -- safe unsigned magnitude.
		binary.BigEndian.PutUint64(buf[:], value)
		magnitude = bytes.TrimLeft(buf[:], "\x00")
	}
	dst = append(dst, sign)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(magnitude))) // #nosec G115 -- hard magnitude bound <= 8192 bytes.
	return append(dst, magnitude...)
}

// DecodePosition checks bytes against an explicitly supplied known axis. It
// rejects noncanonical values, unknown versions, truncation and trailing bytes.
// No axis resolver callback or executable descriptor is loaded from the data.
func DecodePosition(src []byte, axis Axis, l Limits) (Position, error) {
	l, err := l.resolved()
	if err != nil {
		return Position{}, err
	}
	if err := axis.validate(); err != nil {
		return Position{}, err
	}
	if err := axis.validateDescriptorBudget(l); err != nil {
		return Position{}, err
	}
	if len(src) > l.MaxValueBytes || len(src) > l.MaxInputBytes {
		return Position{}, fmt.Errorf("%w: encoded position bytes", ErrResourceLimit)
	}
	if len(src) < positionHeaderBytes || src[0] != 'T' || src[1] != 'P' {
		return Position{}, ErrInvalidEncoding
	}
	if src[2] != positionCodecVersion {
		return Position{}, errors.Join(ErrInvalidEncoding, ErrUnknownVersion)
	}
	profile := Profile(src[3])
	if !validProfile(profile) {
		return Position{}, errors.Join(ErrInvalidEncoding, ErrUnknownProfile)
	}
	if profile != axis.desc.Profile || !bytes.Equal(src[4:20], axis.desc.ID[:]) || !bytes.Equal(src[20:52], axis.digest[:]) {
		return Position{}, ErrAxisMismatch
	}
	d := positionDecoder{src: src[positionHeaderBytes:], limits: l}
	p, err := d.position(axis)
	if err != nil {
		return Position{}, err
	}
	if len(d.src) != 0 {
		return Position{}, ErrInvalidEncoding
	}
	return p, nil
}

type positionDecoder struct {
	src    []byte
	limits Limits
}

// position decodes a coordinate payload under a validated shared axis header.
func (d *positionDecoder) position(axis Axis) (Position, error) {
	p := Position{axis: axis}
	var err error
	switch axis.desc.Profile {
	case ProfileIntegerZ:
		p.integer, err = d.integer()
	case ProfileRationalQ:
		p.rational, err = d.rational()
	case ProfileLexicographicQN:
		p.rational, err = d.rational()
		if err == nil {
			p.micro, err = d.integer()
		}
		if err == nil && p.micro.Sign() < 0 {
			err = ErrInvalidEncoding
		}
	default:
		err = ErrUnknownProfile
	}
	if err != nil {
		return Position{}, err
	}
	return p, nil
}

func (d *positionDecoder) integer() (Integer, error) {
	if len(d.src) < 3 {
		return Integer{}, ErrInvalidEncoding
	}
	sign := d.src[0]
	count := int(binary.BigEndian.Uint16(d.src[1:3]))
	d.src = d.src[3:]
	if sign > 2 || count > len(d.src) || (count == 0) != (sign == 0) {
		return Integer{}, ErrInvalidEncoding
	}
	if count > (d.limits.MaxMagnitudeBits+7)/8 {
		return Integer{}, ErrResourceLimit
	}
	magnitude := d.src[:count]
	d.src = d.src[count:]
	if count == 0 {
		return Integer{}, nil
	}
	if magnitude[0] == 0 {
		return Integer{}, ErrInvalidEncoding
	}
	// Top byte determines exact bit length before SetBytes allocates.
	top := magnitude[0]
	topBits := 0
	for top != 0 {
		topBits++
		top >>= 1
	}
	if (count-1)*8+topBits > d.limits.MaxMagnitudeBits {
		return Integer{}, ErrResourceLimit
	}
	if count <= 8 {
		var value uint64
		for _, digit := range magnitude {
			value = (value << 8) | uint64(digit)
		}
		if sign == 1 && value <= math.MaxInt64 {
			return Int64(int64(value)), nil
		} // #nosec G115 -- explicitly range checked.
		if sign == 2 && value <= uint64(math.MaxInt64)+1 {
			if value == uint64(math.MaxInt64)+1 {
				return Int64(math.MinInt64), nil
			}
			return Int64(-int64(value)), nil // #nosec G115 -- value <= MaxInt64.
		}
	}
	n := new(big.Int).SetBytes(magnitude)
	if sign == 2 {
		n.Neg(n)
	}
	return ownedInteger(n), nil
}

func (d *positionDecoder) rational() (Rational, error) {
	n, err := d.integer()
	if err != nil {
		return Rational{}, err
	}
	den, err := d.integer()
	if err != nil {
		return Rational{}, err
	}
	if den.Sign() <= 0 {
		return Rational{}, ErrInvalidEncoding
	}
	if n.Sign() == 0 {
		if compareInteger(den, Int64(1)) != Equal {
			return Rational{}, ErrInvalidEncoding
		}
		return Rational{}, nil
	}
	g := new(big.Int).GCD(nil, nil, n.bigCopy(), den.bigCopy())
	if g.Cmp(big.NewInt(1)) != 0 {
		return Rational{}, ErrInvalidEncoding
	}
	if compareInteger(den, Int64(1)) == Equal {
		den = Integer{}
	}
	return Rational{num: n, den: den}, nil
}

// PositionHash hashes the canonical exact coordinate and stable axis definition,
// excluding every physical storage choice. It does not hash a source record or
// replace an assertion ID. The codec is not an order-preserving index key.
func PositionHash(p Position, l Limits) ([32]byte, error) {
	encoded, err := AppendPosition(nil, p, l)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("rho-tkg:position:v1\x00"), encoded...)), nil
}
