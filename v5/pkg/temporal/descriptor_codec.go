package temporal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"unicode/utf8"
)

// Provisional envelope v1: DO/version/preservation-mode, ID(16), schema(4),
// correlation(16), sized type, reference count(4), ordered references, sized
// payload. References contain sized role, ID(16), sized type, schema(4), hash(32).
// All sizes/counts are uint32 big-endian. Empty payload/reference lists have one
// representation; payload bytes and reference order are otherwise untouched.
const opaqueDescriptorCodecVersion = 1

// AppendOpaqueDescriptor appends canonical preservation bytes after validating
// every size and metadata field. Errors leave all destination backing bytes
// untouched; value/descriptor budgets exclude the caller's existing prefix.
func AppendOpaqueDescriptor(dst []byte, d OpaqueDescriptor, l Limits) ([]byte, error) {
	l, err := l.resolved()
	if err != nil {
		return dst, err
	}
	size, err := d.validate(l)
	if err != nil {
		return dst, err
	}
	if size > int(^uint(0)>>1)-len(dst) {
		return dst, ErrResourceLimit
	}
	dst = slices.Grow(dst, size)
	s := d.spec
	dst = append(dst, 'D', 'O', opaqueDescriptorCodecVersion, byte(DescriptorPreservationOnly))
	dst = append(dst, s.ID[:]...)
	dst = binary.BigEndian.AppendUint32(dst, s.SchemaVersion)
	dst = append(dst, s.Correlation[:]...)
	dst = appendDescriptorField(dst, []byte(s.Type))
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(s.References))) // #nosec G115 -- complete envelope bounded to 1 MiB.
	for _, ref := range s.References {
		dst = appendDescriptorField(dst, []byte(ref.Role))
		dst = append(dst, ref.ID[:]...)
		dst = appendDescriptorField(dst, []byte(ref.Type))
		dst = binary.BigEndian.AppendUint32(dst, ref.SchemaVersion)
		dst = append(dst, ref.Integrity[:]...)
	}
	return appendDescriptorField(dst, s.Payload), nil
}

func appendDescriptorField(dst, data []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(data))) // #nosec G115 -- metadata and payload prevalidated against <= 1 MiB.
	return append(dst, data...)
}

// envelopeCursor borrows already bounded input only during validation. Size
// fields cannot cause allocation or a slice beyond delivered bytes.
type envelopeCursor struct{ src []byte }

func (c *envelopeCursor) take(n int) ([]byte, error) {
	if n < 0 || n > len(c.src) {
		return nil, ErrInvalidEncoding
	}
	out := c.src[:n]
	c.src = c.src[n:]
	return out, nil
}
func (c *envelopeCursor) sized() ([]byte, error) {
	count, err := c.take(4)
	if err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(count)
	if uint64(n) > uint64(len(c.src)) {
		return nil, ErrInvalidEncoding
	}
	return c.take(int(n))
} // #nosec G115 -- n bounded by delivered <= 1 MiB input.

type opaqueWireView struct {
	id                            DescriptorID
	version                       uint32
	correlation                   CorrelationID
	typeName, payload, references []byte
	count                         int
}

func validDescriptorNameBytes(b []byte) bool { return utf8.Valid(b) && len(bytes.TrimSpace(b)) > 0 }
func scanDescriptorReference(c *envelopeCursor) (DescriptorReference, error) {
	role, err := c.sized()
	if err != nil {
		return DescriptorReference{}, err
	}
	id, err := c.take(16)
	if err != nil {
		return DescriptorReference{}, err
	}
	typeName, err := c.sized()
	if err != nil {
		return DescriptorReference{}, err
	}
	version, err := c.take(4)
	if err != nil {
		return DescriptorReference{}, err
	}
	hash, err := c.take(32)
	if err != nil {
		return DescriptorReference{}, err
	}
	var ref DescriptorReference
	copy(ref.ID[:], id)
	copy(ref.Integrity[:], hash)
	ref.SchemaVersion = binary.BigEndian.Uint32(version)
	if ref.ID == (DescriptorID{}) || ref.SchemaVersion == 0 || !validDescriptorNameBytes(role) || !validDescriptorNameBytes(typeName) {
		return DescriptorReference{}, errors.Join(ErrInvalidEncoding, ErrInvalidDescriptor)
	}
	// This scanner deliberately does not allocate strings. The second pass,
	// after complete envelope validation, owns names through string copies.
	return ref, nil
}

func inspectOpaqueEnvelope(src []byte, l Limits) (opaqueWireView, error) {
	if len(src) > l.MaxInputBytes || len(src) > l.MaxValueBytes || len(src) > l.MaxDescriptorBytes {
		return opaqueWireView{}, descriptorBudgetError()
	}
	if len(src) < opaqueDescriptorFixedBytes || src[0] != 'D' || src[1] != 'O' {
		return opaqueWireView{}, ErrInvalidEncoding
	}
	if src[2] != opaqueDescriptorCodecVersion {
		return opaqueWireView{}, errors.Join(ErrInvalidEncoding, ErrUnknownVersion)
	}
	if src[3] != byte(DescriptorPreservationOnly) {
		return opaqueWireView{}, ErrInvalidEncoding
	}
	view := opaqueWireView{}
	copy(view.id[:], src[4:20])
	view.version = binary.BigEndian.Uint32(src[20:24])
	copy(view.correlation[:], src[24:40])
	if view.id == (DescriptorID{}) || view.version == 0 {
		return opaqueWireView{}, errors.Join(ErrInvalidEncoding, ErrInvalidDescriptor)
	}
	c := envelopeCursor{src: src[40:]}
	var err error
	view.typeName, err = c.sized()
	if err != nil {
		return opaqueWireView{}, err
	}
	if !validDescriptorNameBytes(view.typeName) {
		return opaqueWireView{}, errors.Join(ErrInvalidEncoding, ErrInvalidDescriptor)
	}
	count, err := c.take(4)
	if err != nil {
		return opaqueWireView{}, err
	}
	n := binary.BigEndian.Uint32(count)
	if len(c.src) < 4 || uint64(n) > uint64((len(c.src)-4)/descriptorReferenceMinimumBytes) {
		return opaqueWireView{}, ErrInvalidEncoding
	}
	view.count = int(n) // #nosec G115 -- count bounded by delivered <= 1 MiB input.
	start := c.src
	for range view.count {
		if _, err := scanDescriptorReference(&c); err != nil {
			return opaqueWireView{}, err
		}
	}
	view.references = start[:len(start)-len(c.src)]
	view.payload, err = c.sized()
	if err != nil {
		return opaqueWireView{}, err
	}
	if len(c.src) != 0 {
		return opaqueWireView{}, ErrInvalidEncoding
	}
	return view, nil
}

// DecodeOpaqueDescriptor explicitly preserves unknown positive payload schema
// versions. Unknown envelope versions decline. Full structural validation runs
// before allocating persistent strings, references or payload copies.
func DecodeOpaqueDescriptor(src []byte, l Limits) (OpaqueDescriptor, error) {
	l, err := l.resolved()
	if err != nil {
		return OpaqueDescriptor{}, err
	}
	view, err := inspectOpaqueEnvelope(src, l)
	if err != nil {
		return OpaqueDescriptor{}, err
	}
	spec := OpaqueDescriptorSpec{ID: view.id, Type: string(view.typeName), SchemaVersion: view.version, Correlation: view.correlation, Payload: bytes.Clone(view.payload)}
	if view.count > 0 {
		spec.References = make([]DescriptorReference, view.count)
	}
	c := envelopeCursor{src: view.references}
	for i := range spec.References {
		// Sizes/fields were checked by inspectOpaqueEnvelope; this pass only owns them.
		role, _ := c.sized()
		id, _ := c.take(16)
		name, _ := c.sized()
		version, _ := c.take(4)
		hash, _ := c.take(32)
		ref := DescriptorReference{Role: string(role), Type: string(name), SchemaVersion: binary.BigEndian.Uint32(version)}
		copy(ref.ID[:], id)
		copy(ref.Integrity[:], hash)
		spec.References[i] = ref
	}
	return OpaqueDescriptor{spec: spec}, nil
}
