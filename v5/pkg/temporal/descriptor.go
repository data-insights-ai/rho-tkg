package temporal

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// ErrInvalidDescriptor identifies malformed opaque envelope metadata. It does
// not claim that rho checked the payload's schema or semantic consistency.
var ErrInvalidDescriptor = errors.New("temporal: invalid opaque descriptor")

// DescriptorID identifies a stored descriptor independently of its payload.
type DescriptorID [16]byte

// CorrelationID identifies a shared latent constraint; zero means none asserted.
type CorrelationID [16]byte

// DescriptorReference preserves a flat external reference in source order.
// Integrity optionally commits target bytes; zero means unspecified. No target
// existence, payload schema or compatibility is inferred by this envelope.
type DescriptorReference struct {
	Role          string
	ID            DescriptorID
	Type          string
	SchemaVersion uint32
	Integrity     [32]byte
}

// OpaqueDescriptorSpec supplies preservation-only metadata and uninterpreted
// bytes. Positive schema versions are retained even when unknown. No embedded
// Go object, evaluator, recursive descriptor or executable callback is accepted.
type OpaqueDescriptorSpec struct {
	ID            DescriptorID
	Type          string
	SchemaVersion uint32
	Correlation   CorrelationID
	References    []DescriptorReference
	Payload       []byte
}

// DescriptorSupport reports what rho guarantees about an opaque value.
type DescriptorSupport uint8

// Opaque support levels distinguish invalid values from byte preservation.
const (
	DescriptorSupportInvalid DescriptorSupport = iota
	DescriptorPreservationOnly
)

// OpaqueDescriptor owns immutable metadata, reference slices and payload bytes.
// Its zero value is invalid. Copying the value safely shares immutable storage.
type OpaqueDescriptor struct{ spec OpaqueDescriptorSpec }

const opaqueDescriptorFixedBytes = 52
const descriptorReferenceFixedBytes = 60
const descriptorReferenceMinimumBytes = descriptorReferenceFixedBytes + 2

// PreserveOpaqueDescriptor explicitly accepts an opaque payload schema. Only
// the bounded envelope is validated, never payload semantics or target objects.
// Every byte and count is checked before copying caller data.
func PreserveOpaqueDescriptor(spec OpaqueDescriptorSpec, l Limits) (OpaqueDescriptor, error) {
	l, err := l.resolved()
	if err != nil {
		return OpaqueDescriptor{}, err
	}
	if _, err := validateDescriptorSpec(spec, l); err != nil {
		return OpaqueDescriptor{}, err
	}
	spec.Type = strings.Clone(spec.Type)
	spec.Payload = bytes.Clone(spec.Payload)
	spec.References = slices.Clone(spec.References)
	for i := range spec.References {
		spec.References[i].Role = strings.Clone(spec.References[i].Role)
		spec.References[i].Type = strings.Clone(spec.References[i].Type)
	}
	return OpaqueDescriptor{spec: spec}, nil
}

func descriptorWireBytes(spec OpaqueDescriptorSpec, l Limits) (int, error) {
	capBytes := min(l.MaxDescriptorBytes, l.MaxValueBytes)
	size := opaqueDescriptorFixedBytes
	if size > capBytes || len(spec.Type) > capBytes-size {
		return 0, ErrResourceLimit
	}
	size += len(spec.Type)
	if len(spec.Payload) > capBytes-size {
		return 0, ErrResourceLimit
	}
	size += len(spec.Payload)
	if len(spec.References) > (capBytes-size)/descriptorReferenceMinimumBytes {
		return 0, ErrResourceLimit
	}
	for _, ref := range spec.References {
		for _, n := range []int{descriptorReferenceFixedBytes, len(ref.Role), len(ref.Type)} {
			if n > capBytes-size {
				return 0, ErrResourceLimit
			}
			size += n
		}
	}
	return size, nil
}

func validDescriptorName(s string) bool { return utf8.ValidString(s) && strings.TrimSpace(s) != "" }
func validateDescriptorSpec(spec OpaqueDescriptorSpec, l Limits) (int, error) {
	size, err := descriptorWireBytes(spec, l)
	if err != nil {
		return 0, err
	}
	if spec.ID == (DescriptorID{}) || spec.SchemaVersion == 0 || !validDescriptorName(spec.Type) {
		return 0, ErrInvalidDescriptor
	}
	for _, ref := range spec.References {
		if ref.ID == (DescriptorID{}) || ref.SchemaVersion == 0 || !validDescriptorName(ref.Role) || !validDescriptorName(ref.Type) {
			return 0, ErrInvalidDescriptor
		}
	}
	return size, nil
}
func (d OpaqueDescriptor) validate(l Limits) (int, error) { return validateDescriptorSpec(d.spec, l) }

// Spec returns owned reference/payload copies. Existing descriptor storage is
// bounded by the hard envelope ceiling; callers never receive mutable aliases.
func (d OpaqueDescriptor) Spec() OpaqueDescriptorSpec {
	spec := d.spec
	spec.Payload = bytes.Clone(spec.Payload)
	spec.References = slices.Clone(spec.References)
	return spec
}

// SupportLevel advertises preservation only; it never enables native predicates
// or claims payload semantics have been interpreted. The zero value is invalid.
func (d OpaqueDescriptor) SupportLevel() DescriptorSupport {
	if d.spec.ID == (DescriptorID{}) {
		return DescriptorSupportInvalid
	}
	return DescriptorPreservationOnly
}

// OpaqueDescriptorIntegrityHash commits canonical envelope bytes, including raw
// payload and ordered references. It is not opaque semantic equality or a proof.
func OpaqueDescriptorIntegrityHash(d OpaqueDescriptor, l Limits) ([32]byte, error) {
	wire, err := AppendOpaqueDescriptor(nil, d, l)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("rho-tkg:opaque-integrity:v1\x00"), wire...)), nil
}

func descriptorBudgetError() error {
	return fmt.Errorf("%w: opaque descriptor bytes", ErrResourceLimit)
}
