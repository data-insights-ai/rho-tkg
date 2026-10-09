package temporal

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Profile identifies a mathematical domain, independently of physical codecs.
type Profile uint8

// Native profile identifiers describe semantic domains, not endpoint widths.
const (
	ProfileIntegerZ Profile = iota + 1
	ProfileRationalQ
	ProfileLexicographicQN
)

// AxisID is a stable reference-system identity; it is not a local dictionary ID.
type AxisID [16]byte

// AxisDescriptor is the immutable definition supplied to NewAxis. Version 1
// supports Z, Q and lexicographic Q×N. Coordinates are expressed in CanonicalUnit;
// Reference identifies a versioned clock/reference system. Changing either
// changes the definition even when ID is reused, and comparisons reject that.
type AxisDescriptor struct {
	ID            AxisID
	Profile       Profile
	Version       uint16
	Reference     string
	CanonicalUnit string
}

// Axis owns a validated descriptor. Its zero value is invalid.
type Axis struct {
	desc   AxisDescriptor
	digest [32]byte
}

// Fixed canonical descriptor payload: ID(16), profile(1), version(2),
// and two string-length prefixes(8). Hash domain separation is outside it.
const axisDescriptorFixedBytes = 27

// axisDescriptorBytes counts the canonical descriptor payload; NewAxis bounded
// both string lengths before constructing the immutable axis.
func axisDescriptorBytes(a Axis) int {
	return axisDescriptorFixedBytes + len(a.desc.Reference) + len(a.desc.CanonicalUnit)
}

func (a Axis) validateDescriptorBudget(l Limits) error {
	if axisDescriptorBytes(a) > l.MaxDescriptorBytes {
		return fmt.Errorf("%w: axis descriptor bytes", ErrResourceLimit)
	}
	return nil
}

// NewAxis validates the complete descriptor before hashing or copying it.
func NewAxis(desc AxisDescriptor, l Limits) (Axis, error) {
	l, err := l.resolved()
	if err != nil {
		return Axis{}, err
	}
	if desc.ID == (AxisID{}) {
		return Axis{}, ErrInvalidAxis
	}
	if !validProfile(desc.Profile) {
		return Axis{}, ErrUnknownProfile
	}
	if desc.Version != 1 {
		return Axis{}, ErrUnknownVersion
	}
	if len(desc.Reference) > l.MaxDescriptorBytes || len(desc.CanonicalUnit) > l.MaxDescriptorBytes-len(desc.Reference) || axisDescriptorFixedBytes > l.MaxDescriptorBytes-len(desc.Reference)-len(desc.CanonicalUnit) {
		return Axis{}, fmt.Errorf("%w: axis descriptor bytes", ErrResourceLimit)
	}
	if !utf8.ValidString(desc.Reference) || !utf8.ValidString(desc.CanonicalUnit) || strings.TrimSpace(desc.Reference) == "" || strings.TrimSpace(desc.CanonicalUnit) == "" {
		return Axis{}, ErrInvalidAxis
	}
	desc.Reference = strings.Clone(desc.Reference)
	desc.CanonicalUnit = strings.Clone(desc.CanonicalUnit)
	buf := append([]byte("rho-tkg:axis:v1\x00"), desc.ID[:]...)
	buf = append(buf, byte(desc.Profile))
	buf = binary.BigEndian.AppendUint16(buf, desc.Version)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(desc.Reference))) // #nosec G115 -- descriptor length is bounded to 1 MiB.
	buf = append(buf, desc.Reference...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(desc.CanonicalUnit))) // #nosec G115 -- descriptor length is bounded to 1 MiB.
	buf = append(buf, desc.CanonicalUnit...)
	return Axis{desc: desc, digest: sha256.Sum256(buf)}, nil
}

func validProfile(p Profile) bool { return p >= ProfileIntegerZ && p <= ProfileLexicographicQN }
func (a Axis) validate() error {
	if a.desc.ID == (AxisID{}) || !validProfile(a.desc.Profile) || a.desc.Version != 1 || a.digest == ([32]byte{}) {
		return ErrInvalidAxis
	}
	return nil
}

// Descriptor returns the definition by value. Strings and ID are immutable/value
// data; modifying the returned fields cannot change the axis.
func (a Axis) Descriptor() AxisDescriptor { return a.desc }

// DefinitionHash returns the stable versioned digest of the full definition.
func (a Axis) DefinitionHash() [32]byte { return a.digest }

func sameAxis(a, b Axis) error {
	if a.desc.ID != b.desc.ID || a.digest != b.digest {
		return ErrAxisMismatch
	}
	return nil
}
